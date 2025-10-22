// SPDX-License-Identifier: AGPL-3.0

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::c2s::{self, Config};
use crate::channel::{ChannelAcl, ChannelConfig, ChannelManager};
use crate::transmitter::{Resource, Transmitter};
use zyn_common::conn::{ConnTx, State};
use zyn_common::service::C2sService;
use zyn_modulator::modulator::{
  AuthRequest, AuthResult, ForwardBroadcastPayloadRequest, ForwardBroadcastPayloadResult, Modulator, Operation,
  SendPrivatePayloadRequest, SendPrivatePayloadResult,
};
use zyn_protocol::ErrorReason::{
  BadRequest, InternalServerError, UnexpectedMessage, UnsupportedProtocolVersion, UsernameInUse,
};
use zyn_protocol::{AuthAckParameters, ConnectAckParameters, IdentifyAckParameters, Message, ModDirectAckParameters};
use zyn_protocol::{ChannelId, Zid};
use zyn_util::pool::PoolBuffer;
use zyn_util::slab::{Slab, SlabRef};
use zyn_util::string_atom::StringAtom;

/// The C2S connection manager.
pub type C2sConnManager = zyn_common::conn::ConnManager<C2sDispatcher, C2sDispatcherFactory, C2sService>;

#[derive(Clone)]
/// A transmitter implementation for C2S (client-to-server) connections.
///
/// This struct wraps a connection transmitter (`ConnTx`) along with the handler
/// identifier to provide a `Transmitter` interface for C2S connections.
struct C2sTransmitter {
  /// The local handler identifier for this connection
  handler: usize,

  /// The underlying connection transmitter
  conn_tx: ConnTx,
}

impl C2sTransmitter {
  fn new(handler: usize, conn_tx: ConnTx) -> Self {
    Self { handler, conn_tx }
  }
}

impl Transmitter for C2sTransmitter {
  fn send_message(&self, message: Message) {
    self.conn_tx.send_message(message);
  }

  fn send_message_with_payload(&self, message: Message, payload_opt: Option<PoolBuffer>) {
    self.conn_tx.send_message_with_payload(message, payload_opt);
  }

  fn resource(&self) -> Resource {
    Resource { domain: None, handler: self.handler }
  }
}

impl std::fmt::Debug for C2sTransmitter {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("C2sTransmitter").field("local_handler", &self.handler).finish()
  }
}

#[derive(Clone, Debug)]
pub struct C2sDispatcherFactory(Arc<Mutex<C2sDispatcherFactoryInner>>);

// ===== impl C2sDispatcherFactory =====

impl C2sDispatcherFactory {
  /// Creates a new C2S `C2sDispatcherFactory`.
  pub async fn new(
    config: Arc<Config>,
    channel_manager: ChannelManager,
    c2s_router: c2s::Router,
    modulator: Option<Arc<dyn Modulator>>,
  ) -> anyhow::Result<Self> {
    let max_connections = config.limits.max_connections;

    let auth_required = {
      match modulator.as_ref() {
        Some(modulator) => modulator.operations().await?.contains(Operation::Auth),
        None => false,
      }
    };

    let dispatchers: Slab<C2sDispatcher> = Slab::with_capacity(max_connections as usize);

    let inner =
      C2sDispatcherFactoryInner { config, channel_manager, c2s_router, modulator, dispatchers, auth_required };

    Ok(Self(Arc::new(Mutex::new(inner))))
  }
}

#[async_trait]
impl zyn_common::conn::DispatcherFactory<C2sDispatcher> for C2sDispatcherFactory {
  async fn create(&mut self, handler: usize, tx: ConnTx) -> SlabRef<C2sDispatcher> {
    let mut inner = self.0.lock().await;

    let dispatcher_opt = inner.dispatchers.acquire().await;
    assert!(dispatcher_opt.is_some());

    let dispatcher_ref = dispatcher_opt.unwrap();

    dispatcher_ref.write().await.init(
      handler,
      inner.config.clone(),
      inner.auth_required,
      inner.modulator.clone(),
      inner.channel_manager.clone(),
      inner.c2s_router.clone(),
      tx,
    );

    dispatcher_ref
  }

  async fn bootstrap(&mut self) -> anyhow::Result<()> {
    Ok(())
  }

  async fn shutdown(&mut self) -> anyhow::Result<()> {
    Ok(())
  }
}

#[derive(Clone, Debug)]
pub struct C2sDispatcherFactoryInner {
  /// The C2S configuration.
  config: Arc<Config>,

  /// The channel manager.
  channel_manager: ChannelManager,

  /// The C2S router.
  c2s_router: c2s::Router,

  /// The modulator, if any.
  modulator: Option<Arc<dyn Modulator>>,

  /// The dispatcher slab.
  dispatchers: Slab<C2sDispatcher>,

  /// Whether authentication is required.
  auth_required: bool,
}

#[derive(Debug, Default)]
pub struct C2sDispatcher(Option<C2sDispatcherInner>);

// ===== impl C2sDispatcher =====

impl C2sDispatcher {
  /// Initializes the dispatcher with the given parameters.
  #[allow(clippy::too_many_arguments)]
  pub fn init(
    &mut self,
    handler: usize,
    config: Arc<Config>,
    auth_required: bool,
    modulator: Option<Arc<dyn Modulator>>,
    channel_manager: ChannelManager,
    c2s_router: c2s::Router,
    conn_tx: ConnTx,
  ) {
    let inner = C2sDispatcherInner {
      config,
      channel_manager,
      c2s_router,
      heartbeat_interval: Default::default(),
      zid: None,
      transmitter: Arc::new(C2sTransmitter::new(handler, conn_tx)),
      modulator,
      auth_required,
    };

    self.0 = Some(inner);
  }
}

#[derive(Debug)]
struct C2sDispatcherInner {
  /// C2S configuration.
  config: Arc<Config>,

  /// Whether authentication is required.
  auth_required: bool,

  /// The transmitter registry.
  c2s_router: c2s::Router,

  /// The connection transmitter.
  transmitter: Arc<C2sTransmitter>,

  /// The modulator, if any.
  modulator: Option<Arc<dyn Modulator>>,

  /// The channel manager.
  channel_manager: ChannelManager,

  /// The ZID assigned to the connection.
  zid: Option<Zid>,

  /// The negotiated heartbeat interval.
  heartbeat_interval: Duration,
}

// ===== impl C2sDispatcherInner =====

impl C2sDispatcherInner {
  /// Handles the initial connection handshake with a client.
  ///
  /// # Arguments
  ///
  /// * `msg` - The message to process. Must be a `Message::C2sConnect` variant.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the connection is accepted.
  ///
  /// # Errors
  ///
  /// Returns an error if the protocol version is not supported or if an unexpected
  /// message type is received.
  async fn dispatch_message_in_connecting_state(&mut self, msg: Message) -> anyhow::Result<()> {
    let config = self.config.clone();
    let config_keep_alive_interval = config.keep_alive_interval;
    let config_min_keep_alive_interval = config.min_keep_alive_interval;

    match msg {
      Message::Connect(params) => {
        if params.protocol_version != 1 {
          return Err(zyn_protocol::Error::new(UnsupportedProtocolVersion).into());
        }

        let mut heartbeat_interval = Duration::from_millis(params.heartbeat_interval as u64);
        if heartbeat_interval.is_zero() {
          heartbeat_interval = config_keep_alive_interval;
        } else if heartbeat_interval < config_min_keep_alive_interval {
          heartbeat_interval = config_min_keep_alive_interval;
        } else if heartbeat_interval > config_keep_alive_interval {
          heartbeat_interval = config_keep_alive_interval
        }
        self.heartbeat_interval = heartbeat_interval;

        // Send the proper reply message informing the client that it is connected.
        let application_protocol =
          if let Some(modulator) = self.modulator.as_ref() { Some(modulator.protocol_name().await?) } else { None };

        let reply_msg = Message::ConnectAck(ConnectAckParameters {
          auth_required: self.auth_required,
          application_protocol,
          heartbeat_interval: heartbeat_interval.as_millis() as u32,
          max_subscriptions: config.limits.max_channels_per_client,
          max_message_size: config.limits.max_message_size,
          max_payload_size: config.limits.max_payload_size,
          max_inflight_requests: config.limits.max_inflight_requests,
        });

        self.transmitter.send_message(reply_msg);

        info!(
          handler = self.transmitter.handler,
          auth_required = self.auth_required,
          max_channels_per_client = config.limits.max_channels_per_client,
          max_message_size = config.limits.max_message_size,
          max_payload_size = config.limits.max_payload_size,
          max_inflight_requests = config.limits.max_inflight_requests,
          "handshake completed"
        );
      },
      _ => {
        return Err(zyn_protocol::Error::new(UnexpectedMessage).into());
      },
    }
    Ok(())
  }

  /// Handles client identification in the connected state.
  ///
  /// # Arguments
  ///
  /// * `msg` - The message to process. Must be a `Message::Identify` or `Message::Auth` variant.
  ///
  /// # Returns
  ///
  /// Returns `Ok(true)` if identification/authentication succeeds and the connection should
  /// transition to the authenticated state. Returns `Ok(false)` if authentication is still
  /// in progress (multi-step authentication).
  ///
  /// # Errors
  ///
  /// Returns an error in the following cases:
  /// * If the username is invalid
  /// * If the username is already in use when attempting to identify (authentication disabled)
  /// * If an unexpected message type is received
  async fn dispatch_message_in_connected_state(&mut self, msg: Message) -> anyhow::Result<bool> {
    match msg {
      Message::Auth(params) => {
        // If authentication is not required, reject the message.
        if !self.auth_required {
          return Err(zyn_protocol::Error::new(UnexpectedMessage).into());
        }

        match self.modulator.as_ref().unwrap().authenticate(AuthRequest { token: params.token }).await {
          Ok(auth_res) => match auth_res.result {
            AuthResult::Success { username } => {
              let zid = {
                match self.make_local_zid(username.clone()) {
                  Ok(zid) => zid,
                  Err(e) => {
                    return Err(zyn_protocol::Error::new(InternalServerError).with_detail(e.to_string()).into());
                  },
                }
              };

              // Register the connection non-exclusively.
              let _ = self.c2s_router.register_connection(
                zid.username.clone(),
                self.transmitter.clone(),
                self.transmitter.handler,
                false,
              );

              self.zid = Some(zid.clone());

              self.transmitter.send_message(Message::AuthAck(AuthAckParameters {
                challenge: None,
                succeeded: Some(true),
                zid: Some(zid.clone().into()),
              }));

              info!(handler = self.transmitter.handler, zid = zid.to_string(), "user authenticated");

              Ok(true)
            },
            AuthResult::Continue { challenge } => {
              self.transmitter.send_message(Message::AuthAck(AuthAckParameters {
                challenge: Some(challenge),
                succeeded: None,
                zid: None,
              }));

              Ok(false)
            },
            AuthResult::Failure => {
              self.transmitter.send_message(Message::AuthAck(AuthAckParameters {
                challenge: None,
                succeeded: Some(false),
                zid: None,
              }));

              Ok(false)
            },
          },
          Err(e) => Err(zyn_protocol::Error::new(InternalServerError).with_detail(e.to_string()).into()),
        }
      },
      Message::Identify(params) => {
        // If authentication is required, reject the message.
        if self.auth_required {
          return Err(zyn_protocol::Error::new(UnexpectedMessage).into());
        }

        // Check if the username is already in use.
        let username = params.username.trim();

        let zid = {
          match self.make_local_zid(username.into()) {
            Ok(zid) => zid,
            Err(e) => {
              return Err(zyn_protocol::Error::new(BadRequest).with_detail(e.to_string()).into());
            },
          }
        };

        // Register the connection exclusively.
        if self.c2s_router.register_connection(
          zid.username.clone(),
          self.transmitter.clone(),
          self.transmitter.handler,
          true,
        ) {
          self.zid = Some(zid);
        } else {
          return Err(zyn_protocol::Error::new(UsernameInUse).into());
        }

        let zid = self.zid.as_ref().unwrap();

        self.transmitter.send_message(Message::IdentifyAck(IdentifyAckParameters { zid: StringAtom::from(zid) }));

        info!(handler = self.transmitter.handler, zid = zid.to_string(), "user identified");

        Ok(true)
      },
      _ => Err(zyn_protocol::Error::new(UnexpectedMessage).into()),
    }
  }

  /// Routes messages received in the authenticated state to their appropriate handlers.
  ///
  /// # Arguments
  ///
  /// * `msg` - The message to dispatch
  /// * `payload` - Optional payload buffer associated with the message
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the message was successfully handled.
  ///
  /// # Errors
  ///
  /// Returns an `UnexpectedMessage` error if the message type is not supported
  /// in the authenticated state.
  async fn dispatch_message_in_authenticated_state(
    &mut self,
    msg: Message,
    payload: Option<PoolBuffer>,
  ) -> anyhow::Result<()> {
    match msg {
      Message::Broadcast { .. } => {
        self.dispatch_broadcast_message(msg, payload.unwrap()).await?;
      },
      Message::GetChannelAcl { .. } => {
        self.dispatch_get_channel_acl_message(msg).await?;
      },
      Message::GetChannelConfiguration { .. } => {
        self.dispatch_get_channel_configuration_message(msg).await?;
      },
      Message::JoinChannel { .. } => {
        self.dispatch_join_message(msg).await?;
      },
      Message::LeaveChannel { .. } => {
        self.dispatch_leave_message(msg).await?;
      },
      Message::ListChannels { .. } => {
        self.dispatch_list_channels_message(msg).await?;
      },
      Message::ListMembers { .. } => {
        self.dispatch_list_members_message(msg).await?;
      },
      Message::ModDirect { .. } => {
        self.dispatch_mod_direct_message(msg, payload.unwrap()).await?;
      },
      Message::SetChannelAcl { .. } => {
        self.dispatch_set_channel_acl_message(msg).await?;
      },
      Message::SetChannelConfiguration { .. } => {
        self.dispatch_set_channel_configuration_message(msg).await?;
      },
      _ => {
        return Err(zyn_protocol::Error::new(UnexpectedMessage).into());
      },
    }
    Ok(())
  }

  /// Handles broadcasting a payload to a channel.
  ///
  /// # Arguments
  ///
  /// * `msg` - The broadcast message containing channel and correlation ID
  /// * `payload` - The payload to broadcast to channel members
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the payload was successfully broadcast.
  ///
  /// # Errors
  ///
  /// Returns an error if the channel ID is invalid or if the broadcast operation fails.
  async fn dispatch_broadcast_message(&mut self, msg: Message, payload: PoolBuffer) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::Broadcast { .. }));

    let mut correlation_id: u16 = 0;
    let mut channel_id: Option<ChannelId> = None;

    if let Message::Broadcast(params) = msg {
      correlation_id = params.id;
      channel_id = Some(Self::parse_channel_id(&params.channel)?);
    }
    let channel_id = channel_id.unwrap();

    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    // Forward the payload to the modulator (if available) for validation and alteration.
    let mut altered_payload = payload;

    if let Some(modulator) = self.modulator.as_ref() {
      let request = ForwardBroadcastPayloadRequest {
        payload: altered_payload.clone(),
        from: zid.clone(),
        channel_handler: channel_id.handler,
      };
      match modulator.forward_broadcast_payload(request).await {
        Ok(forward_res) => match forward_res.result {
          ForwardBroadcastPayloadResult::Valid => {
            // Keep the original payload
          },
          ForwardBroadcastPayloadResult::ValidWithAlteration { altered_payload: modified_payload } => {
            altered_payload = modified_payload;
          },
          ForwardBroadcastPayloadResult::Invalid => {
            return Err(zyn_protocol::Error::new(BadRequest).with_id(correlation_id).into());
          },
        },
        Err(e) => {
          error!(
            handler = self.transmitter.handler,
            zid = zid.to_string(),
            channel = channel_id.to_string(),
            error = e.to_string(),
            "payload validation failed"
          );
          return Err(zyn_protocol::Error::new(InternalServerError).with_id(correlation_id).into());
        },
      }
    }

    // Submit the request to broadcast the payload.
    let payload_length = altered_payload.as_slice().len() as u32;

    self
      .channel_manager
      .broadcast_payload(altered_payload, channel_id.clone(), zid.clone(), transmitter, correlation_id)
      .await?;

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.to_string(),
      content_length = payload_length,
      "broadcasted payload"
    );

    Ok(())
  }

  /// Handles requests to retrieve a channel's access control list (ACL).
  ///
  /// # Arguments
  ///
  /// * `msg` - The get ACL message containing channel and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the ACL was successfully retrieved and sent.
  ///
  /// # Errors
  ///
  /// Returns an error if the channel ID is invalid or if the ACL retrieval fails.
  async fn dispatch_get_channel_acl_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::GetChannelAcl { .. }));

    let mut correlation_id: u16 = 0;
    let mut channel_id: Option<ChannelId> = None;

    if let Message::GetChannelAcl(params) = msg {
      correlation_id = params.id;
      channel_id = Some(Self::parse_channel_id(&params.channel)?);
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    let channel_id = channel_id.unwrap();

    // Submit the request to get the channel ACL.
    self.channel_manager.get_channel_acl(channel_id.clone(), zid.clone(), transmitter, correlation_id).await?;

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.to_string(),
      "got channel ACL"
    );

    Ok(())
  }

  /// Handles requests to retrieve a channel's configuration.
  ///
  /// # Arguments
  ///
  /// * `msg` - The get configuration message containing channel and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the configuration was successfully retrieved and sent.
  ///
  /// # Errors
  ///
  /// Returns an error if the channel ID is invalid or if the configuration retrieval fails.
  async fn dispatch_get_channel_configuration_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::GetChannelConfiguration { .. }));

    let mut channel_id: Option<ChannelId> = None;
    let mut correlation_id: u16 = 0;

    if let Message::GetChannelConfiguration(params) = msg {
      correlation_id = params.id;
      channel_id = Some(Self::parse_channel_id(&params.channel)?);
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    let channel_id = channel_id.unwrap();

    // Submit the request to get the channel configuration.
    self
      .channel_manager
      .get_channel_configuration(channel_id.clone(), zid.clone(), transmitter, correlation_id)
      .await?;

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.to_string(),
      "got channel configuration"
    );

    Ok(())
  }

  /// Handles requests to update a channel's access control list (ACL).
  ///
  /// # Arguments
  ///
  /// * `msg` - The set ACL message containing channel, new ACL settings, and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the ACL was successfully updated.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The channel ID is invalid
  /// * The user lacks permission to modify the ACL
  /// * The ACL update operation fails
  async fn dispatch_set_channel_acl_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::SetChannelAcl { .. }));

    let mut correlation_id: u16 = 0;
    let mut channel_id: Option<ChannelId> = None;
    let mut acl = ChannelAcl::default();

    if let Message::SetChannelAcl(params) = msg {
      correlation_id = params.id;
      channel_id = Some(Self::parse_channel_id(&params.channel)?);

      let allow_join_list = {
        match params.allow_join.into_iter().map(|s| Self::parse_zid(&s)).collect() {
          Ok(list) => list,
          Err(e) => return Err(e),
        }
      };
      let allow_publish_list = {
        match params.allow_publish.into_iter().map(|s| Self::parse_zid(&s)).collect() {
          Ok(list) => list,
          Err(e) => return Err(e),
        }
      };
      let allow_read_list = {
        match params.allow_read.into_iter().map(|s| Self::parse_zid(&s)).collect() {
          Ok(list) => list,
          Err(e) => return Err(e),
        }
      };

      acl = ChannelAcl::new(allow_join_list, allow_publish_list, allow_read_list);
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    let channel_id = channel_id.unwrap();

    // Submit the request to set the channel ACL.
    self.channel_manager.set_channel_acl(acl, channel_id.clone(), zid.clone(), transmitter, correlation_id).await?;

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.to_string(),
      "set channel ACL"
    );

    Ok(())
  }

  /// Handles requests to update a channel's configuration.
  ///
  /// # Arguments
  ///
  /// * `msg` - The set configuration message containing channel, new settings, and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the configuration was successfully updated.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The channel ID is invalid
  /// * The user lacks permission to modify the configuration
  /// * The configuration update operation fails
  async fn dispatch_set_channel_configuration_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::SetChannelConfiguration { .. }));

    let mut correlation_id: u16 = 0;
    let mut channel_id: Option<ChannelId> = None;

    let mut channel_config = ChannelConfig::default();

    if let Message::SetChannelConfiguration(params) = msg {
      correlation_id = params.id;

      channel_id = Some(Self::parse_channel_id(&params.channel)?);

      channel_config.max_clients = params.max_clients;
      channel_config.max_payload_size = params.max_payload_size;
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    let channel_id = channel_id.unwrap();

    // Submit the request to set the channel configuration.
    self
      .channel_manager
      .set_channel_configuration(channel_config, channel_id.clone(), zid.clone(), transmitter, correlation_id)
      .await?;

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.to_string(),
      "set channel configuration"
    );

    Ok(())
  }

  /// Handles channel join requests.
  ///
  /// This method supports both joining existing channels and creating new ones:
  /// * If a channel ID is provided, attempts to join that channel
  /// * If no channel ID is provided, creates a new channel and joins it
  /// * Supports joining on behalf of another user if authorized
  ///
  /// # Arguments
  ///
  /// * `msg` - The join message containing optional channel ID and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the join operation succeeds.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The channel ID is invalid
  /// * The user lacks permission to join
  /// * The channel is full
  async fn dispatch_join_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::JoinChannel { .. }));

    let mut channel_id: Option<ChannelId> = None;
    let mut correlation_id: u16 = 0;
    let mut on_behalf_zid: Option<Zid> = None;

    if let Message::JoinChannel(params) = msg {
      correlation_id = params.id;

      if let Some(channel) = params.channel {
        channel_id = Some(Self::parse_channel_id(&channel)?);
      }

      if let Some(zid_str) = params.on_behalf {
        on_behalf_zid = Some(Self::parse_zid(&zid_str)?);
      }
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    // Submit the request to join the channel.
    let mut as_owner = false;

    if let Some(channel_id) = channel_id.as_ref() {
      self
        .channel_manager
        .join_channel(channel_id.clone(), zid.clone(), on_behalf_zid, transmitter, correlation_id)
        .await?;
    } else {
      channel_id = Some(self.channel_manager.join_new_channel(zid.clone(), transmitter, correlation_id).await?);
      as_owner = true;
    }

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.unwrap().to_string(),
      as_owner = as_owner,
      "joined channel"
    );

    Ok(())
  }

  /// Handles channel leave requests.
  ///
  /// Supports leaving a channel directly or on behalf of another user if authorized.
  ///
  /// # Arguments
  ///
  /// * `msg` - The leave message containing channel ID and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the leave operation succeeds.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The channel ID is invalid
  /// * The user is not in the channel
  /// * The user lacks permission to remove others
  async fn dispatch_leave_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::LeaveChannel { .. }));

    let mut channel_id: Option<ChannelId> = None;
    let mut correlation_id: u16 = 0;
    let mut on_behalf_zid: Option<Zid> = None;

    if let Message::LeaveChannel(params) = msg {
      correlation_id = params.id;
      channel_id = Some(Self::parse_channel_id(&params.channel)?);

      if let Some(zid_str) = params.on_behalf {
        on_behalf_zid = Some(Self::parse_zid(&zid_str)?);
      }
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    let channel_id = channel_id.unwrap();

    // Submit the request to leave the channel.
    self
      .channel_manager
      .leave_channel(channel_id.clone(), zid.clone(), on_behalf_zid, Some(transmitter), correlation_id)
      .await?;

    info!(handler = self.transmitter.handler, zid = zid.to_string(), channel = channel_id.to_string(), "left channel");

    Ok(())
  }

  /// Handles requests to list available channels.
  ///
  /// Can list either all accessible channels or only owned channels.
  ///
  /// # Arguments
  ///
  /// * `msg` - The list channels message containing correlation ID and owner filter
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel list was successfully retrieved and sent.
  async fn dispatch_list_channels_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::ListChannels { .. }));

    let mut correlation_id: u16 = 0;
    let mut as_owner: bool = false;

    if let Message::ListChannels(params) = msg {
      correlation_id = params.id;
      as_owner = params.owner;
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    // Submit the request to list the channels.
    self.channel_manager.list_channels(zid.clone(), as_owner, transmitter, correlation_id).await?;

    info!(handler = self.transmitter.handler, zid = zid.to_string(), as_owner = as_owner, "listed channels");

    Ok(())
  }

  /// Handles requests to list channel members.
  ///
  /// # Arguments
  ///
  /// * `msg` - The list members message containing channel and correlation ID
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the member list was successfully retrieved and sent.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The channel ID is invalid
  /// * The user lacks permission to list members
  async fn dispatch_list_members_message(&mut self, msg: Message) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::ListMembers { .. }));

    let mut correlation_id: u16 = 0;
    let mut channel_id: Option<ChannelId> = None;

    if let Message::ListMembers(params) = msg {
      correlation_id = params.id;
      channel_id = Some(Self::parse_channel_id(&params.channel)?);
    }
    let zid = self.zid.as_ref().unwrap().clone();
    let transmitter = self.transmitter.clone();

    let channel_id = channel_id.unwrap();

    // Submit the request to list the members.
    self.channel_manager.list_members(channel_id.clone(), zid.clone(), transmitter, correlation_id).await?;

    info!(
      handler = self.transmitter.handler,
      zid = zid.to_string(),
      channel = channel_id.to_string(),
      "listed members"
    );

    Ok(())
  }

  /// Dispatches a private payload directly to the modulator for processing.
  ///
  /// # Arguments
  ///
  /// * `msg` - The `MOD_DIRECT` message containing the correlation ID and payload length
  /// * `payload` - The raw payload data to be forwarded to the modulator
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` on successful processing and acknowledgment.
  async fn dispatch_mod_direct_message(&mut self, msg: Message, payload: PoolBuffer) -> anyhow::Result<()> {
    assert!(matches!(msg, Message::ModDirect { .. }));

    let modulator = {
      match self.modulator.as_ref() {
        Some(modulator) => modulator,
        None => return Err(zyn_protocol::Error::new(UnexpectedMessage).into()),
      }
    };

    // Check if direct forwarding is supported.
    if !modulator.operations().await?.contains(Operation::SendPrivatePayload) {
      return Err(zyn_protocol::Error::new(UnexpectedMessage).into());
    }

    let params = match msg {
      Message::ModDirect(params) => params,
      _ => unreachable!(),
    };

    let correlation_id: u16 = {
      match params.id {
        Some(id) => id,
        None => return Err(zyn_protocol::Error::new(BadRequest).into()),
      }
    };

    let zid = self.zid.as_ref().unwrap().clone();

    let request = SendPrivatePayloadRequest { payload, from: zid.username.clone() };
    let response = modulator.send_private_payload(request).await?;
    if matches!(response.result, SendPrivatePayloadResult::Invalid) {
      return Err(zyn_protocol::Error::new(BadRequest).with_id(correlation_id).into());
    }
    let transmitter = self.transmitter.clone();

    // Send the response back to the client.
    transmitter.send_message(Message::ModDirectAck(ModDirectAckParameters { id: correlation_id }));

    info!(handler = transmitter.handler, zid = zid.to_string(), "modulator payload forwarded");

    Ok(())
  }

  fn make_local_zid(&self, username: StringAtom) -> anyhow::Result<Zid> {
    match Zid::new(username, StringAtom::from(self.config.listener.domain.as_str())) {
      Ok(zid) => Ok(zid),
      Err(e) => Err(anyhow::Error::new(e)),
    }
  }

  fn parse_channel_id(s: &str) -> anyhow::Result<ChannelId> {
    match ChannelId::from_str(s) {
      Ok(id) => Ok(id),
      Err(e) => Err(zyn_protocol::Error::new(BadRequest).with_detail(e.to_string()).into()),
    }
  }

  fn parse_zid(s: &str) -> anyhow::Result<Zid> {
    match Zid::from_str(s) {
      Ok(zid) => Ok(zid),
      Err(e) => Err(zyn_protocol::Error::new(BadRequest).with_detail(e.to_string()).into()),
    }
  }
}

#[async_trait]
impl zyn_common::conn::Dispatcher for C2sDispatcher {
  async fn dispatch_message(
    &mut self,
    msg: Message,
    payload: Option<PoolBuffer>,
    state: State,
  ) -> anyhow::Result<Option<State>> {
    let inner = self.0.as_mut().unwrap();

    match state {
      State::Connecting => {
        inner.dispatch_message_in_connecting_state(msg).await?;
        Ok(Some(State::Connected))
      },
      State::Connected => {
        if inner.dispatch_message_in_connected_state(msg).await? {
          return Ok(Some(State::Authenticated { heartbeat_interval: inner.heartbeat_interval }));
        }
        Ok(None)
      },
      State::Authenticated { .. } => {
        inner.dispatch_message_in_authenticated_state(msg, payload).await?;
        Ok(None)
      },
    }
  }

  async fn bootstrap(&mut self) -> anyhow::Result<()> {
    Ok(())
  }

  async fn shutdown(&mut self) -> anyhow::Result<()> {
    let inner = self.0.as_mut().unwrap();

    if let Some(zid) = inner.zid.take() {
      // Leave from all channels.
      inner.channel_manager.leave_all_channels(zid.clone()).await?;

      // Unregister the username.
      inner.c2s_router.unregister_connection(&zid.username, inner.transmitter.handler);
    }

    Ok(())
  }
}
