// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use tokio::sync::RwLock;
use zyn_protocol::ErrorReason::{
  BadRequest, ChannelIsFull, ChannelNotFound, Forbidden, NotAllowed, NotImplemented, PolicyViolation, UserInChannel,
  UserNotInChannel, UserNotRegistered,
};
use zyn_protocol::{
  BroadcastAckParameters, ChannelAclParameters, ChannelConfigurationParameters, JoinChannelAckParameters,
  LeaveChannelAckParameters, ListChannelsAckParameters, ListMembersAckParameters, Message, MessageParameters, QoS,
};
use zyn_protocol::{ChannelId, Zid};
use zyn_protocol::{Event, EventKind};
use zyn_util::pool::PoolBuffer;
use zyn_util::slab::Slab;
use zyn_util::string_atom::StringAtom;

use crate::notifier::Notifier;
use crate::router::GlobalRouter;
use crate::transmitter::{Resource, Transmitter};

/// The channel manager inner state.
#[derive(Debug)]
struct ChannelManagerInner {
  /// The channels.
  channels: Slab<Channel>,

  /// The channels a user is a member of.
  in_channels: HashMap<StringAtom, HashSet<ChannelId>>,

  /// The global router.
  router: GlobalRouter,

  /// The event notifier.
  notifier: Notifier,

  /// The maximum allowed clients per channel.
  max_clients_per_channel: u32,

  /// The maximum number of channels a user can join.
  max_channels_per_client: u32,

  /// The maximum payload size allowed.
  max_payload_size: u32,
}

// ===== impl ChannelManagerInner =====

impl ChannelManagerInner {
  fn check_subscription_limit(&self, username: &StringAtom, correlation_id: u32) -> anyhow::Result<()> {
    if let Some(in_channels) = self.in_channels.get(username)
      && in_channels.len() >= self.max_channels_per_client as usize
    {
      return Err(
        zyn_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("subscription limit reached")
          .into(),
      );
    }
    Ok(())
  }

  fn validate_channel_configuration(&self, config: &ChannelConfig, correlation_id: u32) -> anyhow::Result<()> {
    if config.max_clients > self.max_clients_per_channel {
      return Err(
        zyn_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_clients exceeds server established limit")
          .into(),
      );
    }

    if config.max_payload_size > self.max_payload_size {
      return Err(
        zyn_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_payload_size exceeds server established limit")
          .into(),
      );
    }
    Ok(())
  }

  fn default_channel_configuration(&self) -> ChannelConfig {
    ChannelConfig { max_clients: self.max_clients_per_channel, max_payload_size: self.max_payload_size }
  }
}

/// The channel manager.
#[derive(Clone, Debug)]
pub struct ChannelManager(Arc<RwLock<ChannelManagerInner>>);

// ===== impl ChannelManager =====

impl ChannelManager {
  /// Creates a new channel manager with the specified configuration.
  ///
  /// # Arguments
  ///
  /// * `router` - The global router
  /// * `max_channels` - Maximum number of channels allowed
  /// * `max_clients_per_channel` - Maximum number of clients allowed per channel
  /// * `max_channels_per_client` - Maximum number of channels a client can join
  /// * `max_payload_size` - Maximum payload size allowed in channels
  ///
  /// # Returns
  ///
  /// A new instance of `ChannelManager`
  pub fn new(
    router: GlobalRouter,
    notifier: Notifier,
    max_channels: u32,
    max_clients_per_channel: u32,
    max_channels_per_client: u32,
    max_payload_size: u32,
  ) -> Self {
    let channels = Slab::with_capacity(max_channels as usize);

    let inner = ChannelManagerInner {
      router,
      notifier,
      channels,
      in_channels: HashMap::new(),
      max_clients_per_channel,
      max_channels_per_client,
      max_payload_size,
    };

    Self(Arc::new(RwLock::new(inner)))
  }

  /// Lists all channels that a user is a member of.
  ///
  /// # Arguments
  ///
  /// * `zid` - The user identifier
  /// * `as_owner` - If true, only lists channels where the user is the owner
  /// * `transmitter` - The transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn list_channels(
    &self,
    zid: Zid,
    as_owner: bool,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;

    // Gather all channels the user is a member of and sort them.
    let mut channels: Vec<StringAtom> = Default::default();

    if let Some(in_channels) = mng_guard.in_channels.get(&zid.username) {
      for channel_id in in_channels.iter() {
        if as_owner {
          let channel_ref = mng_guard.channels.ref_from_handler(channel_id.handler as usize).await.unwrap();

          if channel_ref.read().await.is_owner(&zid) {
            channels.push(channel_id.into());
          }
        } else {
          channels.push(channel_id.into());
        }
      }
    }
    drop(mng_guard);

    channels.sort();

    // Send response back to the client.
    transmitter.send_message(Message::ListChannelsAck(ListChannelsAckParameters { id: correlation_id, channels }));

    Ok(())
  }

  /// Lists all members of a specific channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier
  /// * `zid` - The user identifier requesting the list
  /// * `transmitter` - The transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn list_members(
    &self,
    channel_id: ChannelId,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotImplemented).into());
    }

    // Check if the channel exists and if the originating connection is a member of it.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let channel_guard = channel_ref.read().await;

    if !channel_guard.is_member(&zid) {
      return Err(zyn_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }
    // Gather all members of the channel and sort them.
    let mut members: Vec<StringAtom> = channel_guard.members.iter().map(|member_zid| member_zid.into()).collect();
    drop(channel_guard);

    members.sort();

    // Send response back to the client.
    transmitter.send_message(Message::ListMembersAck(ListMembersAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
      members,
    }));

    Ok(())
  }

  /// Creates and joins a new channel.
  ///
  /// # Arguments
  ///
  /// * `zid` - The user identifier creating the channel
  /// * `transmitter` - The transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// The ID of the newly created channel
  pub async fn join_new_channel(
    &mut self,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<ChannelId> {
    let mut mng_guard = self.0.write().await;

    // Check if maximum number of subscriptions is reached.
    mng_guard.check_subscription_limit(&zid.username, correlation_id)?;

    // Acquire a channel.
    let channel_ref_opt = mng_guard.channels.acquire().await;

    if channel_ref_opt.is_none() {
      return Err(
        zyn_protocol::Error::new(PolicyViolation).with_id(correlation_id).with_detail("channel limit reached").into(),
      );
    }
    let channel_ref = channel_ref_opt.unwrap();
    let mut channel_guard = channel_ref.write().await;

    // Initialize the channel.
    let config = mng_guard.default_channel_configuration();

    channel_guard.0 = Some(ChannelInner {
      handler: channel_ref.handler as u32,
      owner: None,
      config,
      acl: ChannelAcl::default(),
      members: HashSet::new(),
      notifier: mng_guard.notifier.clone(),
    });

    // Insert the member into the channel and update the list of channels
    // the connection is a member of.
    channel_guard.insert_member(zid.clone());

    let channel_id =
      ChannelId::new(channel_ref.handler as u32, mng_guard.router.c2s_router().local_domain().clone()).unwrap();

    let in_channels = &mut mng_guard.in_channels;
    in_channels.entry(zid.username.clone()).or_default().insert(channel_id.clone());

    drop(mng_guard);

    // Send response back to the client.
    transmitter.send_message(Message::JoinChannelAck(JoinChannelAckParameters {
      id: correlation_id,
      channel: (&channel_id).into(),
    }));

    Ok(channel_id)
  }

  /// Joins an existing channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier to join
  /// * `zid` - The user identifier joining the channel
  /// * `oh_behalf_zid` - Optional user identifier to join on behalf of
  /// * `transmitter` - The connection transaction for the client
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn join_channel(
    &mut self,
    channel_id: ChannelId,
    zid: Zid,
    oh_behalf_zid: Option<Zid>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mut mng_guard = self.0.write().await;

    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }
    // Check if the channel handler exists.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let mut channel_guard = channel_ref.write().await;

    // Get the ZID of the new member.
    let new_member_zid = {
      match oh_behalf_zid {
        Some(oh_behalf_zid) => {
          // Ensure the client is authorized to join the channel on behalf of another user.
          if !channel_guard.is_owner(&zid) {
            return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
          }

          if !mng_guard.router.c2s_router().has_connection(&oh_behalf_zid.username) {
            return Err(zyn_protocol::Error::new(UserNotRegistered).with_id(correlation_id).into());
          }

          oh_behalf_zid.clone()
        },
        None => zid.clone(),
      }
    };
    // Check if the new member is allowed to join the channel.
    let acl = &channel_guard.acl;

    if !acl.is_join_allowed(&new_member_zid) {
      return Err(zyn_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Insert the member into the channel in case it is not already a member
    // and the channel is not full, and notify all members about the new member.
    let config = &channel_guard.config;

    if channel_guard.is_member(&new_member_zid) {
      return Err(zyn_protocol::Error::new(UserInChannel).with_id(correlation_id).into());
    } else if channel_guard.member_count() >= config.max_clients as usize {
      return Err(zyn_protocol::Error::new(ChannelIsFull).with_id(correlation_id).into());
    }
    // Check if the maximum number of subscriptions is reached.
    mng_guard.check_subscription_limit(&new_member_zid.username, correlation_id)?;

    channel_guard.insert_member(new_member_zid.clone());

    channel_guard
      .notify_member_joined(
        &new_member_zid,
        Some(transmitter.resource()),
        false,
        mng_guard.router.c2s_router().local_domain().clone(),
      )
      .await?;

    // Update the list of channels the connection is a member of.
    let in_channels = &mut mng_guard.in_channels;
    in_channels.entry(new_member_zid.username.clone()).or_default().insert(channel_id.clone());

    drop(mng_guard);

    // Send response back to the client.
    transmitter.send_message(Message::JoinChannelAck(JoinChannelAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
    }));

    Ok(())
  }

  /// Leaves a channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier to leave
  /// * `zid` - The user identifier leaving the channel
  /// * `on_behalf_zid` - Optional user identifier to leave on behalf of
  /// * `transmitter` - Optional connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn leave_channel(
    &mut self,
    channel_id: ChannelId,
    zid: Zid,
    on_behalf_zid: Option<Zid>,
    transmitter: Option<Arc<dyn Transmitter>>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mut mng_guard = self.0.write().await;

    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }
    // Check if the channel exists.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let mut channel_guard = channel_ref.write().await;

    let left_member_zid = {
      match on_behalf_zid {
        Some(z) => {
          if !channel_guard.is_owner(&zid) {
            return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
          }
          z
        },
        None => zid.clone(),
      }
    };

    if !channel_guard.is_member(&left_member_zid) {
      return Err(zyn_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    // Notify members about the left member, and remove the member from the channel.
    let as_owner = channel_guard.is_owner(&left_member_zid);

    let resource = transmitter.as_ref().map(|transmitter| transmitter.resource());

    channel_guard
      .notify_member_left(&left_member_zid, resource, as_owner, router.c2s_router().local_domain().clone())
      .await?;

    channel_guard.remove_member(&left_member_zid);

    // Update the list of channels the connection is a member of.
    let in_channels = &mut mng_guard.in_channels;

    if let Some(in_channels_set) = in_channels.get_mut(&left_member_zid.username) {
      in_channels_set.remove(&channel_id);
      if in_channels_set.is_empty() {
        in_channels.remove(&left_member_zid.username);
      }
    }
    drop(mng_guard);

    // Send response back to the client if a handler is provided.
    if let Some(transmitter) = transmitter {
      transmitter.send_message(Message::LeaveChannelAck(LeaveChannelAckParameters { id: correlation_id }));
    }

    // If the channel is now empty, release it.
    if channel_guard.is_empty() {
      drop(channel_guard);

      channel_ref.release().await;

      return Ok(());
    }

    // If the left member was the owner, pick a new owner (randomly) and notify all members.
    if as_owner {
      let new_owner_zid = channel_guard.pick_new_owner().unwrap();

      channel_guard
        .notify_member_joined(&new_owner_zid, None, true, router.c2s_router().local_domain().clone())
        .await?;
    }

    Ok(())
  }

  /// Leaves all channels that a user is a member of.
  ///
  /// # Arguments
  ///
  /// * `zid` - The user identifier leaving all channels
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn leave_all_channels(&mut self, zid: Zid) -> anyhow::Result<()> {
    let mut mng_guard = self.0.write().await;
    let in_channels_opt = mng_guard.in_channels.remove(&zid.username);
    drop(mng_guard);

    if let Some(in_channels) = in_channels_opt {
      for channel_id in in_channels.iter() {
        self.leave_channel(channel_id.clone(), zid.clone(), None, None, 0).await?;
      }
    }
    Ok(())
  }

  /// Gets the access control list (ACL) for a channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier
  /// * `zid` - The user identifier requesting the ACL
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn get_channel_acl(
    &self,
    channel_id: ChannelId,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Check if the channel exists.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let channel_guard = channel_ref.read().await;

    // Only owner is allowed to get the channel ACL.
    if !channel_guard.is_owner(&zid) {
      return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Obtain the channel's ACL.
    let acl = &channel_guard.acl;

    let allow_join: Vec<StringAtom> = acl.join_acl.allow_list().iter().map(|z| z.into()).collect();
    let allow_publish: Vec<StringAtom> = acl.publish_acl.allow_list().iter().map(|z| z.into()).collect();
    let allow_read: Vec<StringAtom> = acl.read_acl.allow_list().iter().map(|z| z.into()).collect();

    drop(channel_guard);

    // Send response back to the client.
    transmitter.send_message(Message::ChannelAcl(ChannelAclParameters {
      id: correlation_id,
      channel: channel_id.into(),
      allow_publish,
      allow_join,
      allow_read,
    }));

    Ok(())
  }

  /// Sets the access control list (ACL) for a channel.
  ///
  /// # Arguments
  ///
  /// * `acl` - The new ACL configuration
  /// * `channel_id` - The channel identifier
  /// * `zid` - The user identifier setting the ACL
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn set_channel_acl(
    &mut self,
    acl: ChannelAcl,
    channel_id: ChannelId,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Check if the channel exists.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let mut channel_guard = channel_ref.write().await;

    // Only the owner of the channel can change its ACL.
    if !channel_guard.is_owner(&zid) {
      return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Validate the new ACL.
    let channel_max_clients = channel_guard.config.max_clients as usize;

    let is_valid_acl = {
      let is_valid_join_list = acl.join_acl.allow_list().len() <= channel_max_clients;
      let is_valid_publish_list = acl.publish_acl.allow_list().len() <= channel_max_clients;
      let is_valid_read_list = acl.read_acl.allow_list().len() <= channel_max_clients;

      is_valid_join_list && is_valid_publish_list && is_valid_read_list
    };

    if !is_valid_acl {
      return Err(
        zyn_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("ACL allow list exceeds max entries")
          .into(),
      );
    }

    // Update the channel ACL.
    channel_guard.acl = acl.clone();
    drop(channel_guard);

    // Send response back to the client.
    transmitter.send_message(Message::ChannelAcl(ChannelAclParameters {
      id: correlation_id,
      channel: channel_id.into(),
      allow_join: acl.join_acl.allow_list().iter().map(|z| z.into()).collect(),
      allow_publish: acl.publish_acl.allow_list().iter().map(|z| z.into()).collect(),
      allow_read: acl.read_acl.allow_list().iter().map(|z| z.into()).collect(),
    }));

    Ok(())
  }

  /// Gets the configuration for a channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier
  /// * `zid` - The user identifier requesting the configuration
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn get_channel_configuration(
    &self,
    channel_id: ChannelId,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Check if the channel exists.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let channel_guard = channel_ref.read().await;

    // Only members of the channel can get its configuration.
    if !channel_guard.is_member(&zid) {
      return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Obtain the channel configuration.
    let config = channel_guard.config.clone();
    drop(channel_guard);

    // Send response back to the client.
    transmitter.send_message(Message::ChannelConfiguration(ChannelConfigurationParameters {
      id: correlation_id,
      channel: channel_id.into(),
      max_clients: config.max_clients,
      max_payload_size: config.max_payload_size,
    }));

    Ok(())
  }

  /// Sets the configuration for a channel.
  ///
  /// # Arguments
  ///
  /// * `config` - The new channel configuration
  /// * `channel_id` - The channel identifier
  /// * `zid` - The user identifier setting the configuration
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn set_channel_configuration(
    &mut self,
    config: ChannelConfig,
    channel_id: ChannelId,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;

    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }
    // Validate the new channel configuration
    mng_guard.validate_channel_configuration(&config, correlation_id)?;
    drop(mng_guard);

    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;
    let mut channel_guard = channel_ref.write().await;

    // Only the owner of the channel can change its configuration.
    if !channel_guard.is_owner(&zid) {
      return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Merge the current configuration with the new one.
    channel_guard.config = channel_guard.config.merge(&config);

    let new_config = &channel_guard.config;

    // Send response back to the client.
    transmitter.send_message(Message::ChannelConfiguration(ChannelConfigurationParameters {
      id: correlation_id,
      channel: channel_id.into(),
      max_clients: new_config.max_clients,
      max_payload_size: new_config.max_payload_size,
    }));

    Ok(())
  }

  /// Broadcasts a payload to all members of a channel.
  ///
  /// # Arguments
  ///
  /// * `payload` - The payload to broadcast
  /// * `channel_id` - The channel identifier
  /// * `zid` - The user identifier broadcasting the payload
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn broadcast_payload(
    &mut self,
    payload: PoolBuffer,
    channel_id: ChannelId,
    zid: Zid,
    transmitter: Arc<dyn Transmitter>,
    qos: Option<u8>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(zyn_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    // Check if the channel exists.
    let channel_ref = channels
      .ref_from_handler(channel_id.handler as usize)
      .await
      .ok_or(zyn_protocol::Error::new(ChannelNotFound).with_id(correlation_id))?;

    let channel_guard = channel_ref.read().await;

    if !channel_guard.is_member(&zid) {
      return Err(zyn_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }
    // Check if the member is allowed to publish to the channel.
    let acl = &channel_guard.acl;

    if !acl.is_publish_allowed(&zid) {
      return Err(zyn_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Validate channel established payload size limit.
    let payload_length = payload.as_slice().len() as u32;

    if payload_length > channel_guard.config.max_payload_size {
      return Err(
        zyn_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("payload size exceeds channel limit")
          .into(),
      );
    }
    let qos = qos.map(QoS::try_from).transpose()?.unwrap_or(QoS::default());

    if qos == QoS::AckOnReceive {
      transmitter.send_message(Message::BroadcastAck(BroadcastAckParameters { id: correlation_id }));
    }

    // Broadcast the payload to all members of the channel, except
    let msg = Message::Message(MessageParameters {
      from: (&zid).into(),
      channel: (&channel_id).into(),
      length: payload_length,
    });

    let targets = channel_guard.members.iter().filter(|m| acl.is_read_allowed(m));

    router.route_to_many(msg, Some(payload), targets, Some(transmitter.resource())).await?;

    if qos == QoS::AckOnRouted {
      transmitter.send_message(Message::BroadcastAck(BroadcastAckParameters { id: correlation_id }));
    }

    Ok(())
  }
}

/// A channel.
#[derive(Debug, Default)]
pub struct Channel(Option<ChannelInner>);

// ===== impl Channel =====

impl Deref for Channel {
  type Target = ChannelInner;

  fn deref(&self) -> &Self::Target {
    assert!(self.0.is_some(), "ChannelInner is not initialized");
    self.0.as_ref().unwrap()
  }
}

impl DerefMut for Channel {
  fn deref_mut(&mut self) -> &mut Self::Target {
    assert!(self.0.is_some(), "ChannelInner is not initialized");
    self.0.as_mut().unwrap()
  }
}

/// Per-domain ACLs.
#[derive(Clone, Debug, Default)]
struct Acl {
  allow_lists: HashMap<StringAtom, HashSet<StringAtom>>,
}

// ===== impl Acl =====

impl Acl {
  /// Creates a new ACL.
  fn new(allow_list: Vec<Zid>) -> Acl {
    let mut allow_lists: HashMap<StringAtom, HashSet<StringAtom>> = HashMap::new();

    for zid in allow_list {
      let domain = zid.domain.clone();
      let username = zid.username.clone();

      // Get or create the HashSet for this domain
      let domain_users = allow_lists.entry(domain).or_default();

      // Only add non-server users
      if !zid.is_server() {
        domain_users.insert(username);
      }
    }
    Acl { allow_lists }
  }

  /// Checks if a ZID is allowed based on the ACL.
  fn is_allowed(&self, zid: &Zid) -> bool {
    // If ACL map is empty, access is allowed by default
    if self.allow_lists.is_empty() {
      return true;
    }
    let domain = zid.domain.clone();
    let username = zid.username.clone();

    if let Some(allowed_users) = self.allow_lists.get(&domain) {
      if allowed_users.is_empty() {
        // If allowed users is empty, access is allowed by default
        return true;
      }
      return allowed_users.contains(&username);
    }
    false
  }

  /// Returns the ACL allowlist.
  pub fn allow_list(&self) -> Vec<Zid> {
    let mut allow_list: Vec<Zid> = Vec::with_capacity(self.allow_lists.len());

    for (domain, allowed_users) in self.allow_lists.iter() {
      if !allowed_users.is_empty() {
        for username in allowed_users.iter() {
          allow_list.push(Zid::new_unchecked(username.clone(), domain.clone()));
        }
      } else {
        allow_list.push(Zid::new_unchecked(StringAtom::default(), domain.clone()));
      }
    }

    allow_list
  }
}

/// The channel ACL.
#[derive(Clone, Debug, Default)]
pub struct ChannelAcl {
  /// Domain ACL for allowed users to join to the channel.
  join_acl: Acl,

  /// Domain ACL for allowed users to publish to the channel.
  publish_acl: Acl,

  /// Domain ACL for allowed users to read from the channel.
  read_acl: Acl,
}

// ===== impl ChannelAcl =====

impl ChannelAcl {
  /// Creates a new channel ACL.
  pub fn new(allow_join_list: Vec<Zid>, allow_publish_list: Vec<Zid>, allow_read_list: Vec<Zid>) -> ChannelAcl {
    // Return the new ChannelACL instance
    ChannelAcl {
      join_acl: Acl::new(allow_join_list),
      publish_acl: Acl::new(allow_publish_list),
      read_acl: Acl::new(allow_read_list),
    }
  }

  /// Checks if a ZID is allowed to join to the channel.
  pub fn is_join_allowed(&self, zid: &Zid) -> bool {
    self.join_acl.is_allowed(zid)
  }

  /// Checks if a ZID is allowed to publish to the channel.
  pub fn is_publish_allowed(&self, zid: &Zid) -> bool {
    self.publish_acl.is_allowed(zid)
  }

  /// Checks if a ZID is allowed to read from the channel.
  pub fn is_read_allowed(&self, zid: &Zid) -> bool {
    self.read_acl.is_allowed(zid)
  }
}

/// The channel configuration.
#[derive(Clone, Debug, Default)]
pub struct ChannelConfig {
  /// The maximum number of clients allowed in the channel.
  pub max_clients: u32,

  /// The maximum payload size allowed.
  pub max_payload_size: u32,
}

// ===== impl ChannelConfig =====

impl ChannelConfig {
  /// Merges two channel configurations.
  pub fn merge(&self, other: &Self) -> Self {
    let mut config = self.clone();

    if other.max_clients > 0 {
      config.max_clients = other.max_clients;
    }
    if other.max_payload_size > 0 {
      config.max_payload_size = other.max_payload_size;
    }

    config
  }
}

/// The inner state of a channel.
#[derive(Debug)]
pub struct ChannelInner {
  /// The channel handler.
  handler: u32,

  /// The owner of the channel.
  owner: Option<Zid>,

  /// The channel configuration.
  config: ChannelConfig,

  /// The channel ACL.
  acl: ChannelAcl,

  /// The members of the channel (including the owner).
  members: HashSet<Zid>,

  /// The notifier for sending events to channel members.
  notifier: Notifier,
}

// ===== impl ChannelInner =====

impl ChannelInner {
  /// Checks if the channel is empty.
  fn is_empty(&self) -> bool {
    self.members.is_empty()
  }

  /// Checks if the channel is owned by a certain handler.
  fn is_owner(&self, zid: &Zid) -> bool {
    self.owner == Some(zid.clone())
  }

  /// Picks a new owner for the channel.
  fn pick_new_owner(&mut self) -> Option<Zid> {
    if self.is_empty() {
      return None;
    }

    let new_owner_zid = self.members.iter().next().unwrap().clone();

    // Update owner handler.
    self.owner = Some(new_owner_zid.clone());

    Some(new_owner_zid)
  }

  /// Checks if the channel has a certain member.
  fn is_member(&self, zid: &Zid) -> bool {
    self.members.contains(zid)
  }

  /// Returns the number of members in the channel.
  fn member_count(&self) -> usize {
    self.members.len()
  }

  /// Inserts a member into the channel.
  fn insert_member(&mut self, zid: Zid) {
    if self.owner.is_none() {
      self.owner = Some(zid.clone());
    }
    self.members.insert(zid);
  }

  /// Removes a member from the channel.
  fn remove_member(&mut self, zid: &Zid) -> bool {
    if self.owner == Some(zid.clone()) {
      self.owner = None;
    }
    self.members.remove(zid)
  }

  /// Notifies all members that a new member has joined the channel.
  async fn notify_member_joined(
    &self,
    zid: &Zid,
    excluding_resource: Option<Resource>,
    as_owner: bool,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler, local_domain);

    let event =
      Event::new(EventKind::MemberJoined).with_channel(channel_id.into()).with_zid(zid.into()).with_owner(as_owner);

    self.notifier.notify(event, self.members.iter(), excluding_resource).await?;

    Ok(())
  }

  /// Notifies all members that a member has left the channel.
  async fn notify_member_left(
    &self,
    zid: &Zid,
    excluding_resource: Option<Resource>,
    as_owner: bool,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler, local_domain);

    let event =
      Event::new(EventKind::MemberLeft).with_channel(channel_id.into()).with_zid(zid.into()).with_owner(as_owner);

    self.notifier.notify(event, self.members.iter(), excluding_resource).await?;

    Ok(())
  }
}
