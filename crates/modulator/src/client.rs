// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;
use std::time::Duration;

use narwhal_common::client::SessionInfo;
use narwhal_common::service::{M2sService, S2mService};
use narwhal_protocol::{DEFAULT_MESSAGE_BUFFER_SIZE, M2sModDirectParameters, Message, S2mAuthParameters, request};
use narwhal_protocol::{
  M2sConnectParameters, S2mConnectParameters, S2mForwardBroadcastPayloadParameters, S2mForwardEventParameters,
  S2mModDirectParameters,
};
use narwhal_util::conn::{Dialer, Stream, TcpDialer, UnixDialer};

use narwhal_util::pool::{Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

use crate::config::{M2sClientConfig, S2mClientConfig, TCP_NETWORK, UNIX_NETWORK};
use crate::modulator::{
  AuthRequest, AuthResponse, AuthResult, ForwardBroadcastPayloadRequest, ForwardBroadcastPayloadResponse,
  ForwardBroadcastPayloadResult, ForwardEventRequest, ForwardEventResponse, Operation, Operations,
  ReceivePrivatePayloadRequest, ReceivePrivatePayloadResponse, SendPrivatePayloadRequest, SendPrivatePayloadResponse,
  SendPrivatePayloadResult,
};

#[derive(Debug, Clone)]
pub struct M2sSessionExtraInfo {}

#[derive(Clone, Debug, Default)]
struct M2sHandshaker {
  shared_secret: Option<StringAtom>,
  heartbeat_interval: Duration,
}

// === impl M2sHandshaker ===

#[async_trait::async_trait]
impl narwhal_common::client::Handshaker<Stream> for M2sHandshaker {
  type SessionExtraInfo = M2sSessionExtraInfo;

  async fn handshake(&self, stream: &mut Stream) -> anyhow::Result<(SessionInfo, M2sSessionExtraInfo)> {
    let pool = Pool::new(1, DEFAULT_MESSAGE_BUFFER_SIZE);
    let message_buff = pool.acquire_buffer().await;

    let connect_msg = Message::M2sConnect(M2sConnectParameters {
      protocol_version: 1,
      secret: self.shared_secret.clone(),
      heartbeat_interval: self.heartbeat_interval.as_millis() as u32,
    });

    let reply_msg = request(connect_msg, stream, message_buff).await?;

    match reply_msg {
      Message::M2sConnectAck(params) => {
        let session_info = SessionInfo {
          heartbeat_interval: params.heartbeat_interval,
          max_inflight_requests: params.max_inflight_requests,
          max_message_size: params.max_message_size,
          max_payload_size: params.max_payload_size,
        };

        Ok((session_info, M2sSessionExtraInfo {}))
      },
      Message::Error(e) => anyhow::bail!("an error occurred during handshake: {}", e.reason),
      _ => anyhow::bail!("unexpected message type during handshake"),
    }
  }
}

#[derive(Clone, Debug)]
pub struct M2sClient {
  client: narwhal_common::client::Client<Stream, M2sHandshaker, M2sService>,
}

// === impl M2sClient ===

impl M2sClient {
  /// Creates a new M2S (Modulator-to-Server) client instance.
  ///
  /// This method initializes a client that connects modulators to servers, handling
  /// the handshake process and establishing communication channels.
  ///
  /// # Arguments
  ///
  /// * `config` - The client configuration containing network settings, timeouts,
  ///   heartbeat intervals, and other connection parameters.
  ///
  /// # Returns
  ///
  /// Returns `Ok(M2sClient)` if the client is successfully created, or an error if:
  /// - The configuration validation fails
  /// - The underlying client creation fails
  pub fn new(config: M2sClientConfig) -> anyhow::Result<Self> {
    // Validate the configuration
    config.validate()?;

    // Create the proper dialer based on the network type
    let dialer: Arc<dyn Dialer<Stream = Stream>> = match config.network.as_str() {
      TCP_NETWORK => Arc::new(TcpDialer::new(config.address.clone())),
      UNIX_NETWORK => Arc::new(UnixDialer::new(config.socket_path.clone())),
      _ => unreachable!("network type already validated"),
    };

    // Create a M2S handshaker
    let shared_secret: Option<StringAtom> =
      { if !config.shared_secret.is_empty() { Some(config.shared_secret.as_str().into()) } else { None } };

    let handshaker = M2sHandshaker { shared_secret, heartbeat_interval: config.heartbeat_interval };

    let client = narwhal_common::client::Client::new(
      "m2s:client",
      narwhal_common::client::Config {
        max_idle_connections: config.max_idle_connections,
        heartbeat_interval: config.heartbeat_interval,
        connect_timeout: config.connect_timeout,
        timeout: config.timeout,
        payload_read_timeout: config.payload_read_timeout,
        backoff_initial_delay: config.backoff_initial_delay,
        backoff_max_delay: config.backoff_max_delay,
        backoff_max_retries: config.backoff_max_retries,
      },
      dialer,
      handshaker,
    )?;

    Ok(Self { client })
  }

  /// Retrieves the current session information from the connected server.
  ///
  /// This method returns details about the active session including connection
  /// metadata and session state information.
  ///
  /// # Returns
  ///
  /// Returns `Ok(SessionInfo)` containing the session details if successful,
  /// or an error if:
  /// - The client is not connected
  /// - The server is unreachable
  /// - The session has been terminated
  pub async fn session_info(&self) -> anyhow::Result<SessionInfo> {
    let (session_info, _) = self.client.session_info().await?;
    Ok(session_info)
  }

  /// Routes a private payload directly to specified target clients.
  ///
  /// This method sends a direct message containing the provided payload to one or more
  /// target clients identified by their usernames.
  ///
  /// # Arguments
  ///
  /// * `payload` - The buffer containing the message data to be sent to the targets
  /// * `targets` - A vector of target client identifiers that should receive the payload
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the payload was successfully routed and acknowledged by the server,
  /// or an error if:
  /// - The client is not connected
  /// - The server fails to acknowledge the message
  /// - An unexpected message type is received instead of the expected acknowledgment
  /// - The network operation fails
  pub async fn route_private_payload(&self, payload: PoolBuffer, targets: Vec<StringAtom>) -> anyhow::Result<()> {
    let id = self.client.next_id().await;

    let params = M2sModDirectParameters { id, targets, length: payload.len() as u32 };

    let handle = self.client.send_message(Message::M2sModDirect(params), Some(payload)).await?;

    match handle.await? {
      Ok((msg, _)) => match msg {
        Message::M2sModDirectAck(_) => Ok(()),
        _ => Err(anyhow::anyhow!("unexpected message type during private payload routing")),
      },
      Err(e) => anyhow::bail!(e),
    }
  }

  /// Gracefully shuts down the client connection.
  ///
  /// This method cleanly terminates the connection to the server, ensuring
  /// all pending operations are completed and resources are properly released.
  /// After calling this method, the client instance should not be used for
  /// further operations.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the shutdown is successful, or an error if:
  /// - The shutdown process encounters an error
  /// - Resources cannot be properly released
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    self.client.shutdown().await
  }
}

#[derive(Debug, Clone)]
pub struct S2mSessionExtraInfo {
  /// The modulator's protocol identifier
  pub protocol_name: StringAtom,

  /// The operations supported by the modulator server
  pub operations: Operations,
}

#[derive(Clone, Debug, Default)]
struct S2mHandshaker {
  shared_secret: Option<StringAtom>,
  heartbeat_interval: Duration,
}

// === impl S2mHandshaker ===

#[async_trait::async_trait]
impl narwhal_common::client::Handshaker<Stream> for S2mHandshaker {
  type SessionExtraInfo = S2mSessionExtraInfo;

  async fn handshake(&self, stream: &mut Stream) -> anyhow::Result<(SessionInfo, S2mSessionExtraInfo)> {
    let pool = Pool::new(1, DEFAULT_MESSAGE_BUFFER_SIZE);
    let message_buff = pool.acquire_buffer().await;

    let connect_msg = Message::S2mConnect(S2mConnectParameters {
      protocol_version: 1,
      secret: self.shared_secret.clone(),
      heartbeat_interval: self.heartbeat_interval.as_millis() as u32,
    });

    let reply_msg = request(connect_msg, stream, message_buff).await?;

    match reply_msg {
      Message::S2mConnectAck(params) => {
        let session_info = SessionInfo {
          heartbeat_interval: params.heartbeat_interval,
          max_inflight_requests: params.max_inflight_requests,
          max_message_size: params.max_message_size,
          max_payload_size: params.max_payload_size,
        };

        let session_extra_info =
          S2mSessionExtraInfo { protocol_name: params.application_protocol, operations: params.operations.into() };

        Ok((session_info, session_extra_info))
      },
      Message::Error(e) => anyhow::bail!("an error occurred during handshake: {}", e.reason),
      _ => anyhow::bail!("unexpected message type during handshake"),
    }
  }
}

#[derive(Clone, Debug)]
pub struct S2mClient {
  client: narwhal_common::client::Client<Stream, S2mHandshaker, S2mService>,
}

// === impl S2mClient ===

impl S2mClient {
  /// Creates a new S2M (Server-to-Modulator) client with the provided configuration.
  ///
  /// This method initializes a client that enables servers to connect to modulators,
  /// facilitating bidirectional communication through the S2M protocol. The client
  /// handles authentication, payload forwarding, and event management.
  ///
  /// # Arguments
  ///
  /// * `config` - The client configuration containing:
  ///   - Network settings (TCP or Unix socket)
  ///   - Connection timeouts and retry policies
  ///   - Heartbeat intervals for connection health monitoring
  ///   - Connection pool settings for managing multiple connections
  ///   - Shared secret for authentication (optional)
  ///
  /// # Returns
  ///
  /// Returns `Ok(S2mClient)` if the client is successfully created, or an error if:
  /// - The configuration validation fails
  /// - Required parameters are missing or invalid
  /// - The underlying client initialization fails
  pub fn new(config: S2mClientConfig) -> anyhow::Result<Self> {
    // Validate the configuration
    config.validate()?;

    // Create the proper dialer based on the network type
    let dialer: Arc<dyn Dialer<Stream = Stream>> = match config.network.as_str() {
      TCP_NETWORK => Arc::new(TcpDialer::new(config.address.clone())),
      UNIX_NETWORK => Arc::new(UnixDialer::new(config.socket_path.clone())),
      _ => unreachable!("network type already validated"),
    };

    // Create a S2M handshaker
    let shared_secret: Option<StringAtom> =
      { if !config.shared_secret.is_empty() { Some(config.shared_secret.as_str().into()) } else { None } };

    let handshaker = S2mHandshaker { shared_secret, heartbeat_interval: config.heartbeat_interval };

    let client = narwhal_common::client::Client::new(
      "s2m:client",
      narwhal_common::client::Config {
        max_idle_connections: config.max_idle_connections,
        heartbeat_interval: config.heartbeat_interval,
        connect_timeout: config.connect_timeout,
        timeout: config.timeout,
        payload_read_timeout: config.payload_read_timeout,
        backoff_initial_delay: config.backoff_initial_delay,
        backoff_max_delay: config.backoff_max_delay,
        backoff_max_retries: config.backoff_max_retries,
      },
      dialer,
      handshaker,
    )?;

    Ok(Self { client })
  }

  /// Retrieves the current session information negotiated with the modulator server.
  ///
  /// This method provides access to the core session parameters that were established
  /// during the handshake process with the modulator server. The session information
  /// includes important operational limits and configuration values that govern
  /// the client-server communication.
  ///
  /// # Returns
  ///
  /// Returns `Ok(SessionInfo)` containing:
  /// - `heartbeat_interval`: The interval for keep-alive heartbeats
  /// - `max_inflight_requests`: Maximum concurrent requests allowed
  /// - `max_message_size`: Maximum size of protocol messages
  /// - `max_payload_size`: Maximum size of forwarded payloads
  ///
  /// Returns an error if:
  /// - The client is not connected
  /// - The session has been terminated
  /// - Network communication fails
  pub async fn session_info(&self) -> anyhow::Result<SessionInfo> {
    let (session_info, _) = self.client.session_info().await?;
    Ok(session_info)
  }

  /// Gracefully shuts down the S2M client and releases all resources.
  ///
  /// This method performs a clean shutdown by:
  /// - Closing all active connections in the connection pool
  /// - Stopping background heartbeat tasks
  /// - Flushing any pending messages
  /// - Releasing all allocated resources
  ///
  /// After calling this method, the client instance becomes invalid and should
  /// not be used for any further operations.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the shutdown completes successfully, or an error if:
  /// - Active operations cannot be safely terminated
  /// - Resources cannot be properly released
  /// - The shutdown process is interrupted
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    self.client.shutdown().await
  }
}

#[async_trait::async_trait]
impl crate::Modulator for S2mClient {
  /// Returns the modulator's protocol identifier.
  ///
  /// # Returns
  ///
  /// A [`StringAtom`] containing the modulator's protocol identifier.
  async fn protocol_name(&self) -> anyhow::Result<StringAtom> {
    let (_, extra_session_info) = self.client.session_info().await?;
    Ok(extra_session_info.protocol_name)
  }

  /// Returns modulator's supported operations.
  ///
  /// # Returns
  ///
  /// An [`Operations`] value containing the supported operations.
  async fn operations(&self) -> anyhow::Result<Operations> {
    let (_, extra_session_info) = self.client.session_info().await?;
    Ok(extra_session_info.operations)
  }

  /// Authenticates with the modulator server using the provided token.
  ///
  /// This method sends an authentication request to the modulator server and waits
  /// for the response. The authentication process may result in success, failure,
  /// or a continuation request for multi-step authentication.
  ///
  /// # Arguments
  ///
  /// * `token` - The authentication token to send to the server. This could be
  ///   a password, API key, or other credential depending on the server's
  ///   authentication mechanism.
  ///
  /// # Returns
  ///
  /// Returns a `Result` containing an `AuthResponse` with a result that can be:
  /// - `AuthResult::Success { username }` - Authentication succeeded with the given username
  /// - `AuthResult::Continue { challenge }` - Server requires additional authentication steps
  /// - `AuthResult::Failure` - Authentication failed
  async fn authenticate(&self, request: AuthRequest) -> anyhow::Result<AuthResponse> {
    if !self.operations().await?.contains(Operation::Auth) {
      return Err(anyhow::anyhow!("authentication operation not supported"));
    }

    let correlation_id = self.client.next_id().await;

    let handle = self
      .client
      .send_message(Message::S2mAuth(S2mAuthParameters { id: correlation_id, token: request.token }), None)
      .await?;

    match handle.await? {
      Ok((msg, _)) => match msg {
        Message::S2mAuthAck(params) => {
          if params.succeeded {
            Ok(AuthResponse { result: AuthResult::Success { username: params.username.unwrap() } })
          } else {
            match params.challenge {
              Some(challenge) => Ok(AuthResponse { result: AuthResult::Continue { challenge } }),
              None => Ok(AuthResponse { result: AuthResult::Failure }),
            }
          }
        },
        Message::Error(e) => Err(anyhow::anyhow!("authentication error: {}", e.reason)),
        _ => Err(anyhow::anyhow!("unexpected message type during authentication")),
      },
      Err(e) => Err(anyhow::anyhow!("authentication failed: {}", e)),
    }
  }

  /// Forwards a message payload to the modulator server for validation and processing.
  ///
  /// This method sends a message payload along with its metadata to the modulator server
  /// for validation. The server will check whether the payload meets the required criteria
  /// and may optionally return a modified version of the payload if transformations are needed.
  /// The operation must be supported by the server (checked via `ForwardMessagePayload` operation flag).
  ///
  /// # Arguments
  ///
  /// * `request` - A [`ForwardBroadcastPayloadRequest`] containing the payload to validate,
  ///   the sender's ID, and the target channel handler
  ///
  /// # Returns
  ///
  /// Returns a `Result` containing a [`ForwardBroadcastPayloadResponse`] which includes
  /// the validation result
  async fn forward_broadcast_payload(
    &self,
    request: ForwardBroadcastPayloadRequest,
  ) -> anyhow::Result<ForwardBroadcastPayloadResponse> {
    if !self.operations().await?.contains(Operation::ForwardBroadcastPayload) {
      return Err(anyhow::anyhow!("payload forwarding operation not supported"));
    }

    let params = S2mForwardBroadcastPayloadParameters {
      id: self.client.next_id().await,
      from: request.from.into(),
      channel: request.channel_handler,
      length: request.payload.len() as u32,
    };

    let handle = self.client.send_message(Message::S2mForwardBroadcastPayload(params), Some(request.payload)).await?;

    match handle.await? {
      Ok((msg, payload_opt)) => match msg {
        Message::S2mForwardBroadcastPayloadAck(params) => {
          let result = if params.valid {
            match payload_opt {
              Some(altered) => ForwardBroadcastPayloadResult::ValidWithAlteration { altered_payload: altered },
              None => ForwardBroadcastPayloadResult::Valid,
            }
          } else {
            ForwardBroadcastPayloadResult::Invalid
          };
          Ok(ForwardBroadcastPayloadResponse { result })
        },
        _ => Err(anyhow::anyhow!("unexpected message type during payload forwarding")),
      },
      Err(e) => anyhow::bail!(e),
    }
  }

  /// Forwards an event to the modulator server for processing.
  ///
  /// This method sends an event notification to the modulator server, which can
  /// be used for logging, monitoring, or triggering server-side actions based on
  /// the event type and associated metadata.
  ///
  /// # Arguments
  ///
  /// * `request` - A `ForwardEventRequest` containing the event information to be forwarded,
  ///   including the event kind, channel, zid and owner.
  ///
  /// # Returns
  ///
  /// Returns a `Result<()>` indicating success or failure of the event forwarding
  /// operation. The method completes successfully when the server acknowledges
  /// receipt of the event.
  async fn forward_event(&self, request: ForwardEventRequest) -> anyhow::Result<ForwardEventResponse> {
    if !self.operations().await?.contains(Operation::ForwardEvent) {
      return Err(anyhow::anyhow!("event forwarding operation not supported"));
    }

    let params = S2mForwardEventParameters {
      id: self.client.next_id().await,
      kind: request.event.kind.into(),
      channel: request.event.channel,
      zid: request.event.zid,
      owner: request.event.owner,
    };

    let handle = self.client.send_message(Message::S2mForwardEvent(params), None).await?;

    match handle.await? {
      Ok((msg, _)) => match msg {
        Message::S2mForwardEventAck { .. } => Ok(ForwardEventResponse {}),
        _ => Err(anyhow::anyhow!("unexpected message type during event forwarding")),
      },
      Err(e) => Err(anyhow::anyhow!("forwarding event failed: {}", e)),
    }
  }

  /// Sends a private payload directly from the client to the modulator.
  ///
  /// This method sends a private payload that bypasses the host and goes directly to the
  /// modulator, establishing a direct client-to-modulator communication channel. This is
  /// useful for implementing custom protocols, authentication flows, or other specialized
  /// interactions that don't require host intervention.
  ///
  /// # Arguments
  ///
  /// * `request` - A [`SendPrivatePayloadRequest`] containing the payload and sender information
  ///
  /// # Returns
  ///
  /// Returns a `Result` containing a [`SendPrivatePayloadResponse`] which indicates
  /// whether the private payload was successfully processed by the modulator
  async fn send_private_payload(
    &self,
    request: SendPrivatePayloadRequest,
  ) -> anyhow::Result<SendPrivatePayloadResponse> {
    if !self.operations().await?.contains(Operation::SendPrivatePayload) {
      return Err(anyhow::anyhow!("send private payload operation not supported"));
    }

    let correlation_id = self.client.next_id().await;

    let handle = self
      .client
      .send_message(
        Message::S2mModDirect(S2mModDirectParameters {
          id: correlation_id,
          from: request.from,
          length: request.payload.len() as u32,
        }),
        Some(request.payload),
      )
      .await?;

    match handle.await? {
      Ok((msg, _)) => match msg {
        Message::S2mModDirectAck(params) => Ok(SendPrivatePayloadResponse {
          result: if params.valid { SendPrivatePayloadResult::Valid } else { SendPrivatePayloadResult::Invalid },
        }),
        _ => Err(anyhow::anyhow!("unexpected message type during client direct operation")),
      },
      Err(e) => anyhow::bail!(e),
    }
  }

  /// Not supported by S2mClient.
  ///
  /// This method is part of the [`Modulator`] trait but is not implemented for S2mClient.
  /// S2mClient does not support receiving private payloads directly from the modulator.
  /// This functionality should be handled via the M2S service instead.
  ///
  /// # Arguments
  ///
  /// * `request` - A [`ReceivePrivatePayloadRequest`] (unused for S2mClient)
  ///
  /// # Panics
  ///
  /// Always panics with `unreachable!()` since this operation should never be called
  /// on S2mClient.
  async fn receive_private_payload(
    &self,
    _request: ReceivePrivatePayloadRequest,
  ) -> anyhow::Result<ReceivePrivatePayloadResponse> {
    unreachable!("receive_private_payload is not supported by S2mClient");
  }
}
