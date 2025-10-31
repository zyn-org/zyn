// SPDX-License-Identifier: AGPL-3.0

#![allow(dead_code)]

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tracing::debug;

use zyn_common::client::{self, Handshaker, SessionInfo};
use zyn_common::service::C2sService;
use zyn_protocol::{
  ConnectParameters, DEFAULT_MESSAGE_BUFFER_SIZE, IdentifyParameters, Message, deserialize, serialize,
};
use zyn_util::codec::StreamReader;
use zyn_util::conn::TlsDialer;
use zyn_util::pool::{Pool, PoolBuffer};
use zyn_util::string_atom::StringAtom;

/// Session information returned after successful C2S handshake.
#[derive(Clone, Debug)]
pub struct C2sSessionExtraInfo {
  /// Whether authentication is required by the server.
  pub auth_required: bool,
}

/// Handshaker implementation for C2S (Client-to-Server) connections.
#[derive(Clone)]
struct C2sHandshaker {
  /// The requested heartbeat interval.
  heartbeat_interval: Duration,
}

// === impl C2sHandshaker ===

#[async_trait::async_trait]
impl Handshaker<TlsStream<TcpStream>> for C2sHandshaker {
  type SessionExtraInfo = C2sSessionExtraInfo;

  async fn handshake(&self, stream: &mut TlsStream<TcpStream>) -> anyhow::Result<(SessionInfo, C2sSessionExtraInfo)> {
    let pool = Pool::new(1, DEFAULT_MESSAGE_BUFFER_SIZE);
    let mut message_buff = pool.must_acquire();

    let connect_msg = Message::Connect(ConnectParameters {
      protocol_version: 1,
      heartbeat_interval: self.heartbeat_interval.as_millis() as u32,
    });

    let n = serialize(&connect_msg, message_buff.as_mut_slice())?;
    stream.write_all(&message_buff.as_slice()[..n]).await?;
    stream.flush().await?;

    let mut stream_reader = StreamReader::with_pool_buffer(stream, message_buff);

    let connect_ack_msg = {
      let _ = stream_reader.next().await?;

      match stream_reader.get_line() {
        Some(line_bytes) => deserialize(Cursor::new(line_bytes))?,
        None => return Err(anyhow!("failed to read ConnectAck from server")),
      }
    };

    let (auth_required, session_info) = match connect_ack_msg {
      Message::ConnectAck(params) => {
        let session_info = SessionInfo {
          heartbeat_interval: params.heartbeat_interval,
          max_inflight_requests: params.max_inflight_requests,
          max_message_size: params.max_message_size,
          max_payload_size: params.max_payload_size,
        };

        (params.auth_required, session_info)
      },
      Message::Error(err) => {
        return Err(anyhow!("connection rejected: {:?}", err.reason));
      },
      _ => {
        return Err(anyhow!("unexpected message during handshake: expected ConnectAck"));
      },
    };

    let extra_info = C2sSessionExtraInfo { auth_required };

    Ok((session_info, extra_info))
  }
}

/// C2S (Client-to-Server) client for connecting to the Zyn server.
#[derive(Clone)]
pub struct C2sClient {
  client: Arc<client::Client<TlsStream<TcpStream>, C2sHandshaker, C2sService>>,
}

impl C2sClient {
  /// Creates a new C2S (Client-to-Server) client instance.
  ///
  /// This method initializes a client that connects to the Zyn server, handling
  /// the handshake process. After creating the client, you must
  /// call `identify()` to authenticate with a username.
  ///
  /// # Arguments
  ///
  /// * `address` - The server address to connect to (e.g., "127.0.0.1:5555").
  /// * `config` - The client configuration containing network settings, timeouts,
  ///   heartbeat intervals, and other connection parameters.
  ///
  /// # Returns
  ///
  /// Returns a `C2sClient` instance that can be used to communicate with the Zyn server.
  pub fn new(address: impl Into<String>, config: client::Config) -> anyhow::Result<Self> {
    let dialer = Arc::new(TlsDialer::new(address.into())?);

    let handshaker = C2sHandshaker { heartbeat_interval: config.heartbeat_interval };

    let client = Arc::new(client::Client::new("c2s-client", config, dialer, handshaker)?);

    Ok(Self { client })
  }

  /// Retrieves the session information established during the handshake.
  ///
  /// This method returns the session details negotiated with the server during
  /// the connection handshake. The information includes configuration limits and
  /// parameters that govern the client-server communication.
  ///
  /// # Returns
  ///
  /// Returns session information including:
  /// * `heartbeat_interval` - The negotiated heartbeat interval in milliseconds
  /// * `max_inflight_requests` - Maximum number of concurrent in-flight requests
  /// * `max_message_size` - Maximum size of a message in bytes
  /// * `max_payload_size` - Maximum size of a payload in bytes
  /// * `extra` - Additional C2S-specific session info
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The client is not connected to the server
  /// * The session information is not available
  pub async fn session_info(&self) -> anyhow::Result<(SessionInfo, C2sSessionExtraInfo)> {
    self.client.session_info().await
  }

  /// Identifies the client with the server using a username.
  ///
  /// This method must be called after creating the client and before performing
  /// any other operations.
  ///
  /// # Arguments
  ///
  /// * `username` - The username to identify as.
  ///
  /// # Returns
  ///
  /// Returns the server-assigned ZID if identification succeeds.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The identification is rejected by the server
  /// * The connection is lost during identification
  /// * The server returns an unexpected response
  pub async fn identify(&self, username: StringAtom) -> anyhow::Result<Option<StringAtom>> {
    let message = Message::Identify(IdentifyParameters { username });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::IdentifyAck(params) => {
        debug!(zid = ?params.zid, "identified successfully");
        Ok(Some(params.zid))
      },
      Message::Error(err) => Err(anyhow!("identification failed: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to identify request")),
    }
  }

  /// Joins a channel on the server.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to join.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel was successfully joined.
  ///
  /// # Errors
  ///
  /// Returns an error if the join operation fails.
  pub async fn join_channel(&self, channel: StringAtom) -> anyhow::Result<()> {
    use zyn_protocol::JoinChannelParameters;

    let id = self.client.next_id().await;
    let message = Message::JoinChannel(JoinChannelParameters { id, channel: Some(channel), on_behalf: None });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::JoinChannelAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to join channel: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to join channel request")),
    }
  }

  /// Leaves a channel on the server.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to leave.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel was successfully left.
  ///
  /// # Errors
  ///
  /// Returns an error if the leave operation fails.
  pub async fn leave_channel(&self, channel: StringAtom) -> anyhow::Result<()> {
    use zyn_protocol::LeaveChannelParameters;

    let id = self.client.next_id().await;
    let message = Message::LeaveChannel(LeaveChannelParameters { id, channel, on_behalf: None });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::LeaveChannelAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to leave channel: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to leave channel request")),
    }
  }

  /// Configures channel settings such as maximum clients and payload size.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to configure.
  /// * `max_clients` - The maximum number of clients allowed in the channel.
  /// * `max_payload_size` - The maximum payload size (in bytes) for messages in the channel.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the channel was successfully configured.
  ///
  /// # Errors
  ///
  /// Returns an error if the configuration operation fails or the user is not
  /// authorized to configure the channel.
  pub async fn configure_channel(
    &self,
    channel: StringAtom,
    max_clients: u32,
    max_payload_size: u32,
  ) -> anyhow::Result<()> {
    use zyn_protocol::SetChannelConfigurationParameters;

    let id = self.client.next_id().await;
    let message = Message::SetChannelConfiguration(SetChannelConfigurationParameters {
      id,
      channel,
      max_clients,
      max_payload_size,
    });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::ChannelConfiguration(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to configure channel: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to configure channel request")),
    }
  }

  /// Sets the access control list (ACL) for a channel.
  ///
  /// This method configures who can join, publish to, and read from a channel.
  /// Only the channel owner or authorized users can set channel ACLs.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to configure.
  /// * `allow_join` - List of user IDs or domains allowed to join the channel.
  /// * `allow_publish` - List of user IDs or domains allowed to publish to the channel.
  /// * `allow_read` - List of user IDs or domains allowed to read from the channel.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the ACL was successfully set.
  ///
  /// # Errors
  ///
  /// Returns an error if the ACL operation fails or the user is not
  /// authorized to modify the channel's ACL.
  pub async fn set_channel_acl(
    &self,
    channel: StringAtom,
    allow_join: Vec<StringAtom>,
    allow_publish: Vec<StringAtom>,
    allow_read: Vec<StringAtom>,
  ) -> anyhow::Result<()> {
    use zyn_protocol::SetChannelAclParameters;

    let id = self.client.next_id().await;
    let message =
      Message::SetChannelAcl(SetChannelAclParameters { id, channel, allow_join, allow_publish, allow_read });

    let handle = self.client.send_message(message, None).await?;
    let (response, _) = handle.await??;

    match response {
      Message::ChannelAcl(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to set channel ACL: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to set channel ACL request")),
    }
  }

  /// Broadcasts a message to a channel.
  ///
  /// # Arguments
  ///
  /// * `channel` - The name of the channel to broadcast to.
  /// * `payload` - The payload data to broadcast.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the broadcast was successful.
  ///
  /// # Errors
  ///
  /// Returns an error if the broadcast operation fails.
  pub async fn broadcast(&self, channel: StringAtom, payload: PoolBuffer) -> anyhow::Result<()> {
    use zyn_protocol::BroadcastParameters;

    let id = self.client.next_id().await;
    let length = payload.len() as u32;
    let message = Message::Broadcast(BroadcastParameters { id, channel, length });

    let handle = self.client.send_message(message, Some(payload)).await?;
    let (response, _) = handle.await??;

    match response {
      Message::BroadcastAck(_) => Ok(()),
      Message::Error(err) => Err(anyhow!("failed to broadcast: {:?}", err.reason)),
      _ => Err(anyhow!("unexpected response to broadcast request")),
    }
  }

  /// Shuts down the client and closes all connections.
  ///
  /// This method gracefully shuts down the client, closing all active connections
  /// and cleaning up resources.
  ///
  /// # Errors
  ///
  /// Returns an error if the shutdown process fails.
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    self.client.shutdown().await
  }
}
