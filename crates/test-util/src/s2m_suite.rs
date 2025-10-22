// SPDX-License-Identifier: AGPL-3.0

use std::sync::Arc;

use tokio::net::TcpStream;

use zyn_modulator::{Modulator, S2mListener};
use zyn_protocol::{Message, S2mConnectParameters};

use crate::TestConn;

/// A test suite for the S2M server.
pub struct S2mSuite<M: Modulator> {
  /// The server configuration.
  config: Arc<zyn_modulator::S2mServerConfig>,

  /// The modulator server listener.
  ln: S2mListener<M>,
}

// ===== impl S2mSuite =====

impl<M: Modulator> S2mSuite<M> {
  /// Creates a new S2mSuite with the given configuration and modulator.
  pub fn with_config(config: zyn_modulator::S2mServerConfig, modulator: M) -> Self {
    let arc_config = Arc::new(config);

    let dispatcher_factory =
      zyn_modulator::conn::S2mDispatcherFactory::<M>::new(arc_config.clone(), Arc::new(modulator));

    let server_config = arc_config.server.clone();

    let conn_mng = zyn_modulator::conn::S2mConnManager::<M>::new(&server_config, dispatcher_factory);

    let ln = S2mListener::new(server_config.listener.clone(), conn_mng.clone());

    Self { config: arc_config, ln }
  }

  /// Returns the server configuration.
  pub fn config(&self) -> Arc<zyn_modulator::S2mServerConfig> {
    self.config.clone()
  }

  /// Sets up the test suite by bootstrapping the listener.
  pub async fn setup(&mut self) -> anyhow::Result<()> {
    self.ln.bootstrap().await?;
    Ok(())
  }

  /// Tears down the test suite by shutting down the listener.
  pub async fn teardown(&mut self) -> anyhow::Result<()> {
    self.ln.shutdown().await?;
    Ok(())
  }

  /// Creates a new socket connection to the server without authentication.
  pub async fn socket_connect(&self) -> anyhow::Result<TestConn<TcpStream>> {
    let addr = self.ln.local_address().expect("local address not set");

    let tcp_stream = TcpStream::connect(&addr).await?;

    let max_message_size = self.config().server.limits.max_message_size as usize;

    let socket = TestConn::new(tcp_stream, max_message_size);

    Ok(socket)
  }

  /// Creates a new authenticated connection to the server.
  ///
  /// # Arguments
  ///
  /// * `secret` - The secret to use for authentication
  pub async fn connect(&mut self, secret: &str) -> anyhow::Result<TestConn<TcpStream>> {
    let mut socket = self.socket_connect().await?;

    socket
      .write_message(Message::S2mConnect(S2mConnectParameters {
        protocol_version: 1,
        secret: Some(secret.into()),
        heartbeat_interval: 0,
      }))
      .await?;

    let client_connected_msg = socket.read_message().await?;
    assert!(matches!(client_connected_msg, Message::S2mConnectAck { .. }));

    Ok(socket)
  }

  /// Creates a new authenticated connection to the server with a custom heartbeat interval.
  ///
  /// # Arguments
  ///
  /// * `secret` - The secret to use for authentication
  /// * `heartbeat_interval` - The heartbeat interval in milliseconds
  pub async fn connect_with_heartbeat(
    &mut self,
    secret: &str,
    heartbeat_interval: u32,
  ) -> anyhow::Result<TestConn<TcpStream>> {
    let mut socket = self.socket_connect().await?;

    socket
      .write_message(Message::S2mConnect(S2mConnectParameters {
        protocol_version: 1,
        secret: Some(secret.into()),
        heartbeat_interval,
      }))
      .await?;

    let client_connected_msg = socket.read_message().await?;
    assert!(matches!(client_connected_msg, Message::S2mConnectAck { .. }));

    Ok(socket)
  }
}

/// Creates a default S2M configuration with a shared secret for testing.
///
/// # Arguments
///
/// * `secret` - The shared secret to use for authentication
pub fn default_s2m_config_with_secret(secret: &str) -> zyn_modulator::S2mServerConfig {
  let s2m_server_config = zyn_modulator::ServerConfig {
    listener: zyn_modulator::ListenerConfig {
      network: zyn_modulator::TCP_NETWORK.to_string(),
      bind_address: "127.0.0.1:0".to_string(), // use a random port
      ..Default::default()
    },
    limits: zyn_modulator::Limits {
      max_connections: 10,
      max_message_size: 256 * 1024,
      payload_pool_memory_budget: 512 * 1024,
      ..Default::default()
    },
    shared_secret: secret.to_string(),
    ..Default::default()
  };

  zyn_modulator::S2mServerConfig { server: s2m_server_config, m2s_client: zyn_modulator::M2sClientConfig::default() }
}
