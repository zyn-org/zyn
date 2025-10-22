// SPDX-License-Identifier: AGPL-3.0

use std::net::SocketAddr;
use std::sync::Arc;

use crate::c2s::config::ListenerConfig;
use crate::util::tls::{create_tls_config, generate_self_signed_cert};
use crate::{c2s, util};

use anyhow::anyhow;
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};
use zyn_common::service::{C2sService, Service};

const LOCALHOST_DOMAIN: &str = "localhost";

/// The C2S connection manager.
type C2sConnManager =
  zyn_common::conn::ConnManager<c2s::conn::C2sDispatcher, c2s::conn::C2sDispatcherFactory, C2sService>;

/// A TLS-enabled TCP listener for client-to-server (C2S) connections.
///
/// The `Listener` manages incoming client connections, handling TLS negotiation
/// and connection management. It supports both self-signed certificates for
/// localhost development and proper TLS certificates for production use.
///
/// The listener works in conjunction with a connection manager to handle
/// individual client connections after they are established.
pub struct C2sListener {
  /// The configuration for the C2S listener.
  config: ListenerConfig,

  /// The connection manager.
  conn_mng: C2sConnManager,

  /// The channel to signal the listener to stop.
  done_tx: Option<mpsc::Sender<()>>,

  /// The local address of the listener.
  local_address: Option<SocketAddr>,
}

// ===== impl C2sListener =====

impl C2sListener {
  /// Creates a new C2S listener with the given configuration and connection manager.
  ///
  /// # Arguments
  ///
  /// * `config` - The configuration for the C2S listener
  /// * `conn_mng` - The connection manager that will handle established connections
  ///
  /// # Returns
  ///
  /// Returns a new `C2sListener` instance that is ready to be bootstrapped.
  pub fn new(config: ListenerConfig, conn_mng: C2sConnManager) -> Self {
    Self { config, conn_mng, done_tx: None, local_address: None }
  }

  /// Bootstraps the listener, starting to accept incoming connections.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the listener was successfully started.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * The connection manager fails to bootstrap
  /// * TLS configuration fails (invalid certificates or keys)
  /// * Unable to bind to the configured address and port
  pub async fn bootstrap(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_none());

    // Bootstrap the connection manager.
    self.conn_mng.bootstrap().await?;

    let (done_tx, mut done_rx) = mpsc::channel(1);
    self.done_tx = Some(done_tx);

    let conn_mng = self.conn_mng.clone();

    let tls_config = self.load_tls_config()?;
    let acceptor = TlsAcceptor::from(tls_config);

    let mut listener = TcpListener::bind(self.get_address()).await?;

    self.local_address = Some(listener.local_addr()?);

    let (running_tx, running_rx) = oneshot::channel();

    tokio::spawn(async move {
      let _ = running_tx.send(());

      loop {
        tokio::select! {
          _ = Self::accept_connection(&mut listener, acceptor.clone(), conn_mng.clone()) => {}
          _ = done_rx.recv() => {
            break;
          }
        }
      }
    });

    // Wait for the listener to start.
    running_rx.await?;

    info!(
      address = self.get_address(),
      domain = self.config.domain,
      service_type = C2sService::NAME,
      "accepting socket connections"
    );
    Ok(())
  }

  /// Gracefully shuts down the listener.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` if the shutdown was successful.
  ///
  /// # Errors
  ///
  /// Returns an error if:
  /// * Unable to send the shutdown signal
  /// * The connection manager fails to shut down properly
  pub async fn shutdown(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_some());

    self.done_tx.take().unwrap().send(()).await?;

    info!(
      address = self.get_address(),
      domain = self.config.domain,
      service_type = C2sService::NAME,
      "stopped accepting socket connections"
    );

    // Wait for the connection manager to stop.
    self.conn_mng.shutdown().await?;

    Ok(())
  }

  /// Returns the local address that the listener is bound to.
  ///
  /// This method will return `None` if called before `bootstrap()` or if
  /// the listener failed to bind to an address.
  ///
  /// # Returns
  ///
  /// Returns the socket address that the listener is bound to, if available.
  pub fn local_address(&self) -> Option<SocketAddr> {
    self.local_address
  }

  fn load_tls_config(&self) -> anyhow::Result<Arc<ServerConfig>> {
    let is_localhost = self.config.domain == LOCALHOST_DOMAIN;

    if self.config.cert_file.is_empty() || self.config.key_file.is_empty() {
      if !is_localhost {
        return Err(anyhow!("certificate and key files must be specified for non-localhost domains"));
      }
      info!(domain = "localhost", service_type = C2sService::NAME, "using self-signed certificate");

      let (certs, key) = generate_self_signed_cert(vec![LOCALHOST_DOMAIN.to_string()])?;

      return create_tls_config(certs, key);
    }
    info!(
      domain = self.config.domain,
      cert_file = self.config.cert_file,
      key_file = self.config.key_file,
      service_type = C2sService::NAME,
      "loading certificate and key files"
    );

    let certs = util::tls::load_certs(&self.config.cert_file)?;
    let key = util::tls::load_private_key(&self.config.key_file)?;

    create_tls_config(certs, key)
  }

  fn get_address(&self) -> String {
    format!("{}:{}", self.config.bind_address, self.config.port)
  }

  async fn accept_connection(
    listener: &mut TcpListener,
    acceptor: TlsAcceptor,
    conn_mng: C2sConnManager,
  ) -> anyhow::Result<()> {
    let (tcp_stream, addr) = listener.accept().await?;

    debug!(local_address = format!("{:?}", addr), service_type = C2sService::NAME, "accepted connection");

    tokio::spawn(async move {
      match handle_connection(tcp_stream, acceptor, conn_mng).await {
        Ok(()) => {},
        Err(err) => {
          warn!(error = ?err, service_type = C2sService::NAME, "failed to handle connection");
        },
      }
    });

    Ok(())
  }
}

async fn handle_connection(
  tcp_stream: tokio::net::TcpStream,
  acceptor: TlsAcceptor,
  conn_mng: C2sConnManager,
) -> anyhow::Result<()> {
  // Set the TCP_NODELAY option to disable Nagle's algorithm.
  tcp_stream.set_nodelay(true)?;

  // Negotiate the TLS connection.
  let tls_stream = acceptor.accept(tcp_stream).await?;

  // Run the connection manager.
  conn_mng.run(tls_stream).await?;

  Ok(())
}
