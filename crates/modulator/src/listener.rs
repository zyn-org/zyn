// SPDX-License-Identifier: AGPL-3.0

use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use anyhow::anyhow;
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};
use zyn_common::service::{M2sService, S2mService, Service};
use zyn_util::conn::Stream;

use crate::config::*;
use crate::conn::{M2sConnManager, S2mConnManager};

/// Type alias for S2M listener.
pub type S2mListener<M> = Listener<S2mConnManager<M>, S2mService>;

/// Type alias for M2S listener.
pub type M2sListener = Listener<M2sConnManager, M2sService>;

/// Modulator server listener.
pub struct Listener<CM, ST> {
  /// The listener configuration.
  config: ListenerConfig,

  /// The connection manager.
  conn_mng: CM,

  /// The channel to signal the listener to stop.
  done_tx: Option<mpsc::Sender<()>>,

  /// The local address of the listener (only for TCP).
  local_address: Option<SocketAddr>,

  /// Phantom data for the service type
  _service_type: std::marker::PhantomData<ST>,
}

// ===== impl Listener =====

impl<CM, ST> Listener<CM, ST>
where
  CM: Clone + Send + Sync + 'static,
  ST: Service,
{
  /// Creates a new listener.
  pub fn new(config: ListenerConfig, conn_mng: CM) -> Self {
    Listener { config, conn_mng, done_tx: None, local_address: None, _service_type: std::marker::PhantomData }
  }
}

impl<D, DF, ST> Listener<zyn_common::conn::ConnManager<D, DF, ST>, ST>
where
  D: zyn_common::conn::Dispatcher,
  DF: zyn_common::conn::DispatcherFactory<D>,
  ST: Service,
{
  /// Starts the listener.
  pub async fn bootstrap(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_none());

    let (done_tx, done_rx) = mpsc::channel(1);
    self.done_tx = Some(done_tx);

    let conn_mng = self.conn_mng.clone();
    conn_mng.bootstrap().await?;

    match self.config.network.as_str() {
      TCP_NETWORK => {
        self.listen_tcp(conn_mng, done_rx).await?;
      },
      UNIX_NETWORK => {
        self.listen_unix(conn_mng, done_rx).await?;
      },
      _ => {
        anyhow::bail!("unsupported network type: {}", self.config.network);
      },
    }
    Ok(())
  }

  /// Returns the local address that the listener is bound to.
  pub fn local_address(&self) -> Option<SocketAddr> {
    self.local_address
  }

  async fn listen_tcp(
    &mut self,
    conn_mng: zyn_common::conn::ConnManager<D, DF, ST>,
    mut done_rx: Receiver<()>,
  ) -> anyhow::Result<()> {
    let config = self.config.clone();

    let mut listener = TcpListener::bind(&config.bind_address)
      .await
      .map_err(|e| anyhow!("failed to bind to TCP socket {}: {}", config.bind_address, e))?;

    let (running_tx, running_rx) = oneshot::channel();

    self.local_address = Some(listener.local_addr()?);

    tokio::spawn(async move {
      let _ = running_tx.send(());

      loop {
        tokio::select! {
            _ = Self::accept_tcp_connection(&mut listener, conn_mng.clone()) => {},
            _ = done_rx.recv() => {
                break;
            }
        }
      }
    });

    // Wait for the listener to start.
    running_rx.await?;

    info!(
      network = TCP_NETWORK,
      address = config.bind_address,
      service_type = ST::NAME,
      "accepting socket connections"
    );
    Ok(())
  }

  async fn listen_unix(
    &self,
    conn_mng: zyn_common::conn::ConnManager<D, DF, ST>,
    mut done_rx: Receiver<()>,
  ) -> anyhow::Result<()> {
    let config = self.config.clone();

    // Validate the socket path is set.
    if config.socket_path.is_empty() {
      anyhow::bail!("unix network selected but socket_path is not set");
    }

    // Check if the socket file already exists and remove it.
    let socket_path = Path::new(&config.socket_path);
    if socket_path.exists() {
      fs::remove_file(socket_path)
        .map_err(|e| anyhow!("failed to remove socket file {}: {}", config.socket_path, e))?;
    }

    // Listen on the Unix socket.
    let mut listener = UnixListener::bind(&config.socket_path)
      .map_err(|e| anyhow!("failed to bind to Unix socket {}: {}", config.socket_path, e))?;

    let (running_tx, running_rx) = oneshot::channel();

    tokio::spawn(async move {
      let _ = running_tx.send(());

      loop {
        tokio::select! {
            _ = Self::accept_unix_connection(&mut listener, conn_mng.clone()) => {},
            _ = done_rx.recv() => {
                break;
            }
        }
      }
    });

    // Wait for the listener to start.
    running_rx.await?;

    info!(network = UNIX_NETWORK, socket_path = config.socket_path, "accepting socket connections");
    Ok(())
  }

  /// Stops the listener.
  pub async fn shutdown(&mut self) -> anyhow::Result<()> {
    assert!(self.done_tx.is_some());

    self.done_tx.take().unwrap().send(()).await?;

    match self.config.network.as_str() {
      TCP_NETWORK => {
        info!(
          network = TCP_NETWORK,
          address = self.config.bind_address,
          service_type = ST::NAME,
          "stopped accepting socket connections",
        );
      },
      UNIX_NETWORK => {
        // Remove the socket file.
        let socket_path = Path::new(&self.config.socket_path);
        if socket_path.exists() {
          fs::remove_file(socket_path)
            .map_err(|e| anyhow!("failed to remove socket file {}: {}", self.config.socket_path, e))?;
        }
        info!(
          network = UNIX_NETWORK,
          socket_path = self.config.socket_path,
          service_type = ST::NAME,
          "stopped accepting socket connections",
        );
      },
      _ => {
        unreachable!("unsupported network type: {}", self.config.network);
      },
    }

    // Wait for the connection manager to stop.
    self.conn_mng.shutdown().await?;

    Ok(())
  }

  async fn accept_tcp_connection(
    listener: &mut TcpListener,
    conn_mng: zyn_common::conn::ConnManager<D, DF, ST>,
  ) -> anyhow::Result<()> {
    let (tcp_stream, addr) = listener.accept().await?;

    debug!(
      network = TCP_NETWORK,
      local_address = format!("{:?}", addr),
      service_type = ST::NAME,
      "accepted connection"
    );

    tokio::spawn(async move {
      match Self::handle_tcp_connection(tcp_stream, conn_mng).await {
        Ok(()) => {},
        Err(err) => {
          warn!(error = ?err, network = TCP_NETWORK, service_type = ST::NAME, "failed to handle connection");
        },
      }
    });

    Ok(())
  }

  async fn accept_unix_connection(
    listener: &mut UnixListener,
    conn_mng: zyn_common::conn::ConnManager<D, DF, ST>,
  ) -> anyhow::Result<()> {
    let (unix_stream, _) = listener.accept().await?;

    debug!(network = UNIX_NETWORK, service_type = ST::NAME, "accepted connection");

    tokio::spawn(async move {
      match Self::handle_unix_connection(unix_stream, conn_mng).await {
        Ok(()) => {},
        Err(err) => {
          warn!(error = ?err, network = UNIX_NETWORK, service_type = ST::NAME, "failed to handle connection");
        },
      }
    });

    Ok(())
  }

  async fn handle_tcp_connection(
    tcp_stream: tokio::net::TcpStream,
    conn_mng: zyn_common::conn::ConnManager<D, DF, ST>,
  ) -> anyhow::Result<()> {
    // Set the TCP_NODELAY option to disable Nagle's algorithm.
    tcp_stream.set_nodelay(true)?;

    conn_mng.run(Stream::Tcp(tcp_stream)).await?;

    Ok(())
  }

  async fn handle_unix_connection(
    unix_stream: tokio::net::UnixStream,
    conn_mng: zyn_common::conn::ConnManager<D, DF, ST>,
  ) -> anyhow::Result<()> {
    conn_mng.run(Stream::Unix(unix_stream)).await?;

    Ok(())
  }
}
