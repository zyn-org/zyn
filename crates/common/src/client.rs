// SPDX-License-Identifier: AGPL-3.0

use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU16;
use std::sync::{Arc, atomic};
use std::time::Duration;

use anyhow::anyhow;
use deadpool::managed::Object;
use deadpool::{Runtime, managed};
use parking_lot::Mutex as PlMutex;
use parking_lot::RwLock as PlRwLock;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::Semaphore;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, OwnedSemaphorePermit, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, warn};

use zyn_protocol::{Message, PongParameters, deserialize, serialize};
use zyn_util::backoff::ExponentialBackoff;
use zyn_util::codec::StreamReader;
use zyn_util::conn::Dialer;
use zyn_util::pool::{MutablePoolBuffer, Pool, PoolBuffer};

use crate::service::Service;

/// Type alias for response sender used in pending requests.
type ResponseSender = oneshot::Sender<anyhow::Result<(Message, Option<PoolBuffer>)>>;

/// TCP network type.
pub const TCP_NETWORK: &str = "tcp";

/// Unix domain socket network type.
pub const UNIX_NETWORK: &str = "unix";

const WRITER_RECEIVER_CAPACITY: usize = 16 * 1024;

const INBOUND_MESSAGE_RECEIVER_CAPACITY: usize = 16 * 1024;

// Zyn client configuration.
#[derive(Clone, Debug)]
pub struct Config {
  /// The maximum number of idle connections to keep in the pool.
  pub max_idle_connections: usize,

  /// The heartbeat interval for the client that should be negotiated
  /// with the server.
  pub heartbeat_interval: Duration,

  /// The client connection timeout.
  /// This is the timeout for establishing a connection to the server.
  pub connect_timeout: Duration,

  /// The client write/read timeout.
  pub timeout: Duration,

  /// The timeout for reading a payload from the server.
  pub payload_read_timeout: Duration,

  /// The initial delay for the backoff strategy.
  pub backoff_initial_delay: Duration,

  /// The maximum delay for the backoff strategy.
  pub backoff_max_delay: Duration,

  /// The maximum number of retries for the backoff strategy.
  pub backoff_max_retries: usize,
}

/// A trait representing the logic required to perform a handshake with the Zyn server.
///
/// The `Handshaker` is responsible for negotiating session parameters over an established stream
/// (TCP or Unix domain socket). This trait allows customization of the handshake logic while
/// remaining agnostic to the transport layer.
///
/// # Associated Types
///
/// * `SessionExtraInfo`: Extra metadata returned by the handshake, which can be used by the client
///   for additional context (e.g., authentication tokens, feature flags, etc.).
#[async_trait::async_trait]
pub trait Handshaker<S>: Clone + Send + Sync + 'static
where
  S: AsyncRead + AsyncWrite,
{
  /// Extra data returned as part of the handshake response, in addition to `SessionInfo`.
  type SessionExtraInfo: Clone + Send + Sync;

  /// Performs a handshake over the provided stream.
  ///
  /// This is typically the first step after establishing a physical connection to the server.
  ///
  /// # Arguments
  ///
  /// * `stream` - A mutable reference to the underlying transport stream used for communication.
  ///
  /// # Returns
  ///
  /// On success, returns a tuple containing:
  /// - `SessionInfo`: Core session parameters negotiated with the server.
  /// - `Self::SessionExtraInfo`: Additional data specific to the implementation.
  ///
  /// # Errors
  ///
  /// Returns an error if the handshake fails due to protocol mismatch, authentication failure,
  /// server error, etc.
  async fn handshake(&self, stream: &mut S) -> anyhow::Result<(SessionInfo, Self::SessionExtraInfo)>;
}

#[derive(Debug)]
pub struct Client<S, HS, ST>(Arc<Mutex<ClientInner<S, HS, ST>>>)
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service;

impl<S, HS, ST> Clone for Client<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  fn clone(&self) -> Self {
    Self(Arc::clone(&self.0))
  }
}

// === impl Client ===

impl<S, HS, ST> Client<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  /// Creates a new `Client` instance with the provided client ID, configuration, and handshaker.
  ///
  /// # Arguments
  ///
  /// * `client_id` - A string-like identifier for the client.
  /// * `config` - Configuration settings for the client.
  /// * `handshaker` - An implementation of the `Handshaker` trait to perform the handshake.
  ///
  /// # Errors
  ///
  /// Returns an error if the connection pool cannot be created.
  pub fn new(
    client_id: impl Into<String>,
    config: Config,
    dialer: Arc<dyn Dialer<Stream = S>>,
    handshaker: HS,
  ) -> anyhow::Result<Self> {
    let inner = ClientInner::new(client_id, config, dialer, handshaker)?;
    Ok(Self(Arc::new(Mutex::new(inner))))
  }

  /// Sends a message to the server and returns a `JoinHandle` to await the response.
  ///
  /// # Arguments
  ///
  /// * `message` - A `Message` instance that must contain a correlation ID.
  /// * `payload_opt` - An optional `PoolBuffer` containing payload data to be sent
  ///   along with the message. Use `None` for messages without payload data.
  ///
  /// # Returns
  ///
  /// A handle to a task that resolves to the response `Message` and an optional payload.
  pub async fn send_message(
    &self,
    message: Message,
    payload_opt: Option<PoolBuffer>,
  ) -> anyhow::Result<JoinHandle<anyhow::Result<(Message, Option<PoolBuffer>)>>> {
    let inner = self.0.lock().await;
    let mut conn = inner.get_connection().await?;
    Ok(conn.send_message(message, payload_opt).await)
  }

  /// Retrieves the current session information, including extra info from the handshake.
  ///
  /// # Returns
  ///
  /// A tuple of `(SessionInfo, H::SessionExtraInfo)` if the handshake was successful.
  ///
  /// # Errors
  ///
  /// Returns an error if no valid connection could be established.
  pub async fn session_info(&self) -> anyhow::Result<(SessionInfo, HS::SessionExtraInfo)> {
    let inner = self.0.lock().await;
    let conn = inner.get_connection().await?;
    Ok(conn.session_info.as_ref().unwrap().clone())
  }

  /// Gracefully shuts down the client by terminating all pooled connections.
  ///
  /// # Returns
  ///
  /// Returns `Ok(())` once the pool is emptied and closed.
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    let inner = self.0.lock().await;
    inner.shutdown().await
  }

  /// Generates the next unique correlation ID for message tracking.
  ///
  /// This method provides a monotonically increasing 16-bit identifier that can be used
  /// to correlate requests with their corresponding responses. The ID counter wraps around
  /// to 0 after reaching the maximum value of `u16::MAX`.
  ///
  /// # Returns
  ///
  /// A unique `u16` correlation ID that can be used for message tracking.
  pub async fn next_id(&self) -> u16 {
    let inner = self.0.lock().await;
    inner.next_id().await
  }

  /// Returns a stream of inbound messages from the server.
  ///
  /// This method provides access to unsolicited messages sent by the server that are not
  /// responses to client requests.
  ///
  /// # Multiplexing
  ///
  /// The returned stream multiplexes inbound messages from all active connections in the
  /// connection pool. Each connection shares the same underlying channel sender, so messages
  /// from any connection will appear in this single stream.
  ///
  /// # Important
  ///
  /// This method can only be called **once** per `Client` instance. Subsequent calls will panic.
  /// This is because the method takes ownership of the internal receiver, ensuring there is only
  /// one consumer of inbound messages.
  ///
  /// # Panics
  ///
  /// Panics if called more than once on the same `Client` instance.
  pub async fn inbound_stream(&self) -> ReceiverStream<(Message, Option<PoolBuffer>)> {
    let mut inner = self.0.lock().await;
    let rx = inner.inbound_rx.take().expect("inbound_stream can only be called once");
    ReceiverStream::new(rx)
  }
}

pub struct ClientInner<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  /// The configuration for the modulator client.
  config: Arc<Config>,

  /// The next correlation id.
  next_id: AtomicU16,

  /// Connection pool for managing connections to the modulator server.
  conn_pool: managed::Pool<ClientConnManager<S, HS, ST>>,

  /// Receiver for inbound messages.
  inbound_rx: Option<mpsc::Receiver<(Message, Option<PoolBuffer>)>>,

  /// Phantom data for the service type.
  _service_type: PhantomData<ST>,
}

impl<S, HS, ST> fmt::Debug for ClientInner<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ClientInner")
      .field("config", &self.config)
      .field("conn_pool", &format_args!("managed::Pool(max_size: {})", self.config.max_idle_connections))
      .finish()
  }
}

// === impl ClientInner ===

impl<S, HS, ST> ClientInner<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  fn new(
    client_id: impl Into<String>,
    config: Config,
    dialer: Arc<dyn Dialer<Stream = S>>,
    handshaker: HS,
  ) -> anyhow::Result<Self> {
    let max_idle_connections = config.max_idle_connections;
    let connect_timeout = config.connect_timeout;

    let arc_client_id = Arc::new(client_id.into());
    let arc_config = Arc::new(config);

    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(INBOUND_MESSAGE_RECEIVER_CAPACITY);

    let conn_pool =
      managed::Pool::builder(ClientConnManager::new(arc_client_id, arc_config.clone(), dialer, handshaker, inbound_tx))
        .max_size(max_idle_connections)
        .create_timeout(Some(connect_timeout))
        .runtime(Runtime::Tokio1)
        .build()?;

    Ok(Self {
      config: arc_config.clone(),
      next_id: AtomicU16::new(1),
      inbound_rx: Some(inbound_rx),
      conn_pool,
      _service_type: PhantomData,
    })
  }

  async fn get_connection(&self) -> anyhow::Result<Object<ClientConnManager<S, HS, ST>>> {
    ExponentialBackoff::new()
      .with_initial_delay(self.config.backoff_initial_delay)
      .with_max_delay(self.config.backoff_max_delay)
      .with_jitter(true)
      .with_max_attempts(self.config.backoff_max_retries)
      .retry_with_backoff(|| async { self.get_connection_from_pool().await })
      .await
  }

  async fn get_connection_from_pool(&self) -> anyhow::Result<Object<ClientConnManager<S, HS, ST>>> {
    let get_pool_conn = || async { self.conn_pool.get().await.map_err(|e| anyhow!(" {}", e)) };

    match get_pool_conn().await {
      Ok(conn) => {
        if !conn.is_unhealthy() {
          return Ok(conn);
        }
        // Filter out unhealthy connections and retry.
        self.conn_pool.retain(|c, _| !c.is_unhealthy());
        get_pool_conn().await
      },
      Err(e) => Err(e),
    }
  }

  async fn shutdown(&self) -> anyhow::Result<()> {
    // Remove all connections from the pool.
    self.conn_pool.retain(|_, _| false);
    self.conn_pool.close();

    Ok(())
  }

  async fn next_id(&self) -> u16 {
    self.next_id.fetch_add(1, atomic::Ordering::SeqCst) + 1
  }
}

struct ClientConnManager<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  /// The client ID used to identify the client.
  client_id: Arc<String>,

  /// The client configuration.
  config: Arc<Config>,

  /// unique connection ID generator.
  next_conn_id: atomic::AtomicU16,

  /// The dialer used to establish connections.
  dialer: Arc<dyn Dialer<Stream = S>>,

  /// The handshaker used to perform the handshake with the server.
  handshaker: HS,

  /// The inbound message sender.
  inbound_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,

  /// Phantom data for the service type.
  _service_type: std::marker::PhantomData<ST>,
}

impl<S, HS, ST> ClientConnManager<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  pub fn new(
    client_id: Arc<String>,
    config: Arc<Config>,
    dialer: Arc<dyn Dialer<Stream = S>>,
    handshaker: HS,
    inbound_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,
  ) -> Self {
    Self {
      client_id,
      config,
      next_conn_id: atomic::AtomicU16::new(1),
      dialer,
      handshaker,
      inbound_tx,
      _service_type: std::marker::PhantomData,
    }
  }
}

impl<S, HS, ST> managed::Manager for ClientConnManager<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  type Type = ClientConn<S, HS, ST>;
  type Error = anyhow::Error;

  async fn create(&self) -> Result<Self::Type, Self::Error> {
    let conn_id = self.next_conn_id.fetch_add(1, atomic::Ordering::SeqCst);

    let mut conn = ClientConn::<S, HS, ST>::new(
      self.client_id.clone(),
      conn_id,
      self.config.clone(),
      self.dialer.clone(),
      self.handshaker.clone(),
      self.inbound_tx.clone(),
    );

    conn.connect().await?;

    Ok(conn)
  }

  async fn recycle(
    &self,
    conn: &mut ClientConn<S, HS, ST>,
    _: &managed::Metrics,
  ) -> managed::RecycleResult<Self::Error> {
    // Check if the connection is still healthy, and if not, shutdown it.
    if conn.is_unhealthy() {
      conn.shutdown().await?;
      return Err(managed::RecycleError::Backend(conn.error_state.take_error().unwrap()));
    }
    Ok(())
  }

  fn detach(&self, conn: &mut ClientConn<S, HS, ST>) {
    tokio::task::block_in_place(|| {
      tokio::runtime::Handle::current().block_on(async {
        match conn.shutdown().await {
          Ok(_) => {
            debug!(
              client_id = conn.client_id.as_str(),
              connection_id = conn.conn_id,
              service_type = ST::NAME,
              "detached client connection"
            )
          },
          Err(e) => error!(
            client_id = conn.client_id.as_str(),
            connection_id = conn.conn_id,
            service_type = ST::NAME,
            "failed to detach client connection: {}",
            e
          ),
        }
      })
    });
  }
}

#[derive(Clone, Debug)]
struct ErrorState(Arc<PlMutex<Option<anyhow::Error>>>);

// === impl ErrorState ===

impl ErrorState {
  pub fn new() -> Self {
    Self(Arc::new(PlMutex::new(None)))
  }

  #[inline]
  pub fn set_error(&self, error: anyhow::Error) {
    let mut state = self.0.lock();
    state.replace(error);
  }

  #[inline]
  pub fn has_error(&self) -> bool {
    let state = self.0.lock();
    state.is_some()
  }

  #[inline]
  pub fn take_error(&self) -> Option<anyhow::Error> {
    let mut state = self.0.lock();
    state.take()
  }
}

#[derive(Copy, Clone, Debug)]
pub struct SessionInfo {
  /// the heartbeat interval negotiated with the modulator server
  pub heartbeat_interval: u32,

  /// the maximum number of inflight requests allowed by the modulator server
  pub max_inflight_requests: u32,

  /// the maximum message size allowed by the modulator server
  pub max_message_size: u32,

  /// the maximum payload size allowed by the modulator server
  pub max_payload_size: u32,
}

struct PendingRequest {
  /// The sender for the response.
  sender: Option<ResponseSender>,

  /// The owned semaphore permit for inflight requests.
  _permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
struct PendingRequests(Arc<PlRwLock<HashMap<u16, PendingRequest>>>);

// == impl PendingRequests ===

impl PendingRequests {
  fn new() -> Self {
    Self(Arc::new(PlRwLock::new(HashMap::new())))
  }

  #[inline]
  fn insert(&self, correlation_id: u16, request: PendingRequest) {
    self.0.write().insert(correlation_id, request);
  }

  #[inline]
  fn take_response_sender(&self, correlation_id: u16) -> Option<ResponseSender> {
    let mut pending_requests = self.0.write();
    pending_requests.get_mut(&correlation_id).and_then(|request| request.sender.take())
  }

  #[inline]
  fn remove(&self, correlation_id: &u16) -> Option<PendingRequest> {
    self.0.write().remove(correlation_id)
  }
}

struct ClientConn<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  /// The client ID used to identify the client.
  client_id: Arc<String>,

  /// Unique connection ID.
  conn_id: u16,

  /// The configuration for the modulator client.
  config: Arc<Config>,

  /// The dialer used to establish connections.
  dialer: Arc<dyn Dialer<Stream = S>>,

  /// The handshaker used to perform the handshake with the server.
  handshaker: HS,

  /// The inbound message sender.
  inbound_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,

  /// The session information after the handshake is completed.
  session_info: Option<(SessionInfo, HS::SessionExtraInfo)>,

  /// Semaphore to limit the number of inflight requests.
  inflight_requests_sem: Option<Arc<Semaphore>>,

  /// Pending requests map, used to track requests that are waiting for a response.
  pending_requests: PendingRequests,

  /// The sender for the writer task.
  writer_tx: Option<mpsc::Sender<(Message, Option<PoolBuffer>)>>,

  /// Connection task tracker.
  task_tracker: TaskTracker,

  /// The shutdown cancellation token.
  shutdown_token: CancellationToken,

  /// Indicates whether the connection is active.
  error_state: ErrorState,

  /// Phantom data for the service type.
  _service_type: PhantomData<ST>,
}

// === impl ClientConn ===

impl<S, HS, ST> ClientConn<S, HS, ST>
where
  S: AsyncRead + AsyncWrite + Send + Sync + 'static,
  HS: Handshaker<S>,
  ST: Service,
{
  fn new(
    client_id: Arc<String>,
    conn_id: u16,
    config: Arc<Config>,
    dialer: Arc<dyn Dialer<Stream = S>>,
    handshaker: HS,
    inbound_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,
  ) -> Self {
    let task_tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();

    Self {
      client_id,
      conn_id,
      config,
      dialer,
      handshaker,
      inbound_tx,
      session_info: None,
      inflight_requests_sem: None,
      pending_requests: PendingRequests::new(),
      writer_tx: None,
      task_tracker,
      shutdown_token,
      error_state: ErrorState::new(),
      _service_type: PhantomData,
    }
  }

  async fn connect(&mut self) -> anyhow::Result<()> {
    // Establish connection using the dialer
    let mut stream = self.dialer.dial().await?;

    // Handshake with the server
    let (session_info, session_extra_info) = self.handshaker.handshake(&mut stream).await?;

    debug!(
      client_id = self.client_id.as_str(),
      conn_id = self.conn_id,
      heartbeat_interval = session_info.heartbeat_interval,
      max_inflight_requests = session_info.max_inflight_requests,
      max_message_size = session_info.max_message_size,
      max_payload_size = session_info.max_payload_size,
      service_type = ST::NAME,
      "client handshake completed",
    );

    // Configure max inflight requests semaphore
    self.inflight_requests_sem = Some(Arc::new(Semaphore::new(session_info.max_inflight_requests as usize)));

    // Create buffer pools for message serialization (reading and writing) and payload handling
    let message_pool = Pool::new(2 * self.config.max_idle_connections, session_info.max_message_size as usize);

    let payload_pool = Pool::new(self.config.max_idle_connections, session_info.max_payload_size as usize);

    // Spawn writer and reader tasks
    let task_tracker = self.task_tracker.clone();
    let shutdown_token = self.shutdown_token.clone();

    let (rh, wh) = tokio::io::split(stream);

    let (writer_tx, writer_rx) = mpsc::channel::<(Message, Option<PoolBuffer>)>(WRITER_RECEIVER_CAPACITY);
    task_tracker.spawn(Self::writer_task(
      self.client_id.clone(),
      self.conn_id,
      wh,
      writer_rx,
      message_pool.must_acquire(),
      self.error_state.clone(),
      shutdown_token.clone(),
    ));
    self.writer_tx = Some(writer_tx.clone());

    task_tracker.spawn(Self::reader_task(
      self.client_id.clone(),
      self.conn_id,
      rh,
      message_pool.must_acquire(),
      payload_pool,
      self.pending_requests.clone(),
      self.error_state.clone(),
      writer_tx.clone(),
      self.inbound_tx.clone(),
      shutdown_token.clone(),
      self.config.payload_read_timeout,
    ));

    self.session_info = Some((session_info, session_extra_info));

    Ok(())
  }

  async fn shutdown(&mut self) -> anyhow::Result<()> {
    // Signal shutdown to all tasks
    self.shutdown_token.cancel();

    // Wait for all tasks to complete
    self.task_tracker.close();
    self.task_tracker.wait().await;

    Ok(())
  }

  async fn send_message(
    &mut self,
    message: Message,
    payload_opt: Option<PoolBuffer>,
  ) -> JoinHandle<anyhow::Result<(Message, Option<PoolBuffer>)>> {
    assert!(self.inflight_requests_sem.is_some());

    let request_timeout = self.config.timeout;
    let pending_requests = self.pending_requests.clone();
    let writer_tx = self.writer_tx.as_ref().unwrap().clone();
    let inflight_requests_sem = self.inflight_requests_sem.as_ref().unwrap().clone();

    self.task_tracker.spawn(async move {
      match tokio::time::timeout(
        request_timeout,
        Self::perform_request(message, payload_opt, pending_requests, writer_tx, inflight_requests_sem),
      )
      .await
      {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("request timed out")),
      }
    })
  }

  #[inline]
  fn is_unhealthy(&self) -> bool {
    self.error_state.has_error()
  }

  async fn perform_request(
    message: Message,
    payload_opt: Option<PoolBuffer>,
    pending_requests: PendingRequests,
    writer_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,
    inflight_requests_sem: Arc<Semaphore>,
  ) -> anyhow::Result<(Message, Option<PoolBuffer>)> {
    // First, check if the message has a correlation ID.
    let correlation_id = match message.correlation_id() {
      Some(id) => id,
      None => {
        return Err(anyhow::anyhow!("message must have a correlation identifier"));
      },
    };

    // Acquire a permit from the semaphore.
    let permit = inflight_requests_sem
      .acquire_owned()
      .await
      .map_err(|_| anyhow!("failed to acquire semaphore permit for inflight requests"))?;

    // Create response channels.
    let (resp_tx, resp_rx) = oneshot::channel();

    // Register the pending request.
    pending_requests.insert(correlation_id, PendingRequest { sender: Some(resp_tx), _permit: permit });

    // Send the message.
    if let Err(e) = writer_tx.send((message, payload_opt)).await {
      pending_requests.remove(&correlation_id);
      return Err(anyhow!("failed to send message: {}", e));
    }

    // Wait for a response with timeout.
    let resp = resp_rx.await;

    pending_requests.remove(&correlation_id);

    // Return the response.
    match resp {
      Ok(Ok((msg, payload_opt))) => Ok((msg, payload_opt)),
      Ok(Err(e)) => Err(e),
      Err(_) => panic!("response channel closed before receiving a response"),
    }
  }

  async fn writer_task(
    client_id: Arc<String>,
    conn_id: u16,
    mut wh: WriteHalf<S>,
    mut rx: Receiver<(Message, Option<PoolBuffer>)>,
    mut message_buff: MutablePoolBuffer,
    error_state: ErrorState,
    shutdown_token: CancellationToken,
  ) {
    loop {
      tokio::select! {
        // Write the message to the stream.
        msg_opt = rx.recv() => {
          match msg_opt {
            Some((msg, payload_opt)) => {
              match Self::write_message(&msg, payload_opt, &mut wh, message_buff.as_mut_slice()).await {
                Ok(_) => {},
                Err(e) => {
                  error!(client_id = client_id.as_str(), connection_id = conn_id, service_type = ST::NAME, "{}", e.to_string());
                  error_state.set_error(e);
                  break;
                },
              }
            }
            None => {
              // The receiver has been closed, exit the loop.
              break;
            }
          }
        },

        // Shutdown token received, exit the loop.
        _ = shutdown_token.cancelled() => {
          break;
        }
      }
    }

    match wh.shutdown().await {
      Ok(_) => {},
      Err(e) => warn!(
        client_id = client_id.as_str(),
        connection_id = conn_id,
        service_type = ST::NAME,
        "failed to shutdown client stream: {}",
        e
      ),
    }
  }

  #[allow(clippy::too_many_arguments)]
  async fn reader_task(
    client_id: Arc<String>,
    conn_id: u16,
    rh: ReadHalf<S>,
    read_buffer: MutablePoolBuffer,
    payload_buffer_pool: Pool,
    pending_requests: PendingRequests,
    error_state: ErrorState,
    writer_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,
    inbound_tx: mpsc::Sender<(Message, Option<PoolBuffer>)>,
    shutdown_token: CancellationToken,
    payload_read_timeout: Duration,
  ) where
    ST: Service,
  {
    let mut stream_reader = StreamReader::with_pool_buffer(rh, read_buffer);

    loop {
      tokio::select! {
        // Read the next line from the stream.
        res = {
          stream_reader.next()
        } => {
          match res {
            Ok(true) => {
              let line_bytes = stream_reader.get_line().unwrap();

              // Deserialize the server message and handle it.
              match deserialize(Cursor::new(line_bytes)) {
                Ok(msg) => {
                    // Read optional payload.
                    let res = match Self::read_message_payload(&msg, &mut stream_reader, payload_buffer_pool.clone(), payload_read_timeout).await {
                        Ok(payload_opt) => Ok((msg.clone(), payload_opt)),
                        Err(e) => Err(anyhow!(e)),
                    };

                    if let Some(correlation_id) = msg.correlation_id() {
                        match msg {
                            Message::Ping(_) => {
                                debug!(client_id = client_id.as_str(), connection_id = conn_id, correlation_id, service_type = ST::NAME, "received ping message");

                                if writer_tx.send((Message::Pong(PongParameters { id: correlation_id }), None)).await.is_ok() {
                                    debug!(client_id = client_id.as_str(), connection_id = conn_id, correlation_id, service_type = ST::NAME, "sent pong message");
                                }
                                continue;
                            },
                            _ => {
                                // If the response correlates with a pending request, send the result to the corresponding sender.
                                if let Some(sender) = pending_requests.take_response_sender(correlation_id) {
                                    if let Err(e) = sender.send(res) {
                                        warn!(client_id = client_id.as_str(), connection_id = conn_id, correlation_id, service_type = ST::NAME, error = ?e, "failed to send client response");
                                    }
                                } else {
                                    warn!(client_id = client_id.as_str(), connection_id = conn_id, correlation_id, service_type = ST::NAME, "unexpected response");
                                }
                            }
                        }
                    } else {
                        match res {
                            Ok((msg, payload_opt)) => {
                                if let Err(e) = inbound_tx.try_send((msg, payload_opt)) {
                                    warn!(client_id = client_id.as_str(), connection_id = conn_id, service_type = ST::NAME, error = ?e, "dropped inbound message: channel full or closed");
                                }
                            },
                            Err(e) => {
                                warn!(client_id = client_id.as_str(), connection_id = conn_id, service_type = ST::NAME, error = ?e, "failed to read inbound message payload");
                            }
                        }
                    }
                }
                Err(e) => {
                  warn!(client_id = client_id.as_str(), connection_id = conn_id, service_type = ST::NAME, "{}", e.to_string());
                  error_state.set_error(e);
                  break;
                }
              }
            }

            Ok(false) => {
              // Stream closed by the server.
              debug!(client_id = client_id.as_str(), connection_id = conn_id, service_type = ST::NAME, "connection closed by peer");
              error_state.set_error(anyhow!("connection closed by peer"));
              break;
            }

            Err(e) => {
              warn!(client_id = client_id.as_str(), connection_id = conn_id, service_type = ST::NAME, "{}", e.to_string());
              error_state.set_error(e.into());
              break;
            }
          }
        }

        // Shutdown token received, exit the loop.
        _ = shutdown_token.cancelled() => {
          break;
        }
      }
    }
  }

  async fn read_message_payload(
    msg: &Message,
    stream_reader: &mut StreamReader<ReadHalf<S>>,
    payload_buffer_pool: Pool,
    payload_read_timeout: Duration,
  ) -> anyhow::Result<Option<PoolBuffer>> {
    match msg.payload_info() {
      Some(payload_info) => {
        let mut pool_buff = {
          match payload_buffer_pool.acquire() {
            Some(buffer) => buffer,
            None => return Err(anyhow!("failed to acquire payload buffer")),
          }
        };

        let payload = &mut pool_buff.as_mut_slice()[..payload_info.length];

        match tokio::time::timeout(payload_read_timeout, Self::read_payload(payload, stream_reader)).await {
          Ok(res) => match res {
            Ok(_) => Ok(Some(pool_buff.freeze(payload_info.length))),
            Err(e) => Err(anyhow!("failed to read payload: {}", e)),
          },
          Err(_) => Err(anyhow!("payload read timed out")),
        }
      },
      None => Ok(None),
    }
  }

  async fn write_message(
    message: &Message,
    payload_opt: Option<PoolBuffer>,
    writer: &mut WriteHalf<S>,
    write_buffer: &mut [u8],
  ) -> anyhow::Result<()> {
    let n = serialize(message, write_buffer).map_err(|e| anyhow!("failed to serialize message: {}", e))?;
    writer.write_all(&write_buffer[..n]).await?;

    if let Some(payload) = payload_opt {
      writer.write_all(payload.as_slice()).await?;
      let _ = writer.write(b"\n").await?;
    }

    writer.flush().await?;

    Ok(())
  }

  async fn read_payload(buffer: &mut [u8], stream_reader: &mut StreamReader<ReadHalf<S>>) -> anyhow::Result<()> {
    stream_reader.read_raw(buffer).await?;

    // Read last byte, and ensure it's a newline.
    let mut cr: [u8; 1] = [0; 1];
    stream_reader.read_raw(&mut cr).await?;

    // Verify it's actually a newline
    if cr[0] != b'\n' {
      return Err(anyhow!("invalid payload format"));
    }

    Ok(())
  }
}
