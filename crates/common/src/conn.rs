// SPDX-License-Identifier: AGPL-3.0

use core::fmt::Debug;
use std::io::Cursor;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use parking_lot::Mutex as PlMutex;
use rand::random;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadHalf};
use tokio::sync::RwLock;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use zyn_protocol::ErrorReason::{
  BadRequest, InternalServerError, MessageChannelIsFull, PolicyViolation, ServerOverloaded, ServerShuttingDown, Timeout,
};
use zyn_protocol::{ErrorParameters, Message, PingParameters, deserialize, serialize};

use zyn_util::codec::{StreamReader, StreamReaderError};
use zyn_util::pool::{BucketedPool, MutablePoolBuffer, Pool, PoolBuffer};
use zyn_util::slab::{Slab, SlabRef};
use zyn_util::string_atom::StringAtom;

use crate::service::Service;

const SERVER_OVERLOADED_ERROR: &[u8] = b"ERROR reason=SERVER_OVERLOADED detail=\\\"max connections reached\\\"\n";

/// Represents the current state of a client connection in the protocol flow.
///
/// Client connections progress through these states in a strictly forward-only manner.
/// Once a connection reaches the `Authenticated` state, it remains in that state
/// until the connection is closed.
///
/// The implementation enforces that state transitions can only advance forward
/// and never regress to a previous state.
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
pub enum State {
  /// Initial state when a client first connects.
  /// In this state, only initial handshake messages should be accepted.
  Connecting,

  /// Client has established the connection but has not yet authenticated.
  /// In this state, only authentication-related messages are accepted.
  /// A timeout will disconnect the client if authentication is not completed.
  Connected,

  /// Terminal state: client has successfully authenticated and can perform all operations.
  /// Once a connection reaches this state, it cannot transition to any other state.
  /// The negotiated heartbeat interval is provided and used for connection health monitoring.
  /// All protocol messages should be accepted in this state.
  Authenticated { heartbeat_interval: Duration },
}

/// A trait for handling messages received by a connection.
///
/// Implementors of this trait are responsible for processing incoming protocol messages,
/// managing connection state transitions, and executing appropriate business logic.
///
/// The `Dispatcher` trait is designed to work within the connection management system
/// and follows a state-based approach to protocol handling, where different message
/// types are allowed in different connection states.
///
/// # State Transitions
///
/// Dispatchers enforce a forward-only state progression through the `State` enum:
/// - `State::Connecting` → `State::Connected` → `State::Authenticated`
///
/// Implementations must ensure state transitions only move forward, never backward.
#[async_trait]
pub trait Dispatcher: Default + Send + Sync + 'static {
  /// Processes an incoming message based on the current connection state.
  ///
  /// This method is the core of the message handling logic. It receives a message,
  /// an optional payload buffer, and the current connection state, and is responsible
  /// for executing the appropriate business logic.
  ///
  /// # State Transitions
  ///
  /// This method may return a new state to transition the connection to. The
  /// connection manager will enforce that transitions only move forward in the
  /// state progression.
  ///
  /// # Parameters
  ///
  /// * `msg` - The protocol message to process
  /// * `payload` - Optional payload data associated with the message,
  ///               typically present for content-bearing messages like broadcasts
  /// * `state` - The current connection state
  ///
  /// # Returns
  ///
  /// * `Ok(Some(new_state))` - Processing succeeded and the connection should
  ///                           transition to the new state
  /// * `Ok(None)` - Processing succeeded with no state change
  /// * `Err(e)` - An error occurred during processing
  ///
  /// # Errors
  ///
  /// Errors are typically wrapped in `ConnError` to provide structured error
  /// information to the client, including whether the error is recoverable.
  async fn dispatch_message(
    &mut self,
    msg: Message,
    payload: Option<PoolBuffer>,
    state: State,
  ) -> anyhow::Result<Option<State>>;

  /// Initializes the dispatcher.
  ///
  /// This method is called once when a new connection is established, before
  /// any messages are processed. It allows the dispatcher to set up its initial
  /// state, register with other system components, or perform other setup tasks.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Bootstrapping succeeded
  /// * `Err(e)` - An error occurred during bootstrapping
  ///
  /// # Errors
  ///
  /// If bootstrapping fails, the connection will be closed immediately.
  async fn bootstrap(&mut self) -> anyhow::Result<()>;

  /// Cleans up the dispatcher's resources.
  ///
  /// This method is called when a connection is closed, either due to a client
  /// disconnect, an error, or server shutdown. It allows the dispatcher to
  /// clean up any resources, unregister from other components, or perform
  /// other teardown tasks.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Shutdown succeeded
  /// * `Err(e)` - An error occurred during shutdown
  ///
  /// # Errors
  ///
  /// Errors during shutdown are logged but generally do not affect the
  /// connection close process.
  async fn shutdown(&mut self) -> anyhow::Result<()>;
}

/// A factory for creating new `Dispatcher` instances.
///
/// This trait separates the creation of dispatchers from their usage,
/// allowing for dependency injection and better testability.
///
/// Implementors typically hold configuration data and references to
/// shared resources that new dispatchers will need access to.
#[async_trait]
pub trait DispatcherFactory<D: Dispatcher>: Clone + Send + Sync + 'static {
  /// Creates a new instance of a dispatcher factory.
  ///
  /// This method is called whenever a new connection is established
  /// and needs a dispatcher to handle its messages.
  ///
  /// # Returns
  ///
  /// A slab reference to the dispatcher instance, ready to handle
  /// messages for the new connection.
  async fn create(&mut self, handler: usize, tx: ConnTx) -> SlabRef<D>;

  /// Bootstraps the dispatcher factory with initial configuration and resources.
  ///
  /// This method is called once during initialization to set up any shared
  /// resources, establish connections, or perform other one-time setup tasks
  /// that the factory needs before it can start creating dispatchers.
  ///
  /// Implementations might use this to:
  /// * Initialize connection pools
  /// * Set up background tasks
  /// * Load configuration from external sources
  /// * Establish connections to external services
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Bootstrap succeeded and the factory is ready to create dispatchers
  /// * `Err(e)` - An error occurred during bootstrap, preventing factory initialization
  async fn bootstrap(&mut self) -> anyhow::Result<()>;

  /// Shuts down the dispatcher factory and cleans up resources.
  ///
  /// This method is called when the connection manager is shutting down,
  /// allowing the factory to clean up any resources it holds.
  ///
  /// # Returns
  ///
  /// * `Ok(())` - Shutdown succeeded
  /// * `Err(e)` - An error occurred during shutdown
  async fn shutdown(&mut self) -> anyhow::Result<()>;
}

/// The connection configuration.
#[derive(Debug, Default)]
pub struct Config {
  /// The maximum number of connections that the manager can handle.
  pub max_connections: u32,

  /// The maximum message size allowed.
  pub max_message_size: u32,

  /// The maximum payload size allowed.
  pub max_payload_size: u32,

  /// The timeout for the connection phase.
  pub connect_timeout: Duration,

  /// The timeout for the authentication phase.
  pub authenticate_timeout: Duration,

  /// The timeout for reading a broadcast payload.
  pub payload_read_timeout: Duration,

  /// Total memory budget in bytes for the payload buffer pool.
  /// The pool will allocate buffers of varying sizes up to this total.
  pub payload_pool_memory_budget: u32,

  /// The maximum number of messages that can be enqueued
  /// before disconnecting the client.
  pub send_message_channel_size: u32,

  /// The timeout for the request.
  pub request_timeout: Duration,

  /// The connection maximum number of inflight requests.
  pub max_inflight_requests: u32,

  /// The maximum number of bytes that can be read per second.
  pub rate_limit: u32,
}

/// The connection manager inner state.
#[derive(Debug)]
struct ConnManagerInner<D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> {
  /// The connection manager configuration.
  config: Arc<Config>,

  /// The connections.
  connections: Slab<Conn<D>>,

  /// The connection dispatcher factory.
  dispatcher_factory: DF,

  /// The message buffer pool.
  message_buffer_pool: Pool,

  /// The payload buffer pool.
  payload_buffer_pool: BucketedPool,

  /// Connection task tracker.
  task_tracker: TaskTracker,

  /// The shutdown cancellation token.
  shutdown_token: CancellationToken,

  /// Phantom data for the service type.
  _service_type: PhantomData<ST>,
}

/// The connection manager.
#[derive(Debug)]
pub struct ConnManager<D: Dispatcher, DF: DispatcherFactory<D>, ST: Service>(Arc<RwLock<ConnManagerInner<D, DF, ST>>>);

// ===== impl ConnManager =====

impl<D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> Clone for ConnManager<D, DF, ST> {
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<D: Dispatcher, DF: DispatcherFactory<D>, ST: Service> ConnManager<D, DF, ST> {
  /// Creates a new connection manager.
  pub fn new(config: impl Into<Config>, dispatcher_factory: DF) -> Self {
    let conn_cfg = config.into();

    let max_connections = conn_cfg.max_connections as usize;

    let connections = Slab::with_capacity(max_connections);

    // Create a message buffer pool with a size of max_connections * 2,
    // one for reading and one for writing.
    let message_buffer_pool = Pool::new(max_connections * 2, conn_cfg.max_message_size as usize);

    // Use the configured memory budget for the payload buffer pool
    let memory_budget_bytes = conn_cfg.payload_pool_memory_budget as usize;

    // Create the bucketed pool with the configured memory budget.
    // The pool will distribute the budget across different size buckets.
    let payload_buffer_pool = BucketedPool::new_with_memory_budget(
      4096,                               // min buffer size: 4KB
      conn_cfg.max_payload_size as usize, // max buffer size
      memory_budget_bytes,                // total memory budget
      max_connections,                    // max buffers per bucket
      2,                                  // 2x growth between buckets
      1.0 / 3.0,                          // 33% decay
    );

    let task_tracker = TaskTracker::new();
    let shutdown_token = CancellationToken::new();

    let inner = ConnManagerInner {
      config: Arc::new(conn_cfg),
      connections,
      dispatcher_factory,
      message_buffer_pool,
      payload_buffer_pool,
      task_tracker,
      shutdown_token,
      _service_type: PhantomData,
    };

    Self(Arc::new(RwLock::new(inner)))
  }

  /// Bootstraps the connection manager.
  pub async fn bootstrap(&self) -> anyhow::Result<()> {
    let mut inner = self.0.write().await;

    inner.dispatcher_factory.bootstrap().await?;

    info!(max_conns = inner.connections.capacity().await, service_type = ST::NAME, "connection manager started");

    Ok(())
  }

  /// Shuts down the connection manager.
  pub async fn shutdown(&self) -> anyhow::Result<()> {
    let mut inner = self.0.write().await;

    let connection_count = inner.connections.len().await;

    // Notify the shutdown to all active connections.
    inner.shutdown_token.cancel();

    // Wait for all connections to finish.
    inner.task_tracker.close();
    inner.task_tracker.wait().await;

    inner.dispatcher_factory.shutdown().await?;

    info!(connection_count = connection_count, service_type = ST::NAME, "connection manager stopped");

    Ok(())
  }

  pub async fn run<T>(&self, mut stream: T) -> anyhow::Result<()>
  where
    T: AsyncRead + AsyncWrite + Unpin,
  {
    let inner = self.0.read().await;

    let config = inner.config.clone();
    let message_buffer_pool = inner.message_buffer_pool.clone();
    let payload_buffer_pool = inner.payload_buffer_pool.clone();
    let mut conns = inner.connections.clone();
    let shutdown_token = inner.shutdown_token.clone();
    let mut dispatcher_factory = inner.dispatcher_factory.clone();

    drop(inner);

    // Acquire a connection.
    let conn_ref_opt = conns.acquire().await;

    // If no connection is available, send an error message and return.
    if conn_ref_opt.is_none() {
      stream.write_all(SERVER_OVERLOADED_ERROR).await?;
      stream.flush().await?;

      stream.shutdown().await?;

      let max_conns = conns.capacity().await;
      warn!(max_conns, service_type = ST::NAME, "max connections limit reached");

      return Ok(());
    }
    let conn_ref = conn_ref_opt.unwrap();

    let read_pool_buffer = message_buffer_pool.must_acquire();
    let write_pool_buffer = message_buffer_pool.must_acquire();

    let send_msg_channel_size = config.send_message_channel_size as usize;

    let (send_msg_tx, send_msg_rx) = channel(send_msg_channel_size);
    let (close_tx, close_rx) = channel(1);
    {
      let mut conn = conn_ref.write().await;

      // Bootstrap the dispatcher.
      let handler = conn_ref.handler;

      let tx = ConnTx { send_msg_tx, close_tx };

      let dispatcher_ref = dispatcher_factory.create(handler, tx.clone()).await;
      dispatcher_ref.write().await.bootstrap().await?;

      conn.0 = Some(ConnInner {
        handler,
        config: config.clone(),
        state: State::Connecting,
        dispatcher_ref,
        task_tracker: TaskTracker::new(),
        cancellation_token: CancellationToken::new(),
        scheduled_task: Arc::new(PlMutex::new(None)),
        heartbeat_id: None,
        inflight_requests: Arc::new(AtomicU32::new(0)),
        tx,
      });

      conn.schedule_timeout(config.connect_timeout, Some(StringAtom::from("connection timeout")));
    }

    let conn_count = conns.len().await;
    info!(handler = conn_ref.handler, connection_count = conn_count, service_type = ST::NAME, "connection registered");

    // Run loop until the connection is closed.
    let payload_read_timeout = config.payload_read_timeout;

    let max_payload_size = config.max_payload_size as usize;

    let rate_limit = config.rate_limit;

    match ConnInner::<D>::run_loop::<T, ST>(
      stream,
      conn_ref.clone(),
      send_msg_rx,
      close_rx,
      shutdown_token,
      read_pool_buffer,
      write_pool_buffer,
      payload_buffer_pool,
      payload_read_timeout,
      max_payload_size,
      rate_limit,
    )
    .await
    {
      Ok(_) => {},
      Err(e) => {
        warn!(handler = conn_ref.handler, service_type = ST::NAME, "connection error: {}", e.to_string());
      },
    }

    conn_ref.release().await;

    let conn_count = conns.len().await;
    info!(
      handler = conn_ref.handler,
      connection_count = conn_count,
      service_type = ST::NAME,
      "connection deregistered"
    );

    Ok(())
  }
}

/// A client connection.
#[derive(Debug, Default)]
pub struct Conn<D: Dispatcher>(Option<ConnInner<D>>);

// ===== impl Conn =====

impl<D: Dispatcher> Deref for Conn<D> {
  type Target = ConnInner<D>;

  fn deref(&self) -> &Self::Target {
    assert!(self.0.is_some(), "ConnInner is not initialized");
    self.0.as_ref().unwrap()
  }
}

impl<D: Dispatcher> DerefMut for Conn<D> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    assert!(self.0.is_some(), "ConnInner is not initialized");
    self.0.as_mut().unwrap()
  }
}

/// Inner connection fields.
#[derive(Debug)]
pub struct ConnInner<D: Dispatcher> {
  /// The connection configuration.
  config: Arc<Config>,

  /// The connection handler.
  handler: usize,

  /// The connection state.
  state: State,

  /// The connection dispatcher.
  dispatcher_ref: SlabRef<D>,

  /// Current number of inflight requests.
  inflight_requests: Arc<AtomicU32>,

  /// The transmitter channels.
  tx: ConnTx,

  /// The currently assigned heartbeat ID.
  heartbeat_id: Option<u16>,

  /// Track tasks associated with connection requests.
  task_tracker: TaskTracker,

  /// Token used to signal request cancellation.
  cancellation_token: CancellationToken,

  /// Current scheduled task (ping or timeout).
  scheduled_task: Arc<PlMutex<Option<tokio::task::JoinHandle<()>>>>,
}

// ===== impl ConnInner =====

impl<D: Dispatcher> ConnInner<D> {
  async fn dispatch_message<ST: Service>(&mut self, msg: Message, payload: Option<PoolBuffer>) -> anyhow::Result<()> {
    let dispatcher_ref = self.dispatcher_ref.clone();

    match self.state {
      State::Authenticated { heartbeat_interval } => {
        // Cancel previous ping.
        self.cancel_scheduled_task();

        // Handle pong message.
        if let Message::Pong(params) = &msg
          && let Some(ping_id) = self.heartbeat_id.take()
        {
          if params.id == ping_id {
            // Reschedule ping.
            self.schedule_ping::<ST>(heartbeat_interval);

            debug!(id = params.id, handler = self.handler, service_type = ST::NAME, "received pong");
          } else {
            let e =
              zyn_protocol::Error { id: None, reason: BadRequest, detail: Some(StringAtom::from("wrong pong id")) };
            Self::notify_error::<ST>(e.into(), self.tx.clone(), self.handler)?;
          }
          return Ok(());
        }

        // Submit the request to the dispatcher asynchronously.
        let handler = self.handler;
        let state = self.state;
        let tx = self.tx.clone();

        self.submit_request::<_, ST>(async move {
          let mut dispatcher = dispatcher_ref.write().await;

          match dispatcher.dispatch_message(msg, payload, state).await {
            Ok(_) => {},
            Err(e) => {
              Self::notify_error::<ST>(e, tx, handler)?;
            },
          }
          Ok(())
        })?;

        self.schedule_ping::<ST>(heartbeat_interval);
      },
      _ => {
        match dispatcher_ref.write().await.dispatch_message(msg, payload, self.state).await {
          Ok(Some(new_state)) => {
            assert!(new_state >= self.state, "invalid state transition");

            let old_state = self.state;
            self.state = new_state;

            // Schedule timeout according to state transition.
            if old_state != self.state {
              // Cancel any previous timeout.
              self.cancel_scheduled_task();

              match self.state {
                State::Connected => {
                  self.schedule_timeout(
                    self.config.authenticate_timeout,
                    Some(StringAtom::from("authentication timeout")),
                  );
                },
                State::Authenticated { heartbeat_interval } => {
                  self.schedule_ping::<ST>(heartbeat_interval);
                },
                _ => {},
              }
            }
          },
          Ok(None) => {},
          Err(e) => {
            Self::notify_error::<ST>(e, self.tx.clone(), self.handler)?;
          },
        }
      },
    }

    Ok(())
  }

  fn submit_request<F, ST>(&mut self, future: F) -> anyhow::Result<()>
  where
    F: Future<Output = anyhow::Result<()>> + Send + 'static,
    ST: Service,
  {
    // First check if the maximum number of inflight requests has been reached,
    // and if not, increment the counter.
    let max_inflight_requests = self.config.max_inflight_requests;
    let inflight_requests = self.inflight_requests.clone();

    let inc_res = inflight_requests.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
      if v < max_inflight_requests { Some(v + 1) } else { None }
    });
    if inc_res.is_err() {
      return Err(
        zyn_protocol::Error {
          id: None,
          reason: PolicyViolation,
          detail: Some(StringAtom::from("max inflight requests reached")),
        }
        .into(),
      );
    }

    // Spawn the request task.
    let task_tracker = self.task_tracker.clone();

    let handler = self.handler;
    let request_timeout = self.config.request_timeout;

    let tx = self.tx.clone();

    let cancellation_token = self.cancellation_token.clone();

    task_tracker.spawn(async move {
      tokio::select! {
        res = future => {
            if let Err(e) = res && let Err(e) = Self::notify_error::<ST>(e, tx, handler) {
                warn!(handler = handler, service_type = ST::NAME, "failed to notify request error: {}", e.to_string());
            }
        },
        _ = tokio::time::sleep(request_timeout) => {
          error!(handler = handler, service_type = ST::NAME, "request timeout");
        },
        _ = cancellation_token.cancelled() => {
          debug!(handler = handler, service_type = ST::NAME, "request cancelled");
        },
      }

      // Decrement the inflight requests counter.
      inflight_requests.fetch_sub(1, Ordering::Relaxed);
    });

    Ok(())
  }

  /// Schedules a timeout task.
  fn schedule_timeout(&mut self, timeout: Duration, detail: Option<StringAtom>) {
    let tx = self.tx.clone();

    let timeout_task = tokio::spawn(async move {
      tokio::time::sleep(timeout).await;

      tx.close(Message::Error(ErrorParameters { id: None, reason: Timeout.into(), detail }));
    });
    self.scheduled_task.lock().replace(timeout_task);
  }

  /// Schedules a ping task.
  fn schedule_ping<ST: Service>(&mut self, heartbeat_interval: Duration) {
    let tx = self.tx.clone();

    let handler = self.handler;

    let scheduled_task = self.scheduled_task.clone();

    // A 3x heartbeat interval is used for the timeout.
    let heartbeat_timeout = heartbeat_interval * 3;

    // Generate a random ping ID.
    let ping_id: u16 = random();
    self.heartbeat_id = Some(ping_id);

    let ping_task = tokio::spawn(async move {
      tokio::time::sleep(heartbeat_interval).await;

      tx.send_message(Message::Ping(PingParameters { id: ping_id }));
      debug!(id = ping_id, handler, service_type = ST::NAME, "sent ping");

      // Schedule keep-alive timeout.
      let timeout_task = tokio::spawn(async move {
        tokio::time::sleep(heartbeat_timeout).await;

        let disconnect_msg = Message::Error(ErrorParameters {
          id: None,
          reason: Timeout.into(),
          detail: Some(StringAtom::from("ping timeout")),
        });

        tx.close(disconnect_msg);
      });
      scheduled_task.lock().replace(timeout_task);
    });
    self.scheduled_task.lock().replace(ping_task);
  }

  /// Cancels currently scheduled task.
  fn cancel_scheduled_task(&mut self) {
    let mut scheduled_task_guard = self.scheduled_task.lock();

    if scheduled_task_guard.is_none() {
      return;
    }
    scheduled_task_guard.take().unwrap().abort();
  }

  /// Shuts down the connection.
  async fn shutdown(&mut self) -> anyhow::Result<()> {
    // Cancel any pending ping or timeout task.
    self.cancel_scheduled_task();

    // Signal cancellation and wait for all request tasks to finish.
    self.cancellation_token.cancel();

    self.task_tracker.close();
    self.task_tracker.wait().await;

    // Shutdown and release the dispatcher.
    self.dispatcher_ref.write().await.shutdown().await?;
    self.dispatcher_ref.release().await;

    Ok(())
  }

  fn notify_error<ST: Service>(e: anyhow::Error, tx: ConnTx, handler: usize) -> anyhow::Result<()> {
    // Notify the client about the error, or disconnect the connection in case it's an internal
    // or non-recoverable error.
    if let Some(conn_err) = e.downcast_ref::<zyn_protocol::Error>() {
      if conn_err.is_recoverable() {
        tx.send_message(conn_err.into());
      } else {
        tx.close(conn_err.into());
      }
    } else {
      error!(handler = handler, service_type = ST::NAME, "internal server error: {}", e.to_string());
      tx.close(Message::Error(ErrorParameters { id: None, reason: InternalServerError.into(), detail: None }));
    }
    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  async fn run_loop<T, ST>(
    stream: T,
    conn_ref: SlabRef<Conn<D>>,
    mut send_msg_rx: Receiver<(Message, Option<PoolBuffer>)>,
    mut close_rx: Receiver<Message>,
    shutdown_token: CancellationToken,
    read_pool_buffer: MutablePoolBuffer,
    mut write_pool_buffer: MutablePoolBuffer,
    payload_buffer_pool: BucketedPool,
    payload_read_timeout: Duration,
    max_payload_size: usize,
    rate_limit: u32,
  ) -> anyhow::Result<()>
  where
    T: AsyncRead + AsyncWrite + Unpin,
    ST: Service,
  {
    let handler = conn_ref.handler;

    // Split the stream into read and write halves.
    let (reader, mut writer) = tokio::io::split(stream);

    let mut stream_reader = StreamReader::with_pool_buffer(reader, read_pool_buffer);

    let write_buffer = write_pool_buffer.as_mut_slice();

    let mut rate_limit_counter = 0;
    let mut rate_limit_last_check = tokio::time::Instant::now();

    'connection_loop: loop {
      tokio::select! {
        // Read the next line from the stream.
        res = {
          stream_reader.next()
        } => {
          match res {
            Ok(true) => {
              let line_bytes = stream_reader.get_line().unwrap();

              // Check if the rate limit is exceeded.
              if rate_limit > 0 {
                let now = tokio::time::Instant::now();
                let elapsed = now.duration_since(rate_limit_last_check);
                if elapsed.as_secs() > 1 {
                    rate_limit_counter = 0;
                    rate_limit_last_check = now;
                }

                rate_limit_counter += line_bytes.len() as u32;
                if rate_limit_counter > rate_limit {
                  let err_message = Message::Error(ErrorParameters{id: None, reason: PolicyViolation.into(), detail: Some("rate limit exceeded".into())});
                  let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
                  break 'connection_loop;
                }
              }

              // Deserialize the message and handle it.
              match deserialize(Cursor::new(line_bytes)) {
                Ok(msg) => {
                  // Dispatch the message to the connection.
                  let mut conn_guard = conn_ref.write().await;

                  let mut payload_opt: Option<PoolBuffer> = None;

                  // Check if the message has an associated payload, and if so,
                  // read it from the connection.
                  if let Some(payload_info) = msg.payload_info() {
                    if payload_info.length > max_payload_size {
                      let err_message = Message::Error(ErrorParameters{id: payload_info.id, reason: PolicyViolation.into(), detail: Some("payload too large".into())});
                      let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
                      break 'connection_loop;
                    }

                    let mut pool_buff = {
                        match payload_buffer_pool.acquire(payload_info.length) {
                            Some(buffer) => buffer,
                            None => {
                                let _ = Self::write_message(&Message::Error(ErrorParameters{id: payload_info.id, reason: ServerOverloaded.into(), detail: Some("payload buffer pool exhausted".into())}), None, &mut writer, write_buffer).await;
                                continue 'connection_loop;
                            }
                        }
                    };

                    let payload = &mut pool_buff.as_mut_slice()[..payload_info.length];

                    match tokio::time::timeout(payload_read_timeout, Self::read_payload(payload, &mut stream_reader, payload_info.id)).await {
                      Ok(res) => {
                        match res {
                          Ok(_) => {
                            payload_opt = Some(pool_buff.freeze(payload_info.length));
                          },
                          Err(e) => {
                            let err_message: Message = {
                                if let Some(e) = e.downcast_ref::<zyn_protocol::Error>() {
                                    e.into()
                                } else {
                                    warn!(handler = handler, service_type = ST::NAME, "failed to read payload: {}", e);
                                    Message::Error(ErrorParameters{id: None, reason: InternalServerError.into(), detail: None})
                                }
                            };

                            let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
                            break 'connection_loop;
                          },
                        }
                      },
                      Err(_) => {
                        let err_message = Message::Error(ErrorParameters{id: None, reason: Timeout.into(), detail: Some("payload read timeout".into())});
                        let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
                        break 'connection_loop;
                      },
                    }
                  }

                  // Dispatch the message to the connection handler.
                  match conn_guard.dispatch_message::<ST>(msg, payload_opt).await {
                    Ok(_) => {},
                    Err(e) => {
                      warn!(handler = handler, service_type = ST::NAME, "failed to dispatch message: {}", e.to_string());
                      break 'connection_loop;
                    },
                  }
                },
                Err(e) => {
                  let err_detail = format!("{}", e);
                  let err_message = Message::Error(ErrorParameters{id: None, reason: BadRequest.into(), detail: Some(StringAtom::from(err_detail))});
                  let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
                  break 'connection_loop;
                },
              }
            },
            Ok(false) => {
              // Stream closed by the client.
              debug!(handler = handler, service_type = ST::NAME, "connection closed by peer");
              break 'connection_loop;
            }
            Err(e) => {
              match e {
                StreamReaderError::MaxLineLengthExceeded => {
                  let err_message = Message::Error(ErrorParameters{ id: None, reason: PolicyViolation.into(), detail: Some("max message size exceeded".into()) });
                  let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
                  break 'connection_loop;
                },
                StreamReaderError::IoError(e) => {
                  if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    error!(handler = handler, service_type = ST::NAME, "failed to read from connection: {}", e.to_string());
                  }
                  break 'connection_loop;
                },
              }
            }
          }
        },

        // Write the message to the stream.
        res = send_msg_rx.recv() => {
          let (msg, payload_opt) = res.unwrap();
          match Self::write_message(&msg, payload_opt, &mut writer, write_buffer).await {
            Ok(_) => {},
            Err(e) => {
              error!(handler = handler, service_type = ST::NAME, "{}", e.to_string());
              break 'connection_loop;
            },
          }
        },

        // Close the connection.
        res = close_rx.recv() => {
          let err_message = res.unwrap();
          let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
          info!(handler = handler, service_type = ST::NAME, "closed connection");
          break 'connection_loop;
        },

        // Close the connection on shutdown.
        _ = shutdown_token.cancelled() => {
          let err_message = Message::Error(ErrorParameters{id: None, reason: ServerShuttingDown.into(), detail: None});
          let _ = Self::write_message(&err_message, None, &mut writer, write_buffer).await;
          info!(handler = handler, service_type = ST::NAME, "closed connection");
          break 'connection_loop;
        },
      }
    }
    conn_ref.write().await.shutdown().await?;

    match writer.shutdown().await {
      Ok(_) => {},
      Err(e) => {
        // Ignore expected socket disconnection errors that occur when client disconnects abruptly
        use std::io::ErrorKind::*;
        match e.kind() {
          NotConnected | BrokenPipe | ConnectionAborted | ConnectionReset | UnexpectedEof => {},
          _ => return Err(e.into()),
        }
      },
    }

    Ok(())
  }

  async fn write_message<T>(
    message: &Message,
    payload_opt: Option<PoolBuffer>,
    writer: &mut T,
    write_buffer: &mut [u8],
  ) -> anyhow::Result<()>
  where
    T: AsyncWrite + Unpin,
  {
    let n = serialize(message, write_buffer).map_err(|e| anyhow!("failed to serialize message: {}", e))?;
    writer.write_all(&write_buffer[..n]).await?;

    if let Some(payload) = payload_opt {
      writer.write_all(payload.as_slice()).await?;
      let _ = writer.write(b"\n").await?;
    }

    writer.flush().await?;

    Ok(())
  }

  async fn read_payload<T>(
    buffer: &mut [u8],
    stream_reader: &mut StreamReader<ReadHalf<T>>,
    correlation_id: Option<u16>,
  ) -> anyhow::Result<()>
  where
    T: AsyncRead + Unpin,
  {
    stream_reader.read_raw(buffer).await?;

    // Read last byte, and ensure it's a newline.
    let mut cr: [u8; 1] = [0; 1];
    stream_reader.read_raw(&mut cr).await?;

    // Verify it's actually a newline
    if cr[0] != b'\n' {
      let mut error = zyn_protocol::Error::new(BadRequest).with_detail(StringAtom::from("invalid payload format"));
      if let Some(id) = correlation_id {
        error = error.with_id(id);
      }
      return Err(error.into());
    }

    Ok(())
  }
}

/// The connection transmitter.
#[derive(Clone, Debug)]
pub struct ConnTx {
  /// The send message channel.
  send_msg_tx: Sender<(Message, Option<PoolBuffer>)>,

  /// The connection close channel.
  close_tx: Sender<Message>,
}

// ===== impl ConnTx =====

impl ConnTx {
  /// Creates a new connection transmitter.
  ///
  /// # Arguments
  ///
  /// * `send_msg_tx` - The send message channel.
  /// * `close_tx` - The connection close channel.
  pub fn new(send_msg_tx: Sender<(Message, Option<PoolBuffer>)>, close_tx: Sender<Message>) -> Self {
    Self { send_msg_tx, close_tx }
  }

  /// Sends a message without a payload to the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to send.
  pub fn send_message(&self, message: Message) {
    self.send_message_with_payload(message, None);
  }

  /// Sends a message with an optional payload to the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to send.
  /// * `payload_opt` - An optional payload to send with the message.
  ///
  /// # Panics
  ///
  /// Panics if the message is not a `Message::Message` or `Message::S2mForwardPayloadAck`.
  pub fn send_message_with_payload(&self, message: Message, payload_opt: Option<PoolBuffer>) {
    assert!(
      payload_opt.is_none()
        || matches!(
          message,
          Message::Message { .. } | Message::ModDirect { .. } | Message::S2mForwardBroadcastPayloadAck { .. }
        ),
      "a Message::Message, Message::ModDirect or Message::S2mForwardBroadcastPayloadAck variant is expected when payload is present"
    );

    match self.send_msg_tx.try_send((message, payload_opt)) {
      Ok(_) => {},
      Err(_) => {
        // The send channel is full, so most likely the client is either not reading
        // or is too slow to process incoming messages. In this case, we close the connection.
        self.close(Message::Error(ErrorParameters { id: None, reason: MessageChannelIsFull.into(), detail: None }));
      },
    }
  }

  /// Closes the connection.
  ///
  /// # Arguments
  ///
  /// * `message` - The message to send to the connection before closing.
  ///
  /// # Panics
  ///
  /// Panics if the message is not a `Message::Error`.
  pub fn close(&self, message: Message) {
    assert!(matches!(message, Message::Error { .. }), "a Message::Error message is expected");
    let _ = self.close_tx.try_send(message);
  }
}
