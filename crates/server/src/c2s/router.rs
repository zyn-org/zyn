// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use dashmap::DashMap;

use zyn_util::pool::PoolBuffer;
use zyn_util::string_atom::StringAtom;

use crate::transmitter::Transmitter;

const DEFAULT_ROUTER_SHARD_COUNT: usize = 128;

struct Entry {
  /// The handler ID for this connection.
  pub handler: usize,

  /// The connection transmitter.
  pub transmitter: Arc<dyn Transmitter>,
}

// ===== impl Entry =====

impl Entry {
  /// Creates a new `Entry` with the specified handler ID and transmitter.
  pub fn new(handler: usize, transmitter: Arc<dyn Transmitter>) -> Self {
    Entry { handler, transmitter }
  }
}

#[derive(Clone)]
pub struct Router {
  /// The local domain for this router.
  local_domain: StringAtom,

  /// The connections map.
  connections: Arc<DashMap<StringAtom, Vec<Entry>>>,
}

// ===== impl Router =====

impl Router {
  /// Creates a new `Router` with the default shard count.
  ///
  /// # Arguments
  ///
  /// * `local_domain` - The local domain for this router
  ///
  /// # Returns
  ///
  /// A new `Router` instance with default capacity
  pub fn new(local_domain: StringAtom) -> Self {
    Self::new_with_shard_count(local_domain, DEFAULT_ROUTER_SHARD_COUNT)
  }

  /// Creates a new `Router` with a specified shard count.
  ///
  /// # Arguments
  ///
  /// * `local_domain` - The local domain for this router
  /// * `shard_count` - The shard count for the map
  ///
  /// # Returns
  ///
  /// A new `Router` instance with the specified capacity
  pub fn new_with_shard_count(local_domain: StringAtom, shard_count: usize) -> Self {
    Self { local_domain, connections: Arc::new(DashMap::with_shard_amount(shard_count)) }
  }

  /// Routes a message to a single target.
  ///
  /// # Arguments
  ///
  /// * `msg` - The message to route
  /// * `payload_opt` - Optional payload buffer to include with the message
  /// * `target` - The target username to route the message to
  /// * `excluding_local_handler` - Optional handler ID to exclude from routing
  ///
  /// # Returns
  ///
  /// `Ok(())` if the routing was successful, or an error if routing failed
  pub fn route_to(
    &self,
    msg: zyn_protocol::Message,
    payload_opt: Option<PoolBuffer>,
    target: StringAtom,
    excluding_local_handler: Option<usize>,
  ) -> anyhow::Result<()> {
    if let Some(entries) = self.connections.get(&target) {
      for entry in entries.iter() {
        if Some(entry.handler) == excluding_local_handler {
          continue;
        }
        entry.transmitter.send_message_with_payload(msg.clone(), payload_opt.clone());
      }
    }

    Ok(())
  }

  /// Registers a connection for a specific username.
  ///
  /// # Arguments
  ///
  /// * `username` - The username to register the connection for
  /// * `transmitter` - The connection transmitter for sending messages
  /// * `handler` - The handler ID for this connection
  /// * `exclusive` - If true, registration fails if connections already exist for this username
  ///
  /// # Returns
  ///
  /// `true` if the connection was successfully registered, `false` if exclusive was requested
  /// and connections already exist
  pub fn register_connection(
    &self,
    username: StringAtom,
    transmitter: Arc<dyn Transmitter>,
    handler: usize,
    exclusive: bool,
  ) -> bool {
    let mut entry = self.connections.entry(username).or_default();

    if exclusive && !entry.is_empty() {
      return false;
    }

    entry.push(Entry::new(handler, transmitter));
    true
  }

  /// Unregisters a connection for a specific username and handler.
  ///
  /// If the connection count for the username reaches zero after unregistering,
  /// the provided closure will be executed.
  ///
  /// # Arguments
  ///
  /// * `username` - The username to unregister the connection for
  /// * `handler` - The handler ID of the connection to remove
  /// * `cleanup` - An async closure to execute when connection count reaches zero
  ///
  /// # Returns
  ///
  /// Returns the result of the closure if executed, or `Ok(())` if not executed
  pub async fn unregister_connection<F, Fut, E>(
    &self,
    username: &StringAtom,
    handler: usize,
    cleanup: F,
  ) -> Result<(), E>
  where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
  {
    self.connections.entry(username.clone()).and_modify(|entries| {
      entries.retain(|entry| entry.handler != handler);
    });

    // Remove the entry if it's empty
    let was_removed = self.connections.remove_if(username, |_, entries| entries.is_empty());

    if was_removed.is_some() { cleanup().await } else { Ok(()) }
  }

  /// Checks if there are any connections registered for a given username.
  ///
  /// # Arguments
  ///
  /// * `username` - The username to check for connections
  ///
  /// # Returns
  ///
  /// `true` if at least one connection exists for the username, `false` otherwise
  pub fn has_connection(&self, username: &StringAtom) -> bool {
    self.connections.get(username).is_some_and(|entries| !entries.is_empty())
  }

  /// Returns the total number of connections.
  ///
  /// # Returns
  ///
  /// The total count of registered connections
  #[allow(clippy::len_without_is_empty)]
  pub fn len(&self) -> usize {
    self.connections.iter().map(|entry| entry.value().len()).sum()
  }

  /// Returns the local domain of this router.
  ///
  /// # Returns
  ///
  /// A clone of the local domain `StringAtom`
  pub fn local_domain(&self) -> StringAtom {
    self.local_domain.clone()
  }
}

impl std::fmt::Debug for Router {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Router").field("local_domain", &self.local_domain).field("total_connections", &self.len()).finish()
  }
}
