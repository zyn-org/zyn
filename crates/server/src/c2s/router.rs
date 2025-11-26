// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock as PlRwLock;

use zyn_util::pool::PoolBuffer;
use zyn_util::string_atom::StringAtom;

use crate::transmitter::Transmitter;

const DEFAULT_ROUTER_SHARD_COUNT: usize = 64;

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

type Shard = Arc<PlRwLock<HashMap<StringAtom, Vec<Entry>>>>;

#[derive(Clone)]
pub struct Router {
  local_domain: StringAtom,

  shards: Arc<[Shard]>,
}

// ===== impl Router =====

impl Router {
  /// Creates a new `Router` with the default number of shards.
  ///
  /// # Arguments
  ///
  /// * `local_domain` - The local domain for this router
  ///
  /// # Returns
  ///
  /// A new `Router` instance with default shard count
  pub fn new(local_domain: StringAtom) -> Self {
    Self::new_with_shard_count(local_domain, DEFAULT_ROUTER_SHARD_COUNT)
  }

  /// Creates a new `Router` with a specified number of shards.
  ///
  /// # Arguments
  ///
  /// * `local_domain` - The local domain for this router
  /// * `num_shards` - The number of shards to use for distributing connections
  ///
  /// # Returns
  ///
  /// A new `Router` instance with the specified shard count
  pub fn new_with_shard_count(local_domain: StringAtom, num_shards: usize) -> Self {
    let mut shards = Vec::with_capacity(num_shards);
    for _ in 0..num_shards {
      shards.push(Arc::new(PlRwLock::new(HashMap::new())));
    }
    Self { local_domain, shards: Arc::from(shards) }
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
    let shard = self.get_shard(&target);
    let shard_guard = shard.read();

    if let Some(entries) = shard_guard.get(&target) {
      for entry in entries {
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
    let shard = self.get_shard(&username);
    let mut shard_guard = shard.write();

    let entry = shard_guard.entry(username).or_default();

    if exclusive && !entry.is_empty() {
      return false;
    }

    entry.push(Entry::new(handler, transmitter));
    true
  }

  /// Unregisters a connection for a specific username and handler.
  ///
  /// # Arguments
  ///
  /// * `username` - The username to unregister the connection for
  /// * `handler` - The handler ID of the connection to remove
  pub fn unregister_connection(&self, username: &StringAtom, handler: usize) {
    let shard = self.get_shard(username);
    let mut shard_guard = shard.write();

    if let Some(txs) = shard_guard.get_mut(username) {
      txs.retain(|entry| entry.handler != handler);
      if txs.is_empty() {
        shard_guard.remove(username);
      }
    }
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
    let shard = self.get_shard(username);
    let shard_guard = shard.read();
    shard_guard.get(username).is_some_and(|entries| !entries.is_empty())
  }

  /// Returns the total number of connections across all shards.
  ///
  /// # Returns
  ///
  /// The total count of registered connections
  #[allow(clippy::len_without_is_empty)]
  pub fn len(&self) -> usize {
    self.shards.iter().map(|shard| shard.read().values().map(|txs| txs.len()).sum::<usize>()).sum()
  }

  /// Returns the local domain of this router.
  ///
  /// # Returns
  ///
  /// A clone of the local domain `StringAtom`
  pub fn local_domain(&self) -> StringAtom {
    self.local_domain.clone()
  }

  fn shard_index(&self, username: &StringAtom) -> usize {
    let mut hasher = DefaultHasher::new();
    username.hash(&mut hasher);
    (hasher.finish() as usize) % self.shards.len()
  }

  fn get_shard(&self, username: &StringAtom) -> &Arc<PlRwLock<HashMap<StringAtom, Vec<Entry>>>> {
    let index = self.shard_index(username);
    &self.shards[index]
  }
}

impl std::fmt::Debug for Router {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Router").field("local_domain", &self.local_domain).field("total_connections", &self.len()).finish()
  }
}
