// SPDX-License-Identifier: BSD-3-Clause

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::RwLock;

use narwhal_protocol::ErrorReason::{
  BadRequest, ChannelIsFull, ChannelNotFound, Forbidden, NotAllowed, NotImplemented, PolicyViolation, UserInChannel,
  UserNotInChannel, UserNotRegistered,
};
use narwhal_protocol::{
  BroadcastAckParameters, ChannelAclParameters, ChannelConfigurationParameters, JoinChannelAckParameters,
  LeaveChannelAckParameters, ListChannelsAckParameters, ListMembersAckParameters, Message, MessageParameters, QoS,
};
use narwhal_protocol::{ChannelId, Nid};
use narwhal_protocol::{Event, EventKind};
use narwhal_util::pool::PoolBuffer;
use narwhal_util::string_atom::StringAtom;

use crate::notifier::Notifier;
use crate::router::GlobalRouter;
use crate::transmitter::{Resource, Transmitter};

const DASH_MAP_SHARD_COUNT: usize = 1024;

/// The channel manager inner state.
#[derive(Debug)]
struct ChannelManagerInner {
  /// The channels.
  channels: Arc<DashMap<StringAtom, Channel>>,

  /// The channels a user is a member of.
  in_channels: Arc<DashMap<StringAtom, HashSet<ChannelId>>>,

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
    let channels_map = DashMap::with_capacity_and_shard_amount(max_channels as usize, DASH_MAP_SHARD_COUNT);
    let in_channels = DashMap::with_shard_amount(DASH_MAP_SHARD_COUNT);

    let inner = ChannelManagerInner {
      router,
      notifier,
      channels: Arc::new(channels_map),
      in_channels: Arc::new(in_channels),
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
  /// * `nid` - The user identifier
  /// * `as_owner` - If true, only lists channels where the user is the owner
  /// * `transmitter` - The transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn list_channels(
    &self,
    nid: Nid,
    as_owner: bool,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let channels = mng_guard.channels.clone();
    let in_channels = mng_guard.in_channels.clone();
    drop(mng_guard);

    // Gather all channels the user is a member of and sort them.
    let mut channel_list: Vec<StringAtom> = Default::default();

    if let Some(in_channels_set) = in_channels.get(&nid.username) {
      for channel_id in in_channels_set.iter() {
        if as_owner {
          match channels.get(&channel_id.handler) {
            Some(channel) => {
              if channel.0.read().await.is_owner(&nid) {
                channel_list.push(channel_id.into());
              }
            },
            None => continue,
          }
        } else {
          channel_list.push(channel_id.into());
        }
      }
    }

    channel_list.sort();

    // Send response back to the client.
    transmitter
      .send_message(Message::ListChannelsAck(ListChannelsAckParameters { id: correlation_id, channels: channel_list }));

    Ok(())
  }

  /// Lists all members of a specific channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier
  /// * `nid` - The user identifier requesting the list
  /// * `transmitter` - The transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn list_members(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(narwhal_protocol::Error::new(NotImplemented).into());
    }

    // Check if the channel exists and if the originating connection is a member of it.
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let channel_inner = channel.0.read().await;

    if !channel_inner.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }
    // Gather all members of the channel and sort them.
    let mut members: Vec<StringAtom> = channel_inner.members.iter().map(|member_nid| member_nid.into()).collect();
    drop(channel_inner);

    members.sort();

    // Send response back to the client.
    transmitter.send_message(Message::ListMembersAck(ListMembersAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
      members,
    }));

    Ok(())
  }

  /// Joins a channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier to join
  /// * `nid` - The user identifier joining the channel
  /// * `on_behalf_nid` - Optional user identifier to join on behalf of
  /// * `transmitter` - The connection transmitter for the client
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result containing a boolean that is `true` if the user joined as the channel owner
  /// (i.e., the channel was created), or `false` if joining an existing channel
  pub async fn join_channel(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    oh_behalf_nid: Option<Nid>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<bool> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();

    let notifier = mng_guard.notifier.clone();
    let in_channels = mng_guard.in_channels.clone();

    let max_channels_per_client = mng_guard.max_channels_per_client;
    let max_clients_per_channel = mng_guard.max_clients_per_channel;
    let max_payload_size = mng_guard.max_payload_size;

    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }
    let handler = channel_id.handler.clone();

    // Check if the channel exists, and create it if it doesn't
    let mut as_owner = false;

    let entry_ref = channels.entry(handler.clone()).or_insert_with(|| {
      let channel_inner = ChannelInner {
        handler: handler.clone(),
        owner: None,
        config: ChannelConfig { max_clients: max_clients_per_channel, max_payload_size },
        acl: ChannelAcl::default(),
        members: HashSet::new(),
        allowed_targets: Arc::default(),
        notifier,
      };

      as_owner = true;

      Channel(Arc::new(RwLock::new(channel_inner)))
    });
    let channel = entry_ref.value().clone();

    let mut channel_inner = channel.0.write().await;

    // Get the NID of the new member.
    let new_member_nid = {
      match oh_behalf_nid {
        Some(oh_behalf_nid) => {
          // Ensure the client is authorized to join the channel on behalf of another user.
          if !channel_inner.is_owner(&nid) {
            return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
          }

          if !router.c2s_router().has_connection(&oh_behalf_nid.username) {
            return Err(narwhal_protocol::Error::new(UserNotRegistered).with_id(correlation_id).into());
          }

          oh_behalf_nid.clone()
        },
        None => nid.clone(),
      }
    };
    // Check if the new member is allowed to join the channel.
    let acl = &channel_inner.acl;

    if !acl.is_join_allowed(&new_member_nid) {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Insert the member into the channel in case it is not already a member
    // and the channel is not full, and notify all members about the new member.
    let config = &channel_inner.config;

    if channel_inner.is_member(&new_member_nid) {
      return Err(narwhal_protocol::Error::new(UserInChannel).with_id(correlation_id).into());
    } else if channel_inner.member_count() >= config.max_clients as usize {
      return Err(narwhal_protocol::Error::new(ChannelIsFull).with_id(correlation_id).into());
    }
    // Check if the maximum number of subscriptions is reached.
    if let Some(in_channels) = in_channels.get(&new_member_nid.username)
      && in_channels.len() >= max_channels_per_client as usize
    {
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("subscription limit reached")
          .into(),
      );
    }

    channel_inner.insert_member(new_member_nid.clone());

    channel_inner
      .notify_member_joined(
        &new_member_nid,
        Some(transmitter.resource()),
        as_owner,
        router.c2s_router().local_domain().clone(),
      )
      .await?;
    drop(channel_inner);

    // Update the list of channels the connection is a member of.
    in_channels.entry(new_member_nid.username.clone()).or_default().insert(channel_id.clone());

    // Send response back to the client.
    transmitter.send_message(Message::JoinChannelAck(JoinChannelAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
    }));

    Ok(as_owner)
  }

  /// Leaves a channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier to leave
  /// * `nid` - The user identifier leaving the channel
  /// * `on_behalf_nid` - Optional user identifier to leave on behalf of
  /// * `transmitter` - Optional connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn leave_channel(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Option<Arc<dyn Transmitter>>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;

    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    let in_channels = mng_guard.in_channels.clone();

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }
    // Check if the channel exists
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let mut channel_inner = channel.0.write().await;

    // Re-verify channel is still in the map after acquiring lock
    // (another thread might have removed it while we were waiting)
    if channels.get(&channel_id.handler).is_none() {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    }

    let left_member_nid = {
      match on_behalf_nid {
        Some(z) => {
          if !channel_inner.is_owner(&nid) {
            return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
          }
          z
        },
        None => nid.clone(),
      }
    };

    if !channel_inner.is_member(&left_member_nid) {
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    // Notify members about the left member, and remove the member from the channel.
    let as_owner = channel_inner.is_owner(&left_member_nid);

    let resource = transmitter.as_ref().map(|transmitter| transmitter.resource());

    channel_inner
      .notify_member_left(&left_member_nid, resource, as_owner, router.c2s_router().local_domain().clone())
      .await?;

    channel_inner.remove_member(&left_member_nid);

    // Update the list of channels the connection is a member of.
    in_channels.remove_if_mut(&left_member_nid.username, |_, in_channels_set| {
      in_channels_set.remove(&channel_id);
      in_channels_set.is_empty()
    });

    // Send response back to the client if a handler is provided.
    if let Some(transmitter) = transmitter {
      transmitter.send_message(Message::LeaveChannelAck(LeaveChannelAckParameters { id: correlation_id }));
    }

    // If the channel is now empty, release it.
    if channel_inner.is_empty() {
      drop(channel_inner);

      // Atomically remove only if the channel is still empty.
      // We use try_read() to avoid blocking. If another thread is currently
      // joining the channel (holding write lock), try_read() fails and we
      // return false, keeping the channel.
      channels.remove_if(&channel_id.handler, |_, channel| {
        if let Ok(inner) = channel.0.try_read() { inner.is_empty() } else { false }
      });

      return Ok(());
    }

    // If the left member was the owner, pick a new owner (randomly) and notify all members.
    if as_owner {
      let new_owner_nid = channel_inner.pick_new_owner().unwrap();

      channel_inner
        .notify_member_joined(&new_owner_nid, None, true, router.c2s_router().local_domain().clone())
        .await?;
    }

    Ok(())
  }

  /// Leaves all channels that a user is a member of.
  ///
  /// # Arguments
  ///
  /// * `nid` - The user identifier leaving all channels
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn leave_all_channels(&mut self, nid: Nid) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let in_channels = mng_guard.in_channels.clone();
    drop(mng_guard);

    if let Some((_, in_channels_set)) = in_channels.remove(&nid.username) {
      for channel_id in in_channels_set.iter() {
        self.leave_channel(channel_id.clone(), nid.clone(), None, None, 0).await?;
      }
    }
    Ok(())
  }

  /// Gets the access control list (ACL) for a channel.
  ///
  /// # Arguments
  ///
  /// * `channel_id` - The channel identifier
  /// * `nid` - The user identifier requesting the ACL
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn get_channel_acl(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Check if the channel exists
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let channel_inner = channel.0.read().await;

    // Only owner is allowed to get the channel ACL.
    if !channel_inner.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Obtain the channel's ACL.
    let acl = &channel_inner.acl;

    let allow_join: Vec<StringAtom> = acl.join_acl.allow_list().iter().map(|z| z.into()).collect();
    let allow_publish: Vec<StringAtom> = acl.publish_acl.allow_list().iter().map(|z| z.into()).collect();
    let allow_read: Vec<StringAtom> = acl.read_acl.allow_list().iter().map(|z| z.into()).collect();

    drop(channel_inner);

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
  /// * `nid` - The user identifier setting the ACL
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
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    // Check if the channel exists
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let mut channel_inner = channel.0.write().await;

    // Only the owner of the channel can change its ACL.
    if !channel_inner.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Validate the new ACL.
    let channel_max_clients = channel_inner.config.max_clients as usize;

    let is_valid_acl = {
      let is_valid_join_list = acl.join_acl.allow_list().len() <= channel_max_clients;
      let is_valid_publish_list = acl.publish_acl.allow_list().len() <= channel_max_clients;
      let is_valid_read_list = acl.read_acl.allow_list().len() <= channel_max_clients;

      is_valid_join_list && is_valid_publish_list && is_valid_read_list
    };

    if !is_valid_acl {
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("ACL allow list exceeds max entries")
          .into(),
      );
    }

    // Update the channel ACL.
    channel_inner.set_acl(acl.clone());
    drop(channel_inner);

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
  /// * `nid` - The user identifier requesting the configuration
  /// * `transmitter` - The connection transmitter for sending the response
  /// * `correlation_id` - The correlation ID for the request
  ///
  /// # Returns
  ///
  /// A result indicating success or failure
  pub async fn get_channel_configuration(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let channels = mng_guard.channels.clone();
    drop(mng_guard);

    // Check if the channel exists
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let channel_inner = channel.0.read().await;

    // Only members of the channel can get its configuration.
    if !channel_inner.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Obtain the channel configuration.
    let config = channel_inner.config.clone();
    drop(channel_inner);

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
  /// * `nid` - The user identifier setting the configuration
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
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let mng_guard = self.0.read().await;
    let router = mng_guard.router.clone();
    let channels = mng_guard.channels.clone();
    let max_clients_per_channel = mng_guard.max_clients_per_channel;
    let max_payload_size = mng_guard.max_payload_size;
    drop(mng_guard);

    // Ensure the channel is local.
    if channel_id.domain != router.c2s_router().local_domain() {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }
    // Validate the new channel configuration
    if config.max_clients > max_clients_per_channel {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_clients exceeds server established limit")
          .into(),
      );
    }

    if config.max_payload_size > max_payload_size {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_payload_size exceeds server established limit")
          .into(),
      );
    }

    // Check if the channel exists
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let mut channel_inner = channel.0.write().await;

    // Only the owner of the channel can change its configuration.
    if !channel_inner.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }
    // Merge the current configuration with the new one.
    let new_config = channel_inner.merge_config(&config);

    drop(channel_inner);

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
  /// * `nid` - The user identifier broadcasting the payload
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
    nid: Nid,
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
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    // Check if the channel exists.
    let channel = {
      match channels.get(&channel_id.handler) {
        Some(kv) => {
          let channel = kv.value();
          (*channel).clone()
        },
        None => return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into()),
      }
    };
    let channel_inner = channel.0.read().await;

    if !channel_inner.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }
    // Check if the member is allowed to publish to the channel.
    let acl = &channel_inner.acl;

    if !acl.is_publish_allowed(&nid) {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }
    let max_payload_size = channel_inner.config.max_payload_size;

    let allowed_targets = channel_inner.allowed_targets.clone();
    drop(channel_inner);

    // Validate channel established payload size limit.
    let payload_length = payload.as_slice().len() as u32;

    if payload_length > max_payload_size {
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("payload size exceeds channel limit")
          .into(),
      );
    }
    let qos = qos.map(QoS::try_from).transpose()?.unwrap_or(QoS::default());

    if qos == QoS::AckOnReceived {
      transmitter.send_message(Message::BroadcastAck(BroadcastAckParameters { id: correlation_id }));
    }

    // Broadcast the payload to all members of the channel, except the sender.
    let msg = Message::Message(MessageParameters {
      from: (&nid).into(),
      channel: (&channel_id).into(),
      length: payload_length,
    });

    router.route_to_many(msg, Some(payload), allowed_targets.iter(), Some(transmitter.resource())).await?;

    if qos == QoS::AckOnDelivered {
      transmitter.send_message(Message::BroadcastAck(BroadcastAckParameters { id: correlation_id }));
    }

    Ok(())
  }
}

/// A channel.
#[derive(Clone, Debug)]
pub struct Channel(Arc<RwLock<ChannelInner>>);

/// The inner state of a channel.
#[derive(Debug)]
pub struct ChannelInner {
  /// The channel handler.
  handler: StringAtom,

  /// The owner of the channel.
  owner: Option<Nid>,

  /// The channel configuration.
  config: ChannelConfig,

  /// The channel ACL.
  acl: ChannelAcl,

  /// The members of the channel (including the owner).
  members: HashSet<Nid>,

  /// The channel allowed targets for broadcasting.
  allowed_targets: Arc<[Nid]>,

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
  fn is_owner(&self, nid: &Nid) -> bool {
    self.owner == Some(nid.clone())
  }

  /// Picks a new owner for the channel.
  fn pick_new_owner(&mut self) -> Option<Nid> {
    if self.is_empty() {
      return None;
    }

    let new_owner_nid = self.members.iter().next().unwrap().clone();

    // Update owner handler.
    self.owner = Some(new_owner_nid.clone());

    Some(new_owner_nid)
  }

  /// Checks if the channel has a certain member.
  fn is_member(&self, nid: &Nid) -> bool {
    self.members.contains(nid)
  }

  /// Returns the number of members in the channel.
  fn member_count(&self) -> usize {
    self.members.len()
  }

  /// Inserts a member into the channel.
  fn insert_member(&mut self, nid: Nid) {
    if self.owner.is_none() {
      self.owner = Some(nid.clone());
    }
    self.members.insert(nid);
    self.update_allowed_targets();
  }

  /// Removes a member from the channel.
  fn remove_member(&mut self, nid: &Nid) -> bool {
    if self.owner == Some(nid.clone()) {
      self.owner = None;
    }
    let removed = self.members.remove(nid);
    self.update_allowed_targets();
    removed
  }

  /// Sets the ACL for the channel.
  fn set_acl(&mut self, acl: ChannelAcl) {
    self.acl = acl;
    self.update_allowed_targets();
  }

  /// Merges the configuration for the channel.
  fn merge_config(&mut self, config: &ChannelConfig) -> ChannelConfig {
    self.config = self.config.merge(config);
    self.config.clone()
  }

  /// Updates the allowed targets for broadcasting.
  fn update_allowed_targets(&mut self) {
    let acl = &self.acl;
    let targets: Vec<Nid> = self.members.iter().filter(|m| acl.is_read_allowed(m)).cloned().collect();
    self.allowed_targets = Arc::from(targets);
  }

  /// Notifies all members that a new member has joined the channel.
  async fn notify_member_joined(
    &self,
    nid: &Nid,
    excluding_resource: Option<Resource>,
    as_owner: bool,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler.clone(), local_domain);

    let event =
      Event::new(EventKind::MemberJoined).with_channel(channel_id.into()).with_nid(nid.into()).with_owner(as_owner);

    self.notifier.notify(event, self.members.iter(), excluding_resource).await?;

    Ok(())
  }

  /// Notifies all members that a member has left the channel.
  async fn notify_member_left(
    &self,
    nid: &Nid,
    excluding_resource: Option<Resource>,
    as_owner: bool,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler.clone(), local_domain);

    let event =
      Event::new(EventKind::MemberLeft).with_channel(channel_id.into()).with_nid(nid.into()).with_owner(as_owner);

    self.notifier.notify(event, self.members.iter(), excluding_resource).await?;

    Ok(())
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
  fn new(allow_list: Vec<Nid>) -> Acl {
    let mut allow_lists: HashMap<StringAtom, HashSet<StringAtom>> = HashMap::new();

    for nid in allow_list {
      let domain = nid.domain.clone();
      let username = nid.username.clone();

      // Get or create the HashSet for this domain
      let domain_users = allow_lists.entry(domain).or_default();

      // Only add non-server users
      if !nid.is_server() {
        domain_users.insert(username);
      }
    }
    Acl { allow_lists }
  }

  /// Checks if a NID is allowed based on the ACL.
  fn is_allowed(&self, nid: &Nid) -> bool {
    // If ACL map is empty, access is allowed by default
    if self.allow_lists.is_empty() {
      return true;
    }
    let domain = nid.domain.clone();
    let username = nid.username.clone();

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
  pub fn allow_list(&self) -> Vec<Nid> {
    let mut allow_list: Vec<Nid> = Vec::with_capacity(self.allow_lists.len());

    for (domain, allowed_users) in self.allow_lists.iter() {
      if !allowed_users.is_empty() {
        for username in allowed_users.iter() {
          allow_list.push(Nid::new_unchecked(username.clone(), domain.clone()));
        }
      } else {
        allow_list.push(Nid::new_unchecked(StringAtom::default(), domain.clone()));
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
  pub fn new(allow_join_list: Vec<Nid>, allow_publish_list: Vec<Nid>, allow_read_list: Vec<Nid>) -> ChannelAcl {
    // Return the new ChannelACL instance
    ChannelAcl {
      join_acl: Acl::new(allow_join_list),
      publish_acl: Acl::new(allow_publish_list),
      read_acl: Acl::new(allow_read_list),
    }
  }

  /// Checks if a NID is allowed to join to the channel.
  pub fn is_join_allowed(&self, nid: &Nid) -> bool {
    self.join_acl.is_allowed(nid)
  }

  /// Checks if a NID is allowed to publish to the channel.
  pub fn is_publish_allowed(&self, nid: &Nid) -> bool {
    self.publish_acl.is_allowed(nid)
  }

  /// Checks if a NID is allowed to read from the channel.
  pub fn is_read_allowed(&self, nid: &Nid) -> bool {
    self.read_acl.is_allowed(nid)
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
