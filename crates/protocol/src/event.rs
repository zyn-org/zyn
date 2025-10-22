// SPDX-License-Identifier: AGPL-3.0

use std::str::FromStr;

use crate::{EventParameters, Message};
use zyn_util::string_atom::StringAtom;

/// Represents the type of event that occurred in the system.
///
/// `EventKind` is used to categorize different types of events that can occur
/// within channels or the broader system. Each variant corresponds to a specific
/// type of activity or state change.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EventKind {
  /// Indicates that a member has joined a channel.
  ///
  /// This event is triggered when a user successfully joins a channel,
  /// either through direct action or being added by another member with
  /// appropriate permissions.
  MemberJoined,

  /// Indicates that a member has left a channel.
  ///
  /// This event is triggered when a user leaves a channel, either voluntarily
  /// or due to being removed by another member with appropriate permissions.
  MemberLeft,
}

impl std::fmt::Display for EventKind {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let s: &str = (*self).into();
    write!(f, "{}", s)
  }
}

impl From<EventKind> for StringAtom {
  fn from(val: EventKind) -> Self {
    let s: &str = val.into();
    StringAtom::from(s)
  }
}

impl From<EventKind> for &str {
  fn from(val: EventKind) -> Self {
    match val {
      EventKind::MemberJoined => "MEMBER_JOINED",
      EventKind::MemberLeft => "MEMBER_LEFT",
    }
  }
}

impl TryFrom<StringAtom> for EventKind {
  type Error = anyhow::Error;

  fn try_from(atom: StringAtom) -> Result<Self, Self::Error> {
    EventKind::from_str(atom.as_ref())
  }
}

impl FromStr for EventKind {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "MEMBER_JOINED" => Ok(EventKind::MemberJoined),
      "MEMBER_LEFT" => Ok(EventKind::MemberLeft),
      _ => anyhow::bail!("unknown event kind: {}", s),
    }
  }
}

/// Represents an event that can be processed by a modulator.
///
/// Events are used to communicate information about various activities or state changes
/// that occur within the system. Each event contains metadata about the event type,
/// associated entities, and contextual information.
#[derive(Clone, Debug)]
pub struct Event {
  /// The event kind.
  pub kind: EventKind,

  /// The channel associated with the event, if any.
  pub channel: Option<StringAtom>,

  /// The zid associated with the event, if any.
  pub zid: Option<StringAtom>,

  /// Tells whether the zid is the channel owner.
  pub owner: Option<bool>,
}

impl Event {
  /// Creates a new event with the given kind.
  pub fn new(kind: EventKind) -> Self {
    Event { kind, channel: None, zid: None, owner: None }
  }

  /// Sets the channel for the event.
  pub fn with_channel(mut self, channel: StringAtom) -> Self {
    self.channel = Some(channel);
    self
  }

  /// Sets the zid for the event.
  pub fn with_zid(mut self, zid: StringAtom) -> Self {
    self.zid = Some(zid);
    self
  }

  /// Sets the owner flag for the event.
  pub fn with_owner(mut self, owner: bool) -> Self {
    self.owner = Some(owner);
    self
  }
}

impl From<Event> for Message {
  fn from(val: Event) -> Self {
    Message::Event(EventParameters {
      kind: val.kind.into(),
      channel: val.channel.clone(),
      zid: val.zid.clone(),
      owner: val.owner,
    })
  }
}
