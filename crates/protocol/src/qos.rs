// SPDX-License-Identifier: AGPL-3.0-only

use std::fmt;

/// Quality of Service level for message acknowledgment.
#[repr(u8)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QoS {
  /// Acknowledge as soon as the payload is received
  AckOnReceive = 0,

  /// Acknowledge when the payload has been enqueued for delivery to all channel members
  #[default]
  AckOnRouted = 1,
}

impl QoS {
  /// Returns the raw u8 value
  pub const fn as_u8(self) -> u8 {
    self as u8
  }

  /// Creates a QoS from a u8 value.
  /// Returns None if the value is not valid.
  pub const fn from_u8(value: u8) -> Option<Self> {
    match value {
      0 => Some(QoS::AckOnReceive),
      1 => Some(QoS::AckOnRouted),
      _ => None,
    }
  }
}

impl From<QoS> for u8 {
  fn from(qos: QoS) -> u8 {
    qos as u8
  }
}

impl TryFrom<u8> for QoS {
  type Error = InvalidQoSError;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    QoS::from_u8(value).ok_or(InvalidQoSError(value))
  }
}

impl fmt::Display for QoS {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      QoS::AckOnReceive => write!(f, "AckOnReceive"),
      QoS::AckOnRouted => write!(f, "AckOnRouted"),
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidQoSError(u8);

impl fmt::Display for InvalidQoSError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "invalid QoS value: {} (must be 0 or 1)", self.0)
  }
}

impl std::error::Error for InvalidQoSError {}
