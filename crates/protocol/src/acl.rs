// SPDX-License-Identifier: BSD-3-Clause

use std::fmt;
use std::str::FromStr;

/// Represents the type of ACL (Access Control List).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclType {
  /// Join ACL - controls who can join the channel
  Join,
  /// Publish ACL - controls who can publish to the channel
  Publish,
  /// Read ACL - controls who can read from the channel
  Read,
}

impl FromStr for AclType {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "join" => Ok(AclType::Join),
      "publish" => Ok(AclType::Publish),
      "read" => Ok(AclType::Read),
      _ => anyhow::bail!("invalid ACL type: {}", s),
    }
  }
}

impl fmt::Display for AclType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      AclType::Join => write!(f, "join"),
      AclType::Publish => write!(f, "publish"),
      AclType::Read => write!(f, "read"),
    }
  }
}

impl AclType {
  /// Returns the string representation of the ACL type.
  pub fn as_str(&self) -> &'static str {
    match self {
      AclType::Join => "join",
      AclType::Publish => "publish",
      AclType::Read => "read",
    }
  }
}

/// Represents the action to perform on an ACL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclAction {
  /// Add entries to the ACL
  Add,
  /// Remove entries from the ACL
  Remove,
}

impl FromStr for AclAction {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "add" => Ok(AclAction::Add),
      "remove" => Ok(AclAction::Remove),
      _ => anyhow::bail!("invalid ACL action: {}", s),
    }
  }
}

impl fmt::Display for AclAction {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      AclAction::Add => write!(f, "add"),
      AclAction::Remove => write!(f, "remove"),
    }
  }
}

impl AclAction {
  /// Returns the string representation of the ACL action.
  pub fn as_str(&self) -> &'static str {
    match self {
      AclAction::Add => "add",
      AclAction::Remove => "remove",
    }
  }
}
