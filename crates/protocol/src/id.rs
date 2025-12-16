// SPDX-License-Identifier: AGPL-3.0-only

use std::fmt::Display;
use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::Regex;

use narwhal_util::string_atom::StringAtom;

/// The maximum length of a username.
const USERNAME_MAX_LENGTH: usize = 256;

const LOCALHOST_DOMAIN: &str = "localhost";

/// The regex used to validate the domain.
///
/// This matches:
/// - Standard domain names (e.g., example.com, sub.example.com)
/// - IPv4 addresses (e.g., 192.168.1.1)
/// - IPv6 addresses in square brackets (e.g., [2001:db8::1])
/// - Optional port numbers (e.g., example.com:8080)
static DOMAIN_REGEX: Lazy<Regex> = Lazy::new(|| {
  Regex::new(
    r"^(?:(?:[a-zA-Z0-9-]{1,63}\.)+[a-zA-Z]{2,63}|(?:\[[0-9a-fA-F:]+\])|(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}))(?::\d{1,5})?$"
  ).unwrap()
});

const DOMAIN_MAX_LENGTH: usize = 253;

/// Validates a domain.
fn validate_domain(domain: &str) -> bool {
  if domain.is_empty() || domain.len() > DOMAIN_MAX_LENGTH {
    return false;
  }
  domain == LOCALHOST_DOMAIN || DOMAIN_REGEX.is_match(domain)
}

/// The error type for channel ID parsing.
#[derive(Clone, Debug, PartialEq)]
pub enum ChannelIdParsingError {
  /// The channel ID has an invalid format.
  InvalidChannelIdFormat,
}

// ===== impl ChannelIdParsingError =====

impl Display for ChannelIdParsingError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::InvalidChannelIdFormat => write!(f, "invalid channel ID format"),
    }
  }
}

impl std::error::Error for ChannelIdParsingError {}

/// The error type for ZID parsing.
#[derive(Clone, Debug, PartialEq)]
pub enum ZidParsingError {
  /// The ZID has an invalid format.
  InvalidZidFormat,
}

// ===== impl ZidParsingError =====

impl Display for ZidParsingError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::InvalidZidFormat => write!(f, "invalid ZID format"),
    }
  }
}

impl std::error::Error for ZidParsingError {}

/// The channel ID used by the client/server to identify a channel.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ChannelId {
  /// The channel handler.
  pub handler: u32,

  /// The domain of the channel.
  pub domain: StringAtom,

  /// The full representation of the channel ID.
  full: StringAtom,
}

// ===== impl ChannelId =====

impl ChannelId {
  /// Creates a new channel ID.
  pub fn new(handler: u32, domain: StringAtom) -> Result<Self, ChannelIdParsingError> {
    let domain_ref = domain.as_ref();

    if !Self::validate(handler, domain_ref) {
      return Err(ChannelIdParsingError::InvalidChannelIdFormat);
    }
    Ok(Self { handler, domain: domain.clone(), full: Self::full_atom(handler, domain) })
  }

  /// Creates a new channel ID without validation.
  ///
  /// # Safety
  ///
  /// This method skips validation checks on the channel and domain.
  /// The caller must ensure that the provided values are valid.
  pub fn new_unchecked(handler: u32, domain: StringAtom) -> Self {
    Self { handler, domain: domain.clone(), full: Self::full_atom(handler, domain) }
  }

  /// Validates a channel ID.
  fn validate(handler: u32, domain: &str) -> bool {
    if handler == 0 {
      return false;
    }

    validate_domain(domain)
  }

  fn full_atom(handler: u32, domain: StringAtom) -> StringAtom {
    StringAtom::from(format!("!{}@{}", handler, domain))
  }
}

impl From<&ChannelId> for StringAtom {
  fn from(ch: &ChannelId) -> Self {
    ch.full.clone()
  }
}

impl From<ChannelId> for StringAtom {
  fn from(ch: ChannelId) -> Self {
    ch.full.clone()
  }
}

impl TryFrom<StringAtom> for ChannelId {
  type Error = ChannelIdParsingError;

  fn try_from(a: StringAtom) -> Result<Self, Self::Error> {
    ChannelId::from_str(a.as_ref())
  }
}

impl Display for ChannelId {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "!{}@{}", self.handler, self.domain)
  }
}

impl FromStr for ChannelId {
  type Err = ChannelIdParsingError;

  fn from_str(full: &str) -> Result<Self, Self::Err> {
    // Check if string starts with '!'
    if !full.starts_with('!') {
      return Err(ChannelIdParsingError::InvalidChannelIdFormat);
    }

    // Find the '@' position
    let at_pos = match full.find('@') {
      Some(pos) => pos,
      None => return Err(ChannelIdParsingError::InvalidChannelIdFormat),
    };

    // Extract the channel and domain parts
    let channel_str = &full[1..at_pos];
    let domain = &full[at_pos + 1..];

    // Parse the channel handler
    let channel_handler = match channel_str.parse::<u32>() {
      Ok(ch) => ch,
      Err(_) => return Err(ChannelIdParsingError::InvalidChannelIdFormat),
    };

    if !Self::validate(channel_handler, domain) {
      return Err(ChannelIdParsingError::InvalidChannelIdFormat);
    }

    Ok(Self { handler: channel_handler, domain: StringAtom::from(domain), full: StringAtom::from(full) })
  }
}

/// The ZID used by the client/server to be authenticated.
///
/// A ZID consists of a username and domain in the format `username@domain`.
/// Server ZIDs have an empty username and are represented simply as `domain`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Zid {
  /// The username of the ZID.
  pub username: StringAtom,

  /// The domain of the ZID.
  pub domain: StringAtom,

  /// The full representation of the ZID.
  full: StringAtom,
}

// ===== impl Zid =====

impl Zid {
  /// Creates a new ZID validating the username and domain.
  pub fn new(username: StringAtom, domain: StringAtom) -> Result<Self, ZidParsingError> {
    let username_ref = username.as_ref();
    let domain_ref = domain.as_ref();

    if !Self::validate(username_ref, domain_ref) {
      return Err(ZidParsingError::InvalidZidFormat);
    }
    Ok(Self { username: username.clone(), domain: domain.clone(), full: Self::full_atom(username, domain) })
  }

  /// Creates a new ZID without validation.
  ///
  /// # Safety
  ///
  /// This method skips validation checks on the username and domain.
  /// The caller must ensure that the provided values are valid.
  pub fn new_unchecked(username: StringAtom, domain: StringAtom) -> Self {
    Self { username: username.clone(), domain: domain.clone(), full: Self::full_atom(username, domain) }
  }

  /// Tells if the ZID is a server ZID.
  ///
  /// Server ZIDs have an empty username and are represented simply as `domain`.
  pub fn is_server(&self) -> bool {
    self.username.is_empty()
  }

  /// Validates a username and domain.
  fn validate(username: &str, domain: &str) -> bool {
    // Username validation
    if username.len() > USERNAME_MAX_LENGTH {
      return false;
    }
    if !username.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '.' || c == '_') {
      return false;
    }

    validate_domain(domain)
  }

  fn full_atom(username: StringAtom, domain: StringAtom) -> StringAtom {
    if username.is_empty() { domain.clone() } else { StringAtom::from(format!("{}@{}", username, domain)) }
  }
}

impl From<&Zid> for StringAtom {
  fn from(z: &Zid) -> Self {
    z.full.clone()
  }
}

impl From<Zid> for StringAtom {
  fn from(z: Zid) -> Self {
    z.full.clone()
  }
}

impl TryFrom<StringAtom> for Zid {
  type Error = ZidParsingError;

  fn try_from(a: StringAtom) -> Result<Self, Self::Error> {
    Zid::from_str(a.as_ref())
  }
}

impl FromStr for Zid {
  type Err = ZidParsingError;

  fn from_str(full: &str) -> Result<Self, Self::Err> {
    let (username, domain) = {
      if let Some((username, domain)) = full.split_once('@') {
        if username.is_empty() {
          return Err(ZidParsingError::InvalidZidFormat);
        }
        (username, domain)
      } else {
        ("", full)
      }
    };

    if !Self::validate(username, domain) {
      return Err(ZidParsingError::InvalidZidFormat);
    }

    Ok(Self { username: StringAtom::from(username), domain: StringAtom::from(domain), full: StringAtom::from(full) })
  }
}

impl Display for Zid {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if self.is_server() { write!(f, "{}", self.domain) } else { write!(f, "{}@{}", self.username, self.domain) }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_valid_channel_id_from_str() {
    // Basic valid case
    let channel_id = ChannelId::from_str("!123@example.com").unwrap();
    assert_eq!(channel_id.handler, 123);
    assert_eq!(&channel_id.domain, "example.com");

    // Test with larger channel number
    let channel_id = ChannelId::from_str("!987654321@domain.test").unwrap();
    assert_eq!(channel_id.handler, 987654321);
    assert_eq!(&channel_id.domain, "domain.test");

    // Test with subdomain
    let channel_id = ChannelId::from_str("!42@sub.example.org").unwrap();
    assert_eq!(channel_id.handler, 42);
    assert_eq!(&channel_id.domain, "sub.example.org");
  }

  #[test]
  fn test_invalid_channel_id_from_str() {
    // Missing ! prefix
    assert!(ChannelId::from_str("123@example.com").is_err());

    // Missing @ symbol
    assert!(ChannelId::from_str("!123example.com").is_err());

    // Empty channel
    assert!(ChannelId::from_str("!@example.com").is_err());

    // Invalid channel (not a number)
    assert!(ChannelId::from_str("!abc@example.com").is_err());

    // Empty domain
    assert!(ChannelId::from_str("!123@").is_err());

    // Channel ID is 0 (invalid per validate method)
    assert!(ChannelId::from_str("!0@example.com").is_err());

    // Invalid domain format
    assert!(ChannelId::from_str("!123@invalid..domain").is_err());
  }

  #[test]
  fn test_zid_try_from() {
    use std::collections::HashMap;

    let test_cases: HashMap<&str, Result<(&str, &str), ZidParsingError>> = HashMap::from([
      // valid cases
      ("user@example.com", Ok(("user", "example.com"))),
      ("user@sub.example.com", Ok(("user", "sub.example.com"))),
      ("example.com", Ok(("", "example.com"))),
      // invalid cases
      ("@example.com", Err(ZidParsingError::InvalidZidFormat)),
      ("user@", Err(ZidParsingError::InvalidZidFormat)),
      ("user@host@example.com", Err(ZidParsingError::InvalidZidFormat)),
      ("", Err(ZidParsingError::InvalidZidFormat)),
      (" user@example.com", Err(ZidParsingError::InvalidZidFormat)),
      ("user@example.com ", Err(ZidParsingError::InvalidZidFormat)),
    ]);

    for (input, expected) in test_cases {
      let result = Zid::from_str(input);
      match expected {
        Ok((expected_username, expected_domain)) => {
          assert!(result.is_ok(), "expected Ok for input '{}', got {:?}", input, result);
          let zid = result.unwrap();
          assert_eq!(zid.username.as_ref(), expected_username, "unexpected username for input '{}'", input);
          assert_eq!(zid.domain.as_ref(), expected_domain, "unexpected domain for input '{}'", input);
        },
        Err(expected_err) => {
          assert!(result.is_err(), "expected Err for input '{}', got {:?}", input, result);
          assert_eq!(result.unwrap_err(), expected_err, "unexpected error for input '{}'", input);
        },
      }
    }
  }

  #[test]
  fn test_zid_try_from_atom() {
    use std::collections::HashMap;

    let test_cases: HashMap<StringAtom, Result<(&str, &str), ZidParsingError>> = HashMap::from([
      // valid cases
      (StringAtom::from("user@example.com"), Ok(("user", "example.com"))),
      (StringAtom::from("user@sub.example.com"), Ok(("user", "sub.example.com"))),
      (StringAtom::from("example.com"), Ok(("", "example.com"))),
      // invalid cases
      (StringAtom::from("@example.com"), Err(ZidParsingError::InvalidZidFormat)),
      (StringAtom::from("user@"), Err(ZidParsingError::InvalidZidFormat)),
      (StringAtom::from("user@host@example.com"), Err(ZidParsingError::InvalidZidFormat)),
      (StringAtom::from(""), Err(ZidParsingError::InvalidZidFormat)),
      (StringAtom::from(" user@example.com"), Err(ZidParsingError::InvalidZidFormat)),
      (StringAtom::from("user@example.com "), Err(ZidParsingError::InvalidZidFormat)),
    ]);

    for (input, expected) in test_cases {
      let result = Zid::try_from(input.clone());
      match expected {
        Ok((expected_username, expected_domain)) => {
          assert!(result.is_ok(), "expected Ok for input '{}', got {:?}", input, result);
          let zid = result.unwrap();
          assert_eq!(zid.username.as_ref(), expected_username, "unexpected username for input '{}'", input);
          assert_eq!(zid.domain.as_ref(), expected_domain, "unexpected domain for input '{}'", input);
        },
        Err(expected_err) => {
          assert!(result.is_err(), "expected Err for input '{}', got {:?}", input, result);
          assert_eq!(result.unwrap_err(), expected_err, "unexpected error for input '{}'", input);
        },
      }
    }
  }
}
