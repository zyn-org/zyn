// SPDX-License-Identifier: AGPL-3.0

use std::time::Duration;

use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListenerConfig {
  /// The domain of the server.
  #[serde(default = "default_domain")]
  pub domain: String,

  /// The path to the certificate file.
  #[serde(default)]
  pub cert_file: String,

  /// The path to the key file.
  #[serde(default)]
  pub key_file: String,

  /// The address to bind to.
  #[serde(default = "default_bind_address")]
  pub bind_address: String,

  /// The port to bind to.
  #[serde(default = "default_port")]
  pub port: u16,
}

impl Default for ListenerConfig {
  fn default() -> Self {
    Self {
      domain: default_domain(),
      cert_file: String::new(),
      key_file: String::new(),
      bind_address: default_bind_address(),
      port: default_port(),
    }
  }
}

// Configuration for the C2S server
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
  /// The C2S listener configuration.
  #[serde(default)]
  pub listener: ListenerConfig,

  /// The timeout for the connection.
  #[serde(default = "default_connect_timeout")]
  pub connect_timeout: Duration,

  /// The timeout for the authentication in milliseconds.
  #[serde(default = "default_authenticate_timeout")]
  pub authenticate_timeout: Duration,

  /// The interval for keep alive messages.
  #[serde(default = "default_keep_alive_interval")]
  pub keep_alive_interval: Duration,

  /// The minimum interval for keep alive messages.
  #[serde(default = "default_min_keep_alive_interval")]
  pub min_keep_alive_interval: Duration,

  /// The timeout for the request.
  #[serde(default = "default_request_timeout")]
  pub request_timeout: Duration,

  /// The timeout for reading a broadcast payload.
  #[serde(default = "default_payload_read_timeout")]
  pub payload_read_timeout: Duration,

  /// The C2S limits.
  #[serde(default)]
  pub limits: Limits,
}

impl Default for Config {
  fn default() -> Self {
    Config {
      listener: ListenerConfig::default(),
      connect_timeout: default_connect_timeout(),
      authenticate_timeout: default_authenticate_timeout(),
      keep_alive_interval: default_keep_alive_interval(),
      min_keep_alive_interval: default_min_keep_alive_interval(),
      payload_read_timeout: default_payload_read_timeout(),
      request_timeout: default_request_timeout(),
      limits: Limits::default(),
    }
  }
}

fn default_domain() -> String {
  "localhost".to_string()
}

fn default_bind_address() -> String {
  "0.0.0.0".to_string()
}

fn default_port() -> u16 {
  22622
}

fn default_connect_timeout() -> Duration {
  Duration::from_secs(30)
}

fn default_authenticate_timeout() -> Duration {
  Duration::from_secs(30)
}

fn default_min_keep_alive_interval() -> Duration {
  Duration::from_secs(10)
}

fn default_keep_alive_interval() -> Duration {
  Duration::from_secs(60)
}

fn default_payload_read_timeout() -> Duration {
  Duration::from_secs(10)
}

fn default_request_timeout() -> Duration {
  Duration::from_secs(20)
}

/// Limits for the C2S listener
#[derive(Debug, Serialize, Deserialize)]
pub struct Limits {
  /// The maximum number of connections that a listener can handle
  #[serde(default = "default_max_connections")]
  pub max_connections: u32,

  /// The maximum number of channels that a server can handle
  #[serde(default = "default_max_channels")]
  pub max_channels: u32,

  /// The maximum number of clients that a channel can handle
  #[serde(default = "default_max_clients_per_channel")]
  pub max_clients_per_channel: u32,

  /// The maximum number of channels that a client can join.
  #[serde(default = "default_max_channels_per_client")]
  pub max_channels_per_client: u32,

  /// The maximum message size allowed.
  #[serde(default = "default_max_message_size")]
  pub max_message_size: u32,

  /// The maximum payload size allowed.
  #[serde(default = "default_max_payload_size")]
  pub max_payload_size: u32,

  /// Total memory budget in bytes for the payload buffer pool.
  /// The pool will allocate buffers of varying sizes up to this total.
  #[serde(default = "default_payload_pool_memory_budget")]
  pub payload_pool_memory_budget: u32,

  /// The maximum number of inflight requests per client.
  #[serde(default = "default_max_inflight_requests")]
  pub max_inflight_requests: u32,

  /// The maximum number of messages that can be enqueued
  /// before disconnecting the client.
  #[serde(default = "default_send_message_channel_size")]
  pub send_message_channel_size: u32,

  /// The maximum number of bytes that can be sent per second.
  #[serde(default = "default_rate_limit")]
  pub rate_limit: u32,
}

fn default_max_connections() -> u32 {
  10_000
}

fn default_max_message_size() -> u32 {
  8 * 1024 // 8KB
}

fn default_max_payload_size() -> u32 {
  64 * 1024 // 64KB
}

fn default_max_inflight_requests() -> u32 {
  100
}

fn default_max_channels() -> u32 {
  5_000
}

fn default_max_clients_per_channel() -> u32 {
  100
}

fn default_max_channels_per_client() -> u32 {
  100
}

fn default_send_message_channel_size() -> u32 {
  1_024
}

fn default_rate_limit() -> u32 {
  256 * 1024 // 256KB
}

fn default_payload_pool_memory_budget() -> u32 {
  256 * 1024 * 1024 // 256MB
}

impl Default for Limits {
  fn default() -> Self {
    Self {
      max_connections: default_max_connections(),
      max_channels: default_max_channels(),
      max_clients_per_channel: default_max_clients_per_channel(),
      max_channels_per_client: default_max_channels_per_client(),
      max_message_size: default_max_message_size(),
      max_payload_size: default_max_payload_size(),
      max_inflight_requests: default_max_inflight_requests(),
      send_message_channel_size: default_send_message_channel_size(),
      rate_limit: default_rate_limit(),
      payload_pool_memory_budget: default_payload_pool_memory_budget(),
    }
  }
}

impl From<&Config> for zyn_common::conn::Config {
  fn from(config: &Config) -> Self {
    zyn_common::conn::Config {
      max_connections: config.limits.max_connections,
      max_message_size: config.limits.max_message_size,
      max_payload_size: config.limits.max_payload_size,
      connect_timeout: config.connect_timeout,
      authenticate_timeout: config.authenticate_timeout,
      payload_read_timeout: config.payload_read_timeout,
      send_message_channel_size: config.limits.send_message_channel_size,
      request_timeout: config.request_timeout,
      max_inflight_requests: config.limits.max_inflight_requests,
      rate_limit: config.limits.rate_limit,
      payload_pool_memory_budget: config.limits.payload_pool_memory_budget,
    }
  }
}
