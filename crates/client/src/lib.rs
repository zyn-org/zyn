// SPDX-License-Identifier: AGPL-3.0

//! # Zyn Client Library
//!
//! This crate provides client implementations for connecting to Zyn servers.
//!
//! ## Client Types
//!
//! - **C2S (Client-to-Server)**: End-user clients connecting to the Zyn server
//!
//! ## Other Client Types
//!
//! The following client types remain in the `zyn-modulator` crate because they are
//! tightly coupled with modulator functionality:
//!
//! - **S2M (Server-to-Modulator)**: Server-initiated connections to modulators
//! - **M2S (Modulator-to-Server)**: Modulator-initiated connections for sending private messages
//!
//! If you need S2M or M2S client functionality, use:
//!
//! ```ignore
//! use zyn_modulator::client::{S2mClient, M2sClient};
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use zyn_client::C2sClient;
//! use zyn_common::client::Config;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Create a config with your desired settings
//! let config = Config {
//!     max_idle_connections: 16,
//!     heartbeat_interval: std::time::Duration::from_secs(60),
//!     connect_timeout: std::time::Duration::from_secs(5),
//!     timeout: std::time::Duration::from_secs(5),
//!     payload_read_timeout: std::time::Duration::from_secs(5),
//!     backoff_initial_delay: std::time::Duration::from_millis(100),
//!     backoff_max_delay: std::time::Duration::from_secs(30),
//!     backoff_max_retries: 5,
//! };
//! let client = C2sClient::new("127.0.0.1:5555", config)?;
//! # Ok(())
//! # }
//! ```

pub mod c2s;

// Re-export main client types for convenience
pub use c2s::{C2sClient, C2sSessionExtraInfo};
