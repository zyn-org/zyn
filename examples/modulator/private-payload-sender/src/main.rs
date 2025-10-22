// A modulator that sends private payload periodically to a concrete target.
//
// SPDX-License-Identifier: AGPL-3.0

use std::fs;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use clap::Parser;
use serde_json::json;
use tokio::sync::broadcast;

use tokio::signal;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

use zyn_modulator::config::S2mServerConfig;
use zyn_modulator::modulator::{
  AuthRequest, AuthResponse, ForwardBroadcastPayloadRequest, ForwardBroadcastPayloadResponse, ForwardEventRequest,
  ForwardEventResponse, Operation, Operations, OutboundPrivatePayload, ReceivePrivatePayloadRequest,
  ReceivePrivatePayloadResponse, SendPrivatePayloadRequest, SendPrivatePayloadResponse,
};
use zyn_modulator::{create_s2m_listener, Modulator};
use zyn_util::string_atom::StringAtom;

const MODULATOR_PROTOCOL_NAME: &str = "private-payload-sender/1.0";

#[derive(Debug, Clone)]
struct PrivatePayloadSender {
  target: StringAtom,
}

#[async_trait]
impl zyn_modulator::Modulator for PrivatePayloadSender {
  /// Returns the unique name of this modulator.
  async fn protocol_name(&self) -> anyhow::Result<StringAtom> {
    Ok(MODULATOR_PROTOCOL_NAME.into())
  }

  /// Declares that this modulator supports only the ReceivePrivatePayload operation.
  async fn operations(&self) -> anyhow::Result<Operations> {
    Ok(Operations::new().with(Operation::ReceivePrivatePayload))
  }

  /// This modulator does not support authentication.
  async fn authenticate(&self, _request: AuthRequest) -> anyhow::Result<AuthResponse> {
    unreachable!()
  }

  /// This modulator does not support forwarding broadcast payloads.
  async fn forward_broadcast_payload(
    &self,
    _request: ForwardBroadcastPayloadRequest,
  ) -> anyhow::Result<ForwardBroadcastPayloadResponse> {
    unreachable!()
  }

  /// This modulator does not support event forwarding.
  async fn forward_event(&self, _request: ForwardEventRequest) -> anyhow::Result<ForwardEventResponse> {
    unreachable!()
  }

  /// This modulator does not support sending private payloads.
  async fn send_private_payload(
    &self,
    _request: SendPrivatePayloadRequest,
  ) -> anyhow::Result<SendPrivatePayloadResponse> {
    unreachable!()
  }

  /// Handles receiving private payload requests by setting up a broadcast channel
  /// that periodically sends JSON test payloads to the configured target user.
  ///
  /// This method:
  /// - Creates a broadcast channel for distributing outbound payloads
  /// - Spawns a background task that runs indefinitely
  /// - Sends a test JSON payload every 5 seconds to the target user
  ///
  /// The test payloads include:
  /// - A message field with "Test private payload"
  /// - A timestamp in RFC3339 format
  /// - An incrementing counter
  ///
  /// # Arguments
  /// * `_request` - The incoming request (currently unused)
  ///
  /// # Returns
  /// A `ReceivePrivatePayloadResponse` containing a broadcast receiver that will
  /// emit `OutboundPrivatePayload` objects for the modulator infrastructure to
  /// distribute to the appropriate clients.
  async fn receive_private_payload(
    &self,
    _request: ReceivePrivatePayloadRequest,
  ) -> anyhow::Result<ReceivePrivatePayloadResponse> {
    let target = self.target.clone();

    // Create a broadcast channel for sending payloads
    let (sender, receiver) = broadcast::channel(100);

    // Create a new pool buffer with the reversed text
    let pool = zyn_util::pool::Pool::new(1, 4096);

    // Spawn a task to send payloads every 5 seconds
    tokio::spawn(async move {
      let mut interval = tokio::time::interval(Duration::from_secs(5));
      let mut counter = 0;

      loop {
        interval.tick().await;
        counter += 1;

        // Create a test JSON payload
        let payload = json!({
          "message": "Test private payload",
          "timestamp": Utc::now().to_rfc3339(),
          "counter": counter,
        });

        // Serialize the payload to bytes
        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        // Create a PoolBuffer from the payload bytes
        {
          let mut mut_pool_buffer = pool.must_acquire();
          let mut_buff_ptr = mut_pool_buffer.as_mut_slice();

          mut_buff_ptr[..payload_bytes.len()].copy_from_slice(&payload_bytes);

          // Create the outbound payload with target
          let outbound_payload = OutboundPrivatePayload {
            payload: mut_pool_buffer.freeze(payload_bytes.len()),
            targets: vec![target.clone()],
          };

          // Send through the broadcast channel
          let _ = sender.send(outbound_payload);
        }
      }
    });

    Ok(ReceivePrivatePayloadResponse { receiver })
  }
}

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "private-payload-sender")]
#[command(about = "S2M example server -- private payload sender", long_about = None)]
struct Cli {
  /// Path to configuration file
  #[arg(short, long, value_name = "FILE")]
  config: Option<String>,

  /// Target username (defaults to "test_user")
  #[arg(short, long, value_name = "USERNAME")]
  target: Option<String>,
}

#[tokio::main]
async fn main() {
  let env_filter = EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy();

  tracing_subscriber::fmt().with_env_filter(env_filter).init();

  let cli = Cli::parse();

  let config = load_config(cli.config).unwrap_or_else(|err| {
    eprintln!("error: {}", err);
    std::process::exit(1);
  });

  let modulator =
    PrivatePayloadSender { target: StringAtom::from(cli.target.unwrap_or_else(|| "test_user".to_string())) };

  match run_s2m_server(config, modulator).await {
    Ok(_) => {},
    Err(e) => eprintln!("error: {}", e),
  }
}

async fn run_s2m_server<M>(config: S2mServerConfig, modulator: M) -> anyhow::Result<()>
where
  M: Modulator,
{
  let protocol_name = modulator.protocol_name().await?;

  let mut ln = create_s2m_listener(config, modulator).await?;

  info!(protocol_name = protocol_name.as_ref(), "📡 starting s2m server...");

  // Bootstrap the listener.
  ln.bootstrap().await?;

  // Wait for stop signal.
  info!("waiting for stop signal... (press Ctrl+C to stop the server)");
  wait_for_stop_signal().await?;
  info!("received stop signal... gracefully shutting down...");

  // Stop listener.
  ln.shutdown().await?;

  info!(protocol_name = protocol_name.as_ref(), "🌙 that's all, folks!");

  Ok(())
}

fn load_config(config_file: Option<String>) -> anyhow::Result<S2mServerConfig> {
  let toml_file = config_file.unwrap_or("config.toml".to_string());

  // Read and parse the TOML file
  let config_content = fs::read_to_string(&toml_file)
    .map_err(|err| anyhow::anyhow!("failed to read config file: {}, {}", toml_file, err))?;

  let config: S2mServerConfig = toml::from_str(&config_content)
    .map_err(|err| anyhow::anyhow!("failed to parse config file: {}, {}", toml_file, err))?;

  Ok(config)
}

async fn wait_for_stop_signal() -> anyhow::Result<()> {
  let mut sig_term = signal::unix::signal(signal::unix::SignalKind::terminate())?;

  tokio::select! {
    _ = signal::ctrl_c() => Ok(()),
    _ = sig_term.recv() => {
      Ok(())
    },
  }
}
