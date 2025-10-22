// SPDX-License-Identifier: AGPL-3.0

mod util;

pub mod c2s;
pub mod channel;
pub mod notifier;
pub mod router;
pub mod telemetry;
pub mod transmitter;
pub mod version;

use std::env;
use std::fs;
use std::sync::Arc;

use crate::channel::ChannelManager;
use crate::notifier::Notifier;
use crate::router::GlobalRouter;
use crate::version::{GIT_BRANCH_NAME, GIT_COMMIT_HASH, VERSION};

use serde_derive::{Deserialize, Serialize};
use tokio::signal;
use tokio::task::JoinHandle;
use tokio_tracing::info;
use tokio_util::sync::CancellationToken;
use tracing as tokio_tracing;

use zyn_util::string_atom::StringAtom;

#[derive(Debug, Default, Serialize, Deserialize)]
struct Config {
  #[serde(default)]
  telemetry: telemetry::Config,

  #[serde(rename = "c2s-server", default)]
  c2s_server: c2s::Config,

  #[serde(default)]
  modulator: zyn_modulator::Config,
}

/// Runs the zyn server with the specified number of worker threads.
///
/// This is the main entry point for the zyn server.
///
/// # Arguments
///
/// * `worker_threads` - The number of worker threads to use for the tokio runtime
///
/// # Returns
///
/// Returns `Ok(())` on successful shutdown, or an error if any step fails during
/// startup or shutdown.
pub async fn run(config_file: Option<String>, worker_threads: usize) -> anyhow::Result<()> {
  // Parse the configuration file.
  let mut cfg = load_config(config_file)?;

  // Initialize the telemetry subscriber.
  telemetry::init(cfg.telemetry)?;

  info!(
    version = VERSION,
    worker_threads = worker_threads,
    branch = GIT_BRANCH_NAME,
    commit = GIT_COMMIT_HASH,
    "🚀 zyn server is starting..."
  );

  // Initialize the modulator.
  let mut modulator_service = zyn_modulator::init_modulator(
    cfg.modulator.clone(),
    cfg.c2s_server.limits.max_message_size,
    cfg.c2s_server.limits.max_payload_size,
  )
  .await?;

  // Adjust message and payload limits based on the modulator's capabilities.
  cfg.c2s_server.limits.max_message_size = modulator_service.adjusted_max_message_size;
  cfg.c2s_server.limits.max_payload_size = modulator_service.adjusted_max_payload_size;

  let c2s_config = Arc::new(cfg.c2s_server);

  let max_channels = c2s_config.limits.max_channels;
  let max_clients_per_channel = c2s_config.limits.max_clients_per_channel;
  let max_channels_per_client = c2s_config.limits.max_channels_per_client;
  let max_payload_size = c2s_config.limits.max_payload_size;

  let local_domain = StringAtom::from(c2s_config.listener.domain.as_str());
  let c2s_router = c2s::Router::new(local_domain.clone());

  let global_router = GlobalRouter::new(c2s_router.clone());

  let notifier = Notifier::new(global_router.clone(), modulator_service.modulator.clone());

  let channel_mng = ChannelManager::new(
    global_router.clone(),
    notifier.clone(),
    max_channels,
    max_clients_per_channel,
    max_channels_per_client,
    max_payload_size,
  );

  let c2s_dispatcher_factory = c2s::conn::C2sDispatcherFactory::new(
    c2s_config.clone(),
    channel_mng.clone(),
    c2s_router.clone(),
    modulator_service.modulator.clone(),
  )
  .await?;

  let c2s_conn_mng = c2s::conn::C2sConnManager::new(c2s_config.as_ref(), c2s_dispatcher_factory);

  let mut c2s_ln = c2s::C2sListener::new(c2s_config.listener.clone(), c2s_conn_mng);

  // Start routing task for modulator private payloads.
  let mut route_m2s_payload_handle = Option::<(JoinHandle<()>, CancellationToken)>::None;

  if let Some(m2s_payload_rx) = modulator_service.m2s_payload_rx.take() {
    route_m2s_payload_handle = Some(c2s::route_m2s_private_payload(m2s_payload_rx, c2s_router));
  }

  modulator_service.bootstrap().await?;
  c2s_ln.bootstrap().await?;

  // Wait for stop signal.
  info!("waiting for stop signal... (press Ctrl+C to stop the server)");
  wait_for_stop_signal().await?;
  info!("received stop signal... gracefully shutting down...");

  c2s_ln.shutdown().await?;
  modulator_service.shutdown().await?;

  if let Some((handle, cancellation_token)) = route_m2s_payload_handle {
    cancellation_token.cancel();
    let _ = handle.await;
  }

  info!("👋 hasta la vista, baby");

  Ok(())
}

/// Sets up a custom panic hook that provides helpful debugging information when the server panics.
///
/// This should be called early in the application's initialization to ensure
/// all panics are caught and reported with the enhanced debugging information.
pub fn setup_panic_hook() {
  let orig_hook = std::panic::take_hook();
  std::panic::set_hook(Box::new(move |panic_info| {
    eprintln!("\n===========================================================");
    eprintln!("                😱 Oops! something went wrong                ");
    eprintln!("===========================================================\n");
    eprintln!("Zyn server has panicked. This is a bug. Please report this");
    eprintln!("at https://github.com/ortuman/zyn/issues/new.");
    eprintln!("If you can reliably reproduce this panic, include the");
    eprintln!("reproduction steps and re-run with the RUST_BACKTRACE=1 env");
    eprintln!("var set and include the backtrace in your report.");
    eprintln!();
    eprintln!("Platform: {} {}", env::consts::OS, env::consts::ARCH);
    eprintln!("Version: {}", version::VERSION);
    eprintln!("Branch: {}", version::GIT_BRANCH_NAME);
    eprintln!("Commit: {}", version::GIT_COMMIT_HASH);
    eprintln!("Args: {:?}", env::args().collect::<Vec<_>>());
    eprintln!();

    orig_hook(panic_info);

    std::process::exit(1);
  }));
}

fn load_config(config_file: Option<String>) -> anyhow::Result<Config> {
  let toml_file = config_file.unwrap_or("config.toml".to_string());

  // Read and parse the TOML file
  let config: Config = match fs::read_to_string(&toml_file) {
    Ok(config_content) => toml::from_str(&config_content)
      .map_err(|err| anyhow::anyhow!("failed to parse config file: {}, {}", toml_file, err))?,
    Err(_) => Config::default(),
  };

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
