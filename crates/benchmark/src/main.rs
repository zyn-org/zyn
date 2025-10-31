// SPDX-License-Identifier: AGPL-3.0

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing::{error, info};

/// Benchmark scenarios
#[derive(Debug, Clone, Copy, ValueEnum)]
enum Scenario {
  /// Simple broadcast scenario - all clients send messages to a shared channel
  Broadcast,
  /// Pub/sub scenario - separate publishers and subscribers
  PubSub,
  /// Connection churn - clients constantly joining and leaving
  Churn,
  /// Mixed workload - realistic combination of operations
  Mixed,
}

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "zyn-bench")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Zyn performance benchmarking tool", long_about = None)]
struct Cli {
  /// Server address to connect to
  #[arg(short, long, default_value = "127.0.0.1:22622")]
  server: SocketAddr,

  /// Number of concurrent clients to simulate
  #[arg(short = 'n', long, default_value = "100")]
  clients: usize,

  /// Duration to run the benchmark
  #[arg(short, long, default_value = "30s", value_parser = parse_duration)]
  duration: Duration,

  /// Benchmark scenario to run
  #[arg(short = 'S', long, value_enum, default_value = "broadcast")]
  scenario: Scenario,

  /// Message rate per client (messages per second, 0 = unlimited)
  #[arg(short, long, default_value = "0")]
  rate: u64,

  /// Output format
  #[arg(short, long, value_enum, default_value = "human")]
  output: OutputFormat,

  /// Channel name to use for testing
  #[arg(long, default_value = "benchmark")]
  channel: String,

  /// Username prefix for test clients
  #[arg(long, default_value = "bench_user")]
  username_prefix: String,

  /// Size of message payload in bytes
  #[arg(long, default_value = "1024")]
  payload_size: usize,
}

/// Output format for results
#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
  /// Human-readable output
  Human,
  /// JSON output
  Json,
  /// CSV output
  Csv,
}

/// Parse duration from string (supports: 30s, 5m, 1h)
fn parse_duration(s: &str) -> Result<Duration> {
  let s = s.trim();

  if let Some(secs) = s.strip_suffix('s') {
    Ok(Duration::from_secs(secs.parse()?))
  } else if let Some(mins) = s.strip_suffix('m') {
    Ok(Duration::from_secs(mins.parse::<u64>()? * 60))
  } else if let Some(hours) = s.strip_suffix('h') {
    Ok(Duration::from_secs(hours.parse::<u64>()? * 3600))
  } else {
    // Default to seconds if no suffix
    Ok(Duration::from_secs(s.parse()?))
  }
}

#[tokio::main]
async fn main() -> Result<()> {
  // Initialize tracing
  tracing_subscriber::fmt().with_target(false).with_thread_ids(false).with_level(true).init();

  let cli = Cli::parse();

  info!("starting zyn-bench");
  info!("server: {}", cli.server);
  info!("clients: {}", cli.clients);
  info!("duration: {:?}", cli.duration);
  info!("scenario: {:?}", cli.scenario);
  info!("channel: {}", cli.channel);

  // Run the benchmark
  match run_benchmark(cli).await {
    Ok(metrics) => {
      info!("benchmark completed successfully");
      print_results(&metrics);
      Ok(())
    },
    Err(e) => {
      error!("benchmark failed: {}", e);
      Err(e)
    },
  }
}

/// Benchmark metrics
#[derive(Debug)]
struct BenchmarkMetrics {
  total_messages_sent: u64,
  total_messages_received: u64,
  total_duration: Duration,
  successful_connections: usize,
  failed_connections: usize,
  errors: u64,
}

impl BenchmarkMetrics {
  fn new() -> Self {
    Self {
      total_messages_sent: 0,
      total_messages_received: 0,
      total_duration: Duration::ZERO,
      successful_connections: 0,
      failed_connections: 0,
      errors: 0,
    }
  }

  fn throughput_sent(&self) -> f64 {
    if self.total_duration.as_secs_f64() > 0.0 {
      self.total_messages_sent as f64 / self.total_duration.as_secs_f64()
    } else {
      0.0
    }
  }

  fn throughput_received(&self) -> f64 {
    if self.total_duration.as_secs_f64() > 0.0 {
      self.total_messages_received as f64 / self.total_duration.as_secs_f64()
    } else {
      0.0
    }
  }
}

/// Run the benchmark with the given configuration
async fn run_benchmark(cli: Cli) -> Result<BenchmarkMetrics> {
  let mut metrics = BenchmarkMetrics::new();

  info!("connecting {} clients to {}...", cli.clients, cli.server);

  // TODO: Implement client connections
  // TODO: Implement scenario execution
  // TODO: Collect metrics

  match cli.scenario {
    Scenario::Broadcast => {
      info!("running broadcast scenario...");
      run_broadcast_scenario(&cli, &mut metrics).await?;
    },
    Scenario::PubSub => {
      info!("running pub/sub scenario...");
      // TODO: Implement pub/sub scenario
      anyhow::bail!("PubSub scenario not yet implemented");
    },
    Scenario::Churn => {
      info!("running churn scenario...");
      // TODO: Implement churn scenario
      anyhow::bail!("Churn scenario not yet implemented");
    },
    Scenario::Mixed => {
      info!("running mixed scenario...");
      // TODO: Implement mixed scenario
      anyhow::bail!("Mixed scenario not yet implemented");
    },
  }

  metrics.total_duration = cli.duration;

  Ok(metrics)
}

/// Run the broadcast scenario
async fn run_broadcast_scenario(cli: &Cli, metrics: &mut BenchmarkMetrics) -> Result<()> {
  // TODO: Implement broadcast scenario
  // 1. Connect all clients
  // 2. Authenticate each client
  // 3. Join the channel
  // 4. Start sending/receiving messages
  // 5. Collect metrics

  info!("broadcast scenario will be implemented here");
  info!("  - creating {} clients", cli.clients);
  info!("  - joining channel '{}'", cli.channel);
  info!("  - running for {:?}", cli.duration);

  // Placeholder: simulate some work
  tokio::time::sleep(Duration::from_secs(1)).await;

  // Placeholder metrics
  metrics.successful_connections = cli.clients;
  metrics.total_messages_sent = 1000;
  metrics.total_messages_received = 1000;

  Ok(())
}

/// Print benchmark results
fn print_results(metrics: &BenchmarkMetrics) {
  println!("\n========================================");
  println!("         Benchmark Results");
  println!("========================================");
  println!();
  println!("Connections:");
  println!("  Successful:  {}", metrics.successful_connections);
  println!("  Failed:      {}", metrics.failed_connections);
  println!();
  println!("Messages:");
  println!("  Sent:        {}", metrics.total_messages_sent);
  println!("  Received:    {}", metrics.total_messages_received);
  println!("  Errors:      {}", metrics.errors);
  println!();
  println!("Throughput:");
  println!("  Sent:        {:.2} msg/s", metrics.throughput_sent());
  println!("  Received:    {:.2} msg/s", metrics.throughput_received());
  println!();
  println!("Duration:      {:.2}s", metrics.total_duration.as_secs_f64());
  println!("========================================");
}
