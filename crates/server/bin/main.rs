// SPDX-License-Identifier: AGPL-3.0

use clap::Parser;

use zyn_server::version::VERSION;

const ENV_WORKER_THREADS: &str = "ZYN_WORKER_THREADS";

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "zyn-server")]
#[command(version = VERSION)]
#[command(about = "Zyn server", long_about = None)]
struct Cli {
  /// Path to configuration file
  #[arg(short, long, value_name = "FILE")]
  config: Option<String>,
}

fn main() {
  zyn_server::setup_panic_hook();

  let worker_threads = {
    let logical_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

    let mut worker_threads = logical_cores;
    if let Ok(value) = std::env::var(ENV_WORKER_THREADS)
      && let Ok(value) = value.parse::<usize>()
    {
      worker_threads = value;
    }
    worker_threads
  };

  let cli = Cli::parse();

  let runtime =
    tokio::runtime::Builder::new_multi_thread().worker_threads(worker_threads).enable_all().build().unwrap();

  runtime.block_on(async {
    match zyn_server::run(cli.config, worker_threads).await {
      Ok(_) => {},
      Err(e) => {
        eprintln!("error: {}", e);
        std::process::exit(1);
      },
    }
  });
}
