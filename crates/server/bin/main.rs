// SPDX-License-Identifier: BSD-3-Clause

use clap::Parser;

use narwhal_server::version::VERSION;

const ENV_WORKER_THREADS: &str = "NARWHAL_WORKER_THREADS";

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "narwhal-server")]
#[command(version = VERSION)]
#[command(about = "Narwhal server", long_about = None)]
struct Cli {
  /// Path to configuration file
  #[arg(short, long, value_name = "FILE")]
  config: Option<String>,
}

fn main() {
  narwhal_server::setup_panic_hook();

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

  let runtime = if worker_threads == 1 {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
  } else {
    tokio::runtime::Builder::new_multi_thread().worker_threads(worker_threads).enable_all().build().unwrap()
  };

  runtime.block_on(async {
    match narwhal_server::run(cli.config, worker_threads).await {
      Ok(_) => {},
      Err(e) => {
        eprintln!("error: {}", e);
        std::process::exit(1);
      },
    }
  });
}
