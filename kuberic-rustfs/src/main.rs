use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use kuberic_rustfs::adapter::AdapterConfig;
use kuberic_rustfs::{controller, server};

#[derive(Parser)]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        local_node: Option<String>,
    },
    Controller {
        #[arg(long)]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    match Arguments::parse().command {
        Command::Serve { config, local_node } => {
            let mut config: AdapterConfig = controller::load_config(&config).await?;
            if let Some(local_node) = local_node {
                config.topology.local_node = Some(local_node);
            }
            server::serve(config).await
        }
        Command::Controller { config } => controller::run(&config).await,
    }
}
