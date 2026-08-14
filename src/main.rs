use std::{env, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand};
use swarm_mcp::{AppState, config::Config, http, probe};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "swarm-mcp", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the role-aware MCP HTTP server.
    Serve,
    /// Verify tool/resource catalogs and cross-role token isolation.
    Probe {
        /// HTTP origin of the running server. Defaults to loopback and `SWARM_MCP_PORT`.
        #[arg(long)]
        base_url: Option<String>,
    },
    /// Verify passive activity ingestion and supervisor visibility.
    ActivityProbe {
        /// HTTP origin of the running server. Defaults to loopback and `SWARM_MCP_PORT`.
        #[arg(long)]
        base_url: Option<String>,
    },
    /// Check the local /ready endpoint (used by the container healthcheck).
    Healthcheck {
        #[arg(long)]
        port: Option<u16>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run(Cli::parse()).await
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command.unwrap_or(Command::Serve) {
        Command::Healthcheck { port } => {
            let port = port.map_or_else(port_from_env, Ok)?;
            probe::healthcheck(port).await
        }
        command => {
            let config = Arc::new(Config::from_env().context("load swarm configuration")?);
            init_tracing(&config.log_level)?;
            match command {
                Command::Serve => {
                    let state = Arc::new(AppState::initialize((*config).clone()).await?);
                    http::serve(state).await
                }
                Command::Probe { base_url } => run_probe(config, base_url).await,
                Command::ActivityProbe { base_url } => run_activity_probe(config, base_url).await,
                Command::Healthcheck { .. } => unreachable!(),
            }
        }
    }
}

async fn run_probe(config: Arc<Config>, base_url: Option<String>) -> anyhow::Result<()> {
    let base_url = base_url.unwrap_or_else(|| loopback_url(config.port));
    let result = probe::catalog_probe(config, &base_url).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn run_activity_probe(config: Arc<Config>, base_url: Option<String>) -> anyhow::Result<()> {
    let base_url = base_url.unwrap_or_else(|| loopback_url(config.port));
    let result = probe::activity_probe(config, &base_url).await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn port_from_env() -> anyhow::Result<u16> {
    env::var("SWARM_MCP_PORT")
        .context("SWARM_MCP_PORT is required")?
        .parse()
        .context("SWARM_MCP_PORT must be a valid port")
}

fn loopback_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn init_tracing(filter: &str) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_new(filter).context("parse SWARM_LOG_LEVEL")?)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr),
        )
        .try_init()
        .context("initialize tracing")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_url_uses_loopback() {
        assert_eq!(loopback_url(3004), "http://127.0.0.1:3004");
    }

    #[tokio::test]
    async fn healthcheck_reports_connection_failure() {
        // A closed port must surface as an error, never a hang or panic.
        let cli = Cli::parse_from(["swarm-mcp", "healthcheck", "--port", "0"]);
        assert!(run(cli).await.is_err());
    }
}
