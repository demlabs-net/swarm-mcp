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
    use std::time::Duration;

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

    /// Minimal valid configuration map, mirroring the config.rs fixture. The
    /// binary cannot reuse the library's test fixtures, so the map is rebuilt
    /// here (`from_env_with` keeps it race-free without touching the process env).
    fn valid_env() -> std::collections::BTreeMap<String, String> {
        let mut env = std::collections::BTreeMap::new();
        env.insert(
            "SWARM_AGENT_ROLES".into(),
            "developer,lead-developer".into(),
        );
        env.insert("SWARM_MANAGER_ROLE".into(), "manager".into());
        env.insert(
            "SWARM_EXECUTOR_DESCRIPTIONS".into(),
            r#"{"developer":"Dev","lead-developer":"Lead"}"#.into(),
        );
        env.insert(
            "SWARM_ORDER_ACL".into(),
            r#"{"manager":["*"],"lead-developer":["developer"]}"#.into(),
        );
        env.insert(
            "SWARM_ACTIVITY_ROUTES".into(),
            r#"{"developer":["lead-developer"]}"#.into(),
        );
        env.insert("SWARM_ROLE_MCP_PATH_TEMPLATE".into(), "/mcp/{role}".into());
        env.insert("SWARM_ACTIVITY_SIGNAL_PATH".into(), "/activity".into());
        for role in ["manager", "developer", "lead-developer"] {
            let prefix = role.to_uppercase().replace('-', "_");
            env.insert(
                format!("{prefix}_API_URL"),
                format!("http://127.0.0.1:1/{role}"),
            );
            env.insert(
                format!("{prefix}_AGENT_API_KEY"),
                format!("api-key-{role}-1234567890"),
            );
            env.insert(
                format!("{prefix}_SWARM_MCP_TOKEN"),
                format!("mcp-token-{role}-1234567890123456"),
            );
        }
        env.insert("SWARM_MCP_ALLOWED_HOSTS".into(), "localhost".into());
        env.insert(
            "SWARM_MCP_ALLOWED_ORIGINS".into(),
            "http://localhost".into(),
        );
        env.insert(
            "SWARM_REPORT_STATUSES".into(),
            "completed,failed,in_progress".into(),
        );
        env.insert(
            "SWARM_AUTHORITY_MCP_INSTRUCTIONS".into(),
            "you order {role} {targets}".into(),
        );
        env.insert(
            "SWARM_ORDER_PROMPT_TEMPLATE".into(),
            "{task_id}|{sender}|{recipient}|{message}".into(),
        );
        env.insert(
            "SWARM_REPORT_PROMPT_TEMPLATE".into(),
            "{sender}|{recipient}|{task_id}|{status}|{message}".into(),
        );
        env.insert(
            "SWARM_PEER_PROMPT_TEMPLATE".into(),
            "{message_id}|{sender}|{recipient}|{message}".into(),
        );
        env.insert(
            "SWARM_TELEGRAM_INBOUND_PROMPT_TEMPLATE".into(),
            "update {update_id} user {user_id} -> {recipient}: {message}".into(),
        );
        env.insert("SWARM_MCP_HOST".into(), "127.0.0.1".into());
        env.insert("SWARM_MCP_PORT".into(), "3004".into());
        env.insert("SWARM_MAX_MESSAGE_CHARS".into(), "10000".into());
        env.insert("SWARM_MCP_MAX_REQUEST_BODY_BYTES".into(), "65536".into());
        env.insert("SWARM_API_TIMEOUT_SECONDS".into(), "5".into());
        env.insert("SWARM_HERMES_MODEL_ALIAS".into(), "hermes".into());
        env.insert(
            "SWARM_STATE_DB_PATH".into(),
            "/tmp/swarm-main-test.db".into(),
        );
        env.insert("SWARM_DB_MAX_CONNECTIONS".into(), "4".into());
        env.insert("SWARM_DB_BUSY_TIMEOUT_SECONDS".into(), "5".into());
        env.insert("SWARM_ACTIVITY_RETENTION_DAYS".into(), "30".into());
        env.insert("SWARM_ACTIVITY_HISTORY_LIMIT".into(), "50".into());
        env.insert("SWARM_ACTIVITY_STALE_AFTER_SECONDS".into(), "3600".into());
        env.insert("SWARM_ACTIVITY_CLOCK_SKEW_SECONDS".into(), "60".into());
        env.insert("SWARM_RECENT_OPERATIONS_LIMIT".into(), "50".into());
        env.insert("SWARM_OPERATION_RETENTION_DAYS".into(), "30".into());
        env.insert("SWARM_DISPATCH_RATE_LIMIT".into(), "100".into());
        env.insert("SWARM_DISPATCH_RATE_WINDOW_SECONDS".into(), "60".into());
        env.insert("SWARM_MAX_INFLIGHT_DISPATCHES".into(), "16".into());
        env.insert("SWARM_PENDING_STALE_SECONDS".into(), "300".into());
        env.insert("SWARM_CLEANUP_INTERVAL_SECONDS".into(), "300".into());
        env.insert("SWARM_MCP_REQUEST_RATE_LIMIT".into(), "1000".into());
        env.insert("SWARM_MCP_REQUEST_RATE_WINDOW_SECONDS".into(), "60".into());
        env.insert("SWARM_TELEGRAM_TIMEOUT_SECONDS".into(), "10".into());
        env.insert("SWARM_TELEGRAM_MESSAGE_LIMIT".into(), "4096".into());
        env.insert(
            "TELEGRAM_API_BASE_URL".into(),
            "https://api.telegram.org".into(),
        );
        env.insert("SWARM_OUTBOX_POLL_INTERVAL_SECONDS".into(), "5".into());
        env.insert("SWARM_OUTBOX_MAX_ATTEMPTS".into(), "5".into());
        env.insert("SWARM_OUTBOX_BATCH_SIZE".into(), "10".into());
        env.insert("SWARM_OUTBOX_RETENTION_DAYS".into(), "30".into());
        env.insert("SWARM_TELEGRAM_POLL_TIMEOUT_SECONDS".into(), "25".into());
        env.insert("SWARM_TELEGRAM_POLL_LIMIT".into(), "100".into());
        env.insert(
            "SWARM_TELEGRAM_INBOUND_RUN_INSTRUCTIONS".into(),
            "run it".into(),
        );
        env.insert("SWARM_MANAGER_MCP_INSTRUCTIONS".into(), "manager".into());
        env.insert("SWARM_EXECUTOR_MCP_INSTRUCTIONS".into(), "executor".into());
        env.insert("SWARM_EXECUTOR_RUN_INSTRUCTIONS".into(), "run it".into());
        env.insert("SWARM_SUPERVISOR_REPORT_INSTRUCTIONS".into(), "read".into());
        env.insert("SWARM_PEER_RUN_INSTRUCTIONS".into(), "read".into());
        env.insert("SWARM_LOG_LEVEL".into(), "info".into());
        env
    }

    /// Child-process body of the Serve command: boots the real server with the
    /// inherited swarm environment and exits cleanly on SIGTERM. Only meaningful
    /// when spawned by `serve_command_serves_and_shuts_down_on_sigterm`.
    #[cfg(unix)]
    #[tokio::test]
    async fn serve_command_child() -> anyhow::Result<()> {
        if std::env::var("SWARM_MCP_CHILD_TEST").is_err() {
            return Ok(());
        }
        let cli = Cli::parse_from(["swarm-mcp", "serve"]);
        run(cli).await?;
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serve_command_serves_and_shuts_down_on_sigterm() -> anyhow::Result<()> {
        let mut env = valid_env();
        env.insert("SWARM_MCP_ALLOWED_HOSTS".into(), "127.0.0.1".into());
        // Reserve a free port, then hand it to the child.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = probe.local_addr()?.port();
        drop(probe);
        env.insert("SWARM_MCP_PORT".into(), port.to_string());
        let db = std::env::temp_dir().join(format!(
            "swarm-main-serve-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        env.insert("SWARM_STATE_DB_PATH".into(), db.display().to_string());

        let mut child = tokio::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("tests::serve_command_child")
            .arg("--nocapture")
            .env("SWARM_MCP_CHILD_TEST", "1")
            .envs(&env)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;

        // Wait until the child actually serves /health: the Serve arm is live.
        let client = reqwest::Client::builder().build()?;
        let health = format!("http://127.0.0.1:{port}/health");
        let mut up = false;
        for _ in 0..100 {
            if client
                .get(&health)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(up, "the child must serve /health on port {port}");

        std::process::Command::new("kill")
            .args(["-TERM", &child.id().expect("child id").to_string()])
            .status()?;
        let status = tokio::time::timeout(Duration::from_secs(15), child.wait()).await??;
        assert!(
            status.success(),
            "graceful shutdown must exit cleanly: {status}"
        );

        for suffix in ["", "-wal", "-shm"] {
            let _ = tokio::fs::remove_file(format!("{}{suffix}", db.display())).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn probe_wrappers_propagate_outcomes() -> anyhow::Result<()> {
        let mut env = valid_env();
        env.insert("SWARM_MCP_ALLOWED_HOSTS".into(), "127.0.0.1".into());
        let config = Arc::new(Config::from_env_with(&|name| env.get(name).cloned())?);

        // The catalog probe against a dead endpoint surfaces the error.
        assert!(
            run_probe(config.clone(), Some("http://127.0.0.1:1".into()))
                .await
                .is_err()
        );

        // With no activity routes the activity probe short-circuits successfully.
        env.insert("SWARM_ACTIVITY_ROUTES".into(), "{}".into());
        let config = Arc::new(Config::from_env_with(&|name| env.get(name).cloned())?);
        assert!(
            run_activity_probe(config, Some("http://127.0.0.1:1".into()))
                .await
                .is_ok()
        );
        Ok(())
    }
}
