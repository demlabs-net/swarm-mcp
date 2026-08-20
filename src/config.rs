use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt,
    net::IpAddr,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, anyhow, bail, ensure};
use regex::Regex;
use serde::de::DeserializeOwned;
use url::Url;

#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Clone, Debug)]
pub struct AgentConfig {
    pub role: String,
    pub api_url: Url,
    pub api_key: Secret,
    pub mcp_token: Secret,
    pub telegram_bot_token: Option<Secret>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramBotMode {
    PerRole,
    Shared,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelegramBacklogMode {
    Discard,
    Process,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_ip: IpAddr,
    pub port: u16,
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub role_path_template: String,
    pub role_paths: BTreeMap<String, String>,
    pub activity_path: String,
    pub manager_role: String,
    pub agent_roles: Vec<String>,
    pub all_roles: Vec<String>,
    pub descriptions: BTreeMap<String, String>,
    pub order_acl: BTreeMap<String, Vec<String>>,
    pub global_authorities: BTreeSet<String>,
    pub activity_routes: BTreeMap<String, Vec<String>>,
    pub agents: BTreeMap<String, AgentConfig>,
    pub max_message_chars: usize,
    pub max_request_body_bytes: usize,
    pub api_timeout: Duration,
    pub hermes_model_alias: String,
    pub state_db_path: PathBuf,
    pub db_max_connections: u32,
    pub db_busy_timeout: Duration,
    pub activity_enabled: bool,
    pub activity_retention_days: i64,
    pub activity_history_limit: i64,
    pub activity_stale_after: Duration,
    pub activity_clock_skew: Duration,
    pub recent_operations_limit: i64,
    pub operation_retention_days: i64,
    pub rate_limit: i64,
    pub rate_window: Duration,
    pub duplicate_window: Duration,
    pub max_inflight_dispatches: usize,
    pub pending_stale_after: Duration,
    pub cleanup_interval: Duration,
    pub mcp_request_rate_limit: usize,
    pub mcp_request_rate_window: Duration,
    pub report_statuses: Vec<String>,
    pub report_wake_statuses: BTreeSet<String>,
    pub telegram_enabled: bool,
    pub telegram_bot_mode: TelegramBotMode,
    pub telegram_bot_token: Option<Secret>,
    pub telegram_timeout: Duration,
    pub telegram_message_limit: usize,
    pub telegram_group_id: Option<String>,
    pub telegram_proxy_url: Option<Url>,
    pub telegram_api_base_url: Url,
    pub outbox_poll_interval: Duration,
    pub outbox_max_attempts: i64,
    pub outbox_batch_size: i64,
    pub outbox_retention_days: i64,
    pub telegram_inbound_enabled: bool,
    pub telegram_allowed_users: BTreeSet<i64>,
    pub telegram_inbound_targets: Vec<String>,
    pub telegram_poll_timeout: Duration,
    pub telegram_poll_limit: usize,
    pub telegram_backlog_mode: TelegramBacklogMode,
    pub telegram_inbound_instructions: String,
    pub telegram_inbound_template: String,
    pub manager_instructions: String,
    pub executor_instructions: String,
    pub authority_instructions: String,
    pub executor_run_instructions: String,
    pub supervisor_report_instructions: String,
    pub peer_run_instructions: String,
    pub order_template: String,
    pub report_template: String,
    pub peer_template: String,
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_env_with(&system_env)
    }

    /// Validate and load the configuration from an arbitrary environment source,
    /// so tests can exercise the full validation matrix without touching the
    /// process environment (which is global and races between parallel tests).
    pub fn from_env_with(env: &dyn Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let role_pattern = Regex::new(r"^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?$")?;
        let agent_roles = csv_required(env, "SWARM_AGENT_ROLES")?
            .into_iter()
            .map(|role| role.to_lowercase())
            .collect::<Vec<_>>();
        ensure!(
            !agent_roles.is_empty(),
            "SWARM_AGENT_ROLES must not be empty"
        );
        ensure!(
            agent_roles.len() <= 32,
            "SWARM_AGENT_ROLES must contain at most 32 executors"
        );
        ensure!(
            agent_roles.iter().all(|role| role_pattern.is_match(role)),
            "SWARM_AGENT_ROLES contains an invalid role name"
        );
        ensure!(
            agent_roles.iter().collect::<BTreeSet<_>>().len() == agent_roles.len(),
            "SWARM_AGENT_ROLES contains duplicates"
        );

        let manager_role = required(env, "SWARM_MANAGER_ROLE")?.to_lowercase();
        ensure!(
            role_pattern.is_match(&manager_role),
            "SWARM_MANAGER_ROLE is invalid"
        );
        ensure!(
            !agent_roles.contains(&manager_role),
            "SWARM_MANAGER_ROLE must not also be an executor"
        );
        let mut all_roles = vec![manager_role.clone()];
        all_roles.extend(agent_roles.clone());
        let role_set = all_roles.iter().cloned().collect::<BTreeSet<_>>();
        let executor_set = agent_roles.iter().cloned().collect::<BTreeSet<_>>();

        let descriptions: BTreeMap<String, String> = json_env(env, "SWARM_EXECUTOR_DESCRIPTIONS")?;
        ensure!(
            descriptions.keys().cloned().collect::<BTreeSet<_>>() == executor_set,
            "SWARM_EXECUTOR_DESCRIPTIONS must describe every executor exactly once"
        );
        ensure!(
            descriptions.values().all(|value| !value.trim().is_empty()),
            "SWARM_EXECUTOR_DESCRIPTIONS contains an empty description"
        );

        let raw_acl: BTreeMap<String, Vec<String>> = json_env(env, "SWARM_ORDER_ACL")?;
        let mut global_authorities = BTreeSet::new();
        let mut order_acl = BTreeMap::new();
        for (source, raw_targets) in &raw_acl {
            ensure!(role_set.contains(source), "unknown ACL authority: {source}");
            let targets = if raw_targets.as_slice() == ["*"] {
                global_authorities.insert(source.clone());
                agent_roles
                    .iter()
                    .filter(|role| *role != source)
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                ensure!(
                    !raw_targets.iter().any(|target| target == "*"),
                    "'*' must be the only target in SWARM_ORDER_ACL.{source}"
                );
                normalize_targets(source, raw_targets, &executor_set)?
            };
            order_acl.insert(source.clone(), targets);
        }
        let manager_targets = order_acl.get(&manager_role).cloned().unwrap_or_default();
        ensure!(
            executor_set.is_subset(&manager_targets.iter().cloned().collect()),
            "manager must be authorized to order every executor"
        );
        ensure!(
            global_authorities.contains(&manager_role),
            "manager must use the ['*'] ACL grant so order_all is available"
        );

        let raw_routes: BTreeMap<String, Vec<String>> = json_env(env, "SWARM_ACTIVITY_ROUTES")?;
        let mut activity_routes = BTreeMap::new();
        for (sender, raw_targets) in raw_routes {
            ensure!(
                executor_set.contains(&sender),
                "only executors may emit activity"
            );
            let targets = normalize_role_targets(&sender, &raw_targets, &role_set)?;
            for target in &targets {
                ensure!(
                    order_acl
                        .get(target)
                        .is_some_and(|allowed| allowed.contains(&sender)),
                    "activity target {target} does not supervise {sender}"
                );
            }
            activity_routes.insert(sender, targets);
        }

        let role_path_template = required(env, "SWARM_ROLE_MCP_PATH_TEMPLATE")?;
        ensure!(
            role_path_template.matches("{role}").count() == 1,
            "SWARM_ROLE_MCP_PATH_TEMPLATE must contain exactly one {{role}}"
        );
        let role_paths = all_roles
            .iter()
            .map(|role| {
                let path = role_path_template.replace("{role}", role);
                validate_path(&path)?;
                Ok((role.clone(), path.trim_end_matches('/').to_string()))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        ensure!(
            role_paths.values().collect::<BTreeSet<_>>().len() == role_paths.len(),
            "role MCP paths must be unique"
        );
        ensure!(
            !role_paths
                .values()
                .any(|path| matches!(path.as_str(), "/health" | "/ready")),
            "role MCP paths conflict with a service health endpoint"
        );
        let activity_path = required(env, "SWARM_ACTIVITY_SIGNAL_PATH")?
            .trim_end_matches('/')
            .to_string();
        validate_path(&activity_path)?;
        ensure!(
            !role_paths.values().any(|path| path == &activity_path),
            "activity path conflicts with a role MCP path"
        );
        ensure!(
            !matches!(activity_path.as_str(), "/health" | "/ready"),
            "activity path conflicts with a service health endpoint"
        );

        let mut agents = BTreeMap::new();
        let mut tokens = BTreeSet::new();
        for role in &all_roles {
            let prefix = role.to_uppercase().replace('-', "_");
            let api_url = parse_http_url(
                &format!("{prefix}_API_URL"),
                &required(env, &format!("{prefix}_API_URL"))?,
            )?;
            let api_key = Secret(required(env, &format!("{prefix}_AGENT_API_KEY"))?);
            ensure!(
                api_key.expose().len() >= 16,
                "{prefix}_AGENT_API_KEY is too short"
            );
            let token_value = required(env, &format!("{prefix}_SWARM_MCP_TOKEN"))?;
            ensure!(
                token_value.len() >= 24,
                "{prefix}_SWARM_MCP_TOKEN is too short"
            );
            ensure!(
                tokens.insert(token_value.clone()),
                "Swarm MCP tokens must be unique"
            );
            let telegram_bot_token =
                optional(env, &format!("{prefix}_TELEGRAM_BOT_TOKEN")).map(Secret);
            agents.insert(
                role.clone(),
                AgentConfig {
                    role: role.clone(),
                    api_url,
                    api_key,
                    mcp_token: Secret(token_value),
                    telegram_bot_token,
                },
            );
        }

        let allowed_hosts = csv_required(env, "SWARM_MCP_ALLOWED_HOSTS")?;
        ensure!(
            !allowed_hosts.is_empty(),
            "SWARM_MCP_ALLOWED_HOSTS is empty"
        );
        for host in &allowed_hosts {
            let authority = http::uri::Authority::from_str(host)
                .with_context(|| format!("invalid allowed host: {host}"))?;
            // The http crate parses a non-numeric port as "no port", which would
            // silently turn a typo like `localhost:bad-port` into a host-only
            // rule matching any port. Reject explicit but malformed ports.
            let has_explicit_port = if host.starts_with('[') {
                host.contains("]:")
            } else {
                host.contains(':')
            };
            ensure!(
                !has_explicit_port || authority.port_u16().is_some(),
                "allowed host has a non-numeric port: {host}"
            );
        }
        let allowed_origins = csv_required(env, "SWARM_MCP_ALLOWED_ORIGINS")?;
        ensure!(
            !allowed_origins.is_empty(),
            "SWARM_MCP_ALLOWED_ORIGINS must explicitly list trusted origins"
        );
        for origin in &allowed_origins {
            if origin == "null" {
                continue;
            }
            let parsed =
                Url::parse(origin).with_context(|| format!("invalid allowed origin: {origin}"))?;
            ensure!(
                matches!(parsed.scheme(), "http" | "https"),
                "allowed origin must use HTTP(S): {origin}"
            );
            ensure!(
                parsed.host_str().is_some(),
                "allowed origin lacks a host: {origin}"
            );
            ensure!(
                parsed.username().is_empty()
                    && parsed.password().is_none()
                    && parsed.path() == "/"
                    && parsed.query().is_none()
                    && parsed.fragment().is_none(),
                "allowed origin must contain only scheme, host, and optional port: {origin}"
            );
        }

        let report_statuses = csv_required(env, "SWARM_REPORT_STATUSES")?
            .into_iter()
            .map(|status| status.to_lowercase())
            .collect::<Vec<_>>();
        ensure!(
            !report_statuses.is_empty(),
            "SWARM_REPORT_STATUSES is empty"
        );
        ensure!(
            report_statuses.iter().collect::<BTreeSet<_>>().len() == report_statuses.len(),
            "SWARM_REPORT_STATUSES contains duplicates"
        );
        ensure!(
            report_statuses.iter().all(|status| {
                !status.is_empty()
                    && status.len() <= 32
                    && status
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            }),
            "SWARM_REPORT_STATUSES contains an invalid status"
        );
        ensure!(
            report_statuses.iter().any(|status| status == "completed"),
            "SWARM_REPORT_STATUSES must contain completed because it is the report default"
        );
        let report_wake_statuses = csv_required(env, "SWARM_REPORT_WAKE_STATUSES")?
            .into_iter()
            .map(|status| status.to_lowercase())
            .collect::<BTreeSet<_>>();
        ensure!(
            report_wake_statuses.contains("completed"),
            "SWARM_REPORT_WAKE_STATUSES must contain completed"
        );
        ensure!(
            report_wake_statuses
                .iter()
                .all(|status| report_statuses.contains(status)),
            "SWARM_REPORT_WAKE_STATUSES must be a subset of SWARM_REPORT_STATUSES"
        );

        let telegram_enabled = bool_env(env, "SWARM_TELEGRAM_ENABLED", false)?;
        let telegram_bot_mode = match optional(env, "SWARM_TELEGRAM_BOT_MODE")
            .unwrap_or_else(|| "per-role".to_string())
            .to_lowercase()
            .as_str()
        {
            "per-role" => TelegramBotMode::PerRole,
            "shared" => TelegramBotMode::Shared,
            _ => bail!("SWARM_TELEGRAM_BOT_MODE must be per-role or shared"),
        };
        let telegram_bot_token = optional(env, "SWARM_TELEGRAM_BOT_TOKEN").map(Secret);
        let telegram_group_id = optional(env, "TELEGRAM_GROUP_ID");
        if telegram_enabled {
            ensure!(telegram_group_id.is_some(), "TELEGRAM_GROUP_ID is required");
            match telegram_bot_mode {
                TelegramBotMode::PerRole => {
                    let missing = all_roles
                        .iter()
                        .filter(|role| {
                            agents
                                .get(*role)
                                .is_none_or(|agent| agent.telegram_bot_token.is_none())
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    ensure!(
                        missing.is_empty(),
                        "per-role Telegram mode is enabled but bot tokens are missing for: {}",
                        missing.join(", ")
                    );
                }
                TelegramBotMode::Shared => ensure!(
                    telegram_bot_token.is_some(),
                    "SWARM_TELEGRAM_BOT_TOKEN is required in shared Telegram mode"
                ),
            }
        }

        let telegram_inbound_enabled = bool_env(env, "SWARM_TELEGRAM_INBOUND_ENABLED", false)?;
        let telegram_allowed_users = optional(env, "TELEGRAM_ALLOWED_USERS")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| {
                        value
                            .parse::<i64>()
                            .with_context(|| format!("invalid Telegram user ID: {value}"))
                    })
                    .collect::<anyhow::Result<BTreeSet<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        ensure!(
            telegram_allowed_users.iter().all(|user_id| *user_id > 0),
            "TELEGRAM_ALLOWED_USERS must contain positive numeric user IDs"
        );
        let telegram_inbound_targets = match optional(env, "SWARM_TELEGRAM_INBOUND_TARGETS") {
            Some(value) if value.trim() == "*" => all_roles.clone(),
            Some(value) => {
                let raw = value
                    .split(',')
                    .map(str::trim)
                    .filter(|target| !target.is_empty())
                    .map(str::to_lowercase)
                    .collect::<Vec<_>>();
                normalize_role_targets("__telegram__", &raw, &role_set)?
            }
            None => Vec::new(),
        };
        if telegram_inbound_enabled {
            ensure!(
                telegram_enabled,
                "Telegram inbound requires Telegram delivery"
            );
            ensure!(
                telegram_bot_mode == TelegramBotMode::Shared,
                "Telegram inbound requires shared Telegram mode"
            );
            ensure!(
                !telegram_allowed_users.is_empty(),
                "TELEGRAM_ALLOWED_USERS is required for Telegram inbound"
            );
            ensure!(
                !telegram_inbound_targets.is_empty(),
                "SWARM_TELEGRAM_INBOUND_TARGETS is required for Telegram inbound"
            );
            let group = telegram_group_id
                .as_deref()
                .context("TELEGRAM_GROUP_ID is required for Telegram inbound")?;
            ensure!(
                group.parse::<i64>().is_ok_and(|value| value != 0),
                "TELEGRAM_GROUP_ID must be a non-zero numeric chat ID for Telegram inbound"
            );
        }

        let authority_instructions = required(env, "SWARM_AUTHORITY_MCP_INSTRUCTIONS")?;
        validate_template(
            "SWARM_AUTHORITY_MCP_INSTRUCTIONS",
            &authority_instructions,
            &["role", "targets"],
            &["role", "targets"],
        )?;
        let order_template = required(env, "SWARM_ORDER_PROMPT_TEMPLATE")?;
        validate_template(
            "SWARM_ORDER_PROMPT_TEMPLATE",
            &order_template,
            &["task_id", "sender", "recipient", "message"],
            &["task_id", "sender", "message"],
        )?;
        let report_template = required(env, "SWARM_REPORT_PROMPT_TEMPLATE")?;
        validate_template(
            "SWARM_REPORT_PROMPT_TEMPLATE",
            &report_template,
            &["sender", "recipient", "task_id", "status", "message"],
            &["sender", "task_id", "status", "message"],
        )?;
        let peer_template = required(env, "SWARM_PEER_PROMPT_TEMPLATE")?;
        validate_template(
            "SWARM_PEER_PROMPT_TEMPLATE",
            &peer_template,
            &["message_id", "sender", "recipient", "task_id", "message"],
            &["message_id", "sender", "task_id", "message"],
        )?;
        let telegram_inbound_template = required(env, "SWARM_TELEGRAM_INBOUND_PROMPT_TEMPLATE")?;
        validate_template(
            "SWARM_TELEGRAM_INBOUND_PROMPT_TEMPLATE",
            &telegram_inbound_template,
            &[
                "update_id",
                "message_id",
                "user_id",
                "username",
                "recipient",
                "message",
            ],
            &["update_id", "user_id", "recipient", "message"],
        )?;

        let telegram_proxy_url = optional_url(env, "TELEGRAM_PROXY_URL")?;
        if let Some(proxy) = &telegram_proxy_url {
            ensure!(
                matches!(proxy.scheme(), "http" | "https" | "socks5" | "socks5h"),
                "TELEGRAM_PROXY_URL uses an unsupported scheme"
            );
            ensure!(
                proxy.username().is_empty() && proxy.password().is_none(),
                "TELEGRAM_PROXY_URL must not embed credentials"
            );
        }

        let config = Self {
            bind_ip: IpAddr::from_str(&required(env, "SWARM_MCP_HOST")?)
                .context("SWARM_MCP_HOST must be an IP address")?,
            port: positive::<u16>(env, "SWARM_MCP_PORT")?,
            allowed_hosts,
            allowed_origins,
            role_path_template,
            role_paths,
            activity_path,
            manager_role,
            agent_roles,
            all_roles,
            descriptions,
            order_acl,
            global_authorities,
            activity_routes,
            agents,
            max_message_chars: positive(env, "SWARM_MAX_MESSAGE_CHARS")?,
            max_request_body_bytes: positive(env, "SWARM_MCP_MAX_REQUEST_BODY_BYTES")?,
            api_timeout: seconds(env, "SWARM_API_TIMEOUT_SECONDS")?,
            hermes_model_alias: required(env, "SWARM_HERMES_MODEL_ALIAS")?,
            state_db_path: PathBuf::from(required(env, "SWARM_STATE_DB_PATH")?),
            db_max_connections: positive(env, "SWARM_DB_MAX_CONNECTIONS")?,
            db_busy_timeout: seconds(env, "SWARM_DB_BUSY_TIMEOUT_SECONDS")?,
            activity_enabled: bool_env(env, "SWARM_ACTIVITY_ENABLED", false)?,
            activity_retention_days: positive(env, "SWARM_ACTIVITY_RETENTION_DAYS")?,
            activity_history_limit: positive(env, "SWARM_ACTIVITY_HISTORY_LIMIT")?,
            activity_stale_after: seconds(env, "SWARM_ACTIVITY_STALE_AFTER_SECONDS")?,
            activity_clock_skew: seconds(env, "SWARM_ACTIVITY_CLOCK_SKEW_SECONDS")?,
            recent_operations_limit: positive(env, "SWARM_RECENT_OPERATIONS_LIMIT")?,
            operation_retention_days: positive(env, "SWARM_OPERATION_RETENTION_DAYS")?,
            rate_limit: positive(env, "SWARM_DISPATCH_RATE_LIMIT")?,
            rate_window: seconds(env, "SWARM_DISPATCH_RATE_WINDOW_SECONDS")?,
            duplicate_window: seconds(env, "SWARM_DUPLICATE_WINDOW_SECONDS")?,
            max_inflight_dispatches: positive(env, "SWARM_MAX_INFLIGHT_DISPATCHES")?,
            pending_stale_after: seconds(env, "SWARM_PENDING_STALE_SECONDS")?,
            cleanup_interval: seconds(env, "SWARM_CLEANUP_INTERVAL_SECONDS")?,
            mcp_request_rate_limit: positive(env, "SWARM_MCP_REQUEST_RATE_LIMIT")?,
            mcp_request_rate_window: seconds(env, "SWARM_MCP_REQUEST_RATE_WINDOW_SECONDS")?,
            report_statuses,
            report_wake_statuses,
            telegram_enabled,
            telegram_bot_mode,
            telegram_bot_token,
            telegram_timeout: seconds(env, "SWARM_TELEGRAM_TIMEOUT_SECONDS")?,
            telegram_message_limit: positive(env, "SWARM_TELEGRAM_MESSAGE_LIMIT")?,
            telegram_group_id,
            telegram_proxy_url,
            telegram_api_base_url: parse_http_url(
                "TELEGRAM_API_BASE_URL",
                &required(env, "TELEGRAM_API_BASE_URL")?,
            )?,
            outbox_poll_interval: seconds(env, "SWARM_OUTBOX_POLL_INTERVAL_SECONDS")?,
            outbox_max_attempts: positive(env, "SWARM_OUTBOX_MAX_ATTEMPTS")?,
            outbox_batch_size: positive(env, "SWARM_OUTBOX_BATCH_SIZE")?,
            outbox_retention_days: positive(env, "SWARM_OUTBOX_RETENTION_DAYS")?,
            telegram_inbound_enabled,
            telegram_allowed_users,
            telegram_inbound_targets,
            telegram_poll_timeout: seconds(env, "SWARM_TELEGRAM_POLL_TIMEOUT_SECONDS")?,
            telegram_poll_limit: positive(env, "SWARM_TELEGRAM_POLL_LIMIT")?,
            telegram_backlog_mode: if bool_env(env, "SWARM_TELEGRAM_PROCESS_BACKLOG", false)? {
                TelegramBacklogMode::Process
            } else {
                TelegramBacklogMode::Discard
            },
            telegram_inbound_instructions: required(
                env,
                "SWARM_TELEGRAM_INBOUND_RUN_INSTRUCTIONS",
            )?,
            telegram_inbound_template,
            manager_instructions: required(env, "SWARM_MANAGER_MCP_INSTRUCTIONS")?,
            executor_instructions: required(env, "SWARM_EXECUTOR_MCP_INSTRUCTIONS")?,
            authority_instructions,
            executor_run_instructions: required(env, "SWARM_EXECUTOR_RUN_INSTRUCTIONS")?,
            supervisor_report_instructions: required(env, "SWARM_SUPERVISOR_REPORT_INSTRUCTIONS")?,
            peer_run_instructions: required(env, "SWARM_PEER_RUN_INSTRUCTIONS")?,
            order_template,
            report_template,
            peer_template,
            log_level: required(env, "SWARM_LOG_LEVEL")?,
        };
        ensure!(
            (512..=4096).contains(&config.telegram_message_limit),
            "SWARM_TELEGRAM_MESSAGE_LIMIT must be between 512 and 4096"
        );
        ensure!(
            (1..=100_000).contains(&config.max_message_chars),
            "SWARM_MAX_MESSAGE_CHARS must be between 1 and 100000"
        );
        ensure!(
            (1_024..=16 * 1024 * 1024).contains(&config.max_request_body_bytes),
            "SWARM_MCP_MAX_REQUEST_BODY_BYTES must be between 1024 and 16777216"
        );
        ensure!(
            config.api_timeout <= Duration::from_secs(300),
            "SWARM_API_TIMEOUT_SECONDS must not exceed 300"
        );
        ensure!(
            config.max_request_body_bytes
                >= config
                    .max_message_chars
                    .saturating_mul(4)
                    .saturating_add(2_048),
            "SWARM_MCP_MAX_REQUEST_BODY_BYTES is too small for SWARM_MAX_MESSAGE_CHARS"
        );
        ensure!(
            config.state_db_path.is_absolute(),
            "SWARM_STATE_DB_PATH must be absolute"
        );
        ensure!(
            config.db_max_connections <= 32,
            "SWARM_DB_MAX_CONNECTIONS must not exceed 32"
        );
        ensure!(
            config.db_busy_timeout <= Duration::from_secs(60),
            "SWARM_DB_BUSY_TIMEOUT_SECONDS must not exceed 60"
        );
        ensure!(
            config.max_inflight_dispatches <= 256,
            "SWARM_MAX_INFLIGHT_DISPATCHES must not exceed 256"
        );
        ensure!(
            config.rate_limit <= 10_000,
            "SWARM_DISPATCH_RATE_LIMIT must not exceed 10000"
        );
        ensure!(
            config.rate_window <= Duration::from_secs(3_600),
            "SWARM_DISPATCH_RATE_WINDOW_SECONDS must not exceed 3600"
        );
        ensure!(
            config.duplicate_window <= Duration::from_secs(86_400),
            "SWARM_DUPLICATE_WINDOW_SECONDS must not exceed 86400"
        );
        ensure!(
            config.recent_operations_limit <= 1_000,
            "SWARM_RECENT_OPERATIONS_LIMIT must not exceed 1000"
        );
        ensure!(
            config.activity_history_limit <= 10_000,
            "SWARM_ACTIVITY_HISTORY_LIMIT must not exceed 10000"
        );
        ensure!(
            config.outbox_batch_size <= 1_000,
            "SWARM_OUTBOX_BATCH_SIZE must not exceed 1000"
        );
        ensure!(
            config.outbox_max_attempts <= 100,
            "SWARM_OUTBOX_MAX_ATTEMPTS must not exceed 100"
        );
        ensure!(
            config.mcp_request_rate_limit <= 100_000,
            "SWARM_MCP_REQUEST_RATE_LIMIT must not exceed 100000"
        );
        ensure!(
            config.mcp_request_rate_window <= Duration::from_secs(3_600),
            "SWARM_MCP_REQUEST_RATE_WINDOW_SECONDS must not exceed 3600"
        );
        ensure!(
            (Duration::from_secs(5)..=Duration::from_secs(86_400))
                .contains(&config.cleanup_interval),
            "SWARM_CLEANUP_INTERVAL_SECONDS must be between 5 and 86400"
        );
        ensure!(
            config.activity_clock_skew <= Duration::from_secs(3_600),
            "SWARM_ACTIVITY_CLOCK_SKEW_SECONDS must not exceed 3600"
        );
        ensure!(
            config.activity_stale_after <= Duration::from_secs(30 * 86_400),
            "SWARM_ACTIVITY_STALE_AFTER_SECONDS must not exceed 2592000"
        );
        ensure!(
            config.telegram_timeout <= Duration::from_secs(60),
            "SWARM_TELEGRAM_TIMEOUT_SECONDS must not exceed 60"
        );
        ensure!(
            config.telegram_poll_timeout <= Duration::from_secs(50),
            "SWARM_TELEGRAM_POLL_TIMEOUT_SECONDS must not exceed 50"
        );
        ensure!(
            config.telegram_poll_limit <= 100,
            "SWARM_TELEGRAM_POLL_LIMIT must not exceed 100"
        );
        ensure!(
            config.outbox_poll_interval <= Duration::from_secs(300),
            "SWARM_OUTBOX_POLL_INTERVAL_SECONDS must not exceed 300"
        );
        ensure!(
            config.activity_retention_days <= 3_650
                && config.operation_retention_days <= 3_650
                && config.outbox_retention_days <= 3_650,
            "retention periods must not exceed 3650 days"
        );
        ensure!(
            config.pending_stale_after > config.api_timeout.saturating_mul(2),
            "SWARM_PENDING_STALE_SECONDS must exceed twice SWARM_API_TIMEOUT_SECONDS"
        );
        ensure!(
            config.pending_stale_after <= Duration::from_secs(86_400),
            "SWARM_PENDING_STALE_SECONDS must not exceed 86400"
        );
        Ok(config)
    }

    pub fn supervisors(&self, role: &str) -> Vec<String> {
        self.all_roles
            .iter()
            .filter(|source| {
                self.order_acl
                    .get(*source)
                    .is_some_and(|targets| targets.iter().any(|target| target == role))
            })
            .cloned()
            .collect()
    }

    pub fn telegram_token_for(&self, sender: &str) -> Option<&Secret> {
        match self.telegram_bot_mode {
            TelegramBotMode::Shared => self.telegram_bot_token.as_ref(),
            TelegramBotMode::PerRole => self
                .agents
                .get(sender)
                .and_then(|agent| agent.telegram_bot_token.as_ref()),
        }
    }
}

fn system_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required(env: &dyn Fn(&str) -> Option<String>, name: &str) -> anyhow::Result<String> {
    optional(env, name).ok_or_else(|| anyhow!("{name} must be configured"))
}

fn optional(env: &dyn Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    env(name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn csv_required(env: &dyn Fn(&str) -> Option<String>, name: &str) -> anyhow::Result<Vec<String>> {
    Ok(required(env, name)?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn json_env<T: DeserializeOwned>(
    env: &dyn Fn(&str) -> Option<String>,
    name: &str,
) -> anyhow::Result<T> {
    serde_json::from_str(&required(env, name)?).with_context(|| format!("parse {name} as JSON"))
}

fn positive<T>(env: &dyn Fn(&str) -> Option<String>, name: &str) -> anyhow::Result<T>
where
    T: FromStr + PartialOrd + Default,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = required(env, name)?
        .parse::<T>()
        .with_context(|| format!("parse {name}"))?;
    ensure!(value > T::default(), "{name} must be positive");
    Ok(value)
}

fn seconds(env: &dyn Fn(&str) -> Option<String>, name: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(positive::<u64>(env, name)?))
}

fn bool_env(
    env: &dyn Fn(&str) -> Option<String>,
    name: &str,
    default: bool,
) -> anyhow::Result<bool> {
    let Some(value) = optional(env, name) else {
        return Ok(default);
    };
    match value.to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}

fn normalize_targets(
    source: &str,
    targets: &[String],
    executors: &BTreeSet<String>,
) -> anyhow::Result<Vec<String>> {
    let mut normalized = Vec::new();
    for raw in targets {
        let target = raw.trim().to_lowercase();
        ensure!(
            executors.contains(&target),
            "unknown target {target} for {source}"
        );
        ensure!(target != source, "{source} cannot target itself");
        if !normalized.contains(&target) {
            normalized.push(target);
        }
    }
    Ok(normalized)
}

fn normalize_role_targets(
    source: &str,
    targets: &[String],
    roles: &BTreeSet<String>,
) -> anyhow::Result<Vec<String>> {
    let mut normalized = Vec::new();
    for raw in targets {
        let target = raw.trim().to_lowercase();
        ensure!(roles.contains(&target), "unknown activity target {target}");
        ensure!(target != source, "{source} cannot target itself");
        if !normalized.contains(&target) {
            normalized.push(target);
        }
    }
    Ok(normalized)
}

fn validate_path(path: &str) -> anyhow::Result<()> {
    ensure!(path.starts_with('/'), "path must be absolute: {path}");
    ensure!(
        !path.contains('?') && !path.contains('#'),
        "path is invalid: {path}"
    );
    ensure!(
        !path.contains("//"),
        "path contains an empty segment: {path}"
    );
    ensure!(path.len() > 1, "path must not be root");
    Ok(())
}

fn parse_http_url(name: &str, value: &str) -> anyhow::Result<Url> {
    let parsed = Url::parse(value).with_context(|| format!("parse {name}"))?;
    ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "{name} must use HTTP(S)"
    );
    ensure!(parsed.host_str().is_some(), "{name} must contain a host");
    ensure!(
        parsed.username().is_empty() && parsed.password().is_none(),
        "{name} must not contain credentials"
    );
    ensure!(
        parsed.query().is_none() && parsed.fragment().is_none(),
        "{name} must not contain query or fragment"
    );
    Ok(parsed)
}

fn optional_url(env: &dyn Fn(&str) -> Option<String>, name: &str) -> anyhow::Result<Option<Url>> {
    optional(env, name)
        .map(|value| Url::parse(&value).with_context(|| format!("parse {name}")))
        .transpose()
}

fn validate_template(
    name: &str,
    template: &str,
    allowed_keys: &[&str],
    required_keys: &[&str],
) -> anyhow::Result<()> {
    let mut keys = BTreeSet::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        ensure!(
            !rest[..start].contains('}'),
            "{name} contains an unmatched closing brace"
        );
        let after = &rest[start + 1..];
        let end = after
            .find('}')
            .with_context(|| format!("{name} contains an unterminated placeholder"))?;
        let key = &after[..end];
        ensure!(
            allowed_keys.contains(&key),
            "{name} contains an unsupported placeholder: {key}"
        );
        keys.insert(key);
        rest = &after[end + 1..];
    }
    ensure!(
        !rest.contains('}'),
        "{name} contains an unmatched closing brace"
    );
    for key in required_keys {
        ensure!(keys.contains(key), "{name} must contain {{{key}}}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_validation_requires_exact_contract() {
        assert!(
            validate_template(
                "TEST",
                "{sender}: {message}",
                &["sender", "message"],
                &["sender", "message"],
            )
            .is_ok()
        );
        assert!(
            validate_template(
                "TEST",
                "{sender}",
                &["sender", "message"],
                &["sender", "message"],
            )
            .is_err()
        );
        assert!(
            validate_template("TEST", "{sender}: {secret}", &["sender"], &["sender"],).is_err()
        );
        assert!(validate_template("TEST", "{sender", &["sender"], &["sender"]).is_err());
    }

    #[test]
    fn path_validation_rules() {
        assert!(validate_path("/mcp/manager").is_ok());
        assert!(validate_path("/a/b/c").is_ok());
        assert!(validate_path("mcp/manager").is_err(), "must be absolute");
        assert!(validate_path("/").is_err(), "must not be root");
        assert!(validate_path("/a//b").is_err(), "empty segment");
        assert!(validate_path("/a?b").is_err(), "query");
        assert!(validate_path("/a#b").is_err(), "fragment");
    }

    #[test]
    fn http_url_validation_rules() {
        assert!(parse_http_url("TEST", "http://example.com:3004").is_ok());
        assert!(parse_http_url("TEST", "https://example.com").is_ok());
        assert!(
            parse_http_url("TEST", "ftp://example.com").is_err(),
            "scheme"
        );
        assert!(parse_http_url("TEST", "http://").is_err(), "missing host");
        assert!(
            parse_http_url("TEST", "http://user:pass@example.com").is_err(),
            "credentials"
        );
        assert!(
            parse_http_url("TEST", "http://example.com?a=1").is_err(),
            "query"
        );
        assert!(
            parse_http_url("TEST", "http://example.com#frag").is_err(),
            "fragment"
        );
    }

    #[test]
    fn target_normalization_deduplicates_and_rejects_self() {
        let executors = BTreeSet::from(["developer".to_string(), "designer".to_string()]);
        assert_eq!(
            normalize_targets(
                "manager",
                &[
                    "developer".to_string(),
                    "developer".to_string(),
                    "designer".to_string()
                ],
                &executors,
            )
            .unwrap(),
            vec!["developer".to_string(), "designer".to_string()]
        );
        assert!(normalize_targets("manager", &["unknown".to_string()], &executors).is_err());
        assert!(normalize_targets("developer", &["developer".to_string()], &executors).is_err());
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret = Secret::new("super-secret-value".to_string());
        assert_eq!(format!("{secret:?}"), "<redacted>");
        assert!(!format!("{secret:?}").contains("super-secret"));
        assert_eq!(secret.expose(), "super-secret-value");
    }

    /// A complete, valid environment matching the fixture hierarchy
    /// (manager + developer + lead-developer, manager is the global authority).
    fn valid_env() -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
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
            "SWARM_REPORT_WAKE_STATUSES".into(),
            "completed,failed".into(),
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
            "{message_id}|{sender}|{recipient}|{task_id}|{message}".into(),
        );
        env.insert(
            "SWARM_TELEGRAM_INBOUND_PROMPT_TEMPLATE".into(),
            "update {update_id} user {user_id} -> {recipient}: {message}".into(),
        );
        env.insert("SWARM_MCP_HOST".into(), "127.0.0.1".into());
        env.insert("SWARM_MCP_PORT".into(), "3004".into());
        env.insert("SWARM_MAX_MESSAGE_CHARS".into(), "10000".into());
        env.insert("SWARM_MCP_MAX_REQUEST_BODY_BYTES".into(), "65536".into());
        env.insert("SWARM_API_TIMEOUT_SECONDS".into(), "30".into());
        env.insert("SWARM_HERMES_MODEL_ALIAS".into(), "hermes".into());
        env.insert("SWARM_STATE_DB_PATH".into(), "/tmp/swarm-test.db".into());
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
        env.insert("SWARM_DUPLICATE_WINDOW_SECONDS".into(), "600".into());
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

    fn load(env: &BTreeMap<String, String>) -> anyhow::Result<Config> {
        Config::from_env_with(&|name| env.get(name).cloned())
    }

    #[test]
    fn from_env_accepts_a_complete_configuration() {
        let config = load(&valid_env()).unwrap();
        assert_eq!(config.manager_role, "manager");
        assert_eq!(config.agent_roles, vec!["developer", "lead-developer"]);
        assert!(config.global_authorities.contains("manager"));
        // supervisors() follows all_roles order: manager first, then executors.
        assert_eq!(
            config.supervisors("developer"),
            vec!["manager", "lead-developer"]
        );
    }

    #[test]
    fn from_env_rejects_missing_vars() {
        let mut env = valid_env();
        env.remove("SWARM_LOG_LEVEL");
        let error = load(&env).unwrap_err().to_string();
        assert!(error.contains("SWARM_LOG_LEVEL"), "{error}");
    }

    #[test]
    fn from_env_rejects_invalid_role_hierarchy() {
        let mut env = valid_env();
        env.insert("SWARM_AGENT_ROLES".into(), "developer,developer".into());
        assert!(load(&env).is_err(), "duplicate executors");

        let mut env = valid_env();
        env.insert("SWARM_AGENT_ROLES".into(), "developer,manager".into());
        assert!(load(&env).is_err(), "manager must not be an executor");

        let mut env = valid_env();
        env.insert("SWARM_AGENT_ROLES".into(), "Developer".into());
        assert!(load(&env).is_err(), "role names are validated");

        let mut env = valid_env();
        env.remove("SWARM_EXECUTOR_DESCRIPTIONS");
        assert!(load(&env).is_err(), "descriptions are required");
    }

    #[test]
    fn from_env_rejects_invalid_acl_and_routes() {
        let mut env = valid_env();
        env.insert(
            "SWARM_ORDER_ACL".into(),
            r#"{"manager":["developer"],"lead-developer":["developer"]}"#.into(),
        );
        let error = load(&env).unwrap_err().to_string();
        assert!(
            error.contains("manager must be authorized to order every executor"),
            "manager must cover every executor: {error}"
        );

        let mut env = valid_env();
        env.insert(
            "SWARM_ORDER_ACL".into(),
            r#"{"manager":["*"],"lead-developer":["designer"]}"#.into(),
        );
        assert!(load(&env).is_err(), "unknown ACL target");

        let mut env = valid_env();
        env.insert(
            "SWARM_ACTIVITY_ROUTES".into(),
            r#"{"manager":["developer"]}"#.into(),
        );
        assert!(load(&env).is_err(), "only executors may emit activity");
    }

    #[test]
    fn from_env_rejects_short_and_duplicate_credentials() {
        let mut env = valid_env();
        env.insert("MANAGER_SWARM_MCP_TOKEN".into(), "too-short".into());
        assert!(load(&env).is_err(), "token must be >= 24 chars");

        let mut env = valid_env();
        env.insert(
            "DEVELOPER_SWARM_MCP_TOKEN".into(),
            "mcp-token-manager-1234567890123456".into(),
        );
        assert!(load(&env).is_err(), "tokens must be unique");

        let mut env = valid_env();
        env.insert("MANAGER_AGENT_API_KEY".into(), "short".into());
        assert!(load(&env).is_err(), "api key must be >= 16 chars");
    }

    #[test]
    fn from_env_rejects_bad_templates_paths_and_origins() {
        let mut env = valid_env();
        env.insert(
            "SWARM_ORDER_PROMPT_TEMPLATE".into(),
            "{sender}|{recipient}|{message}".into(),
        );
        assert!(load(&env).is_err(), "order template must contain task_id");

        let mut env = valid_env();
        env.insert(
            "SWARM_ORDER_PROMPT_TEMPLATE".into(),
            "{unknown} {sender}".into(),
        );
        assert!(load(&env).is_err(), "unknown placeholder");

        let mut env = valid_env();
        env.insert("SWARM_ACTIVITY_SIGNAL_PATH".into(), "/mcp/manager".into());
        assert!(
            load(&env).is_err(),
            "activity path must not collide with a role path"
        );

        let mut env = valid_env();
        env.insert("SWARM_ACTIVITY_SIGNAL_PATH".into(), "/health".into());
        assert!(
            load(&env).is_err(),
            "activity path must not collide with /health"
        );

        let mut env = valid_env();
        env.insert(
            "SWARM_MCP_ALLOWED_ORIGINS".into(),
            "http://localhost/callback".into(),
        );
        assert!(
            load(&env).is_err(),
            "origin must contain only scheme/host/port"
        );

        let mut env = valid_env();
        env.insert(
            "SWARM_MCP_ALLOWED_HOSTS".into(),
            "localhost:bad-port".into(),
        );
        assert!(load(&env).is_err(), "host must be a valid authority");
    }

    #[test]
    fn from_env_enforces_telegram_mode_contracts() {
        let mut env = valid_env();
        env.insert("SWARM_TELEGRAM_ENABLED".into(), "true".into());
        env.insert("TELEGRAM_GROUP_ID".into(), "-100123".into());
        let error = load(&env).unwrap_err().to_string();
        assert!(
            error.contains("bot tokens"),
            "per-role mode needs bot tokens: {error}"
        );

        let mut env = valid_env();
        env.insert("SWARM_TELEGRAM_ENABLED".into(), "true".into());
        env.insert("SWARM_TELEGRAM_BOT_MODE".into(), "shared".into());
        env.insert("SWARM_TELEGRAM_BOT_TOKEN".into(), "123456:ABC".into());
        env.insert("TELEGRAM_GROUP_ID".into(), "-100123".into());
        assert!(load(&env).is_ok(), "shared mode with a token is valid");

        let mut env = valid_env();
        env.insert("SWARM_TELEGRAM_ENABLED".into(), "true".into());
        env.insert("SWARM_TELEGRAM_BOT_MODE".into(), "shared".into());
        env.insert("SWARM_TELEGRAM_BOT_TOKEN".into(), "123456:ABC".into());
        env.insert("TELEGRAM_GROUP_ID".into(), "-100123".into());
        env.insert("SWARM_TELEGRAM_INBOUND_ENABLED".into(), "true".into());
        let error = load(&env).unwrap_err().to_string();
        assert!(
            error.contains("TELEGRAM_ALLOWED_USERS"),
            "inbound needs allowed users: {error}"
        );
    }

    #[test]
    fn from_env_accepts_the_ambient_environment_or_fails_fast() {
        // The thin wrapper must route through the same validation; the outcome
        // depends on the ambient environment, but either way the wrapper runs.
        match Config::from_env() {
            Ok(_) => {}
            Err(error) => assert!(error.to_string().contains("must be configured")),
        }
    }

    #[test]
    fn from_env_enforces_numeric_ranges() {
        let cases = [
            ("SWARM_DISPATCH_RATE_LIMIT", "20000"),
            ("SWARM_DB_MAX_CONNECTIONS", "64"),
            ("SWARM_MAX_INFLIGHT_DISPATCHES", "512"),
            ("SWARM_CLEANUP_INTERVAL_SECONDS", "1"),
            ("SWARM_MCP_REQUEST_RATE_LIMIT", "200000"),
            ("SWARM_TELEGRAM_POLL_LIMIT", "200"),
            ("SWARM_TELEGRAM_MESSAGE_LIMIT", "100"),
            ("SWARM_API_TIMEOUT_SECONDS", "600"),
            ("SWARM_PENDING_STALE_SECONDS", "10"),
        ];
        for (name, value) in cases {
            let mut env = valid_env();
            env.insert(name.to_string(), value.to_string());
            assert!(load(&env).is_err(), "{name}={value} must be rejected");
        }
    }

    #[test]
    fn from_env_enforces_cross_field_constraints() {
        // body size must fit the max message length.
        let mut env = valid_env();
        env.insert("SWARM_MAX_MESSAGE_CHARS".into(), "100000".into());
        env.insert("SWARM_MCP_MAX_REQUEST_BODY_BYTES".into(), "65536".into());
        assert!(load(&env).is_err(), "body too small for the message limit");

        // report statuses must include the default.
        let mut env = valid_env();
        env.insert("SWARM_REPORT_STATUSES".into(), "failed,in_progress".into());
        assert!(load(&env).is_err(), "completed is the report default");

        // wake statuses must be valid report statuses and include completion.
        let mut env = valid_env();
        env.insert("SWARM_REPORT_WAKE_STATUSES".into(), "failed".into());
        assert!(
            load(&env).is_err(),
            "completed reports must wake a supervisor"
        );
        let mut env = valid_env();
        env.insert(
            "SWARM_REPORT_WAKE_STATUSES".into(),
            "completed,unknown".into(),
        );
        assert!(load(&env).is_err(), "wake statuses must be a report subset");

        // the DB path must be absolute.
        let mut env = valid_env();
        env.insert("SWARM_STATE_DB_PATH".into(), "relative/state.db".into());
        assert!(load(&env).is_err(), "state DB path must be absolute");

        // unknown bool values are rejected.
        let mut env = valid_env();
        env.insert("SWARM_TELEGRAM_ENABLED".into(), "maybe".into());
        assert!(load(&env).is_err(), "bool env must parse");
    }

    #[test]
    fn from_env_accepts_edge_cases_in_origins_and_proxies() {
        // "null" is a legitimate origin entry.
        let mut env = valid_env();
        env.insert(
            "SWARM_MCP_ALLOWED_ORIGINS".into(),
            "http://localhost,null".into(),
        );
        assert!(load(&env).is_ok(), "null origin is allowed when listed");

        // socks5 proxies are supported, but credentials are not.
        let mut env = valid_env();
        env.insert(
            "TELEGRAM_PROXY_URL".into(),
            "socks5h://127.0.0.1:1080".into(),
        );
        assert!(load(&env).is_ok(), "socks5h proxy is valid");
        let mut env = valid_env();
        env.insert(
            "TELEGRAM_PROXY_URL".into(),
            "https://user:pass@proxy.example".into(),
        );
        assert!(load(&env).is_err(), "proxy must not embed credentials");
    }

    #[test]
    fn from_env_accepts_a_bracketed_ipv6_host_with_port() {
        let mut env = valid_env();
        env.insert(
            "SWARM_MCP_ALLOWED_HOSTS".into(),
            "[::1]:3004,localhost".into(),
        );
        assert!(load(&env).is_ok(), "IPv6 host with port is valid");
        let mut env = valid_env();
        env.insert("SWARM_MCP_ALLOWED_HOSTS".into(), "[::1]:bad".into());
        assert!(
            load(&env).is_err(),
            "IPv6 host with a malformed port is rejected"
        );
    }
}
