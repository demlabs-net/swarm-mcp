//! Shared test fixtures: temp `SQLite` paths, a complete in-memory `Config`, and
//! a mock Hermes `/v1/runs` HTTP server. Only compiled under `#[cfg(test)]`.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde_json::Value;
use uuid::Uuid;

use crate::config::{
    AgentConfig, Config, ManagerReportWakeMode, Secret, TelegramBacklogMode, TelegramBotMode,
};

/// Unique temp `SQLite` path; clean up with `remove_db_files`.
pub(crate) fn temp_db_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("swarm-mcp-{name}-{}.db", Uuid::new_v4().simple()))
}

pub(crate) async fn remove_db_files(path: &Path) {
    for candidate in [
        path.to_path_buf(),
        path.with_extension("db-wal"),
        path.with_extension("db-shm"),
    ] {
        let _ = tokio::fs::remove_file(candidate).await;
    }
}

/// Minimal but complete role hierarchy:
/// - manager (global authority): orders developer and lead-developer
/// - lead-developer: orders developer (supervisor of developer)
/// - developer: plain executor, emits activity to lead-developer
pub(crate) fn fixture_config(state_db_path: &Path) -> Config {
    let manager_role = "manager".to_string();
    let agent_roles = vec!["developer".to_string(), "lead-developer".to_string()];
    let all_roles = [manager_role.clone()]
        .into_iter()
        .chain(agent_roles.clone())
        .collect::<Vec<_>>();
    let mut agents = BTreeMap::new();
    for role in &all_roles {
        agents.insert(
            role.clone(),
            AgentConfig {
                role: role.clone(),
                api_url: Some("http://127.0.0.1:1".parse().expect("fixture URL")),
                api_key: Some(Secret::new(format!("fixture-api-key-{role}"))),
                api_kind: crate::config::ApiKind::Hermes,
                api_model: None,
                mcp_token: Secret::new(format!("fixture-mcp-token-{role}-0123456789")),
                telegram_bot_token: None,
            },
        );
    }
    Config {
        bind_ip: "127.0.0.1".parse().expect("fixture IP"),
        port: 0,
        allowed_hosts: vec!["localhost".to_string()],
        allowed_origins: vec!["http://localhost".to_string()],
        role_path_template: "/mcp/{role}".to_string(),
        role_paths: all_roles
            .iter()
            .map(|role| (role.clone(), format!("/mcp/{role}")))
            .collect(),
        activity_path: "/activity".to_string(),
        manager_role,
        agent_roles,
        all_roles,
        descriptions: BTreeMap::from([
            ("developer".to_string(), "Developer executor".to_string()),
            (
                "lead-developer".to_string(),
                "Lead developer executor".to_string(),
            ),
        ]),
        order_acl: BTreeMap::from([
            (
                "manager".to_string(),
                vec!["developer".to_string(), "lead-developer".to_string()],
            ),
            ("lead-developer".to_string(), vec!["developer".to_string()]),
        ]),
        global_authorities: BTreeSet::from(["manager".to_string()]),
        activity_routes: BTreeMap::from([(
            "developer".to_string(),
            vec!["lead-developer".to_string()],
        )]),
        agents,
        max_message_chars: 10_000,
        max_request_body_bytes: 64 * 1024,
        api_timeout: Duration::from_secs(5),
        hermes_model_alias: "fixture-model".to_string(),
        state_db_path: state_db_path.to_path_buf(),
        db_max_connections: 4,
        db_busy_timeout: Duration::from_secs(2),
        activity_enabled: true,
        activity_retention_days: 30,
        activity_history_limit: 50,
        activity_stale_after: Duration::from_secs(3600),
        activity_clock_skew: Duration::from_secs(60),
        recent_operations_limit: 50,
        operation_retention_days: 30,
        rate_limit: 100,
        rate_window: Duration::from_secs(60),
        duplicate_window: Duration::from_secs(600),
        messaging_reenable_cooldown: Duration::ZERO,
        max_inflight_dispatches: 16,
        pending_stale_after: Duration::from_secs(300),
        cleanup_interval: Duration::from_secs(300),
        mcp_request_rate_limit: 1_000,
        mcp_request_rate_window: Duration::from_secs(60),
        report_statuses: vec![
            "completed".to_string(),
            "failed".to_string(),
            "in_progress".to_string(),
        ],
        report_wake_statuses: BTreeSet::from(["completed".to_string(), "failed".to_string()]),
        manager_report_wake_mode: ManagerReportWakeMode::Immediate,
        telegram_enabled: false,
        telegram_bot_mode: TelegramBotMode::PerRole,
        telegram_bot_token: None,
        telegram_timeout: Duration::from_secs(10),
        telegram_message_limit: 4096,
        telegram_group_id: None,
        telegram_proxy_url: None,
        telegram_api_base_url: "https://api.telegram.org".parse().expect("fixture URL"),
        outbox_poll_interval: Duration::from_secs(5),
        outbox_max_attempts: 5,
        outbox_batch_size: 10,
        outbox_retention_days: 30,
        telegram_inbound_enabled: false,
        telegram_allowed_users: BTreeSet::new(),
        telegram_inbound_targets: Vec::new(),
        telegram_poll_timeout: Duration::from_secs(25),
        telegram_poll_limit: 100,
        telegram_backlog_mode: TelegramBacklogMode::Discard,
        telegram_inbound_instructions: "execute the Telegram request".to_string(),
        telegram_inbound_template: "update {update_id} from user {user_id}: {message}".to_string(),
        manager_instructions: "You are the swarm manager.".to_string(),
        executor_instructions: "You are an executor.".to_string(),
        authority_instructions: "You may order: {targets}".to_string(),
        executor_run_instructions: "Carry out the order.".to_string(),
        supervisor_report_instructions: "Read the report.".to_string(),
        peer_run_instructions: "Read the message.".to_string(),
        order_template: "{task_id}|{sender}|{recipient}|{message}".to_string(),
        report_template: "{sender}|{recipient}|{task_id}|{status}|{message}".to_string(),
        peer_template: "{message_id}|{sender}|{recipient}|{message}".to_string(),
        log_level: "info".to_string(),
    }
}

/// Behavior of a mock Hermes server: `(bearer, request_body_json) -> (status, response_json)`.
pub(crate) type MockHermes = Arc<dyn Fn(&str, &str) -> (u16, Value) + Send + Sync>;

/// Spawn a mock Hermes `/v1/runs` endpoint on an ephemeral port and return its base URL.
pub(crate) async fn spawn_mock_hermes(behavior: MockHermes) -> String {
    let router = axum::Router::new()
        .route(
            "/v1/runs",
            axum::routing::post({
                let behavior = behavior.clone();
                move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
                    let behavior = behavior.clone();
                    async move {
                        let bearer = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let (status, payload) = behavior(&bearer, &body.to_string());
                        (
                            axum::http::StatusCode::from_u16(status).expect("mock status is valid"),
                            axum::Json(payload),
                        )
                    }
                }
            }),
        )
        .route(
            "/v1/runs/{run_id}",
            axum::routing::get({
                let behavior = behavior.clone();
                move |headers: axum::http::HeaderMap,
                      axum::extract::Path(run_id): axum::extract::Path<String>| {
                    let behavior = behavior.clone();
                    async move {
                        let bearer = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let (status, payload) =
                            behavior(&bearer, &format!("GET /v1/runs/{run_id}"));
                        (
                            axum::http::StatusCode::from_u16(status).expect("mock status is valid"),
                            axum::Json(payload),
                        )
                    }
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            axum::routing::post({
                let behavior = behavior.clone();
                move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<Value>| {
                    let behavior = behavior.clone();
                    async move {
                        let bearer = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let (status, payload) =
                            behavior(&bearer, &format!("POST /v1/chat/completions {body}"));
                        (
                            axum::http::StatusCode::from_u16(status).expect("mock status is valid"),
                            axum::Json(payload),
                        )
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock Hermes");
    let address = listener.local_addr().expect("mock Hermes address");
    tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("mock Hermes serves");
    });
    format!("http://{address}")
}
