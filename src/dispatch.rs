use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{anyhow, ensure};
use chrono::{DateTime, Utc};
use futures::{StreamExt, future::join_all};
use reqwest::{Client, Proxy};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    config::{ApiKind, Config, ManagerReportWakeMode, TelegramBotMode},
    store::{AuditMessage, MediaPayload, OutboxItem, Reservation, Store},
};

#[derive(Clone)]
pub struct Dispatcher {
    config: Arc<Config>,
    store: Store,
    hermes_client: Client,
    telegram_client: Client,
    inflight: Arc<Semaphore>,
    messaging_gate: Arc<RwLock<()>>,
}

enum RunOutcome {
    /// The run finished and produced this final answer.
    Completed(String),
    /// The run finished without a usable answer (failed/cancelled/empty).
    Failed,
    /// The run is still in progress.
    Running,
}

/// Where a dispatch to a role went: a Hermes run or an OpenAI-style
/// initiation (the role answers through the swarm MCP server).
#[derive(Clone, Debug)]
pub enum DispatchHandle {
    Run(String),
    Initiated,
}

impl DispatchHandle {
    /// `("run_id", id)` or `("initiated", "true")` for JSON results.
    fn key_value(&self) -> (&'static str, String) {
        match self {
            DispatchHandle::Run(run_id) => ("run_id", run_id.clone()),
            DispatchHandle::Initiated => ("initiated", "true".to_string()),
        }
    }
}

#[derive(Debug)]
pub struct ToolOutcome {
    pub value: Value,
    pub is_error: bool,
}

#[derive(Debug)]
struct RunFailure {
    error: anyhow::Error,
    indeterminate: bool,
    retry_after_seconds: Option<u64>,
}

impl RunFailure {
    fn rejected(error: anyhow::Error) -> Self {
        Self {
            error,
            indeterminate: false,
            retry_after_seconds: None,
        }
    }

    fn indeterminate(error: anyhow::Error) -> Self {
        Self {
            error,
            indeterminate: true,
            retry_after_seconds: None,
        }
    }

    fn retryable(error: anyhow::Error, retry_after_seconds: u64) -> Self {
        Self {
            error,
            indeterminate: false,
            retry_after_seconds: Some(retry_after_seconds.max(1)),
        }
    }

    fn is_retryable(&self) -> bool {
        self.retry_after_seconds.is_some()
    }

    fn status(&self) -> &'static str {
        if self.indeterminate {
            "indeterminate"
        } else {
            "failed"
        }
    }
}

impl std::fmt::Display for RunFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

#[derive(Debug)]
struct TelegramFailure {
    message: String,
    retry_after_seconds: Option<i64>,
    permanent: bool,
}

impl TelegramFailure {
    fn new(message: impl Into<String>, retry_after_seconds: Option<i64>) -> Self {
        Self {
            message: message.into(),
            retry_after_seconds,
            permanent: false,
        }
    }

    fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retry_after_seconds: None,
            permanent: true,
        }
    }
}

impl std::fmt::Display for TelegramFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// Shared aggregation of per-target broadcast outcomes (`order_all`, `msg_all`,
/// Telegram inbound). A broadcast is `accepted` only when every target run was
/// accepted; `indeterminate` when nothing was accepted and at least one target
/// may have been reached; `partial` when some but not all runs were accepted.
struct BroadcastSummary {
    label: &'static str,
    results: Map<String, Value>,
    succeeded: usize,
    indeterminate: usize,
    retryable: usize,
    retry_after_seconds: u64,
}

impl BroadcastSummary {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            results: Map::new(),
            succeeded: 0,
            indeterminate: 0,
            retryable: 0,
            retry_after_seconds: 0,
        }
    }

    fn push(&mut self, target: String, result: Result<DispatchHandle, RunFailure>) {
        match result {
            Ok(handle) => {
                self.succeeded += 1;
                let (key, value) = handle.key_value();
                self.results.insert(target, json!({ (key): value }));
            }
            Err(error) => {
                warn!(%target, error = %error, "{}", self.label);
                self.indeterminate += usize::from(error.indeterminate);
                let mut result = json!({
                    "error": error.to_string(),
                    "status": error.status(),
                    "recovery_required": error.indeterminate,
                });
                if let Some(retry_after_seconds) = error.retry_after_seconds {
                    self.retryable += 1;
                    self.retry_after_seconds = self.retry_after_seconds.max(retry_after_seconds);
                    result["retryable"] = json!(true);
                    result["retry_after_seconds"] = json!(retry_after_seconds);
                }
                self.results.insert(target, result);
            }
        }
    }

    fn entirely_retryable(&self, total: usize) -> bool {
        total > 0 && self.succeeded == 0 && self.retryable == total
    }

    fn status(&self, total: usize) -> &'static str {
        if self.succeeded == total {
            "accepted"
        } else if self.succeeded == 0 && self.indeterminate > 0 {
            "indeterminate"
        } else if self.succeeded == 0 {
            "failed"
        } else {
            "partial"
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct OrderArgs {
    pub agent: String,
    pub command: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BroadcastArgs {
    pub command: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ReportArgs {
    pub summary: String,
    pub task_id: String,
    #[serde(default = "completed_status")]
    pub status: String,
    pub recipient: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MessageArgs {
    pub agent: String,
    pub message: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MessageAllArgs {
    pub message: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DisableMessagingArgs {
    pub agent: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default = "default_true")]
    pub clear_queue: bool,
}

#[derive(Debug, Deserialize)]
pub struct EnableMessagingArgs {
    pub agent: String,
    pub reason: String,
    pub expected_disabled_at: String,
}

#[derive(Debug, Deserialize)]
pub struct ClearMessageQueueArgs {
    pub agent: String,
    #[serde(default = "default_true")]
    pub include_dead: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramReplyArgs {
    pub message: String,
    #[serde(default)]
    pub files: Vec<TelegramFileArgs>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelegramFileArgs {
    pub filename: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    pub content_b64: String,
}

#[derive(Debug)]
pub struct TelegramInboundArgs {
    pub update_id: i64,
    pub message_id: i64,
    pub user_id: i64,
    pub username: String,
    pub message: String,
    pub targets: Vec<String>,
    /// Source Telegram chat: private chat id for DM, group/channel id otherwise.
    pub chat_id: i64,
}

fn completed_status() -> String {
    "completed".to_string()
}

fn default_true() -> bool {
    true
}

impl Dispatcher {
    pub fn new(config: Arc<Config>, store: Store) -> anyhow::Result<Self> {
        let hermes_client = Client::builder()
            .timeout(config.api_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let mut telegram_builder = Client::builder()
            .timeout(config.telegram_timeout)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(proxy) = &config.telegram_proxy_url {
            telegram_builder = telegram_builder.proxy(Proxy::all(proxy.as_str())?);
        }
        let telegram_client = telegram_builder.build()?;
        Ok(Self {
            inflight: Arc::new(Semaphore::new(config.max_inflight_dispatches)),
            messaging_gate: Arc::new(RwLock::new(())),
            config,
            store,
            hermes_client,
            telegram_client,
        })
    }

    pub async fn order(&self, sender: &str, args: OrderArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        let target = args.agent.trim().to_lowercase();
        let allowed = self
            .config
            .order_acl
            .get(sender)
            .cloned()
            .unwrap_or_default();
        if !allowed.contains(&target) {
            return tool_error(json!({
                "ok": false,
                "error": format!("{sender} is not authorized to order '{target}'"),
                "allowed": allowed,
            }));
        }
        if let Some(outcome) = self
            .messaging_block(sender, std::slice::from_ref(&target))
            .await
        {
            return outcome;
        }
        let command = match self.clean_text(&args.command, "command") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let task_id = format!("task_{}", Uuid::new_v4().simple());
        let targets = vec![target.clone()];
        let fingerprint = fingerprint(&json!({
            "target": target,
            "command": normalize_dispatch_text(&command),
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &task_id,
                sender,
                "order",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        let message = match render_order_template(
            &self.config.order_template,
            &task_id,
            sender,
            &target,
            &command,
        ) {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_dispatch(&task_id, error, sender, "ORDER_FAILED", &target, &command)
                    .await;
            }
        };
        match self
            .dispatch_role(&target, &message, &self.config.executor_run_instructions)
            .await
        {
            Ok(handle) => {
                let (key, value) = handle.key_value();
                let mut result = json!({
                    "ok": true,
                    "task_id": task_id,
                    "authority": sender,
                    "agent": target,
                });
                result[key] = json!(value);
                let audit = self.audit(
                    sender,
                    "ORDER",
                    &target,
                    &format!("{task_id}\n{command}"),
                    &mut result,
                );
                self.finish(&task_id, "accepted", &result, audit, false)
                    .await
            }
            Err(error) => {
                self.fail_run(&task_id, error, sender, "ORDER_FAILED", &target, &command)
                    .await
            }
        }
    }

    pub async fn order_all(&self, sender: &str, args: BroadcastArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        if !self.config.global_authorities.contains(sender) {
            return tool_error(json!({
                "ok": false,
                "error": format!("{sender} has no broadcast authority"),
            }));
        }
        let command = match self.clean_text(&args.command, "command") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let task_id = format!("task_{}", Uuid::new_v4().simple());
        let targets = self
            .config
            .order_acl
            .get(sender)
            .cloned()
            .unwrap_or_default();
        if let Some(outcome) = self.messaging_block(sender, &targets).await {
            return outcome;
        }
        let fingerprint = fingerprint(&json!({
            "targets": targets,
            "command": normalize_dispatch_text(&command),
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &task_id,
                sender,
                "order_all",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        let requests = targets.iter().map(|target| async {
            let result = match render_order_template(
                &self.config.order_template,
                &task_id,
                sender,
                target,
                &command,
            ) {
                Ok(message) => {
                    self.dispatch_role(target, &message, &self.config.executor_run_instructions)
                        .await
                }
                Err(error) => Err(RunFailure::rejected(error)),
            };
            (target.clone(), result)
        });
        let mut summary = BroadcastSummary::new("broadcast dispatch failed");
        for (target, result) in join_all(requests).await {
            summary.push(target, result);
        }
        let ok = summary.succeeded == targets.len();
        let status = summary.status(targets.len());
        let mut result = json!({
            "ok": ok,
            "task_id": task_id,
            "authority": sender,
            "results": summary.results,
        });
        let audit = self.audit(
            sender,
            "ORDER_ALL",
            &targets.join(", "),
            &format!("{task_id}\n{command}"),
            &mut result,
        );
        self.finish(&task_id, status, &result, audit, !ok).await
    }

    pub async fn report(&self, sender: &str, args: ReportArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        if let Some(outcome) = self.messaging_block(sender, &[]).await {
            return outcome;
        }
        let summary = match self.clean_text(&args.summary, "summary") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let task_id = match validate_identifier(&args.task_id, "task_id") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let status = args.status.trim().to_lowercase();
        if !self.config.report_statuses.contains(&status) {
            return tool_error(json!({
                "ok": false,
                "error": "unsupported report status",
                "allowed": self.config.report_statuses,
            }));
        }
        let issuer = match self.store.task_issuer(&task_id, sender).await {
            Ok(Some(value)) => value,
            Ok(None) => {
                return tool_error(json!({
                    "ok": false,
                    "error": "task_id is not an accepted Swarm order assigned to the sending role",
                    "task_id": task_id,
                    "sender": sender,
                }));
            }
            Err(error) => {
                error!(%sender, %task_id, error = %error, "task lineage lookup failed");
                return tool_error(json!({
                    "ok": false,
                    "error": "task lineage is temporarily unavailable",
                }));
            }
        };
        let recipient = args
            .recipient
            .as_deref()
            .unwrap_or(&issuer)
            .trim()
            .to_lowercase();
        if recipient != issuer {
            return tool_error(json!({
                "ok": false,
                "error": "reports must return to the authority that issued the task",
                "issuer": issuer,
                "recipient": recipient,
                "task_id": task_id,
            }));
        }
        if let Some(outcome) = self
            .messaging_block(sender, std::slice::from_ref(&recipient))
            .await
        {
            return outcome;
        }
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let dispatch_id = format!("report_{}", Uuid::new_v4().simple());
        let targets = vec![recipient.clone()];
        let fingerprint = fingerprint(&json!({
            "recipient": recipient,
            "task_id": task_id,
            "status": status,
            "summary": normalize_dispatch_text(&summary),
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &dispatch_id,
                sender,
                "report",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        if !self.config.report_wake_statuses.contains(&status) {
            let mut result = json!({
                "ok": true,
                "report_id": dispatch_id,
                "recipient": recipient,
                "task_id": task_id,
                "status": status,
                "summary": summary,
                "supervisor_woken": false,
            });
            let audit = self.audit(
                sender,
                "REPORT",
                &recipient,
                &format!("{task_id} [{status}]\n{summary}"),
                &mut result,
            );
            return self
                .finish(&dispatch_id, "accepted", &result, audit, false)
                .await;
        }
        if recipient == self.config.manager_role
            && self.config.manager_report_wake_mode == ManagerReportWakeMode::Scheduled
        {
            let mut result = json!({
                "ok": true,
                "report_id": dispatch_id,
                "recipient": recipient,
                "task_id": task_id,
                "status": status,
                "summary": summary,
                "supervisor_woken": false,
                "wake_deferred": true,
                "wake_policy": "scheduled_reconciliation",
            });
            let audit = self.audit(
                sender,
                "REPORT",
                &recipient,
                &format!("{task_id} [{status}]\n{summary}"),
                &mut result,
            );
            return self
                .finish(&dispatch_id, "accepted", &result, audit, false)
                .await;
        }
        let message = match render_template(
            &self.config.report_template,
            &BTreeMap::from([
                ("sender", sender),
                ("recipient", recipient.as_str()),
                ("task_id", task_id.as_str()),
                ("status", status.as_str()),
                ("message", summary.as_str()),
            ]),
        ) {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_dispatch(
                        &dispatch_id,
                        error,
                        sender,
                        "REPORT_FAILED",
                        &recipient,
                        &summary,
                    )
                    .await;
            }
        };
        match self
            .dispatch_role(
                &recipient,
                &message,
                &self.config.supervisor_report_instructions,
            )
            .await
        {
            Ok(handle) => {
                let (key, value) = handle.key_value();
                let mut result = json!({
                    "ok": true,
                    "report_id": dispatch_id,
                    "recipient": recipient,
                    "task_id": task_id,
                    "status": status,
                    "summary": summary,
                    "supervisor_woken": true,
                });
                result[format!("recipient_{key}")] = json!(value);
                let audit = self.audit(
                    sender,
                    "REPORT",
                    &recipient,
                    &format!("{task_id} [{status}]\n{summary}"),
                    &mut result,
                );
                self.finish(&dispatch_id, "accepted", &result, audit, false)
                    .await
            }
            Err(error) if error.is_retryable() => {
                let mut result = json!({
                    "ok": true,
                    "report_id": dispatch_id,
                    "recipient": recipient,
                    "task_id": task_id,
                    "status": status,
                    "summary": summary,
                    "supervisor_woken": false,
                    "wake_deferred": true,
                    "wake_policy": "supervisor_busy",
                    "retry_after_seconds": error.retry_after_seconds,
                });
                let audit = self.audit(
                    sender,
                    "REPORT",
                    &recipient,
                    &format!("{task_id} [{status}]\n{summary}"),
                    &mut result,
                );
                self.finish(&dispatch_id, "accepted", &result, audit, false)
                    .await
            }
            Err(error) => {
                self.fail_run(
                    &dispatch_id,
                    error,
                    sender,
                    "REPORT_FAILED",
                    &recipient,
                    &summary,
                )
                .await
            }
        }
    }

    pub async fn message(&self, sender: &str, args: MessageArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        let target = args.agent.trim().to_lowercase();
        let allowed = self
            .config
            .agent_roles
            .iter()
            .filter(|role| role.as_str() != sender)
            .cloned()
            .collect::<Vec<_>>();
        if !allowed.contains(&target) {
            return tool_error(json!({
                "ok": false,
                "error": "target must be another executor",
                "allowed": allowed,
            }));
        }
        if let Some(outcome) = self
            .messaging_block(sender, std::slice::from_ref(&target))
            .await
        {
            return outcome;
        }
        let message = match self.clean_text(&args.message, "message") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let message_id = format!("msg_{}", Uuid::new_v4().simple());
        let targets = vec![target.clone()];
        let fingerprint = fingerprint(&json!({
            "target": target,
            "message": normalize_dispatch_text(&message),
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &message_id,
                sender,
                "msg_to",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        let body = match render_template(
            &self.config.peer_template,
            &BTreeMap::from([
                ("message_id", message_id.as_str()),
                ("sender", sender),
                ("recipient", target.as_str()),
                ("message", message.as_str()),
            ]),
        ) {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_dispatch(&message_id, error, sender, "MSG_FAILED", &target, &message)
                    .await;
            }
        };
        match self
            .dispatch_role(&target, &body, &self.config.peer_run_instructions)
            .await
        {
            Ok(handle) => {
                let (key, value) = handle.key_value();
                let mut result = json!({
                    "ok": true,
                    "message_id": message_id,
                    "agent": target,
                });
                result[key] = json!(value);
                let audit = self.audit(
                    sender,
                    "MSG",
                    &target,
                    &format!("{message_id}\n{message}"),
                    &mut result,
                );
                self.finish(&message_id, "accepted", &result, audit, false)
                    .await
            }
            Err(error) => {
                self.fail_run(&message_id, error, sender, "MSG_FAILED", &target, &message)
                    .await
            }
        }
    }

    pub async fn message_all(&self, sender: &str, args: MessageAllArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        let message = match self.clean_text(&args.message, "message") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let message_id = format!("msg_{}", Uuid::new_v4().simple());
        let targets = self
            .config
            .agent_roles
            .iter()
            .filter(|role| role.as_str() != sender)
            .cloned()
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return tool_error(json!({
                "ok": false,
                "error": "no peer executors to message",
            }));
        }
        if let Some(outcome) = self.messaging_block(sender, &targets).await {
            return outcome;
        }
        let fingerprint = fingerprint(&json!({
            "targets": targets,
            "message": normalize_dispatch_text(&message),
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &message_id,
                sender,
                "msg_all",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        let requests = targets.iter().map(|target| async {
            let body = render_template(
                &self.config.peer_template,
                &BTreeMap::from([
                    ("message_id", message_id.as_str()),
                    ("sender", sender),
                    ("recipient", target.as_str()),
                    ("message", message.as_str()),
                ]),
            );
            let result = match body {
                Ok(body) => {
                    self.dispatch_role(target, &body, &self.config.peer_run_instructions)
                        .await
                }
                Err(error) => Err(RunFailure::rejected(error)),
            };
            (target.clone(), result)
        });
        let mut summary = BroadcastSummary::new("peer broadcast failed");
        for (target, result) in join_all(requests).await {
            summary.push(target, result);
        }
        let ok = summary.succeeded == targets.len();
        let status = summary.status(targets.len());
        let mut result = json!({
            "ok": ok,
            "message_id": message_id,
            "results": summary.results,
        });
        let audit = self.audit(
            sender,
            "MSG_ALL",
            &targets.join(", "),
            &format!("{message_id}\n{message}"),
            &mut result,
        );
        self.finish(&message_id, status, &result, audit, !ok).await
    }

    pub async fn telegram_inbound(&self, args: TelegramInboundArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        if !self.config.telegram_inbound_enabled {
            return tool_error(json!({
                "ok": false,
                "error": "Telegram inbound is disabled",
            }));
        }
        if args.update_id < 0 || args.message_id <= 0 || args.user_id <= 0 {
            return tool_error(json!({
                "ok": false,
                "error": "Telegram update contains invalid identifiers",
            }));
        }
        let private_chat = args.chat_id == args.user_id;
        let message = match self.clean_text(&args.message, "message") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let mut targets = Vec::new();
        for raw_target in args.targets {
            let target = raw_target.trim().to_lowercase();
            if !self.config.telegram_inbound_targets.contains(&target) {
                return tool_error(json!({
                    "ok": false,
                    "error": format!("Telegram is not authorized to target '{target}'"),
                    "allowed": self.config.telegram_inbound_targets,
                }));
            }
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        if targets.is_empty() {
            return tool_error(json!({
                "ok": false,
                "error": "Telegram command has no targets",
            }));
        }
        if let Some(outcome) = self
            .messaging_block(&self.config.manager_role, &targets)
            .await
        {
            return outcome;
        }

        let dispatch_id = format!("telegram_{}", args.update_id);
        let idempotency_key = format!("telegram-update-{}", args.update_id);
        let fingerprint = fingerprint(&json!({
            "update_id": args.update_id,
            "message_id": args.message_id,
            "user_id": args.user_id,
            "username": args.username,
            "message": message,
            "targets": targets,
            "chat_id": args.chat_id,
        }));
        let manager = self.config.manager_role.clone();
        if let Some(outcome) = self
            .reserve_or_replay(
                &dispatch_id,
                &manager,
                "telegram_inbound",
                &targets,
                Some(&idempotency_key),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        if let Err(error) = self
            .store
            .set_dispatch_telegram_chat(&dispatch_id, args.chat_id)
            .await
        {
            warn!(dispatch_id = %dispatch_id, error = %error, "persisting Telegram source chat failed");
        }

        let update_id = args.update_id.to_string();
        let message_id = args.message_id.to_string();
        let user_id = args.user_id.to_string();
        let requests = targets.iter().map(|target| async {
            let body = render_template(
                &self.config.telegram_inbound_template,
                &BTreeMap::from([
                    ("update_id", update_id.as_str()),
                    ("message_id", message_id.as_str()),
                    ("user_id", user_id.as_str()),
                    ("username", args.username.as_str()),
                    ("recipient", target.as_str()),
                    ("message", message.as_str()),
                ]),
            );
            let result = match body {
                Ok(body) => {
                    self.dispatch_role(target, &body, &self.config.telegram_inbound_instructions)
                        .await
                }
                Err(error) => Err(RunFailure::rejected(error)),
            };
            (target.clone(), result)
        });
        let mut summary = BroadcastSummary::new("Telegram inbound dispatch failed");
        for (target, result) in join_all(requests).await {
            summary.push(target, result);
        }
        // Запоминаем запущенные раны (долговечно, в БД), чтобы их итоговый
        // ответ доставился оператору в исходный чат (см. spawn_run_reply_worker).
        for (target, value) in &summary.results {
            if let Some(run_id) = value.get("run_id").and_then(Value::as_str)
                && let Err(error) = self
                    .store
                    .track_run_reply(run_id, target, args.chat_id)
                    .await
            {
                warn!(run_id = %run_id, error = %error, "tracking run reply failed");
            }
        }
        let ok = summary.succeeded == targets.len();
        let status = summary.status(targets.len());
        let entirely_retryable = summary.entirely_retryable(targets.len());
        let retry_after_seconds = summary.retry_after_seconds;
        let mut result = json!({
            "ok": ok,
            "dispatch_id": dispatch_id,
            "source": "telegram",
            "update_id": args.update_id,
            "message_id": args.message_id,
            "user_id": args.user_id,
            "username": args.username,
            "results": summary.results.clone(),
        });
        if entirely_retryable {
            result["retryable"] = json!(true);
            result["retry_after_seconds"] = json!(retry_after_seconds.max(1));
        }
        let audit = if entirely_retryable {
            // Queue pressure is not an auditable rejection. The Telegram
            // gateway retains this update and will retry it; emitting an
            // outbox item here would spam the shared group on every retry.
            result["telegram"] = json!({
                "queued": false,
                "enabled": self.config.telegram_enabled,
                "delivery": "retrying_inbound",
            });
            None
        } else if private_chat {
            // A private operator conversation must never be copied into the
            // shared Telegram audit group. The dispatch itself stays durable,
            // and its final answer is delivered back to the source chat by the
            // run-reply worker.
            result["telegram"] = json!({
                "queued": false,
                "enabled": self.config.telegram_enabled,
                "privacy": "private_chat",
            });
            None
        } else {
            self.audit(
                &manager,
                "TELEGRAM_INBOUND",
                &targets.join(", "),
                &format!(
                    "{dispatch_id} [{status}]\nTelegram user {} ({}) → {}:\n{message}\n\nDispatch result: {}",
                    args.username,
                    args.user_id,
                    targets.join(", "),
                    Value::Object(summary.results)
                ),
                &mut result,
            )
        };
        self.finish(&dispatch_id, status, &result, audit, !ok).await
    }

    /// Delivers the agent's answer (text and/or one or more files) to the
    /// operator who started the most recent Telegram inbound dispatch
    /// targeting `sender` (used by the `telegram_reply` MCP tool).
    pub async fn telegram_reply(
        &self,
        sender: &str,
        message: String,
        files: Vec<TelegramFileArgs>,
    ) -> ToolOutcome {
        // Files-only replies are valid: the message is optional when files
        // are attached, so only clean a non-empty message.
        let message = if message.trim().is_empty() {
            String::new()
        } else {
            match self.clean_text(&message, "message") {
                Ok(value) => value,
                Err(error) => return tool_error_message(error),
            }
        };
        if message.is_empty() && files.is_empty() {
            return tool_error(json!({
                "ok": false,
                "error": "Telegram reply must not be empty",
            }));
        }
        let mut media = Vec::with_capacity(files.len());
        for file in files {
            if file.filename.is_empty() || file.content_b64.is_empty() {
                return tool_error(json!({
                    "ok": false,
                    "error": "Telegram file must have a filename and content_b64",
                }));
            }
            media.push(MediaPayload {
                filename: file.filename,
                mime_type: file.mime_type,
                content_b64: file.content_b64,
            });
        }
        // MCP-only roles may reply proactively with no prior dispatch: the
        // chat falls back to the latest operator chat, then to the group.
        let chat_id = match self.store.telegram_chat_for_role(sender).await {
            Ok(chat_id) => chat_id,
            Err(error) => {
                warn!(role = %sender, error = %error, "Telegram reply chat lookup failed");
                return tool_error_message(error);
            }
        };
        let audit = AuditMessage {
            id: format!("reply_{}", Uuid::new_v4().simple()),
            sender: sender.to_string(),
            event: "TELEGRAM_REPLY".to_string(),
            recipients: "operator".to_string(),
            text: message,
            chat_id,
            media,
        };
        if let Err(error) = self.store.enqueue_outbox(audit).await {
            warn!(role = %sender, error = %error, "Telegram reply enqueue failed");
            return tool_error_message(error);
        }
        ToolOutcome {
            value: json!({
                "ok": true,
                "queued": true,
                "chat_id": chat_id,
            }),
            is_error: false,
        }
    }

    pub fn spawn_outbox_worker(&self, cancellation: CancellationToken) -> JoinHandle<()> {
        let dispatcher = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(dispatcher.config.outbox_poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = dispatcher.flush_outbox().await {
                            error!(error = %error, "Telegram outbox flush failed");
                        }
                    }
                }
            }
        })
    }

    /// Polls Telegram-dispatched runs and delivers their final answer to the
    /// operator's chat — the reply does not depend on the model calling
    /// `telegram_reply` (`api_server` runs are fire-and-forget).
    pub fn spawn_run_reply_worker(&self, cancellation: CancellationToken) -> JoinHandle<()> {
        let dispatcher = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = dispatcher.flush_run_replies().await {
                            error!(error = %error, "Telegram run reply flush failed");
                        }
                    }
                }
            }
        })
    }

    async fn flush_run_replies(&self) -> anyhow::Result<()> {
        let ttl = Duration::from_secs(6 * 60 * 60);
        let now = Utc::now().timestamp_millis();
        for (run_id, role, chat_id, created_ms) in self.store.due_run_replies().await? {
            // Долгие задачи (медленные модели) живут до 6 часов; просроченные
            // снимаются с трекинга, чтобы поллер не висел вечно.
            if now - created_ms > i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX) {
                self.store.untrack_run_reply(&run_id).await?;
                continue;
            }
            match self.fetch_run_outcome(&run_id, &role).await {
                Ok(outcome) => {
                    self.deliver_run_reply(&run_id, &role, chat_id, outcome)
                        .await;
                }
                // transient HTTP error: retry on the next tick
                Err(error) => {
                    debug!(run_id = %run_id, error = %error, "run reply poll failed; will retry");
                }
            }
        }
        Ok(())
    }

    async fn fetch_run_outcome(&self, run_id: &str, role: &str) -> anyhow::Result<RunOutcome> {
        let agent = self
            .config
            .agents
            .get(role)
            .ok_or_else(|| anyhow!("unknown target role {role}"))?;
        let (Some(api_url), Some(api_key)) = (&agent.api_url, &agent.api_key) else {
            return Ok(RunOutcome::Running);
        };
        let url = format!(
            "{}/v1/runs/{}",
            api_url.as_str().trim_end_matches('/'),
            run_id
        );
        let response = self
            .hermes_client
            .get(&url)
            .bearer_auth(api_key.expose())
            .send()
            .await
            .map_err(|error| anyhow!("run status request failed: {error}"))?;
        if !response.status().is_success() {
            return Ok(RunOutcome::Running);
        }
        let payload: Value = response.json().await?;
        match payload.get("status").and_then(Value::as_str) {
            Some("completed") => Ok(RunOutcome::Completed(
                payload
                    .get("output")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            )),
            Some("failed" | "cancelled") => Ok(RunOutcome::Failed),
            _ => Ok(RunOutcome::Running),
        }
    }

    async fn deliver_run_reply(&self, run_id: &str, role: &str, chat_id: i64, outcome: RunOutcome) {
        let text = match outcome {
            RunOutcome::Completed(output) if !output.is_empty() => output,
            RunOutcome::Completed(_) => {
                format!("⚠️ Агент {role} ответил пустым сообщением.")
            }
            RunOutcome::Failed => {
                format!("⚠️ Агент {role} не смог обработать запрос.")
            }
            RunOutcome::Running => return,
        };
        let audit = AuditMessage {
            id: format!("reply_{}", Uuid::new_v4().simple()),
            sender: role.to_string(),
            event: "TELEGRAM_REPLY".to_string(),
            recipients: "operator".to_string(),
            text,
            chat_id: Some(chat_id),
            media: vec![],
        };
        if let Err(error) = self.store.enqueue_outbox(audit).await {
            warn!(run_id = %run_id, error = %error, "Telegram run reply enqueue failed");
            return;
        }
        if let Err(error) = self.store.untrack_run_reply(run_id).await {
            warn!(run_id = %run_id, error = %error, "untracking run reply failed");
        }
    }

    async fn flush_outbox(&self) -> anyhow::Result<()> {
        if !self.config.telegram_enabled {
            return Ok(());
        }
        for item in self.store.due_outbox(self.config.outbox_batch_size).await? {
            let _messaging_guard = self.messaging_gate.read().await;
            // The due list is a snapshot taken before this per-item gate. A
            // manager may disable and cancel a role between those two points,
            // so re-check the persisted row and every role involved while the
            // read gate prevents a concurrent control transition.
            if !self.store.outbox_delivery_eligible(&item.id).await? {
                continue;
            }
            match self.send_telegram(&item).await {
                Ok(()) => {
                    self.store.mark_outbox_delivered(&item.id).await?;
                    info!(outbox_id = %item.id, "Telegram audit delivered");
                }
                Err(error) => {
                    let attempts = if error.permanent {
                        self.config.outbox_max_attempts
                    } else {
                        item.attempts + 1
                    };
                    warn!(outbox_id = %item.id, attempts, error = %error, "Telegram audit delivery failed");
                    let next_attempt_ms = self
                        .store
                        .mark_outbox_failed(
                            &item.id,
                            attempts,
                            self.config.outbox_max_attempts,
                            &error.to_string(),
                            error.retry_after_seconds,
                        )
                        .await?;
                    if !error.permanent {
                        let sender_scope = match self.config.telegram_bot_mode {
                            TelegramBotMode::Shared => None,
                            TelegramBotMode::PerRole => Some(item.sender.as_str()),
                        };
                        self.store
                            .defer_pending_outbox_until(sender_scope, next_attempt_ms)
                            .await?;
                        // `due_outbox` is a snapshot. Continuing would ignore the
                        // newly persisted transport backoff for rows already in
                        // this batch and hammer the same Telegram transport.
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_telegram(&self, item: &OutboxItem) -> Result<(), TelegramFailure> {
        let token = self
            .config
            .telegram_token_for(&item.sender)
            .ok_or_else(|| {
                TelegramFailure::permanent("Telegram bot token is not configured for delivery mode")
            })?;
        let group = self
            .config
            .telegram_group_id
            .as_ref()
            .ok_or_else(|| TelegramFailure::permanent("Telegram group is not configured"))?;
        let api_base = self
            .config
            .telegram_api_base_url
            .as_str()
            .trim_end_matches('/');
        let chat_id = item
            .chat_id
            .map_or_else(|| group.clone(), |chat_id| chat_id.to_string());
        if !item.media.is_empty() {
            return self
                .send_telegram_media(api_base, token.expose(), &chat_id, item)
                .await;
        }
        let url = format!("{api_base}/bot{}/sendMessage", token.expose());
        let chunks = telegram_chunks_for_item(item, self.config.telegram_message_limit);
        let start = usize::try_from(item.next_chunk).unwrap_or(usize::MAX);
        if start > chunks.len() {
            return Err(TelegramFailure::permanent(
                "Telegram outbox chunk cursor is invalid",
            ));
        }
        for (index, chunk) in chunks.into_iter().enumerate().skip(start) {
            let response = self
                .telegram_client
                .post(&url)
                .json(&json!({
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_web_page_preview": true,
                }))
                .send()
                .await
                .map_err(|_| TelegramFailure::new("Telegram request failed", None))?;
            validate_telegram_response(response).await?;
            self.store
                .mark_outbox_chunk_sent(&item.id, i64::try_from(index + 1).unwrap_or(i64::MAX))
                .await
                .map_err(|error| {
                    TelegramFailure::new(format!("outbox checkpoint failed: {error}"), None)
                })?;
        }
        Ok(())
    }

    /// Deliver an attached file as a single sendPhoto (image/*) or
    /// sendDocument message; the item text becomes the caption (≤1024 chars).
    /// Deliver attached files: one sendPhoto (image/*) or sendDocument message
    /// per file; the item text is the caption of the first file (≤1024 chars).
    /// The outbox chunk cursor tracks the number of files already sent, so a
    /// retry after a mid-batch failure resumes without re-sending earlier
    /// files.
    async fn send_telegram_media(
        &self,
        api_base: &str,
        token: &str,
        chat_id: &str,
        item: &OutboxItem,
    ) -> Result<(), TelegramFailure> {
        use base64::Engine as _;
        let caption: String = item.text.chars().take(1024).collect();
        let start = usize::try_from(item.next_chunk).unwrap_or(usize::MAX);
        if start > item.media.len() {
            return Err(TelegramFailure::permanent(
                "Telegram media chunk cursor is invalid",
            ));
        }
        for (index, media) in item.media.iter().enumerate().skip(start) {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&media.content_b64)
                .map_err(|_| TelegramFailure::new("Telegram media base64 decode failed", None))?;
            let mime = media
                .mime_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".into());
            let is_image = mime.starts_with("image/");
            let method = if is_image {
                "sendPhoto"
            } else {
                "sendDocument"
            };
            let field = if is_image { "photo" } else { "document" };
            let part = reqwest::multipart::Part::bytes(bytes)
                .file_name(media.filename.clone())
                .mime_str(&mime)
                .map_err(|error| {
                    TelegramFailure::new(format!("media mime failed: {error}"), None)
                })?;
            let form = reqwest::multipart::Form::new()
                .text("chat_id", chat_id.to_string())
                .text(
                    "caption",
                    if index == 0 {
                        caption.clone()
                    } else {
                        String::new()
                    },
                )
                .part(field, part);
            let url = format!("{api_base}/bot{token}/{method}");
            let response = self
                .telegram_client
                .post(&url)
                .multipart(form)
                .send()
                .await
                .map_err(|_| TelegramFailure::new("Telegram media request failed", None))?;
            validate_telegram_response(response).await?;
            // Checkpoint per file so a retry resumes after this one.
            self.store
                .mark_outbox_chunk_sent(&item.id, i64::try_from(index + 1).unwrap_or(i64::MAX))
                .await
                .map_err(|error| {
                    TelegramFailure::new(format!("outbox checkpoint failed: {error}"), None)
                })?;
        }
        Ok(())
    }

    /// Dispatch a message to a role: Hermes `api_server` run (current
    /// mechanism) or OpenAI-compatible initiation for third-party roles.
    async fn dispatch_role(
        &self,
        role: &str,
        message: &str,
        instructions: &str,
    ) -> Result<DispatchHandle, RunFailure> {
        let agent = self
            .config
            .agents
            .get(role)
            .ok_or_else(|| RunFailure::rejected(anyhow!("unknown target role")))?;
        match agent.api_kind {
            ApiKind::OpenAi => {
                self.initiate_openai(agent, message, instructions).await?;
                Ok(DispatchHandle::Initiated)
            }
            ApiKind::Hermes => {
                let run_id = self.start_run(role, message, instructions).await?;
                Ok(DispatchHandle::Run(run_id))
            }
            ApiKind::Mcp => Err(RunFailure::rejected(anyhow!(
                "role {role} is MCP-only and initiates interaction itself; it has no dispatch endpoint"
            ))),
        }
    }

    /// Fire-and-forget initiation of a third-party role via an
    /// OpenAI-compatible endpoint. The role then works and sends its answers
    /// through the swarm MCP server (`telegram_reply`) at any point.
    async fn initiate_openai(
        &self,
        agent: &crate::config::AgentConfig,
        message: &str,
        instructions: &str,
    ) -> Result<(), RunFailure> {
        let api_url = agent
            .api_url
            .as_ref()
            .ok_or_else(|| RunFailure::rejected(anyhow!("role {} has no api_url", agent.role)))?;
        let api_key = agent
            .api_key
            .as_ref()
            .ok_or_else(|| RunFailure::rejected(anyhow!("role {} has no api key", agent.role)))?;
        let url = format!(
            "{}/v1/chat/completions",
            api_url.as_str().trim_end_matches('/')
        );
        let model = agent
            .api_model
            .clone()
            .unwrap_or_else(|| self.config.hermes_model_alias.clone());
        let response = self
            .hermes_client
            .post(&url)
            .bearer_auth(api_key.expose())
            .json(&json!({
                "model": model,
                "messages": [
                    {"role": "system", "content": instructions},
                    {"role": "user", "content": message},
                ],
                "stream": false,
            }))
            .send()
            .await
            .map_err(|error| {
                RunFailure::indeterminate(anyhow!("openai initiate to {}: {error}", agent.role))
            })?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(RunFailure::rejected(anyhow!(
                "openai initiate to {} returned HTTP {status}",
                agent.role
            )));
        }
        Ok(())
    }

    async fn start_run(
        &self,
        role: &str,
        message: &str,
        instructions: &str,
    ) -> Result<String, RunFailure> {
        let agent = self
            .config
            .agents
            .get(role)
            .ok_or_else(|| RunFailure::rejected(anyhow!("unknown target role")))?;
        let api_url = agent
            .api_url
            .as_ref()
            .ok_or_else(|| RunFailure::rejected(anyhow!("role {role} has no api_url")))?;
        let api_key = agent
            .api_key
            .as_ref()
            .ok_or_else(|| RunFailure::rejected(anyhow!("role {role} has no api key")))?;
        let _permit = tokio::time::timeout(self.config.api_timeout, self.inflight.acquire())
            .await
            .map_err(|_| RunFailure::rejected(anyhow!("dispatcher is saturated")))?
            .map_err(|_| RunFailure::rejected(anyhow!("dispatcher is shutting down")))?;
        let url = format!("{}/v1/runs", api_url.as_str().trim_end_matches('/'));
        let response = self
            .hermes_client
            .post(url)
            .bearer_auth(api_key.expose())
            .json(&json!({
                "model": self.config.hermes_model_alias,
                "input": message,
                "instructions": instructions,
            }))
            .send()
            .await
            .map_err(|error| {
                RunFailure::indeterminate(anyhow!("dispatch run to {role}: {error}"))
            })?;
        if response.status() != reqwest::StatusCode::ACCEPTED {
            let status = response.status();
            let retry_after_seconds = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1);
            let error = anyhow!("{role} API returned HTTP {status}");
            return Err(if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                RunFailure::retryable(error, retry_after_seconds)
            } else if status.is_server_error() {
                RunFailure::indeterminate(error)
            } else {
                RunFailure::rejected(error)
            });
        }
        if let Some(length) = response.content_length()
            && length > u64::try_from(self.config.max_request_body_bytes).unwrap_or(u64::MAX)
        {
            return Err(RunFailure::indeterminate(anyhow!(
                "{role} API response is too large"
            )));
        }
        let mut body = Vec::with_capacity(
            response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0)
                .min(self.config.max_request_body_bytes),
        );
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| {
                RunFailure::indeterminate(anyhow!("read Hermes run response: {error}"))
            })?;
            let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
                RunFailure::indeterminate(anyhow!("{role} API response is too large"))
            })?;
            if next_len > self.config.max_request_body_bytes {
                return Err(RunFailure::indeterminate(anyhow!(
                    "{role} API response is too large"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        let payload: Value = serde_json::from_slice(&body).map_err(|error| {
            RunFailure::indeterminate(anyhow!("decode Hermes run response: {error}"))
        })?;
        payload
            .get("run_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                RunFailure::indeterminate(anyhow!("{role} API returned an invalid run identifier"))
            })
    }

    pub async fn disable_messaging(&self, sender: &str, args: DisableMessagingArgs) -> ToolOutcome {
        if let Err(outcome) = self.authorize_messaging_control(sender) {
            return outcome;
        }
        let target = match self.control_target(&args.agent) {
            Ok(target) => target,
            Err(outcome) => return outcome,
        };
        let reason = if args.reason.trim().is_empty() {
            "disabled by swarm manager".to_string()
        } else {
            match Self::clean_control_reason(&args.reason) {
                Ok(reason) => reason,
                Err(error) => return tool_error_message(error),
            }
        };
        let _messaging_guard = self.messaging_gate.write().await;
        match self
            .store
            .set_role_messaging(&target, false, sender, &reason, args.clear_queue)
            .await
        {
            Ok(value) => ToolOutcome {
                value,
                is_error: false,
            },
            Err(error) => {
                error!(%sender, %target, error = %error, "disable role messaging failed");
                tool_error(json!({"ok": false, "error": "persistent messaging control failed"}))
            }
        }
    }

    pub async fn enable_messaging(&self, sender: &str, args: EnableMessagingArgs) -> ToolOutcome {
        if let Err(outcome) = self.authorize_messaging_control(sender) {
            return outcome;
        }
        let target = match self.control_target(&args.agent) {
            Ok(target) => target,
            Err(outcome) => return outcome,
        };
        let reason = match Self::clean_control_reason(&args.reason) {
            Ok(reason) => reason,
            Err(error) => return tool_error_message(error),
        };
        let expected_disabled_ms = match DateTime::parse_from_rfc3339(&args.expected_disabled_at) {
            Ok(value) => value.timestamp_millis(),
            Err(_) => {
                return tool_error(json!({
                    "ok": false,
                    "error": "expected_disabled_at must be the exact RFC 3339 changed_at value from swarm://messaging",
                }));
            }
        };
        let _messaging_guard = self.messaging_gate.write().await;
        let state = match self.store.role_messaging_state(&target).await {
            Ok(state) => state,
            Err(error) => {
                error!(%sender, %target, error = %error, "read role messaging state failed");
                return tool_error(
                    json!({"ok": false, "error": "persistent messaging control failed"}),
                );
            }
        };
        let Some((enabled, changed_ms)) = state else {
            return tool_error(json!({
                "ok": false,
                "error": "role has no disabled messaging state; inspect swarm://messaging",
            }));
        };
        if enabled {
            return tool_error(json!({
                "ok": false,
                "error": "messaging is already enabled; no action was taken",
            }));
        }
        if changed_ms != expected_disabled_ms {
            return tool_error(json!({
                "ok": false,
                "error": "messaging state changed after it was inspected; read swarm://messaging again",
            }));
        }
        let now = Utc::now().timestamp_millis();
        let cooldown_ms =
            i64::try_from(self.config.messaging_reenable_cooldown.as_millis()).unwrap_or(i64::MAX);
        let remaining_ms = cooldown_ms.saturating_sub(now.saturating_sub(changed_ms));
        if remaining_ms > 0 {
            return tool_error(json!({
                "ok": false,
                "error": "messaging re-enable cooldown is active; stop the current run and wait for an explicit human resume request",
                "retry_after_seconds": (remaining_ms.saturating_add(999)) / 1000,
            }));
        }
        match self
            .store
            .set_role_messaging(&target, true, sender, &reason, false)
            .await
        {
            Ok(value) => ToolOutcome {
                value,
                is_error: false,
            },
            Err(error) => {
                error!(%sender, %target, error = %error, "enable role messaging failed");
                tool_error(json!({"ok": false, "error": "persistent messaging control failed"}))
            }
        }
    }

    pub async fn clear_message_queue(
        &self,
        sender: &str,
        args: ClearMessageQueueArgs,
    ) -> ToolOutcome {
        if let Err(outcome) = self.authorize_messaging_control(sender) {
            return outcome;
        }
        let target = match self.control_target(&args.agent) {
            Ok(target) => target,
            Err(outcome) => return outcome,
        };
        let _messaging_guard = self.messaging_gate.write().await;
        match self
            .store
            .clear_role_outbox(&target, args.include_dead, sender)
            .await
        {
            Ok(value) => ToolOutcome {
                value,
                is_error: false,
            },
            Err(error) => {
                error!(%sender, %target, error = %error, "clear role message queue failed");
                tool_error(json!({"ok": false, "error": "persistent queue control failed"}))
            }
        }
    }

    async fn messaging_block(&self, sender: &str, targets: &[String]) -> Option<ToolOutcome> {
        match self.store.role_messaging_enabled(sender).await {
            Ok(false) => {
                return Some(tool_error(json!({
                    "ok": false,
                    "error": "messaging is disabled for the sending role",
                    "agent": sender,
                    "messaging_enabled": false,
                    "action_required": "stop_current_run_and_escalate_to_human",
                })));
            }
            Ok(true) => {}
            Err(error) => {
                error!(%sender, error = %error, "read sender messaging state failed");
                return Some(tool_error(json!({
                    "ok": false,
                    "error": "persistent messaging state is unavailable",
                })));
            }
        }
        match self.store.disabled_roles(targets).await {
            Ok(disabled) if disabled.is_empty() => None,
            Ok(disabled) => Some(tool_error(json!({
                "ok": false,
                "error": "messaging is disabled for one or more target roles",
                "disabled_agents": disabled,
                "action_required": "stop_current_run_and_escalate_to_human",
            }))),
            Err(error) => {
                error!(%sender, error = %error, "read target messaging state failed");
                Some(tool_error(json!({
                    "ok": false,
                    "error": "persistent messaging state is unavailable",
                })))
            }
        }
    }

    fn authorize_messaging_control(&self, sender: &str) -> Result<(), ToolOutcome> {
        if sender != self.config.manager_role {
            return Err(tool_error(json!({
                "ok": false,
                "error": "messaging controls require the swarm manager role",
            })));
        }
        Ok(())
    }

    fn control_target(&self, value: &str) -> Result<String, ToolOutcome> {
        let target = value.trim().to_lowercase();
        if !self.config.agent_roles.contains(&target) {
            return Err(tool_error(json!({
                "ok": false,
                "error": "messaging controls apply only to configured executor roles",
                "allowed": self.config.agent_roles,
            })));
        }
        Ok(target)
    }

    fn clean_control_reason(value: &str) -> anyhow::Result<String> {
        let value = value.trim();
        ensure!(!value.is_empty(), "reason must not be empty");
        ensure!(
            value.chars().count() <= 500,
            "reason exceeds 500 characters"
        );
        Ok(value.to_string())
    }

    async fn reserve_or_replay(
        &self,
        id: &str,
        sender: &str,
        kind: &str,
        targets: &[String],
        idempotency_key: Option<&str>,
        fingerprint: &str,
    ) -> Option<ToolOutcome> {
        match self
            .store
            .reserve_dispatch_guarded(
                id,
                sender,
                kind,
                targets,
                idempotency_key,
                fingerprint,
                self.config.rate_limit,
                self.config.rate_window,
                self.config.duplicate_window,
            )
            .await
        {
            Ok(Reservation::Reserved) => None,
            Ok(Reservation::Existing(mut value)) => {
                if let Some(object) = value.as_object_mut() {
                    object.insert("deduplicated".to_string(), Value::Bool(true));
                }
                Some(ToolOutcome {
                    is_error: !value.get("ok").and_then(Value::as_bool).unwrap_or(false),
                    value,
                })
            }
            Ok(Reservation::Pending { dispatch_id }) => Some(tool_error(json!({
                "ok": false,
                "pending": true,
                "dispatch_id": dispatch_id,
                "error": "an operation with this idempotency key is still pending",
            }))),
            Ok(Reservation::Conflict) => Some(tool_error(json!({
                "ok": false,
                "error": "idempotency key was already used with different arguments",
            }))),
            Ok(Reservation::RateLimited {
                retry_after_seconds,
            }) => Some(tool_error(json!({
                "ok": false,
                "error": "dispatch rate limit exceeded",
                "retryable": true,
                "retry_after_seconds": retry_after_seconds,
            }))),
            Err(error) => {
                error!(%sender, %kind, error = %error, "dispatch reservation failed");
                Some(tool_error(json!({
                    "ok": false,
                    "error": "persistent dispatch reservation failed",
                })))
            }
        }
    }

    fn audit(
        &self,
        sender: &str,
        event: &str,
        recipients: &str,
        text: &str,
        result: &mut Value,
    ) -> Option<AuditMessage> {
        self.audit_with_chat(sender, event, recipients, text, None, result)
    }

    fn audit_with_chat(
        &self,
        sender: &str,
        event: &str,
        recipients: &str,
        text: &str,
        chat_id: Option<i64>,
        result: &mut Value,
    ) -> Option<AuditMessage> {
        if !self.config.telegram_enabled {
            result["telegram"] = json!({"queued": false, "enabled": false});
            return None;
        }
        let id = format!("audit_{}", Uuid::new_v4().simple());
        result["telegram"] = json!({"queued": true, "outbox_id": id});
        Some(AuditMessage {
            id,
            sender: sender.to_string(),
            event: event.to_string(),
            recipients: recipients.to_string(),
            text: text.to_string(),
            chat_id,
            media: vec![],
        })
    }

    async fn finish(
        &self,
        id: &str,
        status: &str,
        result: &Value,
        audit: Option<AuditMessage>,
        is_error: bool,
    ) -> ToolOutcome {
        if let Err(error) = self.store.finish_dispatch(id, status, result, audit).await {
            error!(dispatch_id = %id, error = %error, "persisting dispatch result failed");
            if let Err(recovery_error) = self
                .store
                .mark_dispatch_indeterminate(
                    id,
                    "downstream accepted the operation, but persistence failed",
                )
                .await
            {
                error!(dispatch_id = %id, error = %recovery_error, "marking dispatch indeterminate failed");
            }
            return tool_error(json!({
                "ok": false,
                "error": "downstream accepted the operation, but persistence failed",
                "dispatch_id": id,
                "recovery_required": true,
            }));
        }
        ToolOutcome {
            value: result.clone(),
            is_error,
        }
    }

    async fn fail_dispatch(
        &self,
        id: &str,
        error_value: anyhow::Error,
        sender: &str,
        event: &str,
        recipients: &str,
        text: &str,
    ) -> ToolOutcome {
        warn!(dispatch_id = %id, error = %error_value, "dispatch failed");
        let mut result = json!({"ok": false, "error": error_value.to_string()});
        let audit = self.audit(
            sender,
            event,
            recipients,
            &format!("{id}\n{text}"),
            &mut result,
        );
        self.finish(id, "failed", &result, audit, true).await
    }

    async fn fail_run(
        &self,
        id: &str,
        failure: RunFailure,
        sender: &str,
        event: &str,
        recipients: &str,
        text: &str,
    ) -> ToolOutcome {
        let status = failure.status();
        warn!(dispatch_id = %id, error = %failure, %status, "dispatch run failed");
        let mut result = json!({
            "ok": false,
            "error": failure.to_string(),
            "recovery_required": failure.indeterminate,
        });
        let audit = self.audit(
            sender,
            event,
            recipients,
            &format!("{id}\n{text}"),
            &mut result,
        );
        self.finish(id, status, &result, audit, true).await
    }

    fn clean_text(&self, value: &str, field: &str) -> anyhow::Result<String> {
        let value = value.trim();
        ensure!(!value.is_empty(), "{field} must not be empty");
        ensure!(
            value.chars().count() <= self.config.max_message_chars,
            "{field} exceeds {} characters",
            self.config.max_message_chars
        );
        Ok(value.to_string())
    }
}

async fn validate_telegram_response(response: reqwest::Response) -> Result<(), TelegramFailure> {
    const MAX_RESPONSE_BYTES: usize = 64 * 1024;

    let status = response.status();
    let header_retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok());
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| TelegramFailure::new("Telegram response failed", None))?;
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| TelegramFailure::new("Telegram response is too large", None))?;
        if next_len > MAX_RESPONSE_BYTES {
            return Err(if telegram_status_is_retryable(status) {
                TelegramFailure::new("Telegram response is too large", None)
            } else {
                TelegramFailure::permanent("Telegram response is too large")
            });
        }
        body.extend_from_slice(&chunk);
    }
    let payload = serde_json::from_slice::<Value>(&body).ok();
    let retry_after_seconds = payload
        .as_ref()
        .and_then(|value| value.pointer("/parameters/retry_after"))
        .and_then(Value::as_i64)
        .or(header_retry_after)
        .map(|seconds| seconds.clamp(1, 86_400));
    if !status.is_success() {
        let message = format!("Telegram API returned HTTP {status}");
        if telegram_status_is_retryable(status) {
            return Err(TelegramFailure::new(message, retry_after_seconds));
        }
        return Err(TelegramFailure::permanent(message));
    }
    if payload.as_ref().and_then(|value| value.get("ok")) != Some(&Value::Bool(true)) {
        return Err(TelegramFailure::permanent(
            "Telegram API returned an invalid success response",
        ));
    }
    Ok(())
}

fn telegram_status_is_retryable(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status.is_server_error()
}

pub fn parse_arguments<T: for<'de> Deserialize<'de>>(
    arguments: Option<Map<String, Value>>,
) -> Result<T, ToolOutcome> {
    serde_json::from_value(Value::Object(arguments.unwrap_or_default())).map_err(|error| {
        tool_error(json!({
            "ok": false,
            "error": format!("invalid tool arguments: {error}"),
        }))
    })
}

fn validate_idempotency(value: Option<&str>) -> anyhow::Result<Option<String>> {
    value
        .map(|key| validate_identifier(key, "idempotency_key"))
        .transpose()
}

pub(crate) const MAX_IDENTIFIER_BYTES: usize = 160;

pub(crate) fn validate_identifier(value: &str, field: &str) -> anyhow::Result<String> {
    let value = value.trim();
    ensure!(!value.is_empty(), "{field} must not be empty");
    ensure!(
        value.len() <= MAX_IDENTIFIER_BYTES,
        "{field} exceeds {MAX_IDENTIFIER_BYTES} bytes"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte)),
        "{field} contains unsupported characters"
    );
    Ok(value.to_string())
}

fn fingerprint(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string());
    format!("{:x}", hasher.finalize())
}

fn normalize_dispatch_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn render_template(
    template: &str,
    values: &BTreeMap<&str, &str>,
) -> anyhow::Result<String> {
    let template = template.replace("\\n", "\n");
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template.as_str();
    while let Some(start) = rest.find('{') {
        rendered.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let end = after
            .find('}')
            .ok_or_else(|| anyhow!("template contains an unterminated placeholder"))?;
        let key = &after[..end];
        let value = values
            .get(key)
            .ok_or_else(|| anyhow!("template contains an unknown placeholder: {key}"))?;
        rendered.push_str(value);
        rest = &after[end + 1..];
    }
    ensure!(
        !rest.contains('}'),
        "template contains an unmatched closing brace"
    );
    rendered.push_str(rest);
    Ok(rendered)
}

fn render_order_template(
    template: &str,
    task_id: &str,
    sender: &str,
    recipient: &str,
    message: &str,
) -> anyhow::Result<String> {
    render_template(
        template,
        &BTreeMap::from([
            ("task_id", task_id),
            ("sender", sender),
            ("recipient", recipient),
            ("message", message),
        ]),
    )
}

fn telegram_chunks(header: &str, text: &str, limit: usize) -> Vec<String> {
    let header = header.chars().take(limit / 2).collect::<String>();
    let prefix = format!("{header}\n");
    let room = limit.saturating_sub(prefix.chars().count()).max(1);
    if text.is_empty() {
        return vec![header.clone()];
    }
    let characters = text.chars().collect::<Vec<_>>();
    characters
        .chunks(room)
        .map(|chunk| format!("{prefix}{}", chunk.iter().collect::<String>()))
        .collect()
}

fn telegram_plain_chunks(text: &str, limit: usize) -> Vec<String> {
    let room = limit.max(1);
    if text.is_empty() {
        return vec![String::new()];
    }
    text.chars()
        .collect::<Vec<_>>()
        .chunks(room)
        .map(|chunk| chunk.iter().collect::<String>())
        .collect()
}

fn telegram_chunks_for_item(item: &OutboxItem, limit: usize) -> Vec<String> {
    // Telegram private chats have positive IDs. Direct agent replies should
    // read like normal conversation, while group-bound messages retain their
    // audit envelope and attribution.
    if item.event == "TELEGRAM_REPLY" && item.chat_id.is_some_and(|chat_id| chat_id > 0) {
        telegram_plain_chunks(&item.text, limit)
    } else {
        let header = format!("[{}] {} -> {}", item.event, item.sender, item.recipients);
        telegram_chunks(&header, &item.text, limit)
    }
}

fn tool_error_message(error: anyhow::Error) -> ToolOutcome {
    tool_error(json!({"ok": false, "error": error.to_string()}))
}

fn tool_error(value: Value) -> ToolOutcome {
    ToolOutcome {
        value,
        is_error: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_substitution_is_single_pass() {
        let values = BTreeMap::from([("message", "literal {sender}"), ("sender", "manager")]);
        assert_eq!(
            render_template("{sender}: {message}", &values).unwrap(),
            "manager: literal {sender}"
        );
    }

    #[test]
    fn template_rejects_unknown_and_unbalanced_placeholders() {
        let values = BTreeMap::from([("message", "hello")]);
        assert!(render_template("{unknown}", &values).is_err());
        assert!(render_template("{message", &values).is_err());
        assert!(render_template("message}", &values).is_err());
    }

    #[test]
    fn telegram_chunks_preserve_unicode_boundaries_and_limits() {
        let chunks = telegram_chunks("[MSG] designer -> developer", &"🦀".repeat(2000), 512);
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 512));
        let reconstructed = chunks
            .iter()
            .map(|chunk| chunk.split_once('\n').unwrap().1)
            .collect::<String>();
        assert_eq!(reconstructed, "🦀".repeat(2000));
    }

    #[test]
    fn private_telegram_reply_chunks_are_plain() {
        let mut item = outbox_item();
        item.event = "TELEGRAM_REPLY".to_string();
        item.recipients = "operator".to_string();
        item.text = "Личный ответ 🦀".repeat(100);
        item.chat_id = Some(42);

        let chunks = telegram_chunks_for_item(&item, 128);
        assert!(chunks.len() > 1);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 128));
        assert!(chunks.iter().all(|chunk| !chunk.contains("TELEGRAM_REPLY")));
        assert_eq!(chunks.concat(), item.text);
    }

    #[test]
    fn group_telegram_reply_keeps_audit_header() {
        let mut item = outbox_item();
        item.event = "TELEGRAM_REPLY".to_string();
        item.recipients = "operator".to_string();
        item.text = "group reply".to_string();
        item.chat_id = Some(-100_123);

        assert_eq!(
            telegram_chunks_for_item(&item, 512),
            vec!["[TELEGRAM_REPLY] manager -> operator\ngroup reply"]
        );
    }

    #[test]
    fn idempotency_keys_are_normalized_before_storage() {
        assert_eq!(
            validate_idempotency(Some("  stable-key  ")).unwrap(),
            Some("stable-key".to_string())
        );
    }

    #[test]
    fn fingerprint_is_deterministic_and_key_order_independent() {
        let first = fingerprint(&json!({"target": "developer", "command": "do it"}));
        let second = fingerprint(&json!({"target": "developer", "command": "do it"}));
        assert_eq!(first, second);
        assert_eq!(first.len(), 64, "SHA-256 hex");
        // serde_json::Map serializes keys sorted (BTreeMap), so object key
        // insertion order must never change the fingerprint.
        assert_eq!(
            fingerprint(&json!({"a": 1, "b": 2})),
            fingerprint(&json!({"b": 2, "a": 1}))
        );
        assert_ne!(
            fingerprint(&json!({"command": "do it"})),
            fingerprint(&json!({"command": "do it "}))
        );
    }

    #[test]
    fn telegram_chunks_bound_an_oversized_header() {
        let chunks = telegram_chunks(&"h".repeat(10_000), "payload", 512);
        assert!(chunks.iter().all(|chunk| chunk.chars().count() <= 512));
    }

    #[test]
    fn order_template_uses_the_actual_recipient() {
        assert_eq!(
            render_order_template(
                "{task_id}|{sender}|{recipient}|{message}",
                "task_1",
                "manager",
                "designer",
                "make a mockup",
            )
            .unwrap(),
            "task_1|manager|designer|make a mockup"
        );
    }

    use std::collections::BTreeSet;

    use crate::testutil;
    use sqlx::Row;

    /// Build a dispatcher whose agents all point at one mock Hermes server.
    async fn dispatcher_with_mock(
        behavior: testutil::MockHermes,
    ) -> anyhow::Result<(Dispatcher, Store, std::path::PathBuf)> {
        let path = testutil::temp_db_path("dispatch");
        let mut config = testutil::fixture_config(&path);
        let mock = testutil::spawn_mock_hermes(behavior).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;
        Ok((dispatcher, store, path))
    }

    #[tokio::test]
    async fn order_accepts_and_persists_run() -> anyhow::Result<()> {
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen_clone = seen.clone();
        let behavior: testutil::MockHermes = Arc::new(move |bearer, body| {
            seen_clone
                .lock()
                .expect("mock log")
                .push(format!("{bearer} {body}"));
            (202, json!({"run_id": "run-1"}))
        });
        let (dispatcher, store, path) = dispatcher_with_mock(behavior).await?;

        let outcome = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "  do the thing  ".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["run_id"], json!("run-1"));
        let task_id = outcome.value["task_id"]
            .as_str()
            .expect("task id")
            .to_string();
        assert!(task_id.starts_with("task_"));

        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(&task_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "accepted");

        {
            let log = seen.lock().expect("mock log");
            assert!(
                log.iter()
                    .any(|entry| entry.contains("fixture-api-key-developer")),
                "the target agent's API key is sent as the bearer token; log: {log:?}"
            );
            assert!(
                log.iter().any(|entry| entry.contains("fixture-model")),
                "model alias is passed through"
            );
            assert!(
                log.iter().any(|entry| entry.contains("do the thing")),
                "trimmed command reaches the agent"
            );
        }
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn order_denies_unauthorized_targets() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "run"})))).await?;

        // developer has no order ACL entry at all.
        let outcome = dispatcher
            .order(
                "developer",
                OrderArgs {
                    agent: "lead-developer".to_string(),
                    command: "x".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert!(
            outcome.value["error"]
                .as_str()
                .unwrap()
                .contains("not authorized")
        );

        // manager cannot order itself.
        let outcome = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "manager".to_string(),
                    command: "x".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn messaging_controls_require_manager_and_gate_both_directions() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_mock(Arc::new(move |_, _| {
            calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (202, json!({"run_id": "run-after-enable"}))
        }))
        .await?;

        let unauthorized = dispatcher
            .disable_messaging(
                "developer",
                DisableMessagingArgs {
                    agent: "lead-developer".to_string(),
                    reason: "not allowed".to_string(),
                    clear_queue: true,
                },
            )
            .await;
        assert!(unauthorized.is_error);
        assert!(store.role_messaging_enabled("lead-developer").await?);

        let disabled = dispatcher
            .disable_messaging(
                "manager",
                DisableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "contain feedback loop".to_string(),
                    clear_queue: true,
                },
            )
            .await;
        assert!(!disabled.is_error, "unexpected: {disabled:?}");
        assert!(!store.role_messaging_enabled("developer").await?);
        let disabled_at = disabled.value["changed_at"]
            .as_str()
            .expect("disable timestamp")
            .to_string();

        let unauthorized_enable = dispatcher
            .enable_messaging(
                "developer",
                EnableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "unauthorized attempt".to_string(),
                    expected_disabled_at: disabled_at.clone(),
                },
            )
            .await;
        assert!(unauthorized_enable.is_error);
        assert!(!store.role_messaging_enabled("developer").await?);

        let unauthorized_clear = dispatcher
            .clear_message_queue(
                "developer",
                ClearMessageQueueArgs {
                    agent: "developer".to_string(),
                    include_dead: true,
                },
            )
            .await;
        assert!(unauthorized_clear.is_error);

        let blocked_order = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "must not start".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert_eq!(blocked_order.value["disabled_agents"], json!(["developer"]));
        assert_eq!(
            blocked_order.value["action_required"],
            json!("stop_current_run_and_escalate_to_human")
        );

        let blocked_report = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "must not leave".to_string(),
                    task_id: "task-blocked".to_string(),
                    status: "completed".to_string(),
                    recipient: Some("manager".to_string()),
                    idempotency_key: None,
                },
            )
            .await;
        assert_eq!(
            blocked_report.value["error"],
            json!("messaging is disabled for the sending role")
        );

        let blocked_peer = dispatcher
            .message(
                "lead-developer",
                MessageArgs {
                    agent: "developer".to_string(),
                    message: "must not arrive".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert_eq!(blocked_peer.value["disabled_agents"], json!(["developer"]));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "blocked traffic must never reach Hermes"
        );

        let enabled = dispatcher
            .enable_messaging(
                "manager",
                EnableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "feedback trigger removed and queue checked".to_string(),
                    expected_disabled_at: disabled_at,
                },
            )
            .await;
        assert!(!enabled.is_error, "unexpected: {enabled:?}");
        let accepted = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "resume".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!accepted.is_error, "unexpected: {accepted:?}");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn messaging_enable_requires_fresh_state_and_cooldown() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("messaging-cooldown");
        let mut config = testutil::fixture_config(&path);
        config.messaging_reenable_cooldown = std::time::Duration::from_secs(600);
        let mock = testutil::spawn_mock_hermes(Arc::new(|_, _| {
            (202, json!({"run_id": "run-after-cooldown"}))
        }))
        .await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config, store.clone())?;

        let disabled = dispatcher
            .disable_messaging(
                "manager",
                DisableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "loop detected".to_string(),
                    clear_queue: true,
                },
            )
            .await;
        let original_disabled_at = disabled.value["changed_at"]
            .as_str()
            .expect("disable timestamp")
            .to_string();
        let cooldown = dispatcher
            .enable_messaging(
                "manager",
                EnableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "too early".to_string(),
                    expected_disabled_at: original_disabled_at.clone(),
                },
            )
            .await;
        assert!(cooldown.is_error);
        assert!(
            cooldown.value["retry_after_seconds"]
                .as_i64()
                .unwrap_or_default()
                > 0
        );
        assert!(!store.role_messaging_enabled("developer").await?);

        sqlx::query("UPDATE role_messaging SET changed_ms = changed_ms - 601000 WHERE role = ?")
            .bind("developer")
            .execute(store.pool())
            .await?;
        let stale = dispatcher
            .enable_messaging(
                "manager",
                EnableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "stale observation".to_string(),
                    expected_disabled_at: original_disabled_at,
                },
            )
            .await;
        assert!(stale.is_error);
        assert!(
            stale.value["error"]
                .as_str()
                .is_some_and(|value| value.contains("changed after"))
        );

        let (_, changed_ms) = store
            .role_messaging_state("developer")
            .await?
            .expect("messaging state");
        let refreshed = DateTime::<Utc>::from_timestamp_millis(changed_ms)
            .expect("valid timestamp")
            .to_rfc3339();
        let enabled = dispatcher
            .enable_messaging(
                "manager",
                EnableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "human requested resume after trigger removal".to_string(),
                    expected_disabled_at: refreshed,
                },
            )
            .await;
        assert!(!enabled.is_error, "unexpected: {enabled:?}");
        assert!(store.role_messaging_enabled("developer").await?);

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn order_all_aggregates_partial_results() -> anyhow::Result<()> {
        let behavior: testutil::MockHermes = Arc::new(|bearer, _| {
            if bearer.ends_with("api-key-developer") {
                (202, json!({"run_id": "run-dev"}))
            } else {
                // lead-developer: server error -> indeterminate (downstream may have accepted)
                (500, json!({"error": "boom"}))
            }
        });
        let (dispatcher, store, path) = dispatcher_with_mock(behavior).await?;

        let outcome = dispatcher
            .order_all(
                "manager",
                BroadcastArgs {
                    command: "sync".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert_eq!(outcome.value["ok"], json!(false));
        assert_eq!(
            outcome.value["results"]["developer"]["run_id"],
            json!("run-dev")
        );
        assert_eq!(
            outcome.value["results"]["lead-developer"]["status"],
            json!("indeterminate")
        );
        assert_eq!(
            outcome.value["results"]["lead-developer"]["recovery_required"],
            json!(true)
        );

        let dispatch_id = outcome.value["task_id"].as_str().expect("task id");
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(dispatch_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "partial", "durable partial result");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn report_returns_to_the_exact_task_issuer() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "run-report"})))).await?;

        // An invented task cannot be reported to any role.
        let outcome = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "done".to_string(),
                    task_id: "t1".to_string(),
                    status: "completed".to_string(),
                    recipient: Some("designer".to_string()),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert!(
            outcome.value["error"]
                .as_str()
                .unwrap()
                .contains("not an accepted Swarm order")
        );

        let unassigned = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "invented lineage".to_string(),
                    task_id: "task-not-assigned".to_string(),
                    status: "completed".to_string(),
                    recipient: Some("lead-developer".to_string()),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(unassigned.is_error);
        assert!(
            unassigned.value["error"]
                .as_str()
                .unwrap()
                .contains("not an accepted Swarm order")
        );

        let assignment = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Implement the assigned work".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let task_id = assignment.value["task_id"]
            .as_str()
            .expect("assigned task id")
            .to_string();

        // A valid supervisor is still the wrong recipient when the manager
        // issued this exact task.
        let wrong_issuer = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "done".to_string(),
                    task_id: task_id.clone(),
                    status: "completed".to_string(),
                    recipient: Some("lead-developer".to_string()),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(wrong_issuer.is_error);
        assert_eq!(wrong_issuer.value["issuer"], json!("manager"));

        // Omitting recipient safely routes to the persisted issuer.
        let outcome = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "done".to_string(),
                    task_id,
                    status: "completed".to_string(),
                    recipient: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["recipient"], json!("manager"));
        assert_eq!(outcome.value["recipient_run_id"], json!("run-report"));
        assert_eq!(outcome.value["summary"], json!("done"));

        // unknown status is rejected.
        let outcome = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "done".to_string(),
                    task_id: "t1".to_string(),
                    status: "weird".to_string(),
                    recipient: Some("lead-developer".to_string()),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn progress_report_is_persisted_without_waking_supervisor() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_mock(Arc::new(move |_, _| {
            calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (202, json!({"run_id": "unexpected"}))
        }))
        .await?;

        let assignment = dispatcher
            .order(
                "lead-developer",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Reach one tested checkpoint".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let task_id = assignment.value["task_id"]
            .as_str()
            .expect("assigned task id")
            .to_string();
        let outcome = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "Implementation reached the tested checkpoint".to_string(),
                    task_id,
                    status: "in_progress".to_string(),
                    recipient: None,
                    idempotency_key: None,
                },
            )
            .await;

        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["supervisor_woken"], json!(false));
        assert!(outcome.value.get("recipient_run_id").is_none());
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the assignment starts one run; progress must not start another"
        );
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(outcome.value["report_id"].as_str().expect("report id"))
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "accepted");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn manager_terminal_report_can_be_reconciled_without_consuming_a_run()
    -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-manager-report-reconcile");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let mock = testutil::spawn_mock_hermes(Arc::new(move |_, _| {
            calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (202, json!({"run_id": "run-assignment"}))
        }))
        .await;
        let mut config = testutil::fixture_config(&path);
        config.manager_report_wake_mode = ManagerReportWakeMode::Scheduled;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config, store.clone())?;

        let assignment = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Implement the assigned work".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let task_id = assignment.value["task_id"]
            .as_str()
            .expect("assigned task id")
            .to_string();
        let outcome = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "terminal evidence".to_string(),
                    task_id,
                    status: "completed".to_string(),
                    recipient: Some("manager".to_string()),
                    idempotency_key: None,
                },
            )
            .await;

        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["supervisor_woken"], json!(false));
        assert_eq!(outcome.value["wake_deferred"], json!(true));
        assert_eq!(
            outcome.value["wake_policy"],
            json!("scheduled_reconciliation")
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the original assignment may start a run"
        );
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(outcome.value["report_id"].as_str().expect("report id"))
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "accepted");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn terminal_report_is_accepted_when_supervisor_is_temporarily_busy() -> anyhow::Result<()>
    {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_mock(Arc::new(move |_, _| {
            if calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                (202, json!({"run_id": "run-assignment"}))
            } else {
                (429, json!({"error": "busy"}))
            }
        }))
        .await?;
        let assignment = dispatcher
            .order(
                "lead-developer",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Implement the assigned work".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let task_id = assignment.value["task_id"]
            .as_str()
            .expect("assigned task id")
            .to_string();

        let outcome = dispatcher
            .report(
                "developer",
                ReportArgs {
                    summary: "terminal evidence".to_string(),
                    task_id,
                    status: "completed".to_string(),
                    recipient: None,
                    idempotency_key: None,
                },
            )
            .await;

        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["supervisor_woken"], json!(false));
        assert_eq!(outcome.value["wake_deferred"], json!(true));
        assert_eq!(outcome.value["wake_policy"], json!("supervisor_busy"));
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(outcome.value["report_id"].as_str().expect("report id"))
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "accepted");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn message_all_without_peers_is_rejected() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-no-peers");
        let mut config = testutil::fixture_config(&path);
        config.agent_roles = vec!["developer".to_string()];
        config.all_roles = vec!["manager".to_string(), "developer".to_string()];
        config.order_acl = BTreeMap::from([("manager".to_string(), vec!["developer".to_string()])]);
        config.global_authorities = BTreeSet::from(["manager".to_string()]);
        config.activity_routes = BTreeMap::new();
        config
            .agents
            .retain(|role, _| role == "manager" || role == "developer");
        config.descriptions.retain(|role, _| role == "developer");
        config
            .role_paths
            .retain(|role, _| role == "manager" || role == "developer");
        let mock =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        // developer has no peers: sending to nobody must not report success.
        let outcome = dispatcher
            .message_all(
                "developer",
                MessageAllArgs {
                    message: "hi".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert!(
            outcome.value["error"]
                .as_str()
                .unwrap()
                .contains("no peer executors")
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn msg_to_without_task_id_is_a_plain_handoff() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-msg-no-task");
        let mut config = testutil::fixture_config(&path);
        let mock =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        // developer → lead-developer, no task_id: must be accepted as a
        // plain coordination message (no order lineage exists for sender).
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "lead-developer".to_string(),
                    message: "Перезвони клиенту: Иван, +79139849832, хочет сайт".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected error: {}", outcome.value);
        assert_eq!(outcome.value["ok"], json!(true));
        assert_eq!(outcome.value["agent"], json!("lead-developer"));

        // Same again — idempotency replay must not flip it to an error.
        let replay = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "lead-developer".to_string(),
                    message: "Перезвони клиенту: Иван, +79139849832, хочет сайт".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!replay.is_error, "replay failed: {}", replay.value);
        assert_eq!(replay.value["ok"], json!(true));

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn idempotency_replays_accepted_and_reexecutes_failed() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let behavior: testutil::MockHermes = Arc::new(move |_, _| {
            if calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                // First attempt is definitively rejected (HTTP 400).
                (400, json!({"error": "bad request"}))
            } else {
                (202, json!({"run_id": "run-ok"}))
            }
        });
        let (dispatcher, store, path) = dispatcher_with_mock(behavior).await?;

        let first = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "retry me".to_string(),
                    idempotency_key: Some("key-retry".to_string()),
                },
            )
            .await;
        assert!(first.is_error);
        let first_status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE sender='manager' AND kind='order' AND idempotency_key='key-retry'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(first_status, "failed");

        // Retry with the SAME key re-executes: the failed dispatch released the key.
        let second = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "retry me".to_string(),
                    idempotency_key: Some("key-retry".to_string()),
                },
            )
            .await;
        assert!(!second.is_error, "retry must re-execute: {second:?}");
        assert_eq!(second.value["run_id"], json!("run-ok"));
        assert!(second.value.get("deduplicated").is_none());
        let second_id = second.value["task_id"].as_str().expect("task id");

        // A further call with the same key replays the accepted result.
        let third = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "retry me".to_string(),
                    idempotency_key: Some("key-retry".to_string()),
                },
            )
            .await;
        assert_eq!(third.value["deduplicated"], json!(true));
        assert_eq!(third.value["task_id"], json!(second_id));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn recent_content_duplicate_is_suppressed_without_caller_key() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let behavior: testutil::MockHermes = Arc::new(move |_, _| {
            calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (202, json!({"run_id": "run-once"}))
        });
        let (dispatcher, _store, path) = dispatcher_with_mock(behavior).await?;

        let first = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Implement the bounded task".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let replay = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "  Implement   the bounded task\n".to_string(),
                    idempotency_key: None,
                },
            )
            .await;

        assert!(!first.is_error, "unexpected: {first:?}");
        assert!(!replay.is_error, "unexpected: {replay:?}");
        assert_eq!(replay.value["deduplicated"], json!(true));
        assert_eq!(replay.value["task_id"], first.value["task_id"]);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    /// Dispatcher with Telegram delivery/inbound enabled (shared bot) and a mock
    /// Bot API for sendMessage.
    async fn dispatcher_with_telegram(
        behavior: testutil::MockHermes,
    ) -> anyhow::Result<(Dispatcher, Store, std::path::PathBuf)> {
        dispatcher_with_telegram_bot(behavior, Arc::new(|_, _| (200, json!({"ok": true}), None)))
            .await
    }

    type MockBot = Arc<dyn Fn(&str, &str) -> (u16, Value, Option<String>) + Send + Sync>;

    /// Dispatcher with Telegram delivery/inbound enabled and a custom Bot API behavior.
    async fn dispatcher_with_telegram_bot(
        behavior: testutil::MockHermes,
        bot: MockBot,
    ) -> anyhow::Result<(Dispatcher, Store, std::path::PathBuf)> {
        let path = testutil::temp_db_path("dispatch-tg");
        let telegram_base = spawn_mock_telegram_with(bot).await;
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = crate::config::TelegramBotMode::Shared;
        config.telegram_bot_token = Some(crate::config::Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_api_base_url = telegram_base.parse()?;
        let mock = testutil::spawn_mock_hermes(behavior).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;
        Ok((dispatcher, store, path))
    }

    async fn spawn_mock_telegram_with(behavior: MockBot) -> String {
        spawn_mock_telegram_with_files(behavior, Vec::new()).await
    }

    /// Mock with inbound-file support: `/getFile` resolves to a fixed
    /// `file_path`, and `/file/bot{token}/{path}` serves `file_bytes` raw.
    async fn spawn_mock_telegram_with_files(behavior: MockBot, file_bytes: Vec<u8>) -> String {
        use axum::{
            Json, Router,
            body::Bytes,
            extract::{OriginalUri, State as AxumState},
            http::StatusCode,
            response::IntoResponse,
            routing::{get, post},
        };
        let handler = move |AxumState(behavior): AxumState<MockBot>,
                            uri: OriginalUri,
                            body: Bytes| async move {
            let body = String::from_utf8_lossy(&body).to_string();
            let (status, payload, retry_after) = behavior(uri.path(), &body);
            let mut response = (
                StatusCode::from_u16(status).expect("mock status"),
                Json(payload),
            )
                .into_response();
            if let Some(after) = retry_after {
                response.headers_mut().insert(
                    reqwest::header::RETRY_AFTER,
                    after.parse().expect("retry-after header"),
                );
            }
            response
        };
        let file_handler = move |uri: OriginalUri| async move {
            let bytes = file_bytes.clone();
            let mut response = (StatusCode::OK, bytes).into_response();
            response.headers_mut().insert(
                reqwest::header::CONTENT_TYPE,
                "image/png".parse().expect("mime"),
            );
            let _ = uri;
            response
        };
        let get_file_handler = move || async move {
            Json(json!({"ok": true, "result": {"file_id": "f1", "file_unique_id": "u1", "file_path": "photos/x.png"}}))
        };
        let router = Router::new()
            .route("/bot{token}/sendMessage", post(handler))
            .route("/bot{token}/sendPhoto", post(handler))
            .route("/bot{token}/sendDocument", post(handler))
            .route("/bot{token}/getFile", get(get_file_handler))
            .route("/file/bot{token}/{*path}", get(file_handler))
            .with_state(behavior);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock Telegram");
        let address = listener.local_addr().expect("mock Telegram address");
        tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("mock Telegram serves");
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn message_reaches_peer_and_persists() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "run-msg"})))).await?;

        let _assignment = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Coordinate one implementation detail".to_string(),
                    idempotency_key: None,
                },
            )
            .await;

        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "lead-developer".to_string(),
                    message: " ping ".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["run_id"], json!("run-msg"));
        let message_id = outcome.value["message_id"].as_str().expect("message id");
        assert!(message_id.starts_with("msg_"));
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(message_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "accepted");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn message_rejects_non_peer_targets() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "run"})))).await?;
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "developer".to_string(),
                    message: "self".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "manager".to_string(),
                    message: "not a peer".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn message_all_records_indeterminate_peer_delivery() -> anyhow::Result<()> {
        let behavior: testutil::MockHermes = Arc::new(|bearer, _| {
            if bearer.ends_with("api-key-developer") {
                (202, json!({"run_id": "run-dev"}))
            } else {
                (500, json!({"error": "boom"}))
            }
        });
        let (dispatcher, store, path) = dispatcher_with_mock(behavior).await?;

        let _assignment = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "Coordinate the assigned task".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        // The developer's only peer is lead-developer, whose server error is
        // indeterminate because the downstream may still have accepted it.
        let outcome = dispatcher
            .message_all(
                "developer",
                MessageAllArgs {
                    message: "sync".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert_eq!(outcome.value["ok"], json!(false));
        assert_eq!(
            outcome.value["results"]["lead-developer"]["status"],
            json!("indeterminate")
        );
        let message_id = outcome.value["message_id"].as_str().expect("message id");
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(message_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "indeterminate");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn order_all_all_server_failures_are_indeterminate() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (500, json!({"error": "boom"})))).await?;
        let outcome = dispatcher
            .order_all(
                "manager",
                BroadcastArgs {
                    command: "sync".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert_eq!(outcome.value["ok"], json!(false));
        assert_eq!(
            outcome.value["results"]["developer"]["status"],
            json!("indeterminate")
        );
        let dispatch_id = outcome.value["task_id"].as_str().expect("task id");
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(dispatch_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            status, "indeterminate",
            "nothing accepted, all may have happened"
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn telegram_inbound_deduplicates_by_update_id() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_telegram(Arc::new(|_, _| (202, json!({"run_id": "run-tg"})))).await?;

        let args = TelegramInboundArgs {
            update_id: 5,
            message_id: 100,
            user_id: 42,
            username: "alice".to_string(),
            message: "hello".to_string(),
            targets: vec!["developer".to_string()],
            chat_id: -100_123,
        };
        let first = dispatcher.telegram_inbound(args).await;
        assert!(!first.is_error, "unexpected: {:?}", first.value);
        assert_eq!(
            first.value["results"]["developer"]["run_id"],
            json!("run-tg")
        );
        assert!(first.value.get("deduplicated").is_none());

        // The same update replayed (poll retry) must not re-dispatch.
        let second = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 5,
                message_id: 100,
                user_id: 42,
                username: "alice".to_string(),
                message: "hello".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: -100_123,
            })
            .await;
        assert_eq!(second.value["deduplicated"], json!(true));

        // A different update with invalid identifiers is rejected before reservation.
        let invalid = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: -1,
                message_id: 100,
                user_id: 42,
                username: "alice".to_string(),
                message: "hello".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: -100_123,
            })
            .await;
        assert!(invalid.is_error);

        // The audit outbox row was queued alongside the accepted dispatch.
        let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(outbox, 1, "one audit entry for the accepted update");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn private_telegram_inbound_is_not_copied_to_the_group_outbox() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_telegram(Arc::new(|_, _| (202, json!({"run_id": "run-private"}))))
                .await?;

        let outcome = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 6,
                message_id: 101,
                user_id: 42,
                username: "alice".to_string(),
                message: "private operator request".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 42,
            })
            .await;

        assert!(!outcome.is_error, "unexpected: {:?}", outcome.value);
        assert_eq!(outcome.value["telegram"]["queued"], json!(false));
        assert_eq!(outcome.value["telegram"]["privacy"], json!("private_chat"));
        let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            outbox, 0,
            "private inbound text and dispatch metadata must not enter the shared group outbox"
        );

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn private_telegram_reply_wire_payload_has_no_audit_header() -> anyhow::Result<()> {
        let sent = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let sent_clone = sent.clone();
        let (dispatcher, _store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "run-private"}))),
            Arc::new(move |_, body| {
                sent_clone.lock().expect("sent").push(body.to_string());
                (200, json!({"ok": true}), None)
            }),
        )
        .await?;

        let inbound = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 7,
                message_id: 102,
                user_id: 42,
                username: "alice".to_string(),
                message: "private operator request".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 42,
            })
            .await;
        assert!(!inbound.is_error, "unexpected: {:?}", inbound.value);

        let reply_text = "Ответ без служебного заголовка";
        let reply = dispatcher
            .telegram_reply("developer", reply_text.to_string(), vec![])
            .await;
        assert!(!reply.is_error, "unexpected: {:?}", reply.value);
        dispatcher.flush_outbox().await?;

        let payload: Value = {
            let bodies = sent.lock().expect("sent");
            assert_eq!(bodies.len(), 1, "only the private reply should be sent");
            serde_json::from_str(&bodies[0])?
        };
        assert_eq!(payload["chat_id"], json!("42"));
        assert_eq!(payload["text"], json!(reply_text));
        assert!(
            !payload["text"]
                .as_str()
                .unwrap_or_default()
                .contains("TELEGRAM_REPLY")
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn telegram_reply_queues_attached_files() -> anyhow::Result<()> {
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("POST /v1/chat/completions") {
                (200, json!({"id": "x", "choices": [{"message": {"role": "assistant", "content": ""}}]}))
            } else {
                (404, json!({"error": "unexpected"}))
            }
        }))
        .await?;
        // Устанавливаем чат оператора диспатчем.
        dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 11,
                message_id: 400,
                user_id: 42,
                username: "alice".to_string(),
                message: "draw".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        let files = vec![
            TelegramFileArgs {
                filename: "cover_ru.png".to_string(),
                mime_type: Some("image/png".to_string()),
                content_b64: "aGVsbG8=".to_string(),
            },
            TelegramFileArgs {
                filename: "report.pdf".to_string(),
                mime_type: None,
                content_b64: "cGRm".to_string(),
            },
        ];
        let reply = dispatcher
            .telegram_reply("developer", "Готово".to_string(), files)
            .await;
        assert!(!reply.is_error, "unexpected: {:?}", reply.value);
        let items = store.due_outbox(10).await?;
        let item = items
            .iter()
            .find(|item| item.event == "TELEGRAM_REPLY")
            .expect("reply with files must be queued");
        assert_eq!(item.media.len(), 2);
        assert_eq!(item.media[0].filename, "cover_ru.png");
        assert_eq!(item.media[0].mime_type.as_deref(), Some("image/png"));
        assert_eq!(item.media[1].filename, "report.pdf");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn telegram_reply_allows_files_without_message() -> anyhow::Result<()> {
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("POST /v1/chat/completions") {
                (200, json!({"id": "x", "choices": [{"message": {"role": "assistant", "content": ""}}]}))
            } else {
                (404, json!({"error": "unexpected"}))
            }
        }))
        .await?;
        dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 12,
                message_id: 401,
                user_id: 42,
                username: "alice".to_string(),
                message: "draw".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        let reply = dispatcher
            .telegram_reply(
                "developer",
                String::new(),
                vec![TelegramFileArgs {
                    filename: "x.png".to_string(),
                    mime_type: Some("image/png".to_string()),
                    content_b64: "eA==".to_string(),
                }],
            )
            .await;
        assert!(
            !reply.is_error,
            "files-only reply must work: {:?}",
            reply.value
        );
        let items = store.due_outbox(10).await?;
        assert!(items.iter().any(|item| item.media.len() == 1));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn media_delivery_resumes_after_a_mid_batch_failure() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let photo_calls = Arc::new(AtomicUsize::new(0));
        let photo_count = photo_calls.clone();
        let document_calls = Arc::new(AtomicUsize::new(0));
        let document_count = document_calls.clone();
        let (dispatcher, store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(move |path, _| {
                if path.ends_with("sendPhoto") {
                    photo_count.fetch_add(1, Ordering::SeqCst);
                    (200, json!({"ok": true}), None)
                } else if path.ends_with("sendDocument") {
                    // Первый документ падает (retry_after=0, чтобы пункт
                    // снова стал due) — доставка должна продолжиться со
                    // второго файла без повтора фото.
                    if document_count.fetch_add(1, Ordering::SeqCst) == 0 {
                        (
                            500,
                            json!({"ok": false, "description": "boom"}),
                            Some("0".to_string()),
                        )
                    } else {
                        (200, json!({"ok": true}), None)
                    }
                } else {
                    (200, json!({"ok": true}), None)
                }
            }),
        )
        .await?;
        dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 13,
                message_id: 402,
                user_id: 42,
                username: "alice".to_string(),
                message: "draw".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        let files = vec![
            TelegramFileArgs {
                filename: "a.png".to_string(),
                mime_type: Some("image/png".to_string()),
                content_b64: "YQ==".to_string(),
            },
            TelegramFileArgs {
                filename: "b.pdf".to_string(),
                mime_type: None,
                content_b64: "Yg==".to_string(),
            },
            TelegramFileArgs {
                filename: "c.pdf".to_string(),
                mime_type: None,
                content_b64: "Yw==".to_string(),
            },
        ];
        assert!(
            !dispatcher
                .telegram_reply("developer", "docs".to_string(), files)
                .await
                .is_error
        );
        // Первый прогон: фото ушло, первый документ упал → пункт остаётся
        // (статус/чекпоинт читаем напрямую — пункт отложен по backoff).
        dispatcher.flush_outbox().await?;
        let row = sqlx::query(
            "SELECT status, attempts, next_chunk FROM telegram_outbox WHERE event = 'TELEGRAM_REPLY'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(row.get::<String, _>("status"), "pending");
        assert_eq!(row.get::<i64, _>("attempts"), 1);
        assert_eq!(
            row.get::<i64, _>("next_chunk"),
            1,
            "только фото зачекпоинчено"
        );
        assert_eq!(photo_calls.load(Ordering::SeqCst), 1);
        assert_eq!(document_calls.load(Ordering::SeqCst), 1);
        // Второй прогон после дефолтного backoff (2^attempts): два документа
        // досылаются, фото не повторяется.
        tokio::time::sleep(Duration::from_secs(3)).await;
        dispatcher.flush_outbox().await?;
        let items = store.due_outbox(10).await?;
        assert!(
            !items.iter().any(|item| item.event == "TELEGRAM_REPLY"),
            "reply delivered"
        );
        assert_eq!(photo_calls.load(Ordering::SeqCst), 1, "фото не повторяется");
        // 1 упавший вызов при первом прогоне + 2 досланных документа.
        assert_eq!(document_calls.load(Ordering::SeqCst), 3);
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn run_reply_worker_drops_expired_tracks_without_delivery() -> anyhow::Result<()> {
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("GET /v1/runs/") {
                (
                    200,
                    json!({
                        "object": "hermes.run",
                        "run_id": "run-old",
                        "status": "running",
                    }),
                )
            } else {
                (404, json!({"error": "unexpected"}))
            }
        }))
        .await?;
        store
            .track_run_reply("run-old", "developer", 424_242)
            .await?;
        // Ставим created_ms в прошлом на 7 часов (TTL = 6ч).
        sqlx::query("UPDATE run_replies SET created_ms = ? WHERE run_id = 'run-old'")
            .bind(Utc::now().timestamp_millis() - 7 * 3600 * 1000)
            .execute(store.pool())
            .await?;
        dispatcher.flush_run_replies().await?;
        assert!(
            store.due_run_replies().await?.is_empty(),
            "expired track dropped"
        );
        let items = store.due_outbox(10).await?;
        assert!(!items.iter().any(|item| item.event == "TELEGRAM_REPLY"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn telegram_reply_falls_back_to_any_roles_operator_chat() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_telegram(Arc::new(|_, _| (202, json!({"run_id": "run-x"})))).await?;
        // Диспатч на developer устанавливает чат оператора.
        dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 14,
                message_id: 403,
                user_id: 42,
                username: "alice".to_string(),
                message: "hi".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        // MCP-only роль (например designer) без собственного диспатча
        // отвечает в тот же чат оператора.
        let reply = dispatcher
            .telegram_reply("designer", "proactive".to_string(), vec![])
            .await;
        assert!(!reply.is_error, "unexpected: {:?}", reply.value);
        let items = store.due_outbox(10).await?;
        let item = items
            .iter()
            .find(|item| item.event == "TELEGRAM_REPLY")
            .expect("reply queued");
        assert_eq!(item.chat_id, Some(424_242));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn run_reply_worker_delivers_final_answer_to_source_chat() -> anyhow::Result<()> {
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("GET /v1/runs/") {
                (
                    200,
                    json!({
                        "object": "hermes.run",
                        "run_id": "run-tg",
                        "status": "completed",
                        "output": "Ответ от агента",
                    }),
                )
            } else {
                (202, json!({"run_id": "run-tg"}))
            }
        }))
        .await?;

        let outcome = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 5,
                message_id: 100,
                user_id: 42,
                username: "alice".to_string(),
                message: "hello".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        assert!(!outcome.is_error, "unexpected: {:?}", outcome.value);
        assert_eq!(store.due_run_replies().await?.len(), 1);

        dispatcher.flush_run_replies().await?;

        let items = store.due_outbox(10).await?;
        let reply = items
            .iter()
            .find(|item| item.event == "TELEGRAM_REPLY")
            .expect("the run reply must be enqueued");
        assert_eq!(reply.text, "Ответ от агента");
        assert_eq!(reply.chat_id, Some(424_242));

        // The run is no longer tracked once delivered.
        assert!(store.due_run_replies().await?.is_empty());
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn openai_role_is_initiated_via_chat_completions() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-openai");
        let telegram_base =
            spawn_mock_telegram_with(Arc::new(|_, _| (200, json!({"ok": true}), None))).await;
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = crate::config::TelegramBotMode::Shared;
        config.telegram_bot_token = Some(crate::config::Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_api_base_url = telegram_base.parse()?;
        let initiated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = initiated.clone();
        let mock = testutil::spawn_mock_hermes(Arc::new(move |_, body| {
            if body.starts_with("POST /v1/chat/completions") {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                (
                    200,
                    json!({"id": "chatcmpl-1", "choices": [{"message": {"role": "assistant", "content": ""}}]}),
                )
            } else {
                (404, json!({"error": "unexpected endpoint"}))
            }
        }))
        .await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        config
            .agents
            .get_mut("developer")
            .expect("fixture has developer")
            .api_kind = crate::config::ApiKind::OpenAi;
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        let outcome = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 7,
                message_id: 200,
                user_id: 42,
                username: "alice".to_string(),
                message: "hello".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        assert!(!outcome.is_error, "unexpected: {:?}", outcome.value);
        assert_eq!(
            outcome.value["results"]["developer"]["initiated"],
            json!("true")
        );
        assert!(initiated.load(std::sync::atomic::Ordering::SeqCst));
        // OpenAI roles have no Hermes run to poll.
        assert!(store.due_run_replies().await?.is_empty());
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn mcp_only_role_rejects_dispatches_and_can_reply_proactively() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-mcp-only");
        let telegram_base =
            spawn_mock_telegram_with(Arc::new(|_, _| (200, json!({"ok": true}), None))).await;
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = crate::config::TelegramBotMode::Shared;
        config.telegram_bot_token = Some(crate::config::Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_api_base_url = telegram_base.parse()?;
        let mock = testutil::spawn_mock_hermes(Arc::new(|_, _| {
            (404, json!({"error": "unexpected endpoint"}))
        }))
        .await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        config
            .agents
            .get_mut("developer")
            .expect("fixture has developer")
            .api_kind = crate::config::ApiKind::Mcp;
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        // Proactive telegram_reply works for MCP-only roles: with no prior
        // dispatch the message falls back to the group chat.
        let reply = dispatcher
            .telegram_reply("developer", "Инициатива снизу".to_string(), vec![])
            .await;
        assert!(!reply.is_error, "unexpected: {:?}", reply.value);
        let items = store.due_outbox(10).await?;
        let item = items
            .iter()
            .find(|item| item.event == "TELEGRAM_REPLY")
            .expect("reply must be queued");
        assert_eq!(item.text, "Инициатива снизу");
        assert_eq!(
            item.chat_id, None,
            "no dispatch yet — falls back to the group"
        );

        // A dispatch addressed to an MCP-only role is rejected with a clear
        // error (the role initiates interaction itself).
        let outcome = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 9,
                message_id: 300,
                user_id: 42,
                username: "alice".to_string(),
                message: "hello".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        let error = outcome.value["results"]["developer"]["error"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(error.contains("MCP-only"), "unexpected: {error}");
        assert!(store.due_run_replies().await?.is_empty());
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn flush_outbox_delivers_queued_audits() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_telegram(Arc::new(|_, _| (202, json!({"run_id": "run-flush"}))))
                .await?;

        // An accepted order queues an audit message in the outbox.
        dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "audit me".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let pending: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox WHERE status='pending'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(pending, 1);

        dispatcher.flush_outbox().await?;
        let delivered: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox WHERE status='delivered'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(delivered, 1, "audit delivered through the mock Bot API");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn flush_outbox_holds_preserved_messages_for_a_disabled_role() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "run-held"}))),
            Arc::new(move |_, _| {
                calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                (200, json!({"ok": true}), None)
            }),
        )
        .await?;
        dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "audit after resume".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        let disabled = dispatcher
            .disable_messaging(
                "manager",
                DisableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "hold queue".to_string(),
                    clear_queue: false,
                },
            )
            .await;
        assert!(!disabled.is_error);
        let disabled_at = disabled.value["changed_at"]
            .as_str()
            .expect("disable timestamp")
            .to_string();

        dispatcher.flush_outbox().await?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let held_status: String =
            sqlx::query_scalar("SELECT status FROM telegram_outbox WHERE event='ORDER'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(held_status, "pending");

        dispatcher
            .enable_messaging(
                "manager",
                EnableMessagingArgs {
                    agent: "developer".to_string(),
                    reason: "queue hold reviewed by operator".to_string(),
                    expected_disabled_at: disabled_at,
                },
            )
            .await;
        dispatcher.flush_outbox().await?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let resumed_status: String =
            sqlx::query_scalar("SELECT status FROM telegram_outbox WHERE event='ORDER'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(resumed_status, "delivered");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn flush_outbox_does_not_retry_permanent_telegram_errors() -> anyhow::Result<()> {
        let (dispatcher, store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "run-flush"}))),
            Arc::new(|_, _| (403, json!({"ok": false}), None)),
        )
        .await?;
        dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "audit once".to_string(),
                    idempotency_key: None,
                },
            )
            .await;

        dispatcher.flush_outbox().await?;

        let (status, attempts): (String, i64) =
            sqlx::query_as("SELECT status, attempts FROM telegram_outbox WHERE event='ORDER'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "dead");
        assert_eq!(attempts, dispatcher.config.outbox_max_attempts);

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn flush_outbox_applies_transport_backoff_to_the_remaining_batch() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "run-flush"}))),
            Arc::new(move |_, _| {
                calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                (
                    429,
                    json!({"ok": false, "description": "flood"}),
                    Some("30".to_string()),
                )
            }),
        )
        .await?;
        for command in ["first audit", "second audit"] {
            dispatcher
                .order(
                    "manager",
                    OrderArgs {
                        agent: "developer".to_string(),
                        command: command.to_string(),
                        idempotency_key: None,
                    },
                )
                .await;
        }
        let before = chrono::Utc::now().timestamp_millis();

        dispatcher.flush_outbox().await?;

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the worker must stop the selected batch after a transient failure"
        );
        let (attempts, earliest_retry): (i64, i64) = sqlx::query_as(
            "SELECT SUM(attempts), MIN(next_attempt_ms) FROM telegram_outbox WHERE status='pending'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(attempts, 1, "only the attempted row consumes a retry");
        assert!(
            earliest_retry >= before + 29_000,
            "Retry-After must defer every pending row on the shared transport"
        );

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    fn outbox_item() -> OutboxItem {
        OutboxItem {
            id: "audit_x".to_string(),
            sender: "manager".to_string(),
            event: "ORDER".to_string(),
            recipients: "developer".to_string(),
            text: "task_1\nmake a mockup".repeat(300),
            attempts: 0,
            next_chunk: 0,
            chat_id: None,
            media: vec![],
        }
    }

    #[tokio::test]
    async fn send_telegram_applies_retry_after_on_429() -> anyhow::Result<()> {
        let (dispatcher, _store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(|_, _| {
                (
                    429,
                    json!({"ok": false, "description": "flood"}),
                    Some("30".into()),
                )
            }),
        )
        .await?;
        let error = dispatcher.send_telegram(&outbox_item()).await.unwrap_err();
        assert_eq!(error.retry_after_seconds, Some(30));
        assert!(!error.permanent, "429 must remain retryable");
        assert!(error.to_string().contains("429"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn send_telegram_rejects_invalid_success_responses() -> anyhow::Result<()> {
        // {"ok": false} with HTTP 200 is still a failure.
        let (dispatcher, _store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(|_, _| (200, json!({"ok": false}), None)),
        )
        .await?;
        let error = dispatcher.send_telegram(&outbox_item()).await.unwrap_err();
        assert!(error.retry_after_seconds.is_none());
        assert!(
            error.permanent,
            "an invalid 2xx payload will not heal on retry"
        );
        assert!(error.to_string().contains("invalid success response"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn send_telegram_resumes_from_the_chunk_cursor() -> anyhow::Result<()> {
        let sent = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let sent_clone = sent.clone();
        let (dispatcher, store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(move |_, body| {
                sent_clone.lock().expect("sent").push(body.to_string());
                (200, json!({"ok": true}), None)
            }),
        )
        .await?;

        // The first chunk (index 0) was already acknowledged.
        let mut item = outbox_item();
        item.next_chunk = 1;
        // The chunk checkpoint needs a real pending outbox row.
        sqlx::query(
            "INSERT INTO telegram_outbox(id,sender,event,recipients,text,status,attempts,next_chunk,next_attempt_ms,created_ms)
             VALUES ('audit_x','manager','ORDER','developer',?,'pending',0,0,0,0)",
        )
        .bind(&item.text)
        .execute(store.pool())
        .await?;
        dispatcher.send_telegram(&item).await.unwrap();
        let bodies = {
            let bodies = sent.lock().expect("sent");
            assert_eq!(bodies.len(), 1, "only the remaining chunks are sent");
            bodies[0].clone()
        };
        let expected_chunks = telegram_chunks(
            "[ORDER] manager -> developer",
            &item.text,
            dispatcher.config.telegram_message_limit,
        );
        assert!(expected_chunks.len() > 1, "fixture text must span chunks");
        let sent_body: Value = serde_json::from_str(&bodies).expect("sendMessage body");
        assert_eq!(
            sent_body["text"],
            json!(expected_chunks[1]),
            "chunk 0 must be skipped, delivery resumes at chunk 1"
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn send_telegram_rejects_an_invalid_cursor() -> anyhow::Result<()> {
        let (dispatcher, _store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(|_, _| (200, json!({"ok": true}), None)),
        )
        .await?;
        let mut item = outbox_item();
        item.next_chunk = 10_000;
        let error = dispatcher.send_telegram(&item).await.unwrap_err();
        assert!(error.permanent, "a corrupt cursor must not be replayed");
        assert!(error.to_string().contains("cursor is invalid"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn send_telegram_dead_letters_permanent_http_errors() -> anyhow::Result<()> {
        let (dispatcher, _store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(|_, _| (403, json!({"ok": false}), None)),
        )
        .await?;
        let error = dispatcher.send_telegram(&outbox_item()).await.unwrap_err();
        assert!(error.permanent);
        assert!(error.to_string().contains("403"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn order_rejects_invalid_arguments_and_oversized_text() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await?;

        // idempotency keys must match the identifier alphabet.
        let outcome = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "x".to_string(),
                    idempotency_key: Some("bad key with spaces!".to_string()),
                },
            )
            .await;
        assert!(outcome.is_error);

        // commands are trimmed and bounded.
        let outcome = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "   ".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        let outcome = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "x".repeat(20_000),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error, "oversized commands are rejected");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn template_failures_persist_as_failed_dispatches() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-broken-template");
        let mut config = testutil::fixture_config(&path);
        // The fixture bypasses startup template validation, so a broken template
        // reaches render time and must fail the dispatch durably.
        config.order_template = "{task_id}|{sender}|{recipient}|{message}|{unknown}".to_string();
        let mock =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        let outcome = dispatcher
            .order(
                "manager",
                OrderArgs {
                    agent: "developer".to_string(),
                    command: "do it".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        // The status lives in the durable ledger, not in the tool payload.
        let status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE status='failed'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "failed");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn order_all_requires_global_authority() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await?;
        let outcome = dispatcher
            .order_all(
                "developer",
                BroadcastArgs {
                    command: "sync".to_string(),
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        assert!(
            outcome.value["error"]
                .as_str()
                .unwrap()
                .contains("no broadcast authority")
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn send_telegram_reads_retry_after_from_the_response_body() -> anyhow::Result<()> {
        let (dispatcher, _store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, _| (202, json!({"run_id": "r"}))),
            Arc::new(|_, _| {
                (
                    429,
                    json!({"ok": false, "parameters": {"retry_after": 15}}),
                    None,
                )
            }),
        )
        .await?;
        let error = dispatcher.send_telegram(&outbox_item()).await.unwrap_err();
        assert_eq!(error.retry_after_seconds, Some(15), "body retry_after wins");
        testutil::remove_db_files(&path).await;
        Ok(())
    }
}
