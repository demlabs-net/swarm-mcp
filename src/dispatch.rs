use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context as _, anyhow, ensure};
use chrono::{DateTime, Utc};
use futures::{StreamExt, future::join_all};
use reqwest::{Client, Proxy};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, RwLock, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    config::{ApiKind, Config, TelegramBotMode},
    store::{AuditMessage, DeliveryQueueItem, MediaPayload, OutboxItem, Reservation, Store},
};

#[derive(Clone)]
pub struct Dispatcher {
    config: Arc<Config>,
    store: Store,
    hermes_client: Client,
    telegram_client: Client,
    inflight: Arc<Semaphore>,
    messaging_gate: Arc<RwLock<()>>,
    delivery_gates: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
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
    Queued { queue_id: String, position: i64 },
}

impl DispatchHandle {
    /// Primary identifier for JSON results.
    fn key_value(&self) -> (&'static str, String) {
        match self {
            DispatchHandle::Run(run_id) => ("run_id", run_id.clone()),
            DispatchHandle::Initiated => ("initiated", "true".to_string()),
            DispatchHandle::Queued { queue_id, .. } => ("queue_id", queue_id.clone()),
        }
    }

    fn decorate_result(&self, result: &mut Value) {
        if let DispatchHandle::Queued { position, .. } = self {
            result["queued"] = json!(true);
            result["queue_position"] = json!(position);
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

/// Shared aggregation of per-target broadcast outcomes (`dispatch_all`, `msg_all`,
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
                let mut result = json!({ (key): value });
                handle.decorate_result(&mut result);
                self.results.insert(target, result);
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
pub struct DispatchArgs {
    #[serde(rename = "recipient", alias = "agent")]
    pub agent: String,
    pub message: String,
    /// Opaque domain reference, such as an SLC task id. Swarm records and
    /// forwards it without validation against any task store.
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BroadcastArgs {
    pub message: String,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MessageArgs {
    #[serde(rename = "recipient", alias = "agent")]
    pub agent: String,
    pub message: String,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MessageAllArgs {
    pub message: String,
    pub correlation_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CancelDeliveryArgs {
    pub queue_id: String,
    pub reason: String,
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
    /// Base64-encoded content. Prefer `path` instead: large base64 passed
    /// through LLM tool-call arguments gets truncated by model output limits.
    #[serde(default)]
    pub content_b64: String,
    /// Path of the file inside the shared files directory (the swarm MCP
    /// reads it directly — reliable for any file size).
    #[serde(default)]
    pub path: Option<String>,
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
            delivery_gates: Arc::new(Mutex::new(BTreeMap::new())),
            config,
            store,
            hermes_client,
            telegram_client,
        })
    }

    pub async fn dispatch_to(&self, sender: &str, args: DispatchArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        let target = args.agent.trim().to_lowercase();
        let allowed = self
            .config
            .dispatch_acl
            .get(sender)
            .cloned()
            .unwrap_or_default();
        if !allowed.contains(&target) {
            return tool_error(json!({
                "ok": false,
                "error": format!("{sender} is not authorized to dispatch to '{target}'"),
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
        let correlation_id = match args.correlation_id.as_deref() {
            Some(value) => match validate_identifier(value, "correlation_id") {
                Ok(value) => Some(value),
                Err(error) => return tool_error_message(error),
            },
            None => None,
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let dispatch_id = format!("dispatch_{}", Uuid::new_v4().simple());
        let targets = vec![target.clone()];
        let fingerprint = fingerprint(&json!({
            "target": target,
            "message": normalize_dispatch_text(&message),
            "correlation_id": correlation_id,
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &dispatch_id,
                sender,
                "dispatch_to",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        let body = match render_dispatch_template(
            &self.config.dispatch_template,
            &dispatch_id,
            sender,
            &target,
            correlation_id.as_deref(),
            &message,
        ) {
            Ok(value) => value,
            Err(error) => {
                return self
                    .fail_dispatch(
                        &dispatch_id,
                        error,
                        sender,
                        "DISPATCH_FAILED",
                        &target,
                        &message,
                    )
                    .await;
            }
        };
        match self
            .dispatch_role_or_queue(
                &dispatch_id,
                sender,
                "dispatch_to",
                &target,
                &body,
                &self.config.executor_run_instructions,
            )
            .await
        {
            Ok(handle) => {
                let (key, value) = handle.key_value();
                let mut result = json!({
                    "ok": true,
                    "dispatch_id": dispatch_id,
                    "sender": sender,
                    "recipient": target,
                    "correlation_id": correlation_id,
                });
                result[key] = json!(value);
                handle.decorate_result(&mut result);
                // Ответ исполнителя (вывод run'а) доставляем в исходный
                // Telegram-чат отправителя: у dispatch_to (в отличие от
                // telegram_inbound) нет собственного chat_id, поэтому берём
                // последний известный чат роли-отправителя.
                if let DispatchHandle::Run(run_id) = &handle {
                    self.track_reply_for_sender(sender, run_id, &target).await;
                }
                let audit = self.audit(
                    sender,
                    "DISPATCH",
                    &target,
                    &format!("{dispatch_id}\n{message}"),
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
                    "DISPATCH_FAILED",
                    &target,
                    &message,
                )
                .await
            }
        }
    }

    /// Зарегистрировать run получателя для доставки финального ответа в
    /// исходный Telegram-чат отправителя (см. `spawn_run_reply_worker`).
    /// Используется там, где у доставки нет собственного `chat_id`
    /// (`dispatch_to`/`msg_to`): чат берётся как последний известный inbound-чат
    /// роли-отправителя. Если чата нет — ответ не трекается (терять нечему).
    async fn track_reply_for_sender(&self, sender: &str, run_id: &str, recipient: &str) {
        if !self.config.telegram_track_dispatch_replies {
            return;
        }
        let Ok(Some(chat_id)) = self.store.telegram_chat_for_role(sender).await else {
            return;
        };
        if let Err(error) = self.store.track_run_reply(run_id, recipient, chat_id).await {
            warn!(
                sender = %sender,
                recipient = %recipient,
                error = %error,
                "tracking run reply failed"
            );
        }
    }

    /// Cancel one queued transport wake. This deliberately accepts only the
    /// Swarm queue id and never interprets an SLC task/correlation id.
    pub async fn cancel_delivery(&self, sender: &str, args: CancelDeliveryArgs) -> ToolOutcome {
        let queue_id = match validate_identifier(&args.queue_id, "queue_id") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let reason = match Self::clean_control_reason(&args.reason) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        // The delivery worker takes the read side before selecting and
        // dispatching an item. The write side makes cancellation linearizable:
        // it either wins before POST /v1/runs or observes a delivered item.
        let _messaging_guard = self.messaging_gate.write().await;
        match self
            .store
            .cancel_delivery(&queue_id, sender, &self.config.manager_role, &reason)
            .await
        {
            Ok(value) => ToolOutcome {
                value,
                is_error: false,
            },
            Err(error) => tool_error(json!({
                "ok": false,
                "queue_id": queue_id,
                "error": error.to_string(),
            })),
        }
    }

    pub async fn dispatch_all(&self, sender: &str, args: BroadcastArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        if !self.config.global_authorities.contains(sender) {
            return tool_error(json!({
                "ok": false,
                "error": format!("{sender} has no broadcast authority"),
            }));
        }
        let message = match self.clean_text(&args.message, "message") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let correlation_id = match args.correlation_id.as_deref() {
            Some(value) => match validate_identifier(value, "correlation_id") {
                Ok(value) => Some(value),
                Err(error) => return tool_error_message(error),
            },
            None => None,
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let dispatch_id = format!("dispatch_{}", Uuid::new_v4().simple());
        let targets = self
            .config
            .dispatch_acl
            .get(sender)
            .cloned()
            .unwrap_or_default();
        if let Some(outcome) = self.messaging_block(sender, &targets).await {
            return outcome;
        }
        let fingerprint = fingerprint(&json!({
            "targets": targets,
            "message": normalize_dispatch_text(&message),
            "correlation_id": correlation_id,
        }));
        if let Some(outcome) = self
            .reserve_or_replay(
                &dispatch_id,
                sender,
                "dispatch_all",
                &targets,
                idempotency_key.as_deref(),
                &fingerprint,
            )
            .await
        {
            return outcome;
        }
        let requests = targets.iter().map(|target| async {
            let result = match render_dispatch_template(
                &self.config.dispatch_template,
                &dispatch_id,
                sender,
                target,
                correlation_id.as_deref(),
                &message,
            ) {
                Ok(message) => {
                    self.dispatch_role_or_queue(
                        &dispatch_id,
                        sender,
                        "dispatch_all",
                        target,
                        &message,
                        &self.config.executor_run_instructions,
                    )
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
            "dispatch_id": dispatch_id,
            "sender": sender,
            "correlation_id": correlation_id,
            "results": summary.results,
        });
        let audit = self.audit(
            sender,
            "DISPATCH_ALL",
            &targets.join(", "),
            &format!("{dispatch_id}\n{message}"),
            &mut result,
        );
        self.finish(&dispatch_id, status, &result, audit, !ok).await
    }

    pub async fn message(&self, sender: &str, args: MessageArgs) -> ToolOutcome {
        let _messaging_guard = self.messaging_gate.read().await;
        let target = args.agent.trim().to_lowercase();
        let allowed = self
            .config
            .all_roles
            .iter()
            .filter(|role| role.as_str() != sender)
            // mcp-only роли не принимают сообщения (endpoint отсутствует).
            .filter(|role| {
                !self
                    .config
                    .agents
                    .get(*role)
                    .is_some_and(|agent| agent.api_kind == ApiKind::Mcp)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !allowed.contains(&target) {
            return tool_error(json!({
                "ok": false,
                "error": "recipient must be another configured role",
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
        let correlation_id = match args.correlation_id.as_deref() {
            Some(value) => match validate_identifier(value, "correlation_id") {
                Ok(value) => Some(value),
                Err(error) => return tool_error_message(error),
            },
            None => None,
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
            "correlation_id": correlation_id,
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
                ("correlation_id", correlation_id.as_deref().unwrap_or("")),
                ("message", message.as_str()),
            ]),
        ) {
            Ok(value) => append_opaque_correlation_guard(value, correlation_id.as_deref()),
            Err(error) => {
                return self
                    .fail_dispatch(&message_id, error, sender, "MSG_FAILED", &target, &message)
                    .await;
            }
        };
        match self
            .dispatch_role_or_queue(
                &message_id,
                sender,
                "msg_to",
                &target,
                &body,
                &self.config.peer_run_instructions,
            )
            .await
        {
            Ok(handle) => {
                let (key, value) = handle.key_value();
                let mut result = json!({
                    "ok": true,
                    "message_id": message_id,
                    "recipient": target,
                    "correlation_id": correlation_id,
                });
                result[key] = json!(value);
                handle.decorate_result(&mut result);
                if let DispatchHandle::Run(run_id) = &handle {
                    self.track_reply_for_sender(sender, run_id, &target).await;
                }
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
        let correlation_id = match args.correlation_id.as_deref() {
            Some(value) => match validate_identifier(value, "correlation_id") {
                Ok(value) => Some(value),
                Err(error) => return tool_error_message(error),
            },
            None => None,
        };
        let idempotency_key = match validate_idempotency(args.idempotency_key.as_deref()) {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let message_id = format!("msg_{}", Uuid::new_v4().simple());
        let targets = self
            .config
            .all_roles
            .iter()
            .filter(|role| role.as_str() != sender)
            // mcp-only роли не принимают сообщения (endpoint отсутствует).
            .filter(|role| {
                !self
                    .config
                    .agents
                    .get(*role)
                    .is_some_and(|agent| agent.api_kind == ApiKind::Mcp)
            })
            .cloned()
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return tool_error(json!({
                "ok": false,
                "error": "no other roles to message",
            }));
        }
        if let Some(outcome) = self.messaging_block(sender, &targets).await {
            return outcome;
        }
        let fingerprint = fingerprint(&json!({
            "targets": targets,
            "message": normalize_dispatch_text(&message),
            "correlation_id": correlation_id,
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
                    ("correlation_id", correlation_id.as_deref().unwrap_or("")),
                    ("message", message.as_str()),
                ]),
            )
            .map(|value| append_opaque_correlation_guard(value, correlation_id.as_deref()));
            let result = match body {
                Ok(body) => {
                    self.dispatch_role_or_queue(
                        &message_id,
                        sender,
                        "msg_all",
                        target,
                        &body,
                        &self.config.peer_run_instructions,
                    )
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
            "correlation_id": correlation_id,
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
                    self.dispatch_role_or_queue(
                        &dispatch_id,
                        &manager,
                        "telegram_inbound",
                        target,
                        &body,
                        &self.config.telegram_inbound_instructions,
                    )
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
            if file.filename.is_empty() {
                return tool_error(json!({
                    "ok": false,
                    "error": "Telegram file must have a filename",
                }));
            }
            // Путь в общей папке — надёжный способ (base64 через tool-call
            // модели обрезается лимитом выходных токенов).
            let content_b64 = if let Some(path) = file.path {
                match self.read_shared_file(&path).await {
                    Ok(content) => content,
                    Err(error) => {
                        warn!(path = %path, error = %error, "reading shared reply file failed");
                        return tool_error(json!({
                            "ok": false,
                            "error": format!("cannot read shared file {path}: {error}"),
                        }));
                    }
                }
            } else {
                if file.content_b64.is_empty() {
                    return tool_error(json!({
                        "ok": false,
                        "error": "Telegram file must have content_b64 or a shared path",
                    }));
                }
                file.content_b64
            };
            media.push(MediaPayload {
                filename: file.filename,
                mime_type: file.mime_type,
                content_b64,
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

    /// Читает файл из общей папки для отправки вложением. Путь может быть
    /// абсолютным (внутри общей папки) или относительным к ней; выход за её
    /// пределы отклоняется. Размер ограничен 20 МБ (лимит Telegram).
    async fn read_shared_file(&self, path: &str) -> anyhow::Result<String> {
        use base64::Engine as _;
        let root = self.config.shared_files_dir.clone();
        let candidate = if std::path::Path::new(path).is_absolute() {
            std::path::PathBuf::from(path)
        } else {
            root.join(path)
        };
        let root_canonical = tokio::fs::canonicalize(&root)
            .await
            .with_context(|| format!("shared files dir {} is unavailable", root.display()))?;
        let file_canonical = tokio::fs::canonicalize(&candidate)
            .await
            .with_context(|| format!("shared file {path} does not exist"))?;
        ensure!(
            file_canonical.starts_with(&root_canonical),
            "path {path} escapes the shared files directory"
        );
        let metadata = tokio::fs::metadata(&file_canonical).await?;
        ensure!(metadata.is_file(), "shared path {path} is not a file");
        ensure!(
            metadata.len() <= 20 * 1024 * 1024,
            "shared file {path} exceeds the 20 MB Telegram limit"
        );
        let bytes = tokio::fs::read(&file_canonical).await?;
        Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
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

    pub fn spawn_delivery_worker(&self, cancellation: CancellationToken) -> JoinHandle<()> {
        let dispatcher = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(dispatcher.config.outbox_poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = dispatcher.flush_delivery_queue().await {
                            error!(error = %error, "role delivery queue flush failed");
                        }
                    }
                }
            }
        })
    }

    async fn flush_delivery_queue(&self) -> anyhow::Result<()> {
        for item in self
            .store
            .due_deliveries(self.config.outbox_batch_size)
            .await?
        {
            let _messaging_guard = self.messaging_gate.read().await;
            let role_gate = self.role_delivery_gate(&item.recipient).await;
            let _role_guard = role_gate.lock().await;
            if !self.store.delivery_eligible(&item.id).await? {
                continue;
            }
            match self
                .dispatch_role(&item.recipient, &item.body, &item.instructions)
                .await
            {
                Ok(handle) => {
                    let run_id = match &handle {
                        DispatchHandle::Run(run_id) => Some(run_id.as_str()),
                        DispatchHandle::Initiated => None,
                        DispatchHandle::Queued { .. } => unreachable!(
                            "delivery worker uses direct dispatch and cannot recursively queue"
                        ),
                    };
                    self.store.mark_delivery_delivered(&item.id, run_id).await?;
                    if item.kind == "telegram_inbound"
                        && let Some(run_id) = run_id
                    {
                        match self
                            .store
                            .telegram_chat_for_dispatch(&item.dispatch_id)
                            .await
                        {
                            Ok(Some(chat_id)) => {
                                if let Err(error) = self
                                    .store
                                    .track_run_reply(run_id, &item.recipient, chat_id)
                                    .await
                                {
                                    warn!(
                                        queue_id = %item.id,
                                        run_id,
                                        error = %error,
                                        "tracking delayed Telegram run reply failed"
                                    );
                                }
                            }
                            Ok(None) => warn!(
                                queue_id = %item.id,
                                dispatch_id = %item.dispatch_id,
                                "delayed Telegram delivery has no source chat"
                            ),
                            Err(error) => warn!(
                                queue_id = %item.id,
                                dispatch_id = %item.dispatch_id,
                                error = %error,
                                "reading delayed Telegram source chat failed"
                            ),
                        }
                    }
                    info!(
                        queue_id = %item.id,
                        dispatch_id = %item.dispatch_id,
                        recipient = %item.recipient,
                        "queued role delivery accepted"
                    );
                }
                Err(error) if error.retry_after_seconds.is_some() => {
                    let retry_after = error.retry_after_seconds.unwrap_or(1);
                    let exponent = u32::try_from(item.attempts.clamp(0, 5)).unwrap_or(5);
                    let backoff = 2_u64.saturating_pow(exponent).clamp(1, 30);
                    let delay = retry_after.max(backoff).min(60);
                    self.store
                        .defer_delivery(&item.id, Duration::from_secs(delay), &error.to_string())
                        .await?;
                    debug!(
                        queue_id = %item.id,
                        recipient = %item.recipient,
                        retry_after_seconds = delay,
                        "role remains busy; delivery stays queued"
                    );
                }
                Err(error) => {
                    self.store
                        .mark_delivery_dead(&item.id, &error.to_string())
                        .await?;
                    warn!(
                        queue_id = %item.id,
                        dispatch_id = %item.dispatch_id,
                        recipient = %item.recipient,
                        error = %error,
                        "queued role delivery requires recovery"
                    );
                }
            }
        }
        Ok(())
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
                Ok(RunOutcome::Running) => {
                    // Агент ещё думает (локальные модели — минуты): держим
                    // «печатает…» живым на каждом тике (интервал ≈ 5 с,
                    // пузырь Telegram живёт ~5–6 с).
                    if let Err(error) = self.send_typing(Some(chat_id)).await {
                        debug!(run_id = %run_id, error = %error, "typing heartbeat failed");
                    }
                }
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

    /// «Печатает…» в целевой чат (— fallback: общий чат).
    async fn send_typing(&self, chat: Option<i64>) -> anyhow::Result<()> {
        if !self.config.telegram_enabled {
            return Ok(());
        }
        let Some(token) = &self.config.telegram_bot_token else {
            return Ok(());
        };
        let chat_id = match (chat, &self.config.telegram_group_id) {
            (Some(chat), _) => chat.to_string(),
            (None, Some(group)) => group.clone(),
            (None, None) => return Ok(()),
        };
        let api_base = self
            .config
            .telegram_api_base_url
            .as_str()
            .trim_end_matches('/');
        let response = self
            .telegram_client
            .post(format!("{api_base}/bot{}/sendChatAction", token.expose()))
            .json(&json!({"chat_id": chat_id, "action": "typing"}))
            .send()
            .await
            .map_err(|_| anyhow!("Telegram sendChatAction request failed"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!("sendChatAction failed: HTTP {status}"));
        }
        Ok(())
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

    /// Attempt immediate delivery, then persist a transport-only FIFO wake on
    /// a definitive busy response. The queue contains no task authority or
    /// status; SLC remains the source of truth for whether work is runnable.
    async fn dispatch_role_or_queue(
        &self,
        operation_id: &str,
        sender: &str,
        kind: &str,
        role: &str,
        message: &str,
        instructions: &str,
    ) -> Result<DispatchHandle, RunFailure> {
        let role_gate = self.role_delivery_gate(role).await;
        let _role_guard = role_gate.lock().await;
        if self
            .store
            .has_pending_delivery(role)
            .await
            .map_err(|error| {
                RunFailure::rejected(anyhow!(
                    "cannot inspect the durable delivery queue for {role}: {error}"
                ))
            })?
        {
            return self
                .queue_role_delivery(
                    operation_id,
                    sender,
                    kind,
                    role,
                    message,
                    instructions,
                    Duration::from_secs(1),
                    "queued behind an earlier recipient delivery",
                )
                .await;
        }
        match self.dispatch_role(role, message, instructions).await {
            Ok(handle) => Ok(handle),
            Err(error) if error.retry_after_seconds.is_some() => {
                let retry_after_seconds = error.retry_after_seconds.unwrap_or(1);
                self.queue_role_delivery(
                    operation_id,
                    sender,
                    kind,
                    role,
                    message,
                    instructions,
                    Duration::from_secs(retry_after_seconds),
                    &error.to_string(),
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    async fn role_delivery_gate(&self, role: &str) -> Arc<Mutex<()>> {
        let mut gates = self.delivery_gates.lock().await;
        gates
            .entry(role.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    #[allow(clippy::too_many_arguments)]
    async fn queue_role_delivery(
        &self,
        operation_id: &str,
        sender: &str,
        kind: &str,
        role: &str,
        message: &str,
        instructions: &str,
        retry_after: Duration,
        error: &str,
    ) -> Result<DispatchHandle, RunFailure> {
        let queue_id = format!("delivery_{operation_id}_{role}");
        let item = DeliveryQueueItem {
            id: queue_id.clone(),
            dispatch_id: operation_id.to_string(),
            sender: sender.to_string(),
            kind: kind.to_string(),
            recipient: role.to_string(),
            body: message.to_string(),
            instructions: instructions.to_string(),
            attempts: 0,
        };
        let position = self
            .store
            .enqueue_delivery(&item, retry_after, error)
            .await
            .map_err(|queue_error| {
                RunFailure::rejected(anyhow!(
                    "{role} is busy and durable delivery enqueue failed: {queue_error}"
                ))
            })?;
        Ok(DispatchHandle::Queued { queue_id, position })
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
            .map_err(|error| role_transport_failure("openai initiate", &agent.role, error))?;
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
            .map_err(|error| role_transport_failure("dispatch run", role, error))?;
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
        let target = match self.queue_control_target(&args.agent) {
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

    fn queue_control_target(&self, value: &str) -> Result<String, ToolOutcome> {
        let target = value.trim().to_lowercase();
        if !self.config.all_roles.contains(&target) {
            return Err(tool_error(json!({
                "ok": false,
                "error": "queue controls apply only to configured swarm roles",
                "allowed": self.config.all_roles,
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

/// A TCP/DNS/connect failure happens before the role can observe the request,
/// so it is safe to retain the wake in the durable FIFO. Timeouts and body
/// errors remain indeterminate because the remote gateway may already have
/// accepted the run.
fn role_transport_failure(action: &str, role: &str, error: reqwest::Error) -> RunFailure {
    let is_connect = error.is_connect();
    let failure = anyhow!("{action} to {role}: {error}");
    if is_connect {
        RunFailure::retryable(failure, 1)
    } else {
        RunFailure::indeterminate(failure)
    }
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

fn render_dispatch_template(
    template: &str,
    dispatch_id: &str,
    sender: &str,
    recipient: &str,
    correlation_id: Option<&str>,
    message: &str,
) -> anyhow::Result<String> {
    render_template(
        template,
        &BTreeMap::from([
            ("dispatch_id", dispatch_id),
            ("sender", sender),
            ("recipient", recipient),
            ("correlation_id", correlation_id.unwrap_or("")),
            ("message", message),
        ]),
    )
    .map(|value| append_opaque_correlation_guard(value, correlation_id))
}

fn append_opaque_correlation_guard(mut body: String, correlation_id: Option<&str>) -> String {
    if let Some(task_id) = correlation_id.filter(|value| !value.is_empty()) {
        body.push_str(
            "\n\n[OPAQUE_CORRELATION] The Correlation value is the exact opaque SLC task ID. ",
        );
        body.push_str("Copy it byte-for-byte into task tools, including apparent truncation or suffixes; never expand, normalize, or reconstruct it from a task name or slug. Exact task_id: `");
        body.push_str(task_id);
        body.push('`');
    }
    body
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
    fn dispatch_template_uses_the_actual_recipient() {
        assert_eq!(
            render_dispatch_template(
                "{dispatch_id}|{sender}|{recipient}|{message}",
                "dispatch_1",
                "manager",
                "designer",
                None,
                "make a mockup",
            )
            .unwrap(),
            "dispatch_1|manager|designer|make a mockup"
        );
    }

    #[test]
    fn correlated_dispatch_appends_an_opaque_task_id_guard() {
        let body = render_dispatch_template(
            "{correlation_id}|{message}",
            "dispatch_1",
            "manager",
            "dev-senior-0",
            Some("whole_design_build_gostinitsa_sky_port_novosibir"),
            "ready",
        )
        .unwrap();

        assert!(body.starts_with("whole_design_build_gostinitsa_sky_port_novosibir|ready"));
        assert!(body.contains("[OPAQUE_CORRELATION]"));
        assert!(body.contains("Copy it byte-for-byte"));
        assert!(body.contains("never expand, normalize, or reconstruct"));
    }

    use std::collections::BTreeSet;

    use crate::testutil;
    use base64::Engine as _;
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
    async fn dispatch_accepts_and_persists_run() -> anyhow::Result<()> {
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "  do the thing  ".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected: {outcome:?}");
        assert_eq!(outcome.value["run_id"], json!("run-1"));
        let dispatch_id = outcome.value["dispatch_id"]
            .as_str()
            .expect("dispatch id")
            .to_string();
        assert!(dispatch_id.starts_with("dispatch_"));

        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(&dispatch_id)
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
    async fn dispatch_denies_unauthorized_targets() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "run"})))).await?;

        // developer has no dispatch ACL entry at all.
        let outcome = dispatcher
            .dispatch_to(
                "developer",
                DispatchArgs {
                    agent: "lead-developer".to_string(),
                    message: "x".to_string(),
                    correlation_id: None,
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

        // manager cannot dispatch to itself.
        let outcome = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "manager".to_string(),
                    message: "x".to_string(),
                    correlation_id: None,
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

        let manager_self_clear = dispatcher
            .clear_message_queue(
                "manager",
                ClearMessageQueueArgs {
                    agent: "manager".to_string(),
                    include_dead: true,
                },
            )
            .await;
        assert!(!manager_self_clear.is_error);
        assert_eq!(manager_self_clear.value["agent"], json!("manager"));

        let blocked_dispatch = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "must not start".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert_eq!(
            blocked_dispatch.value["disabled_agents"],
            json!(["developer"])
        );
        assert_eq!(
            blocked_dispatch.value["action_required"],
            json!("stop_current_run_and_escalate_to_human")
        );

        let blocked_peer = dispatcher
            .message(
                "lead-developer",
                MessageArgs {
                    agent: "developer".to_string(),
                    message: "must not arrive".to_string(),
                    correlation_id: None,
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "resume".to_string(),
                    correlation_id: None,
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
    async fn dispatch_all_aggregates_partial_results() -> anyhow::Result<()> {
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
            .dispatch_all(
                "manager",
                BroadcastArgs {
                    message: "sync".to_string(),
                    correlation_id: None,
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

        let dispatch_id = outcome.value["dispatch_id"].as_str().expect("dispatch id");
        let status: String = sqlx::query_scalar("SELECT status FROM dispatches WHERE id = ?")
            .bind(dispatch_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "partial", "durable partial result");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn executor_can_wake_manager_after_writing_slc_event() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-no-peers");
        let mut config = testutil::fixture_config(&path);
        config.agent_roles = vec!["developer".to_string()];
        config.all_roles = vec!["manager".to_string(), "developer".to_string()];
        config.dispatch_acl =
            BTreeMap::from([("manager".to_string(), vec!["developer".to_string()])]);
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

        // Task reports live in SLC, but an executor must still be able to wake
        // the manager through the transport after writing a terminal event.
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "manager".to_string(),
                    message: "SLC task task_example has a terminal event".to_string(),
                    correlation_id: Some("task_example".to_string()),
                    idempotency_key: Some("event_example".to_string()),
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected error: {}", outcome.value);
        assert_eq!(outcome.value["ok"], json!(true));
        assert_eq!(outcome.value["recipient"], json!("manager"));
        assert_eq!(outcome.value["correlation_id"], json!("task_example"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn msg_to_without_dispatch_id_is_a_plain_handoff() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-msg-no-correlation");
        let mut config = testutil::fixture_config(&path);
        let mock =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        // developer → lead-developer, no dispatch_id: must be accepted as a
        // plain coordination message (no domain correlation is required).
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "lead-developer".to_string(),
                    message: "Перезвони клиенту: Иван, +79139849832, хочет сайт".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!outcome.is_error, "unexpected error: {}", outcome.value);
        assert_eq!(outcome.value["ok"], json!(true));
        assert_eq!(outcome.value["recipient"], json!("lead-developer"));

        // Same again — idempotency replay must not flip it to an error.
        let replay = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "lead-developer".to_string(),
                    message: "Перезвони клиенту: Иван, +79139849832, хочет сайт".to_string(),
                    correlation_id: None,
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "retry me".to_string(),
                    correlation_id: None,
                    idempotency_key: Some("key-retry".to_string()),
                },
            )
            .await;
        assert!(first.is_error);
        let first_status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE sender='manager' AND kind='dispatch_to' AND idempotency_key='key-retry'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(first_status, "failed");

        // Retry with the SAME key re-executes: the failed dispatch released the key.
        let second = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "retry me".to_string(),
                    correlation_id: None,
                    idempotency_key: Some("key-retry".to_string()),
                },
            )
            .await;
        assert!(!second.is_error, "retry must re-execute: {second:?}");
        assert_eq!(second.value["run_id"], json!("run-ok"));
        assert!(second.value.get("deduplicated").is_none());
        let second_id = second.value["dispatch_id"].as_str().expect("dispatch id");

        // A further call with the same key replays the accepted result.
        let third = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "retry me".to_string(),
                    correlation_id: None,
                    idempotency_key: Some("key-retry".to_string()),
                },
            )
            .await;
        assert_eq!(third.value["deduplicated"], json!(true));
        assert_eq!(third.value["dispatch_id"], json!(second_id));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn busy_hermes_dispatch_is_accepted_into_durable_fifo() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let behavior: testutil::MockHermes = Arc::new(move |_, _| {
            if calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                (429, json!({"error": "role busy"}))
            } else {
                (202, json!({"run_id": "run-after-busy"}))
            }
        });
        let (dispatcher, store, path) = dispatcher_with_mock(behavior).await?;
        let args = DispatchArgs {
            agent: "developer".to_string(),
            message: "SLC task task_fifo is ready".to_string(),
            correlation_id: Some("task_fifo".to_string()),
            idempotency_key: Some("fifo-wake".to_string()),
        };
        let outcome = dispatcher.dispatch_to("manager", args).await;
        assert!(!outcome.is_error, "busy is queueable: {}", outcome.value);
        assert_eq!(outcome.value["queued"], json!(true));
        assert_eq!(outcome.value["queue_position"], json!(1));
        let queue_id = outcome.value["queue_id"].as_str().unwrap().to_string();
        let status: String = sqlx::query_scalar("SELECT status FROM delivery_outbox WHERE id=?")
            .bind(&queue_id)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(status, "pending");

        sqlx::query("UPDATE delivery_outbox SET next_attempt_ms=0 WHERE id=?")
            .bind(&queue_id)
            .execute(store.pool())
            .await?;
        dispatcher.flush_delivery_queue().await?;
        let delivered: (String, Option<String>) =
            sqlx::query_as("SELECT status, run_id FROM delivery_outbox WHERE id=?")
                .bind(&queue_id)
                .fetch_one(store.pool())
                .await?;
        assert_eq!(delivered.0, "delivered");
        assert_eq!(delivered.1.as_deref(), Some("run-after-busy"));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_hermes_role_is_accepted_into_durable_fifo() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("dispatch-unavailable");
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let unavailable = format!("http://{}", listener.local_addr()?);
        drop(listener);

        let mut config = testutil::fixture_config(&path);
        config.api_timeout = Duration::from_secs(1);
        for agent in config.agents.values_mut() {
            agent.api_url = Some(unavailable.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config, store.clone())?;

        let outcome = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "wake after maintenance".to_string(),
                    correlation_id: Some("task_after_maintenance".to_string()),
                    idempotency_key: Some("wake-after-maintenance".to_string()),
                },
            )
            .await;

        assert!(
            !outcome.is_error,
            "connect failure must be queued: {}",
            outcome.value
        );
        assert_eq!(outcome.value["queued"], json!(true));
        assert_eq!(outcome.value["queue_position"], json!(1));
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE status='pending' AND recipient='developer'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(pending, 1);

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn busy_delivery_queue_exposes_only_one_fifo_head_per_role() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_mock(Arc::new(move |_, _| {
            calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (429, json!({"error": "role busy"}))
        }))
        .await?;
        for index in 1..=2 {
            let outcome = dispatcher
                .dispatch_to(
                    "manager",
                    DispatchArgs {
                        agent: "developer".to_string(),
                        message: format!("wake {index}"),
                        correlation_id: Some(format!("task_fifo_{index}")),
                        idempotency_key: Some(format!("fifo-wake-{index}")),
                    },
                )
                .await;
            assert!(!outcome.is_error);
            assert_eq!(outcome.value["queue_position"], json!(index));
        }
        sqlx::query("UPDATE delivery_outbox SET next_attempt_ms=0")
            .execute(store.pool())
            .await?;
        let due = store.due_deliveries(10).await?;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].kind, "dispatch_to");
        assert_eq!(due[0].recipient, "developer");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a later wake must queue behind the first without overtaking it"
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn queued_delivery_can_be_cancelled_without_touching_later_fifo_work()
    -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_mock(Arc::new(move |_, _| {
            if calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                (429, json!({"error": "role busy"}))
            } else {
                (202, json!({"run_id": "run-valid-tail"}))
            }
        }))
        .await?;
        let first = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "obsolete wake".to_string(),
                    correlation_id: Some("obsolete_slc_item".to_string()),
                    idempotency_key: Some("obsolete-wake".to_string()),
                },
            )
            .await;
        let second = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "valid wake".to_string(),
                    correlation_id: Some("valid_slc_item".to_string()),
                    idempotency_key: Some("valid-wake".to_string()),
                },
            )
            .await;
        let first_id = first.value["queue_id"].as_str().unwrap().to_string();
        let second_id = second.value["queue_id"].as_str().unwrap().to_string();
        assert_eq!(second.value["queue_position"], json!(2));

        let unauthorized = dispatcher
            .cancel_delivery(
                "developer",
                CancelDeliveryArgs {
                    queue_id: first_id.clone(),
                    reason: "recipient cannot discard inbound work".to_string(),
                },
            )
            .await;
        assert!(unauthorized.is_error);
        assert!(
            unauthorized.value["error"]
                .as_str()
                .unwrap()
                .contains("original sender or swarm manager")
        );

        let cancelled = dispatcher
            .cancel_delivery(
                "manager",
                CancelDeliveryArgs {
                    queue_id: first_id.clone(),
                    reason: "canonical SLC work was cancelled before execution".to_string(),
                },
            )
            .await;
        assert!(!cancelled.is_error, "{}", cancelled.value);
        assert_eq!(cancelled.value["status"], json!("cancelled"));
        assert_eq!(cancelled.value["next_queue_id"], json!(second_id));
        assert_eq!(cancelled.value["remaining_pending"], json!(1));

        let replay = dispatcher
            .cancel_delivery(
                "manager",
                CancelDeliveryArgs {
                    queue_id: first_id,
                    reason: "idempotent operator retry".to_string(),
                },
            )
            .await;
        assert!(!replay.is_error);
        assert_eq!(replay.value["already_cancelled"], json!(true));

        let due = store.due_deliveries(10).await?;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, second_id);
        dispatcher.flush_delivery_queue().await?;
        let delivered: (String, Option<String>) =
            sqlx::query_as("SELECT status, run_id FROM delivery_outbox WHERE id=?")
                .bind(&second_id)
                .fetch_one(store.pool())
                .await?;
        assert_eq!(delivered.0, "delivered");
        assert_eq!(delivered.1.as_deref(), Some("run-valid-tail"));

        let too_late = dispatcher
            .cancel_delivery(
                "manager",
                CancelDeliveryArgs {
                    queue_id: second_id,
                    reason: "cannot retract an accepted Hermes run".to_string(),
                },
            )
            .await;
        assert!(too_late.is_error);
        assert!(
            too_late.value["error"]
                .as_str()
                .unwrap()
                .contains("can no longer be cancelled")
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn non_manager_sender_can_cancel_only_its_own_queued_delivery() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (429, json!({"error": "role busy"})))).await?;
        let queued = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "manager".to_string(),
                    message: "eligible SLC wake".to_string(),
                    correlation_id: Some("sender_owned_item".to_string()),
                    idempotency_key: Some("sender-owned-wake".to_string()),
                },
            )
            .await;
        assert!(!queued.is_error);
        let queue_id = queued.value["queue_id"].as_str().unwrap().to_string();

        let cancelled = dispatcher
            .cancel_delivery(
                "developer",
                CancelDeliveryArgs {
                    queue_id,
                    reason: "the originating SLC event is no longer runnable".to_string(),
                },
            )
            .await;
        assert!(!cancelled.is_error, "{}", cancelled.value);
        assert_eq!(cancelled.value["sender"], json!("developer"));
        assert_eq!(cancelled.value["status"], json!("cancelled"));
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "Implement the bounded task".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        let replay = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "  Implement   the bounded task\n".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;

        assert!(!first.is_error, "unexpected: {first:?}");
        assert!(!replay.is_error, "unexpected: {replay:?}");
        assert_eq!(replay.value["deduplicated"], json!(true));
        assert_eq!(replay.value["dispatch_id"], first.value["dispatch_id"]);
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
            Json(
                json!({"ok": true, "result": {"file_id": "f1", "file_unique_id": "u1", "file_path": "photos/x.png"}}),
            )
        };
        let router = Router::new()
            .route("/bot{token}/sendMessage", post(handler))
            .route("/bot{token}/sendChatAction", post(handler))
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "Coordinate one implementation detail".to_string(),
                    correlation_id: None,
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
                    correlation_id: None,
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
    async fn message_rejects_self_and_unknown_recipients() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "run"})))).await?;
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "developer".to_string(),
                    message: "self".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        let outcome = dispatcher
            .message(
                "developer",
                MessageArgs {
                    agent: "unknown-role".to_string(),
                    message: "not configured".to_string(),
                    correlation_id: None,
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "Coordinate the assigned task".to_string(),
                    correlation_id: None,
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
                    correlation_id: None,
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
    async fn dispatch_all_all_server_failures_are_indeterminate() -> anyhow::Result<()> {
        let (dispatcher, store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (500, json!({"error": "boom"})))).await?;
        let outcome = dispatcher
            .dispatch_all(
                "manager",
                BroadcastArgs {
                    message: "sync".to_string(),
                    correlation_id: None,
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
        let dispatch_id = outcome.value["dispatch_id"].as_str().expect("dispatch id");
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
    async fn telegram_inbound_busy_role_is_durable_and_tracks_delayed_reply() -> anyhow::Result<()>
    {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(move |_, _| {
            if calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                (429, json!({"error": "role busy"}))
            } else {
                (202, json!({"run_id": "run-delayed-telegram"}))
            }
        }))
        .await?;

        let outcome = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 8,
                message_id: 103,
                user_id: 42,
                username: "alice".to_string(),
                message: "queue this during maintenance".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;

        assert!(
            !outcome.is_error,
            "queueing must accept inbound: {outcome:?}"
        );
        assert_eq!(outcome.value["results"]["developer"]["queued"], json!(true));
        let queue_id = outcome.value["results"]["developer"]["queue_id"]
            .as_str()
            .expect("queue id")
            .to_string();
        sqlx::query("UPDATE delivery_outbox SET next_attempt_ms=0 WHERE id=?")
            .bind(&queue_id)
            .execute(store.pool())
            .await?;

        dispatcher.flush_delivery_queue().await?;

        let delivered: (String, Option<String>) =
            sqlx::query_as("SELECT status, run_id FROM delivery_outbox WHERE id=?")
                .bind(&queue_id)
                .fetch_one(store.pool())
                .await?;
        assert_eq!(delivered.0, "delivered");
        assert_eq!(delivered.1.as_deref(), Some("run-delayed-telegram"));
        let replies = store.due_run_replies().await?;
        assert!(replies.iter().any(|(run_id, role, chat_id, _)| {
            run_id == "run-delayed-telegram" && role == "developer" && *chat_id == 424_242
        }));

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
                path: None,
            },
            TelegramFileArgs {
                filename: "report.pdf".to_string(),
                mime_type: None,
                content_b64: "cGRm".to_string(),
                path: None,
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
                    path: None,
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
    async fn telegram_reply_reads_files_from_the_shared_path() -> anyhow::Result<()> {
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("POST /v1/chat/completions") {
                (200, json!({"id": "x", "choices": [{"message": {"role": "assistant", "content": ""}}]}))
            } else {
                (404, json!({"error": "unexpected"}))
            }
        }))
        .await?;
        let shared_dir = dispatcher.config.shared_files_dir.clone();
        std::fs::create_dir_all(&shared_dir)?;
        let payload = b"\x89PNG-real-bytes-123";
        std::fs::write(shared_dir.join("reply_cover.png"), payload)?;
        dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 15,
                message_id: 404,
                user_id: 42,
                username: "alice".to_string(),
                message: "draw".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        // Абсолютный путь внутри общей папки.
        let reply = dispatcher
            .telegram_reply(
                "developer",
                "готово".to_string(),
                vec![TelegramFileArgs {
                    filename: "cover.png".to_string(),
                    mime_type: Some("image/png".to_string()),
                    content_b64: String::new(),
                    path: Some(
                        shared_dir
                            .join("reply_cover.png")
                            .to_string_lossy()
                            .to_string(),
                    ),
                }],
            )
            .await;
        assert!(!reply.is_error, "unexpected: {:?}", reply.value);
        // Относительный путь тоже работает.
        let reply2 = dispatcher
            .telegram_reply(
                "developer",
                "готово2".to_string(),
                vec![TelegramFileArgs {
                    filename: "cover2.png".to_string(),
                    mime_type: None,
                    content_b64: String::new(),
                    path: Some("reply_cover.png".to_string()),
                }],
            )
            .await;
        assert!(!reply2.is_error, "unexpected: {:?}", reply2.value);
        let items = store.due_outbox(10).await?;
        let covers: Vec<_> = items
            .iter()
            .filter(|item| item.event == "TELEGRAM_REPLY")
            .collect();
        assert_eq!(covers.len(), 2);
        let expected = base64::engine::general_purpose::STANDARD.encode(payload);
        for cover in &covers {
            assert_eq!(
                cover.media[0].content_b64, expected,
                "файл прочитан целиком"
            );
        }
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&shared_dir);
        Ok(())
    }

    #[tokio::test]
    async fn telegram_reply_rejects_paths_outside_the_shared_dir() -> anyhow::Result<()> {
        let (dispatcher, _store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("POST /v1/chat/completions") {
                (200, json!({"id": "x", "choices": [{"message": {"role": "assistant", "content": ""}}]}))
            } else {
                (404, json!({"error": "unexpected"}))
            }
        }))
        .await?;
        let shared_dir = dispatcher.config.shared_files_dir.clone();
        std::fs::create_dir_all(&shared_dir)?;
        std::fs::write(shared_dir.join("secrets.txt"), b"secret")?;
        for bad_path in ["/etc/hostname", "/opt/data/../etc/passwd"] {
            let reply = dispatcher
                .telegram_reply(
                    "developer",
                    "x".to_string(),
                    vec![TelegramFileArgs {
                        filename: "evil.png".to_string(),
                        mime_type: None,
                        content_b64: String::new(),
                        path: Some(bad_path.to_string()),
                    }],
                )
                .await;
            assert!(
                reply.is_error,
                "path {bad_path} must be rejected: {:?}",
                reply.value
            );
        }
        // Отсутствующий файл внутри общей папки — тоже ошибка.
        let reply = dispatcher
            .telegram_reply(
                "developer",
                "x".to_string(),
                vec![TelegramFileArgs {
                    filename: "nope.png".to_string(),
                    mime_type: None,
                    content_b64: String::new(),
                    path: Some("no_such_file.png".to_string()),
                }],
            )
            .await;
        assert!(
            reply.is_error,
            "missing file must be rejected: {:?}",
            reply.value
        );
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&shared_dir);
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
                path: None,
            },
            TelegramFileArgs {
                filename: "b.pdf".to_string(),
                mime_type: None,
                content_b64: "Yg==".to_string(),
                path: None,
            },
            TelegramFileArgs {
                filename: "c.pdf".to_string(),
                mime_type: None,
                content_b64: "Yw==".to_string(),
                path: None,
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

    /// Покеда агент думает — воркер должен держать «печатает…» живым:
    /// на каждом тике flush шлётся `sendChatAction(chat_id)`.
    #[tokio::test]
    async fn flush_run_replies_keeps_typing_while_agent_runs() -> anyhow::Result<()> {
        let seen: Arc<std::sync::Mutex<Vec<(String, Value)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_bot = seen.clone();
        let (dispatcher, store, path) = dispatcher_with_telegram_bot(
            Arc::new(|_, body| {
                if body.starts_with("GET /v1/runs/") {
                    (
                        200,
                        json!({"object": "hermes.run", "run_id": "run-tg", "status": "running"}),
                    )
                } else {
                    (202, json!({"run_id": "run-tg"}))
                }
            }),
            Arc::new(move |path, body| {
                seen_bot
                    .lock()
                    .unwrap()
                    .push((path.to_string(), json!(body)));
                (200, json!({"ok": true}), None)
            }),
        )
        .await?;

        dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 7,
                message_id: 101,
                user_id: 42,
                username: "alice".to_string(),
                message: "hello".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;

        dispatcher.flush_run_replies().await?;

        let calls = seen.lock().unwrap().clone();
        let typing = calls
            .iter()
            .filter(|entry| entry.0.ends_with("sendChatAction"))
            .collect::<Vec<_>>();
        assert!(
            !typing.is_empty(),
            "heartbeat sendChatAction must fire while running: {calls:?}"
        );
        assert!(
            typing[0].1.to_string().contains("424242"),
            "typing must target the source chat: {typing:?}",
        );
        // Пузырь «печатает» ставится ровно один раз за тик (без спама sendMessage).
        assert!(
            !calls.iter().any(|entry| entry.0.ends_with("sendMessage")),
            "heartbeat must not send messages: {calls:?}",
        );
        // Пока running — трек не снимается, ответ ещё не поставлен.
        assert_eq!(store.due_run_replies().await?.len(), 1);
        // Ответа ещё нет: в outbox не должно быть TELEGRAM_REPLY (audit-запись
        // приёма — не считается).
        assert!(
            store
                .due_outbox(10)
                .await?
                .iter()
                .all(|item| item.event != "TELEGRAM_REPLY")
        );
        drop(calls);
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn dispatch_to_tracks_run_reply_to_senders_chat() -> anyhow::Result<()> {
        // Менеджер получил приказ из Telegram-чата, затем dispatch_to
        // контактеру — ответ контактера (вывод run) должен уйти в тот же чат.
        let (dispatcher, store, path) = dispatcher_with_telegram(Arc::new(|_, body| {
            if body.starts_with("GET /v1/runs/") {
                (
                    200,
                    json!({
                        "object": "hermes.run",
                        "run_id": "run-dt",
                        "status": "completed",
                        "output": "Отчёт контактера",
                    }),
                )
            } else {
                (202, json!({"run_id": "run-dt"}))
            }
        }))
        .await?;
        let inbound = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 77,
                message_id: 501,
                user_id: 42,
                username: "alice".to_string(),
                message: "приказ".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        assert!(!inbound.is_error, "unexpected: {:?}", inbound.value);

        let out = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "отчитайся о звонках".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert!(!out.is_error, "unexpected: {:?}", out.value);
        let tracks = store.due_run_replies().await?;
        assert!(
            tracks
                .iter()
                .any(|(run_id, role, chat_id, _)| run_id == "run-dt"
                    && role == "developer"
                    && *chat_id == 424_242),
            "ответ run'а контактера должен отслеживаться для чата менеджера: {tracks:?}"
        );

        dispatcher.flush_run_replies().await?;
        let items = store.due_outbox(10).await?;
        let reply = items
            .iter()
            .find(|item| item.event == "TELEGRAM_REPLY")
            .expect("the dispatch run reply must be enqueued");
        assert_eq!(reply.text, "Отчёт контактера");
        assert_eq!(reply.chat_id, Some(424_242));
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn dispatch_to_can_run_silently_without_inheriting_operator_chat() -> anyhow::Result<()> {
        let (mut dispatcher, store, path) =
            dispatcher_with_telegram(Arc::new(|_, _| (202, json!({"run_id": "run-silent"}))))
                .await?;
        Arc::make_mut(&mut dispatcher.config).telegram_track_dispatch_replies = false;

        let inbound = dispatcher
            .telegram_inbound(TelegramInboundArgs {
                update_id: 88,
                message_id: 601,
                user_id: 42,
                username: "alice".to_string(),
                message: "operator request".to_string(),
                targets: vec!["developer".to_string()],
                chat_id: 424_242,
            })
            .await;
        assert!(!inbound.is_error, "unexpected: {:?}", inbound.value);
        // Direct Telegram runs keep their normal reply contract. Remove that
        // fixture track so the assertion below observes only dispatch_to.
        store.untrack_run_reply("run-silent").await?;
        let out = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "silent internal wake".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;

        assert!(!out.is_error, "unexpected: {:?}", out.value);
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

        // An accepted transport dispatch queues an audit message in the outbox.
        dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "audit me".to_string(),
                    correlation_id: None,
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "audit after resume".to_string(),
                    correlation_id: None,
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
            sqlx::query_scalar("SELECT status FROM telegram_outbox WHERE event='DISPATCH'")
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
            sqlx::query_scalar("SELECT status FROM telegram_outbox WHERE event='DISPATCH'")
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
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "audit once".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;

        dispatcher.flush_outbox().await?;

        let (status, attempts): (String, i64) =
            sqlx::query_as("SELECT status, attempts FROM telegram_outbox WHERE event='DISPATCH'")
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
                .dispatch_to(
                    "manager",
                    DispatchArgs {
                        agent: "developer".to_string(),
                        message: command.to_string(),
                        correlation_id: None,
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
            event: "DISPATCH".to_string(),
            recipients: "developer".to_string(),
            text: "dispatch_1\nmake a mockup".repeat(300),
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
             VALUES ('audit_x','manager','DISPATCH','developer',?,'pending',0,0,0,0)",
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
            "[DISPATCH] manager -> developer",
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
    async fn dispatch_rejects_invalid_arguments_and_oversized_text() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await?;

        // idempotency keys must match the identifier alphabet.
        let outcome = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "x".to_string(),
                    correlation_id: None,
                    idempotency_key: Some("bad key with spaces!".to_string()),
                },
            )
            .await;
        assert!(outcome.is_error);

        // commands are trimmed and bounded.
        let outcome = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "   ".to_string(),
                    correlation_id: None,
                    idempotency_key: None,
                },
            )
            .await;
        assert!(outcome.is_error);
        let outcome = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "x".repeat(20_000),
                    correlation_id: None,
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
        config.dispatch_template =
            "{dispatch_id}|{sender}|{recipient}|{message}|{unknown}".to_string();
        let mock =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(mock.parse()?);
        }
        let config = Arc::new(config);
        let store = Store::connect(&config).await?;
        let dispatcher = Dispatcher::new(config.clone(), store.clone())?;

        let outcome = dispatcher
            .dispatch_to(
                "manager",
                DispatchArgs {
                    agent: "developer".to_string(),
                    message: "do it".to_string(),
                    correlation_id: None,
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
    async fn dispatch_all_requires_global_authority() -> anyhow::Result<()> {
        let (dispatcher, _store, path) =
            dispatcher_with_mock(Arc::new(|_, _| (202, json!({"run_id": "r"})))).await?;
        let outcome = dispatcher
            .dispatch_all(
                "developer",
                BroadcastArgs {
                    message: "sync".to_string(),
                    correlation_id: None,
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
