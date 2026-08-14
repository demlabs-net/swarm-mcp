use std::{collections::BTreeMap, sync::Arc};

use anyhow::{anyhow, ensure};
use futures::{StreamExt, future::join_all};
use reqwest::{Client, Proxy};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::{sync::Semaphore, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    store::{AuditMessage, OutboxItem, Reservation, Store},
};

#[derive(Clone)]
pub struct Dispatcher {
    config: Arc<Config>,
    store: Store,
    hermes_client: Client,
    telegram_client: Client,
    inflight: Arc<Semaphore>,
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
}

impl RunFailure {
    fn rejected(error: anyhow::Error) -> Self {
        Self {
            error,
            indeterminate: false,
        }
    }

    fn indeterminate(error: anyhow::Error) -> Self {
        Self {
            error,
            indeterminate: true,
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
}

impl TelegramFailure {
    fn new(message: impl Into<String>, retry_after_seconds: Option<i64>) -> Self {
        Self {
            message: message.into(),
            retry_after_seconds,
        }
    }
}

impl std::fmt::Display for TelegramFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderArgs {
    pub agent: String,
    pub command: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BroadcastArgs {
    pub command: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportArgs {
    pub summary: String,
    #[serde(default)]
    pub task_id: String,
    #[serde(default = "completed_status")]
    pub status: String,
    pub recipient: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageArgs {
    pub agent: String,
    pub message: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageAllArgs {
    pub message: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug)]
pub struct TelegramInboundArgs {
    pub update_id: i64,
    pub message_id: i64,
    pub user_id: i64,
    pub username: String,
    pub message: String,
    pub targets: Vec<String>,
}

fn completed_status() -> String {
    "completed".to_string()
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
            config,
            store,
            hermes_client,
            telegram_client,
        })
    }

    pub async fn order(&self, sender: &str, args: OrderArgs) -> ToolOutcome {
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
        let fingerprint = fingerprint(&json!({"target": target, "command": command}));
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
            .start_run(&target, &message, &self.config.executor_run_instructions)
            .await
        {
            Ok(run_id) => {
                let mut result = json!({
                    "ok": true,
                    "task_id": task_id,
                    "authority": sender,
                    "agent": target,
                    "run_id": run_id,
                });
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
        let fingerprint = fingerprint(&json!({"targets": targets, "command": command}));
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
                    self.start_run(target, &message, &self.config.executor_run_instructions)
                        .await
                }
                Err(error) => Err(RunFailure::rejected(error)),
            };
            (target.clone(), result)
        });
        let mut results = Map::new();
        let mut succeeded = 0_usize;
        let mut indeterminate = 0_usize;
        for (target, result) in join_all(requests).await {
            match result {
                Ok(run_id) => {
                    succeeded += 1;
                    results.insert(target, json!({"run_id": run_id}));
                }
                Err(error) => {
                    warn!(%target, error = %error, "broadcast dispatch failed");
                    indeterminate += usize::from(error.indeterminate);
                    results.insert(
                        target,
                        json!({
                            "error": error.to_string(),
                            "status": error.status(),
                            "recovery_required": error.indeterminate,
                        }),
                    );
                }
            }
        }
        let ok = succeeded == targets.len();
        let status = if ok {
            "accepted"
        } else if succeeded == 0 && indeterminate > 0 {
            "indeterminate"
        } else if succeeded == 0 {
            "failed"
        } else {
            "partial"
        };
        let mut result = json!({
            "ok": ok,
            "task_id": task_id,
            "authority": sender,
            "results": results,
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
        let supervisors = self.config.supervisors(sender);
        let recipient = args
            .recipient
            .as_deref()
            .unwrap_or(&self.config.manager_role)
            .trim()
            .to_lowercase();
        if !supervisors.contains(&recipient) {
            return tool_error(json!({
                "ok": false,
                "error": format!("{recipient} is not a supervisor of {sender}"),
                "allowed": supervisors,
            }));
        }
        let summary = match self.clean_text(&args.summary, "summary") {
            Ok(value) => value,
            Err(error) => return tool_error_message(error),
        };
        let task_id = if args.task_id.trim().is_empty() {
            "untracked".to_string()
        } else {
            match validate_identifier(&args.task_id, "task_id") {
                Ok(value) => value,
                Err(error) => return tool_error_message(error),
            }
        };
        let status = args.status.trim().to_lowercase();
        if !self.config.report_statuses.contains(&status) {
            return tool_error(json!({
                "ok": false,
                "error": "unsupported report status",
                "allowed": self.config.report_statuses,
            }));
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
            "summary": summary,
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
            .start_run(
                &recipient,
                &message,
                &self.config.supervisor_report_instructions,
            )
            .await
        {
            Ok(run_id) => {
                let mut result = json!({
                    "ok": true,
                    "report_id": dispatch_id,
                    "recipient": recipient,
                    "recipient_run_id": run_id,
                    "task_id": task_id,
                    "status": status,
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
        let fingerprint = fingerprint(&json!({"target": target, "message": message}));
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
            .start_run(&target, &body, &self.config.peer_run_instructions)
            .await
        {
            Ok(run_id) => {
                let mut result = json!({
                    "ok": true,
                    "message_id": message_id,
                    "agent": target,
                    "run_id": run_id,
                });
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
        let fingerprint = fingerprint(&json!({"targets": targets, "message": message}));
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
                    self.start_run(target, &body, &self.config.peer_run_instructions)
                        .await
                }
                Err(error) => Err(RunFailure::rejected(error)),
            };
            (target.clone(), result)
        });
        let mut results = Map::new();
        let mut succeeded = 0_usize;
        let mut indeterminate = 0_usize;
        for (target, result) in join_all(requests).await {
            match result {
                Ok(run_id) => {
                    succeeded += 1;
                    results.insert(target, json!({"run_id": run_id}));
                }
                Err(error) => {
                    warn!(%target, error = %error, "peer broadcast failed");
                    indeterminate += usize::from(error.indeterminate);
                    results.insert(
                        target,
                        json!({
                            "error": error.to_string(),
                            "status": error.status(),
                            "recovery_required": error.indeterminate,
                        }),
                    );
                }
            }
        }
        let ok = succeeded == targets.len();
        let status = if ok {
            "accepted"
        } else if succeeded == 0 && indeterminate > 0 {
            "indeterminate"
        } else if succeeded == 0 {
            "failed"
        } else {
            "partial"
        };
        let mut result = json!({
            "ok": ok,
            "message_id": message_id,
            "results": results,
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

        let dispatch_id = format!("telegram_{}", args.update_id);
        let idempotency_key = format!("telegram-update-{}", args.update_id);
        let fingerprint = fingerprint(&json!({
            "update_id": args.update_id,
            "message_id": args.message_id,
            "user_id": args.user_id,
            "username": args.username,
            "message": message,
            "targets": targets,
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
                    self.start_run(target, &body, &self.config.telegram_inbound_instructions)
                        .await
                }
                Err(error) => Err(RunFailure::rejected(error)),
            };
            (target.clone(), result)
        });
        let mut results = Map::new();
        let mut succeeded = 0_usize;
        let mut indeterminate = 0_usize;
        for (target, result) in join_all(requests).await {
            match result {
                Ok(run_id) => {
                    succeeded += 1;
                    results.insert(target, json!({"run_id": run_id}));
                }
                Err(error) => {
                    warn!(%target, error = %error, "Telegram inbound dispatch failed");
                    indeterminate += usize::from(error.indeterminate);
                    results.insert(
                        target,
                        json!({
                            "error": error.to_string(),
                            "status": error.status(),
                            "recovery_required": error.indeterminate,
                        }),
                    );
                }
            }
        }
        let ok = succeeded == targets.len();
        let status = if ok {
            "accepted"
        } else if succeeded == 0 && indeterminate > 0 {
            "indeterminate"
        } else if succeeded == 0 {
            "failed"
        } else {
            "partial"
        };
        let mut result = json!({
            "ok": ok,
            "dispatch_id": dispatch_id,
            "source": "telegram",
            "update_id": args.update_id,
            "message_id": args.message_id,
            "user_id": args.user_id,
            "username": args.username,
            "results": results,
        });
        let audit = self.audit(
            &manager,
            "TELEGRAM_INBOUND",
            &targets.join(", "),
            &format!(
                "{dispatch_id} [{status}]\nTelegram user {} ({}) → {}:\n{message}\n\nDispatch result: {}",
                args.username,
                args.user_id,
                targets.join(", "),
                Value::Object(results.clone())
            ),
            &mut result,
        );
        self.finish(&dispatch_id, status, &result, audit, !ok).await
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

    async fn flush_outbox(&self) -> anyhow::Result<()> {
        if !self.config.telegram_enabled {
            return Ok(());
        }
        for item in self.store.due_outbox(self.config.outbox_batch_size).await? {
            match self.send_telegram(&item).await {
                Ok(()) => {
                    self.store.mark_outbox_delivered(&item.id).await?;
                    info!(outbox_id = %item.id, "Telegram audit delivered");
                }
                Err(error) => {
                    let attempts = item.attempts + 1;
                    warn!(outbox_id = %item.id, attempts, error = %error, "Telegram audit delivery failed");
                    self.store
                        .mark_outbox_failed(
                            &item.id,
                            attempts,
                            self.config.outbox_max_attempts,
                            &error.to_string(),
                            error.retry_after_seconds,
                        )
                        .await?;
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
                TelegramFailure::new(
                    "Telegram bot token is not configured for delivery mode",
                    None,
                )
            })?;
        let group = self
            .config
            .telegram_group_id
            .as_ref()
            .ok_or_else(|| TelegramFailure::new("Telegram group is not configured", None))?;
        let url = format!(
            "{}/bot{}/sendMessage",
            self.config
                .telegram_api_base_url
                .as_str()
                .trim_end_matches('/'),
            token.expose()
        );
        let header = format!("[{}] {} -> {}", item.event, item.sender, item.recipients);
        let chunks = telegram_chunks(&header, &item.text, self.config.telegram_message_limit);
        let start = usize::try_from(item.next_chunk).unwrap_or(usize::MAX);
        if start > chunks.len() {
            return Err(TelegramFailure::new(
                "Telegram outbox chunk cursor is invalid",
                None,
            ));
        }
        for (index, chunk) in chunks.into_iter().enumerate().skip(start) {
            let response = self
                .telegram_client
                .post(&url)
                .json(&json!({
                    "chat_id": group,
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
        let _permit = tokio::time::timeout(self.config.api_timeout, self.inflight.acquire())
            .await
            .map_err(|_| RunFailure::rejected(anyhow!("dispatcher is saturated")))?
            .map_err(|_| RunFailure::rejected(anyhow!("dispatcher is shutting down")))?;
        let url = format!("{}/v1/runs", agent.api_url.as_str().trim_end_matches('/'));
        let response = self
            .hermes_client
            .post(url)
            .bearer_auth(agent.api_key.expose())
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
            let error = anyhow!("{role} API returned HTTP {}", response.status());
            return Err(if response.status().is_server_error() {
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
            .reserve_dispatch(
                id,
                sender,
                kind,
                targets,
                idempotency_key,
                fingerprint,
                self.config.rate_limit,
                self.config.rate_window,
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
            return Err(TelegramFailure::new("Telegram response is too large", None));
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
        return Err(TelegramFailure::new(
            format!("Telegram API returned HTTP {status}"),
            retry_after_seconds,
        ));
    }
    if payload.as_ref().and_then(|value| value.get("ok")) != Some(&Value::Bool(true)) {
        return Err(TelegramFailure::new(
            "Telegram API returned an invalid success response",
            retry_after_seconds,
        ));
    }
    Ok(())
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

fn validate_identifier(value: &str, field: &str) -> anyhow::Result<String> {
    let value = value.trim();
    ensure!(!value.is_empty(), "{field} must not be empty");
    ensure!(value.len() <= 160, "{field} exceeds 160 bytes");
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
    fn idempotency_keys_are_normalized_before_storage() {
        assert_eq!(
            validate_idempotency(Some("  stable-key  ")).unwrap(),
            Some("stable-key".to_string())
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
}
