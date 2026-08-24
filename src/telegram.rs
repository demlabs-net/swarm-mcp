use std::{sync::Arc, time::Duration};

use anyhow::{Context, anyhow, bail, ensure};
use futures::StreamExt;
use reqwest::{Client, Proxy};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{AppState, config::TelegramBacklogMode, dispatch::TelegramInboundArgs};

const MAX_TELEGRAM_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
    #[serde(default)]
    channel_post: Option<TelegramMessage>,
}

#[derive(Debug, Deserialize)]
struct TelegramMessage {
    message_id: i64,
    from: Option<TelegramUser>,
    chat: TelegramChat,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramUser {
    id: i64,
    #[serde(default)]
    is_bot: bool,
    username: Option<String>,
    first_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramChat {
    id: i64,
}

enum ParsedCommand {
    Dispatch {
        targets: Vec<String>,
        message: String,
    },
    Reply(String),
}

pub fn spawn_inbound_worker(
    state: Arc<AppState>,
    cancellation: CancellationToken,
) -> Option<JoinHandle<()>> {
    state.config.telegram_inbound_enabled.then(|| {
        tokio::spawn(async move {
            // Config validation makes init failure near-impossible, but never exit
            // permanently on a transient failure: retry with capped backoff.
            let mut backoff = Duration::from_secs(1);
            loop {
                if cancellation.is_cancelled() {
                    return;
                }
                match TelegramGateway::new(state.clone()) {
                    Ok(gateway) => {
                        // run() only returns after cancellation (polling errors are
                        // handled inside with their own backoff).
                        if let Err(error) = gateway.run(cancellation.clone()).await {
                            error!(error = %error, "Telegram inbound gateway stopped");
                        }
                        return;
                    }
                    Err(error) => {
                        error!(
                            error = %error,
                            retry_seconds = backoff.as_secs(),
                            "initialize Telegram inbound gateway failed; retrying"
                        );
                        tokio::select! {
                            () = cancellation.cancelled() => return,
                            () = tokio::time::sleep(backoff) => {}
                        }
                        backoff = backoff.saturating_mul(2).min(Duration::from_secs(60));
                    }
                }
            }
        })
    })
}

struct TelegramGateway {
    state: Arc<AppState>,
    client: Client,
    bot_url: String,
    group_id: i64,
}

impl TelegramGateway {
    fn new(state: Arc<AppState>) -> anyhow::Result<Self> {
        let token = state
            .config
            .telegram_bot_token
            .as_ref()
            .context("shared Telegram bot token is not configured")?;
        let group_id = state
            .config
            .telegram_group_id
            .as_deref()
            .context("Telegram group is not configured")?
            .parse::<i64>()
            .context("Telegram group is not numeric")?;
        let mut builder = Client::builder()
            .timeout(state.config.telegram_poll_timeout + Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(proxy) = &state.config.telegram_proxy_url {
            builder = builder.proxy(Proxy::all(proxy.as_str())?);
        }
        let client = builder.build()?;
        let bot_url = format!(
            "{}/bot{}",
            state
                .config
                .telegram_api_base_url
                .as_str()
                .trim_end_matches('/'),
            token.expose()
        );
        Ok(Self {
            state,
            client,
            bot_url,
            group_id,
        })
    }

    async fn run(self, cancellation: CancellationToken) -> anyhow::Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let cycle = tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                result = self.poll_once() => result,
            };
            match cycle {
                Ok(()) => backoff = Duration::from_secs(1),
                Err(error) => {
                    warn!(error = %error, retry_seconds = backoff.as_secs(), "Telegram inbound polling failed");
                    tokio::select! {
                        () = cancellation.cancelled() => return Ok(()),
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(60));
                }
            }
        }
    }

    async fn poll_once(&self) -> anyhow::Result<()> {
        let stored_offset = self.state.store.telegram_update_offset().await?;
        if stored_offset.is_none()
            && self.state.config.telegram_backlog_mode == TelegramBacklogMode::Discard
        {
            let updates = self.fetch_updates(-1, 0, 1).await?;
            let offset = updates
                .last()
                .map_or(Ok(0), |update| next_offset(update.update_id))?;
            self.state
                .store
                .advance_telegram_update_offset(offset)
                .await?;
            self.state.store.mark_telegram_poll_success().await?;
            info!(
                offset,
                "Telegram inbound initialized without replaying backlog"
            );
            return Ok(());
        }

        let offset = stored_offset.unwrap_or(0);
        let mut updates = self
            .fetch_updates(
                offset,
                self.state.config.telegram_poll_timeout.as_secs(),
                self.state.config.telegram_poll_limit,
            )
            .await?;
        updates.sort_by_key(|update| update.update_id);
        for update in updates {
            // A stale item can precede fresh items in the same sorted response.
            // Skip only that item; breaking here would starve every later update
            // and make the gateway fetch the same batch forever.
            if update.update_id < offset {
                continue;
            }
            self.process_update(&update).await?;
            self.state
                .store
                .advance_telegram_update_offset(next_offset(update.update_id)?)
                .await?;
        }
        self.state.store.mark_telegram_poll_success().await?;
        Ok(())
    }

    async fn fetch_updates(
        &self,
        offset: i64,
        timeout_seconds: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<TelegramUpdate>> {
        let response = self
            .client
            .post(format!("{}/getUpdates", self.bot_url))
            .timeout(Duration::from_secs(timeout_seconds.saturating_add(10)))
            .json(&json!({
                "offset": offset,
                "timeout": timeout_seconds,
                "limit": limit,
                "allowed_updates": ["message", "channel_post"],
            }))
            .send()
            .await
            .map_err(|_| anyhow!("Telegram getUpdates request failed"))?;
        let payload = telegram_payload(response).await?;
        serde_json::from_value(
            payload
                .get("result")
                .cloned()
                .context("Telegram getUpdates result is missing")?,
        )
        .context("decode Telegram updates")
    }

    async fn process_update(&self, update: &TelegramUpdate) -> anyhow::Result<()> {
        ensure!(update.update_id >= 0, "Telegram update ID is negative");
        let Some(message) = update.message.as_ref().or(update.channel_post.as_ref()) else {
            return Ok(());
        };
        let Some(user) = &message.from else {
            return Ok(());
        };
        // Принимаем сообщения из настроенного чата (группа/канал) и личные
        // сообщения (в личке chat.id совпадает с id отправителя).
        if message.chat.id != self.group_id && message.chat.id != user.id {
            return Ok(());
        }
        if user.is_bot || !self.state.config.telegram_allowed_users.contains(&user.id) {
            return Ok(());
        }
        let Some(text) = message.text.as_deref().map(str::trim) else {
            return Ok(());
        };
        let command = parse_command(text, &self.state.config.telegram_inbound_targets)
            .unwrap_or_else(|| ParsedCommand::Dispatch {
                targets: vec![self.state.config.manager_role.clone()],
                message: text.to_string(),
            });
        match command {
            ParsedCommand::Reply(text) => {
                if let Err(error) = self.send_text(message.chat.id, &text).await {
                    warn!(error = %error, "send Telegram command help failed");
                }
            }
            ParsedCommand::Dispatch {
                targets,
                message: text,
            } => {
                let username = user
                    .username
                    .clone()
                    .or_else(|| user.first_name.clone())
                    .unwrap_or_else(|| format!("user-{}", user.id));
                let outcome = self
                    .state
                    .dispatcher
                    .telegram_inbound(TelegramInboundArgs {
                        update_id: update.update_id,
                        message_id: message.message_id,
                        user_id: user.id,
                        username,
                        message: text,
                        targets,
                        chat_id: message.chat.id,
                    })
                    .await;
                if outcome.value.get("error").and_then(Value::as_str)
                    == Some("persistent dispatch reservation failed")
                {
                    bail!("Telegram dispatch could not be reserved");
                }
                let audit_queued = outcome
                    .value
                    .pointer("/telegram/queued")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if outcome.is_error && !audit_queued {
                    let reason = outcome
                        .value
                        .get("error")
                        .and_then(Value::as_str)
                        .unwrap_or("the request was not accepted");
                    let notice = format!(
                        "Swarm command {} was not accepted: {reason}",
                        update.update_id
                    );
                    if let Err(error) = self.send_text(message.chat.id, &notice).await {
                        warn!(error = %error, "send Telegram dispatch rejection failed");
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> anyhow::Result<()> {
        let response = self
            .client
            .post(format!("{}/sendMessage", self.bot_url))
            .json(&json!({
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": true,
            }))
            .send()
            .await
            .map_err(|_| anyhow!("Telegram sendMessage request failed"))?;
        telegram_payload(response).await?;
        Ok(())
    }
}

fn parse_command(text: &str, allowed_targets: &[String]) -> Option<ParsedCommand> {
    let text = text.trim();
    if !text.starts_with('/') {
        return None;
    }
    let (raw_command, rest) = text
        .split_once(char::is_whitespace)
        .map_or((text, ""), |(command, rest)| (command, rest.trim()));
    let command = raw_command
        .trim_start_matches('/')
        .split('@')
        .next()
        .unwrap_or_default()
        .to_lowercase()
        .replace('_', "-");
    let targets_text = allowed_targets.join(", ");
    let help = || {
        ParsedCommand::Reply(format!(
            "Swarm commands:\n/to <role> <message>\n/<role> <message>\n/all <message>\n/roles\nAllowed roles: {targets_text}"
        ))
    };
    match command.as_str() {
        "start" | "help" => Some(help()),
        "roles" => Some(ParsedCommand::Reply(format!(
            "Allowed Swarm roles: {targets_text}"
        ))),
        "all" => Some(if rest.is_empty() {
            help()
        } else {
            ParsedCommand::Dispatch {
                targets: allowed_targets.to_vec(),
                message: rest.to_string(),
            }
        }),
        "to" => {
            let Some((raw_target, message)) = rest.split_once(char::is_whitespace) else {
                return Some(help());
            };
            let target = raw_target.to_lowercase().replace('_', "-");
            let message = message.trim();
            Some(
                if allowed_targets.contains(&target) && !message.is_empty() {
                    ParsedCommand::Dispatch {
                        targets: vec![target],
                        message: message.to_string(),
                    }
                } else {
                    help()
                },
            )
        }
        role if allowed_targets.iter().any(|target| target == role) => Some(if rest.is_empty() {
            help()
        } else {
            ParsedCommand::Dispatch {
                targets: vec![role.to_string()],
                message: rest.to_string(),
            }
        }),
        _ => Some(help()),
    }
}

fn next_offset(update_id: i64) -> anyhow::Result<i64> {
    update_id
        .checked_add(1)
        .context("Telegram update ID overflow")
}

async fn telegram_payload(response: reqwest::Response) -> anyhow::Result<Value> {
    let status = response.status();
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| anyhow!("Telegram response failed"))?;
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .context("Telegram response is too large")?;
        ensure!(
            next_len <= MAX_TELEGRAM_RESPONSE_BYTES,
            "Telegram response is too large"
        );
        body.extend_from_slice(&chunk);
    }
    let payload: Value = serde_json::from_slice(&body).context("decode Telegram response")?;
    ensure!(status.is_success(), "Telegram API returned HTTP {status}");
    ensure!(
        payload.get("ok") == Some(&Value::Bool(true)),
        "Telegram API returned an unsuccessful response"
    );
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets() -> Vec<String> {
        vec![
            "manager".to_string(),
            "developer".to_string(),
            "lead-developer".to_string(),
        ]
    }

    #[test]
    fn parses_direct_alias_and_bot_mention() {
        let ParsedCommand::Dispatch { targets, message } =
            parse_command("/lead_developer@swarm_bot review this", &targets()).unwrap()
        else {
            panic!("expected a dispatch command");
        };
        assert_eq!(targets, vec!["lead-developer"]);
        assert_eq!(message, "review this");
    }

    #[test]
    fn parses_to_and_all_commands() {
        let ParsedCommand::Dispatch {
            targets: direct_targets,
            message,
        } = parse_command("/to developer implement it", &targets()).unwrap()
        else {
            panic!("expected a direct dispatch");
        };
        assert_eq!(direct_targets, vec!["developer"]);
        assert_eq!(message, "implement it");

        let ParsedCommand::Dispatch { targets: all, .. } =
            parse_command("/all status check", &targets()).unwrap()
        else {
            panic!("expected a broadcast dispatch");
        };
        assert_eq!(all, targets());
    }

    #[test]
    fn ignores_plain_group_messages() {
        assert!(parse_command("hello everyone", &targets()).is_none());
    }

    #[test]
    fn unknown_commands_and_empty_forms_fall_back_to_help() {
        let ParsedCommand::Reply(help) =
            parse_command("/frobnicate aggressively", &targets()).unwrap()
        else {
            panic!("unknown command must be a help reply");
        };
        assert!(help.contains("Allowed roles: manager, developer, lead-developer"));

        assert!(matches!(
            parse_command("/all   ", &targets()).unwrap(),
            ParsedCommand::Reply(_)
        ));
        assert!(matches!(
            parse_command("/to unknown-role hi", &targets()).unwrap(),
            ParsedCommand::Reply(_)
        ));
        assert!(matches!(
            parse_command("/developer", &targets()).unwrap(),
            ParsedCommand::Reply(_)
        ));
    }

    #[test]
    fn strips_foreign_bot_mention_suffix() {
        let ParsedCommand::Dispatch { targets, message } =
            parse_command("/developer@some_other_bot do it now", &targets()).unwrap()
        else {
            panic!("expected a dispatch");
        };
        assert_eq!(targets, vec!["developer"]);
        assert_eq!(message, "do it now");
    }

    #[test]
    fn next_offset_rejects_overflow() {
        assert_eq!(
            next_offset(i64::MAX).unwrap_err().to_string(),
            "Telegram update ID overflow"
        );
        assert_eq!(next_offset(41).unwrap(), 42);
    }

    use std::{collections::BTreeSet, sync::Mutex};

    use crate::{
        config::{Secret, TelegramBotMode},
        testutil,
    };

    /// Behavior of the mock Bot API: `(path, json_body) -> (status, payload, retry_after)`.
    type MockBot = Arc<dyn Fn(&str, &str) -> (u16, Value, Option<String>) + Send + Sync>;

    async fn spawn_mock_telegram(behavior: MockBot) -> String {
        use axum::{
            Json, Router,
            extract::{OriginalUri, State as AxumState},
            http::StatusCode,
            response::IntoResponse,
            routing::post,
        };
        let handler = move |AxumState(behavior): AxumState<MockBot>,
                            uri: OriginalUri,
                            Json(body): Json<Value>| async move {
            let (status, payload, retry_after) = behavior(uri.path(), &body.to_string());
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
        let router = Router::new()
            .route("/bot{token}/getUpdates", post(handler))
            .route("/bot{token}/sendMessage", post(handler))
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

    /// Bot API behavior that serves fixed updates for getUpdates and accepts
    /// sendMessage, recording the sent texts.
    fn canned_bot(updates: Value, sent: Arc<Mutex<Vec<String>>>) -> MockBot {
        Arc::new(move |path, body| {
            if path.ends_with("getUpdates") {
                (200, json!({"ok": true, "result": updates.clone()}), None)
            } else {
                sent.lock().expect("mock sent").push(body.to_string());
                (200, json!({"ok": true}), None)
            }
        })
    }

    fn allowed_update() -> Value {
        json!({
            "update_id": 5,
            "message": {
                "message_id": 100,
                "from": {"id": 42, "is_bot": false, "username": "alice"},
                "chat": {"id": -100_123},
                "text": "/developer hello"
            }
        })
    }

    async fn gateway_with_mocks(
        backlog: TelegramBacklogMode,
        bot: MockBot,
    ) -> anyhow::Result<(TelegramGateway, crate::store::Store, std::path::PathBuf)> {
        let path = testutil::temp_db_path("telegram");
        let telegram_base = spawn_mock_telegram(bot).await;
        let hermes_base =
            testutil::spawn_mock_hermes(Arc::new(|_, _| (202, json!({"run_id": "run-1"})))).await;
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = TelegramBotMode::Shared;
        config.telegram_bot_token = Some(Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_backlog_mode = backlog;
        config.telegram_api_base_url = telegram_base.parse()?;
        for agent in config.agents.values_mut() {
            agent.api_url = hermes_base.parse()?;
        }
        let config = Arc::new(config);
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(AppState {
            config,
            store: store.clone(),
            dispatcher,
        });
        let gateway = TelegramGateway::new(state)?;
        Ok((gateway, store, path))
    }

    #[tokio::test]
    async fn poll_once_fast_forwards_backlog_in_discard_mode() -> anyhow::Result<()> {
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Discard,
            canned_bot(json!([allowed_update()]), Arc::new(Mutex::new(Vec::new()))),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        let dispatches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dispatches")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(dispatches, 0, "backlog is skipped, not dispatched");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn poll_once_dispatches_updates_and_advances_offset() -> anyhow::Result<()> {
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([allowed_update()]), Arc::new(Mutex::new(Vec::new()))),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        let status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id='telegram_5'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "accepted", "the Telegram command reached the swarm");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn poll_once_skips_stale_updates_without_starving_fresh_ones() -> anyhow::Result<()> {
        let stale = allowed_update();
        let mut fresh = allowed_update();
        fresh["update_id"] = json!(6);
        fresh["message"]["message_id"] = json!(101);
        fresh["message"]["text"] = json!("/developer fresh command");
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([stale, fresh]), Arc::new(Mutex::new(Vec::new()))),
        )
        .await?;
        store.advance_telegram_update_offset(6).await?;

        gateway.poll_once().await?;

        assert_eq!(store.telegram_update_offset().await?, Some(7));
        let stale_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dispatches WHERE id='telegram_5'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(stale_count, 0, "the stale update must stay ignored");
        let fresh_status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id='telegram_6'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(fresh_status, "accepted");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn poll_once_ignores_unlisted_users_but_advances_offset() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["from"]["id"] = json!(999);
        update["message"]["from"]["username"] = json!("stranger");
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([update]), Arc::new(Mutex::new(Vec::new()))),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(
            store.telegram_update_offset().await?,
            Some(6),
            "the offset advances even for ignored updates"
        );
        let dispatches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dispatches")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(dispatches, 0, "an unlisted user must not reach the swarm");

        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn process_update_replies_with_help() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = json!("/help");
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([update]), sent.clone()),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        {
            let sent = sent.lock().expect("sent");
            assert_eq!(sent.len(), 1, "help must be answered in the group");
            assert!(
                sent[0].contains("Swarm commands:"),
                "help text: {}",
                sent[0]
            );
        }
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn process_update_ignores_messages_from_other_chats() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["chat"]["id"] = json!(999);
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([update]), sent.clone()),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        assert_eq!(
            sent.lock().expect("sent").len(),
            0,
            "a foreign chat must be ignored silently"
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn process_update_ignores_bots_and_textless_messages() -> anyhow::Result<()> {
        let mut bot_update = allowed_update();
        bot_update["message"]["from"]["is_bot"] = json!(true);
        let mut textless = allowed_update();
        textless["update_id"] = json!(6);
        textless["message"]["text"] = Value::Null;
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let (gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([bot_update, textless]), sent.clone()),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(7));
        assert_eq!(
            sent.lock().expect("sent").len(),
            0,
            "bots and messages without text never reach the swarm"
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn poll_once_surfaces_bot_api_failures() -> anyhow::Result<()> {
        // The Bot API answers with a non-JSON body: the payload decode must fail.
        let (gateway, _store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            Arc::new(|path, _| {
                if path.ends_with("getUpdates") {
                    (200, json!("this is not an object"), None)
                } else {
                    (200, json!({"ok": true}), None)
                }
            }),
        )
        .await?;
        assert!(gateway.poll_once().await.is_err());
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn poll_once_tolerates_send_message_failures() -> anyhow::Result<()> {
        // sendMessage fails: the help reply is best-effort and must not fail the poll.
        let mut update = allowed_update();
        update["message"]["text"] = json!("/help");
        let update = Arc::new(update);
        let updates = update.clone();
        let (gateway, _store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            Arc::new(move |path, _| {
                if path.ends_with("getUpdates") {
                    (200, json!({"ok": true, "result": [*updates.clone()]}), None)
                } else {
                    (500, json!({"ok": false}), None)
                }
            }),
        )
        .await?;
        gateway.poll_once().await?;
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn inbound_worker_is_absent_when_disabled() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("telegram-worker");
        let config = Arc::new(testutil::fixture_config(&path));
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(AppState {
            config,
            store,
            dispatcher,
        });
        // The fixture has inbound disabled by default.
        assert!(spawn_inbound_worker(state, CancellationToken::new()).is_none());
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn inbound_worker_runs_and_stops_on_cancellation() -> anyhow::Result<()> {
        let (gateway, _store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            canned_bot(json!([]), Arc::new(Mutex::new(Vec::new()))),
        )
        .await?;
        let token = CancellationToken::new();
        let worker = spawn_inbound_worker(gateway.state.clone(), token.clone())
            .expect("inbound is enabled in this fixture");
        tokio::time::sleep(Duration::from_millis(300)).await;
        token.cancel();
        worker.await.expect("worker joined");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn inbound_worker_retries_gateway_initialization() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("telegram-worker-init");
        let mut config = testutil::fixture_config(&path);
        // The fixture bypasses config validation: inbound enabled but no shared
        // token -> TelegramGateway::new fails and the worker must retry.
        config.telegram_inbound_enabled = true;
        let config = Arc::new(config);
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(AppState {
            config,
            store,
            dispatcher,
        });
        let token = CancellationToken::new();
        let worker = spawn_inbound_worker(state, token.clone()).expect("worker spawned");
        tokio::time::sleep(Duration::from_millis(150)).await;
        token.cancel();
        worker.await.expect("worker joined");
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn run_loop_backs_off_and_resets_after_success() -> anyhow::Result<()> {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let bot: MockBot = Arc::new(move |path, _| {
            if path.ends_with("getUpdates") {
                if calls_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    (500, json!({"ok": false}), None)
                } else {
                    (200, json!({"ok": true, "result": []}), None)
                }
            } else {
                (200, json!({"ok": true}), None)
            }
        });
        let (gateway, _store, path) = gateway_with_mocks(TelegramBacklogMode::Process, bot).await?;
        let token = CancellationToken::new();
        let run = tokio::spawn(gateway.run(token.clone()));
        // First poll fails (backoff 1s), the retry succeeds and resets the backoff.
        tokio::time::sleep(Duration::from_millis(1_600)).await;
        token.cancel();
        run.await.expect("run joined")?;
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "the poll must have been retried after the failure"
        );
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn poll_once_maps_network_failures() -> anyhow::Result<()> {
        let path = testutil::temp_db_path("telegram-dead-api");
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = TelegramBotMode::Shared;
        config.telegram_bot_token = Some(Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        // A dead Bot API endpoint exercises the transport error mapping.
        config.telegram_api_base_url = "http://127.0.0.1:1".parse()?;
        let config = Arc::new(config);
        let store = crate::store::Store::connect(&config).await?;
        let dispatcher = crate::dispatch::Dispatcher::new(config.clone(), store.clone())?;
        let state = Arc::new(AppState {
            config,
            store,
            dispatcher,
        });
        let gateway = TelegramGateway::new(state)?;
        let error = gateway.poll_once().await.unwrap_err().to_string();
        assert!(error.contains("getUpdates request failed"), "{error}");
        testutil::remove_db_files(&path).await;
        Ok(())
    }
}
