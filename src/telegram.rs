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
            match TelegramGateway::new(state) {
                Ok(gateway) => {
                    if let Err(error) = gateway.run(cancellation).await {
                        error!(error = %error, "Telegram inbound gateway stopped");
                    }
                }
                Err(error) => {
                    error!(error = %error, "initialize Telegram inbound gateway failed");
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
                "allowed_updates": ["message"],
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
        let Some(message) = &update.message else {
            return Ok(());
        };
        if message.chat.id != self.group_id {
            return Ok(());
        }
        let Some(user) = &message.from else {
            return Ok(());
        };
        if user.is_bot || !self.state.config.telegram_allowed_users.contains(&user.id) {
            return Ok(());
        }
        let Some(text) = message.text.as_deref().map(str::trim) else {
            return Ok(());
        };
        let Some(command) = parse_command(text, &self.state.config.telegram_inbound_targets) else {
            return Ok(());
        };
        match command {
            ParsedCommand::Reply(text) => {
                if let Err(error) = self.send_text(&text).await {
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
                    if let Err(error) = self.send_text(&notice).await {
                        warn!(error = %error, "send Telegram dispatch rejection failed");
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_text(&self, text: &str) -> anyhow::Result<()> {
        let response = self
            .client
            .post(format!("{}/sendMessage", self.bot_url))
            .json(&json!({
                "chat_id": self.group_id,
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
}
