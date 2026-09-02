use std::{
    collections::HashSet,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    time::Duration,
};

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
static FILE_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

type AttachmentTarget<'a> = (&'a str, Option<i64>, Option<String>, Option<String>);

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
    /// Photos attached to the message (largest size is used); the caption
    /// carries the accompanying text.
    #[serde(default)]
    photo: Vec<TelegramPhotoSize>,
    #[serde(default)]
    document: Option<TelegramDocument>,
    #[serde(default)]
    caption: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramPhotoSize {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    width: i64,
    height: i64,
    #[serde(default)]
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TelegramDocument {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    file_name: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
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
    api_base: String,
    token: String,
    group_id: i64,
    queued_notices: Mutex<HashSet<i64>>,
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
        let api_base = state
            .config
            .telegram_api_base_url
            .as_str()
            .trim_end_matches('/')
            .to_string();
        let token_value = token.expose().to_string();
        let bot_url = format!("{api_base}/bot{token_value}");
        Ok(Self {
            state,
            client,
            bot_url,
            api_base,
            token: token_value,
            group_id,
            queued_notices: Mutex::new(HashSet::new()),
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
        // Текст: обычный text или caption фото/документа. Сообщение с одними
        // файлами (без текста) тоже обрабатывается.
        let raw_text = message
            .text
            .as_deref()
            .or(message.caption.as_deref())
            .map(str::trim)
            .unwrap_or_default();
        let mut text = raw_text.to_string();
        // Вложения: скачиваем файлы через Bot API и сохраняем в общую с
        // агентами папку, путь передаём в сообщении.
        let notes = self.download_attachments(message).await;
        if text.is_empty() && notes.is_empty() {
            // Сообщение без текста и без вложений — нечего диспатчить.
            return Ok(());
        }
        for note in notes {
            text.push_str(&note);
        }
        let command = parse_mention(&text, &self.state.config.telegram_inbound_targets)
            .or_else(|| parse_command(&text, &self.state.config.telegram_inbound_targets))
            .unwrap_or_else(|| ParsedCommand::Dispatch {
                targets: vec![self.state.config.manager_role.clone()],
                message: text.clone(),
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
                if outcome
                    .value
                    .get("retryable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    let first_notice = self
                        .queued_notices
                        .lock()
                        .expect("Telegram queue notice lock")
                        .insert(update.update_id);
                    if first_notice {
                        let notice = "⏳ Запрос принят и ждёт свободного слота менеджера. Отвечу автоматически; повторять сообщение не нужно.";
                        if let Err(error) = self.send_text(message.chat.id, notice).await {
                            warn!(error = %error, "send Telegram queued notice failed");
                        }
                    }
                    bail!("Telegram target is busy; update retained for retry");
                }
                self.queued_notices
                    .lock()
                    .expect("Telegram queue notice lock")
                    .remove(&update.update_id);
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

    /// Скачивает файловые вложения сообщения через Bot API (getFile + файл),
    /// сохраняет в общую с агентами папку и возвращает заметки для сообщения
    /// диспатча. Сообщения с одними файлами (без текста) тоже обрабатываются.
    async fn download_attachments(&self, message: &TelegramMessage) -> Vec<String> {
        let mut notes = Vec::new();
        let mut targets: Vec<AttachmentTarget<'_>> = Vec::new();
        // Самое крупное фото (если несколько — берём максимальное).
        if let Some(photo) = message.photo.iter().max_by_key(|p| p.width * p.height) {
            targets.push((
                &photo.file_id,
                photo.file_size,
                Some("image/jpeg".to_string()),
                Some(format!("photo_{}x{}.jpg", photo.width, photo.height)),
            ));
        }
        if let Some(document) = &message.document {
            targets.push((
                &document.file_id,
                document.file_size,
                document.mime_type.clone(),
                document.file_name.clone(),
            ));
        }
        let dir = self.state.config.shared_files_dir.clone();
        if let Err(error) = tokio::fs::create_dir_all(&dir).await {
            warn!(error = %error, path = %dir.display(), "inbound files dir create failed");
            return notes;
        }
        for (file_id, size, mime, name) in targets {
            match self.download_file(file_id).await {
                Ok(bytes) => {
                    let saved = save_inbound_file(&dir, name.as_deref(), &bytes).await;
                    match saved {
                        Ok(path) => {
                            let downloaded_size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
                            let size_kb = size.unwrap_or(downloaded_size) / 1024;
                            notes.push(format!(
                                "\n📎 Вложение: {} ({}), {} КБ — путь: {}",
                                path.file_name().map_or_else(
                                    || "file".into(),
                                    |n| n.to_string_lossy().to_string()
                                ),
                                mime.as_deref().unwrap_or("application/octet-stream"),
                                size_kb,
                                path.display()
                            ));
                        }
                        Err(error) => {
                            warn!(file_id = %file_id, error = %error, "saving inbound file failed");
                        }
                    }
                }
                Err(error) => {
                    warn!(file_id = %file_id, error = %error, "downloading inbound file failed");
                }
            }
        }
        // Чистим файлы старше суток (входящие вложения не должны копиться).
        cleanup_inbound_files(&dir, Duration::from_secs(24 * 60 * 60)).await;
        notes
    }

    async fn download_file(&self, file_id: &str) -> anyhow::Result<Vec<u8>> {
        // Явные таймауты на каждый запрос: клиент гейтвея ограничен
        // (poll_timeout + 10s), а скачивание вложения может быть дольше.
        let get_url = format!("{}/getFile?file_id={}", self.bot_url, file_id);
        let payload: Value = self
            .client
            .get(&get_url)
            .timeout(Duration::from_secs(30))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let file_path = payload
            .pointer("/result/file_path")
            .and_then(Value::as_str)
            .context("getFile result is missing file_path")?;
        let file_url = format!(
            "{}/file/bot{}/{}",
            self.api_base,
            self.token,
            file_path.trim_start_matches('/')
        );
        let bytes = self
            .client
            .get(&file_url)
            .timeout(Duration::from_secs(120))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        Ok(bytes.to_vec())
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

/// Сохраняет вложение в папку входящих файлов с уникальным именем
/// (`<unix_ms>_<safe_name>`); возвращает полный путь.
async fn save_inbound_file(
    dir: &std::path::Path,
    name: Option<&str>,
    bytes: &[u8],
) -> anyhow::Result<std::path::PathBuf> {
    let safe: String = name
        .unwrap_or("file")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .take(80)
        .collect();
    let safe = if safe.trim().is_empty() {
        "file".to_string()
    } else {
        safe
    };
    let sequence = FILE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = dir.join(format!(
        "{}_{sequence}_{safe}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default()
    ));
    tokio::fs::write(&path, bytes).await?;
    Ok(path)
}

/// Удаляет входящие файлы старше `max_age`.
async fn cleanup_inbound_files(dir: &std::path::Path, max_age: Duration) {
    let Ok(entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    let mut entries = entries;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Ok(metadata) = entry.metadata().await
            && metadata.is_file()
            && let Ok(modified) = metadata.modified()
            && let Ok(age) = modified.elapsed()
            && age > max_age
        {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
}

/// Parses an "@role message" mention (e.g. "@writer сделай X", "@all текст").
/// Returns None when the message is not a mention of an allowed target.
fn parse_mention(text: &str, allowed_targets: &[String]) -> Option<ParsedCommand> {
    let text = text.trim();
    let (raw_mention, rest) = text
        .split_once(char::is_whitespace)
        .map_or((text, ""), |(m, r)| (m, r.trim()));
    let mention = raw_mention.strip_prefix('@')?.to_lowercase();
    let mention = mention.split('@').next().unwrap_or_default();
    if mention == "all" {
        return if rest.is_empty() {
            None
        } else {
            Some(ParsedCommand::Dispatch {
                targets: allowed_targets.to_vec(),
                message: rest.to_string(),
            })
        };
    }
    if allowed_targets.iter().any(|target| target == mention) && !rest.is_empty() {
        return Some(ParsedCommand::Dispatch {
            targets: vec![mention.to_string()],
            message: rest.to_string(),
        });
    }
    None
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
    fn parses_role_mentions() {
        let allowed = targets();
        let ParsedCommand::Dispatch { targets, message } =
            parse_mention("@developer implement it", &allowed).unwrap()
        else {
            panic!("expected a mention dispatch");
        };
        assert_eq!(targets, vec!["developer"]);
        assert_eq!(message, "implement it");

        let ParsedCommand::Dispatch { targets: all, .. } =
            parse_mention("@all status check", &allowed).unwrap()
        else {
            panic!("expected an @all dispatch");
        };
        assert_eq!(all, allowed);

        assert!(parse_mention("plain text", &allowed).is_none());
        assert!(parse_mention("@unknown do it", &allowed).is_none());
        assert!(parse_mention("@developer", &allowed).is_none());
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
        spawn_mock_telegram_with_files(behavior, Vec::new()).await
    }

    /// Mock с поддержкой входящих файлов: `/getFile` отдаёт фиксированный
    /// `file_path`, `/file/bot{token}/{path}` — сырые `file_bytes`.
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
        let file_handler = move || async move {
            let bytes = file_bytes.clone();
            let mut response = (StatusCode::OK, bytes).into_response();
            response.headers_mut().insert(
                reqwest::header::CONTENT_TYPE,
                "image/png".parse().expect("mime"),
            );
            response
        };
        let get_file_handler = move || async move {
            Json(
                json!({"ok": true, "result": {"file_id": "f1", "file_unique_id": "u1", "file_path": "photos/x.png"}}),
            )
        };
        let router = Router::new()
            .route("/bot{token}/getUpdates", post(handler))
            .route("/bot{token}/sendMessage", post(handler))
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
        gateway_with_mocks_and_hermes(
            backlog,
            bot,
            Arc::new(|_, _| (202, json!({"run_id": "run-1"}))),
        )
        .await
    }

    async fn gateway_with_mocks_and_files_hermes(
        backlog: TelegramBacklogMode,
        bot: MockBot,
        file_bytes: Vec<u8>,
        hermes_behavior: testutil::MockHermes,
    ) -> anyhow::Result<(TelegramGateway, crate::store::Store, std::path::PathBuf)> {
        let path = testutil::temp_db_path("telegram-files");
        let telegram_base = spawn_mock_telegram_with_files(bot, file_bytes).await;
        let hermes_base = testutil::spawn_mock_hermes(hermes_behavior).await;
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = TelegramBotMode::Shared;
        config.telegram_bot_token = Some(Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["manager".to_string(), "developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_backlog_mode = backlog;
        config.telegram_api_base_url = telegram_base.parse()?;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(hermes_base.parse()?);
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

    async fn gateway_with_mocks_and_hermes(
        backlog: TelegramBacklogMode,
        bot: MockBot,
        hermes_behavior: testutil::MockHermes,
    ) -> anyhow::Result<(TelegramGateway, crate::store::Store, std::path::PathBuf)> {
        let path = testutil::temp_db_path("telegram");
        let telegram_base = spawn_mock_telegram(bot).await;
        let hermes_base = testutil::spawn_mock_hermes(hermes_behavior).await;
        let mut config = testutil::fixture_config(&path);
        config.telegram_enabled = true;
        config.telegram_bot_mode = TelegramBotMode::Shared;
        config.telegram_bot_token = Some(Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["manager".to_string(), "developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_backlog_mode = backlog;
        config.telegram_api_base_url = telegram_base.parse()?;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(hermes_base.parse()?);
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
    async fn busy_target_retains_update_and_sends_one_queue_notice() -> anyhow::Result<()> {
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let (gateway, store, path) = gateway_with_mocks_and_hermes(
            TelegramBacklogMode::Process,
            canned_bot(json!([allowed_update()]), sent.clone()),
            Arc::new(|_, _| (429, json!({"error": "busy"}))),
        )
        .await?;

        let first = gateway.poll_once().await;
        assert!(
            first.is_err(),
            "busy target must retain the Telegram update"
        );
        assert_eq!(
            store.telegram_update_offset().await?,
            None,
            "the durable offset must not advance before target admission"
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id='telegram_5'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "failed", "the failed reservation is retryable");
        let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            outbox, 0,
            "transient queue pressure must not spam the shared audit group"
        );
        assert_eq!(sent.lock().expect("sent").len(), 1);
        assert!(
            sent.lock().expect("sent")[0].contains("ждёт свободного слота"),
            "operator receives a queue notice"
        );

        let second = gateway.poll_once().await;
        assert!(second.is_err());
        assert_eq!(
            sent.lock().expect("sent").len(),
            1,
            "the same update must not spam repeated queue notices"
        );

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
    async fn inbound_files_are_downloaded_and_passed_to_the_dispatch() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = Value::Null;
        update["message"]["caption"] = json!("Вот логотип");
        update["message"]["photo"] = json!([
            {"file_id": "small", "file_unique_id": "us", "width": 100, "height": 50, "file_size": 100},
            {"file_id": "large", "file_unique_id": "ul", "width": 800, "height": 400, "file_size": 4000}
        ]);
        update["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "logo.png", "mime_type": "image/png", "file_size": 2048
        });
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            b"\x89PNG-fake-bytes".to_vec(),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        // Каталог входящих файлов — рядом с тестовой БД; чистим перед
        // прогоном, т.к. cleanup удаляет только файлы старше суток.
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        // Диспатч ушёл: сообщение содержит подпись и заметки о двух вложениях.
        let row: String =
            sqlx::query_scalar("SELECT result_json FROM dispatches WHERE id = 'telegram_5'")
                .fetch_one(store.pool())
                .await?;
        let value: serde_json::Value = serde_json::from_str(&row)?;
        assert!(
            value["results"]["manager"]["run_id"].as_str().is_some(),
            "простой текст с вложением уходит менеджеру: {value}"
        );
        // Сам диспатч (тело запроса к Hermes) несёт подпись и пути к файлам.
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("Вот логотип"),
                "подпись фото передана: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains("📎 Вложение:"),
                "заметки о вложениях: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains("logo.png"),
                "имя документа: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains(inbound_dir.to_str().unwrap()),
                "путь для агента должен указывать на общую папку: {}",
                bodies[0]
            );
        }
        let files = std::fs::read_dir(&inbound_dir)?;
        let mut names = Vec::new();
        for entry in files.flatten() {
            names.push(entry.file_name().to_string_lossy().to_string());
        }
        assert_eq!(names.len(), 2, "фото и документ сохранены: {names:?}");
        assert!(
            names.iter().any(|n| n.ends_with("logo.png")),
            "имя документа сохранено: {names:?}"
        );
        assert!(
            names.iter().any(|n| n.contains("photo_800x400.jpg")),
            "самое крупное фото: {names:?}"
        );
        // Содержимое файла совпадает с тем, что отдал Bot API.
        let saved = std::fs::read(
            inbound_dir.join(names.iter().find(|n| n.ends_with("logo.png")).unwrap()),
        )?;
        assert_eq!(saved, b"\x89PNG-fake-bytes");
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn inbound_file_cleanup_removes_old_attachments() -> anyhow::Result<()> {
        let update = allowed_update();
        let updates = json!([update]);
        let (gateway, _store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        let stale = inbound_dir.join("stale.png");
        std::fs::write(&stale, b"old")?;
        // Ставим mtime на 25 часов назад (cleanup — старше суток).
        let times = std::fs::FileTimes::new()
            .set_modified(std::time::SystemTime::now() - Duration::from_secs(25 * 60 * 60));
        std::fs::File::open(&stale)?.set_times(times)?;
        // Обработанное сообщение запускает cleanup входящих файлов.
        gateway.poll_once().await?;
        assert!(!stale.exists(), "файл старше суток удалён");
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
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
