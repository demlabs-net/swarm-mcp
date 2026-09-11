use std::{
    collections::{HashMap, HashSet},
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

/// How long a download memo entry may survive without the update being
/// processed (busy-recipient retries usually resolve far sooner).
const DOWNLOAD_MEMO_TTL: Duration = Duration::from_secs(30 * 60);
/// Upper bound on memoized files (each holds only note strings).
const DOWNLOAD_MEMO_MAX_ENTRIES: usize = 128;
/// Media albums: wait up to this many short re-fetches for the remaining
/// parts of the group before dispatching it as one unit.
const TELEGRAM_ALBUM_WAIT_ROUNDS: usize = 3;
const TELEGRAM_ALBUM_WAIT_TIMEOUT_SECS: u64 = 1;
/// How long an already-dispatched media group is remembered so that a late
/// part arriving afterwards is explicitly marked instead of looking like a
/// brand-new request.
const DISPATCHED_ALBUM_TTL: Duration = Duration::from_secs(10 * 60);
const DISPATCHED_ALBUM_MAX: usize = 64;

type AttachmentTarget<'a> = (&'a str, Option<i64>, Option<String>, Option<String>);

/// Result of downloading one message's attachments.
struct AttachmentResult {
    /// Notes appended to the dispatch text (paths + optional inline text +
    /// failure summary + SLC file instructions).
    notes: Vec<String>,
    /// Attachments that could not be downloaded/saved on this attempt.
    failures: usize,
    /// Whether the message carried any file attachment at all.
    had_attachments: bool,
}

/// One successfully downloaded attachment, cached by `file_id` so that
/// busy-recipient retries (which reprocess the whole update) and repeated
/// media (album parts, edited captions) never download the same file twice.
/// Only the file metadata is memoized: the attachment note is stable, while
/// inline text is re-derived per message against the current length budget.
#[derive(Clone)]
struct CachedAttachment {
    file_id: String,
    path: std::path::PathBuf,
    mime: String,
    file_name: String,
    size_kb: i64,
}

impl CachedAttachment {
    fn note(&self) -> String {
        format!(
            "\n📎 Вложение: {} ({}), {} КБ — путь: {}",
            self.file_name,
            self.mime,
            self.size_kb,
            self.path.display()
        )
    }
}

#[derive(Debug, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
    /// Telegram pushes corrections of earlier messages as separate updates
    /// with fresh update ids; they are dispatched like a new message (the
    /// edited content is what the operator wants processed).
    #[serde(default)]
    edited_message: Option<TelegramMessage>,
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
    #[serde(default)]
    video: Option<TelegramVideo>,
    #[serde(default)]
    audio: Option<TelegramAudio>,
    #[serde(default)]
    voice: Option<TelegramVoice>,
    #[serde(default)]
    video_note: Option<TelegramVideoNote>,
    #[serde(default)]
    sticker: Option<TelegramSticker>,
    #[serde(default)]
    animation: Option<TelegramAnimation>,
    /// Same value on every message of a media album: the gateway waits for the
    /// whole group and dispatches it as one request instead of N.
    #[serde(default)]
    media_group_id: Option<String>,
    /// Topic id for messages inside a topic group (forums); replies to those
    /// messages stay in their topic automatically, plain notices need it.
    #[serde(default)]
    message_thread_id: Option<i64>,
    /// The message this one replies to (Telegram reply threading). Present
    /// when the operator replied to an earlier message; only its id and quote
    /// context are forwarded — the original body stays in SLC's own history.
    #[serde(default)]
    reply_to_message: Option<Box<TelegramMessage>>,
    /// The Telegram quote attached to this message (the operator quoted a
    /// fragment of an earlier message); the text is forwarded so the agent
    /// sees exactly what was quoted.
    #[serde(default)]
    quote: Option<TelegramQuote>,
}

/// Minimal shape of the `quote` field of an incoming Message
/// (text plus formatting the transport does not need).
#[derive(Debug, Deserialize)]
struct TelegramQuote {
    #[serde(default)]
    text: Option<String>,
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
struct TelegramVideo {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    width: Option<i64>,
    #[serde(default)]
    height: Option<i64>,
    #[serde(default)]
    _duration: Option<i64>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
    #[serde(default)]
    file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramAudio {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    _duration: Option<i64>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
    #[serde(default)]
    file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramVoice {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    _duration: Option<i64>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TelegramVideoNote {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    _length: Option<i64>,
    #[serde(default)]
    _duration: Option<i64>,
    #[serde(default)]
    file_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TelegramSticker {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    _width: Option<i64>,
    #[serde(default)]
    _height: Option<i64>,
    #[serde(default)]
    is_animated: Option<bool>,
    #[serde(default)]
    is_video: Option<bool>,
    #[serde(default)]
    file_size: Option<i64>,
    #[serde(default)]
    file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramAnimation {
    file_id: String,
    #[serde(rename = "file_unique_id")]
    _file_unique_id: String,
    #[serde(default)]
    width: Option<i64>,
    #[serde(default)]
    height: Option<i64>,
    #[serde(default)]
    _duration: Option<i64>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    file_size: Option<i64>,
    #[serde(default)]
    file_name: Option<String>,
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
    /// Successfully downloaded attachments keyed by `file_id` (see
    /// `CachedAttachment`), with the memo entry creation time. Entries live
    /// up to `DOWNLOAD_MEMO_TTL` and expire by count.
    downloads: Mutex<HashMap<String, (std::time::Instant, CachedAttachment)>>,
    /// Media groups already dispatched recently (late album parts are marked).
    dispatched_albums: Mutex<HashMap<String, std::time::Instant>>,
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
            downloads: Mutex::new(HashMap::new()),
            dispatched_albums: Mutex::new(HashMap::new()),
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
        let mut pending = self
            .fetch_updates(
                offset,
                self.state.config.telegram_poll_timeout.as_secs(),
                self.state.config.telegram_poll_limit,
            )
            .await?;
        pending.sort_by_key(|update| update.update_id);
        let mut index = 0;
        // Альбомы (media group) приходят несколькими update'ами; группа
        // диспатчится один раз после короткого окна ожидания её завершения.
        let mut debounced_groups: HashSet<String> = HashSet::new();
        while index < pending.len() {
            // A stale item can precede fresh items in the same sorted response.
            // Skip only that item; breaking here would starve every later update
            // and make the gateway fetch the same batch forever.
            if pending[index].update_id < offset {
                index += 1;
                continue;
            }
            ensure!(
                pending[index].update_id >= 0,
                "Telegram update ID is negative"
            );
            let group =
                update_message(&pending[index]).and_then(|message| message.media_group_id.clone());
            if let Some(group_id) = &group
                && debounced_groups.insert(group_id.clone())
            {
                for _ in 0..TELEGRAM_ALBUM_WAIT_ROUNDS {
                    let extra = self
                        .fetch_updates(
                            pending[index].update_id,
                            TELEGRAM_ALBUM_WAIT_TIMEOUT_SECS,
                            self.state.config.telegram_poll_limit,
                        )
                        .await?;
                    if extra.is_empty() {
                        break;
                    }
                    let known: HashSet<i64> =
                        pending.iter().map(|update| update.update_id).collect();
                    let fresh: Vec<TelegramUpdate> = extra
                        .into_iter()
                        .filter(|update| !known.contains(&update.update_id))
                        .collect();
                    let more_parts = fresh.iter().any(|update| {
                        update_message(update).and_then(|message| message.media_group_id.as_deref())
                            == Some(group_id.as_str())
                    });
                    pending.extend(fresh);
                    pending.sort_by_key(|update| update.update_id);
                    if !more_parts {
                        break;
                    }
                }
            }
            // Смежные части одной группы — один диспатч; часть группы, к
            // которой примешалось чужое сообщение, уйдёт отдельным диспатчем
            // (на практике Telegram присылает альбом подряд).
            let run_end = match &group {
                Some(group_id) => {
                    let mut end = index + 1;
                    while end < pending.len()
                        && update_message(&pending[end]).is_some_and(|message| {
                            message.media_group_id.as_deref() == Some(group_id.as_str())
                        })
                    {
                        end += 1;
                    }
                    end
                }
                None => index + 1,
            };
            let unit: Vec<&TelegramMessage> = pending[index..run_end]
                .iter()
                .filter_map(update_message)
                .collect();
            if unit.is_empty() {
                // update без сообщения (новые типы): потребляем его, не
                // диспатча и не зависая на нём.
                self.state
                    .store
                    .advance_telegram_update_offset(next_offset(pending[index].update_id)?)
                    .await?;
                index += 1;
                continue;
            }
            // Часть группы, уже диспатченной ранее (например, после окна
            // ожидания), помечается явно — SLC не примет её за новый запрос.
            let late_part = group
                .as_deref()
                .is_some_and(|group_id| self.is_late_album_part(group_id));
            self.process_dispatch_unit(pending[index].update_id, &unit, late_part)
                .await?;
            for update in &pending[index..run_end] {
                self.state
                    .store
                    .advance_telegram_update_offset(next_offset(update.update_id)?)
                    .await?;
            }
            // Помечаем группу только после успешного продвижения offset'ов:
            // busy-ретрай той же единицы маркером не испортится.
            if let Some(group_id) = &group {
                self.mark_album_dispatched(group_id);
            }
            index = run_end;
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
                "allowed_updates": ["message", "edited_message"],
            }))
            .send()
            .await
            .map_err(|_| anyhow!("Telegram getUpdates request failed"))?;
        let payload = telegram_payload(response).await?;
        let updates: Vec<TelegramUpdate> = serde_json::from_value(
            payload
                .get("result")
                .cloned()
                .context("Telegram getUpdates result is missing")?,
        )
        .context("decode Telegram updates")?;
        self.state
            .store
            .observe_telegram_inputs(&updates.iter().map(|u| u.update_id).collect::<Vec<_>>())
            .await?;
        Ok(updates)
    }

    /// Диспатчит одну единицу входящих сообщений: обычный update либо весь
    /// медиа-альбом сразу (части группы объединены в один запрос, вложения —
    /// в один диспатч). `update_id` — id первого update'а единицы (ключ
    /// идемпотентности); атрибуты берутся из первого сообщения — части
    /// альбома всегда от одного отправителя в одном чате.
    async fn process_dispatch_unit(
        &self,
        update_id: i64,
        messages: &[&TelegramMessage],
        late_album_part: bool,
    ) -> anyhow::Result<()> {
        if !self.state.store.claim_telegram_input(update_id).await? {
            // An old observed update is consumed even if a notice cannot be sent.
            return Ok(());
        }
        let Some(message) = messages.first() else {
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
        // Топик, из которого пришло сообщение (форумы): обычные уведомления
        // бота уходят в него же; ответы на сообщение и так остаются в топике.
        let thread_id = messages
            .iter()
            .find_map(|message| message.message_thread_id);
        if self.state.config.operator_hold {
            self.state.store.consume_telegram_input(update_id).await?;
            if let Err(error) = self.send_text(
                message.chat.id,
                "Operator HOLD is active. No agent was started and this message will not be replayed. Inspection remains available; only the operator can resume execution.",
                thread_id,
            )
            .await {
                warn!(error = %error, "held inbound notice failed; update will still be acknowledged");
            }
            return Ok(());
        }
        // Текст единицы: text/caption каждой части (у альбома подпись обычно
        // на одной из них), склеенные в порядке следования. Сообщения с
        // одними файлами (без текста) тоже обрабатываются.
        let mut unit_text = if late_album_part {
            "\n⚠️ Поздняя часть медиа-альбома — обработана отдельным запросом (остальные части диспатчились ранее).".to_string()
        } else {
            String::new()
        };
        unit_text.push_str(
            &messages
                .iter()
                .filter_map(|message| message.text.as_deref().or(message.caption.as_deref()))
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n"),
        );
        let mut text = unit_text.clone();
        // Вложения: скачиваем файлы через Bot API и сохраняем в общую с
        // агентами папку, путь передаём в сообщении. Ретраи занятого
        // получателя повторяют обработку, но скачанные файлы берутся из
        // memo (по file_id), а не качаются заново.
        let attachments = self
            .download_attachments(messages, unit_text.chars().count())
            .await;
        if text.is_empty() && attachments.notes.is_empty() {
            if attachments.had_attachments && attachments.failures > 0 {
                // Файл-только сообщение, вложения которого не удалось
                // получить: диспатчить нечего, но молча терять нельзя.
                let notice = "⚠️ Не удалось загрузить приложенные файлы. Проверьте размер/формат и отправьте ещё раз.";
                if let Err(error) = self.send_text(message.chat.id, notice, thread_id).await {
                    warn!(error = %error, "send Telegram attachment failure notice failed");
                }
            }
            // Сообщение без текста и без вложений — нечего диспатчить.
            return Ok(());
        }
        for note in attachments.notes {
            text.push_str(&note);
        }
        let reply_message_id = messages.iter().find_map(|message| {
            message
                .reply_to_message
                .as_ref()
                .map(|reply| reply.message_id)
        });
        let reply_quote = messages
            .iter()
            .find_map(|message| {
                message
                    .quote
                    .as_ref()
                    .and_then(|quote| quote.text.as_deref())
            })
            .map(str::trim)
            .filter(|quote| !quote.is_empty())
            .map(|quote| quote.chars().take(1024).collect::<String>());
        let command = parse_mention(&text, &self.state.config.telegram_inbound_targets)
            .or_else(|| parse_command(&text, &self.state.config.telegram_inbound_targets))
            .unwrap_or_else(|| ParsedCommand::Dispatch {
                targets: vec![self.state.config.manager_role.clone()],
                message: text.clone(),
            });
        match command {
            ParsedCommand::Reply(text) => {
                if let Err(error) = self.send_text(message.chat.id, &text, thread_id).await {
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
                        update_id,
                        message_id: message.message_id,
                        user_id: user.id,
                        username,
                        message: text,
                        targets,
                        chat_id: message.chat.id,
                        reply_message_id,
                        reply_quote,
                        thread_id,
                    })
                    .await;
                // Пользователь ждёт ответа агентом — сразу ставим «печатает».
                let _ = self.send_typing(message.chat.id, thread_id).await;
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
                        .insert(update_id);
                    if first_notice {
                        let notice = "⏳ Запрос принят и ждёт свободного слота менеджера. Отвечу автоматически; повторять сообщение не нужно.";
                        if let Err(error) = self.send_text(message.chat.id, notice, thread_id).await
                        {
                            warn!(error = %error, "send Telegram queued notice failed");
                        }
                    }
                    bail!("Telegram target is busy; update retained for retry");
                }
                let transport_queued = outcome
                    .value
                    .get("results")
                    .and_then(Value::as_object)
                    .is_some_and(|results| {
                        results.values().any(|result| {
                            result.get("queued").and_then(Value::as_bool) == Some(true)
                        })
                    });
                if transport_queued {
                    let first_notice = self
                        .queued_notices
                        .lock()
                        .expect("Telegram queue notice lock")
                        .insert(update_id);
                    if first_notice {
                        let notice = "⏳ Запрос сохранён и ждёт восстановления менеджера. Отвечу автоматически; повторять сообщение не нужно.";
                        if let Err(error) = self.send_text(message.chat.id, notice, thread_id).await
                        {
                            warn!(error = %error, "send Telegram durable queue notice failed");
                        }
                    }
                } else {
                    self.queued_notices
                        .lock()
                        .expect("Telegram queue notice lock")
                        .remove(&update_id);
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
                    let notice = format!("Swarm command {update_id} was not accepted: {reason}");
                    if let Err(error) = self.send_text(message.chat.id, &notice, thread_id).await {
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
    /// Уже скачанные на прежних попытках файлы (занятый получатель приводит к
    /// повторной обработке того же update) берутся из memo без повторного
    /// скачивания.
    async fn download_attachments(
        &self,
        messages: &[&TelegramMessage],
        base_text_len: usize,
    ) -> AttachmentResult {
        let mut targets: Vec<AttachmentTarget<'_>> = Vec::new();
        for message in messages {
            // Самое крупное фото каждого сообщения (по одному медиа на часть
            // альбома; у одиночного фото среди размеров берём максимальный).
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
            if let Some(video) = &message.video {
                let name = video.file_name.clone().or_else(|| {
                    let w = video.width.unwrap_or(0);
                    let h = video.height.unwrap_or(0);
                    Some(format!("video_{w}x{h}.mp4"))
                });
                targets.push((
                    &video.file_id,
                    video.file_size,
                    video
                        .mime_type
                        .clone()
                        .or_else(|| Some("video/mp4".to_string())),
                    name,
                ));
            }
            if let Some(audio) = &message.audio {
                let name = audio
                    .file_name
                    .clone()
                    .or_else(|| Some("audio.mp3".to_string()));
                targets.push((
                    &audio.file_id,
                    audio.file_size,
                    audio
                        .mime_type
                        .clone()
                        .or_else(|| Some("audio/mpeg".to_string())),
                    name,
                ));
            }
            if let Some(voice) = &message.voice {
                targets.push((
                    &voice.file_id,
                    voice.file_size,
                    voice
                        .mime_type
                        .clone()
                        .or_else(|| Some("audio/ogg".to_string())),
                    Some("voice.ogg".to_string()),
                ));
            }
            if let Some(video_note) = &message.video_note {
                targets.push((
                    &video_note.file_id,
                    video_note.file_size,
                    Some("video/mp4".to_string()),
                    Some("video_note.mp4".to_string()),
                ));
            }
            if let Some(sticker) = &message.sticker {
                let is_animated = sticker.is_animated.unwrap_or(false);
                let is_video = sticker.is_video.unwrap_or(false);
                let (mime, ext) = if is_animated {
                    ("application/json", "tgs")
                } else if is_video {
                    ("video/webm", "webm")
                } else {
                    ("image/webp", "webp")
                };
                targets.push((
                    &sticker.file_id,
                    sticker.file_size,
                    Some(mime.to_string()),
                    sticker
                        .file_name
                        .clone()
                        .or_else(|| Some(format!("sticker.{ext}"))),
                ));
            }
            if let Some(animation) = &message.animation {
                let name = animation.file_name.clone().or_else(|| {
                    let w = animation.width.unwrap_or(0);
                    let h = animation.height.unwrap_or(0);
                    Some(format!("animation_{w}x{h}.gif"))
                });
                targets.push((
                    &animation.file_id,
                    animation.file_size,
                    animation
                        .mime_type
                        .clone()
                        .or_else(|| Some("image/gif".to_string())),
                    name,
                ));
            }
        }
        let had_attachments = !targets.is_empty();
        let dir = self.state.config.shared_files_dir.clone();
        if let Err(error) = tokio::fs::create_dir_all(&dir).await {
            warn!(error = %error, path = %dir.display(), "inbound files dir create failed");
            return AttachmentResult {
                notes: Vec::new(),
                failures: targets.len(),
                had_attachments,
            };
        }
        // Memo: успешно скачанные на прошлых попытках вложения (по file_id).
        let memo_notes: Vec<CachedAttachment> = {
            let mut cache = self.downloads.lock().expect("Telegram download memo lock");
            let now = std::time::Instant::now();
            cache.retain(|_, (inserted, _)| now.duration_since(*inserted) < DOWNLOAD_MEMO_TTL);
            if cache.len() > DOWNLOAD_MEMO_MAX_ENTRIES {
                let mut by_age: Vec<(String, std::time::Instant)> = cache
                    .iter()
                    .map(|(id, &(inserted, _))| (id.clone(), inserted))
                    .collect();
                by_age.sort_by_key(|(_, inserted)| *inserted);
                for (id, _) in by_age.iter().take(cache.len() - DOWNLOAD_MEMO_MAX_ENTRIES) {
                    cache.remove(id);
                }
            }
            cache.values().map(|(_, cached)| cached.clone()).collect()
        };

        let mut notes = Vec::new();
        let mut failures = 0;
        // Сколько символов уже занято в тексте диспатча: инлайн содержимого
        // не должен выводить сообщение за SWARM_MAX_MESSAGE_CHARS.
        let instructions = &self.state.config.file_inbound_instructions;
        let reserved = if instructions.is_empty() {
            512
        } else {
            instructions.chars().count().saturating_add(512)
        };
        let mut text_len = base_text_len.saturating_add(reserved);
        let total_attachments = targets.len();
        for (file_id, size, mime, name) in targets {
            let memoized: Option<CachedAttachment> = memo_notes
                .iter()
                .find(|cached| cached.file_id == file_id)
                .cloned();
            let (mime_str, file_name, attachment_note, fresh_bytes) = if let Some(cached) =
                memoized.as_ref()
            {
                (
                    cached.mime.clone(),
                    cached.file_name.clone(),
                    cached.note(),
                    None,
                )
            } else {
                let max_file_bytes =
                    i64::try_from(self.state.config.inbound_file_max_bytes).unwrap_or(i64::MAX);
                if let Some(declared) = size
                    && declared > max_file_bytes
                {
                    failures += 1;
                    warn!(
                        file_id = %file_id,
                        declared_bytes = declared,
                        "inbound file exceeds the configured download limit"
                    );
                    continue;
                }
                let bytes = match self.download_file(file_id).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        failures += 1;
                        warn!(file_id = %file_id, error = %error, "downloading inbound file failed");
                        continue;
                    }
                };
                let path = match save_inbound_file(&dir, name.as_deref(), &bytes).await {
                    Ok(path) => path,
                    Err(error) => {
                        failures += 1;
                        warn!(file_id = %file_id, error = %error, "saving inbound file failed");
                        continue;
                    }
                };
                let downloaded_size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
                let size_kb = size.unwrap_or(downloaded_size) / 1024;
                let mime_str = mime
                    .as_deref()
                    .unwrap_or("application/octet-stream")
                    .to_string();
                let file_name = path
                    .file_name()
                    .map_or_else(|| "file".into(), |n| n.to_string_lossy().to_string());
                let attachment_note = format!(
                    "\n📎 Вложение: {} ({}), {} КБ — путь: {}",
                    file_name,
                    mime_str,
                    size_kb,
                    path.display()
                );
                self.memoize_download(CachedAttachment {
                    file_id: file_id.to_string(),
                    path: path.clone(),
                    mime: mime_str.clone(),
                    file_name: file_name.clone(),
                    size_kb,
                });
                (mime_str, file_name, attachment_note, Some(bytes))
            };
            text_len = text_len.saturating_add(attachment_note.chars().count());
            notes.push(attachment_note);
            // Инлайним содержимое малых текстовых файлов. Решение всегда
            // принимается по текущему бюджету длины сообщения: закэшированный
            // файл перечитывается (ограниченно), свежий использует байты из
            // скачивания — кэш не может протащить инлайн в контекст, где он
            // не умещается.
            let inline_limit = self
                .state
                .config
                .file_inline_max_bytes
                .min(self.state.config.max_message_chars.saturating_sub(text_len));
            if inline_limit >= 256 && is_text_mime(&mime_str, &file_name) {
                let content = match fresh_bytes {
                    Some(bytes) if !bytes.is_empty() && bytes.len() <= inline_limit => {
                        std::str::from_utf8(&bytes).ok().map(str::to_owned)
                    }
                    Some(_) => None,
                    None => {
                        // Закэшированный файл: memoized обязателен (иначе мы
                        // скачали бы файл в этой же итерации).
                        let Some(cached) = memoized.as_ref() else {
                            continue;
                        };
                        if let Some(bytes) = read_inline_bytes(&cached.path, inline_limit).await {
                            std::str::from_utf8(&bytes).ok().map(str::to_owned)
                        } else {
                            // Не молчим о пропавшем файле: TTL-очистка могла
                            // убрать его между скачиванием и инлайном.
                            warn!(
                                file_id = %file_id,
                                path = %cached.path.display(),
                                "cached inbound file is unreadable; inline content skipped"
                            );
                            None
                        }
                    }
                };
                if let Some(content) = content
                    && !content.is_empty()
                {
                    let inline_note = format!("\n📄 Содержимое {file_name}:\n```\n{content}\n```");
                    text_len = text_len.saturating_add(inline_note.chars().count());
                    notes.push(inline_note);
                }
            }
        }
        // Инструкция агенту — только когда хоть один файл реально получен.
        if !instructions.is_empty() && !notes.is_empty() {
            notes.push(format!("\n\n{instructions}"));
        }
        if failures > 0 && (!notes.is_empty() || base_text_len > 0) {
            notes.push(format!(
                "\n⚠️ {failures} из {total_attachments} вложений не удалось загрузить (сеть, размер или формат)."
            ));
        }
        // Чистим файлы старше настроенного TTL.
        cleanup_inbound_files(&dir, self.state.config.inbound_file_ttl).await;
        AttachmentResult {
            notes,
            failures,
            had_attachments,
        }
    }

    fn memoize_download(&self, cached: CachedAttachment) {
        self.downloads
            .lock()
            .expect("Telegram download memo lock")
            .entry(cached.file_id.clone())
            .and_modify(|(_, existing)| *existing = cached.clone())
            .or_insert_with(|| (std::time::Instant::now(), cached));
    }

    /// Помечает группу как диспатченную (после успешного продвижения
    /// offset'ов) и проверяет, не является ли текущая единица поздней частью
    /// уже обработанного альбома.
    fn mark_album_dispatched(&self, group_id: &str) {
        let mut albums = self.dispatched_albums.lock().expect("album memo lock");
        let now = std::time::Instant::now();
        albums.retain(|_, dispatched| now.duration_since(*dispatched) < DISPATCHED_ALBUM_TTL);
        if albums.len() >= DISPATCHED_ALBUM_MAX {
            let mut by_age: Vec<(String, std::time::Instant)> = albums
                .iter()
                .map(|(id, &dispatched)| (id.clone(), dispatched))
                .collect();
            by_age.sort_by_key(|(_, dispatched)| *dispatched);
            for (id, _) in by_age.iter().take(albums.len() - DISPATCHED_ALBUM_MAX + 1) {
                albums.remove(id);
            }
        }
        albums.insert(group_id.to_string(), now);
    }

    fn is_late_album_part(&self, group_id: &str) -> bool {
        let albums = self.dispatched_albums.lock().expect("album memo lock");
        albums.contains_key(group_id)
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
        let max_bytes = self.state.config.inbound_file_max_bytes;
        let response = self
            .client
            .get(&file_url)
            .timeout(Duration::from_secs(120))
            .send()
            .await?
            .error_for_status()?;
        // Никогда не читаем тело без лимита: объём файла ограничен сверху
        // конфигом (по умолчанию 20 MiB — лимит скачивания Bot API).
        if let Some(length) = response.content_length()
            && length > u64::try_from(max_bytes).unwrap_or(u64::MAX)
        {
            return Err(anyhow!(
                "inbound file exceeds the configured download limit"
            ));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| anyhow!("Telegram file download failed"))?;
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .context("Telegram file is too large")?;
            if next_len > max_bytes {
                return Err(anyhow!(
                    "inbound file exceeds the configured download limit"
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// «Печатает…» в чате (и в топике, если сообщение пришло из него):
    /// показываем сразу при приёме запроса и при каждом тике воркера, пока
    /// агент ещё работает (Telegram-пузырь живёт ~5 с).
    async fn send_typing(&self, chat_id: i64, thread_id: Option<i64>) -> anyhow::Result<()> {
        let mut payload = json!({
            "chat_id": chat_id,
            "action": "typing",
        });
        if let Some(thread_id) = thread_id {
            payload["message_thread_id"] = json!(thread_id);
        }
        let response = self
            .client
            .post(format!("{}/sendChatAction", self.bot_url))
            .json(&payload)
            .send()
            .await
            .map_err(|_| anyhow!("Telegram sendChatAction request failed"))?;
        telegram_payload(response).await?;
        Ok(())
    }

    async fn send_text(
        &self,
        chat_id: i64,
        text: &str,
        thread_id: Option<i64>,
    ) -> anyhow::Result<()> {
        let mut payload = json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": true,
        });
        if let Some(thread_id) = thread_id {
            payload["message_thread_id"] = json!(thread_id);
        }
        let response = self
            .client
            .post(format!("{}/sendMessage", self.bot_url))
            .json(&payload)
            .send()
            .await
            .map_err(|_| anyhow!("Telegram sendMessage request failed"))?;
        telegram_payload(response).await?;
        Ok(())
    }
}

/// Returns `true` if the MIME type or filename extension indicates a text file
/// suitable for inlining into the dispatch message.
fn is_text_mime(mime: &str, filename: &str) -> bool {
    if mime.starts_with("text/") || mime == "application/json" || mime == "application/xml" {
        return true;
    }
    let ext = filename.rsplit('.').next().unwrap_or("");
    matches!(
        ext,
        "md" | "txt"
            | "csv"
            | "json"
            | "yaml"
            | "yml"
            | "toml"
            | "xml"
            | "rs"
            | "py"
            | "js"
            | "ts"
            | "html"
            | "css"
            | "sh"
            | "env"
            | "cfg"
            | "ini"
            | "conf"
    )
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

/// Перечитывает содержимое закэшированного файла для инлайна. Инлайн
/// допускается только если файл целиком умещается в лимит: обрезанный
/// контент большого файла никогда не подставляется в сообщение.
async fn read_inline_bytes(path: &std::path::Path, limit: usize) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let metadata = tokio::fs::metadata(path).await.ok()?;
    if metadata.len() > u64::try_from(limit).unwrap_or(u64::MAX) {
        return None;
    }
    let file = tokio::fs::File::open(path).await.ok()?;
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    file.take(u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .await
        .ok()?;
    Some(bytes)
}

/// The single user message carried by an update: a regular message or the
/// corrected copy of an edited one.
fn update_message(update: &TelegramUpdate) -> Option<&TelegramMessage> {
    update.message.as_ref().or(update.edited_message.as_ref())
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
    async fn busy_target_durably_queues_update_and_sends_one_notice() -> anyhow::Result<()> {
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let (gateway, store, path) = gateway_with_mocks_and_hermes(
            TelegramBacklogMode::Process,
            canned_bot(json!([allowed_update()]), sent.clone()),
            Arc::new(|_, _| (429, json!({"error": "busy"}))),
        )
        .await?;

        gateway.poll_once().await?;
        assert_eq!(
            store.telegram_update_offset().await?,
            Some(6),
            "the update advances only after its wake is durably queued"
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id='telegram_5'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "accepted", "the durable wake is accepted");
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE dispatch_id='telegram_5' AND status='pending'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(pending, 1, "the busy role wake must survive in the FIFO");
        let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            outbox, 1,
            "the accepted group command has one normal audit record"
        );
        assert_eq!(sent.lock().expect("sent").len(), 1);
        assert!(
            sent.lock().expect("sent")[0].contains("ждёт восстановления менеджера"),
            "operator receives a queue notice"
        );

        gateway.poll_once().await?;
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
    async fn held_inbound_is_acknowledged_even_if_notice_fails() -> anyhow::Result<()> {
        let update = allowed_update();
        let (mut gateway, store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            Arc::new(move |path, _| {
                if path.ends_with("getUpdates") {
                    (200, json!({"ok": true, "result": [update.clone()]}), None)
                } else {
                    (500, json!({"ok": false}), None)
                }
            }),
        )
        .await?;
        let mut config = (*gateway.state.config).clone();
        config.operator_hold = true;
        let config = Arc::new(config);
        gateway.state = Arc::new(AppState {
            dispatcher: crate::dispatch::Dispatcher::new(config.clone(), store.clone())?,
            config,
            store: store.clone(),
        });
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dispatches")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(count, 0);
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

    #[tokio::test]
    async fn video_attachment_is_downloaded_and_noted() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = Value::Null;
        update["message"]["caption"] = json!("Видео с презентации");
        update["message"]["video"] = json!({
            "file_id": "vid1",
            "file_unique_id": "uv1",
            "width": 1920,
            "height": 1080,
            "duration": 120,
            "mime_type": "video/mp4",
            "file_size": 5_000_000,
            "file_name": "presentation.mp4"
        });
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            b"\x00\x00\x00\x1cftypmp42".to_vec(),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("Видео с презентации"),
                "caption передан: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains("📎 Вложение:"),
                "заметка о видео: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains("presentation.mp4"),
                "имя видеофайла: {}",
                bodies[0]
            );
        }
        let files = std::fs::read_dir(&inbound_dir)?;
        let names: Vec<String> = files
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(names.len(), 1, "видео сохранено: {names:?}");
        assert!(
            names[0].ends_with("presentation.mp4"),
            "имя видеофайла: {names:?}"
        );
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn file_inbound_instructions_appended_when_files_present() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = Value::Null;
        update["message"]["caption"] = json!("Документ");
        update["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "spec.md", "mime_type": "text/markdown", "file_size": 100
        });
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            b"# Spec content".to_vec(),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        // Set file_inbound_instructions.
        {
            let mut config_mut = gateway.state.config.as_ref().clone();
            config_mut.file_inbound_instructions =
                "SLC: slc_add_document, category: documentation".to_string();
            // We need to rebuild the state with updated config. Instead, just check
            // that the default fixture instructions appear in the body.
        }
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("📎 Вложение:"),
                "заметка о файле: {}",
                bodies[0]
            );
            assert!(bodies[0].contains("spec.md"), "имя файла: {}", bodies[0]);
            // The fixture config has file_inbound_instructions set.
            assert!(
                bodies[0].contains("slc_add_document"),
                "инструкция SLC в диспатче: {}",
                bodies[0]
            );
        }
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn small_text_file_is_inlined_in_dispatch() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = Value::Null;
        update["message"]["caption"] = json!("ТЗ");
        update["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "tz.md", "mime_type": "text/markdown", "file_size": 50
        });
        let file_content = b"# \xd0\xa2\xd0\x97\n\n\xd0\xa0\xd0\xb5\xd0\xb4\xd0\xb8\xd0\xb7\xd0\xb0\xd0\xb9\xd0\xbd \xd1\x81\xd0\xb0\xd0\xb9\xd1\x82\xd0\xb0";
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let content_for_mock = file_content.to_vec();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            content_for_mock,
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("📄 Содержимое"),
                "содержимое файла инлайнится: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains("tz.md"),
                "имя файла в инлайне: {}",
                bodies[0]
            );
        }
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[test]
    fn is_text_mime_matches_text_and_code_extensions() {
        assert!(is_text_mime("text/plain", "readme.txt"));
        assert!(is_text_mime("text/markdown", "doc.md"));
        assert!(is_text_mime("application/json", "data.json"));
        assert!(is_text_mime("application/xml", "config.xml"));
        assert!(is_text_mime("text/x-python", "script.py"));
        assert!(is_text_mime("text/x-rust", "main.rs"));
        assert!(is_text_mime("text/html", "page.html"));
        assert!(is_text_mime("text/css", "style.css"));
        assert!(is_text_mime("application/octet-stream", "config.yaml"));
        assert!(is_text_mime("application/octet-stream", "setup.toml"));
        assert!(is_text_mime("application/octet-stream", ".env"));
        assert!(!is_text_mime("image/png", "photo.png"));
        assert!(!is_text_mime("video/mp4", "video.mp4"));
        assert!(!is_text_mime("audio/mpeg", "song.mp3"));
        assert!(!is_text_mime("application/pdf", "doc.pdf"));
    }

    /// Gateway fixture с настраиваемым Config (шаблон/лимиты) и файловым
    /// моком Bot API — для сценариев, где нужны кастомные значения.
    async fn gateway_custom(
        config: &mut crate::config::Config,
        file_bytes: Vec<u8>,
        bot: MockBot,
        hermes_behavior: testutil::MockHermes,
    ) -> anyhow::Result<(TelegramGateway, crate::store::Store, std::path::PathBuf)> {
        let telegram_base = spawn_mock_telegram_with_files(bot, file_bytes).await;
        let hermes_base = testutil::spawn_mock_hermes(hermes_behavior).await;
        config.telegram_enabled = true;
        config.telegram_bot_mode = TelegramBotMode::Shared;
        config.telegram_bot_token = Some(Secret::new("test-bot-token".to_string()));
        config.telegram_group_id = Some("-100123".to_string());
        config.telegram_inbound_enabled = true;
        config.telegram_inbound_targets = vec!["manager".to_string(), "developer".to_string()];
        config.telegram_allowed_users = BTreeSet::from([42]);
        config.telegram_backlog_mode = TelegramBacklogMode::Process;
        config.telegram_api_base_url = telegram_base.parse()?;
        for agent in config.agents.values_mut() {
            agent.api_url = Some(hermes_base.parse()?);
        }
        let path = config.state_db_path.clone();
        let config = Arc::new(config.clone());
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
    async fn reply_and_quote_context_reaches_the_dispatch() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = json!("поправь вот это");
        update["message"]["reply_to_message"] = json!({
            "message_id": 90,
            "from": {"id": 42, "is_bot": false, "username": "alice"},
            "chat": {"id": -100_123},
            "text": "старый текст"
        });
        update["message"]["quote"] = json!({
            "text": "цитируемый фрагмент",
            "position": 3,
            "is_manual": true
        });
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let mut config = testutil::fixture_config(&testutil::temp_db_path("tg-reply"));
        config.telegram_inbound_template =
            "id={message_id} reply={reply_message_id} quote=[{reply_quote}] msg={message}"
                .to_string();
        let (gateway, _store, path) = gateway_custom(
            &mut config,
            Vec::new(),
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        gateway.poll_once().await?;
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("reply=90"),
                "reply_message_id передан в шаблон: {}",
                bodies[0]
            );
            assert!(
                bodies[0].contains("quote=[цитируемый фрагмент]"),
                "текст цитаты передан в шаблон: {}",
                bodies[0]
            );
        }
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn busy_target_durably_queues_already_downloaded_attachments() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = Value::Null;
        update["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "note.txt", "mime_type": "text/plain", "file_size": 2048
        });
        let updates = json!([update]);
        let hermes_calls = Arc::new(Mutex::new(Vec::<u16>::new()));
        let calls = hermes_calls.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            b"hello from the txt file".to_vec(),
            Arc::new(move |_, _| {
                let mut calls = calls.lock().expect("calls");
                calls.push(1);
                if calls.len() == 1 {
                    // Получатель занят: wake и уже скачанное вложение должны
                    // сохраниться в durable delivery FIFO без повторного poll.
                    (429, json!({"error": "busy"}))
                } else {
                    (202, json!({"run_id": "run-1"}))
                }
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        gateway.poll_once().await?;
        assert_eq!(
            hermes_calls.lock().expect("calls").len(),
            1,
            "durably queued update is not dispatched again by Telegram polling"
        );
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE dispatch_id='telegram_5' AND status='pending'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(
            pending, 1,
            "attachment wake is retained in the durable FIFO"
        );
        let files = std::fs::read_dir(&inbound_dir)?;
        let names: Vec<_> = files
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            names.len(),
            1,
            "вложение скачано один раз и сохранено для queued wake: {names:?}"
        );
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_file_only_message_notifies_without_dispatch() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = Value::Null;
        update["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "big.bin", "mime_type": "application/octet-stream",
            "file_size": 21 * 1024 * 1024
        });
        let updates = json!([update]);
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let sent_clone = sent.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |path, body| {
                if path.ends_with("getUpdates") {
                    (200, json!({"ok": true, "result": updates.clone()}), None)
                } else {
                    sent_clone.lock().expect("sent").push(body.to_string());
                    (200, json!({"ok": true}), None)
                }
            }),
            b"x".to_vec(),
            Arc::new(|_, _| (202, json!({"run_id": "run-1"}))),
        )
        .await?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(6));
        let dispatches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dispatches")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(dispatches, 0, "диспатчить нечего");
        {
            let sent = sent.lock().expect("sent");
            assert_eq!(sent.len(), 1);
            assert!(
                sent[0].contains("Не удалось загрузить приложенные файлы"),
                "пользователь уведомлён о потере файла: {}",
                sent[0]
            );
        }
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn inline_text_respects_the_dispatch_message_length() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = json!("/developer ".to_owned() + &"x".repeat(2500));
        update["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "notes.txt", "mime_type": "text/plain", "file_size": 2048
        });
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let mut config = testutil::fixture_config(&testutil::temp_db_path("tg-inline"));
        config.max_message_chars = 3200;
        let (gateway, _store, path) = gateway_custom(
            &mut config,
            "y".repeat(2048).into_bytes(),
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("📎 Вложение:"),
                "файл сохранён с путём: {}",
                bodies[0]
            );
            assert!(
                !bodies[0].contains("📄 Содержимое"),
                "инлайн не должен превысить лимит длины сообщения: {}",
                bodies[0]
            );
        }
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    fn album_part(update_id: i64, message_id: i64, file_id: &str, name: &str) -> Value {
        json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "from": {"id": 42, "is_bot": false, "username": "alice"},
                "chat": {"id": -100_123},
                "media_group_id": "album-1",
                "document": {
                    "file_id": file_id,
                    "file_unique_id": format!("u{file_id}"),
                    "file_name": name,
                    "mime_type": "image/png",
                    "file_size": 512
                }
            }
        })
    }

    #[tokio::test]
    async fn edited_message_is_dispatched_with_the_new_text() -> anyhow::Result<()> {
        let update = json!({
            "update_id": 9,
            "edited_message": {
                "message_id": 3,
                "from": {"id": 42, "is_bot": false, "username": "alice"},
                "chat": {"id": -100_123},
                "text": "/developer исправленный запрос"
            }
        });
        let updates = json!([update]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(10));
        let dispatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dispatches WHERE id='telegram_9'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(dispatch, 1, "правка сообщения диспатчится");
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1);
            assert!(
                bodies[0].contains("исправленный запрос"),
                "новый текст доходит до агента: {}",
                bodies[0]
            );
        }
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn album_parts_arriving_together_are_dispatched_once() -> anyhow::Result<()> {
        let updates = json!([
            album_part(5, 201, "f1", "a.png"),
            album_part(6, 202, "f2", "b.png"),
            album_part(7, 203, "f3", "c.png"),
        ]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            b"\x89PNG-fake".to_vec(),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(8));
        let dispatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dispatches WHERE id='telegram_5'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(dispatch, 1, "альбом — один диспатч");
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 1, "все части в одном запросе");
            for name in ["a.png", "b.png", "c.png"] {
                assert!(bodies[0].contains(name), "часть {name} в диспатче");
            }
        }
        let files = std::fs::read_dir(&inbound_dir)?;
        let count = files.flatten().count();
        assert_eq!(count, 3, "все три файла альбома сохранены");
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn album_split_across_poll_responses_is_still_dispatched_once() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| {
                let call = calls_clone.fetch_add(1, Ordering::SeqCst);
                let result = match call {
                    // Первый ответ: только первая часть альбома; остальные
                    // приходят во время окна ожидания.
                    0 => json!([album_part(5, 201, "f1", "a.png")]),
                    1 => json!([
                        album_part(6, 202, "f2", "b.png"),
                        album_part(7, 203, "f3", "c.png")
                    ]),
                    _ => json!([]),
                };
                (200, json!({"ok": true, "result": result}), None)
            }),
            b"\x89PNG-fake".to_vec(),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(8));
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(
                bodies.len(),
                1,
                "части, пришедшие в разных ответах, объединены"
            );
            for name in ["a.png", "b.png", "c.png"] {
                assert!(bodies[0].contains(name), "часть {name} в диспатче");
            }
        }
        assert!(
            calls.load(Ordering::SeqCst) >= 3,
            "окно ожидания допрашивает Bot API"
        );
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn topic_messages_are_answered_in_the_same_thread() -> anyhow::Result<()> {
        let mut update = allowed_update();
        update["message"]["text"] = json!("/help");
        update["message"]["message_thread_id"] = json!(77);
        let updates = json!([update]);
        let sent = Arc::new(Mutex::new(Vec::<String>::new()));
        let sent_clone = sent.clone();
        let (gateway, _store, path) = gateway_with_mocks(
            TelegramBacklogMode::Process,
            Arc::new(move |api_path, body| {
                if api_path.ends_with("getUpdates") {
                    (200, json!({"ok": true, "result": updates.clone()}), None)
                } else {
                    sent_clone.lock().expect("sent").push(body.to_string());
                    (200, json!({"ok": true}), None)
                }
            }),
        )
        .await?;
        gateway.poll_once().await?;
        {
            let sent = sent.lock().expect("sent");
            assert_eq!(sent.len(), 1);
            assert!(
                sent[0].contains("message_thread_id") && sent[0].contains("77"),
                "помощь уходит в тот же топик: {}",
                sent[0]
            );
        }
        testutil::remove_db_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn memoized_file_does_not_inline_into_an_over_budget_message() -> anyhow::Result<()> {
        // Тот же file_id во втором сообщении с длинным текстом: файл не
        // качается повторно, а инлайн не протаскивается из кэша — решение
        // принимается по бюджету текущего сообщения.
        let mut first = allowed_update();
        first["update_id"] = json!(5);
        first["message"]["message_id"] = json!(100);
        first["message"]["text"] = Value::Null;
        first["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "notes.txt", "mime_type": "text/plain", "file_size": 2048
        });
        let mut second = allowed_update();
        second["update_id"] = json!(6);
        second["message"]["message_id"] = json!(101);
        second["message"]["text"] = json!("/developer ".to_owned() + &"x".repeat(2400));
        second["message"]["document"] = first["message"]["document"].clone();
        let updates = json!([first, second]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let mut config = testutil::fixture_config(&testutil::temp_db_path("tg-memo-inline"));
        config.max_message_chars = 3200;
        let (gateway, store, path) = gateway_custom(
            &mut config,
            "y".repeat(2048).into_bytes(),
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(7));
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 2, "оба сообщения диспатчатся");
            assert!(
                bodies[0].contains("📄 Содержимое"),
                "в коротком сообщении файл инлайнится: {}",
                bodies[0]
            );
            assert!(
                !bodies[1].contains("📄 Содержимое"),
                "кэш не должен превысить лимит длины второго сообщения: {}",
                bodies[1]
            );
        }
        let files = std::fs::read_dir(&inbound_dir)?;
        assert_eq!(files.flatten().count(), 1, "файл скачан один раз");
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn late_album_part_is_marked_not_resent_as_fresh() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, store, path) = gateway_with_mocks_and_files_hermes(
            TelegramBacklogMode::Process,
            Arc::new(move |_, _| {
                let call = calls_clone.fetch_add(1, Ordering::SeqCst);
                let result = match call {
                    // Первый поллинг: часть 1 одна (дебаунс пуст) — альбом
                    // диспатчится как есть и группа помечается.
                    0 => json!([album_part(5, 201, "f1", "a.png")]),
                    // Второй поллинг: запоздавшая часть 2 той же группы.
                    2 => json!([album_part(6, 202, "f2", "b.png")]),
                    _ => json!([]),
                };
                (200, json!({"ok": true, "result": result}), None)
            }),
            b"\x89PNG-fake".to_vec(),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        gateway.poll_once().await?;
        assert_eq!(store.telegram_update_offset().await?, Some(7));
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 2);
            assert!(
                bodies[1].contains("Поздняя часть медиа-альбома"),
                "запоздавшая часть помечена явно, а не выдана за новый запрос: {}",
                bodies[1]
            );
            assert!(
                !bodies[0].contains("Поздняя часть медиа-альбома"),
                "первая часть маркер не получает: {}",
                bodies[0]
            );
        }
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn cached_big_file_is_never_inlined_partially() -> anyhow::Result<()> {
        // Первое сообщение: большой текстовый файл (больше inline-лимита) —
        // инлайн не делается, но файл сохраняется и попадает в memo. Второе
        // сообщение с тем же file_id не должно инлайнить обрезанный контент.
        let mut first = allowed_update();
        first["update_id"] = json!(5);
        first["message"]["message_id"] = json!(100);
        first["message"]["text"] = Value::Null;
        first["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "big.txt", "mime_type": "text/plain", "file_size": 60000
        });
        let mut second = allowed_update();
        second["update_id"] = json!(6);
        second["message"]["message_id"] = json!(101);
        second["message"]["text"] = Value::Null;
        second["message"]["document"] = first["message"]["document"].clone();
        let updates = json!([first, second]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let big_content = "x".repeat(60_000);
        let (gateway, _store, path) = gateway_custom(
            &mut testutil::fixture_config(&testutil::temp_db_path("tg-big-inline")),
            big_content.into_bytes(),
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 2);
            for (index, body) in bodies.iter().enumerate() {
                assert!(
                    !body.contains("📄 Содержимое"),
                    "большой файл не инлайнится (даже обрезком) в сообщении {index}: {}",
                    body.chars().take(300).collect::<String>()
                );
            }
        }
        let files = std::fs::read_dir(&inbound_dir)?;
        assert_eq!(files.flatten().count(), 1, "файл скачан один раз");
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }

    #[tokio::test]
    async fn cached_small_file_is_inlined_again_from_disk() -> anyhow::Result<()> {
        let mut first = allowed_update();
        first["update_id"] = json!(5);
        first["message"]["message_id"] = json!(100);
        first["message"]["text"] = Value::Null;
        first["message"]["document"] = json!({
            "file_id": "doc1", "file_unique_id": "ud",
            "file_name": "notes.txt", "mime_type": "text/plain", "file_size": 2048
        });
        let mut second = allowed_update();
        second["update_id"] = json!(6);
        second["message"]["message_id"] = json!(101);
        second["message"]["text"] = Value::Null;
        second["message"]["document"] = first["message"]["document"].clone();
        let updates = json!([first, second]);
        let hermes_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let bodies = hermes_bodies.clone();
        let (gateway, _store, path) = gateway_custom(
            &mut testutil::fixture_config(&testutil::temp_db_path("tg-small-inline")),
            "контент из кэша".repeat(50).into_bytes(),
            Arc::new(move |_, _| (200, json!({"ok": true, "result": updates.clone()}), None)),
            Arc::new(move |_, body| {
                bodies.lock().expect("bodies").push(body.to_string());
                (202, json!({"run_id": "run-1"}))
            }),
        )
        .await?;
        let inbound_dir = gateway.state.config.shared_files_dir.clone();
        let _ = std::fs::remove_dir_all(&inbound_dir);
        std::fs::create_dir_all(&inbound_dir)?;
        gateway.poll_once().await?;
        {
            let bodies = hermes_bodies.lock().expect("bodies");
            assert_eq!(bodies.len(), 2);
            assert!(
                bodies[1].contains("контент из кэша"),
                "инлайн для повторного файла перечитывается с диска: {}",
                bodies[1].chars().take(300).collect::<String>()
            );
        }
        testutil::remove_db_files(&path).await;
        let _ = std::fs::remove_dir_all(&inbound_dir);
        Ok(())
    }
}
