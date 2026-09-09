use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, anyhow, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use tokio::sync::Mutex;

use crate::config::Config;

const SCHEMA_VERSION: i64 = 8;
const TELEGRAM_OFFSET_KEY: &str = "telegram_update_offset";
const TELEGRAM_POLL_SUCCESS_KEY: &str = "telegram_poll_success";

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    reservation_lock: Arc<Mutex<()>>,
}

#[derive(Debug)]
pub enum Reservation {
    Reserved,
    Existing(Value),
    Pending { dispatch_id: String },
    Conflict,
    RateLimited { retry_after_seconds: i64 },
}

/// Optional file attachment delivered with a Telegram reply.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MediaPayload {
    pub filename: String,
    pub mime_type: Option<String>,
    /// Base64-encoded file content.
    pub content_b64: String,
}

#[derive(Clone, Debug)]
pub struct AuditMessage {
    pub id: String,
    pub sender: String,
    pub event: String,
    pub recipients: String,
    pub text: String,
    /// Telegram chat to deliver to; None means the configured group.
    pub chat_id: Option<i64>,
    /// Original Telegram message this delivery replies to (visual threading).
    pub reply_to_message_id: Option<i64>,
    /// Topic inside a forum group to deliver into (used when there is no
    /// reply link — a reply anchors its topic by itself).
    pub thread_id: Option<i64>,
    /// Optional file attachments (each sent as sendPhoto/sendDocument).
    pub media: Vec<MediaPayload>,
}

#[derive(Clone, Debug)]
pub struct OutboxItem {
    pub id: String,
    pub sender: String,
    pub event: String,
    pub recipients: String,
    pub text: String,
    pub attempts: i64,
    pub next_chunk: i64,
    /// Telegram chat to deliver to; None means the configured group.
    pub chat_id: Option<i64>,
    /// Original Telegram message this delivery replies to (visual threading).
    pub reply_to_message_id: Option<i64>,
    /// Topic inside a forum group to deliver into (used when there is no
    /// reply link — a reply anchors its topic by itself).
    pub thread_id: Option<i64>,
    /// Optional file attachments (each sent as sendPhoto/sendDocument).
    pub media: Vec<MediaPayload>,
}

/// One opaque role delivery waiting for the destination Hermes profile to
/// release its single-writer run slot. This is transport state, not task state;
/// task authority and lifecycle remain in SLC.
#[derive(Clone, Debug)]
pub struct DeliveryQueueItem {
    pub id: String,
    pub dispatch_id: String,
    pub sender: String,
    pub kind: String,
    pub recipient: String,
    pub body: String,
    pub instructions: String,
    pub attempts: i64,
}

pub struct ActivityRecord<'a> {
    pub sender: &'a str,
    pub targets: &'a [String],
    pub event: &'a str,
    pub turn_id: &'a str,
    pub detail: &'a str,
    pub occurred_at: Option<&'a str>,
    pub clock_skew: Duration,
}

impl Store {
    pub async fn connect(config: &Config) -> anyhow::Result<Self> {
        Self::connect_path(
            &config.state_db_path,
            config.db_max_connections,
            config.db_busy_timeout,
        )
        .await
    }

    async fn connect_path(
        path: &Path,
        max_connections: u32,
        busy_timeout: Duration,
    ) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create database directory {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(busy_timeout);
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await?;
        let store = Self {
            pool,
            reservation_lock: Arc::new(Mutex::new(())),
        };
        store.migrate().await?;
        store.recover_pending_dispatches().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        let current_version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut *tx)
            .await?;
        ensure!(
            current_version <= SCHEMA_VERSION,
            "database schema version {current_version} is newer than supported version {SCHEMA_VERSION}"
        );
        Self::migrate_python_activity_tables(&mut tx).await?;
        for statement in SCHEMA {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        if !Self::has_column(&mut tx, "telegram_outbox", "next_chunk").await? {
            sqlx::query(
                "ALTER TABLE telegram_outbox ADD COLUMN next_chunk INTEGER NOT NULL DEFAULT 0",
            )
            .execute(&mut *tx)
            .await?;
        }
        if !Self::has_column(&mut tx, "telegram_outbox", "chat_id").await? {
            sqlx::query("ALTER TABLE telegram_outbox ADD COLUMN chat_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "telegram_outbox", "media_json").await? {
            sqlx::query("ALTER TABLE telegram_outbox ADD COLUMN media_json TEXT")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "telegram_outbox", "reply_to_message_id").await? {
            sqlx::query("ALTER TABLE telegram_outbox ADD COLUMN reply_to_message_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "telegram_outbox", "thread_id").await? {
            sqlx::query("ALTER TABLE telegram_outbox ADD COLUMN thread_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "dispatches", "telegram_chat_id").await? {
            sqlx::query("ALTER TABLE dispatches ADD COLUMN telegram_chat_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "dispatches", "telegram_message_id").await? {
            sqlx::query("ALTER TABLE dispatches ADD COLUMN telegram_message_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "dispatches", "telegram_thread_id").await? {
            sqlx::query("ALTER TABLE dispatches ADD COLUMN telegram_thread_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "run_replies", "message_id").await? {
            sqlx::query("ALTER TABLE run_replies ADD COLUMN message_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        if !Self::has_column(&mut tx, "run_replies", "thread_id").await? {
            sqlx::query("ALTER TABLE run_replies ADD COLUMN thread_id INTEGER")
                .execute(&mut *tx)
                .await?;
        }
        Self::import_python_activity_tables(&mut tx).await?;
        sqlx::query(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn has_column(
        tx: &mut Transaction<'_, Sqlite>,
        table: &str,
        column: &str,
    ) -> anyhow::Result<bool> {
        let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(&mut **tx)
            .await?;
        Ok(rows.iter().any(|row| {
            row.try_get::<String, _>("name")
                .is_ok_and(|name| name == column)
        }))
    }

    async fn migrate_python_activity_tables(
        tx: &mut Transaction<'_, Sqlite>,
    ) -> anyhow::Result<()> {
        for table in ["activity_events", "activity_state"] {
            let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
                .fetch_all(&mut **tx)
                .await?;
            let is_python_schema = columns.iter().any(|row| {
                row.try_get::<String, _>("name")
                    .is_ok_and(|name| name == "recorded_at")
            });
            if is_python_schema {
                let legacy = format!("{table}_python_legacy");
                let legacy_exists: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                )
                .bind(&legacy)
                .fetch_one(&mut **tx)
                .await?;
                if legacy_exists == 0 {
                    sqlx::query(&format!("ALTER TABLE {table} RENAME TO {legacy}"))
                        .execute(&mut **tx)
                        .await?;
                } else {
                    return Err(anyhow!(
                        "both {table} and {legacy} use the Python schema; manual recovery required"
                    ));
                }
            }
        }
        Ok(())
    }

    async fn import_python_activity_tables(tx: &mut Transaction<'_, Sqlite>) -> anyhow::Result<()> {
        let events_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='activity_events_python_legacy'",
        )
        .fetch_one(&mut **tx)
        .await?;
        if events_exists > 0 {
            sqlx::query(
                r#"INSERT OR IGNORE INTO activity_events
                   (sender, target, event, event_rank, turn_id, detail, occurred_ms, received_ms)
                   SELECT sender, target, event,
                          CASE event WHEN 'started' THEN 1 ELSE 2 END,
                          turn_id, detail,
                          COALESCE(CAST(strftime('%s', recorded_at) AS INTEGER) * 1000, unixepoch() * 1000),
                          COALESCE(CAST(strftime('%s', recorded_at) AS INTEGER) * 1000, unixepoch() * 1000)
                   FROM activity_events_python_legacy"#,
            )
            .execute(&mut **tx)
            .await?;
        }
        let state_exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='activity_state_python_legacy'",
        )
        .fetch_one(&mut **tx)
        .await?;
        if state_exists > 0 {
            sqlx::query(
                r#"INSERT OR IGNORE INTO activity_state
                   (sender, target, event, event_rank, turn_id, detail, occurred_ms, received_ms)
                   SELECT sender, target, event,
                          CASE event WHEN 'started' THEN 1 ELSE 2 END,
                          turn_id, detail,
                          COALESCE(CAST(strftime('%s', recorded_at) AS INTEGER) * 1000, unixepoch() * 1000),
                          COALESCE(CAST(strftime('%s', recorded_at) AS INTEGER) * 1000, unixepoch() * 1000)
                   FROM activity_state_python_legacy"#,
            )
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }

    pub async fn ready(&self) -> anyhow::Result<()> {
        let mut connection = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await?;
        let check = sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&mut *connection)
            .await;
        let rollback = sqlx::query("ROLLBACK").execute(&mut *connection).await;
        check.context("readiness query failed")?;
        rollback.context("readiness rollback failed")?;
        Ok(())
    }

    async fn recover_pending_dispatches(&self) -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        let pending = sqlx::query(
            "SELECT id, sender, kind FROM dispatches WHERE status='pending' ORDER BY created_ms",
        )
        .fetch_all(&mut *tx)
        .await?;
        for dispatch in pending {
            let dispatch_id: String = dispatch.get("id");
            let kind: String = dispatch.get("kind");
            let target_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM dispatch_targets WHERE dispatch_id=?")
                    .bind(&dispatch_id)
                    .fetch_one(&mut *tx)
                    .await?;
            let deliveries = sqlx::query(
                r#"SELECT q.id, q.recipient, q.status, q.attempts, q.sequence,
                          CASE WHEN q.status='pending' THEN (
                            SELECT COUNT(*) FROM delivery_outbox earlier
                            WHERE earlier.recipient=q.recipient
                              AND earlier.status='pending'
                              AND earlier.sequence <= q.sequence
                          ) END AS queue_position
                   FROM delivery_outbox q WHERE q.dispatch_id=? ORDER BY q.sequence"#,
            )
            .bind(&dispatch_id)
            .fetch_all(&mut *tx)
            .await?;
            let (status, result) = if deliveries.is_empty() {
                (
                    "indeterminate",
                    json!({
                        "ok": false,
                        "dispatch_id": dispatch_id,
                        "error": "server restarted before dispatch completion was persisted",
                        "recovery_required": true,
                    }),
                )
            } else {
                let recovery_required = target_count
                    != i64::try_from(deliveries.len()).unwrap_or(i64::MAX)
                    || deliveries.iter().any(|delivery| {
                        !matches!(
                            delivery.get::<String, _>("status").as_str(),
                            "pending" | "delivered"
                        )
                    });
                let queued_after_restart = deliveries
                    .iter()
                    .any(|delivery| delivery.get::<String, _>("status") == "pending");
                let fully_delivered = !recovery_required
                    && deliveries
                        .iter()
                        .all(|delivery| delivery.get::<String, _>("status") == "delivered");
                let single_delivery = (deliveries.len() == 1).then(|| {
                    let delivery = &deliveries[0];
                    (
                        delivery.get::<String, _>("id"),
                        delivery.get::<String, _>("recipient"),
                        delivery.get::<Option<i64>, _>("queue_position"),
                    )
                });
                let queued = deliveries
                    .into_iter()
                    .map(|delivery| {
                        json!({
                            "queue_id": delivery.get::<String, _>("id"),
                            "recipient": delivery.get::<String, _>("recipient"),
                            "status": delivery.get::<String, _>("status"),
                            "attempts": delivery.get::<i64, _>("attempts"),
                            "sequence": delivery.get::<i64, _>("sequence"),
                            "queue_position": delivery.get::<Option<i64>, _>("queue_position"),
                        })
                    })
                    .collect::<Vec<_>>();
                let mut recovered = json!({
                    "ok": !recovery_required,
                    "sender": dispatch.get::<String, _>("sender"),
                    "kind": kind,
                    "queued": queued_after_restart,
                    "fully_delivered": fully_delivered,
                    "recovered_after_restart": true,
                    "recovery_required": recovery_required,
                    "deliveries": queued,
                });
                let id_field = if matches!(kind.as_str(), "msg_to" | "msg_all") {
                    "message_id"
                } else {
                    "dispatch_id"
                };
                recovered[id_field] = json!(dispatch_id);
                if let Some((queue_id, recipient, queue_position)) = single_delivery {
                    recovered["recipient"] = json!(recipient);
                    recovered["queue_id"] = json!(queue_id);
                    if let Some(queue_position) = queue_position {
                        recovered["queue_position"] = json!(queue_position);
                    }
                }
                ("accepted", recovered)
            };
            sqlx::query(
                "UPDATE dispatches SET status=?, result_json=?, updated_ms=? WHERE id=? AND status='pending'",
            )
            .bind(status)
            .bind(serde_json::to_string(&result)?)
            .bind(now)
            .bind(&dispatch_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_activity(
        &self,
        record: ActivityRecord<'_>,
    ) -> anyhow::Result<BTreeMapResult> {
        let received_ms = Utc::now().timestamp_millis();
        let skew_ms = i64::try_from(record.clock_skew.as_millis()).unwrap_or(i64::MAX);
        let occurred_ms = record
            .occurred_at
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map_or(received_ms, |value| value.timestamp_millis())
            .clamp(
                received_ms.saturating_sub(skew_ms),
                received_ms.saturating_add(skew_ms),
            );
        let event_rank = if record.event == "started" {
            1_i64
        } else {
            2_i64
        };
        let mut tx = self.pool.begin().await?;
        let mut results = BTreeMapResult::default();
        for target in record.targets {
            let inserted = sqlx::query(
                r#"INSERT OR IGNORE INTO activity_events
                   (sender, target, event, event_rank, turn_id, detail, occurred_ms, received_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(record.sender)
            .bind(target)
            .bind(record.event)
            .bind(event_rank)
            .bind(record.turn_id)
            .bind(record.detail)
            .bind(occurred_ms)
            .bind(received_ms)
            .execute(&mut *tx)
            .await?
            .rows_affected()
                > 0;
            if inserted {
                sqlx::query(
                    r#"INSERT INTO activity_state
                   (sender, target, event, event_rank, turn_id, detail, occurred_ms, received_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(sender, target) DO UPDATE SET
                     event=excluded.event,
                     event_rank=excluded.event_rank,
                     turn_id=excluded.turn_id,
                     detail=excluded.detail,
                     occurred_ms=excluded.occurred_ms,
                     received_ms=excluded.received_ms
                   WHERE excluded.occurred_ms > activity_state.occurred_ms
                      OR (excluded.occurred_ms = activity_state.occurred_ms
                          AND excluded.event_rank >= activity_state.event_rank)"#,
                )
                .bind(record.sender)
                .bind(target)
                .bind(record.event)
                .bind(event_rank)
                .bind(record.turn_id)
                .bind(record.detail)
                .bind(occurred_ms)
                .bind(received_ms)
                .execute(&mut *tx)
                .await?;
            }
            results
                .0
                .insert(target.clone(), json!({"recorded": inserted}));
        }
        tx.commit().await?;
        Ok(results)
    }

    pub async fn activity_snapshot(
        &self,
        role: &str,
        allowed_senders: &[String],
        enabled: bool,
        history_limit: i64,
        stale_after: Duration,
    ) -> anyhow::Result<Value> {
        let current_rows = if allowed_senders.is_empty() {
            Vec::new()
        } else {
            let placeholders = std::iter::repeat_n("?", allowed_senders.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT sender, event, turn_id, detail, occurred_ms, received_ms
                 FROM activity_state WHERE target = ? AND sender IN ({placeholders})
                 ORDER BY occurred_ms DESC"
            );
            let mut query = sqlx::query(&sql).bind(role);
            for sender in allowed_senders {
                query = query.bind(sender);
            }
            query.fetch_all(&self.pool).await?
        };
        let recent_rows = if allowed_senders.is_empty() {
            Vec::new()
        } else {
            let placeholders = std::iter::repeat_n("?", allowed_senders.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "SELECT sender, event, turn_id, detail, occurred_ms, received_ms
                 FROM activity_events WHERE target = ? AND sender IN ({placeholders})
                 ORDER BY id DESC LIMIT ?"
            );
            let mut query = sqlx::query(&sql).bind(role);
            for sender in allowed_senders {
                query = query.bind(sender);
            }
            query.bind(history_limit).fetch_all(&self.pool).await?
        };
        let now = Utc::now().timestamp_millis();
        let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
        // Both queries filter senders in SQL; the rows are already caller-scoped.
        let current = current_rows
            .iter()
            .map(|row| activity_json(row, now, stale_ms))
            .collect::<Vec<_>>();
        let recent = recent_rows
            .iter()
            .map(|row| activity_json(row, now, stale_ms))
            .collect::<Vec<_>>();
        let active = current
            .iter()
            .filter(|event| event["event"] == "started" && event["stale"] == false)
            .cloned()
            .collect::<Vec<_>>();
        Ok(json!({
            "caller": role,
            "enabled": enabled,
            "mode": "record-only",
            "active": active,
            "current": current,
            "recent": recent,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_dispatch(
        &self,
        id: &str,
        sender: &str,
        kind: &str,
        targets: &[String],
        idempotency_key: Option<&str>,
        fingerprint: &str,
        rate_limit: i64,
        rate_window: Duration,
    ) -> anyhow::Result<Reservation> {
        self.reserve_dispatch_guarded(
            id,
            sender,
            kind,
            targets,
            idempotency_key,
            fingerprint,
            rate_limit,
            rate_window,
            Duration::ZERO,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_dispatch_guarded(
        &self,
        id: &str,
        sender: &str,
        kind: &str,
        targets: &[String],
        idempotency_key: Option<&str>,
        fingerprint: &str,
        rate_limit: i64,
        rate_window: Duration,
        duplicate_window: Duration,
    ) -> anyhow::Result<Reservation> {
        let _guard = self.reservation_lock.lock().await;
        let mut tx = self.pool.begin().await?;
        if let Some(key) = idempotency_key {
            // A 'failed' dispatch definitively produced no downstream side effects
            // (the agent API rejected the run), so the key is released: a retry with
            // the same key re-executes instead of replaying the old failure. All other
            // terminal states (accepted/partial/indeterminate) replay, because the
            // downstream may have accepted the operation.
            sqlx::query(
                r#"DELETE FROM dispatches
                   WHERE sender = ? AND kind = ? AND idempotency_key = ? AND status = 'failed'"#,
            )
            .bind(sender)
            .bind(kind)
            .bind(key)
            .execute(&mut *tx)
            .await?;
            if let Some(row) = sqlx::query(
                r#"SELECT id, fingerprint, result_json FROM dispatches
                   WHERE sender = ? AND kind = ? AND idempotency_key = ?"#,
            )
            .bind(sender)
            .bind(kind)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
            {
                if row.get::<String, _>("fingerprint") != fingerprint {
                    return Ok(Reservation::Conflict);
                }
                let existing_id = row.get::<String, _>("id");
                let result = row.try_get::<Option<String>, _>("result_json")?;
                return Ok(match result {
                    Some(value) => Reservation::Existing(serde_json::from_str(&value)?),
                    None => Reservation::Pending {
                        dispatch_id: existing_id,
                    },
                });
            }
        }

        let now = Utc::now().timestamp_millis();
        if !duplicate_window.is_zero() {
            let duplicate_window_ms =
                i64::try_from(duplicate_window.as_millis()).unwrap_or(i64::MAX);
            let duplicate_cutoff = now.saturating_sub(duplicate_window_ms);
            if let Some(row) = sqlx::query(
                r#"SELECT id, result_json FROM dispatches
                   WHERE sender = ? AND kind = ? AND fingerprint = ?
                     AND status != 'failed' AND created_ms >= ?
                   ORDER BY created_ms DESC LIMIT 1"#,
            )
            .bind(sender)
            .bind(kind)
            .bind(fingerprint)
            .bind(duplicate_cutoff)
            .fetch_optional(&mut *tx)
            .await?
            {
                let existing_id = row.get::<String, _>("id");
                let result = row.try_get::<Option<String>, _>("result_json")?;
                return Ok(match result {
                    Some(value) => Reservation::Existing(serde_json::from_str(&value)?),
                    None => Reservation::Pending {
                        dispatch_id: existing_id,
                    },
                });
            }
        }
        let window_ms = i64::try_from(rate_window.as_millis()).unwrap_or(i64::MAX);
        let cutoff = now.saturating_sub(window_ms);
        sqlx::query("DELETE FROM rate_events WHERE created_ms < ?")
            .bind(cutoff)
            .execute(&mut *tx)
            .await?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rate_events WHERE actor = ?")
            .bind(sender)
            .fetch_one(&mut *tx)
            .await?;
        if count >= rate_limit {
            let oldest: Option<i64> =
                sqlx::query_scalar("SELECT MIN(created_ms) FROM rate_events WHERE actor = ?")
                    .bind(sender)
                    .fetch_one(&mut *tx)
                    .await?;
            let retry_ms = oldest.map_or(window_ms, |value| {
                value.saturating_add(window_ms).saturating_sub(now)
            });
            return Ok(Reservation::RateLimited {
                retry_after_seconds: (retry_ms / 1000).max(1),
            });
        }
        sqlx::query("INSERT INTO rate_events(actor, created_ms) VALUES (?, ?)")
            .bind(sender)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            r#"INSERT INTO dispatches
               (id, sender, kind, idempotency_key, fingerprint, status, created_ms, updated_ms)
               VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)"#,
        )
        .bind(id)
        .bind(sender)
        .bind(kind)
        .bind(idempotency_key)
        .bind(fingerprint)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        for target in targets {
            sqlx::query("INSERT INTO dispatch_targets(dispatch_id, target) VALUES (?, ?)")
                .bind(id)
                .bind(target)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(Reservation::Reserved)
    }

    pub async fn finish_dispatch(
        &self,
        id: &str,
        status: &str,
        result: &Value,
        audit: Option<AuditMessage>,
    ) -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE dispatches SET status = ?, result_json = ?, updated_ms = ? WHERE id = ? AND status = 'pending'",
        )
        .bind(status)
        .bind(serde_json::to_string(result)?)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated != 1 {
            return Err(anyhow!(
                "dispatch is not pending or does not exist (already finalized?)"
            ));
        }
        if let Some(audit) = audit {
            insert_outbox(&mut tx, &audit, now).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn set_dispatch_telegram_source(
        &self,
        id: &str,
        chat_id: i64,
        message_id: i64,
        thread_id: Option<i64>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE dispatches SET telegram_chat_id = ?, telegram_message_id = ?, telegram_thread_id = ? WHERE id = ?",
        )
            .bind(chat_id)
            .bind(message_id)
            .bind(thread_id)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Exact reply target for one Telegram-originated dispatch. Unlike the
    /// role-level lookup, this never falls back to another command and is safe
    /// when a durable delivery is accepted after an outage.
    pub async fn telegram_reply_target_for_dispatch(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<(i64, Option<i64>, Option<i64>)>> {
        let row = sqlx::query(
            r#"SELECT telegram_chat_id, telegram_message_id, telegram_thread_id
               FROM dispatches
               WHERE id = ? AND kind = 'telegram_inbound'
                 AND telegram_chat_id IS NOT NULL"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| {
            (
                row.get("telegram_chat_id"),
                row.get("telegram_message_id"),
                row.get("telegram_thread_id"),
            )
        }))
    }

    /// Source Telegram chat of the most recent Telegram inbound dispatch that
    /// targeted `role`. Strictly role-scoped: never silently borrow another
    /// role's or another operator's chat — callers decide their explicit
    /// fallback (the shared group) when this returns `None`.
    pub async fn telegram_chat_for_role(&self, role: &str) -> anyhow::Result<Option<i64>> {
        let chat_id: Option<i64> = sqlx::query_scalar(
            r#"SELECT d.telegram_chat_id
               FROM dispatches d
               JOIN dispatch_targets t ON t.dispatch_id = d.id
               WHERE t.target = ? AND d.kind = 'telegram_inbound'
                 AND d.telegram_chat_id IS NOT NULL
               ORDER BY d.created_ms DESC LIMIT 1"#,
        )
        .bind(role)
        .fetch_optional(&self.pool)
        .await?;
        Ok(chat_id)
    }

    /// Chat + topic of the most recent Telegram inbound dispatch targeting
    /// `role` (same strict scope as `telegram_chat_for_role`).
    pub async fn telegram_chat_and_thread_for_role(
        &self,
        role: &str,
    ) -> anyhow::Result<Option<(i64, Option<i64>)>> {
        let row = sqlx::query(
            r#"SELECT d.telegram_chat_id, d.telegram_thread_id
               FROM dispatches d
               JOIN dispatch_targets t ON t.dispatch_id = d.id
               WHERE t.target = ? AND d.kind = 'telegram_inbound'
                 AND d.telegram_chat_id IS NOT NULL
               ORDER BY d.created_ms DESC LIMIT 1"#,
        )
        .bind(role)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| (row.get("telegram_chat_id"), row.get("telegram_thread_id"))))
    }

    /// Chat of the operator who interacted with the swarm most recently — the
    /// documented heuristic for routing *run answers of dispatches* (the SLC
    /// manager role itself usually has no inbound Telegram chat of its own).
    /// Deliberate and logged by the caller; never used by the proactive
    /// `telegram_reply` tool (strictly role-scoped).
    pub async fn telegram_latest_operator_chat(&self) -> anyhow::Result<Option<i64>> {
        let chat_id: Option<i64> = sqlx::query_scalar(
            r#"SELECT telegram_chat_id
               FROM dispatches
               WHERE kind = 'telegram_inbound' AND telegram_chat_id IS NOT NULL
               ORDER BY created_ms DESC LIMIT 1"#,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(chat_id)
    }

    pub async fn enqueue_outbox(&self, audit: AuditMessage) -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        insert_outbox(&mut tx, &audit, now).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn enqueue_delivery(
        &self,
        item: &DeliveryQueueItem,
        retry_after: Duration,
        error: &str,
    ) -> anyhow::Result<i64> {
        let now = Utc::now().timestamp_millis();
        let retry_ms = i64::try_from(retry_after.as_millis()).unwrap_or(i64::MAX);
        let mut tx = self.pool.begin().await?;
        let dispatch_status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id=?")
                .bind(&item.dispatch_id)
                .fetch_one(&mut *tx)
                .await?;
        ensure!(
            dispatch_status == "pending",
            "delivery may be queued only while its operation is pending"
        );
        sqlx::query(
            r#"INSERT OR IGNORE INTO delivery_outbox
               (id, dispatch_id, sender, kind, recipient, body, instructions,
                status, attempts, next_attempt_ms, last_error, created_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)"#,
        )
        .bind(&item.id)
        .bind(&item.dispatch_id)
        .bind(&item.sender)
        .bind(&item.kind)
        .bind(&item.recipient)
        .bind(&item.body)
        .bind(&item.instructions)
        .bind(now.saturating_add(retry_ms.max(1_000)))
        .bind(error)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let persisted = sqlx::query(
            r#"SELECT sequence, dispatch_id, sender, kind, recipient, body,
                      instructions, status
               FROM delivery_outbox WHERE id=?"#,
        )
        .bind(&item.id)
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            persisted.get::<String, _>("dispatch_id") == item.dispatch_id
                && persisted.get::<String, _>("sender") == item.sender
                && persisted.get::<String, _>("kind") == item.kind
                && persisted.get::<String, _>("recipient") == item.recipient
                && persisted.get::<String, _>("body") == item.body
                && persisted.get::<String, _>("instructions") == item.instructions,
            "delivery queue id already exists with a different payload"
        );
        ensure!(
            persisted.get::<String, _>("status") == "pending",
            "delivery queue id already reached a terminal state"
        );
        let sequence: i64 = persisted.get("sequence");
        let position: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM delivery_outbox
               WHERE recipient = ? AND status = 'pending'
                 AND sequence <= ?"#,
        )
        .bind(&item.recipient)
        .bind(sequence)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        ensure!(position >= 1, "queued delivery has no FIFO position");
        Ok(position)
    }

    pub async fn has_pending_delivery(&self, recipient: &str) -> anyhow::Result<bool> {
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE recipient = ? AND status = 'pending'",
        )
        .bind(recipient)
        .fetch_one(&self.pool)
        .await?;
        Ok(pending > 0)
    }

    /// Cancel one undelivered transport item without touching any task state.
    /// The original sender owns its queue item; the configured manager may
    /// perform incident recovery across roles.  Delivery and cancellation are
    /// serialized by the dispatcher's messaging gate before this transaction.
    pub async fn cancel_delivery(
        &self,
        id: &str,
        actor: &str,
        manager_role: &str,
        reason: &str,
    ) -> anyhow::Result<Value> {
        let now = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"SELECT dispatch_id, sender, recipient, status
               FROM delivery_outbox WHERE id=?"#,
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("delivery queue item not found"))?;
        let dispatch_id: String = row.get("dispatch_id");
        let sender: String = row.get("sender");
        let recipient: String = row.get("recipient");
        let previous_status: String = row.get("status");
        ensure!(
            actor == sender || actor == manager_role,
            "only the original sender or swarm manager can cancel this delivery"
        );

        let already_cancelled = previous_status == "cancelled";
        if !already_cancelled {
            ensure!(
                matches!(previous_status.as_str(), "pending" | "dead"),
                "delivery is {previous_status} and can no longer be cancelled"
            );
            let cancellation = format!(
                "cancelled by {actor} at {}: {reason}",
                millis_to_rfc3339(now)
            );
            let updated = sqlx::query(
                r#"UPDATE delivery_outbox
                   SET status='cancelled', last_error=?
                   WHERE id=? AND status IN ('pending','dead')"#,
            )
            .bind(cancellation)
            .bind(id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            ensure!(
                updated == 1,
                "delivery changed while cancellation was applied"
            );
        }

        let next = sqlx::query(
            r#"SELECT id FROM delivery_outbox
               WHERE recipient=? AND status='pending'
               ORDER BY sequence LIMIT 1"#,
        )
        .bind(&recipient)
        .fetch_optional(&mut *tx)
        .await?;
        let next_queue_id = next.as_ref().map(|row| row.get::<String, _>("id"));
        if let Some(next_queue_id) = next_queue_id.as_deref() {
            // Removing a FIFO head is an explicit scheduling change. Make the
            // next item immediately eligible; a still-busy Hermes role will
            // return 429 and be deferred through the normal bounded backoff.
            sqlx::query(
                "UPDATE delivery_outbox SET next_attempt_ms=? WHERE id=? AND next_attempt_ms>?",
            )
            .bind(now)
            .bind(next_queue_id)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        let remaining_pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE recipient=? AND status='pending'",
        )
        .bind(&recipient)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(json!({
            "ok": true,
            "queue_id": id,
            "dispatch_id": dispatch_id,
            "sender": sender,
            "recipient": recipient,
            "previous_status": previous_status,
            "status": "cancelled",
            "already_cancelled": already_cancelled,
            "cancelled_by": actor,
            "cancelled_at": millis_to_rfc3339(now),
            "next_queue_id": next_queue_id,
            "remaining_pending": remaining_pending,
        }))
    }

    /// At most one due FIFO head is returned for each recipient. A role-level
    /// Hermes `max_concurrent_runs=1` remains the final single-writer guard.
    pub async fn due_deliveries(&self, limit: i64) -> anyhow::Result<Vec<DeliveryQueueItem>> {
        let rows = sqlx::query(
            r#"SELECT q.id, q.dispatch_id, q.sender, q.kind, q.recipient,
                      q.body, q.instructions, q.attempts
               FROM delivery_outbox q
               WHERE q.status = 'pending' AND q.next_attempt_ms <= ?
                 AND NOT EXISTS (
                   SELECT 1 FROM delivery_outbox earlier
                   WHERE earlier.recipient = q.recipient
                     AND earlier.status = 'pending'
                     AND earlier.sequence < q.sequence
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM role_messaging r
                   WHERE r.enabled = 0
                     AND (r.role = q.sender OR r.role = q.recipient)
                 )
               ORDER BY q.sequence LIMIT ?"#,
        )
        .bind(Utc::now().timestamp_millis())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| DeliveryQueueItem {
                id: row.get("id"),
                dispatch_id: row.get("dispatch_id"),
                sender: row.get("sender"),
                kind: row.get("kind"),
                recipient: row.get("recipient"),
                body: row.get("body"),
                instructions: row.get("instructions"),
                attempts: row.get("attempts"),
            })
            .collect())
    }

    pub async fn delivery_eligible(&self, id: &str) -> anyhow::Result<bool> {
        let now = Utc::now().timestamp_millis();
        let eligible: i64 = sqlx::query_scalar(
            r#"SELECT COUNT(*) FROM delivery_outbox q
               WHERE q.id = ? AND q.status = 'pending' AND q.next_attempt_ms <= ?
                 AND NOT EXISTS (
                   SELECT 1 FROM delivery_outbox earlier
                   WHERE earlier.recipient = q.recipient
                     AND earlier.status = 'pending'
                     AND earlier.sequence < q.sequence
                 )
                 AND NOT EXISTS (
                   SELECT 1 FROM role_messaging r
                   WHERE r.enabled = 0
                     AND (r.role = q.sender OR r.role = q.recipient)
                 )"#,
        )
        .bind(id)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok(eligible == 1)
    }

    pub async fn mark_delivery_delivered(
        &self,
        id: &str,
        run_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let updated = sqlx::query(
            r#"UPDATE delivery_outbox
               SET status='delivered', delivered_ms=?, run_id=?, last_error=NULL
               WHERE id=? AND status='pending'"#,
        )
        .bind(Utc::now().timestamp_millis())
        .bind(run_id)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(updated == 1, "delivery queue item is not pending");
        Ok(())
    }

    pub async fn defer_delivery(
        &self,
        id: &str,
        retry_after: Duration,
        error: &str,
    ) -> anyhow::Result<()> {
        let delay_ms = i64::try_from(retry_after.as_millis()).unwrap_or(i64::MAX);
        let updated = sqlx::query(
            r#"UPDATE delivery_outbox
               SET attempts=attempts+1, next_attempt_ms=?, last_error=?
               WHERE id=? AND status='pending'"#,
        )
        .bind(
            Utc::now()
                .timestamp_millis()
                .saturating_add(delay_ms.max(1_000)),
        )
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(updated == 1, "delivery queue item is not pending");
        Ok(())
    }

    pub async fn mark_delivery_dead(&self, id: &str, error: &str) -> anyhow::Result<()> {
        let updated = sqlx::query(
            r#"UPDATE delivery_outbox
               SET status='dead', attempts=attempts+1, last_error=?
               WHERE id=? AND status='pending'"#,
        )
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(updated == 1, "delivery queue item is not pending");
        Ok(())
    }

    /// Durable tracking of Telegram-dispatched Hermes runs awaiting their
    /// final answer (survives restarts; see the run-reply worker). The source
    /// message id lets the final answer be delivered as a Telegram reply to
    /// the operator's original message.
    pub async fn track_run_reply(
        &self,
        run_id: &str,
        role: &str,
        chat_id: i64,
        message_id: Option<i64>,
        thread_id: Option<i64>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO run_replies (run_id, role, chat_id, message_id, thread_id, created_ms) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(run_id)
        .bind(role)
        .bind(chat_id)
        .bind(message_id)
        .bind(thread_id)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// All tracked runs: `(run_id, role, chat_id, reply_to_message_id,
    /// thread_id, created_ms)`.
    pub async fn due_run_replies(
        &self,
    ) -> anyhow::Result<Vec<(String, String, i64, Option<i64>, Option<i64>, i64)>> {
        let rows = sqlx::query(
            "SELECT run_id, role, chat_id, message_id, thread_id, created_ms FROM run_replies",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get("run_id"),
                    row.get("role"),
                    row.get("chat_id"),
                    row.get("message_id"),
                    row.get("thread_id"),
                    row.get("created_ms"),
                )
            })
            .collect())
    }

    pub async fn untrack_run_reply(&self, run_id: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM run_replies WHERE run_id = ?")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn mark_dispatch_indeterminate(&self, id: &str, reason: &str) -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let result = json!({
            "ok": false,
            "error": reason,
            "recovery_required": true,
            "dispatch_id": id,
        });
        let updated = sqlx::query(
            "UPDATE dispatches SET status='indeterminate', result_json=?, updated_ms=? WHERE id=? AND status='pending'",
        )
        .bind(serde_json::to_string(&result)?)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        // id is the primary key, so at most one row can match; zero rows means the
        // dispatch was already finalized (or never existed) and has nothing to recover.
        debug_assert!(updated <= 1);
        Ok(())
    }

    pub async fn recover_stale_pending(&self, stale_after: Duration) -> anyhow::Result<u64> {
        let cutoff = Utc::now()
            .timestamp_millis()
            .saturating_sub(i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX));
        let result = json!({
            "ok": false,
            "error": "dispatch remained pending past the recovery deadline",
            "recovery_required": true,
        });
        let updated = sqlx::query(
            "UPDATE dispatches SET status='indeterminate', result_json=?, updated_ms=? WHERE status='pending' AND updated_ms < ?",
        )
        .bind(serde_json::to_string(&result)?)
        .bind(Utc::now().timestamp_millis())
        .bind(cutoff)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated)
    }

    pub async fn recent_operations(&self, role: &str, limit: i64) -> anyhow::Result<Value> {
        let rows = sqlx::query(
            r#"SELECT DISTINCT d.id, d.sender, d.kind, d.status, d.result_json,
                              d.created_ms, d.updated_ms
               FROM dispatches d
               LEFT JOIN dispatch_targets t ON t.dispatch_id = d.id
               WHERE d.sender = ? OR t.target = ?
               ORDER BY d.created_ms DESC LIMIT ?"#,
        )
        .bind(role)
        .bind(role)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut operations = Vec::new();
        for row in rows {
            let id = row.get::<String, _>("id");
            let kind = row.get::<String, _>("kind");
            let mut targets = sqlx::query_scalar::<_, String>(
                "SELECT target FROM dispatch_targets WHERE dispatch_id = ? ORDER BY target",
            )
            .bind(&id)
            .fetch_all(&self.pool)
            .await?;
            let mut result: Option<Value> = row
                .try_get::<Option<String>, _>("result_json")?
                .and_then(|raw| serde_json::from_str(&raw).ok());
            let is_sender = row.get::<String, _>("sender") == role;
            if !is_sender {
                targets.retain(|target| target == role);
                result = result.map(|value| project_result_for_recipient(value, role));
            }
            let delivery_rows = sqlx::query(
                r#"SELECT id, recipient, status, attempts, last_error,
                          created_ms, delivered_ms, run_id
                   FROM delivery_outbox
                   WHERE dispatch_id = ? AND (? OR recipient = ?)
                   ORDER BY sequence"#,
            )
            .bind(&id)
            .bind(is_sender)
            .bind(role)
            .fetch_all(&self.pool)
            .await?;
            let deliveries = delivery_rows
                .into_iter()
                .map(|delivery| {
                    let delivered_ms = delivery
                        .try_get::<Option<i64>, _>("delivered_ms")
                        .ok()
                        .flatten();
                    json!({
                        "queue_id": delivery.get::<String, _>("id"),
                        "recipient": delivery.get::<String, _>("recipient"),
                        "status": delivery.get::<String, _>("status"),
                        "attempts": delivery.get::<i64, _>("attempts"),
                        "last_error": delivery.try_get::<Option<String>, _>("last_error").ok().flatten(),
                        "run_id": delivery.try_get::<Option<String>, _>("run_id").ok().flatten(),
                        "created_at": millis_to_rfc3339(delivery.get("created_ms")),
                        "delivered_at": delivered_ms.map(millis_to_rfc3339),
                    })
                })
                .collect::<Vec<_>>();
            operations.push(json!({
                "id": id,
                "sender": row.get::<String, _>("sender"),
                "kind": kind,
                "targets": targets,
                "status": row.get::<String, _>("status"),
                "result": result,
                "deliveries": deliveries,
                "created_at": millis_to_rfc3339(row.get("created_ms")),
                "updated_at": millis_to_rfc3339(row.get("updated_ms")),
            }));
        }
        Ok(json!({"caller": role, "operations": operations}))
    }

    pub async fn recent_outbox(
        &self,
        role: &str,
        manager_role: &str,
        limit: i64,
    ) -> anyhow::Result<Value> {
        let rows = if role == manager_role {
            sqlx::query(
                r#"SELECT id, sender, event, recipients, status, attempts, next_chunk,
                          last_error, created_ms, delivered_ms
                   FROM telegram_outbox ORDER BY created_ms DESC LIMIT ?"#,
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"SELECT id, sender, event, recipients, status, attempts, next_chunk,
                          last_error, created_ms, delivered_ms
                   FROM telegram_outbox WHERE sender = ? ORDER BY created_ms DESC LIMIT ?"#,
            )
            .bind(role)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        let items = rows
            .into_iter()
            .map(|row| {
                let delivered_ms = row.try_get::<Option<i64>, _>("delivered_ms").ok().flatten();
                json!({
                    "id": row.get::<String, _>("id"),
                    "sender": row.get::<String, _>("sender"),
                    "event": row.get::<String, _>("event"),
                    "recipients": row.get::<String, _>("recipients"),
                    "status": row.get::<String, _>("status"),
                    "attempts": row.get::<i64, _>("attempts"),
                    "next_chunk": row.get::<i64, _>("next_chunk"),
                    "last_error": row.try_get::<Option<String>, _>("last_error").ok().flatten(),
                    "created_at": millis_to_rfc3339(row.get("created_ms")),
                    "delivered_at": delivered_ms.map(millis_to_rfc3339),
                })
            })
            .collect::<Vec<_>>();
        let mut response = json!({"caller": role, "items": items});
        if role == manager_role {
            let offset = self.telegram_update_offset().await?;
            let last_poll_at = self
                .service_state_value(TELEGRAM_POLL_SUCCESS_KEY)
                .await?
                .map(|(_, updated_ms)| millis_to_rfc3339(updated_ms));
            response["inbound_update_offset"] = json!(offset);
            response["inbound_last_poll_at"] = json!(last_poll_at);
        }
        Ok(response)
    }

    pub async fn role_messaging_enabled(&self, role: &str) -> anyhow::Result<bool> {
        let enabled =
            sqlx::query_scalar::<_, i64>("SELECT enabled FROM role_messaging WHERE role = ?")
                .bind(role)
                .fetch_optional(&self.pool)
                .await?;
        Ok(enabled.unwrap_or(1) != 0)
    }

    pub async fn role_messaging_state(&self, role: &str) -> anyhow::Result<Option<(bool, i64)>> {
        let state = sqlx::query("SELECT enabled, changed_ms FROM role_messaging WHERE role = ?")
            .bind(role)
            .fetch_optional(&self.pool)
            .await?;
        Ok(state.map(|row| (row.get::<i64, _>("enabled") != 0, row.get("changed_ms"))))
    }

    pub async fn disabled_roles(&self, roles: &[String]) -> anyhow::Result<Vec<String>> {
        let mut disabled = Vec::new();
        for role in roles {
            if !self.role_messaging_enabled(role).await? {
                disabled.push(role.clone());
            }
        }
        Ok(disabled)
    }

    pub async fn set_role_messaging(
        &self,
        role: &str,
        enabled: bool,
        changed_by: &str,
        reason: &str,
        clear_queue: bool,
    ) -> anyhow::Result<Value> {
        let now = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"INSERT INTO role_messaging(role, enabled, changed_by, reason, changed_ms)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(role) DO UPDATE SET
                 enabled=excluded.enabled,
                 changed_by=excluded.changed_by,
                 reason=excluded.reason,
                 changed_ms=excluded.changed_ms"#,
        )
        .bind(role)
        .bind(i64::from(enabled))
        .bind(changed_by)
        .bind(reason)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let (cancelled, cancelled_deliveries) = if !enabled && clear_queue {
            (
                cancel_role_outbox(&mut tx, role, true, changed_by, now).await?,
                cancel_role_delivery_outbox(&mut tx, role, true, changed_by, now).await?,
            )
        } else {
            (0, 0)
        };
        tx.commit().await?;
        Ok(json!({
            "ok": true,
            "agent": role,
            "messaging_enabled": enabled,
            "changed_by": changed_by,
            "reason": reason,
            "changed_at": millis_to_rfc3339(now),
            "cancelled_outbox_items": cancelled,
            "cancelled_delivery_items": cancelled_deliveries,
        }))
    }

    pub async fn clear_role_outbox(
        &self,
        role: &str,
        include_dead: bool,
        changed_by: &str,
    ) -> anyhow::Result<Value> {
        let now = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        let cancelled = cancel_role_outbox(&mut tx, role, include_dead, changed_by, now).await?;
        let cancelled_deliveries =
            cancel_role_delivery_outbox(&mut tx, role, include_dead, changed_by, now).await?;
        tx.commit().await?;
        Ok(json!({
            "ok": true,
            "agent": role,
            "cancelled_outbox_items": cancelled,
            "cancelled_delivery_items": cancelled_deliveries,
            "included_dead_items": include_dead,
            "changed_by": changed_by,
            "changed_at": millis_to_rfc3339(now),
        }))
    }

    pub async fn messaging_snapshot(&self, roles: &[String]) -> anyhow::Result<Value> {
        let state_rows =
            sqlx::query("SELECT role, enabled, changed_by, reason, changed_ms FROM role_messaging")
                .fetch_all(&self.pool)
                .await?;
        let states = state_rows
            .into_iter()
            .map(|row| (row.get::<String, _>("role"), row))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut queue =
            std::collections::BTreeMap::<String, std::collections::BTreeMap<String, i64>>::new();
        let mut delivery_queue =
            std::collections::BTreeMap::<String, std::collections::BTreeMap<String, i64>>::new();
        for role in roles {
            // `recipients` is an internal comma-separated list of validated role
            // names. Surrounding both sides with commas makes this an exact token
            // match, so `developer` cannot match `lead-developer`.
            let rows = sqlx::query(
                r#"SELECT status, COUNT(*) AS item_count
                   FROM telegram_outbox
                   WHERE status IN ('pending', 'dead')
                     AND (
                       sender = ?
                       OR instr(
                         ',' || replace(recipients, ' ', '') || ',',
                         ',' || ? || ','
                       ) > 0
                     )
                   GROUP BY status"#,
            )
            .bind(role)
            .bind(role)
            .fetch_all(&self.pool)
            .await?;
            for row in rows {
                queue
                    .entry(role.clone())
                    .or_default()
                    .insert(row.get("status"), row.get("item_count"));
            }
            let rows = sqlx::query(
                r#"SELECT status, COUNT(*) AS item_count
                   FROM delivery_outbox
                   WHERE status IN ('pending', 'dead')
                     AND (sender = ? OR recipient = ?)
                   GROUP BY status"#,
            )
            .bind(role)
            .bind(role)
            .fetch_all(&self.pool)
            .await?;
            for row in rows {
                delivery_queue
                    .entry(role.clone())
                    .or_default()
                    .insert(row.get("status"), row.get("item_count"));
            }
        }
        let items = roles
            .iter()
            .map(|role| {
                let state = states.get(role);
                json!({
                    "agent": role,
                    "messaging_enabled": state.is_none_or(|row| row.get::<i64, _>("enabled") != 0),
                    "changed_by": state.map(|row| row.get::<String, _>("changed_by")),
                    "reason": state.map(|row| row.get::<String, _>("reason")),
                    "changed_at": state.map(|row| millis_to_rfc3339(row.get("changed_ms"))),
                    "outbox": queue.get(role).cloned().unwrap_or_default(),
                    "delivery_queue": delivery_queue.get(role).cloned().unwrap_or_default(),
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"roles": items}))
    }

    pub async fn due_outbox(&self, limit: i64) -> anyhow::Result<Vec<OutboxItem>> {
        let rows = sqlx::query(
            r#"SELECT o.id, o.sender, o.event, o.recipients, o.text,
                      o.attempts, o.next_chunk, o.chat_id, o.reply_to_message_id,
                      o.thread_id, o.media_json
               FROM telegram_outbox o
               WHERE o.status = 'pending' AND o.next_attempt_ms <= ?
                 AND NOT EXISTS (
                   SELECT 1 FROM role_messaging r
                   WHERE r.enabled = 0
                     AND (
                       r.role = o.sender
                       OR instr(
                         ',' || replace(o.recipients, ' ', '') || ',',
                         ',' || r.role || ','
                       ) > 0
                     )
                 )
               ORDER BY o.created_ms LIMIT ?"#,
        )
        .bind(Utc::now().timestamp_millis())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| OutboxItem {
                id: row.get("id"),
                sender: row.get("sender"),
                event: row.get("event"),
                recipients: row.get("recipients"),
                text: row.get("text"),
                attempts: row.get("attempts"),
                next_chunk: row.get("next_chunk"),
                chat_id: row.get("chat_id"),
                reply_to_message_id: row.get("reply_to_message_id"),
                thread_id: row.get("thread_id"),
                media: row
                    .get::<Option<String>, _>("media_json")
                    .and_then(|json| serde_json::from_str(&json).ok())
                    .unwrap_or_default(),
            })
            .collect())
    }

    pub async fn outbox_delivery_eligible(&self, id: &str) -> anyhow::Result<bool> {
        let eligible = sqlx::query_scalar::<_, i64>(
            r#"SELECT EXISTS(
                 SELECT 1 FROM telegram_outbox o
                 WHERE o.id = ? AND o.status = 'pending'
                   AND NOT EXISTS (
                     SELECT 1 FROM role_messaging r
                     WHERE r.enabled = 0
                       AND (
                         r.role = o.sender
                         OR instr(
                           ',' || replace(o.recipients, ' ', '') || ',',
                           ',' || r.role || ','
                         ) > 0
                       )
                   )
               )"#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(eligible != 0)
    }

    pub async fn mark_outbox_chunk_sent(&self, id: &str, next_chunk: i64) -> anyhow::Result<()> {
        let updated =
            sqlx::query("UPDATE telegram_outbox SET next_chunk=? WHERE id=? AND status='pending'")
                .bind(next_chunk)
                .bind(id)
                .execute(&self.pool)
                .await?
                .rows_affected();
        if updated != 1 {
            return Err(anyhow!("outbox item is not pending"));
        }
        Ok(())
    }

    pub async fn mark_outbox_delivered(&self, id: &str) -> anyhow::Result<()> {
        let updated = sqlx::query(
            "UPDATE telegram_outbox SET status='delivered', delivered_ms=?, last_error=NULL WHERE id=? AND status='pending'",
        )
        .bind(Utc::now().timestamp_millis())
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(updated == 1, "outbox item is not pending");
        Ok(())
    }

    pub async fn mark_outbox_failed(
        &self,
        id: &str,
        attempts: i64,
        max_attempts: i64,
        error: &str,
        retry_after_seconds: Option<i64>,
    ) -> anyhow::Result<i64> {
        let terminal = attempts >= max_attempts;
        let delay_seconds = retry_after_seconds
            .unwrap_or_else(|| 2_i64.saturating_pow(u32::try_from(attempts.min(8)).unwrap_or(8)));
        let next = Utc::now()
            .timestamp_millis()
            .saturating_add(delay_seconds.saturating_mul(1000));
        let updated = sqlx::query(
            r#"UPDATE telegram_outbox
               SET status=?, attempts=?, next_attempt_ms=?, last_error=?
               WHERE id=? AND status='pending'"#,
        )
        .bind(if terminal { "dead" } else { "pending" })
        .bind(attempts)
        .bind(next)
        .bind(error.chars().take(500).collect::<String>())
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(updated == 1, "outbox item is not pending");
        Ok(next)
    }

    /// One-shot fallback for a delivery that failed permanently *because it was
    /// a reply*: the source message may have been deleted or is older than the
    /// Telegram reply window (48h). The reply link is dropped and the item
    /// becomes due again immediately so the content itself is still delivered
    /// as a plain message.
    pub async fn clear_outbox_reply(&self, id: &str) -> anyhow::Result<()> {
        let updated = sqlx::query(
            r#"UPDATE telegram_outbox
               SET reply_to_message_id = NULL,
                   next_attempt_ms = ?,
                   last_error = 'reply link dropped after a permanent delivery failure'
               WHERE id=? AND status='pending'"#,
        )
        .bind(Utc::now().timestamp_millis())
        .bind(id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        ensure!(updated == 1, "outbox item is not pending");
        Ok(())
    }

    pub async fn defer_pending_outbox_until(
        &self,
        sender: Option<&str>,
        not_before_ms: i64,
    ) -> anyhow::Result<u64> {
        let updated = if let Some(sender) = sender {
            sqlx::query(
                r#"UPDATE telegram_outbox
                   SET next_attempt_ms = MAX(next_attempt_ms, ?)
                   WHERE status='pending' AND sender=?"#,
            )
            .bind(not_before_ms)
            .bind(sender)
            .execute(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"UPDATE telegram_outbox
                   SET next_attempt_ms = MAX(next_attempt_ms, ?)
                   WHERE status='pending'"#,
            )
            .bind(not_before_ms)
            .execute(&self.pool)
            .await?
        };
        Ok(updated.rows_affected())
    }

    pub async fn cleanup(
        &self,
        activity_retention_days: i64,
        operation_retention_days: i64,
        outbox_retention_days: i64,
        rate_window: Duration,
        pending_stale_after: Duration,
    ) -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        let cutoff = now.saturating_sub(activity_retention_days.saturating_mul(86_400_000));
        sqlx::query("DELETE FROM activity_events WHERE received_ms < ?")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        let rate_window_ms = i64::try_from(rate_window.as_millis()).unwrap_or(i64::MAX);
        sqlx::query("DELETE FROM rate_events WHERE created_ms < ?")
            .bind(now.saturating_sub(rate_window_ms))
            .execute(&self.pool)
            .await?;
        // `delivery_outbox` rows reference their originating dispatch with
        // ON DELETE CASCADE. A role may legitimately remain disabled or
        // unavailable longer than operation retention, so never age out the
        // parent while an opaque wake is still pending; doing so would
        // silently drop accepted FIFO work.
        sqlx::query(
            r#"DELETE FROM dispatches
               WHERE status != 'pending' AND updated_ms < ?
                 AND NOT EXISTS (
                   SELECT 1 FROM delivery_outbox q
                   WHERE q.dispatch_id = dispatches.id AND q.status = 'pending'
                 )"#,
        )
        .bind(now.saturating_sub(operation_retention_days.saturating_mul(86_400_000)))
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "DELETE FROM telegram_outbox WHERE status IN ('delivered','dead','cancelled') AND created_ms < ?",
        )
        .bind(now.saturating_sub(outbox_retention_days.saturating_mul(86_400_000)))
        .execute(&self.pool)
        .await?;
        self.recover_stale_pending(pending_stale_after).await?;
        Ok(())
    }

    pub async fn telegram_update_offset(&self) -> anyhow::Result<Option<i64>> {
        Ok(self
            .service_state_value(TELEGRAM_OFFSET_KEY)
            .await?
            .map(|(value, _)| value))
    }

    async fn service_state_value(&self, key: &str) -> anyhow::Result<Option<(i64, i64)>> {
        sqlx::query_as("SELECT value_int, updated_ms FROM service_state WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn mark_telegram_poll_success(&self) -> anyhow::Result<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            r#"INSERT INTO service_state(key, value_int, updated_ms)
               VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET
                 value_int = excluded.value_int,
                 updated_ms = excluded.updated_ms"#,
        )
        .bind(TELEGRAM_POLL_SUCCESS_KEY)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn advance_telegram_update_offset(&self, offset: i64) -> anyhow::Result<()> {
        ensure!(offset >= 0, "Telegram update offset must not be negative");
        let now = Utc::now().timestamp_millis();
        sqlx::query(
            r#"INSERT INTO service_state(key, value_int, updated_ms)
               VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET
                 value_int = MAX(service_state.value_int, excluded.value_int),
                 updated_ms = excluded.updated_ms"#,
        )
        .bind(TELEGRAM_OFFSET_KEY)
        .bind(offset)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[derive(Default)]
pub struct BTreeMapResult(pub std::collections::BTreeMap<String, Value>);

fn activity_json(row: &sqlx::sqlite::SqliteRow, now: i64, stale_ms: i64) -> Value {
    let occurred_ms = row.get::<i64, _>("occurred_ms");
    json!({
        "sender": row.get::<String, _>("sender"),
        "event": row.get::<String, _>("event"),
        "turn_id": row.get::<String, _>("turn_id"),
        "detail": row.get::<String, _>("detail"),
        "occurred_at": millis_to_rfc3339(occurred_ms),
        "received_at": millis_to_rfc3339(row.get("received_ms")),
        "stale": now.saturating_sub(occurred_ms) > stale_ms,
    })
}

fn millis_to_rfc3339(value: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(value)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .to_rfc3339()
}

fn project_result_for_recipient(mut value: Value, role: &str) -> Value {
    if let Some(object) = value.as_object_mut()
        && let Some(results) = object.get_mut("results").and_then(Value::as_object_mut)
    {
        results.retain(|target, _| target == role);
    }
    value
}

async fn insert_outbox(
    tx: &mut Transaction<'_, Sqlite>,
    audit: &AuditMessage,
    now: i64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"INSERT INTO telegram_outbox
           (id, sender, event, recipients, text, status, attempts, next_chunk, next_attempt_ms, created_ms, chat_id, reply_to_message_id, thread_id, media_json)
           VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?, ?, ?, ?, ?)"#,
    )
    .bind(&audit.id)
    .bind(&audit.sender)
    .bind(&audit.event)
    .bind(&audit.recipients)
    .bind(&audit.text)
    .bind(now)
    .bind(now)
    .bind(audit.chat_id)
    .bind(audit.reply_to_message_id)
    .bind(audit.thread_id)
    .bind(if audit.media.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&audit.media).unwrap_or_default())
    })
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn cancel_role_outbox(
    tx: &mut Transaction<'_, Sqlite>,
    role: &str,
    include_dead: bool,
    changed_by: &str,
    now: i64,
) -> anyhow::Result<u64> {
    let statuses = if include_dead {
        "status IN ('pending', 'dead')"
    } else {
        "status = 'pending'"
    };
    let reason = format!("cancelled by {changed_by} at {}", millis_to_rfc3339(now));
    let statement = format!(
        "UPDATE telegram_outbox SET status='cancelled', last_error=? \
         WHERE (sender=? OR instr(',' || replace(recipients, ' ', '') || ',', \
                ',' || ? || ',') > 0) AND {statuses}"
    );
    Ok(sqlx::query(&statement)
        .bind(reason)
        .bind(role)
        .bind(role)
        .execute(&mut **tx)
        .await?
        .rows_affected())
}

async fn cancel_role_delivery_outbox(
    tx: &mut Transaction<'_, Sqlite>,
    role: &str,
    include_dead: bool,
    changed_by: &str,
    now: i64,
) -> anyhow::Result<u64> {
    let statuses = if include_dead {
        "status IN ('pending', 'dead')"
    } else {
        "status = 'pending'"
    };
    let reason = format!("cancelled by {changed_by} at {}", millis_to_rfc3339(now));
    let statement = format!(
        "UPDATE delivery_outbox SET status='cancelled', last_error=? \
         WHERE (sender=? OR recipient=?) AND {statuses}"
    );
    Ok(sqlx::query(&statement)
        .bind(reason)
        .bind(role)
        .bind(role)
        .execute(&mut **tx)
        .await?
        .rows_affected())
}

const SCHEMA: &[&str] = &[
    r#"CREATE TABLE IF NOT EXISTS activity_events (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         sender TEXT NOT NULL,
         target TEXT NOT NULL,
         event TEXT NOT NULL,
         event_rank INTEGER NOT NULL,
         turn_id TEXT NOT NULL,
         detail TEXT NOT NULL,
         occurred_ms INTEGER NOT NULL,
         received_ms INTEGER NOT NULL,
         UNIQUE(sender, target, event, turn_id)
       )"#,
    "CREATE INDEX IF NOT EXISTS activity_events_target_idx ON activity_events(target, id DESC)",
    r#"CREATE TABLE IF NOT EXISTS activity_state (
         sender TEXT NOT NULL,
         target TEXT NOT NULL,
         event TEXT NOT NULL,
         event_rank INTEGER NOT NULL,
         turn_id TEXT NOT NULL,
         detail TEXT NOT NULL,
         occurred_ms INTEGER NOT NULL,
         received_ms INTEGER NOT NULL,
         PRIMARY KEY(sender, target)
       )"#,
    r#"CREATE TABLE IF NOT EXISTS dispatches (
         id TEXT PRIMARY KEY,
         sender TEXT NOT NULL,
         kind TEXT NOT NULL,
         idempotency_key TEXT,
         fingerprint TEXT NOT NULL,
         status TEXT NOT NULL,
         result_json TEXT,
         created_ms INTEGER NOT NULL,
         updated_ms INTEGER NOT NULL,
         telegram_chat_id INTEGER,
         telegram_message_id INTEGER,
         telegram_thread_id INTEGER
       )"#,
    "CREATE UNIQUE INDEX IF NOT EXISTS dispatches_idempotency_idx ON dispatches(sender, kind, idempotency_key) WHERE idempotency_key IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS dispatches_sender_idx ON dispatches(sender, created_ms DESC)",
    "CREATE INDEX IF NOT EXISTS dispatches_fingerprint_idx ON dispatches(sender, kind, fingerprint, created_ms DESC)",
    r#"CREATE TABLE IF NOT EXISTS dispatch_targets (
         dispatch_id TEXT NOT NULL REFERENCES dispatches(id) ON DELETE CASCADE,
         target TEXT NOT NULL,
         PRIMARY KEY(dispatch_id, target)
       )"#,
    "CREATE INDEX IF NOT EXISTS dispatch_targets_target_idx ON dispatch_targets(target, dispatch_id)",
    r#"CREATE TABLE IF NOT EXISTS rate_events (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         actor TEXT NOT NULL,
         created_ms INTEGER NOT NULL
       )"#,
    "CREATE INDEX IF NOT EXISTS rate_events_actor_idx ON rate_events(actor, created_ms)",
    // Window-pruning indexes for the cleanup and rate-window deletes, which
    // filter on the timestamp column alone (the actor/target indexes do not
    // help those scans).
    "CREATE INDEX IF NOT EXISTS rate_events_created_idx ON rate_events(created_ms)",
    "CREATE INDEX IF NOT EXISTS activity_events_received_idx ON activity_events(received_ms)",
    "CREATE INDEX IF NOT EXISTS dispatches_updated_idx ON dispatches(updated_ms)",
    r#"CREATE TABLE IF NOT EXISTS delivery_outbox (
         sequence INTEGER PRIMARY KEY AUTOINCREMENT,
         id TEXT NOT NULL UNIQUE,
         dispatch_id TEXT NOT NULL REFERENCES dispatches(id) ON DELETE CASCADE,
         sender TEXT NOT NULL,
         kind TEXT NOT NULL,
         recipient TEXT NOT NULL,
         body TEXT NOT NULL,
         instructions TEXT NOT NULL,
         status TEXT NOT NULL,
         attempts INTEGER NOT NULL,
         next_attempt_ms INTEGER NOT NULL,
         last_error TEXT,
         created_ms INTEGER NOT NULL,
         delivered_ms INTEGER,
         run_id TEXT
       )"#,
    "CREATE INDEX IF NOT EXISTS delivery_outbox_due_idx ON delivery_outbox(status, next_attempt_ms, sequence)",
    "CREATE INDEX IF NOT EXISTS delivery_outbox_recipient_idx ON delivery_outbox(recipient, status, sequence)",
    r#"CREATE TABLE IF NOT EXISTS telegram_outbox (
         id TEXT PRIMARY KEY,
         sender TEXT NOT NULL,
         event TEXT NOT NULL,
         recipients TEXT NOT NULL,
         text TEXT NOT NULL,
         status TEXT NOT NULL,
         attempts INTEGER NOT NULL,
         next_chunk INTEGER NOT NULL DEFAULT 0,
         next_attempt_ms INTEGER NOT NULL,
         last_error TEXT,
         created_ms INTEGER NOT NULL,
         delivered_ms INTEGER,
         chat_id INTEGER,
         reply_to_message_id INTEGER,
         thread_id INTEGER,
         media_json TEXT
       )"#,
    "CREATE INDEX IF NOT EXISTS telegram_outbox_due_idx ON telegram_outbox(status, next_attempt_ms)",
    r#"CREATE TABLE IF NOT EXISTS role_messaging (
         role TEXT PRIMARY KEY,
         enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
         changed_by TEXT NOT NULL,
         reason TEXT NOT NULL,
         changed_ms INTEGER NOT NULL
       )"#,
    r#"CREATE TABLE IF NOT EXISTS service_state (
         key TEXT PRIMARY KEY,
         value_int INTEGER NOT NULL,
         updated_ms INTEGER NOT NULL
       )"#,
    r#"CREATE TABLE IF NOT EXISTS run_replies (
         run_id TEXT PRIMARY KEY,
         role TEXT NOT NULL,
         chat_id INTEGER NOT NULL,
         message_id INTEGER,
         thread_id INTEGER,
         created_ms INTEGER NOT NULL
       )"#,
];

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn migrates_python_activity_and_enforces_idempotency() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-store-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let legacy = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;
        sqlx::query(
            "CREATE TABLE activity_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender TEXT NOT NULL, target TEXT NOT NULL, event TEXT NOT NULL,
                turn_id TEXT NOT NULL, detail TEXT NOT NULL, recorded_at TEXT NOT NULL
            )",
        )
        .execute(&legacy)
        .await?;
        sqlx::query(
            "CREATE TABLE activity_state (
                sender TEXT NOT NULL, target TEXT NOT NULL, event TEXT NOT NULL,
                turn_id TEXT NOT NULL, detail TEXT NOT NULL, recorded_at TEXT NOT NULL,
                PRIMARY KEY (sender, target)
            )",
        )
        .execute(&legacy)
        .await?;
        sqlx::query(
            "INSERT INTO activity_events(sender,target,event,turn_id,detail,recorded_at)
             VALUES ('developer','lead-developer','completed','turn_1','done','2026-08-07T00:00:00Z')",
        )
        .execute(&legacy)
        .await?;
        sqlx::query(
            "INSERT INTO activity_state(sender,target,event,turn_id,detail,recorded_at)
             VALUES ('developer','lead-developer','completed','turn_1','done','2026-08-07T00:00:00Z')",
        )
        .execute(&legacy)
        .await?;
        legacy.close().await;

        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let imported: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM activity_events")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(imported, 1);
        let legacy_table: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='table' AND name='activity_events_python_legacy'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(legacy_table, 1);

        let targets = vec!["developer".to_string()];
        assert!(matches!(
            store
                .reserve_dispatch(
                    "dispatch_1",
                    "manager",
                    "dispatch_to",
                    &targets,
                    Some("stable-key"),
                    "fingerprint",
                    1,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Reserved
        ));
        store
            .finish_dispatch("dispatch_1", "accepted", &json!({"ok": true}), None)
            .await?;
        assert!(matches!(
            store
                .reserve_dispatch(
                    "dispatch_2",
                    "manager",
                    "dispatch_to",
                    &targets,
                    Some("stable-key"),
                    "fingerprint",
                    1,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Existing(_)
        ));
        assert!(matches!(
            store
                .reserve_dispatch(
                    "dispatch_3",
                    "manager",
                    "dispatch_to",
                    &targets,
                    None,
                    "different",
                    1,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::RateLimited { .. }
        ));

        sqlx::query(
            "INSERT INTO dispatches
             (id,sender,kind,fingerprint,status,result_json,created_ms,updated_ms)
             VALUES ('broadcast_1','manager','dispatch_all','fp','partial',?,1,1)",
        )
        .bind(
            json!({
                "ok": false,
                "results": {
                    "developer": {"run_id": "developer-run"},
                    "designer": {"run_id": "designer-run"}
                }
            })
            .to_string(),
        )
        .execute(store.pool())
        .await?;
        for target in ["developer", "designer"] {
            sqlx::query(
                "INSERT INTO dispatch_targets(dispatch_id,target) VALUES ('broadcast_1',?)",
            )
            .bind(target)
            .execute(store.pool())
            .await?;
        }
        let visible = store.recent_operations("developer", 10).await?;
        let operation = visible["operations"]
            .as_array()
            .and_then(|operations| {
                operations
                    .iter()
                    .find(|operation| operation["id"] == "broadcast_1")
            })
            .expect("broadcast must be visible to its target");
        assert_eq!(operation["targets"], json!(["developer"]));
        assert_eq!(
            operation["result"]["results"],
            json!({"developer": {"run_id": "developer-run"}})
        );

        store.pool.close().await;
        for candidate in [
            path.clone(),
            path.with_extension("db-wal"),
            path.with_extension("db-shm"),
        ] {
            let _ = tokio::fs::remove_file(candidate).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn migrates_outbox_chunk_cursor_and_checks_transitions() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-outbox-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let legacy = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;
        sqlx::query(
            "CREATE TABLE telegram_outbox (
                id TEXT PRIMARY KEY, sender TEXT NOT NULL, event TEXT NOT NULL,
                recipients TEXT NOT NULL, text TEXT NOT NULL, status TEXT NOT NULL,
                attempts INTEGER NOT NULL, next_attempt_ms INTEGER NOT NULL,
                last_error TEXT, created_ms INTEGER NOT NULL, delivered_ms INTEGER
            )",
        )
        .execute(&legacy)
        .await?;
        sqlx::query(
            "INSERT INTO telegram_outbox
             (id,sender,event,recipients,text,status,attempts,next_attempt_ms,created_ms)
             VALUES ('audit_1','manager','ORDER','developer','test','pending',0,0,0)",
        )
        .execute(&legacy)
        .await?;
        legacy.close().await;

        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let cursor: i64 =
            sqlx::query_scalar("SELECT next_chunk FROM telegram_outbox WHERE id='audit_1'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(cursor, 0);
        store.mark_outbox_chunk_sent("audit_1", 1).await?;
        store.mark_outbox_delivered("audit_1").await?;
        assert!(store.mark_outbox_delivered("audit_1").await.is_err());
        assert_eq!(store.telegram_update_offset().await?, None);
        store.advance_telegram_update_offset(42).await?;
        store.advance_telegram_update_offset(12).await?;
        assert_eq!(store.telegram_update_offset().await?, Some(42));
        store.mark_telegram_poll_success().await?;
        let manager_outbox = store.recent_outbox("manager", "manager", 10).await?;
        assert_eq!(manager_outbox["inbound_update_offset"], json!(42));
        assert!(manager_outbox["inbound_last_poll_at"].is_string());
        let executor_outbox = store.recent_outbox("developer", "manager", 10).await?;
        assert!(executor_outbox.get("inbound_update_offset").is_none());
        assert!(executor_outbox.get("inbound_last_poll_at").is_none());

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn restart_recovers_a_durable_queued_dispatch_as_accepted() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-delivery-recovery-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        store
            .reserve_dispatch(
                "recover_dispatch",
                "manager",
                "dispatch_to",
                &["developer".to_string()],
                Some("recover-key"),
                "recover-fingerprint",
                100,
                Duration::from_secs(60),
            )
            .await?;
        store
            .enqueue_delivery(
                &DeliveryQueueItem {
                    id: "delivery_recover_dispatch_developer".to_string(),
                    dispatch_id: "recover_dispatch".to_string(),
                    sender: "manager".to_string(),
                    kind: "dispatch_to".to_string(),
                    recipient: "developer".to_string(),
                    body: "opaque wake".to_string(),
                    instructions: "read SLC".to_string(),
                    attempts: 0,
                },
                Duration::from_secs(1),
                "role busy",
            )
            .await?;
        store.pool.close().await;

        let recovered = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let (status, result_json): (String, String) =
            sqlx::query_as("SELECT status,result_json FROM dispatches WHERE id='recover_dispatch'")
                .fetch_one(recovered.pool())
                .await?;
        assert_eq!(status, "accepted");
        let result: Value = serde_json::from_str(&result_json)?;
        assert_eq!(result["ok"], json!(true));
        assert_eq!(result["queued"], json!(true));
        assert_eq!(result["recovered_after_restart"], json!(true));
        assert_eq!(
            result["queue_id"],
            json!("delivery_recover_dispatch_developer")
        );
        assert_eq!(result["queue_position"], json!(1));
        assert_eq!(result["recipient"], json!("developer"));
        assert_eq!(result["deliveries"][0]["status"], json!("pending"));

        recovered.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn restart_marks_partial_queue_persistence_as_recovery_required() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-partial-delivery-recovery-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        store
            .reserve_dispatch(
                "partial_recover_dispatch",
                "manager",
                "dispatch_to",
                &["developer".to_string(), "tester".to_string()],
                Some("partial-recover-key"),
                "partial-recover-fingerprint",
                100,
                Duration::from_secs(60),
            )
            .await?;
        store
            .enqueue_delivery(
                &DeliveryQueueItem {
                    id: "delivery_partial_recover_developer".to_string(),
                    dispatch_id: "partial_recover_dispatch".to_string(),
                    sender: "manager".to_string(),
                    kind: "dispatch_to".to_string(),
                    recipient: "developer".to_string(),
                    body: "opaque wake".to_string(),
                    instructions: "read SLC".to_string(),
                    attempts: 0,
                },
                Duration::from_secs(1),
                "role busy",
            )
            .await?;
        store.pool.close().await;

        let recovered = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let (status, result_json): (String, String) = sqlx::query_as(
            "SELECT status,result_json FROM dispatches WHERE id='partial_recover_dispatch'",
        )
        .fetch_one(recovered.pool())
        .await?;
        assert_eq!(
            status, "accepted",
            "the same idempotency key must not redispatch"
        );
        let result: Value = serde_json::from_str(&result_json)?;
        assert_eq!(result["ok"], json!(false));
        assert_eq!(result["recovery_required"], json!(true));
        assert_eq!(result["queued"], json!(true));
        assert_eq!(result["dispatch_id"], json!("partial_recover_dispatch"));

        recovered.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn delivery_revalidation_requires_due_fifo_head() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-delivery-eligibility-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        for index in 1..=2 {
            let dispatch_id = format!("eligibility_dispatch_{index}");
            store
                .reserve_dispatch(
                    &dispatch_id,
                    "manager",
                    "dispatch_to",
                    &["developer".to_string()],
                    Some(&format!("eligibility-key-{index}")),
                    &format!("eligibility-fingerprint-{index}"),
                    100,
                    Duration::from_secs(60),
                )
                .await?;
            store
                .enqueue_delivery(
                    &DeliveryQueueItem {
                        id: format!("eligibility_delivery_{index}"),
                        dispatch_id,
                        sender: "manager".to_string(),
                        kind: "dispatch_to".to_string(),
                        recipient: "developer".to_string(),
                        body: format!("wake {index}"),
                        instructions: "read SLC".to_string(),
                        attempts: 0,
                    },
                    Duration::from_secs(1),
                    "role busy",
                )
                .await?;
        }
        sqlx::query("UPDATE delivery_outbox SET next_attempt_ms=0")
            .execute(store.pool())
            .await?;
        assert!(store.delivery_eligible("eligibility_delivery_1").await?);
        assert!(!store.delivery_eligible("eligibility_delivery_2").await?);

        sqlx::query(
            "UPDATE delivery_outbox SET next_attempt_ms=? WHERE id='eligibility_delivery_1'",
        )
        .bind(Utc::now().timestamp_millis() + 60_000)
        .execute(store.pool())
        .await?;
        assert!(!store.delivery_eligible("eligibility_delivery_1").await?);
        assert!(!store.delivery_eligible("eligibility_delivery_2").await?);

        sqlx::query(
            "UPDATE delivery_outbox SET status='delivered' WHERE id='eligibility_delivery_1'",
        )
        .execute(store.pool())
        .await?;
        assert!(store.delivery_eligible("eligibility_delivery_2").await?);

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn messaging_circuit_breaker_is_persistent_and_cancels_queue() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-messaging-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        assert!(store.role_messaging_enabled("developer").await?);

        for (id, status) in [
            ("audit_pending", "pending"),
            ("audit_dead", "dead"),
            ("audit_delivered", "delivered"),
        ] {
            sqlx::query(
                r#"INSERT INTO telegram_outbox
                   (id,sender,event,recipients,text,status,attempts,next_chunk,
                    next_attempt_ms,created_ms,delivered_ms)
                   VALUES (?,'developer','REPORT','manager','test',?,0,0,0,0,
                           CASE WHEN ?='delivered' THEN 1 ELSE NULL END)"#,
            )
            .bind(id)
            .bind(status)
            .bind(status)
            .execute(store.pool())
            .await?;
        }
        sqlx::query(
            r#"INSERT INTO telegram_outbox
               (id,sender,event,recipients,text,status,attempts,next_chunk,
                next_attempt_ms,created_ms,delivered_ms)
               VALUES
                 ('audit_to_developer_pending','manager','ORDER_ALL',
                  'developer, tester','test','pending',0,0,0,0,NULL),
                 ('audit_to_developer_dead','manager','ORDER',
                  'developer','test','dead',0,0,0,0,NULL),
                 ('audit_to_developer_delivered','manager','ORDER',
                  'developer','test','delivered',0,0,0,0,1),
                 ('audit_to_lead_pending','manager','ORDER',
                  'lead-developer','test','pending',0,0,0,0,NULL)"#,
        )
        .execute(store.pool())
        .await?;
        sqlx::query(
            r#"INSERT INTO dispatches
               (id,sender,kind,fingerprint,status,created_ms,updated_ms)
               VALUES
                 ('delivery_dispatch_1','manager','dispatch_to','f1','accepted',0,0),
                 ('delivery_dispatch_2','manager','dispatch_to','f2','accepted',0,0),
                 ('delivery_dispatch_3','manager','dispatch_to','f3','accepted',0,0),
                 ('delivery_dispatch_4','manager','dispatch_to','f4','accepted',0,0)"#,
        )
        .execute(store.pool())
        .await?;
        sqlx::query(
            r#"INSERT INTO delivery_outbox
               (id,dispatch_id,sender,kind,recipient,body,instructions,status,
                attempts,next_attempt_ms,created_ms,delivered_ms)
               VALUES
                 ('delivery_pending','delivery_dispatch_1','manager','dispatch_to',
                  'developer','wake','run','pending',0,0,0,NULL),
                 ('delivery_dead','delivery_dispatch_2','developer','msg_to',
                  'manager','wake','run','dead',1,0,1,NULL),
                 ('delivery_delivered','delivery_dispatch_3','manager','dispatch_to',
                  'developer','wake','run','delivered',0,0,2,3),
                 ('delivery_unrelated','delivery_dispatch_4','manager','dispatch_to',
                  'lead-developer','wake','run','pending',0,0,3,NULL)"#,
        )
        .execute(store.pool())
        .await?;

        let disabled = store
            .set_role_messaging(
                "developer",
                false,
                "manager",
                "operator emergency stop",
                true,
            )
            .await?;
        assert_eq!(
            disabled["cancelled_outbox_items"],
            json!(4),
            "both outbound and inbound undelivered audits are cancelled"
        );
        assert_eq!(
            disabled["cancelled_delivery_items"],
            json!(2),
            "pending and dead role wakes are cancelled in both directions"
        );
        assert!(!store.role_messaging_enabled("developer").await?);
        let statuses = sqlx::query_as::<_, (String, String)>(
            "SELECT id,status FROM telegram_outbox ORDER BY id",
        )
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(statuses["audit_pending"], "cancelled");
        assert_eq!(statuses["audit_dead"], "cancelled");
        assert_eq!(statuses["audit_delivered"], "delivered");
        assert_eq!(statuses["audit_to_developer_pending"], "cancelled");
        assert_eq!(statuses["audit_to_developer_dead"], "cancelled");
        assert_eq!(statuses["audit_to_developer_delivered"], "delivered");
        assert_eq!(
            statuses["audit_to_lead_pending"], "pending",
            "exact recipient matching must not confuse developer with lead-developer"
        );
        let delivery_statuses = sqlx::query_as::<_, (String, String)>(
            "SELECT id,status FROM delivery_outbox ORDER BY id",
        )
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(delivery_statuses["delivery_pending"], "cancelled");
        assert_eq!(delivery_statuses["delivery_dead"], "cancelled");
        assert_eq!(delivery_statuses["delivery_delivered"], "delivered");
        assert_eq!(delivery_statuses["delivery_unrelated"], "pending");

        let snapshot = store
            .messaging_snapshot(&["developer".to_string(), "lead-developer".to_string()])
            .await?;
        assert_eq!(snapshot["roles"][0]["messaging_enabled"], json!(false));
        assert_eq!(snapshot["roles"][1]["messaging_enabled"], json!(true));
        assert_eq!(snapshot["roles"][0]["outbox"], json!({}));
        assert_eq!(snapshot["roles"][0]["delivery_queue"], json!({}));
        assert_eq!(snapshot["roles"][1]["outbox"]["pending"], json!(1));
        assert_eq!(snapshot["roles"][1]["delivery_queue"]["pending"], json!(1));

        store
            .set_role_messaging("developer", true, "manager", "resume", false)
            .await?;
        assert!(store.role_messaging_enabled("developer").await?);
        let cancelled: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox WHERE status='cancelled'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(
            cancelled, 4,
            "re-enabling must never replay cancelled items"
        );

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn disabled_role_holds_preserved_outbox_without_starving_other_roles()
    -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-messaging-hold-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        sqlx::query(
            r#"INSERT INTO telegram_outbox
               (id,sender,event,recipients,text,status,attempts,next_chunk,
                next_attempt_ms,created_ms,delivered_ms)
               VALUES
                 ('audit_from_developer','developer','REPORT','manager',
                  'outbound','pending',0,0,0,1,NULL),
                 ('audit_to_developer','manager','ORDER','developer',
                  'inbound','pending',0,0,0,2,NULL),
                 ('audit_to_lead','manager','ORDER','lead-developer',
                  'unrelated','pending',0,0,0,3,NULL)"#,
        )
        .execute(store.pool())
        .await?;
        sqlx::query(
            r#"INSERT INTO dispatches
               (id,sender,kind,fingerprint,status,created_ms,updated_ms)
               VALUES ('held_delivery_dispatch','manager','dispatch_to','held','accepted',0,0)"#,
        )
        .execute(store.pool())
        .await?;
        sqlx::query(
            r#"INSERT INTO delivery_outbox
               (id,dispatch_id,sender,kind,recipient,body,instructions,status,
                attempts,next_attempt_ms,created_ms)
               VALUES ('held_delivery','held_delivery_dispatch','manager','dispatch_to',
                       'developer','wake','run','pending',0,0,0)"#,
        )
        .execute(store.pool())
        .await?;
        assert_eq!(store.due_outbox(10).await?.len(), 3);
        assert_eq!(store.due_deliveries(10).await?.len(), 1);

        let disabled = store
            .set_role_messaging(
                "developer",
                false,
                "manager",
                "hold without deleting",
                false,
            )
            .await?;
        assert_eq!(disabled["cancelled_outbox_items"], json!(0));
        assert_eq!(disabled["cancelled_delivery_items"], json!(0));
        assert!(
            !store
                .outbox_delivery_eligible("audit_from_developer")
                .await?
        );
        assert!(!store.outbox_delivery_eligible("audit_to_developer").await?);
        assert!(store.outbox_delivery_eligible("audit_to_lead").await?);
        let due_while_disabled = store.due_outbox(10).await?;
        assert_eq!(due_while_disabled.len(), 1);
        assert_eq!(due_while_disabled[0].id, "audit_to_lead");
        let pending: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox WHERE status='pending'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(pending, 3, "clear_queue=false must preserve held items");
        assert!(store.due_deliveries(10).await?.is_empty());

        store
            .set_role_messaging("developer", true, "manager", "resume", false)
            .await?;
        assert_eq!(store.due_outbox(10).await?.len(), 3);
        assert_eq!(store.due_deliveries(10).await?.len(), 1);

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn refuses_a_database_from_a_newer_schema() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-future-schema-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await?;
        sqlx::query(&format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1))
            .execute(&pool)
            .await?;
        pool.close().await;

        let result = Store::connect_path(&path, 1, Duration::from_secs(2)).await;
        assert!(result.is_err());
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn idempotency_key_is_released_after_failed_dispatch() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-failed-key-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let targets = vec!["developer".to_string()];

        // Definitively failed dispatch releases the key: the retry re-executes.
        assert!(matches!(
            store
                .reserve_dispatch(
                    "t_1",
                    "manager",
                    "dispatch_to",
                    &targets,
                    Some("stable-key"),
                    "fingerprint",
                    100,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Reserved
        ));
        store
            .finish_dispatch("t_1", "failed", &json!({"ok": false}), None)
            .await?;
        assert!(matches!(
            store
                .reserve_dispatch(
                    "t_2",
                    "manager",
                    "dispatch_to",
                    &targets,
                    Some("stable-key"),
                    "fingerprint",
                    100,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Reserved
        ));

        // An accepted result still replays forever.
        store
            .finish_dispatch("t_2", "accepted", &json!({"ok": true}), None)
            .await?;
        assert!(matches!(
            store
                .reserve_dispatch(
                    "t_3",
                    "manager",
                    "dispatch_to",
                    &targets,
                    Some("stable-key"),
                    "fingerprint",
                    100,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Existing(_)
        ));

        // A non-failed row with different arguments is still a conflict.
        assert!(matches!(
            store
                .reserve_dispatch(
                    "t_4",
                    "manager",
                    "dispatch_to",
                    &targets,
                    Some("stable-key"),
                    "other-fingerprint",
                    100,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Conflict
        ));

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn finish_dispatch_rejects_already_finalized_rows() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-finish-guard-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let targets = vec!["developer".to_string()];

        assert!(matches!(
            store
                .reserve_dispatch(
                    "g_1",
                    "manager",
                    "dispatch_to",
                    &targets,
                    None,
                    "fingerprint",
                    100,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Reserved
        ));
        store
            .finish_dispatch("g_1", "accepted", &json!({"ok": true}), None)
            .await?;
        // A second finish must not overwrite the persisted result.
        assert!(
            store
                .finish_dispatch("g_1", "accepted", &json!({"ok": true}), None)
                .await
                .is_err()
        );
        // Unknown ids are rejected as before.
        assert!(
            store
                .finish_dispatch("g_missing", "accepted", &json!({"ok": true}), None)
                .await
                .is_err()
        );

        // An indeterminate (recovered) row is also final.
        assert!(matches!(
            store
                .reserve_dispatch(
                    "g_2",
                    "manager",
                    "dispatch_to",
                    &targets,
                    None,
                    "fingerprint",
                    100,
                    Duration::from_secs(60),
                )
                .await?,
            Reservation::Reserved
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        store.recover_stale_pending(Duration::ZERO).await?;
        assert!(
            store
                .finish_dispatch("g_2", "accepted", &json!({"ok": true}), None)
                .await
                .is_err()
        );

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn activity_snapshot_filters_by_caller_visibility() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-snapshot-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let now = chrono::Utc::now();
        // 'started' is 10s old: active under a 1h stale window, stale under 1s.
        let started_at = (now - chrono::Duration::seconds(10)).to_rfc3339();
        store
            .record_activity(ActivityRecord {
                sender: "developer",
                targets: &["lead-developer".to_string()],
                event: "started",
                turn_id: "turn_dev",
                detail: "working",
                occurred_at: Some(&started_at),
                clock_skew: Duration::from_secs(60),
            })
            .await?;
        store
            .record_activity(ActivityRecord {
                sender: "designer",
                targets: &["lead-developer".to_string()],
                event: "completed",
                turn_id: "turn_design",
                detail: "done",
                occurred_at: Some(&now.to_rfc3339()),
                clock_skew: Duration::from_secs(60),
            })
            .await?;

        // Caller may only see 'developer' events.
        let snapshot = store
            .activity_snapshot(
                "lead-developer",
                &["developer".to_string()],
                true,
                10,
                Duration::from_secs(3600),
            )
            .await?;
        assert_eq!(snapshot["caller"], json!("lead-developer"));
        let current = snapshot["current"].as_array().unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0]["sender"], json!("developer"));
        assert_eq!(current[0]["event"], json!("started"));
        assert_eq!(snapshot["active"], json!([current[0].clone()]));
        let recent = snapshot["recent"].as_array().unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0]["sender"], json!("developer"));

        // A stale 'started' is not reported as active.
        let stale_snapshot = store
            .activity_snapshot(
                "lead-developer",
                &["developer".to_string()],
                true,
                10,
                Duration::from_secs(1),
            )
            .await?;
        assert_eq!(stale_snapshot["active"], json!([]));

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn mark_outbox_failed_applies_backoff_and_dead() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-outbox-backoff-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        store
            .reserve_dispatch(
                "o_1",
                "manager",
                "dispatch_to",
                &["developer".to_string()],
                None,
                "fingerprint",
                100,
                Duration::from_secs(60),
            )
            .await?;
        store
            .finish_dispatch(
                "o_1",
                "accepted",
                &json!({"ok": true}),
                Some(AuditMessage {
                    id: "audit_o1".to_string(),
                    sender: "manager".to_string(),
                    event: "ORDER".to_string(),
                    recipients: "developer".to_string(),
                    text: "task".to_string(),
                    chat_id: None,
                    reply_to_message_id: None,
                    thread_id: None,
                    media: vec![],
                }),
            )
            .await?;

        // Exponential backoff: attempts=1 -> next_attempt = now + 2s.
        store
            .mark_outbox_failed("audit_o1", 1, 5, "boom", None)
            .await?;
        let row: (String, i64, i64, String) = sqlx::query_as(
            "SELECT status, attempts, next_attempt_ms, last_error FROM telegram_outbox WHERE id='audit_o1'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(row.0, "pending");
        assert_eq!(row.1, 1);
        assert_eq!(row.3, "boom");
        let now = chrono::Utc::now().timestamp_millis();
        assert!(
            (now + 1_000..=now + 3_000).contains(&row.2),
            "backoff ~2s, got {}",
            row.2
        );

        // Retry-After override wins over backoff.
        store
            .mark_outbox_failed("audit_o1", 2, 5, "slow down", Some(30))
            .await?;
        let retry_after: i64 =
            sqlx::query_scalar("SELECT next_attempt_ms FROM telegram_outbox WHERE id='audit_o1'")
                .fetch_one(store.pool())
                .await?;
        assert!((now + 29_000..=now + 31_000).contains(&retry_after));

        // Terminal attempt -> dead.
        store
            .mark_outbox_failed("audit_o1", 5, 5, "gave up", None)
            .await?;
        let status: String =
            sqlx::query_scalar("SELECT status FROM telegram_outbox WHERE id='audit_o1'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "dead");

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_enforces_retention_and_recovers_stale_pending() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-cleanup-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let now = chrono::Utc::now().timestamp_millis();
        let old = now - 100 * 86_400_000;

        sqlx::query(
            "INSERT INTO activity_events(sender,target,event,event_rank,turn_id,detail,occurred_ms,received_ms)
             VALUES ('developer','lead-developer','completed',2,'old_turn','x',?,?)",
        )
        .bind(old)
        .bind(old)
        .execute(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO dispatches(id,sender,kind,fingerprint,status,result_json,created_ms,updated_ms)
             VALUES ('old_d','manager','dispatch_to','fp','accepted',NULL,?,?)",
        )
        .bind(old)
        .bind(old)
        .execute(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO dispatches(id,sender,kind,fingerprint,status,result_json,created_ms,updated_ms)
             VALUES ('old_queued_d','manager','dispatch_to','queued-fp','accepted',NULL,?,?)",
        )
        .bind(old)
        .bind(old)
        .execute(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO delivery_outbox
             (id,dispatch_id,sender,kind,recipient,body,instructions,status,attempts,next_attempt_ms,last_error,created_ms)
             VALUES ('old_pending_delivery','old_queued_d','manager','dispatch_to','developer','wake','read SLC','pending',0,?,'role disabled',?)",
        )
        .bind(old)
        .bind(old)
        .execute(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO telegram_outbox(id,sender,event,recipients,text,status,attempts,next_chunk,next_attempt_ms,created_ms,delivered_ms)
             VALUES ('old_o','manager','ORDER','developer','x','delivered',1,0,?,?,?)",
        )
        .bind(old)
        .bind(old)
        .bind(old)
        .execute(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO dispatches(id,sender,kind,fingerprint,status,created_ms,updated_ms)
             VALUES ('stale_p','manager','dispatch_to','fp','pending',?,?)",
        )
        .bind(old)
        .bind(old)
        .execute(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO dispatches(id,sender,kind,fingerprint,status,created_ms,updated_ms)
             VALUES ('fresh_p','manager','dispatch_to','fp','pending',?,?)",
        )
        .bind(now)
        .bind(now)
        .execute(store.pool())
        .await?;

        store
            .cleanup(
                30,
                30,
                30,
                Duration::from_secs(60),
                Duration::from_secs(3600),
            )
            .await?;

        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM activity_events")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(events, 0, "old activity purged");
        let old_dispatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dispatches WHERE id='old_d'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(old_dispatch, 0, "old dispatch purged");
        let queued_dispatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dispatches WHERE id='old_queued_d'")
                .fetch_one(store.pool())
                .await?;
        let pending_delivery: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE id='old_pending_delivery'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(queued_dispatch, 1, "pending delivery retains its parent");
        assert_eq!(pending_delivery, 1, "pending delivery survives retention");
        let old_outbox: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_outbox WHERE id='old_o'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(old_outbox, 0, "delivered outbox purged");
        let stale_status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id='stale_p'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(stale_status, "indeterminate", "stale pending recovered");
        let fresh_status: String =
            sqlx::query_scalar("SELECT status FROM dispatches WHERE id='fresh_p'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(fresh_status, "pending", "fresh pending untouched");

        store
            .mark_delivery_delivered("old_pending_delivery", Some("run-after-outage"))
            .await?;
        store
            .cleanup(
                30,
                30,
                30,
                Duration::from_secs(60),
                Duration::from_secs(3600),
            )
            .await?;
        let released_dispatch: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dispatches WHERE id='old_queued_d'")
                .fetch_one(store.pool())
                .await?;
        let released_delivery: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM delivery_outbox WHERE id='old_pending_delivery'",
        )
        .fetch_one(store.pool())
        .await?;
        assert_eq!(released_dispatch, 0, "terminal queue parent may be purged");
        assert_eq!(released_delivery, 0, "terminal delivery follows its parent");

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn due_outbox_deliveries_are_created_in_order_and_honor_limit() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-due-outbox-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let now = chrono::Utc::now().timestamp_millis();
        for (id, created, due) in [
            ("due_a", 1_i64, now - 1),
            ("due_b", 2_i64, now - 1),
            ("not_due", 3_i64, now + 60_000),
        ] {
            sqlx::query(
                "INSERT INTO telegram_outbox(id,sender,event,recipients,text,status,attempts,next_chunk,next_attempt_ms,created_ms)
                 VALUES (?, 'manager', 'ORDER', 'developer', 'x', 'pending', 0, 0, ?, ?)",
            )
            .bind(id)
            .bind(due)
            .bind(created)
            .execute(store.pool())
            .await?;
        }

        let limited = store.due_outbox(2).await?;
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].id, "due_a");
        assert_eq!(limited[1].id, "due_b");
        let all = store.due_outbox(10).await?;
        assert_eq!(all.len(), 2, "not-due item is excluded");

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn recent_operations_sender_view_keeps_all_targets() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-sender-view-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        sqlx::query(
            "INSERT INTO dispatches
             (id,sender,kind,fingerprint,status,result_json,created_ms,updated_ms)
             VALUES ('broadcast_s','manager','dispatch_all','fp','partial',?,2,2)",
        )
        .bind(
            json!({
                "ok": false,
                "results": {
                    "developer": {"run_id": "run-dev"},
                    "lead-developer": {"error": "nope", "status": "failed"}
                }
            })
            .to_string(),
        )
        .execute(store.pool())
        .await?;
        for target in ["developer", "lead-developer"] {
            sqlx::query(
                "INSERT INTO dispatch_targets(dispatch_id,target) VALUES ('broadcast_s',?)",
            )
            .bind(target)
            .execute(store.pool())
            .await?;
        }

        let sender_view = store.recent_operations("manager", 10).await?;
        let operation = sender_view["operations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|operation| operation["id"] == "broadcast_s")
            .expect("sender sees the broadcast");
        assert_eq!(
            operation["targets"],
            json!(["developer", "lead-developer"]),
            "sender view keeps all targets"
        );
        assert_eq!(
            operation["result"]["results"]["lead-developer"]["status"],
            json!("failed")
        );

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn parallel_reservations_share_one_rate_slot() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-parallel-reserve-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let targets = vec!["developer".to_string()];
        let (first, second) = tokio::join!(
            store.reserve_dispatch(
                "p_1",
                "manager",
                "dispatch_to",
                &targets,
                None,
                "fp",
                1,
                Duration::from_secs(60),
            ),
            store.reserve_dispatch(
                "p_2",
                "manager",
                "dispatch_to",
                &targets,
                None,
                "fp",
                1,
                Duration::from_secs(60),
            ),
        );
        let outcomes = [first?, second?];
        let reserved = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Reservation::Reserved))
            .count();
        let limited = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Reservation::RateLimited { .. }))
            .count();
        assert_eq!(
            reserved, 1,
            "exactly one dispatch wins the single rate slot"
        );
        assert_eq!(limited, 1, "the other reservation is rate limited");

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn ready_obtains_and_rolls_back_a_write_lock() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-ready-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        store.ready().await?;
        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn record_activity_deduplicates_repeated_events() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-dedupe-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let now = chrono::Utc::now().to_rfc3339();
        let targets = vec!["lead-developer".to_string()];
        let record = || ActivityRecord {
            sender: "developer",
            targets: &targets,
            event: "completed",
            turn_id: "turn_x",
            detail: "done",
            occurred_at: Some(&now),
            clock_skew: Duration::from_secs(60),
        };
        let first = store.record_activity(record()).await?;
        assert_eq!(first.0["lead-developer"]["recorded"], json!(true));
        // A replayed hook event is deduplicated and only refreshes state.
        let second = store.record_activity(record()).await?;
        assert_eq!(second.0["lead-developer"]["recorded"], json!(false));
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM activity_events")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(events, 1, "unique(sender,target,event,turn_id) enforced");
        let state_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM activity_state")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(state_rows, 1);

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn activity_snapshot_with_empty_visibility_is_empty() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-empty-visibility-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        let now = chrono::Utc::now().to_rfc3339();
        store
            .record_activity(ActivityRecord {
                sender: "developer",
                targets: &["lead-developer".to_string()],
                event: "completed",
                turn_id: "turn_x",
                detail: "done",
                occurred_at: Some(&now),
                clock_skew: Duration::from_secs(60),
            })
            .await?;
        let snapshot = store
            .activity_snapshot("lead-developer", &[], true, 10, Duration::from_secs(3600))
            .await?;
        assert_eq!(snapshot["current"], json!([]));
        assert_eq!(snapshot["recent"], json!([]));
        assert_eq!(snapshot["active"], json!([]));

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn mark_dispatch_indeterminate_records_recovery_result() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-indeterminate-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        store
            .reserve_dispatch(
                "ind_1",
                "manager",
                "dispatch_to",
                &["developer".to_string()],
                None,
                "fp",
                100,
                Duration::from_secs(60),
            )
            .await?;
        store
            .mark_dispatch_indeterminate("ind_1", "persistence failed")
            .await?;
        let (status, result): (String, String) =
            sqlx::query_as("SELECT status, result_json FROM dispatches WHERE id='ind_1'")
                .fetch_one(store.pool())
                .await?;
        assert_eq!(status, "indeterminate");
        let result: Value = serde_json::from_str(&result)?;
        assert_eq!(result["recovery_required"], json!(true));
        assert_eq!(result["error"], json!("persistence failed"));
        // Already-finalized rows are a no-op.
        store.mark_dispatch_indeterminate("ind_1", "again").await?;

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    #[tokio::test]
    async fn outbox_transitions_reject_non_pending_items() -> anyhow::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "swarm-mcp-outbox-transitions-test-{}.db",
            Uuid::new_v4().simple()
        ));
        let store = Store::connect_path(&path, 2, Duration::from_secs(2)).await?;
        store
            .reserve_dispatch(
                "o_t",
                "manager",
                "dispatch_to",
                &["developer".to_string()],
                None,
                "fp",
                100,
                Duration::from_secs(60),
            )
            .await?;
        store
            .finish_dispatch(
                "o_t",
                "accepted",
                &json!({"ok": true}),
                Some(AuditMessage {
                    id: "audit_ot".to_string(),
                    sender: "manager".to_string(),
                    event: "ORDER".to_string(),
                    recipients: "developer".to_string(),
                    text: "task".to_string(),
                    chat_id: None,
                    reply_to_message_id: None,
                    thread_id: None,
                    media: vec![],
                }),
            )
            .await?;
        store.mark_outbox_delivered("audit_ot").await?;
        assert!(
            store.mark_outbox_chunk_sent("audit_ot", 1).await.is_err(),
            "chunk checkpoint on a delivered item is rejected"
        );

        store.pool.close().await;
        remove_sqlite_files(&path).await;
        Ok(())
    }

    async fn remove_sqlite_files(path: &Path) {
        for candidate in [
            path.to_path_buf(),
            path.with_extension("db-wal"),
            path.with_extension("db-shm"),
        ] {
            let _ = tokio::fs::remove_file(candidate).await;
        }
    }
}
