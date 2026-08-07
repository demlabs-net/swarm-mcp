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

const SCHEMA_VERSION: i64 = 2;

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

#[derive(Clone, Debug)]
pub struct AuditMessage {
    pub id: String,
    pub sender: String,
    pub event: String,
    pub recipients: String,
    pub text: String,
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
        let result = json!({
            "ok": false,
            "error": "server restarted before dispatch completion was persisted",
            "recovery_required": true,
        });
        sqlx::query(
            "UPDATE dispatches SET status='indeterminate', result_json=?, updated_ms=? WHERE status='pending'",
        )
        .bind(serde_json::to_string(&result)?)
        .bind(now)
        .execute(&self.pool)
        .await?;
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
        let current_rows = sqlx::query(
            r#"SELECT sender, event, turn_id, detail, occurred_ms, received_ms
               FROM activity_state WHERE target = ? ORDER BY occurred_ms DESC"#,
        )
        .bind(role)
        .fetch_all(&self.pool)
        .await?;
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
        let current = current_rows
            .iter()
            .filter(|row| allowed_senders.contains(&row.get::<String, _>("sender")))
            .map(|row| activity_json(row, now, stale_ms))
            .collect::<Vec<_>>();
        let recent = recent_rows
            .iter()
            .filter(|row| allowed_senders.contains(&row.get::<String, _>("sender")))
            .take(usize::try_from(history_limit).unwrap_or(usize::MAX))
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
        let _guard = self.reservation_lock.lock().await;
        let mut tx = self.pool.begin().await?;
        if let Some(key) = idempotency_key
            && let Some(row) = sqlx::query(
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

        let now = Utc::now().timestamp_millis();
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
            "UPDATE dispatches SET status = ?, result_json = ?, updated_ms = ? WHERE id = ?",
        )
        .bind(status)
        .bind(serde_json::to_string(result)?)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated != 1 {
            return Err(anyhow!("dispatch does not exist"));
        }
        if let Some(audit) = audit {
            insert_outbox(&mut tx, &audit, now).await?;
        }
        tx.commit().await?;
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
        if updated > 1 {
            return Err(anyhow!("multiple dispatches updated"));
        }
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
            operations.push(json!({
                "id": id,
                "sender": row.get::<String, _>("sender"),
                "kind": row.get::<String, _>("kind"),
                "targets": targets,
                "status": row.get::<String, _>("status"),
                "result": result,
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
        Ok(json!({"caller": role, "items": items}))
    }

    pub async fn due_outbox(&self, limit: i64) -> anyhow::Result<Vec<OutboxItem>> {
        let rows = sqlx::query(
            r#"SELECT id, sender, event, recipients, text, attempts, next_chunk
               FROM telegram_outbox
               WHERE status = 'pending' AND next_attempt_ms <= ?
               ORDER BY created_ms LIMIT ?"#,
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
            })
            .collect())
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
    ) -> anyhow::Result<()> {
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
        Ok(())
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
        sqlx::query("DELETE FROM dispatches WHERE status != 'pending' AND updated_ms < ?")
            .bind(now.saturating_sub(operation_retention_days.saturating_mul(86_400_000)))
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "DELETE FROM telegram_outbox WHERE status IN ('delivered','dead') AND created_ms < ?",
        )
        .bind(now.saturating_sub(outbox_retention_days.saturating_mul(86_400_000)))
        .execute(&self.pool)
        .await?;
        self.recover_stale_pending(pending_stale_after).await?;
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
           (id, sender, event, recipients, text, status, attempts, next_chunk, next_attempt_ms, created_ms)
           VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?)"#,
    )
    .bind(&audit.id)
    .bind(&audit.sender)
    .bind(&audit.event)
    .bind(&audit.recipients)
    .bind(&audit.text)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
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
         updated_ms INTEGER NOT NULL
       )"#,
    "CREATE UNIQUE INDEX IF NOT EXISTS dispatches_idempotency_idx ON dispatches(sender, kind, idempotency_key) WHERE idempotency_key IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS dispatches_sender_idx ON dispatches(sender, created_ms DESC)",
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
         delivered_ms INTEGER
       )"#,
    "CREATE INDEX IF NOT EXISTS telegram_outbox_due_idx ON telegram_outbox(status, next_attempt_ms)",
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
                    "task_1",
                    "manager",
                    "order",
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
            .finish_dispatch("task_1", "accepted", &json!({"ok": true}), None)
            .await?;
        assert!(matches!(
            store
                .reserve_dispatch(
                    "task_2",
                    "manager",
                    "order",
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
                    "task_3",
                    "manager",
                    "order",
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
             VALUES ('broadcast_1','manager','order_all','fp','partial',?,1,1)",
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
