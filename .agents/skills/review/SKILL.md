---
name: review
description: Deep code review checklist for the swarm-mcp codebase. Use when asked to review the Rust code, audit security invariants, check correctness of dispatch/idempotency/Telegram semantics, or verify a change before merge.
---

# Review swarm-mcp

swarm-mcp is an authenticated, role-aware transport and wake adapter for an
agent swarm. SLC MCP owns every task, assignment, lineage link, task message,
status transition, progress report, and terminal report. A change is correct
only if it preserves that boundary and the transport invariants below.

## 1. Security invariants

- **Token isolation**: each role has one `mcp_token` (config.rs `agents` map). A token must only ever expose that role's catalog. `http.rs::role_auth` gates every MCP path; `http.rs::activity_auth` maps the token back to exactly one role via `AuthenticatedRole`. Never loosen this.
- **ACL enforcement**: `dispatch_to` is allowed only for targets in
  `dispatch_acl[sender]`; `dispatch_all` only for `global_authorities`;
  `msg_to`/`msg_all` may wake any other configured role; messaging controls
  apply only to executors and are available only to the configured manager.
  Runtime checks in `Dispatcher` remain the source of truth.
- **Domain boundary**: never resolve an SLC id, infer an issuer/assignee,
  enforce a task transition, store a report body as task truth, or derive a
  wake from task status. `correlation_id` and message text are opaque transport
  input. An SLC `delivery` object may be forwarded unchanged.
- **Secrets**: `Secret` redacts `Debug`. Bot tokens are embedded in Telegram request URLs — reqwest errors are mapped with `map_err(|_| ...)` and must never propagate the original error or URL (see `dispatch.rs::send_telegram`, `telegram.rs`). Keep it that way.
- **Host/Origin guard**: `validate_host_and_origin` middleware is global. Note `host_allowed` semantics: a configured host without a port matches ANY port (tested in `http.rs`). Origins are compared as scheme+host+port tuples; `"null"` is allowed only if explicitly configured.
- **Body limits**: `DefaultBodyLimit` plus per-request streaming caps (`start_run`, `validate_telegram_response`, `telegram_payload`). Never read unbounded bodies.
- **Rate limits**: per-role `RequestLimiter` (in-memory) for MCP/activity HTTP; persistent SQLite `rate_events` window in `reserve_dispatch` for tool calls. Both must stay bounded (config caps in `config.rs`).

## 2. Dispatch state machine

States: `pending` → `accepted | partial | failed | indeterminate` (recovery path `pending` → `indeterminate`).

- Every tool flow that starts downstream work: validate → `reserve_or_replay` → `start_run` → `finish`/`fail_*`. `finish` persists result + optional Telegram audit atomically (`store.rs::finish_dispatch`). Manager messaging controls do not start downstream work; their state change and queue cancellation must remain one SQLite transaction under the write gate.
- A definitive Hermes HTTP 429 is the one exception to immediate start: persist
  the opaque wake in `delivery_outbox`, finish the operation as accepted with
  `queued=true`, and let the delivery worker retry only the per-recipient FIFO
  head. The per-role delivery gate must prevent a newer request from overtaking
  an existing head. Never inspect SLC or derive task order here.
- Idempotency: same `(sender, kind, idempotency_key)` + fingerprint → replay `Existing`; different fingerprint → `Conflict`; unfinished → `Pending`. A definitively `failed` dispatch releases its key and a retry re-executes; `accepted`, `partial`, and `indeterminate` remain replayable because downstream side effects may exist. Do not change this silently (see REVIEW-PLAN.md).
- `indeterminate` means "downstream may have accepted" — recovery must never turn it back into `pending`.
- Broadcasts (`dispatch_all`, `msg_all`, `telegram_inbound`) aggregate per-target results into `accepted/partial/failed/indeterminate`; `partial` results must be durable and replayable.

## 3. Store & migrations

- `store.rs::migrate` is transactional, uses `PRAGMA user_version` (`SCHEMA_VERSION = 8`), refuses newer schemas, and migrates legacy Python tables (`*_python_legacy`) by rename+import. Any schema change: bump `SCHEMA_VERSION`, add a statement to `SCHEMA` or an idempotent `ALTER`/`has_column` guard, and keep it idempotent for fresh and existing DBs.
- `role_messaging` is fail-open only for an absent row (the normal pre-control default). Store read errors fail closed in dispatch. Disable/clear must cancel undelivered Telegram audits and role wakes involving the role in either direction; enabling must never revive `cancelled` rows. With `clear_queue=false`, preserved rows must be excluded from due batches while either endpoint is disabled and become eligible only after re-enable.
- `delivery_outbox.sequence` is the recipient FIFO order; wall-clock time and
  UUID lexical order must never choose the head. At most one due head per
  recipient may be returned in a worker snapshot.
- Activity ordering: `activity_state` updates only if `excluded.occurred_ms >` current (or equal with higher `event_rank`) — preserves deterministic turn order.
- `reservation_lock` (global mutex) serializes `reserve_dispatch` for rate-limit + idempotency atomicity — do not remove without replacing the atomicity argument.

## 4. Telegram semantics

- Outbox is at-least-once: chunk checkpoints (`next_chunk`) mean a crash can duplicate the last chunk. Documented behavior — do not "fix" it into exactly-once without a delivery-ack protocol.
- `mark_outbox_failed` applies `Retry-After` or exponential backoff `2^attempts` (capped), terminal at `max_attempts` → `dead`.
- A retryable delivery failure stops the current due-item snapshot and defers all pending rows sharing the affected bot transport. Permanent configuration/cursor/HTTP errors must not be retried. Every snapshotted row must be revalidated under the messaging read gate before network I/O so a completed manager disable cannot leak a stale delivery.
- Inbound (`telegram.rs`): offset is advanced only AFTER a successful `process_update`; errors must not advance the offset (at-least-once). `allowed_users`/`group_id` checks happen before any dispatch. `parse_command` maps underscores to hyphens and strips `@bot` suffixes.

## 5. MCP catalog consistency

- `mcp.rs::tools()`/`resources()` must stay in sync with `probe.rs::expected_tools()`/`expected_resources()` — `probe` fails the pipeline check if they diverge.
- Tool schemas enumerate allowed targets from ACL/peers. Generic delivery
  tools intentionally allow and ignore extra envelope fields so transport
  adapters and small models remain forward-compatible; security decisions use
  only the parsed, validated fields.
- `swarm://hierarchy`, `swarm://operations`, `swarm://outbox` are visible to every role; `swarm://activity` only to dispatch authorities; `swarm://executors` and `swarm://messaging` only to the manager. Visibility is enforced at the store query level (`recent_operations`, `recent_outbox`, `activity_snapshot` filter by caller) — keep enforcement in the store, not only in the handler.

## 6. Concurrency, lifecycle & observability

- In-flight dispatches bounded by `Semaphore` (`max_inflight_dispatches`); acquiring the permit has a timeout.
- Shutdown: `CancellationToken` shared by MCP services, Telegram outbox worker,
  role-delivery worker, Telegram inbound worker, and cleanup task; `serve()`
  awaits all workers after cancel.
- Logging is structured JSON (`tracing_subscriber`); use `error!`/`warn!`/`info!` with fields, never `println!`/`eprintln!` in the server.
- Dead code watchlist: `AppState::pool()` is used only by tests.

## 7. Docs

README.md and REVIEW.md must not reference `src/old_python/` (removed in "Removed old shit") and must describe the current Rust behavior (e.g., idempotency retry semantics, Telegram at-least-once).
