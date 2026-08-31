---
name: testing
description: How to write tests for the swarm-mcp Rust codebase. Use when adding or extending test coverage, writing unit tests for store/dispatch/http/mcp/telegram/probe, or mocking the downstream Hermes API or Telegram Bot API.
---

# Testing swarm-mcp

Tests live in `#[cfg(test)] mod tests` inside each module. The main dependencies
provide most fixtures (`tokio`, `sqlx`, `uuid`, `serde_json`); `tower` is the
small router-test dev dependency. All tests must run offline.

## Patterns by module

### store.rs (best covered module — follow this style)
- Create a temp DB: `std::env::temp_dir().join(format!("swarm-mcp-<name>-{}.db", Uuid::new_v4().simple()))`.
- Build a legacy schema with a raw `SqlitePoolOptions` when testing migrations (see `migrates_python_activity_and_enforces_idempotency`), then call `Store::connect_path(&path, max_connections, busy_timeout)` and assert on migrated state.
- Clean up with the `remove_sqlite_files` helper (`.db`, `-wal`, `-shm`).
- Async tests use `#[tokio::test]` and return `anyhow::Result<()>`.
- Direct SQL assertions via `store.pool()` + `sqlx::query_scalar` are the established idiom.

Coverage gaps to fill (as of REVIEW-PLAN.md): `activity_snapshot` visibility filtering, `cleanup` retention, `recover_stale_pending`, `mark_outbox_failed` backoff/dead transition, `due_outbox` ordering, `finish_dispatch` audit insertion, `recent_operations` sender view.

For `delivery_outbox`, assert insertion order through its integer `sequence`,
only one due FIFO head per recipient, pause/resume under
`clear_queue=false`, cancellation in both directions, and no direct Hermes
call for a later delivery while an earlier head is pending.

### dispatch.rs
- Pure functions are directly testable: `render_template`, `render_order_template`, `validate_identifier`/`validate_idempotency`, `telegram_chunks`, `fingerprint`, `parse_arguments` (bad/extra/unknown fields → `ToolOutcome` error), `telegram_chunks` unicode boundaries.
- Tool flows (`dispatch_to`, `msg_to`, `telegram_inbound`, broadcasts) call
  `start_run` → HTTP POST to `agent.api_url`. To test them without a real
  swarm, spin up a local mock Hermes server with axum (see below) and build a
  `Dispatcher` directly from a hand-made `Config` (fields are `pub`).
- Lock the domain boundary with tests: a correlation id remains opaque, the
  transport accepts the four-field delivery object emitted by SLC, and an
  executor can wake its SLC task issuer (including the manager) without a
  Swarm-side task lookup.
- `TelegramFailure`/`RunFailure` status classification (rejected vs indeterminate) is unit-testable via the store + mock server.
- Busy-Hermes tests return 429 once, assert the tool returns accepted
  `queued=true`, then flush the worker into a 202 response. A second wake for
  the same recipient must join the queue without another mock-server call.

Mock Hermes server sketch:
```rust
let router = axum::Router::new().route("/v1/runs", axum::routing::post(move |headers, body| async move {
    // assert Bearer token, model alias, instructions; return 202 {"run_id": "..."}
}));
let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
let addr = listener.local_addr()?;
tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
```
Then point `agent.api_url` at `format!("http://{addr}")`.

### http.rs
- Pure helpers already tested: `bearer_token`, `host_allowed`, `origin_allowed`, `RequestLimiter`.
- Middleware (`role_auth`, `activity_auth`, `validate_host_and_origin`) and the `activity` handler can be tested by building the `Router` exactly like `serve()` does with a test `Config`/`AppState` (needs a temp DB — use `Store::connect_path`), then `tower::ServiceExt::oneshot(request)` (axum `Router` implements `tower::Service`).
- Assert: wrong/absent tokens → 401 + `WWW-Authenticate: Bearer`; bad Host → 403; oversize body → 413; rate limit → 429; valid activity signal → 202 with `recorded` flags.

### mcp.rs (currently zero tests)
- `tools()`, `resources()`, `hierarchy()`, `instructions()` are pure given a `Config` + `Store` — build a `RoleMcp::new(state, role)` with a test config and assert the catalog for manager vs executor vs dispatch authority (targets enum, `dispatch_all` presence, `swarm://activity`/`swarm://executors` visibility, and no task/report tools).
- `call_tool` with unknown tool → `METHOD_NOT_FOUND`.

### telegram.rs
- `parse_command` is well covered; extend for: unknown command → help, `/all` with empty rest → help, `@other_bot` suffix, case normalization, `_`→`-`.
- `next_offset` overflow; `TelegramBacklogMode::Discard` initialization path (mock getUpdates via a local axum server returning `{"ok": true, "result": [...]}`).

### config.rs
- Only `validate_template` is tested. For `from_env` tests use unique env names and `std::env::set_var`/`remove_var` — but tests run in parallel, so either run those tests in one thread (`#[serial]`-style via a `static Mutex`) or refactor `from_env` to accept an env source (`fn from_env_with(&dyn Fn(&str) -> Option<String>)` is the cleanest and keeps parallelism). Prefer the refactor over global env mutation.

### probe.rs (currently zero tests)
- `expected_tools`/`expected_resources` are pure — parametrize over manager/executor/authority configs and assert exact sets; must match `mcp.rs` catalog generation.
- `catalog_probe`/`activity_probe` need a live server: reuse the mock-server pattern and a full `Config`.

## Config fixture helper

A hand-built `Config` (all fields `pub`) is the fastest fixture. Use
`BTreeMap::from` for `dispatch_acl`/`agents`/`activity_routes`, small
durations, `Secret::new` values, and `state_db_path` pointing at a temp file.
Remember config invariants only enforced in `from_env` are NOT re-checked in
struct literals — tests must construct valid states themselves.

## CI

`cargo test --locked --all-targets` currently runs 145 tests. Keep runtime of
the suite under a few seconds after compilation; store tests use real temporary
SQLite files and loopback mock servers only.
