# swarm-mcp: review findings & remediation plan

Date: 2026-08-14 · Review scope: full Rust codebase (src/, ~5400 LOC) at commit `c494788` (rebased `develop`).
Baseline: `cargo fmt` ✅ · `cargo test --locked --all-targets` ✅ (17 tests) · `cargo build --locked` ✅ ·
`cargo clippy --locked --all-targets -- -D warnings` ❌ (fails on toolchains ≥ 1.90, see C-01).

Severity: **[Crit]** blocks merge / production risk · **[High]** real defect or design gap · **[Med]** correctness/robustness ·
**[Low]** hygiene.

---

## Part 1 — Findings

### Correctness & behavior

| ID | Sev | Location | Finding | Proposed fix |
|---|---|---|---|---|
| B-01 | High | `store.rs:403-425` + `dispatch.rs:986-1044` | Idempotency replay returns the persisted result for **any** completed dispatch, including `failed`/`indeterminate`. A retry with the same `idempotency_key` after a failure replays the old error instead of re-executing. README.md ("reuse the same key when retrying") contradicts this behavior. | Decide semantics: (a) replay only `accepted`/`partial` results and allow re-execution after `failed` (delete/ignore failed rows), or (b) keep replay-all and fix README/instructions to say "failed keys are terminal — retry with a new key". Then unit-test the chosen contract. |
| B-02 | Med | `store.rs:481-508` | `finish_dispatch` has no status guard (`WHERE id = ?` only). A stray second call (e.g. from a future retry path or late callback after recovery) overwrites the persisted result. `mark_dispatch_indeterminate`'s `updated > 1` check is dead code (PK lookup). | Add `AND status = 'pending'`; on 0 rows return a distinguishable "already finalized" error; simplify the `> 1` guard. Test double-finish. |
| B-03 | Med | `config.rs` vs `dispatch.rs:1222` | Length units are inconsistent: `max_message_chars` counts **chars**, `validate_identifier` limits **160 bytes**, `clean_text` chars vs schema `maxLength` (JSON Schema counts code points — consistent, but byte-vs-char rules deserve a single documented policy). | Introduce one `text_policy.rs` helper set (char-count for messages, byte-count for identifiers) and reuse it in config validation, schemas, and runtime checks; add tests. |
| B-04 | Low | `http.rs:440-452` vs `dispatch.rs:1222-1233` | `validate_identifier` is duplicated; the `http.rs` copy ignores its `field` argument and hardcodes `"turn_id"` in the error message. | Extract one shared `validate_identifier(value, field)` (fix the error message to use `field`); delete the duplicate. |
| B-05 | Low | `store.rs:772` area / `telegram.rs:170-180` | `poll_once` `continue`s on `update_id < offset` inside a sorted loop — a `break` is correct and cheaper; the offset is re-validated on every cycle anyway. | `break` instead of `continue`; add a test with out-of-order updates. |

### Performance & scale

| ID | Sev | Location | Finding | Proposed fix |
|---|---|---|---|---|
| P-01 | Med | `store.rs:430-432, 753-756` | `DELETE FROM rate_events WHERE created_ms < ?` and the `rate_events` count run a full scan (`rate_events_actor_idx` indexes `(actor, created_ms)`, not `created_ms`). On every reservation this grows with table size. | Add `CREATE INDEX rate_events_created_idx ON rate_events(created_ms)` (schema bump v3→v4 with `has_column`-style guard or `CREATE INDEX IF NOT EXISTS` in SCHEMA). |
| P-02 | Med | `store.rs:337-360` | `activity_snapshot` fetches **all** `activity_state` rows for the target and filters by `allowed_senders` in Rust; the `activity_events` query is filtered in SQL but filtered again in memory (and `.take()` re-applies the LIMIT). | Push `sender IN (...)` into the `activity_state` query too; drop the redundant in-memory filters; keep the `LIMIT` in SQL only. Add a test asserting exact row visibility. |
| P-03 | Low | `store.rs:554-596` | `recent_operations` runs one `dispatch_targets` query per row (N+1) and the `d.sender = ? OR t.target = ?` predicate defeats the `(sender, created_ms)` index. | Acceptable at current scale — note in code comment; if it ever matters, switch to a single JOIN with `GROUP BY` or window function. |
| P-04 | Low | `store.rs:390-401` | Global `reservation_lock` serializes all reservations (idempotency + rate limit atomicity). Correct, but a throughput ceiling and a single point of contention. | Keep for now; document the trade-off. If needed later: per-actor keyed locks + SQLite `BEGIN IMMEDIATE` re-check. |

### Toolchain & CI

| ID | Sev | Location | Finding | Proposed fix |
|---|---|---|---|---|
| C-01 | Med | `dispatch.rs:1292`, `Cargo.toml` | `cargo clippy -D warnings` **fails locally** (toolchain 1.97): `clippy::implicit_clone` on `header.to_string()`. CI passes because the image is pinned to `rust:1.89` where the lint doesn't exist — local and CI are verifying different things. | Fix `header.to_string()` → `header.clone()`; add `rust-toolchain.toml` pinning `1.89` (channel + `components: [clippy, rustfmt]`) so local == CI; optionally pin the image digest in the file too. |
| C-02 | Low | `.gitlab-ci.yml` | No coverage reporting; `rustsec` installs `cargo-audit` on every run (~1-2 min). | Add a `coverage` job (`cargo llvm-cov` or `tarpaulin`) with a threshold once coverage grows; consider caching the auditer binary. |

### Documentation & dead code

| ID | Sev | Location | Finding | Proposed fix |
|---|---|---|---|---|
| D-01 | Med | `README.md:8`, `REVIEW.md:16` | Both docs claim the Python implementation is preserved under `src/old_python/` — the directory was removed by the "Removed old shit" commit. | Rewrite those sentences (say the Python code was archived/removed and review it via git history). |
| D-02 | Low | `config.rs:715-720` | `Config::role_for_path` is never used. | Remove it (or wire it into `http.rs` path→role mapping if needed later). |
| D-03 | Low | `lib.rs:39-42` | `AppState::pool()` is used only by tests. | Keep (documented convenience) or gate with `#[cfg(test)]`. |
| D-04 | Low | `dispatch.rs:87-97, 1229` | `TelegramFailure`/`RunFailure` duplicate error-shaping logic across four tool flows (`order`, `report`, `msg_to`, `message_all`, `telegram_inbound`) — the broadcast aggregation block is copy-pasted 3×. | Extract a `broadcast_result()` helper (aggregation + status computation + audit); behavior unchanged, covered by new dispatch tests. |

### Security review notes (verified OK — do not regress)

- Token isolation: per-role bearer check before routing; cross-role probe exists (`probe`). ✅
- Constant-time token comparison (`subtle::ConstantTimeEq`); length leak is acceptable (uniform format). ✅
- Telegram bot tokens embedded in request URLs never reach logs/errors (reqwest errors are discarded via `map_err`). ✅
- Host/Origin guard is global, body limits enforced at every boundary. ✅
- Rate limits applied after authentication (unauthenticated floods are rejected without consuming the limiter). ✅
- ACL enforced at runtime in the dispatcher, not only in the JSON schema. ✅

---

## Part 2 — Test coverage plan

Current: 17 tests (config 1, http 4, dispatch 6, telegram 3, store 3; **0** in mcp, probe, main, lib). CI gate: `cargo test --locked --all-targets`.

### T-1 Store (`store.rs`) — target +8 tests
1. `activity_snapshot` visibility: sender not in `allowed_senders` is excluded from `current` and `recent`; `active` excludes stale `started`; history limit honored.
2. `record_activity` ordering: late (older `occurred_at`) terminal event does not overwrite a newer turn in `activity_state`; duplicate `(sender,target,event,turn_id)` is ignored (count unchanged).
3. `cleanup`: retention deletes old events/dispatches/outbox, keeps fresh ones; `recover_stale_pending` marks only stale pending → `indeterminate`.
4. `mark_outbox_failed`: backoff schedule (`2^attempts` capped), `retry_after_seconds` override, terminal `dead` at `max_attempts`, `last_error` truncated to 500 chars.
5. `due_outbox`: ordering by `created_ms`, limit honored, only `pending` + due items returned.
6. `finish_dispatch`: persists result + inserts outbox audit in one transaction; second finish (B-02 fix) rejected; unknown id → error.
7. `recent_operations`: sender view shows all targets + full result; recipient view shows only own target + projected result; `recover_pending_dispatches` startup behavior.
8. `reserve_dispatch`: idempotency `Conflict` on fingerprint mismatch, `Pending` when result not yet persisted, `RateLimited` retry-after math.

### T-2 Dispatch (`dispatch.rs`) — target +6 tests (mock Hermes server)
Mock pattern: local axum server on ephemeral port asserting `Authorization: Bearer`, `model` alias, `input`/`instructions`; returns `202 {"run_id": ...}` (or 500/400 to exercise `indeterminate`/`rejected`).
1. `order` happy path: accepted result + ledger row + audit outbox row (telegram enabled).
2. `order` unauthorized target → error, no dispatch row; empty/oversized `command` → error; template render failure → `failed`.
3. `order_all` aggregation: one target 500 → `partial` with per-target statuses; all fail → `failed`; all 500 → `indeterminate`.
4. Idempotency: same key + same fingerprint replays `Existing` with `deduplicated: true`; different fingerprint → `Conflict`; B-01 chosen semantics.
5. `report` ACL: non-supervisor recipient rejected; valid supervisor accepted; invalid `status` rejected.
6. `telegram_inbound`: dedupe by update_id; invalid identifiers rejected; disabled feature → error.

### T-3 HTTP (`http.rs`) — target +5 tests
Build the router as `serve()` does (test `Config` + temp store), exercise with `tower::ServiceExt::oneshot`:
1. Role endpoint: wrong token → 401 + `WWW-Authenticate: Bearer`; correct token → 200.
2. Cross-role: role A token against role B path → 401.
3. `validate_host_and_origin`: missing Host → 400; disallowed Host → 403; disallowed Origin → 403; allowed → passes.
4. `activity` handler: invalid event / turn_id / occurred_at → 422; oversize detail → 413; disabled mode → 202 `recorded: false`; valid → 202 with per-target `recorded`.
5. Rate limiting: role endpoint returns 429 after `mcp_request_rate_limit` (use a tiny window in the test config).

### T-4 MCP (`mcp.rs`) — target +4 tests (currently zero)
1. Manager catalog: tools `{order, order_all}`, resources incl. `swarm://executors` + `swarm://activity`.
2. Executor catalog: tools `{report, msg_to, msg_all}` only; no `order_all`; `order` schema enumerates exactly the ACL targets.
3. `hierarchy()` JSON: `caller`, `may_order`, `caller_supervisors`, safeguards map.
4. `call_tool` with unknown tool → `METHOD_NOT_FOUND`; `read_resource` with unknown uri → resource_not_found.

### T-5 Telegram (`telegram.rs`) — target +3 tests
1. `parse_command`: unknown command → help; `/all` with empty text → help; `@other_bot` suffix stripped; case/`_` normalization.
2. `next_offset` overflow → error.
3. `poll_once` first-run with `Discard` backlog: mock `getUpdates` returns `[{"update_id": 5, ...}]` → offset fast-forwarded to 6, no dispatch; with `Process` → dispatches and advances.

### T-6 Config (`config.rs`) — target +2 suites
1. Refactor `from_env` to `from_env_with(&dyn Fn(&str) -> Option<String>)` (keeps tests parallel-safe; `from_env` delegates). Then test the validation matrix: duplicate roles, manager-in-executors, missing ACL grant for manager, bad templates (missing/unknown placeholders), conflicting paths, short tokens, bad origins/hosts, per-role vs shared telegram mode gaps.
2. Keep the pure `validate_template` tests; add `validate_path`/`parse_http_url`/`normalize_targets` cases.

### T-7 Probe (`probe.rs`) — target +2 tests (currently zero)
1. `expected_tools`/`expected_resources` parametrized over manager / authority / plain executor configs — must mirror `mcp.rs` (guards drift).
2. `activity_probe` skip path when no activity routes configured.

### T-8 Integration (optional, stretch)
Boot the full server (`http::serve`) on an ephemeral port with a test config + mock Hermes, run `catalog_probe` against it, assert `ok: true` — replaces the "needs live deployment" gap F-22 referenced. Best placed as an ignored-by-default test or a CI job with a running container.

---

## Part 3 — Execution order

**Phase 0 — Quick wins (< 1 h, safe, no behavior change)**
1. C-01: fix `implicit_clone`; add `rust-toolchain.toml` (pin 1.89).
2. D-01: fix README.md / REVIEW.md `old_python` references.
3. D-02/D-04: remove dead `role_for_path`; extract the broadcast helper (with tests from T-2.3 to lock behavior).
4. Run full verify skill suite; everything green on 1.89 and current toolchain.

**Phase 1 — Behavior decisions (needs product owner)**
5. B-01: pick idempotency-retry semantics, implement, update README + MCP instructions + tests (T-2.4).
6. B-02: `finish_dispatch` status guard + test (T-1.6).

**Phase 2 — Test coverage build-out (largest chunk)**
7. T-1 store suite (8 tests) → 8. T-3 http suite (5) → 9. T-4 mcp suite (4) → 10. T-2 dispatch suite (6) → 11. T-5 telegram (3) → 12. T-7 probe (2) → 13. T-6 config refactor + matrix (2 suites).
   After each step: `cargo test` + `cargo clippy` green. Target: **~47 tests**.

**Phase 3 — Hardening**
14. P-01/P-02: indexes + snapshot query pushdown (schema v4 — bump `SCHEMA_VERSION`, guard `ALTER`/index creation, add migration test).
15. B-03: shared text-policy helper; align schema/runtime/config units.
16. P-03/P-04: comments documenting the accepted trade-offs (or fixes if measurements demand).

**Phase 4 — CI**
17. C-02: coverage job with threshold (e.g. ≥ 50% line, then raise); keep `-D warnings`.
18. T-8 optional e2e job.

Acceptance: `verify` skill passes locally AND in CI; coverage ≥ 50% line (from ~20% today); REVIEW.md gains a "Rust 0.2.1 hardening" section referencing this plan's disposition table.

---

## Artifacts

- `.agents/skills/verify/SKILL.md` — reproduce CI checks locally.
- `.agents/skills/review/SKILL.md` — invariants checklist for future reviews.
- `.agents/skills/testing/SKILL.md` — test patterns & mock-server recipes.
- `.agents/skills/config-roles/SKILL.md` — role/config operations reference.

---

# Iteration 2 (2026-08-14)

Scope: deployment config (compose commits `59444f6`/`52cfb5e`), Dockerfile/`.dockerignore`, CI fix verification,
deeper checks of `http.rs` routing and edge cases. Baseline unchanged: fmt ✅ clippy ✅ (after C-01 fix) tests ✅ (17).

## Resolved since iteration 1

| ID | Disposition |
|---|---|
| C-01 | **Fixed & pushed** — `header.to_string()` → `header.clone()`; clippy now passes on 1.97 and 1.89. |
| CI config | **Fixed & pushed** — `variables` moved out of `default:` (GitLab rejected the pipeline: "default config contains unknown keys: variables"); dedicated `tests` job in its own stage (`test` → `verify` → `audit`, sequential so three cargo builds never race on one runner's `target/` cache); `.cargo/bin/` added to cache (cargo-audit + rustup shims persist). Pushed as `4707b06` + `458107a`. |
| D-02, D-03 | Not yet addressed (still open, Phase 0). |

## New findings

| ID | Sev | Location | Finding | Proposed fix |
|---|---|---|---|---|
| DEP-1 | Med | `docker-compose.yml:9-101` | `environment:` block uses `${VAR}` interpolation, which reads the **shell / root `.env` only — never `env_file: ../swarm/.env`** (Compose precedence: `environment:` > `env_file:`). Any var listed in `environment:` but missing from the shell interpolates to **empty and overrides the env_file value** → `Config::from_env` fails at startup with "…must be configured". So the env_file addition only works when the shell/root `.env` already defines every var — otherwise it silently defeats itself. | Drop the `environment:` mapping for vars that come from `env_file` (keep only the `:-default` ones: `SWARM_MCP_MEMORY_LIMIT`, `SWARM_MCP_CPU_LIMIT`, `SWARM_MCP_PIDS_LIMIT`, `SWARM_LOG_MAX_SIZE`, `SWARM_LOG_MAX_FILES`), or run compose with `--env-file ../swarm/.env`. |
| DEP-2 | Low | `docker-compose.yml:74-101` | The `environment:` allowlist hardcodes 7 roles (`MANAGER_`, `DEVELOPER_`, `JUNIOR_`, `DESIGNER_`, `LEAD_DEVELOPER_`, `TESTER_`, `DEVOPS_`) — contradicts the commit goal "роли любого роя без правки allowlist". With `env_file` in place the allowlist is redundant. | Removed as part of the DEP-1 fix; update `.agents/skills/config-roles/SKILL.md` step 4 accordingly. |
| DEP-3 | Low | `.dockerignore` | Stale `/src/old_python` entry (directory removed long ago). | Delete the line. |
| DEP-4 | Info | `docker-compose.yml:118` | `network_mode: host` — HEALTHCHECK (`127.0.0.1:$SWARM_MCP_PORT/ready`) and webhook agents work only if `SWARM_MCP_PORT` is free on the host; `container_name`/`networks` are inert under host mode (already removed). `tmpfs /tmp` covers read-only root. Verified consistent. | No action; document in config-roles skill. |
| DEP-5 | Info | `Dockerfile` | `EXPOSE 3004` is informational (host mode ignores it); HEALTHCHECK subcommand needs only `SWARM_MCP_PORT`. Verified consistent. | No action. |
| V-1 | Info | `src/http.rs:174-181` | `.with_state::<()>(state.clone())` relies on axum's state-baking semantics: per-role routers carry their own middleware state (`RoleAuth`/`ActivityAuth`), the outer `Arc<AppState>` is baked into route handlers, and the final router state is `()`. Compiles, runs, and the deployment `probe` exercises it end-to-end — verified working, but non-obvious. | Add a short comment explaining the construction (guards against a future refactor removing `with_state`). |
| B-06 | Low | `dispatch.rs:561-657` | `message_all` with a single-executor swarm (sender has no peers → `targets == []`) returns `ok: true`, status `accepted`, empty results — edge case, arguably should be a no-op error. `order_all` cannot hit this (manager is never an executor). | Document or reject; add a unit test locking the chosen behavior. |

## Reconfirmed (no change)

- **B-01** (idempotency replay of `failed` dispatches) — re-read the flow: `reserve_dispatch` returns `result_json` for **any** persisted status, so a retry with the same key after a failure replays the old error with `deduplicated: true`; re-execution is impossible. The README/instructions wording still needs to match the chosen semantics.
- **P-01** — `rate_events`/`activity_events`/`dispatches` cleanup deletes scan without a `created_ms`/`updated_ms` index (`rate_events_actor_idx(actor, created_ms)` doesn't help `WHERE created_ms < ?`).
- **P-02** — `activity_snapshot` still filters `activity_state` rows in memory.
- B-02, B-03, B-04, B-05, P-03, P-04, D-01, C-02 — unchanged, see Part 1.
- No panics/`unwrap` in request paths (only `expect()` on config-validated invariants, `unwrap_or` fallbacks). ✅

## Revised execution order

**Phase 0 — Quick wins** (updated)
1. ✅ C-01 (clippy) + CI yaml restructure — **done, pushed**.
2. D-01: fix README.md / REVIEW.md `old_python` references.
3. DEP-1 + DEP-2: drop redundant `environment:` mapping (keep `:-default` entries only); update `config-roles` skill.
4. DEP-3: remove stale `.dockerignore` entry.
5. D-02/D-04: remove dead `role_for_path`; extract the broadcast helper (with T-2.3 tests).
6. V-1: comment the `with_state::<()>` construction; B-06: decide + test `message_all` empty-peers edge.

Phases 1–4 unchanged from Part 3.
