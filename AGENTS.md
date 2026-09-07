# swarm-mcp

Authenticated, role-aware MCP transport and wake adapter for the Hermes agent
swarm. Task identity, assignment, lineage, conversations, status, and reports
belong exclusively to SLC MCP. Rust (axum + rmcp Streamable HTTP + SQLx/SQLite).

## Local verification (must be green before pushing)

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked --release
```

CI mirrors this in three sequential stages: `tests` → `quality` → `rustsec`
(cargo-audit). See `.agents/skills/verify/`.

## Skills

- `.agents/skills/verify/` — reproduce the CI checks locally, toolchain notes.
- `.agents/skills/review/` — security/correctness invariants checklist (use before merging changes).
- `.agents/skills/testing/` — test patterns, fixtures (`src/testutil.rs`), mock Hermes recipe.
- `.agents/skills/config-roles/` — roles, ACL, Telegram modes, config validation reference.

## Non-negotiables

- One bearer token per role; runtime ACL checks in the dispatcher are the source
  of truth (JSON Schema enums are defense in depth, not the check).
- Treat every `correlation_id` and delivery payload as opaque. Never add task
  lookup, assignment, status, report, or lineage semantics to this repository.
- Every mutation: durable reservation → downstream call → durable result.
  Idempotency keys replay `accepted`/`partial`/`indeterminate`; `failed`
  dispatches release the key (retry re-executes).
- A definitive Hermes busy response is not a task failure: the opaque wake is
  persisted in the per-recipient `delivery_outbox`, later deliveries cannot
  overtake its head, and the tool result is durably `accepted` with
  `queued=true`. Task readiness/order remains exclusively in SLC.
- Telegram and busy-recipient delivery outboxes are at-least-once;
  Telegram is chunked at `SWARM_TELEGRAM_MESSAGE_LIMIT`.
- No panics in request paths; `Secret` Debug stays redacted; bot tokens never
  reach logs (reqwest errors are mapped away).
- The DB is SQLite (WAL). Do not add a second engine unless it is feature-gated
  and optional — the SQL is deliberately sqlite-specific.
- Keep README.md / REVIEW.md / REVIEW-PLAN.md and the local review/testing
  skills in sync with behavior.

## Ops

- `swarm-mcp probe` / `activity-probe` verify a live deployment non-destructively.
- Compose takes the whole config from `env_file: ../swarm/.env` — do not add
  `environment:` overrides (empty `${VAR}` interpolation would override env_file).
