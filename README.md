# Swarm MCP

Swarm MCP is the authenticated communication and authorization plane for the
Hermes development swarm. Version 0.2 is implemented in Rust and exposes a
separate Streamable HTTP MCP endpoint for every role. A bearer token grants one
role catalog only; it cannot be reused against another role's endpoint.

The retired Python implementation was removed from the tree; the migration
review of it lives in [REVIEW.md](REVIEW.md) (the code itself is preserved in
git history under `src/swarm_mcp/`). It is not part of the deployed service,
and the Python `requirements.txt` has been removed.

The detailed review of the retired implementation, completed fixes, known
limits, and capability roadmap is in [REVIEW.md](REVIEW.md).

## Role hierarchy

The hierarchy is data-driven:

```dotenv
SWARM_AGENT_ROLES=developer,junior,designer,lead-developer,tester,devops
SWARM_MANAGER_ROLE=manager
SWARM_ORDER_ACL={"manager":["*"],"lead-developer":["developer","junior"]}
```

With this configuration the catalogs are:

| Caller | Tools |
|---|---|
| `manager` | `order`, `order_all`, `messaging_disable`, `messaging_enable`, `messaging_clear_queue` |
| `lead-developer` | `order`, `report`, `msg_to`, `msg_all` |
| `developer`, `junior`, `designer`, `tester`, `devops` | `report`, `msg_to`, `msg_all` |

`"*"` grants swarm-wide `order` and `order_all`. An explicit list grants only
single-target `order`; the generated JSON Schema enumerates exactly the targets
allowed to that caller. Reverse ACL lookup determines the valid `report`
recipients. Executors may coordinate only with other executors.

Every dispatch tool accepts an optional `idempotency_key`. Callers should reuse
the same key when retrying the same logical action. Reusing a key with different
arguments is rejected. A dispatch that **definitively failed** (the agent API
rejected it, nothing was started) releases its key: retrying with the same key
re-executes. `accepted`, `partial`, and `indeterminate` results replay — an
`indeterminate` result means the downstream may have accepted the operation, so
do not re-run it; inspect `swarm://operations` first. Keys must not be recycled
for unrelated work; their records expire with `SWARM_OPERATION_RETENTION_DAYS`.
Independently of caller keys, the server suppresses a content-identical
sender/kind/target dispatch inside `SWARM_DUPLICATE_WINDOW_SECONDS`. Whitespace
differences do not bypass this circuit breaker. A genuinely new correction must
contain new evidence or a changed action.

Starting with 0.3.0, `report`, `msg_to`, and `msg_all` require the originating
`task_id`. This keeps
progress, handoffs, and peer coordination attached to one work item; untracked
chat cannot wake another Hermes run. The server verifies that the ID belongs to
an accepted `order`/`order_all` which actually targeted the sending role, so a
model cannot invent a fresh ID to evade duplicate suppression.

Progress reports are durable and Telegram-audited but do not automatically
start another supervisor run. Only statuses listed in
`SWARM_REPORT_WAKE_STATUSES` wake the supervisor; the production default is
`completed,blocked,failed`. This separates observation from orchestration and
prevents a stream of routine updates from recursively consuming manager turns.

## Role dispatch modes

Swarm MCP is a message bus: dispatches carry a message to a role, and roles
communicate back through the MCP server (`telegram_reply`, `report`,
`msg_to`, …). Tasks and knowledge live outside the bus (e.g. in SLC MCP); the
bus never queues work. How a dispatch reaches a role is per-role, configured
with `<ROLE>_API_KIND`:

| Mode | `<ROLE>_API_KIND` | Initiation | Answers | Required config |
|---|---|---|---|---|
| Hermes agent | `hermes` (default) | Swarm MCP calls the Hermes api_server (`POST /v1/runs`), polls the run and delivers its final `output` to the operator automatically | automatic + `telegram_reply` | `<ROLE>_API_URL`, `<ROLE>_AGENT_API_KEY` |
| OpenAI-compatible system | `openai` | Swarm MCP fires `POST {api_url}/v1/chat/completions` with `{message, instructions}` (bearer `<ROLE>_AGENT_API_KEY`); the system starts its own agent | through the swarm MCP server (`telegram_reply` at any point during the work) | `<ROLE>_API_URL`, `<ROLE>_AGENT_API_KEY`, optional `<ROLE>_API_MODEL` |
| MCP-only participant | `mcp` | none — the role initiates interaction with the roe by its own logic (its own schedule, tasks pulled from SLC, etc.); dispatches addressed to it are rejected with a clear error | proactive `telegram_reply` at any time | `<ROLE>_SWARM_MCP_TOKEN` only |

A roe can mix all three kinds freely: inbound routing (`/команды`, `@роль`,
plain text to the manager) picks the target role and dispatches it according to
that role's mode. Hermes results carry `run_id` in the dispatch result;
OpenAI initiation carries `initiated: true`.

`telegram_reply` is the shared answer channel for non-Hermes roles. It may be
called at any point during the work (intermediate updates and/or the final
answer). The target chat resolves as: the most recent Telegram inbound dispatch
of the calling role → the most recent Telegram inbound dispatch of any role
(the operator's chat) → the group. This lets MCP-only participants message the
operator proactively even before anyone has addressed them.

## Resources

| URI | Visibility | Purpose |
|---|---|---|
| `swarm://hierarchy` | every role | Effective roles, ACL, supervisors, tools, and safeguards |
| `swarm://executors` | manager | Compatibility alias for the hierarchy |
| `swarm://operations` | every role | Recent durable operations sent or received by that role, including the report summary visible to its sender and recipient |
| `swarm://activity` | ordering authorities | Current and recent passive subordinate lifecycle state |
| `swarm://outbox` | every role | Telegram delivery state; manager also sees the inbound offset and last successful poll |
| `swarm://messaging` | manager | Persistent executor circuit-breaker state and undelivered queue counts |

The activity endpoint records only lifecycle state. It never starts an agent,
changes cron configuration, or sends Telegram messages. Timestamps make
out-of-order delivery deterministic, repeated hook events are deduplicated, and
stale `started` records are not reported as active forever.

## Delivery and persistence

Before contacting an agent API, the server reserves the operation in SQLite.
The store provides:

- WAL mode and an asynchronous connection pool;
- a role-scoped operation ledger;
- idempotency conflict and replay handling;
- automatic recent-content duplicate suppression;
- per-role persistent rate limits and a global in-flight dispatch bound;
- explicit `accepted`, `partial`, `failed`, and `indeterminate` states;
- automatic import of the old Python activity tables;
- transactional, forward-version-checked schema migrations;
- configurable operation/activity/outbox retention;
- a durable Telegram outbox with bounded retries, `Retry-After` support, and
  per-chunk checkpoints.

The manager can atomically disable an executor's Swarm MCP send/receive path
and cancel pending/dead Telegram audit items sent by or addressed to it.
Disabled state survives a restart. A disabled executor cannot send reports or
peer messages and cannot receive orders, peer messages, broadcasts, or
Telegram-inbound dispatches. `messaging_enable` never revives cancelled outbox
records. With `clear_queue=false`, pending audits involving the disabled role
are held without occupying the delivery batch and resume only after the role is
explicitly enabled. Delivered audit history remains intact.

Starting with 0.4.0, re-enable is guarded against an agent bypassing a breaker
merely to make its rejected order succeed. `messaging_enable` requires a
non-empty remediation reason and the exact `changed_at` value from a fresh
`swarm://messaging` read. It is rejected until
`SWARM_MESSAGING_REENABLE_COOLDOWN_SECONDS` expires, and a stale observation is
rejected atomically. A breaker rejection is a stop-and-escalate condition for
the current agent run; resume belongs to a later, explicit human request.

Telegram delivery is asynchronous. A successful tool response reports an
`outbox_id`; temporary Telegram failure does not turn a successfully accepted
agent run into a failed command. Telegram delivery is at-least-once, so a crash
between Telegram accepting a message and the local acknowledgement can produce
a duplicate. Non-retryable Telegram 4xx responses are dead-lettered on the
first attempt instead of being replayed repeatedly. A retryable transport
failure stops the current batch and defers every pending item sharing that bot
transport, so `Retry-After` cannot be bypassed by later rows from the same
already-selected batch.

Peer-delivery prompts are task-scoped and deliberately one-way by default: ACK, closure,
stand-by, and unchanged-evidence messages must not trigger another `msg_to`.
This prevents conversational acknowledgement loops from turning into new
Hermes runs. The persistent manager circuit breaker remains the hard stop for
unexpected model behavior.

## Shared Telegram gateway

Telegram audit delivery has two mutually exclusive modes:

| `SWARM_TELEGRAM_BOT_MODE` | Token source | Inbound commands |
|---|---|---|
| `per-role` | `<ROLE>_TELEGRAM_BOT_TOKEN` for every role | disabled |
| `shared` | one `SWARM_TELEGRAM_BOT_TOKEN` owned by Swarm MCP | optional |

In shared mode every `order`, `order_all`, `report`, `msg_to`, and `msg_all` is
copied to `TELEGRAM_GROUP_ID` by the same bot. Commands originating in that
group are audited there as well. Private chats are isolated: their inbound
text, user metadata, and dispatch result never enter the group outbox, and the
final answer is sent back to the source chat without an audit header. Group
audit messages still identify the event, sender, and recipients, so individual
Hermes containers do not need Telegram credentials. Executors report through
Swarm MCP; their reports are therefore published by the shared bot as well.

Set `SWARM_TELEGRAM_INBOUND_ENABLED=true` to let authorized people address the
swarm through that bot. The gateway accepts messages either from the exact
numeric `TELEGRAM_GROUP_ID` or from a private chat whose positive chat ID
matches the sender ID; in both cases the sender must be listed in
`TELEGRAM_ALLOWED_USERS`. Plain group conversation is ignored. Supported
commands are:

```text
/manager <message>
/developer <message>
/designer <message>
/lead_developer <message>
/tester <message>
/devops <message>
/to <role> <message>
/all <message>
/roles
/help
```

Role commands are generated from the configured live roster. Disabled Compose
profiles are not included in `SWARM_AGENT_ROLES`, cannot receive orders, and do
not appear in `/roles` or the MCP hierarchy resource.

`SWARM_TELEGRAM_INBOUND_TARGETS` limits which configured roles these commands
may address; `*` enables all roles. Each accepted Telegram update is reserved
under the manager identity in the same durable operation ledger as MCP calls.
The Telegram update ID is its idempotency key, and the next polling offset is
stored in SQLite after processing, preventing ordinary restart replays.

The default `SWARM_TELEGRAM_PROCESS_BACKLOG=false` discards old queued updates
the first time inbound polling starts. Change it only when deliberately
replaying the existing bot backlog. The bot must not have a webhook configured,
because Telegram does not allow `getUpdates` while a webhook is active. Run one
Swarm MCP replica per bot token; two long pollers would race for the same update
stream. Delivery and polling use the common `TELEGRAM_PROXY_URL` when set.

The server reuses bounded HTTP clients, disables redirects, limits upstream
response bodies, bounds semaphore wait time, and shuts down MCP sessions and
background workers gracefully. Valid MCP and activity traffic is admitted
through per-role request windows before it can create unbounded work.

## HTTP endpoints and security

| Endpoint | Authentication |
|---|---|
| `/roles/<role>/mcp` | the exact role's bearer token |
| `/activity` | any configured executor token |
| `/health` | none; liveness only, no roster or secrets |
| `/ready` | none; obtains and rolls back a SQLite write transaction |

All routes, including health and activity, enforce `Host`. Requests carrying an
`Origin` header must match the configured origin allowlist. MCP and JSON bodies
have a shared size limit. Secret values use a redacted debug representation and
role comparisons use constant-time token equality.

MCP is served on one Streamable HTTP endpoint per role, as required by the MCP
transport model. POST and GET are handled by the official Rust SDK service.

## Configuration

Runtime configuration lives in `../swarm/.env`; non-secret examples are in
`../swarm/.env.example`. Configuration is validated before binding a socket.
Unknown hierarchy roles, duplicate tokens, invalid activity routes, incomplete
prompt placeholders, unsafe URLs, and malformed limits fail startup.
Both Compose files pass an explicit Swarm MCP environment allowlist rather than
injecting unrelated provider, Forgejo, or desktop secrets from the shared file.

Important groups:

| Group | Variables |
|---|---|
| Network | `SWARM_MCP_HOST`, `SWARM_MCP_PORT`, `SWARM_MCP_ALLOWED_HOSTS`, `SWARM_MCP_ALLOWED_ORIGINS`, `SWARM_MCP_MAX_REQUEST_BODY_BYTES` |
| Hierarchy | `SWARM_AGENT_ROLES`, `SWARM_MANAGER_ROLE`, `SWARM_ORDER_ACL`, `SWARM_EXECUTOR_DESCRIPTIONS` |
| Role credentials | `<ROLE>_SWARM_MCP_TOKEN`, `<ROLE>_API_KIND` (`hermes`/`openai`/`mcp`), `<ROLE>_API_URL`, `<ROLE>_AGENT_API_KEY`, `<ROLE>_API_MODEL` |
| Dispatch guards | `SWARM_DISPATCH_RATE_LIMIT`, `SWARM_DISPATCH_RATE_WINDOW_SECONDS`, `SWARM_DUPLICATE_WINDOW_SECONDS`, `SWARM_MESSAGING_REENABLE_COOLDOWN_SECONDS`, `SWARM_MAX_INFLIGHT_DISPATCHES`, `SWARM_PENDING_STALE_SECONDS`, `SWARM_REPORT_WAKE_STATUSES` |
| HTTP admission | `SWARM_MCP_REQUEST_RATE_LIMIT`, `SWARM_MCP_REQUEST_RATE_WINDOW_SECONDS` |
| State | `SWARM_STATE_DB_PATH`, `SWARM_DB_MAX_CONNECTIONS`, `SWARM_DB_BUSY_TIMEOUT_SECONDS`, `SWARM_RECENT_OPERATIONS_LIMIT`, `SWARM_OPERATION_RETENTION_DAYS`, `SWARM_CLEANUP_INTERVAL_SECONDS` |
| Activity | `SWARM_ACTIVITY_ENABLED`, `SWARM_ACTIVITY_ROUTES`, `SWARM_ACTIVITY_CLOCK_SKEW_SECONDS`, `SWARM_ACTIVITY_*` |
| Telegram delivery | `SWARM_TELEGRAM_ENABLED`, `SWARM_TELEGRAM_BOT_MODE`, `SWARM_TELEGRAM_BOT_TOKEN`, `<ROLE>_TELEGRAM_BOT_TOKEN`, `TELEGRAM_GROUP_ID`, `TELEGRAM_PROXY_URL`, `SWARM_OUTBOX_*` |
| Telegram inbound | `SWARM_TELEGRAM_INBOUND_ENABLED`, `TELEGRAM_ALLOWED_USERS`, `SWARM_TELEGRAM_INBOUND_TARGETS`, `SWARM_TELEGRAM_POLL_*`, `SWARM_TELEGRAM_PROCESS_BACKLOG`, `SWARM_TELEGRAM_INBOUND_*` |
| Prompts | `SWARM_*_INSTRUCTIONS`, `SWARM_*_PROMPT_TEMPLATE` |

## Local verification

Rust 1.89 or newer is required:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked --release
cargo audit --file Cargo.lock
```

Start the server with the swarm environment and run non-destructive deployment
probes:

```bash
swarm-mcp serve
swarm-mcp probe
swarm-mcp activity-probe
```

`probe` verifies every role's exact tool/resource catalog plus every wrong
token/endpoint combination. `activity-probe` checks every configured route and
also proves the event is absent from non-target authority resources. Neither
probe invokes an agent tool.

## Docker

Перед сборкой runtime-образа создайте пакет из текущего checkout:

```bash
scripts/build-deb.sh
docker build -t demlabs/swarm-mcp:local .
```

Скрипт использует закреплённый Rust builder и `cargo-deb`, удаляет старые
одноимённые пакеты и оставляет в `dist/` ровно один `.deb`. Это не позволяет
Docker cache незаметно развернуть бинарник от предыдущего commit.

The image uses digest-pinned Rust, Dockerfile frontend, and minimal Debian base
images. The runtime installs no packages and copies only the CA bundle and
compiled binary. It runs as UID/GID 1000 with no Linux capabilities, a read-only
root filesystem, and only the `/data` SQLite volume writable. The image
healthcheck calls `/ready` through the same compiled binary.

From the swarm directory:

```bash
docker compose build swarm-mcp
docker compose up -d swarm-mcp
docker compose exec -T swarm-mcp swarm-mcp probe
docker compose exec -T swarm-mcp swarm-mcp activity-probe
```

The full deployment remains `swarm/deploy-agent-dev.sh`; it synchronizes this
repository to `agent@agent-dev-0`, backs up SQLite and the previous image,
starts Swarm MCP alone, runs both probes, and rolls back that state/image if the
preflight fails. Hermes, Firefox, and Forgejo deployment inputs are pinned in
`swarm/.env`.
