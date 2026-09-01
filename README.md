# Swarm MCP

Swarm MCP is a role-scoped **transport and wake-up adapter** for agent runtimes.
It delivers payloads, applies delivery ACLs, records delivery attempts,
provides circuit breakers, and optionally mirrors traffic to Telegram.

It is deliberately not a task system. Task identity, assignment, parent/root
lineage, status, task reports, and task conversation are owned by SLC MCP. A
caller normally creates or updates an SLC task first, then uses Swarm only when
the recipient needs an immediate wake-up. The SLC id may be carried as an
opaque `correlation_id`; Swarm never resolves it or changes task state. This
keeps the workflow intact if the transport is later replaced by Matrix, NATS,
email, or another adapter.

## Transport topology

`SWARM_DISPATCH_ACL` controls only who may start delivery to which runtime:

```dotenv
SWARM_MANAGER_ROLE=manager
SWARM_AGENT_ROLES=dev-senior-0,dev-middle-0,dev-junior-0,tester,devops
SWARM_DISPATCH_ACL={"manager":["*"],"dev-senior-0":["dev-middle-0","dev-junior-0","tester","devops"]}
```

`"*"` grants `dispatch_to` to every executor and exposes `dispatch_all`.
An explicit list grants only single-target `dispatch_to`. This ACL says
nothing about who may assign or complete a task; configure that independently
in SLC MCP.

| Caller | Transport tools |
|---|---|
| Any role | `msg_to`, `msg_all`, `cancel_delivery`, `telegram_reply` |
| Role with delivery targets | `dispatch_to` |
| Global transport authority | `dispatch_all` |
| Manager | messaging circuit-breaker controls |

`dispatch_to` and `dispatch_all` create `dispatch_...` delivery IDs.
`msg_to` and `msg_all` create `msg_...` IDs. Their optional
`correlation_id` is external and opaque. There is no `order`, `report`,
task lineage lookup, status transition, or terminal-report wake policy in this
service.

When a Hermes destination returns HTTP 429 because its single-writer run slot
is occupied, the delivery is accepted into a durable per-recipient transport
FIFO. The tool returns `queued=true`, a `queue_id`, and `queue_position`; this
is a successful transport outcome and must not be retried. Later deliveries to
that recipient join the FIFO without attempting to overtake its head. A worker
retries the head after `Retry-After` and records `delivered` or `dead` state in
`swarm://operations`. Eligibility is rechecked immediately before delivery,
including due time and FIFO-head ownership. This queue contains opaque
transport payloads only and never decides task readiness or order; that remains
SLC's responsibility.

If an undelivered wake becomes obsolete, its original sender or the manager can
call `cancel_delivery` with the exact `queue_id`. Cancellation is linearized
against the delivery worker, advances only that recipient's transport FIFO,
and rejects an item that already started a Hermes run. It never accepts a task
ID or changes SLC state; broad role queue clearing remains an emergency circuit
operation, not ordinary scheduling.

Single-recipient tools accept `recipient`, `correlation_id`, `idempotency_key`,
and `message`, so the content-free four-field `delivery` object returned by SLC
workflow calls can be passed directly. Unknown future envelope metadata is
deliberately ignored by the adapter. The legacy input alias `agent` remains
accepted during migration but is not advertised.

## Role delivery modes

| Role mode | `<ROLE>_API_KIND` | Delivery behavior |
|---|---|---|
| Hermes | `hermes` | starts `/v1/runs` and records the returned run ID |
| OpenAI-compatible | `openai` | initiates a bounded chat completion |
| Pull/MCP-only | `mcp` | rejects push delivery; the participant polls SLC/its own inbox |

The delivered prompt contains `dispatch_id`, sender, recipient, optional
`correlation_id`, and message. The recipient must read canonical task state
from SLC rather than treating the transport payload as authoritative.

## Resources

| Resource | Visibility | Meaning |
|---|---|---|
| `swarm://hierarchy` | every role | transport topology, delivery ACL, tools, safeguards, and explicit SLC task-domain boundary |
| `swarm://operations` | sender/recipient | recent durable delivery attempts and outcomes |
| `swarm://outbox` | sender; manager sees all | Telegram audit delivery state |
| `swarm://activity` | configured delivery authorities | passive runtime lifecycle signals; never task state |
| `swarm://messaging` | manager | persistent circuit-breaker state |

## Delivery and persistence

SQLite stores only transport concerns: reservations, idempotency, rate limits,
delivery targets/results, busy-recipient wake outbox, Telegram outbox state, circuit breakers, inbound
offsets, and passive runtime activity. It does not store task issuer,
assignee, status, report body, or lineage.

Retries use a caller-owned `idempotency_key`. Reusing a key with different
delivery arguments is rejected. Accepted, partial, and indeterminate outcomes
replay; definitive failures release the key for a real retry. A queued wake is
persisted before its operation result. If the process stops between those two
writes, startup recognizes the durable queue row and recovers the operation as
accepted with the original single-recipient queue ID/position instead of
inviting a duplicate wake. If queue rows cover only part
of a multi-target operation or contain a dead/cancelled outcome, the same
idempotency key remains finalized but replays `ok=false` with
`recovery_required=true`; it is never silently presented as complete. A separate
content-duplicate window suppresses accidental repeated delivery.

Messaging circuit breakers block both directions through this adapter and can
hold or cancel undelivered role wakes and Telegram audit records. They do not
pause or mutate the corresponding SLC task.

## Shared Telegram gateway

Telegram audit delivery has two mutually exclusive modes:

| `SWARM_TELEGRAM_BOT_MODE` | Token source | Inbound commands |
|---|---|---|
| `per-role` | `<ROLE>_TELEGRAM_BOT_TOKEN` for every role | disabled |
| `shared` | one `SWARM_TELEGRAM_BOT_TOKEN` owned by Swarm MCP | optional |

In shared mode every `dispatch_to`, `dispatch_all`, `msg_to`, and `msg_all` delivery is
copied to `TELEGRAM_GROUP_ID` by the same bot. Commands originating in that
group are audited there as well. Private chats are isolated: their inbound
text, user metadata, and dispatch result never enter the group outbox, and the
final answer is sent back to the source chat without an audit header. Group
audit messages still identify the event, sender, and recipients, so individual
Hermes containers do not need Telegram credentials. Task reports are written
only to SLC MCP. If an SLC event needs an immediate wake, its actor may pass
the exact content-free four-field `delivery` object returned by SLC through
this transport; report and task bodies are never mirrored here.

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
profiles are not included in `SWARM_AGENT_ROLES`, cannot receive transport dispatches, and do
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
| Hierarchy | `SWARM_AGENT_ROLES`, `SWARM_MANAGER_ROLE`, `SWARM_DISPATCH_ACL`, `SWARM_EXECUTOR_DESCRIPTIONS` |
| Role credentials | `<ROLE>_SWARM_MCP_TOKEN`, `<ROLE>_API_KIND` (`hermes`/`openai`/`mcp`), `<ROLE>_API_URL`, `<ROLE>_AGENT_API_KEY`, `<ROLE>_API_MODEL` |
| Dispatch guards | `SWARM_DISPATCH_RATE_LIMIT`, `SWARM_DISPATCH_RATE_WINDOW_SECONDS`, `SWARM_DUPLICATE_WINDOW_SECONDS`, `SWARM_MESSAGING_REENABLE_COOLDOWN_SECONDS`, `SWARM_MAX_INFLIGHT_DISPATCHES`, `SWARM_PENDING_STALE_SECONDS` |
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
