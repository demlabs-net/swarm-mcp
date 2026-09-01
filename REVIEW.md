# Swarm MCP deep review and roadmap

> Historical audit snapshot for the retired pre-0.5 task-aware interface.
> Names such as `order`, `report`, task lineage, and task status below describe
> removed behavior only. The current contract is in `README.md` and
> `AGENTS.md`: SLC MCP owns workflow; Swarm MCP is transport and wake delivery.

## Executive assessment

The retired Python server was a useful functional prototype: it separated role
catalogs, checked reverse hierarchy relationships, and deliberately kept
activity signals passive. It was not safe enough to be the control plane for a
busy autonomous swarm. Its primary failure mode was not a syntax bug but the
absence of delivery semantics: every retry was a new side effect, broadcasts
could partially succeed without durable reconciliation, and Telegram was an
inline best-effort action. This made duplicate runs, silent audit loss, and
message storms possible.

The Rust rewrite replaces the runtime in the same repository. It uses the
official `rmcp` Streamable HTTP server, Axum, Tokio, SQLx/SQLite, typed startup
configuration, and shared Reqwest clients. The Python sources were removed from
the tree; they remain in git history as the audit snapshot.

The review covered the old server, probes, activity hook, Docker image,
standalone and swarm Compose definitions, deployment script, authentication,
MCP catalogs, database lifecycle, downstream agent calls, and Telegram audit
delivery. The transport design was checked against the MCP Streamable HTTP
transport requirements and the official Rust SDK.

## Findings and disposition

| ID | Severity | Finding in the Python runtime | Consequence | Rust disposition |
|---|---:|---|---|---|
| F-01 | Critical | Mutating tools had no idempotency key or durable reservation | Client/network retries could start duplicate agents and cause Telegram spam | Persistent fingerprinted reservations; same-key replay and conflict detection |
| F-02 | Critical | No rate limit, queue bound, or dispatch backpressure | A loop or cron could fan out unbounded runs | Persistent per-role window limit, bounded global semaphore, bounded wait |
| F-03 | High | No operation/task ledger | Callers could not determine whether a retry was safe | `dispatches`/`dispatch_targets` ledger and role-scoped `swarm://operations` |
| F-04 | High | Broadcasts returned partial results only to the current request | Results disappeared after disconnect and naive retries duplicated successful targets | Durable `partial` result with per-target run IDs/errors and safe replay |
| F-05 | High | Telegram ran inline after the agent call with no retry queue | Audit failure was silently reduced to `false`; successful work and audit status were coupled | Transactional outbox, retry/dead states, asynchronous delivery |
| F-06 | High | A Telegram transport exception could include the bot token embedded in the request URL | Credential disclosure in logs or stored errors | Sanitized Telegram transport errors; secret wrapper redacts debug output |
| F-07 | High | Synchronous `sqlite3` calls ran inside async request handlers | Event-loop stalls and lock amplification under concurrent activity | Async SQLx pool, busy timeout, WAL, indexed queries |
| F-08 | High | Activity state used receive time and unconditionally overwrote current state | Late terminal events could corrupt a newer turn; `started` stayed active forever | Client occurrence timestamps, deterministic ordering, event rank, stale-state calculation |
| F-09 | High | Activity deduplication was only in the hook; the database accepted duplicates | Restarted/replayed hooks polluted history | Database uniqueness across sender/target/event/turn |
| F-10 | High | Activity hook created its dedupe marker before delivery and retained it after failure | One temporary network failure permanently suppressed the signal | Durable local SQLite outbox with atomic lease, backoff/dead-letter, bounded queue, and explicit `occurred_at` |
| F-11 | High | MCP transport security did not uniformly cover the parent health/activity application | Host/origin policy differed by route; health exposed hierarchy | Global Host/Origin enforcement; minimal liveness payload; strict body limit |
| F-12 | High | Health was liveness only and no readiness endpoint existed | Orchestration could route traffic to a process with unusable state | `/health` plus SQLite read/write `/ready` and Docker healthcheck |
| F-13 | Medium | A new HTTP client was created for every agent or Telegram operation | Connection churn, extra TLS handshakes, and resource pressure | Shared bounded clients, redirects disabled, timeouts configured once |
| F-14 | Medium | Downstream response bodies were unbounded | A bad upstream could consume arbitrary memory | Content-Length and streamed body-size enforcement |
| F-15 | Medium | Configuration and MCP server objects were built at module import time | Hard testing, no clean failure boundary, accidental side effects on import | Typed `Config::from_env`, explicit initialization, no runtime globals |
| F-16 | Medium | Prompt placeholders were checked only when a tool was called | A typo created a persisted failed command during production use | Startup validation of exact placeholder contracts; single-pass substitution |
| F-17 | Medium | `report.status`, identifiers, paths, origins, and several maps were weakly validated | Inconsistent data and runtime-only errors | Strict identifiers/status enum, URL/path/ACL/route/token validation |
| F-18 | Medium | `msg_to` schema accepted any string even though runtime rejected most values | Agents discovered a broader contract than they could use | Dynamic schemas enumerate authorized targets and peers |
| F-19 | Medium | Python source, probes, and runtime dependencies shared one image | Large mutable runtime and live smoke scripts could trigger real agents | Compiled non-root runtime; archived Python excluded via `.dockerignore`; probes are read-only |
| F-20 | Medium | No graceful shutdown coordination for sessions/background work | In-flight state could be abandoned without an explicit boundary | Cancellation token shared by MCP services and workers; pending operations become `indeterminate` after restart |
| F-21 | Low | Logs lacked a structured, consistent format | Harder correlation and machine processing | Structured JSON tracing and stable operation IDs |
| F-22 | Low | No automated unit tests; probes depended on a live deployment | Regressions in parsing/chunking/security were easy | Unit tests, compiled catalog/activity probes, and GitLab CI with Clippy/RustSec |
| F-23 | High | No live role-level messaging kill switch and peer prompts encouraged replies | Reciprocal ACK/closure messages could create a slow, unbounded cascade of new Hermes runs and Telegram audits | Persistent manager-only disable/enable/queue-clear controls, bidirectional dispatch enforcement, one-way peer instructions, and first-attempt dead-lettering for permanent Telegram 4xx errors |
| F-24 | High | Idempotency depended on the model reusing a caller-supplied key | A drifting model could omit or rotate keys and start the same downstream run repeatedly | Configurable server-side recent-content suppression keyed by sender, operation kind, targets, and whitespace-normalized payload |
| F-25 | High | Peer messages had no task lineage and reports allowed an empty or invented task ID | Free-form coordination could become an untraceable conversational loop or rotate IDs to bypass suppression | `task_id` is required for `report`, `msg_to`, and `msg_all`, verified against an accepted order assigned to the sender, and included in prompts, results, and duplicate fingerprints |
| F-26 | High | The surrounding Hermes profiles allowed 500 outer turns, 250 delegated turns, repeated continuation nudges, and warning-only tool-loop detection | A single bad strategy could consume a large context and repeatedly call tools even though Swarm MCP itself was rate-limited | Swarm deployment now applies per-role turn/output/delegation budgets, one corrective continuation, unattended hard stops, and role-specific tool visibility |
| F-27 | Medium | Every Hermes model call reloaded all SLC manuals and persisted up to 6000 characters of repeated transcript | Useful task context was displaced by repeated policy text and episodic noise | Lifecycle hooks now load compact active context, save a 1200-character evidence snapshot, and rely on just-in-time document retrieval |
| F-28 | High | Every `in_progress` report started a new supervisor run | Routine progress could recursively consume manager turns and produce further orders or Telegram traffic | Reports remain durable and audited, but only configurable terminal/material statuses wake a supervisor |
| F-29 | Critical | A restarted manager resumed an old run and called `messaging_enable` immediately after its order was rejected by the persistent breaker | The agent entrusted with containment could undo containment as a tool-recovery step and restart the same flood | Re-enable now requires a fresh state timestamp, a remediation reason, and a configurable cooldown; breaker errors explicitly require stop-and-human-escalation, and the tool is no longer pinned in the manager's ordinary working set |

## New architecture

```text
role token
   │
   ▼
Host/Origin/body guard ──► exact role MCP catalog
                                │
                                ▼
                     ACL + input validation
                                │
                                ▼
                SQLite reservation / rate limit
                         │                │
                         │                └── duplicate/conflict replay
                         ▼
                bounded Hermes API call(s)
                         │
                         ▼
              durable result + Telegram outbox
                         │
                         └──► retrying Telegram worker
```

SQLite is deliberately kept for a single-container deployment. Existing
Python `activity_events` and `activity_state` tables are detected, renamed to
`*_python_legacy`, and imported into the ordered Rust schema. A startup pass
recovers a `pending` operation as accepted when its durable delivery-outbox row
proves the wake was queued. Pending operations without that evidence become
`indeterminate`: the server will not blindly redeliver a side effect whose
outcome is unknown.

## Known limits that cannot be solved inside this server alone

1. **Hermes does not expose a confirmed downstream idempotency contract.** The
   server reserves before calling Hermes, which prevents ordinary retries. A
   process crash after Hermes accepts a run but before SQLite stores the run ID
   leaves an `indeterminate` record. Automatic retry would risk duplication.
   Hermes should accept the Swarm task/message ID as an idempotency key and
   expose lookup by that key.

2. **Telegram is at-least-once.** Telegram does not offer an application
   idempotency key for `sendMessage`. A crash after remote acceptance and before
   the local delivered update can duplicate an audit message.

   Shared-bot inbound commands use a durable Telegram update offset and the
   update ID as the dispatch idempotency key. This prevents ordinary restart
   replay, but it does not change the outbound `sendMessage` limitation. The
   bot uses long polling and therefore requires a single active Swarm MCP
   replica and no configured Telegram webhook.

3. **SQLite and local MCP sessions imply a single active replica.** Running two
   replicas against a shared filesystem is not a supported HA design. Horizontal
   scaling requires a network database, a distributed limiter/outbox claim, and
   a shared MCP session store or stateless-only protocol policy.

4. **Static bearer tokens have no overlap rotation.** Tokens are compared in
   constant time and isolated by endpoint, but rotation currently requires a
   coordinated environment update and restart.

5. **The role messaging circuit breaker is manual.** The manager can now block
   either direction for an executor and cancel its audit queue. Re-enable is
   stale-state-checked and cooldown-gated, but the server cannot prove that an
   MCP call was caused by a human sentence; the manager profile therefore treats
   breaker rejection as terminal and discovers the re-enable tool only for an
   explicit later resume turn. Transport failure thresholds do not automatically
   trip the breaker; automatic half-open recovery remains future work.

6. **Legacy MCP sessions are process-local.** Request admission bounds creation
   rate, but legacy clients that never send session deletion can retain session
   state until restart. Switching fully to stateless MCP requires confirming
   every deployed Hermes client negotiates the newer protocol first.

## Feedback-loop follow-up review (2026-08-20)

The post-incident review covered every retry, polling, broadcast, authorization,
and outbox path in the Rust server. It found and corrected six related gaps:

- messaging controls now enforce manager authority inside `Dispatcher`, not
  only through MCP catalog visibility;
- disabling or clearing a role cancels undelivered audits both from and to that
  role, including manager orders already waiting for Telegram;
- permanent local delivery failures and non-retryable Telegram responses are
  dead-lettered immediately;
- a transient Telegram failure stops the selected batch and persists transport
  backoff for the remaining affected rows;
- delivery revalidates each snapshotted outbox row under the messaging gate;
  preserved rows involving a disabled role are held outside the due batch and
  cannot leak through a disable race or starve unrelated delivery;
- a stale Telegram update is skipped without starving newer updates returned in
  the same batch.

The documented single-replica/at-least-once limits still apply: an outbound
Telegram acceptance followed by a process crash can duplicate the last chunk,
and multiple active server replicas require a distributed outbox claim.

## Remediation and capability plan

### Phase 0 — migration gate

- Back up `/data/swarm.db` before the first Rust deployment.
- Build with the locked dependency graph and run unit tests and Clippy.
- Start against a copy of the production database and verify the legacy import.
- Run `swarm-mcp probe` and `swarm-mcp activity-probe` inside the Compose
  network.
- Exercise one explicitly approved order/report round trip and verify the
  operation ledger and Telegram outbox before restoring normal cron activity.

### Phase 1 — end-to-end exactly-once contract

- Add `idempotency_key` support to Hermes `POST /v1/runs`.
- Add `GET /v1/runs/by-idempotency-key/<key>` or an equivalent lookup.
- Persist per-target state (`reserved`, `accepted`, `failed`) before assembling
  a broadcast result.
- Reconcile `indeterminate` operations automatically from Hermes instead of
  requiring human review.
- Add safe `retry_failed_targets(operation_id)` without replaying successful
  broadcast members.

### Phase 2 — operator controls

- Completed: persistent manager-only per-role messaging disable/enable, queue
  cancellation, `swarm://messaging`, and negative-path catalog/auth probes.
- Add non-mutating `get_operation(id)` and filtered/paginated operation
  resources.
- Add privileged `cancel_operation(id)` only after Hermes supports cancellation.
- Add task deadlines, priorities, and maximum fan-out policies per authority.
- Add explicit approval gates for deployment-class orders.
- Add per-target and per-tool rate policies rather than one actor-wide window.

### Phase 3 — resilience and observability

- Add a downstream circuit breaker with half-open recovery per agent.
- Claim outbox rows atomically so multiple workers cannot deliver the same row.
- Export OpenTelemetry traces and Prometheus metrics for latency, saturation,
  dispatch state, outbox age, dead letters, authentication rejection, and DB
  contention.
- Add correlation IDs to downstream requests and Telegram audit text.
- Add dashboarding and alerts for the already configurable operation/activity/
  outbox retention and dead-letter state.

### Phase 4 — dynamic hierarchy and secret lifecycle

- Move hierarchy policy into a versioned config document with atomic reload and
  validation; keep secrets outside that document.
- Support multiple active tokens per role during a bounded rotation window.
- Prefer Docker secrets or a secret manager over plain environment values.
- Add scoped capabilities beyond `order`: repository administration, deploy,
  review approval, and test acceptance can then be modeled independently.
- Record policy version and authorization decision with every operation.

### Phase 5 — high availability

- Replace SQLite with PostgreSQL when more than one active instance is needed.
- Use transactional row claims for rate limits and outbox delivery.
- Configure a shared MCP session store, or require a stateless MCP protocol
  version once every client supports it.
- Add chaos tests for process death in every reservation/delivery transition.

## Acceptance criteria for the first Rust deployment

- No Python process or package is present in the runtime image.
- Every role sees exactly the tools generated by `SWARM_ORDER_ACL`.
- Every token is rejected at every other role endpoint.
- Retrying a tool with the same idempotency key returns the stored result and
  does not call Hermes again.
- Reusing that key with different arguments fails.
- The configured rate limit rejects excess operations without agent calls.
- A Telegram outage leaves pending/dead outbox evidence without losing the
  accepted agent result.
- Manager-only role controls block both sender and recipient paths, persist
  across restart, cancel undelivered audit items, and never replay cancelled
  items when re-enabled.
- Activity reads and probes never start an agent or send Telegram.
- `/ready` fails when SQLite is unavailable.
- The service runs as non-root with a read-only root filesystem.

## Transport FIFO review (2026-09-01, v0.6.1)

Hermes correctly keeps `max_concurrent_runs=1` per role profile, but a second
wake previously surfaced that protection as a caller-visible HTTP 429. Version
0.6.1 keeps task ordering in SLC and adds only a transport retry boundary:

- a definitive Hermes 429 persists the opaque wake in SQLite and finishes the
  original transport operation as accepted with `queued=true`;
- one integer sequence orders each recipient FIFO, and an in-process
  per-recipient gate prevents newer calls from overtaking an existing head;
- the worker retries only one head per recipient, honors `Retry-After`, and
  records `delivered` or `dead` without interpreting correlation IDs;
- messaging disable/clear pauses or cancels both Telegram audits and queued
  wakes, with eligibility rechecked under the messaging gate;
- final eligibility also rechecks due time and FIFO-head ownership, so a stale
  worker snapshot cannot overtake after another worker changes the row;
- enqueue verifies an existing queue ID has the identical still-pending
  payload, and restart recovery finalizes queue-backed pending operations as
  accepted, reconstructing the single-recipient queue ID/position rather than
  exposing a retryable-looking indeterminate result;
  incomplete or dead/cancelled multi-target evidence is finalized with
  `ok=false,recovery_required=true` instead of being reported as success;
- shutdown awaits the new worker, and `swarm://operations`/`messaging` expose
  transport status without payload bodies or task state.

Accepted limitation: like the existing Telegram outbox, a process/SQLite
failure after downstream acceptance but before the delivered update can repeat
the last wake. True exactly-once delivery still requires Hermes-side
idempotency. The single-active-Swarm-MCP topology remains mandatory.
