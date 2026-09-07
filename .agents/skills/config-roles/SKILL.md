---
name: config-roles
description: Configuration and role management reference for swarm-mcp. Use when adding or removing a role, changing the delivery ACL or activity routes, enabling Telegram modes/inbound, or diagnosing config validation failures.
---

# Config & roles for swarm-mcp

All configuration comes from environment variables, validated once by `Config::from_env` (`src/config.rs`, ~700 lines of checks — read it before changing anything). The deployment feeds it via `docker-compose.yml` from `../swarm/.env`. Validation errors fail startup with a precise message; they are the first thing to read when a role misbehaves.

## Core layout

- `SWARM_MANAGER_ROLE` — one manager (e.g. `manager`).
- `SWARM_AGENT_ROLES` — CSV of executors (≤ 32, lowercase `[a-z0-9-]`, unique). Manager must not be an executor.
- Every role (manager + executors) needs `{PREFIX}_API_URL`, `{PREFIX}_AGENT_API_KEY` (≥ 16 chars), `{PREFIX}_SWARM_MCP_TOKEN` (≥ 24 chars, unique across roles), where `{PREFIX}` = role uppercased with `-` → `_` (e.g. `lead-developer` → `LEAD_DEVELOPER_`).
- `SWARM_EXECUTOR_DESCRIPTIONS` — JSON map, must describe every executor exactly once.
- `SWARM_ROLE_MCP_PATH_TEMPLATE` — must contain exactly one `{role}`; paths must be unique and must not collide with `/health`, `/ready`, or `SWARM_ACTIVITY_SIGNAL_PATH`.
- `SWARM_DISPATCH_ACL` — JSON `{source: [targets]}`. `["*"]` makes the
  source a global delivery authority (gets `dispatch_all` and may wake every
  executor). Explicit lists grant only `dispatch_to`. This ACL is transport
  reachability, not task delegation; SLC configures task authority separately.
- `SWARM_ACTIVITY_ROUTES` — JSON `{executor: [targets]}`; every target must be
  a role authorized to dispatch to that executor. Only executors may emit
  passive activity.

## Adding a new role (e.g. `designer`)

1. Add `designer` to `SWARM_AGENT_ROLES` and `SWARM_EXECUTOR_DESCRIPTIONS`.
2. Grant the required delivery reachability in `SWARM_DISPATCH_ACL`. Configure
   task assignment independently in SLC MCP's `SLC_TASK_ASSIGN_ACL`.
3. Add env entries: `DESIGNER_API_URL`, `DESIGNER_AGENT_API_KEY`, `DESIGNER_SWARM_MCP_TOKEN` (and optionally `DESIGNER_TELEGRAM_BOT_TOKEN` for per-role mode).
4. `docker-compose.yml` must forward the new `*_SWARM_MCP_TOKEN`/`*_TELEGRAM_BOT_TOKEN`/`*_API_URL`/`*_AGENT_API_KEY` variables — the compose file currently lists seven hardcoded roles; keep it in sync.
5. Run `swarm-mcp probe` to verify the catalog and token isolation for the new role.

## Telegram

- `SWARM_TELEGRAM_ENABLED` gates delivery. Modes: `per-role` (each role needs its own bot token) or `shared` (one `SWARM_TELEGRAM_BOT_TOKEN`). `TELEGRAM_GROUP_ID` is required when enabled.
- Inbound (`SWARM_TELEGRAM_INBOUND_ENABLED`) requires shared mode + `TELEGRAM_ALLOWED_USERS` (CSV of numeric user IDs) + `SWARM_TELEGRAM_INBOUND_TARGETS` (`*` or role CSV) + numeric non-zero `TELEGRAM_GROUP_ID`.
- `SWARM_TELEGRAM_PROCESS_BACKLOG` — `false` (default) discards the backlog on first run by fast-forwarding the update offset; `true` processes it.
- Templates: `SWARM_DISPATCH_PROMPT_TEMPLATE`,
  `SWARM_PEER_PROMPT_TEMPLATE`, `SWARM_TELEGRAM_INBOUND_PROMPT_TEMPLATE`,
  `SWARM_AUTHORITY_MCP_INSTRUCTIONS` and the `*_INSTRUCTIONS` vars.
  Placeholder contracts are validated at startup; adding a placeholder
  requires editing both validation and render sites.

## Cross-checks to know

- Swarm has no supervisor or report relation. SLC stores issuer/assignee and
  validates task messages/reports. Swarm `msg_to` can wake any other role.
- Rate/retention caps are validated in `from_env` (e.g. `SWARM_PENDING_STALE_SECONDS` must exceed 2× `SWARM_API_TIMEOUT_SECONDS`; body size must fit `SWARM_MAX_MESSAGE_CHARS` × 4 + 2048).
- `SWARM_MCP_ALLOWED_HOSTS` entries without a port match ANY port; with a port they match only that port. Origins are strict `scheme://host[:port]` tuples; `"null"` is a valid entry.
- Probes (`probe`, `activity-probe`) need the full env and a running server; the Docker HEALTHCHECK uses `healthcheck` which only needs `SWARM_MCP_PORT`.
