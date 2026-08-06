# Swarm MCP Server

This service is the authenticated communication plane for the Hermes
development swarm. It exposes one MCP catalog per role. A role token is valid
only at that role's endpoint; it cannot be reused to discover or call another
catalog.

The endpoint template and hierarchy are configured in `../swarm/.env`:

```dotenv
SWARM_ROLE_MCP_PATH_TEMPLATE=/roles/{role}/mcp
SWARM_ORDER_ACL={"manager":["*"],"lead-developer":["developer"]}
```

With that ACL, the catalogs are:

| Caller | Tools |
|---|---|
| manager | `order`, `order_all` |
| lead-developer | `order`, `report`, `msg_to`, `msg_all` |
| developer, designer, tester, devops | `report`, `msg_to`, `msg_all` |

`*` grants swarm-wide `order` and `order_all`. An explicit target list grants
only `order`, and its MCP input schema enumerates only those targets. Roles
without an ACL entry cannot issue orders. The `swarm://hierarchy` resource
shows the authenticated caller's targets, supervisors, and capabilities.

`order` starts an asynchronous Hermes run and tells the subordinate to report
back to the issuing authority. `report` accepts only recipients that supervise
the caller according to the reverse ACL; it defaults to the manager for
backward compatibility. Peer messages remain available to executor roles.

The authenticated `/activity` endpoint accepts lifecycle signals only from
routes declared in `SWARM_ACTIVITY_ROUTES`. Developer start/completion/failure
signals currently start a lead-developer run that verifies the recurring
inspection cron and performs review when useful. Orders, reports, peer
messages, and activity signals are copied to the shared Telegram group by the
sender's bot.

All role names, paths, ACLs, activity routes, tokens, timeouts, instructions,
and English prompt templates live in `../swarm/.env`; see
`../swarm/.env.example`. The service has no published host port.

Validate every role catalog and cross-role authentication after deployment:

```bash
docker exec dev-swarm-mcp python -m swarm_mcp.probe
```

Optional end-to-end checks:

```bash
docker exec dev-swarm-mcp python -m swarm_mcp.smoke developer
docker exec dev-swarm-mcp python -m swarm_mcp.report_smoke developer
```
