"""Authenticated, role-aware communication plane for the development swarm."""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import hmac
import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _required(name: str) -> str:
    value = _env(name)
    if not value:
        raise RuntimeError(f"{name} must be configured")
    return value


def _positive_int(name: str) -> int:
    try:
        value = int(_required(name))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


logging.basicConfig(level=_required("SWARM_LOG_LEVEL"))
logger = logging.getLogger(__name__)
# httpx INFO records include complete Telegram URLs, including bot tokens.
logging.getLogger("httpx").setLevel(logging.WARNING)


def _roles() -> tuple[str, ...]:
    roles = tuple(
        dict.fromkeys(
            item.strip().lower()
            for item in _required("SWARM_AGENT_ROLES").split(",")
            if item.strip()
        )
    )
    if not roles:
        raise RuntimeError("SWARM_AGENT_ROLES must contain at least one role")
    return roles


AGENT_ROLES = _roles()
MANAGER_ROLE = _required("SWARM_MANAGER_ROLE").lower()
if MANAGER_ROLE in AGENT_ROLES:
    raise RuntimeError("SWARM_MANAGER_ROLE must not also appear in SWARM_AGENT_ROLES")
ALL_ROLES = (MANAGER_ROLE, *AGENT_ROLES)

MAX_MESSAGE_CHARS = _positive_int("SWARM_MAX_MESSAGE_CHARS")
API_TIMEOUT_SECONDS = _positive_int("SWARM_API_TIMEOUT_SECONDS")
TELEGRAM_TIMEOUT_SECONDS = _positive_int("SWARM_TELEGRAM_TIMEOUT_SECONDS")
HERMES_MODEL_ALIAS = _required("SWARM_HERMES_MODEL_ALIAS")
ROLE_MCP_PATH_TEMPLATE = _required("SWARM_ROLE_MCP_PATH_TEMPLATE")
ACTIVITY_SIGNAL_PATH = _required("SWARM_ACTIVITY_SIGNAL_PATH")
_caller_role: contextvars.ContextVar[str] = contextvars.ContextVar(
    "swarm_caller_role", default=""
)


def _json_object(name: str) -> dict[str, Any]:
    try:
        value = json.loads(_required(name))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{name} must be a JSON object")
    return value


def _executor_descriptions() -> dict[str, str]:
    value = _json_object("SWARM_EXECUTOR_DESCRIPTIONS")
    normalized = {
        str(role).strip().lower(): str(description).strip()
        for role, description in value.items()
    }
    if set(normalized) != set(AGENT_ROLES) or any(
        not text for text in normalized.values()
    ):
        raise RuntimeError(
            "SWARM_EXECUTOR_DESCRIPTIONS must describe every executor exactly once"
        )
    return normalized


def _role_lists(name: str, *, allow_wildcard: bool = False) -> dict[str, tuple[str, ...]]:
    value = _json_object(name)
    result: dict[str, tuple[str, ...]] = {}
    for raw_source, raw_targets in value.items():
        source = str(raw_source).strip().lower()
        if source not in ALL_ROLES:
            raise RuntimeError(f"Unknown source role '{source}' in {name}")
        if not isinstance(raw_targets, list):
            raise RuntimeError(f"{name}.{source} must be a JSON list")
        targets = tuple(
            dict.fromkeys(str(target).strip().lower() for target in raw_targets)
        )
        if allow_wildcard and targets == ("*",):
            result[source] = tuple(role for role in AGENT_ROLES if role != source)
            continue
        if "*" in targets:
            raise RuntimeError(f"{name}.{source}: '*' must be the only target")
        unknown = set(targets) - set(AGENT_ROLES)
        if unknown:
            raise RuntimeError(f"Unknown target roles in {name}.{source}: {sorted(unknown)}")
        if source in targets:
            raise RuntimeError(f"{name}.{source} cannot target itself")
        result[source] = targets
    return result


EXECUTOR_DESCRIPTIONS = _executor_descriptions()
RAW_ORDER_ACL = _json_object("SWARM_ORDER_ACL")
ORDER_ACL = _role_lists("SWARM_ORDER_ACL", allow_wildcard=True)
GLOBAL_AUTHORITIES = {
    str(role).strip().lower()
    for role, targets in RAW_ORDER_ACL.items()
    if isinstance(targets, list) and targets == ["*"]
}
ACTIVITY_ROUTES = _role_lists("SWARM_ACTIVITY_ROUTES")


def _supervisors(role: str) -> tuple[str, ...]:
    return tuple(source for source in ALL_ROLES if role in ORDER_ACL.get(source, ()))


def _role_path(role: str) -> str:
    try:
        path = ROLE_MCP_PATH_TEMPLATE.format(role=role)
    except (KeyError, ValueError) as exc:
        raise RuntimeError("SWARM_ROLE_MCP_PATH_TEMPLATE must contain {role}") from exc
    if not path.startswith("/"):
        raise RuntimeError("SWARM_ROLE_MCP_PATH_TEMPLATE must produce an absolute path")
    return path.rstrip("/")


def _split_mcp_path(path: str) -> tuple[str, str]:
    parts = [part for part in path.split("/") if part]
    if len(parts) < 2:
        raise RuntimeError("Each role MCP path must contain a mount and transport path")
    return "/" + "/".join(parts[:-1]), "/" + parts[-1]


ROLE_PATHS = {role: _role_path(role) for role in ALL_ROLES}
if len(set(ROLE_PATHS.values())) != len(ROLE_PATHS):
    raise RuntimeError("Role MCP paths must be unique")
ROLE_MOUNTS = {role: _split_mcp_path(path)[0] for role, path in ROLE_PATHS.items()}
ROLE_STREAM_PATHS = {
    role: _split_mcp_path(path)[1] for role, path in ROLE_PATHS.items()
}


@dataclass(frozen=True)
class Agent:
    role: str
    url: str
    api_key: str
    mcp_token: str
    telegram_bot_token: str


def _prefix(role: str) -> str:
    return role.upper().replace("-", "_")


def _load_agent(role: str) -> Agent:
    prefix = _prefix(role)
    return Agent(
        role=role,
        url=_env(f"{prefix}_API_URL").rstrip("/"),
        api_key=_env(f"{prefix}_AGENT_API_KEY"),
        mcp_token=_env(f"{prefix}_SWARM_MCP_TOKEN"),
        telegram_bot_token=_env(f"{prefix}_TELEGRAM_BOT_TOKEN"),
    )


AGENTS = {role: _load_agent(role) for role in ALL_ROLES}


def _validate_configuration() -> None:
    tokens: set[str] = set()
    for role, agent in AGENTS.items():
        if not agent.url or not agent.api_key:
            raise RuntimeError(f"API configuration is incomplete for role '{role}'")
        if len(agent.mcp_token) < 24:
            raise RuntimeError(f"{_prefix(role)}_SWARM_MCP_TOKEN must be a strong secret")
        if agent.mcp_token in tokens:
            raise RuntimeError("Swarm MCP tokens must be unique")
        tokens.add(agent.mcp_token)
    if set(ORDER_ACL) - set(ALL_ROLES):
        raise RuntimeError("SWARM_ORDER_ACL contains an unknown authority")
    if set(ACTIVITY_ROUTES) - set(AGENT_ROLES):
        raise RuntimeError("Only executor roles may emit activity signals")
    if set(AGENT_ROLES) - set(ORDER_ACL.get(MANAGER_ROLE, ())):
        raise RuntimeError("The manager must be authorized to order every executor")


def _transport_security() -> TransportSecuritySettings:
    allowed = [
        item.strip()
        for item in _required("SWARM_MCP_ALLOWED_HOSTS").split(",")
        if item.strip()
    ]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=allowed,
    )


def _format_template(name: str, **values: str) -> str:
    template = _required(name).replace("\\n", "\n")
    try:
        return template.format(**values)
    except (KeyError, ValueError) as exc:
        raise RuntimeError(f"Invalid template in {name}") from exc


def _role_instructions(role: str) -> str:
    pieces: list[str] = []
    if role == MANAGER_ROLE:
        pieces.append(_required("SWARM_MANAGER_MCP_INSTRUCTIONS"))
    else:
        pieces.append(_required("SWARM_EXECUTOR_MCP_INSTRUCTIONS"))
    targets = ORDER_ACL.get(role, ())
    if targets:
        pieces.append(
            _format_template(
                "SWARM_AUTHORITY_MCP_INSTRUCTIONS",
                role=role,
                targets=", ".join(targets),
            )
        )
    return "\n\n".join(pieces)


ROLE_SERVERS = {
    role: FastMCP(
        f"swarm-{role}",
        instructions=_role_instructions(role),
        streamable_http_path=ROLE_STREAM_PATHS[role],
        transport_security=_transport_security(),
    )
    for role in ALL_ROLES
}


def _clean_text(value: str, field: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field} must not be empty")
    if len(value) > MAX_MESSAGE_CHARS:
        raise ValueError(f"{field} exceeds {MAX_MESSAGE_CHARS} characters")
    return value


async def _start_run(role: str, message: str, instructions: str) -> str:
    target = AGENTS[role]
    payload = {
        "model": HERMES_MODEL_ALIAS,
        "input": message,
        "instructions": instructions,
    }
    async with httpx.AsyncClient(
        timeout=float(API_TIMEOUT_SECONDS), follow_redirects=False
    ) as client:
        response = await client.post(
            f"{target.url}/v1/runs",
            json=payload,
            headers={"Authorization": f"Bearer {target.api_key}"},
        )
    if response.status_code != 202:
        logger.warning("Run dispatch to %s failed with HTTP %s", role, response.status_code)
        raise RuntimeError(f"{role} API returned HTTP {response.status_code}")
    data = response.json()
    run_id = data.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise RuntimeError(f"{role} API returned an invalid run identifier")
    return run_id


def _telegram_chunks(header: str, text: str) -> list[str]:
    limit = _positive_int("SWARM_TELEGRAM_MESSAGE_LIMIT")
    prefix = f"{header}\n"
    room = max(256, limit - len(prefix))
    return [prefix + text[index:index + room] for index in range(0, len(text), room)] or [header]


async def _telegram(sender: str, event: str, recipients: str, text: str) -> bool:
    if _required("SWARM_TELEGRAM_ENABLED").lower() not in {"1", "true", "yes", "on"}:
        return False
    group_id = _env("TELEGRAM_GROUP_ID")
    token = AGENTS[sender].telegram_bot_token
    if not group_id or not token:
        logger.warning("Telegram audit is not configured for sender %s", sender)
        return False
    proxy = _env("TELEGRAM_PROXY_URL") or None
    header = f"[{event}] {sender} -> {recipients}"
    try:
        async with httpx.AsyncClient(
            timeout=float(TELEGRAM_TIMEOUT_SECONDS),
            proxy=proxy,
            follow_redirects=False,
        ) as client:
            for chunk in _telegram_chunks(header, text):
                response = await client.post(
                    f"{_required('TELEGRAM_API_BASE_URL').rstrip('/')}/bot{token}/sendMessage",
                    json={
                        "chat_id": group_id,
                        "text": chunk,
                        "disable_web_page_preview": True,
                    },
                )
                if not response.is_success:
                    logger.warning(
                        "Telegram audit failed for %s with HTTP %s",
                        sender,
                        response.status_code,
                    )
                    return False
        return True
    except httpx.HTTPError:
        logger.exception("Telegram audit failed for sender %s", sender)
        return False


def _task_message(task_id: str, sender: str, command: str) -> str:
    return _format_template(
        "SWARM_ORDER_PROMPT_TEMPLATE",
        task_id=task_id,
        sender=sender,
        recipient=sender,
        message=command,
    )


def _hierarchy_payload(role: str) -> dict[str, Any]:
    targets = ORDER_ACL.get(role, ())
    return {
        "caller": role,
        "manager": MANAGER_ROLE,
        "roles": [
            {
                "role": item,
                "description": (
                    "Swarm-wide manager"
                    if item == MANAGER_ROLE
                    else EXECUTOR_DESCRIPTIONS[item]
                ),
                "may_order": list(ORDER_ACL.get(item, ())),
            }
            for item in ALL_ROLES
        ],
        "caller_may_order": list(targets),
        "caller_supervisors": list(_supervisors(role)) if role in AGENT_ROLES else [],
        "tools": {
            "single_target_order": bool(targets),
            "broadcast_order": role in GLOBAL_AUTHORITIES,
            "report": role in AGENT_ROLES,
            "peer_messages": role in AGENT_ROLES,
        },
    }


def _caller(expected: str) -> str:
    role = _caller_role.get()
    if role != expected:
        raise RuntimeError("Authenticated role does not match this MCP catalog")
    return role


async def _dispatch_order(authority: str, target: str, command: str) -> dict[str, Any]:
    _caller(authority)
    allowed = ORDER_ACL.get(authority, ())
    if target not in allowed:
        return {
            "ok": False,
            "error": f"{authority} is not authorized to order '{target}'",
            "allowed": list(allowed),
        }
    try:
        command = _clean_text(command, "command")
        task_id = f"task_{uuid.uuid4().hex}"
        run_id = await _start_run(
            target,
            _task_message(task_id, authority, command),
            _required("SWARM_EXECUTOR_RUN_INSTRUCTIONS"),
        )
        telegram = await _telegram(
            authority, "ORDER", target, f"{task_id}\n{command}"
        )
        return {
            "ok": True,
            "task_id": task_id,
            "authority": authority,
            "agent": target,
            "run_id": run_id,
            "telegram": telegram,
        }
    except (ValueError, RuntimeError, httpx.HTTPError) as exc:
        logger.exception("Order failed: %s -> %s", authority, target)
        return {"ok": False, "error": str(exc)}


async def _dispatch_order_all(authority: str, command: str) -> dict[str, Any]:
    _caller(authority)
    if authority not in GLOBAL_AUTHORITIES:
        return {"ok": False, "error": f"{authority} has no broadcast authority"}
    try:
        command = _clean_text(command, "command")
        task_id = f"task_{uuid.uuid4().hex}"
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}

    async def dispatch(target: str) -> tuple[str, str | None, str | None]:
        try:
            run_id = await _start_run(
                target,
                _task_message(task_id, authority, command),
                _required("SWARM_EXECUTOR_RUN_INSTRUCTIONS"),
            )
            return target, run_id, None
        except (RuntimeError, httpx.HTTPError) as exc:
            logger.exception("Broadcast order failed: %s -> %s", authority, target)
            return target, None, str(exc)

    targets = ORDER_ACL[authority]
    dispatched = await asyncio.gather(*(dispatch(target) for target in targets))
    telegram = await _telegram(
        authority, "ORDER_ALL", ", ".join(targets), f"{task_id}\n{command}"
    )
    results = {
        target: ({"run_id": run_id} if run_id else {"error": error})
        for target, run_id, error in dispatched
    }
    return {
        "ok": all(run_id for _, run_id, _ in dispatched),
        "task_id": task_id,
        "authority": authority,
        "results": results,
        "telegram": telegram,
    }


async def _dispatch_report(
    sender: str,
    summary: str,
    task_id: str,
    status: str,
    recipient: str,
) -> dict[str, Any]:
    _caller(sender)
    supervisors = _supervisors(sender)
    recipient = recipient.strip().lower() or MANAGER_ROLE
    if recipient not in supervisors:
        return {
            "ok": False,
            "error": f"{recipient} is not a supervisor of {sender}",
            "allowed": list(supervisors),
        }
    try:
        summary = _clean_text(summary, "summary")
        task_id = task_id.strip() or "untracked"
        status = status.strip().lower() or "completed"
        message = _format_template(
            "SWARM_REPORT_PROMPT_TEMPLATE",
            sender=sender,
            recipient=recipient,
            task_id=task_id,
            status=status,
            message=summary,
        )
        run_id = await _start_run(
            recipient,
            message,
            _required("SWARM_SUPERVISOR_REPORT_INSTRUCTIONS"),
        )
        telegram = await _telegram(
            sender,
            "REPORT",
            recipient,
            f"{task_id} [{status}]\n{summary}",
        )
        return {
            "ok": True,
            "recipient": recipient,
            "recipient_run_id": run_id,
            "task_id": task_id,
            "telegram": telegram,
        }
    except (ValueError, RuntimeError, httpx.HTTPError) as exc:
        logger.exception("Report failed: %s -> %s", sender, recipient)
        return {"ok": False, "error": str(exc)}


async def _peer_message(sender: str, target: str, message: str, message_id: str) -> str:
    body = _format_template(
        "SWARM_PEER_PROMPT_TEMPLATE",
        message_id=message_id,
        sender=sender,
        recipient=target,
        message=message,
    )
    return await _start_run(target, body, _required("SWARM_PEER_RUN_INSTRUCTIONS"))


async def _dispatch_message(sender: str, target: str, message: str) -> dict[str, Any]:
    _caller(sender)
    allowed = tuple(role for role in AGENT_ROLES if role != sender)
    if target not in allowed:
        return {
            "ok": False,
            "error": "Target must be another executor",
            "allowed": list(allowed),
        }
    try:
        message = _clean_text(message, "message")
        message_id = f"msg_{uuid.uuid4().hex}"
        run_id = await _peer_message(sender, target, message, message_id)
        telegram = await _telegram(sender, "MSG", target, f"{message_id}\n{message}")
        return {
            "ok": True,
            "message_id": message_id,
            "agent": target,
            "run_id": run_id,
            "telegram": telegram,
        }
    except (ValueError, RuntimeError, httpx.HTTPError) as exc:
        logger.exception("Peer message failed: %s -> %s", sender, target)
        return {"ok": False, "error": str(exc)}


async def _dispatch_message_all(sender: str, message: str) -> dict[str, Any]:
    _caller(sender)
    targets = tuple(role for role in AGENT_ROLES if role != sender)
    try:
        message = _clean_text(message, "message")
        message_id = f"msg_{uuid.uuid4().hex}"
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}

    async def dispatch(target: str) -> tuple[str, str | None, str | None]:
        try:
            run_id = await _peer_message(sender, target, message, message_id)
            return target, run_id, None
        except (RuntimeError, httpx.HTTPError) as exc:
            logger.exception("Broadcast message failed: %s -> %s", sender, target)
            return target, None, str(exc)

    dispatched = await asyncio.gather(*(dispatch(target) for target in targets))
    telegram = await _telegram(
        sender, "MSG_ALL", ", ".join(targets), f"{message_id}\n{message}"
    )
    results = {
        target: ({"run_id": run_id} if run_id else {"error": error})
        for target, run_id, error in dispatched
    }
    return {
        "ok": all(run_id for _, run_id, _ in dispatched),
        "message_id": message_id,
        "results": results,
        "telegram": telegram,
    }


def _register_hierarchy_resource(role: str, server: FastMCP) -> None:
    async def hierarchy() -> str:
        """Return the authenticated caller's hierarchy and capabilities."""
        _caller(role)
        return json.dumps(_hierarchy_payload(role), ensure_ascii=False, indent=2)

    hierarchy.__name__ = f"hierarchy_{_prefix(role).lower()}"
    server.resource(
        "swarm://hierarchy",
        name="hierarchy",
        title="Development swarm hierarchy",
        description="Role hierarchy, order ACL, supervisors, and caller capabilities.",
        mime_type="application/json",
    )(hierarchy)
    if role == MANAGER_ROLE:
        server.resource(
            "swarm://executors",
            name="executors",
            title="Development swarm executors",
            description="Compatibility alias for the authoritative hierarchy resource.",
            mime_type="application/json",
        )(hierarchy)


def _register_order_tools(role: str, server: FastMCP) -> None:
    allowed = ORDER_ACL.get(role, ())
    if not allowed:
        return
    AllowedTarget = Literal.__getitem__(allowed)

    async def role_order(agent: str, command: str) -> dict[str, Any]:
        """Assign a task to one executor authorized by the configured hierarchy."""
        return await _dispatch_order(role, agent.strip().lower(), command)

    role_order.__name__ = f"order_as_{_prefix(role).lower()}"
    role_order.__annotations__["agent"] = AllowedTarget
    server.tool(name="order")(role_order)

    if role in GLOBAL_AUTHORITIES:
        async def role_order_all(command: str) -> dict[str, Any]:
            """Assign the same task to every executor under this global authority."""
            return await _dispatch_order_all(role, command)

        role_order_all.__name__ = f"order_all_as_{_prefix(role).lower()}"
        server.tool(name="order_all")(role_order_all)


def _register_executor_tools(role: str, server: FastMCP) -> None:
    if role not in AGENT_ROLES:
        return
    supervisors = _supervisors(role)
    Supervisor = Literal.__getitem__(supervisors)

    async def report(
        summary: str,
        task_id: str = "",
        status: str = "completed",
        recipient: str = MANAGER_ROLE,
    ) -> dict[str, Any]:
        """Report progress or completion to an authorized supervisor."""
        return await _dispatch_report(role, summary, task_id, status, recipient)

    report.__name__ = f"report_as_{_prefix(role).lower()}"
    report.__annotations__["recipient"] = Supervisor
    server.tool(name="report")(report)

    async def msg_to(agent: str, message: str) -> dict[str, Any]:
        """Send a direct coordination message to another executor."""
        return await _dispatch_message(role, agent.strip().lower(), message)

    msg_to.__name__ = f"msg_to_as_{_prefix(role).lower()}"
    server.tool(name="msg_to")(msg_to)

    async def msg_all(message: str) -> dict[str, Any]:
        """Send the same coordination message to every other executor."""
        return await _dispatch_message_all(role, message)

    msg_all.__name__ = f"msg_all_as_{_prefix(role).lower()}"
    server.tool(name="msg_all")(msg_all)


for _role, _server in ROLE_SERVERS.items():
    _register_hierarchy_resource(_role, _server)
    _register_order_tools(_role, _server)
    _register_executor_tools(_role, _server)


def _role_for_token(supplied: str) -> str:
    if not supplied.startswith("Bearer "):
        return ""
    token = supplied[7:]
    for role, agent in AGENTS.items():
        if hmac.compare_digest(token, agent.mcp_token):
            return role
    return ""


async def _activity_dispatch(
    sender: str,
    target: str,
    event: str,
    turn_id: str,
    detail: str,
) -> dict[str, Any]:
    text = _format_template(
        "SWARM_ACTIVITY_PROMPT_TEMPLATE",
        sender=sender,
        recipient=target,
        event=event,
        turn_id=turn_id,
        message=detail,
    )
    try:
        run_id = await _start_run(
            target,
            text,
            _required("SWARM_ACTIVITY_RUN_INSTRUCTIONS"),
        )
        telegram = await _telegram(
            sender,
            "ACTIVITY",
            target,
            f"{event} turn={turn_id}\n{detail}".strip(),
        )
        return {"run_id": run_id, "telegram": telegram}
    except (RuntimeError, httpx.HTTPError) as exc:
        logger.exception("Activity signal failed: %s -> %s", sender, target)
        return {"error": str(exc)}


def main() -> None:
    import uvicorn

    _validate_configuration()

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        async with contextlib.AsyncExitStack() as stack:
            for server in ROLE_SERVERS.values():
                await stack.enter_async_context(server.session_manager.run())
            yield

    app = FastAPI(
        title="Swarm MCP Server",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        path = request.url.path
        if path == "/health":
            return await call_next(request)
        supplied_role = _role_for_token(request.headers.get("authorization", ""))
        if not supplied_role:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)

        expected_role = ""
        if path == ACTIVITY_SIGNAL_PATH:
            expected_role = supplied_role if supplied_role in AGENT_ROLES else ""
        else:
            for role, mount in ROLE_MOUNTS.items():
                if path == mount or path.startswith(mount + "/"):
                    expected_role = role
                    break
        if supplied_role != expected_role:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)

        context_token = _caller_role.set(supplied_role)
        try:
            return await call_next(request)
        finally:
            _caller_role.reset(context_token)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "roles": list(ALL_ROLES),
            "order_acl": {role: list(targets) for role, targets in ORDER_ACL.items()},
            "activity_routes": {
                role: list(targets) for role, targets in ACTIVITY_ROUTES.items()
            },
        }

    @app.post(ACTIVITY_SIGNAL_PATH)
    async def activity_signal(request: Request) -> JSONResponse:
        sender = _caller_role.get()
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            event = _clean_text(str(payload.get("event", "")), "event").lower()
            if event not in {"started", "completed", "failed"}:
                raise ValueError("event must be started, completed, or failed")
            turn_id = _clean_text(str(payload.get("turn_id", "unknown")), "turn_id")
            detail = str(payload.get("detail", "")).strip()[:MAX_MESSAGE_CHARS]
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

        targets = ACTIVITY_ROUTES.get(sender, ())
        if not targets:
            return JSONResponse({"ok": True, "sender": sender, "results": {}})
        dispatched = await asyncio.gather(
            *(
                _activity_dispatch(sender, target, event, turn_id, detail)
                for target in targets
            )
        )
        results = dict(zip(targets, dispatched, strict=True))
        ok = all("run_id" in result for result in dispatched)
        return JSONResponse(
            {"ok": ok, "sender": sender, "event": event, "results": results},
            status_code=202 if ok else 502,
        )

    for role, server in ROLE_SERVERS.items():
        app.mount(ROLE_MOUNTS[role], server.streamable_http_app())

    uvicorn.run(
        app,
        host=_required("SWARM_MCP_HOST"),
        port=_positive_int("SWARM_MCP_PORT"),
        log_level=_required("SWARM_UVICORN_LOG_LEVEL"),
    )


if __name__ == "__main__":
    main()
