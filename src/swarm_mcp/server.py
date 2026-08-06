"""swarm-mcp — MCP server for Hermes swarm orchestration.

Exposes a single `order` tool that lets the manager send commands to
sub-agents via their webhook API and WAIT for the agent's response.

Architecture:
  Manager agent ──MCP order(agent, command)──→ swarm-mcp (:3004)
                                                  ↓ POST /webhooks/manager-command
                                              Sub-agent Hermes webhook (:8644-8648)
                                                  ↓ agent processes command
                                              Response read back from agent's
                                              state.db (SQLite) and returned.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────
# Agent webhook ports (same as dispatch.sh)
AGENT_PORTS: dict[str, int] = {
    "kb-organizer": 8645,
    "analyst": 8646,
    "contactor": 8647,
    "secretary": 8644,
    "manager": 8648,
}

# Where each agent's state.db lives on the host (mounted from /opt/data)
AGENT_DATA_DIRS: dict[str, str] = {
    "kb-organizer": "/opt/hermes/agents/kb-organizer",
    "analyst": "/opt/hermes/agents/analyst",
    "contactor": "/opt/hermes/agents/contactor",
    "secretary": "/opt/hermes/agents/secretary",
    "manager": "/opt/hermes/agents/manager",
}

WEBHOOK_PATH = "/webhooks/manager-command"

# ── MCP Server ─────────────────────────────────────────────────────
mcp = FastMCP(
    "swarm",
    instructions=(
        "Инструменты оркестрации роя Hermes. "
        "order() — передать приказ агенту и дождаться его ответа."
    ),
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)


def _agent_state_db(agent: str) -> str | None:
    """Path to agent's state.db, or None if unknown agent."""
    data_dir = AGENT_DATA_DIRS.get(agent)
    if not data_dir:
        return None
    path = os.path.join(data_dir, "state.db")
    return path if os.path.exists(path) else None


def _last_agent_response(agent: str) -> str | None:
    """Read the last assistant response from the agent's state.db.

    Returns the text of the most recent assistant message, or None.
    """
    db_path = _agent_state_db(agent)
    if not db_path:
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT m.content FROM messages m
            JOIN sessions s ON s.id = m.session_id
            WHERE m.role = 'assistant' AND m.content IS NOT NULL
              AND m.content != ''
            ORDER BY m.timestamp DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.warning("state.db read failed for %s: %s", agent, e)
        return None


def _last_agent_session_time(agent: str) -> float | None:
    """Timestamp of the agent's most recent session (started_at)."""
    db_path = _agent_state_db(agent)
    if not db_path:
        return None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=2)
        cur = conn.cursor()
        cur.execute("SELECT MAX(started_at) FROM sessions")
        row = cur.fetchone()
        conn.close()
        return row[0] if row and row[0] else None
    except Exception:
        return None


@mcp.tool()
async def order(agent: str, command: str, wait_s: int = 60) -> str:
    """Передать приказ агенту роя и дождаться его ответа.

    Отправляет команду через webhook Hermes указанному агенту, ждёт
    обработки (по умолчанию до 60 секунд) и возвращает ответ агента.

    Args:
        agent: Имя агента-адресата: kb-organizer, analyst, contactor,
            secretary или manager
        command: Текст приказа для агента
        wait_s: Сколько секунд ждать ответ (максимум 300)

    Returns:
        Статус отправки + ответ агента (если успел обработать).
    """
    agent = agent.strip().lower()
    port = AGENT_PORTS.get(agent)
    if not port:
        return f"Error: неизвестный агент '{agent}'. Доступные: {', '.join(AGENT_PORTS)}"

    wait_s = max(0, min(int(wait_s), 300))

    # Record current state before sending (to detect new session)
    before_time = _last_agent_session_time(agent)

    # Send command via webhook
    url = f"http://127.0.0.1:{port}{WEBHOOK_PATH}"
    payload = json.dumps({"command": command}, ensure_ascii=False)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                url,
                content=payload,
                headers={"Content-Type": "application/json"},
            )
    except Exception as e:
        return f"Error: не удалось отправить приказ: {e}"

    if resp.status_code != 202:
        return (
            f"Error: webhook вернул {resp.status_code}: {resp.text[:200]}"
        )

    # Poll for a new assistant response
    deadline = time.monotonic() + wait_s
    last_seen = None
    while time.monotonic() < deadline:
        # Wait a bit before first poll — agent needs time to start
        await asyncio.sleep(3)
        session_time = _last_agent_session_time(agent)
        if session_time and before_time and session_time <= before_time:
            continue  # no new session yet
        response = _last_agent_response(agent)
        if response and response != last_seen:
            last_seen = response
            # New response found — return it (session likely still running
            # but we have a meaningful answer)
            break

    if last_seen:
        return (
            f"✅ Приказ отправлен агенту {agent} (HTTP 202).\n\n"
            f"📨 Ответ агента:\n{last_seen[:2000]}"
        )

    return (
        f"✅ Приказ отправлен агенту {agent} (HTTP 202), но ответ "
        f"не получен за {wait_s}с. Проверь статус позже через "
        f"faracrm_get_chat_messages в общем чате."
    )


# ── Entrypoint ─────────────────────────────────────────────────────


def main() -> None:
    import contextlib

    import uvicorn
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        async with mcp.session_manager.run():
            yield

    app = FastAPI(title="Swarm MCP Server", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    mcp_app = mcp.streamable_http_app()
    app.mount("/", mcp_app)

    port = int(os.getenv("MCP_PORT", "3004"))
    host = os.getenv("MCP_HOST", "0.0.0.0")

    logger.info("Starting Swarm MCP server on %s:%d", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
