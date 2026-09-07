#!/usr/bin/env python3
"""Hermes lifecycle hook that loads and saves the role's SLC context."""

from __future__ import annotations

import json
import os
import re
import sys
import uuid
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import httpx


SLC_MCP_URL = os.environ["SLC_MCP_URL"]
SLC_SEAT_ID = os.environ["SLC_SEAT_ID"]
AUDIT_PATH = Path("/opt/data/logs/slc-context-hooks.jsonl")
STATE_DIR = AUDIT_PATH.parent / ".slc-context-hooks"
SUMMARY_LIMIT = int(os.environ.get("SLC_CONTEXT_SUMMARY_LIMIT", "6000"))
MARKER_MAX_AGE_SECONDS = 7 * 24 * 60 * 60

_INLINE_SECRET_PATTERNS = (
    re.compile(r"(?i)(\b(?:api[_-]?key|token|password|secret)\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)(\bBearer\s+)([^\s,;]+)"),
    re.compile(r"(?i)([?&](?:api[_-]?key|token|password|secret)=)([^&\s]+)"),
    re.compile(
        r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----",
        re.DOTALL,
    ),
)


def _payload() -> dict[str, Any]:
    raw = sys.stdin.read()
    data = json.loads(raw or "{}")
    return data if isinstance(data, dict) else {}


def _extra(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("extra")
    return value if isinstance(value, dict) else {}


def _marker(turn_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", turn_id or "unknown")[:160]
    return STATE_DIR / f"{safe}.saved"


def _redact_sensitive(value: str) -> str:
    redacted = value
    secret_values = {
        raw
        for name, raw in os.environ.items()
        if raw
        and len(raw) >= 8
        and name.upper().endswith(("_API_KEY", "_TOKEN", "_PASSWORD", "_SECRET"))
    }
    for secret in sorted(secret_values, key=len, reverse=True):
        redacted = redacted.replace(secret, "[REDACTED]")
    for pattern in _INLINE_SECRET_PATTERNS:
        if "PRIVATE KEY" in pattern.pattern:
            redacted = pattern.sub("[REDACTED PRIVATE KEY]", redacted)
        else:
            redacted = pattern.sub(r"\1[REDACTED]", redacted)
    return redacted


def _cleanup_markers() -> None:
    if not STATE_DIR.exists():
        return
    cutoff = datetime.now(UTC).timestamp() - MARKER_MAX_AGE_SECONDS
    for marker in STATE_DIR.glob("*.saved"):
        try:
            if marker.stat().st_mtime < cutoff:
                marker.unlink()
        except FileNotFoundError:
            continue


def _audit(event: str, turn_id: str, status: str, error: str = "") -> None:
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": event,
        "seat_id": SLC_SEAT_ID,
        "turn_id": turn_id,
        "status": status,
    }
    if error:
        entry["error"] = _redact_sensitive(error)[:500]
    with AUDIT_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _call_tool(name: str, arguments: dict[str, Any]) -> str:
    """Perform one bounded stateless JSON-RPC tool call.

    The Rust SLC MCP accepts streamable-HTTP JSON-RPC without allocating a
    transport session.  This is intentional for short-lived lifecycle hooks:
    there is no SSE receive task to keep alive and no session to delete.
    """
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "X-Seat-ID": SLC_SEAT_ID,
    }
    token = os.environ.get("SLC_MCP_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with httpx.Client(timeout=httpx.Timeout(45.0, connect=10.0)) as client:
        response = client.post(
            SLC_MCP_URL,
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
        )
        response.raise_for_status()
        body = response.text
        if body.lstrip().startswith("data:"):
            body = "\n".join(
                line[5:].strip() for line in body.splitlines() if line.startswith("data:")
            )
        payload = json.loads(body or "{}")
        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        result = payload.get("result", {})
        if result.get("isError"):
            details = "\n".join(
                str(item.get("text", ""))
                for item in result.get("content", [])
                if isinstance(item, dict) and item.get("text")
            )
            raise RuntimeError(details or f"SLC tool {name} failed")
        content = result.get("content", [])
        return "\n".join(
            str(item.get("text", ""))
            for item in content
            if isinstance(item, dict) and item.get("text")
        )


def _summary(extra: dict[str, Any], *, fallback: bool = False) -> str:
    task_id = str(extra.get("task_id") or "")
    user_message = str(extra.get("user_message") or "")
    response = str(extra.get("assistant_response") or "")
    if response:
        parts = [f"Task {task_id}" if task_id else "Agent loop completed"]
        if user_message:
            parts.append(f"Request: {user_message}")
        parts.append(f"Result: {response}")
        return _redact_sensitive("\n\n".join(parts))[:SUMMARY_LIMIT]

    status = "interrupted" if extra.get("interrupted") else "failed" if extra.get("failed") else "completed"
    reason = str(extra.get("turn_exit_reason") or "")
    text = f"Agent loop {status}"
    if task_id:
        text += f" for task {task_id}"
    if reason:
        text += f". Exit reason: {reason}"
    if fallback:
        text += ". Saved by the session-end fallback hook."
    return _redact_sensitive(text)[:SUMMARY_LIMIT]


def _run(payload: dict[str, Any]) -> None:
    event = str(payload.get("hook_event_name") or "")
    extra = _extra(payload)
    turn_id = str(extra.get("turn_id") or "")
    _cleanup_markers()
    _audit(event, turn_id, "started")

    if event == "pre_llm_call":
        context = _call_tool(
            "update_context",
            {"include_base_docs": True},
        )

        # Load active task, focuses, and recent memory for workflow state tracking
        extra_context_parts = []
        try:
            active_task = _call_tool("get_active_task", {})
            if active_task and active_task.strip() and "null" not in active_task.lower():
                extra_context_parts.append(f"ACTIVE TASK:\n{active_task}")
        except Exception:
            pass

        try:
            focuses = _call_tool("focus_list", {})
            if focuses and focuses.strip() and "null" not in focuses.lower() and "[]" not in focuses:
                extra_context_parts.append(f"ACTIVE FOCUSES:\n{focuses}")
        except Exception:
            pass

        try:
            recent = _call_tool("recall", {"limit": 5})
            if recent and recent.strip():
                extra_context_parts.append(f"RECENT MEMORY:\n{recent}")
        except Exception:
            pass

        full_context = f"SLC context for {SLC_SEAT_ID}:\n{context}"
        if extra_context_parts:
            full_context += "\n\n--- WORKFLOW STATE ---\n" + "\n\n".join(extra_context_parts)

        print(json.dumps({"context": full_context}))
        _audit(event, turn_id, "completed")
        return

    if event == "post_llm_call":
        _call_tool(
            "save_context",
            {
                "summary": _summary(extra),
                "include_base_docs": True,
            },
        )
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        _marker(turn_id).touch()
        _audit(event, turn_id, "completed")
        return

    if event in {"on_session_end", "api_request_error"}:
        marker = _marker(turn_id)
        if not marker.exists():
            summary_extra = dict(extra)
            if event == "api_request_error":
                summary_extra["failed"] = True
                summary_extra["turn_exit_reason"] = (
                    extra.get("error_message") or extra.get("error_type") or "provider request error"
                )
            _call_tool(
                "save_context",
                {
                    "summary": _summary(summary_extra, fallback=True),
                    "include_base_docs": True,
                },
            )
        marker.unlink(missing_ok=True)
        _audit(event, turn_id, "completed")


def _runner_payload(action: str, turn_id: str, exit_code: int = 0) -> dict[str, Any]:
    if action == "runner-start":
        return {
            "hook_event_name": "pre_llm_call",
            "extra": {"turn_id": turn_id, "task_id": f"runner:{os.environ.get('SWARM_ROLE', 'agent')}"},
        }
    return {
        "hook_event_name": "post_llm_call",
        "extra": {
            "turn_id": turn_id,
            "task_id": f"runner:{os.environ.get('SWARM_ROLE', 'agent')}",
            "assistant_response": f"Delegated coding runner finished with exit code {exit_code}; the outer agent iteration records task evidence and decisions.",
            "failed": exit_code != 0,
        },
    }


def main() -> int:
    payload: dict[str, Any] = {}
    try:
        if len(sys.argv) > 1 and sys.argv[1] in {"runner-start", "runner-end"}:
            turn_id = sys.argv[2] if len(sys.argv) > 2 else f"runner-{uuid.uuid4().hex}"
            exit_code = int(sys.argv[3]) if len(sys.argv) > 3 else 0
            payload = _runner_payload(sys.argv[1], turn_id, exit_code)
        else:
            payload = _payload()
        _run(payload)
        return 0
    except Exception as exc:
        try:
            extra = _extra(payload)
            _audit(
                str(payload.get("hook_event_name") or "unknown"),
                str(extra.get("turn_id") or ""),
                "failed",
                f"{type(exc).__name__}: {exc}",
            )
        except Exception:
            pass
        # Never echo remote response bodies or URLs containing credentials.
        print(f"SLC context hook failed: {type(exc).__name__}; inspect protected audit log", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
