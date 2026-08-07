"""Archived probe for passive, role-scoped lifecycle activity."""
from __future__ import annotations

import asyncio
import json
import os
import uuid

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


def _prefix(role: str) -> str:
    return role.upper().replace("-", "_")


async def _read_activity(base: str, role: str) -> dict[str, object]:
    path = os.environ["SWARM_ROLE_MCP_PATH_TEMPLATE"].format(role=role)
    token = os.environ[f"{_prefix(role)}_SWARM_MCP_TOKEN"]
    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"}
    ) as client:
        async with streamable_http_client(base + path, http_client=client) as (
            read,
            write,
            _,
        ):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.read_resource("swarm://activity")
    if not result.contents or not hasattr(result.contents[0], "text"):
        raise RuntimeError("swarm://activity returned no text content")
    payload = json.loads(result.contents[0].text)
    if not isinstance(payload, dict):
        raise RuntimeError("swarm://activity returned a non-object payload")
    return payload


async def main() -> None:
    routes = json.loads(os.environ["SWARM_ACTIVITY_ROUTES"])
    route = next(
        (
            (str(sender), str(target))
            for sender, targets in routes.items()
            for target in targets
        ),
        None,
    )
    if route is None:
        print(json.dumps({"ok": True, "skipped": "no activity routes"}))
        return

    sender, target = route
    port = os.environ["SWARM_MCP_PORT"]
    base = f"http://127.0.0.1:{port}"
    turn_id = f"activity_probe_{uuid.uuid4().hex}"
    token = os.environ[f"{_prefix(sender)}_SWARM_MCP_TOKEN"]
    async with httpx.AsyncClient() as client:
        response = await client.post(
            base + os.environ["SWARM_ACTIVITY_SIGNAL_PATH"],
            headers={"Authorization": f"Bearer {token}"},
            json={
                "event": "completed",
                "turn_id": turn_id,
                "detail": "Passive activity deployment probe.",
            },
        )
    response.raise_for_status()
    delivery = response.json()
    if delivery.get("disabled"):
        print(json.dumps({"ok": True, "disabled": True}, sort_keys=True))
        return
    if delivery.get("mode") != "record-only":
        raise RuntimeError("Activity endpoint is not in record-only mode")

    snapshot = await _read_activity(base, target)
    matching = [
        event
        for event in snapshot.get("recent", [])
        if event.get("sender") == sender and event.get("turn_id") == turn_id
    ]
    if snapshot.get("mode") != "record-only" or not matching:
        raise RuntimeError("Activity signal was not visible to its supervisor")
    print(
        json.dumps(
            {
                "ok": True,
                "mode": snapshot["mode"],
                "sender": sender,
                "target": target,
                "turn_id": turn_id,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
