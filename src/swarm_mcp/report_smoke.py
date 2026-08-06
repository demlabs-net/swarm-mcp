"""End-to-end report delivery smoke test without invoking an executor LLM."""
from __future__ import annotations

import asyncio
import json
import os
import sys

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


def _prefix(role: str) -> str:
    return role.upper().replace("-", "_")


async def main() -> None:
    role = sys.argv[1] if len(sys.argv) > 1 else "developer"
    base = f"http://127.0.0.1:{os.environ['SWARM_MCP_PORT']}"
    url = base + os.environ["SWARM_ROLE_MCP_PATH_TEMPLATE"].format(role=role)
    token = os.environ[f"{_prefix(role)}_SWARM_MCP_TOKEN"]
    arguments = {
        "task_id": "smoke_test",
        "status": "completed",
        "summary": "Swarm MCP report and Telegram audit smoke test passed.",
    }
    async with httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"}) as client:
        async with streamable_http_client(url, http_client=client) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("report", arguments)
                payload = result.structuredContent or {
                    "content": [getattr(item, "text", "") for item in result.content]
                }
                print(json.dumps(payload, sort_keys=True))
                if result.isError:
                    raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
