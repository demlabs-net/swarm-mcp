"""Archived Python probe for role-specific MCP catalogs and endpoint isolation."""
from __future__ import annotations

import asyncio
import json
import os

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


def _prefix(role: str) -> str:
    return role.upper().replace("-", "_")


def _roles() -> tuple[str, ...]:
    manager = os.environ["SWARM_MANAGER_ROLE"].strip().lower()
    executors = tuple(
        role.strip().lower()
        for role in os.environ["SWARM_AGENT_ROLES"].split(",")
        if role.strip()
    )
    return (manager, *executors)


def _path(role: str) -> str:
    return os.environ["SWARM_ROLE_MCP_PATH_TEMPLATE"].format(role=role)


async def _catalog(base: str, role: str) -> tuple[list[str], list[str]]:
    token = os.environ[f"{_prefix(role)}_SWARM_MCP_TOKEN"]
    async with httpx.AsyncClient(headers={"Authorization": f"Bearer {token}"}) as client:
        async with streamable_http_client(base + _path(role), http_client=client) as (
            read,
            write,
            _,
        ):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                resources = await session.list_resources()
                return (
                    sorted(tool.name for tool in tools.tools),
                    sorted(str(resource.uri) for resource in resources.resources),
                )


def _expected_tools(role: str, manager: str, executors: tuple[str, ...]) -> list[str]:
    acl = json.loads(os.environ["SWARM_ORDER_ACL"])
    configured = acl.get(role, [])
    tools: list[str] = [] if role == manager else ["msg_all", "msg_to", "report"]
    if configured:
        tools.append("order")
    if configured == ["*"]:
        tools.append("order_all")
    return sorted(tools)


def _expected_resources(role: str, manager: str) -> list[str]:
    acl = json.loads(os.environ["SWARM_ORDER_ACL"])
    resources = ["swarm://hierarchy"]
    if role == manager:
        resources.append("swarm://executors")
    if acl.get(role):
        resources.append("swarm://activity")
    return sorted(resources)


async def main() -> None:
    port = os.environ["SWARM_MCP_PORT"]
    base = f"http://127.0.0.1:{port}"
    roles = _roles()
    manager, executors = roles[0], roles[1:]
    catalogs = await asyncio.gather(*(_catalog(base, role) for role in roles))

    results: dict[str, object] = {}
    ok = True
    for role, (tools, resources) in zip(roles, catalogs, strict=True):
        expected = _expected_tools(role, manager, executors)
        expected_resources = _expected_resources(role, manager)
        role_ok = tools == expected and resources == expected_resources
        results[role] = {
            "tools": tools,
            "expected": expected,
            "resources": resources,
            "expected_resources": expected_resources,
            "ok": role_ok,
        }
        ok = ok and role_ok

    # Every role token must fail against another role's catalog.
    cross_auth_rejected = True
    async with httpx.AsyncClient(follow_redirects=False) as client:
        for index, role in enumerate(roles):
            other = roles[(index + 1) % len(roles)]
            response = await client.get(
                base + _path(other),
                headers={
                    "Authorization": f"Bearer {os.environ[f'{_prefix(role)}_SWARM_MCP_TOKEN']}"
                },
            )
            cross_auth_rejected = cross_auth_rejected and response.status_code == 401

    result = {"roles": results, "cross_auth_rejected": cross_auth_rejected}
    print(json.dumps(result, sort_keys=True))
    if not ok or not cross_auth_rejected:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
