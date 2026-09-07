#!/usr/bin/env python3
"""Deactivate a fixed set of 9router API keys after their recorded expiry."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import re
import urllib.error
import urllib.request
import uuid
from typing import Any


NAME = re.compile(r"[a-z0-9][a-z0-9-]{2,79}")


def env_values(path: Path) -> dict[str, str]:
    result = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        key, separator, value = line.partition("=")
        if separator:
            result[key] = value.strip().strip("\"'")
    return result


def b64url(value: bytes) -> bytes:
    return base64.urlsafe_b64encode(value).rstrip(b"=")


def admin_cookie(secret: str, now: datetime) -> str:
    timestamp = int(now.timestamp())
    header = b64url(json.dumps({"alg": "HS256"}, separators=(",", ":")).encode())
    payload = b64url(
        json.dumps(
            {"authenticated": True, "iat": timestamp, "exp": timestamp + 300},
            separators=(",", ":"),
        ).encode()
    )
    signing_input = header + b"." + payload
    signature = b64url(hmac.new(secret.encode(), signing_input, hashlib.sha256).digest())
    return "auth_token=" + (signing_input + b"." + signature).decode()


def parse_timestamp(raw: object, field: str) -> datetime:
    value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if value.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)


def parse_metadata(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    schema = raw.get("schema")
    if schema not in (1, 2) or not isinstance(raw.get("keys"), list):
        raise ValueError("unsupported expiry metadata")
    shared_expiry = (
        parse_timestamp(raw.get("expiresAt"), "expiresAt") if schema == 1 else None
    )
    keys = []
    for item in raw["keys"]:
        key_id, name = str(item.get("id", "")), str(item.get("name", ""))
        uuid.UUID(key_id)
        if not NAME.fullmatch(name):
            raise ValueError(f"invalid key name: {name!r}")
        expires = shared_expiry or parse_timestamp(
            item.get("expiresAt"), f"expiresAt for {name}"
        )
        keys.append({"id": key_id, "name": name, "expiresAt": expires})
    if not keys or len({item["id"] for item in keys}) != len(keys):
        raise ValueError("expiry metadata must contain unique keys")
    if len({item["name"] for item in keys}) != len(keys):
        raise ValueError("expiry metadata must contain unique names")
    return keys


def deactivate(base_url: str, cookie: str, item: dict[str, str]) -> None:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/keys/{item['id']}",
        data=b'{"isActive":false}',
        headers={"Cookie": cookie, "Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return
        raise RuntimeError(f"9router rejected deactivation for {item['name']}: HTTP {exc.code}") from None
    returned = payload.get("key") or {}
    if (
        returned.get("id") != item["id"]
        or returned.get("name") != item["name"]
        or returned.get("isActive") not in (False, 0)
    ):
        raise RuntimeError(f"9router did not deactivate {item['name']}")


def active_targets(
    base_url: str, cookie: str, expected: list[dict[str, str]]
) -> list[dict[str, str]]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/keys",
        headers={"Cookie": cookie},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"9router rejected key inventory: HTTP {exc.code}") from None
    current = payload.get("keys") if isinstance(payload, dict) else None
    if not isinstance(current, list):
        raise RuntimeError("9router returned an invalid key inventory")
    names = {
        str(item.get("id")): str(item.get("name"))
        for item in current
        if isinstance(item, dict) and item.get("id")
    }
    targets = []
    for item in expected:
        current_name = names.get(item["id"])
        if current_name is None:
            continue  # A key already deleted by an operator is safely expired.
        if current_name != item["name"]:
            raise RuntimeError(
                f"9router key id for {item['name']} now belongs to {current_name}"
            )
        targets.append(item)
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--router-env", type=Path, required=True)
    parser.add_argument("--base-url", default="http://10.9.9.1:20128")
    args = parser.parse_args()
    try:
        keys = parse_metadata(args.metadata)
        now = datetime.now(timezone.utc)
        expired = [item for item in keys if now >= item["expiresAt"]]
        if not expired:
            next_expiry = min(item["expiresAt"] for item in keys)
            print(f"9router keys remain active; next expiry is {next_expiry.isoformat()}")
            return
        secret = env_values(args.router_env).get("JWT_SECRET", "")
        if not secret:
            raise ValueError("router environment has no JWT_SECRET")
        cookie = admin_cookie(secret, now)
        targets = active_targets(args.base_url, cookie, expired)
        for item in targets:
            deactivate(args.base_url, cookie, item)
        print(f"deactivated {len(targets)} expired 9router key(s)")
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"9router key expiry failed: {exc}") from None


if __name__ == "__main__":
    main()
