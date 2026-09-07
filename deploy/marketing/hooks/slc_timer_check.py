#!/usr/bin/env python3
"""SLC Timer Check — polls SLC for due reminders and injects them.

Runs as a periodic task inside each agent. Checks SLC for pending
reminders that are due, and returns them as actionable context.
"""
import json
import sys
import os
import urllib.request
from datetime import datetime, UTC

SLC_URL = os.environ.get("SLC_MCP_URL", "http://agent-sales-0:3000/mcp")
SEAT_ID = os.environ.get("SEAT_ID", "default")


def call_mcp(method, params=None):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    try:
        req = urllib.request.Request(
            SLC_URL,
            data=json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "X-Seat-ID": SEAT_ID,
                **({"Authorization": "Bearer " + os.environ["SLC_MCP_TOKEN"].strip()}
                   if os.environ.get("SLC_MCP_TOKEN", "").strip() else {}),
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def get_due_reminders():
    """Get reminders that are due now."""
    result = call_mcp("tools/call", {
        "name": "list_reminders",
        "arguments": {"status": "pending"}
    })
    if "result" not in result:
        return []
    
    content = result["result"].get("content", [])
    if not content:
        return []
    
    text = content[0].get("text", "")
    # Parse reminders from text - look for due ones
    due = []
    now = datetime.now(UTC)
    for line in text.split("\n"):
        if "⏰" in line or "due" in line.lower() or "напоминание" in line.lower():
            due.append(line.strip())
    return due


def get_active_tasks():
    """Get active tasks from SLC."""
    result = call_mcp("tools/call", {
        "name": "get_active_task",
        "arguments": {}
    })
    if "result" not in result:
        return None
    content = result["result"].get("content", [])
    if content:
        return content[0].get("text", "")
    return None


def main():
    due_reminders = get_due_reminders()
    active_task = get_active_tasks()
    
    output = []
    if due_reminders:
        output.append("🔔 DUE REMINDERS:")
        for r in due_reminders[:5]:
            output.append(f"  - {r}")
    
    if active_task:
        output.append(f"📋 ACTIVE TASK: {active_task[:200]}")
    
    if output:
        print("\n".join(output))
    else:
        print("OK: no pending reminders")


if __name__ == "__main__":
    main()
