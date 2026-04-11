"""loom plugin for Hermes Agent.

Exposes CSV-backed data operations as agent tools.
The repo root is read from LOOM_ROOT env var (defaults to cwd).
"""
from __future__ import annotations

import json
import os
from pathlib import Path


def _root() -> Path:
    return Path(os.environ.get("LOOM_ROOT", ".")).resolve()


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _catalog(args: dict, **_) -> str:
    root = _root()
    catalog_path = root / "catalog.yaml"
    schema_path = root / "schema.yaml"
    parts = []
    if catalog_path.exists():
        parts.append(f"# catalog.yaml\n{catalog_path.read_text()}")
    if schema_path.exists():
        parts.append(f"# schema.yaml\n{schema_path.read_text()}")
    if not parts:
        return json.dumps({"error": "No catalog.yaml or schema.yaml found in data root."})
    return "\n\n".join(parts)


def _query(args: dict, **_) -> str:
    from loom.core.store import read_table, query_rows
    root = _root()
    table = args["table"]
    filters = args.get("filters") or {}
    fields = args.get("fields")
    search = args.get("search")
    sort_by = args.get("sort_by")
    limit = args.get("limit")
    offset = args.get("offset")
    try:
        rows = read_table(root, table)
        result = query_rows(
            rows,
            filters or None,
            fields or None,
            search=search,
            sort_by=sort_by,
            limit=limit,
            offset=offset,
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _add(args: dict, **_) -> str:
    from loom.core.schema import Schema, SchemaError
    from loom.core.store import apply_auto_fields, read_table, write_table
    root = _root()
    table = args["table"]
    row = args["data"]
    try:
        schema = Schema.load(root)
        col_defs = schema.columns(table)
        row = apply_auto_fields(col_defs, row, is_new=True)
        errors = schema.validate_row(table, row)
        if errors:
            return json.dumps({"error": "Validation failed", "details": errors})
    except SchemaError:
        pass
    rows = read_table(root, table)
    rows.append(row)
    write_table(root, table, rows)
    return json.dumps({"ok": True, "row": row, "committed": False})


def _update(args: dict, **_) -> str:
    from loom.core.schema import Schema, SchemaError
    from loom.core.store import apply_auto_fields, find_row, read_table, write_table
    root = _root()
    table = args["table"]
    row_id = args["id"]
    patch = args["data"]
    rows = read_table(root, table)
    idx, existing = find_row(rows, row_id)
    if existing is None:
        return json.dumps({"error": f"Row not found: id={row_id}"})
    updated = {**existing, **patch}
    try:
        schema = Schema.load(root)
        col_defs = schema.columns(table)
        updated = apply_auto_fields(col_defs, updated, is_new=False)
        errors = schema.validate_row(table, updated)
        if errors:
            return json.dumps({"error": "Validation failed", "details": errors})
    except SchemaError:
        pass
    rows[idx] = updated
    write_table(root, table, rows)
    return json.dumps({"ok": True, "row": updated, "committed": False})


def _delete(args: dict, **_) -> str:
    from loom.core.store import find_row, read_table, write_table
    root = _root()
    table = args["table"]
    row_id = args["id"]
    rows = read_table(root, table)
    idx, existing = find_row(rows, row_id)
    if existing is None:
        return json.dumps({"error": f"Row not found: id={row_id}"})
    rows.pop(idx)
    write_table(root, table, rows)
    return json.dumps({"ok": True, "deleted_id": row_id, "committed": False})


def _sync(args: dict, **_) -> str:
    from loom.core.git_ops import GitError, commit_changes, push, sync as git_sync
    root = _root()
    try:
        sha = commit_changes(root, "data: auto-commit before sync")
        result = git_sync(root)
        if result["status"] == "ok":
            push(root)
        if sha:
            result["auto_committed"] = sha
        return json.dumps(result, ensure_ascii=False)
    except GitError as e:
        return json.dumps({"error": str(e)})


def _conflicts(args: dict, **_) -> str:
    conflicts_file = _root() / ".loom_conflicts.json"
    if not conflicts_file.exists():
        return json.dumps([])
    return conflicts_file.read_text()


def _commit(args: dict, **_) -> str:
    from loom.core.git_ops import commit_changes
    root = _root()
    message = args.get("message") or "data: save changes"
    sha = commit_changes(root, message)
    if not sha:
        return json.dumps({"ok": True, "commit": None, "message": "Nothing to commit."})
    return json.dumps({"ok": True, "commit": sha})


def _resolve(args: dict, **_) -> str:
    from loom.core.store import find_row, read_table, write_table
    from loom.core.git_ops import commit_changes
    root = _root()
    conflict_id = args["conflict_id"]
    value = args["value"]
    conflicts_file = root / ".loom_conflicts.json"
    items = json.loads(conflicts_file.read_text()) if conflicts_file.exists() else []
    target = next((c for c in items if c["id"] == conflict_id), None)
    if target is None:
        return json.dumps({"error": f"Conflict not found: {conflict_id}"})
    rows = read_table(root, target["table"])
    idx, row = find_row(rows, target["row_id"])
    if row is None:
        return json.dumps({"error": f"Row not found: {target['row_id']}"})
    rows[idx][target["field"]] = value
    write_table(root, target["table"], rows)
    remaining = [c for c in items if c["id"] != conflict_id]
    if remaining:
        conflicts_file.write_text(json.dumps(remaining, ensure_ascii=False, indent=2))
    else:
        conflicts_file.unlink(missing_ok=True)
    sha = commit_changes(root, f"resolve conflict {conflict_id} -> {value}")
    return json.dumps({"ok": True, "conflict_id": conflict_id, "value": value, "commit": sha})


def _stats(args: dict, **_) -> str:
    from loom.core.store import read_table, aggregate_rows
    root = _root()
    table = args["table"]
    group_by = args.get("group_by")
    agg = args.get("agg")
    try:
        rows = read_table(root, table)
        result = aggregate_rows(rows, group_by=group_by, agg=agg)
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _discover(args: dict, **_) -> str:
    """Fetch the Agent Card from a remote loom A2A agent."""
    import httpx
    agent_url = args["agent_url"].rstrip("/")
    try:
        resp = httpx.get(f"{agent_url}/.well-known/agent.json", timeout=10)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        return json.dumps({"error": str(e)})


def _call(args: dict, **_) -> str:
    """Send a natural language query to a remote loom A2A agent."""
    import uuid
    import httpx
    agent_url = args["agent_url"].rstrip("/")
    query = args["query"]
    task_id = str(uuid.uuid4())
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "message/send",
        "params": {
            "message": {
                "role": "user",
                "parts": [{"kind": "text", "text": query}],
                "messageId": task_id,
            }
        },
    }
    try:
        resp = httpx.post(agent_url, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        # Extract text from result artifacts
        result = data.get("result", {})
        artifacts = result.get("artifacts", [])
        texts = []
        for artifact in artifacts:
            for part in artifact.get("parts", []):
                if part.get("kind") == "text":
                    texts.append(part["text"])
        if texts:
            return "\n".join(texts)
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def register(ctx):
    ctx.register_tool(
        name="loom_catalog",
        toolset="loom",
        emoji="📋",
        description="Read catalog.yaml and schema.yaml to understand available tables and their structure. Call this first before any data operation.",
        schema={
            "name": "loom_catalog",
            "description": "Read catalog.yaml and schema.yaml to understand available tables, columns, and relationships. Always call this first.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        handler=_catalog,
    )

    ctx.register_tool(
        name="loom_query",
        toolset="loom",
        emoji="🔍",
        description="Query rows from a table with filters, search, sorting, and pagination.",
        schema={
            "name": "loom_query",
            "description": "Query rows from a table. Supports exact filters, fuzzy search, sorting, and pagination. Returns a JSON array.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                    "filters": {
                        "type": "object",
                        "description": "Key-value pairs to filter by exact match, e.g. {\"status\": \"active\"}",
                    },
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to return. Omit to return all.",
                    },
                    "search": {
                        "type": "string",
                        "description": "Fuzzy search term — matches any field containing this text (case-insensitive).",
                    },
                    "sort_by": {
                        "type": "string",
                        "description": "Sort by field name. Prefix with - for descending, e.g. \"-created_at\".",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of rows to return.",
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Skip first N rows (for pagination).",
                    },
                },
                "required": ["table"],
            },
        },
        handler=_query,
    )

    ctx.register_tool(
        name="loom_add",
        toolset="loom",
        emoji="➕",
        description="Add a new row to a table.",
        schema={
            "name": "loom_add",
            "description": "Add a new row to a table. Auto-generates id and created_at if defined in schema.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "data": {
                        "type": "object",
                        "description": "Field values for the new row.",
                    },
                },
                "required": ["table", "data"],
            },
        },
        handler=_add,
    )

    ctx.register_tool(
        name="loom_update",
        toolset="loom",
        emoji="✏️",
        description="Update fields of an existing row by id.",
        schema={
            "name": "loom_update",
            "description": "Update an existing row in a table by id. Only provided fields are changed.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "id": {"type": "string", "description": "Row id to update"},
                    "data": {
                        "type": "object",
                        "description": "Fields to update.",
                    },
                },
                "required": ["table", "id", "data"],
            },
        },
        handler=_update,
    )

    ctx.register_tool(
        name="loom_delete",
        toolset="loom",
        emoji="🗑️",
        description="Delete a row from a table by id.",
        schema={
            "name": "loom_delete",
            "description": "Delete a row from a table by id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                    "id": {"type": "string"},
                },
                "required": ["table", "id"],
            },
        },
        handler=_delete,
    )

    ctx.register_tool(
        name="loom_commit",
        toolset="loom",
        emoji="💾",
        description="Commit all pending data changes to git.",
        schema={
            "name": "loom_commit",
            "description": "Commit all pending data changes. Call this when the user asks to save or commit, or before syncing.",
            "parameters": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "Optional commit message describing the changes.",
                    },
                },
                "required": [],
            },
        },
        handler=_commit,
    )

    ctx.register_tool(
        name="loom_sync",
        toolset="loom",
        emoji="🔄",
        description="Sync with remote (pull + merge + push). Returns conflicts if any.",
        schema={
            "name": "loom_sync",
            "description": "Pull remote changes, auto-merge, and push. If conflicts exist returns them for resolution.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        handler=_sync,
    )

    ctx.register_tool(
        name="loom_conflicts",
        toolset="loom",
        emoji="⚠️",
        description="List current merge conflicts as structured JSON.",
        schema={
            "name": "loom_conflicts",
            "description": "List current unresolved merge conflicts. Each conflict has id, table, row_id, field, base, mine, theirs.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        handler=_conflicts,
    )

    ctx.register_tool(
        name="loom_resolve",
        toolset="loom",
        emoji="✅",
        description="Resolve a merge conflict by choosing the winning value.",
        schema={
            "name": "loom_resolve",
            "description": "Resolve a merge conflict. Use loom_conflicts to get conflict ids first.",
            "parameters": {
                "type": "object",
                "properties": {
                    "conflict_id": {
                        "type": "string",
                        "description": "Conflict id from loom_conflicts output",
                    },
                    "value": {
                        "type": "string",
                        "description": "The resolved value to use",
                    },
                },
                "required": ["conflict_id", "value"],
            },
        },
        handler=_resolve,
    )

    ctx.register_tool(
        name="loom_stats",
        toolset="loom",
        emoji="📊",
        description="Aggregate statistics on a table (count, sum, avg, min, max) with optional grouping.",
        schema={
            "name": "loom_stats",
            "description": "Compute aggregate statistics on a table. Supports count, sum, avg, min, max with optional group_by.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name"},
                    "group_by": {
                        "description": "Field(s) to group by. String or array of strings.",
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ],
                    },
                    "agg": {
                        "type": "object",
                        "description": "Aggregation map: {column: function}. Functions: count, sum, avg, min, max. Example: {\"value\": \"sum\", \"id\": \"count\"}",
                    },
                },
                "required": ["table", "agg"],
            },
        },
        handler=_stats,
    )

    ctx.register_tool(
        name="loom_discover",
        toolset="loom",
        emoji="🔭",
        description="Fetch the Agent Card of a remote loom A2A agent to understand what data it exposes.",
        schema={
            "name": "loom_discover",
            "description": "Fetch Agent Card from a remote loom A2A agent. Call this first to understand what a remote agent can provide before calling loom_call.",
            "parameters": {
                "type": "object",
                "properties": {
                    "agent_url": {
                        "type": "string",
                        "description": "Base URL of the remote loom agent, e.g. http://192.168.1.10:8100",
                    },
                },
                "required": ["agent_url"],
            },
        },
        handler=_discover,
    )

    ctx.register_tool(
        name="loom_call",
        toolset="loom",
        emoji="📡",
        description="Query a remote loom A2A agent in natural language.",
        schema={
            "name": "loom_call",
            "description": (
                "Send a natural language query to a remote loom A2A agent and get data back. "
                "Use loom_discover first to understand what the agent can provide."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent_url": {
                        "type": "string",
                        "description": "Base URL of the remote loom agent",
                    },
                    "query": {
                        "type": "string",
                        "description": "Natural language query, e.g. 'list all active contacts'",
                    },
                },
                "required": ["agent_url", "query"],
            },
        },
        handler=_call,
    )

