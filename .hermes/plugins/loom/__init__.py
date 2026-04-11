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
    try:
        rows = read_table(root, table)
        result = query_rows(rows, filters or None, fields or None)
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
        description="Query rows from a table with optional filters and field selection.",
        schema={
            "name": "loom_query",
            "description": "Query rows from a table. Returns a JSON array of matching rows.",
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
