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
    from loom.core.git_ops import GitError, commit_changes, diff_summary, push, sync as git_sync
    root = _root()
    try:
        message = (args.get("message") or "").strip()
        if not message:
            message = diff_summary(root) or "data: sync"
        sha = commit_changes(root, message)
        result = git_sync(root)
        pushed = False
        push_error = None
        if result["status"] == "ok":
            try:
                push(root)
                pushed = True
            except GitError as e:
                push_error = str(e)
        if sha:
            result["committed"] = sha
            result["commit_message"] = message
        result["pushed"] = pushed
        if push_error:
            result["push_error"] = push_error
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


def _join(args: dict, **_) -> str:
    from loom.core.catalog import Catalog
    from loom.core.store import join_tables
    root = _root()
    catalog = Catalog.load(root)
    try:
        result = join_tables(
            root, catalog,
            left_table=args["left_table"],
            right_table=args["right_table"],
            join_type=args.get("join_type", "inner"),
            left_col=args.get("left_col"),
            right_col=args.get("right_col"),
            filters=args.get("filters"),
            fields=args.get("fields"),
            limit=args.get("limit"),
        )
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _tree(args: dict, **_) -> str:
    from loom.core.store import read_table, tree_descendants, tree_ancestors, tree_path
    root = _root()
    table = args["table"]
    node_id = args["node_id"]
    direction = args.get("direction", "down")
    parent_col = args.get("parent_col", "parent_id")
    rows = read_table(root, table)
    try:
        if direction == "path":
            return json.dumps({"path": tree_path(rows, node_id, parent_col)})
        elif direction == "up":
            result = tree_ancestors(rows, node_id, parent_col)
        else:
            result = tree_descendants(rows, node_id, parent_col)
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _validate(args: dict, **_) -> str:
    from loom.core.catalog import Catalog
    from loom.core.schema import validate_foreign_keys
    root = _root()
    catalog = Catalog.load(root)
    tables = args.get("tables")
    try:
        errors = validate_foreign_keys(root, catalog, tables)
        return json.dumps({"ok": len(errors) == 0, "errors": errors}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})


def _compute_list(args: dict, **_) -> str:
    from loom.core.compute import load_pipelines
    root = _root()
    pipelines = load_pipelines(root)
    result = [
        {"name": name, "description": p.get("description", ""), "steps": len(p.get("steps", []))}
        for name, p in pipelines.items()
    ]
    return json.dumps(result, ensure_ascii=False)


def _compute_run(args: dict, **_) -> str:
    from loom.core.compute import ComputeError, run_pipeline
    root = _root()
    try:
        result = run_pipeline(
            root,
            args["pipeline"],
            args["period"],
            dry_run=args.get("dry_run", False),
        )
        return json.dumps(result, ensure_ascii=False)
    except ComputeError as e:
        return json.dumps({"error": str(e)})


def _view_list(args: dict, **_) -> str:
    from loom.core.views import load_views
    root = _root()
    views = load_views(root)
    result = [
        {"name": name, "description": v.get("description", ""), "steps": len(v.get("steps", []))}
        for name, v in views.items()
    ]
    return json.dumps(result, ensure_ascii=False)


def _view_query(args: dict, **_) -> str:
    from loom.core.views import ViewError, run_view
    root = _root()
    try:
        result = run_view(root, args["name"], args.get("params"))
        limit = args.get("limit")
        if limit:
            result = result[:limit]
        return json.dumps(result, ensure_ascii=False)
    except ViewError as e:
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


def _form(args: dict, **_) -> str:
    """Generate a form spec JSON for creating a new record in a table."""
    from loom.core.schema import Schema
    from loom.core.catalog import Catalog
    from loom.core.store import read_table
    root = _root()
    table = args["table"]
    schema = Schema.load(root)
    col_defs = schema.columns(table)

    try:
        catalog = Catalog.load(root)
    except Exception:
        catalog = None

    table_info = schema._data.get("tables", {}).get(table, {})
    table_label = table

    fields = []
    for col_name, col_def in col_defs.items():
        if col_def.get("auto"):
            continue

        field = {"name": col_name, "label": col_def.get("description", col_name)}
        col_type = col_def.get("type", "string")

        if col_type == "enum":
            field["type"] = "select"
            field["options"] = [{"value": "", "label": "-- 请选择 --"}]
            for v in col_def.get("values", []):
                field["options"].append({"value": v, "label": v})
        elif col_type == "number":
            field["type"] = "number"
        elif col_type == "datetime":
            field["type"] = "date"
        elif col_type == "uuid" and not col_def.get("primary"):
            rel = catalog.find_rel_from(table, col_name) if catalog else None
            if rel:
                ref_table = rel["to"]
                ref_col = rel["toCol"]
                try:
                    ref_rows = read_table(root, ref_table)
                except Exception:
                    ref_rows = []
                field["type"] = "select"
                field["options"] = [{"value": "", "label": "-- 请选择 --"}]
                for row in ref_rows:
                    label = row.get("name") or row.get("short_name") or row.get("code") or row.get(ref_col, "")
                    field["options"].append({"value": row[ref_col], "label": label})
            else:
                field["type"] = "text"
        else:
            field["type"] = "text"

        if col_def.get("required"):
            field["required"] = True

        fields.append(field)

    spec = {"table": table, "title": f"新建记录 — {table}", "fields": fields}
    return json.dumps(spec, ensure_ascii=False)


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
        description="Commit pending changes, pull remote, merge, and push. Returns conflicts if any.",
        schema={
            "name": "loom_sync",
            "description": (
                "Commit any pending local changes, pull remote updates, auto-merge, and push. "
                "Provide a meaningful `message` describing what changed (e.g. '新增南理工商机，更新跟进状态'). "
                "If omitted, a message is auto-generated from the diff. "
                "Returns {status, pushed, committed, commit_message} or conflicts for resolution."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "Commit message describing the changes made in this session. Write in the same language as the data.",
                    },
                },
                "required": [],
            },
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
        name="loom_join",
        toolset="loom",
        emoji="🔗",
        description="Join two tables using catalog relationships.",
        schema={
            "name": "loom_join",
            "description": "Join two tables. Automatically infers join columns from catalog.yaml relationships. Returns prefixed columns (table.col).",
            "parameters": {
                "type": "object",
                "properties": {
                    "left_table": {"type": "string", "description": "Left table name"},
                    "right_table": {"type": "string", "description": "Right table name"},
                    "join_type": {
                        "type": "string", "enum": ["inner", "left"],
                        "description": "Join type (default: inner)",
                    },
                    "left_col": {"type": "string", "description": "Override left join column"},
                    "right_col": {"type": "string", "description": "Override right join column"},
                    "filters": {
                        "type": "object",
                        "description": "Filters on joined result: {\"table.col\": \"value\"}",
                    },
                    "fields": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Columns to return (table.col format)",
                    },
                    "limit": {"type": "integer", "description": "Max rows"},
                },
                "required": ["left_table", "right_table"],
            },
        },
        handler=_join,
    )

    ctx.register_tool(
        name="loom_tree",
        toolset="loom",
        emoji="🌳",
        description="Traverse tree/hierarchy structures (departments, cost categories, customers).",
        schema={
            "name": "loom_tree",
            "description": "Query tree structures. Get descendants (down), ancestors (up), or full path of a node.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table with tree structure"},
                    "node_id": {"type": "string", "description": "Node ID to start from"},
                    "direction": {
                        "type": "string", "enum": ["down", "up", "path"],
                        "description": "down=descendants, up=ancestors, path=full path string",
                    },
                    "parent_col": {
                        "type": "string",
                        "description": "Parent column name (default: parent_id)",
                    },
                },
                "required": ["table", "node_id"],
            },
        },
        handler=_tree,
    )

    ctx.register_tool(
        name="loom_validate",
        toolset="loom",
        emoji="✔️",
        description="Validate foreign key references across tables.",
        schema={
            "name": "loom_validate",
            "description": "Check foreign key integrity across all tables using catalog relationships. Returns list of violations.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Specific tables to validate (default: all)",
                    },
                },
                "required": [],
            },
        },
        handler=_validate,
    )

    ctx.register_tool(
        name="loom_compute_list",
        toolset="loom",
        emoji="📋",
        description="List available compute/allocation pipelines defined in compute.yaml.",
        schema={
            "name": "loom_compute_list",
            "description": "List all compute pipelines. Each pipeline defines a multi-step cost allocation or calculation process.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        handler=_compute_list,
    )

    ctx.register_tool(
        name="loom_compute_run",
        toolset="loom",
        emoji="⚙️",
        description="Run a compute pipeline (e.g. cost allocation) for a specific period.",
        schema={
            "name": "loom_compute_run",
            "description": "Execute a compute pipeline for a given year-month period. Use dry_run=true to preview without writing.",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline": {"type": "string", "description": "Pipeline name from compute.yaml"},
                    "period": {"type": "string", "description": "Year-month, e.g. 2026-05"},
                    "dry_run": {"type": "boolean", "description": "Preview without writing (default: false)"},
                },
                "required": ["pipeline", "period"],
            },
        },
        handler=_compute_run,
    )

    ctx.register_tool(
        name="loom_view_list",
        toolset="loom",
        emoji="👁️",
        description="List predefined analysis views from views.yaml.",
        schema={
            "name": "loom_view_list",
            "description": "List all predefined analysis views. Views are saved multi-step queries for common analyses.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        handler=_view_list,
    )

    ctx.register_tool(
        name="loom_view_query",
        toolset="loom",
        emoji="📈",
        description="Execute a predefined analysis view.",
        schema={
            "name": "loom_view_query",
            "description": "Run a predefined view by name. Views combine queries, joins, and computations into reusable analyses.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "View name from views.yaml"},
                    "params": {
                        "type": "object",
                        "description": "Optional parameters to pass to the view",
                    },
                    "limit": {"type": "integer", "description": "Max rows to return"},
                },
                "required": ["name"],
            },
        },
        handler=_view_query,
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

    ctx.register_tool(
        name="loom_form",
        toolset="loom",
        emoji="📝",
        description=(
            "Generate a form spec for creating a new record in a table. "
            "Returns a JSON object describing the form fields, types, and FK dropdown options. "
            "You MUST output the returned JSON verbatim inside a ```loom-form fenced code block — "
            "the frontend will render it as an interactive form for the user to fill in."
        ),
        schema={
            "name": "loom_form",
            "description": (
                "Generate a form spec for creating a new record. Output the result "
                "inside a ```loom-form code block so the frontend renders it as a form."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name to create form for"},
                },
                "required": ["table"],
            },
        },
        handler=_form,
    )

