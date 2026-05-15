"""Predefined view engine — reads views.yaml and executes multi-step queries."""
from __future__ import annotations

from pathlib import Path

import yaml

from loom.core.catalog import Catalog
from loom.core.store import (
    StoreError,
    aggregate_rows,
    join_tables,
    query_rows,
    read_table,
)


class ViewError(Exception):
    pass


def load_views(repo_root: Path) -> dict:
    path = repo_root / "views.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data.get("views", {})


def run_view(repo_root: Path, view_name: str, params: dict | None = None) -> list[dict]:
    views = load_views(repo_root)
    if view_name not in views:
        raise ViewError(f"View '{view_name}' not found")

    view = views[view_name]
    steps = view.get("steps", [])
    context: dict[str, list[dict]] = {}
    result: list[dict] = []

    catalog = None

    for step in steps:
        if "query" in step:
            q = step["query"]
            alias = step.get("as", "_last")
            table = q["table"]
            rows = read_table(repo_root, table)

            filters = q.get("filters")
            fields = q.get("fields")
            search = q.get("search")
            sort_by = q.get("sort_by")
            limit = q.get("limit")

            if params:
                if filters:
                    filters = {k: params.get(v, v) if v.startswith("$") else v
                               for k, v in filters.items()}

            group_by = q.get("group_by")
            agg = q.get("agg")

            if group_by and agg:
                queried = aggregate_rows(rows, group_by=group_by, agg=agg)
            else:
                queried = query_rows(
                    rows, filters=filters, fields=fields,
                    search=search, sort_by=sort_by, limit=limit,
                )

            context[alias] = queried
            result = queried

        elif "join" in step:
            j = step["join"]
            left_key = j["left"]
            right_key = j["right"]
            join_on = j.get("join_on", [])
            join_type = j.get("type", "inner")

            left_rows = context.get(left_key, [])
            right_rows = context.get(right_key, [])

            if not join_on or not left_rows or not right_rows:
                result = []
            else:
                if isinstance(join_on, dict):
                    left_cols = list(join_on.keys())
                    right_cols = list(join_on.values())
                else:
                    left_cols = join_on
                    right_cols = join_on
                joined = _in_memory_join(left_rows, right_rows, left_cols, right_cols, join_type)
                alias = step.get("as", "_last")
                context[alias] = joined
                result = joined

        elif "compute" in step:
            expressions = step["compute"]
            source_key = step.get("source", "_last")
            rows = context.get(source_key, result)

            for row in rows:
                for field, expr in expressions.items():
                    try:
                        local_vars = {}
                        for k, v in row.items():
                            try:
                                local_vars[k] = float(v) if v else 0
                            except (ValueError, TypeError):
                                local_vars[k] = 0
                        row[field] = eval(expr, {"__builtins__": {}}, local_vars)
                    except Exception:
                        row[field] = 0

            alias = step.get("as", "_last")
            context[alias] = rows
            result = rows

    return result


def _in_memory_join(
    left: list[dict],
    right: list[dict],
    left_cols: list[str],
    right_cols: list[str],
    join_type: str = "inner",
) -> list[dict]:
    right_index: dict[tuple, list[dict]] = {}
    for r in right:
        key = tuple(str(r.get(c, "")) for c in right_cols)
        right_index.setdefault(key, []).append(r)

    result = []
    for lr in left:
        key = tuple(str(lr.get(c, "")) for c in left_cols)
        matches = right_index.get(key, [])
        if matches:
            for rr in matches:
                merged = dict(lr)
                for k, v in rr.items():
                    if k not in merged:
                        merged[k] = v
                result.append(merged)
        elif join_type == "left":
            result.append(dict(lr))

    return result
