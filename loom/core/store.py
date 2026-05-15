"""CSV read/write operations."""
from __future__ import annotations

import csv
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from loom.core.catalog import Catalog


class StoreError(Exception):
    pass


def _table_path(repo_root: Path, table: str) -> Path:
    return repo_root / f"{table}.csv"


def read_table(repo_root: Path, table: str) -> list[dict]:
    path = _table_path(repo_root, table)
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_table(repo_root: Path, table: str, rows: list[dict]) -> None:
    path = _table_path(repo_root, table)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def apply_auto_fields(col_defs: dict, row: dict, is_new: bool) -> dict:
    """Fill in auto-generated fields (id, timestamps)."""
    result = dict(row)
    for col_name, col_def in col_defs.items():
        if not col_def.get("auto"):
            continue
        col_type = col_def.get("type", "string")
        if col_type == "uuid" and is_new:
            result.setdefault(col_name, str(uuid.uuid4()))
        elif col_type == "datetime":
            if is_new and col_name not in result:
                result[col_name] = datetime.now(timezone.utc).isoformat()
            elif not is_new and col_def.get("on_update"):
                result[col_name] = datetime.now(timezone.utc).isoformat()
    return result


def find_row(rows: list[dict], row_id: str) -> tuple[int, dict | None]:
    """Return (index, row) or (−1, None)."""
    for i, row in enumerate(rows):
        if row.get("id") == row_id:
            return i, row
    return -1, None


def query_rows(
    rows: list[dict],
    filters: dict[str, str] | None = None,
    fields: list[str] | None = None,
    search: str | None = None,
    sort_by: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict]:
    result = rows

    # Exact-match filters
    if filters:
        for key, value in filters.items():
            result = [r for r in result if r.get(key) == value]

    # Fuzzy search across all string fields
    if search:
        term = search.lower()
        result = [
            r for r in result
            if any(term in str(v).lower() for v in r.values() if v)
        ]

    # Sort
    if sort_by:
        desc = sort_by.startswith("-")
        field = sort_by.lstrip("-")
        result = sorted(
            result,
            key=lambda r: r.get(field) or "",
            reverse=desc,
        )

    # Pagination
    if offset:
        result = result[offset:]
    if limit:
        result = result[:limit]

    # Field selection (applied last)
    if fields:
        result = [{f: r.get(f) for f in fields} for r in result]

    return result


def aggregate_rows(
    rows: list[dict],
    group_by: str | list[str] | None = None,
    agg: dict[str, str] | None = None,
) -> list[dict]:
    """Aggregate rows using pandas. Returns list of dicts.

    agg maps column names to functions: count, sum, avg/mean, min, max.
    """
    import pandas as pd

    if not rows:
        return []
    if not agg:
        return [{"count": len(rows)}]

    df = pd.DataFrame(rows)

    # Coerce numeric columns referenced in agg
    for col, func in agg.items():
        if func in ("sum", "avg", "mean", "min", "max") and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Map user-facing function names to pandas names
    agg_map = {}
    for col, func in agg.items():
        if col not in df.columns:
            continue
        if func in ("avg", "mean"):
            agg_map[col] = "mean"
        elif func == "count":
            agg_map[col] = "count"
        else:
            agg_map[col] = func

    if not agg_map:
        return [{"count": len(rows)}]

    if group_by:
        if isinstance(group_by, str):
            group_by = [group_by]
        grouped = df.groupby(group_by, dropna=False).agg(agg_map).reset_index()
    else:
        grouped = df.agg(agg_map).to_frame().T

    # Clean up column names and NaN
    grouped = grouped.fillna("")
    return grouped.to_dict("records")


def join_tables(
    repo_root: Path,
    catalog: "Catalog",
    left_table: str,
    right_table: str,
    join_type: str = "inner",
    left_col: str | None = None,
    right_col: str | None = None,
    filters: dict[str, str] | None = None,
    fields: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    if left_col and right_col:
        lc, rc = left_col, right_col
    else:
        rel = catalog.find_rel_any_direction(left_table, right_table)
        if not rel:
            raise StoreError(
                f"No relationship between {left_table} and {right_table} in catalog"
            )
        lc, rc = rel["fromCol"], rel["toCol"]

    left_rows = read_table(repo_root, left_table)
    right_rows = read_table(repo_root, right_table)

    right_index: dict[str, list[dict]] = {}
    for r in right_rows:
        key = r.get(rc, "")
        right_index.setdefault(key, []).append(r)

    result = []
    for lr in left_rows:
        key = lr.get(lc, "")
        matches = right_index.get(key, [])
        if matches:
            for rr in matches:
                merged = {f"{left_table}.{k}": v for k, v in lr.items()}
                merged.update({f"{right_table}.{k}": v for k, v in rr.items()})
                result.append(merged)
        elif join_type == "left":
            merged = {f"{left_table}.{k}": v for k, v in lr.items()}
            if right_rows:
                for k in right_rows[0].keys():
                    merged[f"{right_table}.{k}"] = ""
            result.append(merged)

    if filters:
        for key, value in filters.items():
            result = [r for r in result if r.get(key) == value]

    if fields:
        result = [{f: r.get(f) for f in fields} for r in result]

    if limit:
        result = result[:limit]

    return result


def tree_descendants(
    rows: list[dict],
    node_id: str,
    parent_col: str = "parent_id",
    include_self: bool = True,
) -> list[dict]:
    by_id = {r["id"]: r for r in rows}
    children_map: dict[str, list[str]] = {}
    for r in rows:
        pid = r.get(parent_col, "")
        if pid:
            children_map.setdefault(pid, []).append(r["id"])

    collected = []
    if include_self and node_id in by_id:
        collected.append(by_id[node_id])

    stack = list(children_map.get(node_id, []))
    while stack:
        cid = stack.pop()
        if cid in by_id:
            collected.append(by_id[cid])
            stack.extend(children_map.get(cid, []))

    return collected


def tree_ancestors(
    rows: list[dict],
    node_id: str,
    parent_col: str = "parent_id",
    include_self: bool = True,
) -> list[dict]:
    by_id = {r["id"]: r for r in rows}
    collected = []
    current = node_id
    seen = set()
    while current and current in by_id and current not in seen:
        seen.add(current)
        if current == node_id and not include_self:
            current = by_id[current].get(parent_col, "")
            continue
        collected.append(by_id[current])
        current = by_id[current].get(parent_col, "")
    return collected


def tree_path(
    rows: list[dict],
    node_id: str,
    parent_col: str = "parent_id",
    name_col: str = "name",
) -> str:
    ancestors = tree_ancestors(rows, node_id, parent_col, include_self=True)
    ancestors.reverse()
    return " > ".join(a.get(name_col, a["id"]) for a in ancestors)


def time_aggregate_rows(
    rows: list[dict],
    date_col: str,
    period: str,
    agg: dict[str, str],
    group_by: list[str] | None = None,
) -> list[dict]:
    import pandas as pd

    if not rows:
        return []

    df = pd.DataFrame(rows)
    if date_col not in df.columns:
        raise StoreError(f"Column '{date_col}' not found")

    if period == "month":
        df["_period"] = df[date_col].str[:7]
    elif period == "quarter":
        def _to_quarter(v: str) -> str:
            parts = str(v).split("-")
            if len(parts) >= 2:
                y, m = parts[0], int(parts[1])
                return f"{y}-Q{(m - 1) // 3 + 1}"
            return str(v)
        df["_period"] = df[date_col].apply(_to_quarter)
    elif period == "year":
        df["_period"] = df[date_col].str[:4]
    else:
        raise StoreError(f"Unknown period: {period}, expected month/quarter/year")

    all_groups = ["_period"] + (group_by or [])

    for col, func in agg.items():
        if func in ("sum", "avg", "mean", "min", "max") and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    agg_map = {}
    for col, func in agg.items():
        if col not in df.columns:
            continue
        agg_map[col] = "mean" if func in ("avg", "mean") else func

    if not agg_map:
        return [{"count": len(rows)}]

    grouped = df.groupby(all_groups, dropna=False).agg(agg_map).reset_index()
    grouped = grouped.rename(columns={"_period": "period"})
    grouped = grouped.fillna("")
    return grouped.to_dict("records")
