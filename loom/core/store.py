"""CSV read/write operations."""
from __future__ import annotations

import csv
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
