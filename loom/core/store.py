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
) -> list[dict]:
    result = rows
    if filters:
        for key, value in filters.items():
            result = [r for r in result if r.get(key) == value]
    if fields:
        result = [{f: r.get(f) for f in fields} for r in result]
    return result
