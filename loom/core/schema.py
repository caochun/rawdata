"""Schema loading and validation."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from loom.core.catalog import Catalog


class SchemaError(Exception):
    pass


class Schema:
    def __init__(self, data: dict):
        self._data = data

    @classmethod
    def load(cls, repo_root: Path) -> "Schema":
        path = repo_root / "schema.yaml"
        if not path.exists():
            raise SchemaError(f"schema.yaml not found in {repo_root}")
        with open(path) as f:
            return cls(yaml.safe_load(f))

    def tables(self) -> list[str]:
        return list(self._data.get("tables", {}).keys())

    def columns(self, table: str) -> dict[str, dict]:
        return self._data["tables"][table]["columns"]

    def merge_strategy(self, table: str, field: str) -> str:
        strategies = (
            self._data["tables"][table].get("merge_strategy") or {}
        )
        return strategies.get(field, "last_write_wins")

    def validate_row(self, table: str, row: dict[str, Any]) -> list[str]:
        """Return list of validation errors, empty means OK."""
        errors = []
        cols = self.columns(table)

        for col_name, col_def in cols.items():
            value = row.get(col_name)

            if col_def.get("required") and not col_def.get("auto"):
                if value is None or value == "":
                    errors.append(f"'{col_name}' is required")
                    continue

            if value is None or value == "":
                continue

            col_type = col_def.get("type", "string")

            if col_type == "enum":
                allowed = col_def.get("values", [])
                if value not in allowed:
                    errors.append(
                        f"'{col_name}' must be one of {allowed}, got '{value}'"
                    )

            if col_type == "string" and "pattern" in col_def:
                if not re.fullmatch(col_def["pattern"], str(value)):
                    errors.append(
                        f"'{col_name}' does not match pattern {col_def['pattern']}"
                    )

        return errors


def validate_foreign_keys(
    repo_root: Path,
    catalog: "Catalog",
    tables: list[str] | None = None,
) -> list[dict]:
    from loom.core.store import read_table

    errors = []
    rels = catalog.relationships()
    table_cache: dict[str, set[str]] = {}

    def _get_ids(table: str, col: str) -> set[str]:
        cache_key = f"{table}.{col}"
        if cache_key not in table_cache:
            rows = read_table(repo_root, table)
            table_cache[cache_key] = {r.get(col, "") for r in rows if r.get(col)}
        return table_cache[cache_key]

    for rel in rels:
        from_table = rel["from"]
        if tables and from_table not in tables:
            continue
        from_col = rel["fromCol"]
        to_table = rel["to"]
        to_col = rel["toCol"]

        valid_ids = _get_ids(to_table, to_col)
        from_rows = read_table(repo_root, from_table)

        for row in from_rows:
            ref_val = row.get(from_col, "")
            if not ref_val:
                continue
            if ref_val not in valid_ids:
                errors.append({
                    "table": from_table,
                    "column": from_col,
                    "row_id": row.get("id", ""),
                    "invalid_ref": ref_val,
                    "ref_table": to_table,
                })

    return errors
