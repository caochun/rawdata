"""Schema loading and validation."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


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
