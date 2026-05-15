"""Catalog loader — reads catalog.yaml relationship graph."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class CatalogError(Exception):
    pass


class Catalog:
    def __init__(self, data: dict):
        self._data = data

    @classmethod
    def load(cls, repo_root: Path) -> "Catalog":
        path = repo_root / "catalog.yaml"
        if not path.exists():
            raise CatalogError(f"catalog.yaml not found in {repo_root}")
        with open(path) as f:
            return cls(yaml.safe_load(f) or {})

    def relationships(self) -> list[dict]:
        return self._data.get("relationships", [])

    def find_rel(self, from_table: str, to_table: str) -> dict | None:
        for rel in self.relationships():
            if rel["from"] == from_table and rel["to"] == to_table:
                return rel
        return None

    def find_rel_any_direction(self, table_a: str, table_b: str) -> dict | None:
        rel = self.find_rel(table_a, table_b)
        if rel:
            return rel
        rel = self.find_rel(table_b, table_a)
        if rel:
            return {
                "from": table_a,
                "fromCol": rel["toCol"],
                "to": table_b,
                "toCol": rel["fromCol"],
                "description": rel.get("description", ""),
            }
        return None

    def find_rel_from(self, from_table: str, from_col: str) -> dict | None:
        """Find FK relationship originating from from_table.from_col."""
        for rel in self.relationships():
            if rel["from"] == from_table and rel["fromCol"] == from_col:
                return rel
        return None

    def related_tables(self, table: str) -> list[str]:
        tables = set()
        for rel in self.relationships():
            if rel["from"] == table:
                tables.add(rel["to"])
            elif rel["to"] == table:
                tables.add(rel["from"])
        return sorted(tables)

    def table_descriptions(self) -> dict[str, str]:
        tables = self._data.get("tables", {})
        return {k: v.get("description", "") for k, v in tables.items()}

    def notes(self) -> list[str]:
        return self._data.get("notes", [])
