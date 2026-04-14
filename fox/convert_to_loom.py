"""Convert fox twin-schema files to loom schema.yaml + catalog.yaml format.

Usage:
    python3 convert_to_loom.py

Output: ../fox-crm/{domain}/schema.yaml + catalog.yaml
"""
from __future__ import annotations
import os
import yaml
from pathlib import Path

FOX_DIR   = Path(__file__).parent
OUT_ROOT  = FOX_DIR.parent / "fox-crm"

# Domain name mapping (file stem → output dir name)
DOMAIN_MAP = {
    "crm_schema":           "crm",
    "customer_mgmt_schema": "customer_mgmt",
    "bidding_schema":       "bidding",
    "contract_schema":      "contract",
    "hr_schema":            "hr",
    "ipasset_schema":       "ipasset",
    "project_schema":       "project",
    "purchase_schema":      "purchase",
}

# Fox type → Loom type
TYPE_MAP = {
    "string":   "string",
    "text":     "string",
    "enum":     "enum",
    "date":     "datetime",
    "datetime": "datetime",
    "decimal":  "string",
    "number":   "string",
    "boolean":  "string",
    "file":     "string",
    "image":    "string",
    "url":      "string",
    "email":    "string",
    "phone":    "string",
    "uuid":     "uuid",
}


def convert_file(src: Path, out_dir: Path):
    data = yaml.safe_load(src.read_text(encoding="utf-8"))
    twins: dict = data.get("twins", {})

    schema_tables: dict = {}
    catalog_tables: dict = {}
    relationships: list = []

    for tname, tv in twins.items():
        if not isinstance(tv, dict):
            continue

        label: str = tv.get("label", tname)
        fields: dict = tv.get("fields", {}) or {}
        related: list = tv.get("related_entities", []) or []

        cols: dict = {}

        # ── 1. id column ──────────────────────────────────────
        cols["id"] = {
            "type": "uuid",
            "primary": True,
            "auto": True,
            "description": "唯一标识",
        }

        # ── 2. FK columns from related_entities (for activity twins) ──
        for rel in related:
            if not isinstance(rel, dict):
                continue
            fk_col  = rel.get("key", "")
            fk_desc = rel.get("label", "")
            if fk_col:
                cols[fk_col] = {
                    "type": "uuid",
                    "description": fk_desc,
                }
                # register relationship
                target_entity = rel.get("entity", "")
                if target_entity:
                    relationships.append({
                        "from":        tname,
                        "fromCol":     fk_col,
                        "to":          target_entity,
                        "toCol":       "id",
                        "description": fk_desc,
                    })

        # ── 3. regular fields ─────────────────────────────────
        for fname, fv in fields.items():
            if not isinstance(fv, dict):
                fv = {"type": str(fv)}

            fox_type  = fv.get("type", "string")
            loom_type = TYPE_MAP.get(fox_type, "string")

            col_def: dict = {
                "type":        loom_type,
                "description": fv.get("label", fname),
            }
            if fv.get("required"):
                col_def["required"] = True
            if loom_type == "enum":
                opts = fv.get("options") or []
                col_def["values"] = [
                    o["value"] if isinstance(o, dict) else str(o)
                    for o in opts
                ]
            cols[fname] = col_def

        # ── 4. created_at ─────────────────────────────────────
        cols["created_at"] = {"type": "datetime", "auto": True, "description": "创建时间"}

        schema_tables[tname] = {"columns": cols}
        catalog_tables[tname] = label

    # ── build schema / catalog dicts ─────────────────────────
    schema  = {"tables": schema_tables}
    catalog = {
        "description": "",
        "tables":       catalog_tables,
        "relationships": relationships,
        "notes":        [],
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "schema.yaml").write_text(
        yaml.dump(schema,  allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    (out_dir / "catalog.yaml").write_text(
        yaml.dump(catalog, allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    print(f"  {out_dir.name}: {len(schema_tables)} tables, {len(relationships)} relationships")


def main():
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    for stem, domain in DOMAIN_MAP.items():
        src = FOX_DIR / f"{stem}.yaml"
        if not src.exists():
            print(f"  SKIP {src.name} (not found)")
            continue
        out_dir = OUT_ROOT / domain
        convert_file(src, out_dir)
    print(f"\nDone → {OUT_ROOT}")


if __name__ == "__main__":
    main()
