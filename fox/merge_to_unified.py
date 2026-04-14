"""Merge all fox-crm domain schemas into a single unified loom model.

Reads fox-crm/{domain}/schema.yaml + catalog.yaml (8 domains, 58 tables),
adds cross-domain FK columns and relationships, writes to fox-crm-all/.

Usage:
    python3 merge_to_unified.py
"""
from __future__ import annotations
import yaml
from pathlib import Path

FOX_CRM = Path(__file__).parent.parent / "fox-crm"
OUT_DIR  = Path(__file__).parent.parent / "fox-crm-all"

DOMAINS = [
    "crm", "customer_mgmt", "bidding", "contract",
    "hr", "ipasset", "project", "purchase",
]

# ── Cross-domain FK columns to inject ────────────────────────────────────────
# (table, col_name, type, target_table, description)
# Only added when the column doesn't already exist
CROSS_DOMAIN_FKS: list[tuple[str, str, str, str, str]] = [
    # bidding ──► crm
    ("bid_pre_reg",      "opportunity_id",   "uuid", "opportunity",   "关联商机"),
    ("bid_tender",       "opportunity_id",   "uuid", "opportunity",   "关联商机"),
    # contract ──► crm + bidding
    ("contract",         "opportunity_id",   "uuid", "opportunity",   "来源商机"),
    ("contract",         "bid_tender_id",    "uuid", "bid_tender",    "来源投标"),
    # project ──► crm + contract
    ("project",          "opportunity_id",   "uuid", "opportunity",   "来源商机"),
    ("project",          "customer_id",      "uuid", "customer",      "交付客户"),
    ("project",          "contract_id",      "uuid", "contract",      "关联合同"),
    # hr ──► project
    ("emp_project",      "project_id",       "uuid", "project",       "参与项目"),
    # purchase ──► project + contract
    ("purchase_request", "project_id",       "uuid", "project",       "所属项目"),
    ("purchase_order",   "project_id",       "uuid", "project",       "所属项目"),
    ("purchase_order",   "contract_id",      "uuid", "contract",      "关联合同"),
    # ipasset ──► project + purchase
    ("ip_application",   "project_id",       "uuid", "project",       "来源项目"),
    ("ip_application",   "purchase_order_id","uuid", "purchase_order","关联采购单"),
    # project_cost ──► purchase
    ("project_cost",     "purchase_order_id","uuid", "purchase_order","关联采购单"),
]

# ── Cross-domain relationships for catalog ───────────────────────────────────
CROSS_DOMAIN_RELS: list[dict] = [
    {"from": "bid_pre_reg",      "fromCol": "opportunity_id",    "to": "opportunity",    "toCol": "id", "description": "预投标登记关联商机"},
    {"from": "bid_tender",       "fromCol": "opportunity_id",    "to": "opportunity",    "toCol": "id", "description": "正式投标关联商机"},
    {"from": "contract",         "fromCol": "opportunity_id",    "to": "opportunity",    "toCol": "id", "description": "合同来源商机"},
    {"from": "contract",         "fromCol": "bid_tender_id",     "to": "bid_tender",     "toCol": "id", "description": "合同来源投标"},
    {"from": "project",          "fromCol": "opportunity_id",    "to": "opportunity",    "toCol": "id", "description": "项目来源商机"},
    {"from": "project",          "fromCol": "customer_id",       "to": "customer",       "toCol": "id", "description": "项目交付客户"},
    {"from": "project",          "fromCol": "contract_id",       "to": "contract",       "toCol": "id", "description": "项目关联合同"},
    {"from": "emp_project",      "fromCol": "project_id",        "to": "project",        "toCol": "id", "description": "员工参与的项目"},
    {"from": "purchase_request", "fromCol": "project_id",        "to": "project",        "toCol": "id", "description": "采购申请所属项目"},
    {"from": "purchase_order",   "fromCol": "project_id",        "to": "project",        "toCol": "id", "description": "采购订单所属项目"},
    {"from": "purchase_order",   "fromCol": "contract_id",       "to": "contract",       "toCol": "id", "description": "采购关联合同"},
    {"from": "ip_application",   "fromCol": "project_id",        "to": "project",        "toCol": "id", "description": "知产申请来源项目"},
    {"from": "ip_application",   "fromCol": "purchase_order_id", "to": "purchase_order", "toCol": "id", "description": "知产申请关联采购单"},
    {"from": "project_cost",     "fromCol": "purchase_order_id", "to": "purchase_order", "toCol": "id", "description": "项目成本关联采购单"},
]


def main():
    merged_tables: dict = {}
    merged_rels: list = []
    merged_table_descs: dict = {}
    domain_notes: list = []

    # ── 1. Load and merge all domains ────────────────────────────────────────
    for dom in DOMAINS:
        schema_path  = FOX_CRM / dom / "schema.yaml"
        catalog_path = FOX_CRM / dom / "catalog.yaml"

        if not schema_path.exists():
            print(f"  SKIP {dom} (missing schema)")
            continue

        schema  = yaml.safe_load(schema_path.read_text(encoding="utf-8")) or {}
        catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) if catalog_path.exists() else {}

        tables  = schema.get("tables", {})
        rels    = catalog.get("relationships", []) if catalog else []
        t_descs = catalog.get("tables", {})        if catalog else {}

        # check for name conflicts
        conflicts = set(tables) & set(merged_tables)
        if conflicts:
            print(f"  WARNING: {dom} has name conflicts: {conflicts}")

        merged_tables.update(tables)
        merged_table_descs.update(t_descs)
        merged_rels.extend(rels)
        domain_notes.append(f"域 {dom}：{len(tables)} 张表")
        print(f"  {dom}: {len(tables)} tables, {len(rels)} intra-domain rels")

    # ── 2. Inject cross-domain FK columns ────────────────────────────────────
    fk_added = 0
    for (tname, col, typ, target, desc) in CROSS_DOMAIN_FKS:
        if tname not in merged_tables:
            print(f"  SKIP FK {tname}.{col}: table not found")
            continue
        cols = merged_tables[tname]["columns"]
        if col in cols:
            continue  # already exists (intra-domain FK with same name)
        # Insert FK column just before created_at
        new_cols: dict = {}
        for k, v in cols.items():
            if k == "created_at":
                new_cols[col] = {"type": typ, "description": desc}
            new_cols[k] = v
        if col not in new_cols:         # created_at might be missing
            new_cols[col] = {"type": typ, "description": desc}
        merged_tables[tname]["columns"] = new_cols
        fk_added += 1

    print(f"\n  FK columns added: {fk_added}")

    # ── 3. Append cross-domain relationships ────────────────────────────────
    # Deduplicate by (from, fromCol, to, toCol)
    existing_rel_keys = {
        (r["from"], r["fromCol"], r["to"], r["toCol"])
        for r in merged_rels if isinstance(r, dict)
    }
    for rel in CROSS_DOMAIN_RELS:
        key = (rel["from"], rel["fromCol"], rel["to"], rel["toCol"])
        if key not in existing_rel_keys:
            merged_rels.append(rel)
            existing_rel_keys.add(key)

    # ── 4. Write output ───────────────────────────────────────────────────────
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    schema_out = {"tables": merged_tables}
    catalog_out = {
        "description": "Fox-CRM 统一数据模型 — 整合 CRM、客户管理、投标、合同、HR、知产、项目、采购八个业务域",
        "tables": merged_table_descs,
        "relationships": merged_rels,
        "notes": domain_notes + [
            "customer_mgmt 各表通过 customer_id/contact_id 关联 crm 域的 customer/contact",
            "主业务链：opportunity → bid_tender → contract → project",
            "project 是成本归集中心，purchase/ipasset/hr.emp_project 均可挂靠",
        ],
    }

    (OUT_DIR / "schema.yaml").write_text(
        yaml.dump(schema_out,  allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    (OUT_DIR / "catalog.yaml").write_text(
        yaml.dump(catalog_out, allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )

    total_rels = len(merged_rels)
    print(f"\n  Total: {len(merged_tables)} tables, {total_rels} relationships")
    print(f"  Output → {OUT_DIR}")


if __name__ == "__main__":
    main()
