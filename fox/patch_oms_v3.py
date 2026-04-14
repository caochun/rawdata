"""Third-pass patch for fox-oms schema.

Changes:
  1. Fix type errors from over-eager number conversion + missed datetime
  2. Remaining string → uuid FK upgrades
  3. Remove redundant string fields in contract (have FK counterparts)
  4. Unify payment_method enum between contract and purchase_order
  5. Rename 'note' → 'remark' in new tables (standardize to remark)
  6. Add ip_application.contract_id FK
  7. Add new catalog relationships

Usage:
    python3 patch_oms_v3.py
"""
from __future__ import annotations
import yaml
from pathlib import Path

OMS = Path(__file__).parent.parent / "fox-oms"


# ── helpers ──────────────────────────────────────────────────────────────────

def replace_col(cols: dict, old_key: str, new_key: str, new_val: dict) -> dict:
    """Replace old_key with new_key (same position), skip if new_key already exists."""
    if old_key not in cols:
        return cols
    result: dict = {}
    for k, v in cols.items():
        if k == old_key:
            if new_key not in result:
                result[new_key] = new_val
        else:
            result[k] = v
    return result


def insert_before(cols: dict, before: str, key: str, val: dict) -> dict:
    if key in cols:
        return cols
    result: dict = {}
    for k, v in cols.items():
        if k == before:
            result[key] = val
        result[k] = v
    if key not in result:
        result[key] = val
    return result


def uuid_fk(desc: str, required: bool = False) -> dict:
    d: dict = {"type": "uuid", "description": desc}
    if required:
        d["required"] = True
    return d


def remove_keys(cols: dict, keys: list[str]) -> dict:
    return {k: v for k, v in cols.items() if k not in keys}


# ── 1. Type error fixes ───────────────────────────────────────────────────────

# (table, column, correct_type)  — for simple type overwrites
TYPE_FIXES: list[tuple[str, str, str]] = [
    # number → string (account numbers, narrative text)
    ("customer_bid_site",     "account_no",          "string"),
    ("bid_account",           "account_no",          "string"),
    ("customer_invoice_info", "bank_account",        "string"),
    ("project_cost",          "cost_name",           "string"),
    ("bid_tender",            "price_strategy_detail","string"),
    ("bid_tender",            "price_score_method",  "string"),
    # string → datetime
    ("bid_research",          "submitted_at",        "datetime"),
]

# bid_tender.price_internal_contact: number → uuid FK (person reference)
# handled separately below


def fix_types(tables: dict) -> dict:
    for tname, cname, new_type in TYPE_FIXES:
        col = tables[tname]["columns"].get(cname)
        if col:
            col["type"] = new_type

    # price_internal_contact: number → uuid FK
    pic = tables["bid_tender"]["columns"].get("price_internal_contact")
    if pic:
        pic["type"] = "uuid"
        pic["description"] = "报价内部对接人"

    return tables


# ── 2. Remaining string → uuid FK upgrades ───────────────────────────────────

def fix_fks(tables: dict) -> dict:

    # cust_department.customer_name → customer_id
    tables["cust_department"]["columns"] = replace_col(
        tables["cust_department"]["columns"],
        "customer_name", "customer_id",
        uuid_fk("所属客户", required=True)
    )

    # employee.department_name → department_id
    tables["employee"]["columns"] = replace_col(
        tables["employee"]["columns"],
        "department_name", "department_id",
        uuid_fk("所属部门")
    )

    # emp_project.customer_name → customer_id
    tables["emp_project"]["columns"] = replace_col(
        tables["emp_project"]["columns"],
        "customer_name", "customer_id",
        uuid_fk("服务客户")
    )

    # bid_research.pre_reg_name → bid_pre_reg_id
    tables["bid_research"]["columns"] = replace_col(
        tables["bid_research"]["columns"],
        "pre_reg_name", "bid_pre_reg_id",
        uuid_fk("所属预投标登记")
    )

    # bid_research.person_in_charge → person_id
    tables["bid_research"]["columns"] = replace_col(
        tables["bid_research"]["columns"],
        "person_in_charge", "person_id",
        uuid_fk("负责人")
    )

    # ip_application.contract_id (new field, insert before created_at)
    tables["ip_application"]["columns"] = insert_before(
        tables["ip_application"]["columns"],
        "created_at",
        "contract_id",
        uuid_fk("关联合同")
    )

    # bid_tender.price_internal_contact already handled in fix_types (uuid)
    # add it to bid_tender handler_id chain
    # (already uuid, just ensure it's in catalog rels — handled in PATCH_RELS)

    return tables


# ── 3. Remove redundant string fields in contract ─────────────────────────────

CONTRACT_REDUNDANT = [
    "opportunity_name",    # have opportunity_id
    "bid_tender_name",     # have bid_tender_id
    "project_name",        # derivable via project.contract_id
]


def fix_contract(tables: dict) -> dict:
    # remove redundant strings
    tables["contract"]["columns"] = remove_keys(
        tables["contract"]["columns"], CONTRACT_REDUNDANT
    )
    # second_customer_name → second_customer_id
    tables["contract"]["columns"] = replace_col(
        tables["contract"]["columns"],
        "second_customer_name", "second_customer_id",
        uuid_fk("第二甲方客户")
    )
    return tables


# ── 4. Unify payment_method enum ─────────────────────────────────────────────

UNIFIED_PAYMENT_METHOD = ["prepaid", "lump_sum", "installment", "milestone", "monthly", "cod"]


def unify_payment_method(tables: dict) -> dict:
    for tname in ("contract", "purchase_order", "contract_payment"):
        col = tables[tname]["columns"].get("payment_method")
        if col and col.get("type") == "enum":
            col["values"] = UNIFIED_PAYMENT_METHOD
    return tables


# ── 5. Rename 'note' → 'remark' (only in new tables added in round 1/2) ──────

# Only rename in tables where 'note' was introduced by us (not original fox tables)
RENAME_NOTE_TABLES = [
    "contract_payment",
    "project_acceptance",
    "competitor",
    "contract_line_item",
    "project_team",
    "customer_partnership",
]


def rename_note_to_remark(tables: dict) -> dict:
    for tname in RENAME_NOTE_TABLES:
        cols = tables[tname]["columns"]
        if "note" in cols and "remark" not in cols:
            # rename in-place preserving order
            new_cols: dict = {}
            for k, v in cols.items():
                new_cols["remark" if k == "note" else k] = v
            tables[tname]["columns"] = new_cols
    return tables


# ── 6. New catalog relationships ──────────────────────────────────────────────

PATCH_RELS: list[dict] = [
    {"from": "cust_department",  "fromCol": "customer_id",      "to": "customer",   "toCol": "id", "description": "客户组织架构所属客户"},
    {"from": "employee",         "fromCol": "department_id",    "to": "department",  "toCol": "id", "description": "员工所属部门"},
    {"from": "emp_project",      "fromCol": "customer_id",      "to": "customer",    "toCol": "id", "description": "项目经历服务客户"},
    {"from": "bid_research",     "fromCol": "bid_pre_reg_id",   "to": "bid_pre_reg", "toCol": "id", "description": "调研表关联预投标"},
    {"from": "bid_research",     "fromCol": "person_id",        "to": "employee",    "toCol": "id", "description": "调研负责人"},
    {"from": "bid_tender",       "fromCol": "price_internal_contact", "to": "employee", "toCol": "id", "description": "报价内部对接人"},
    {"from": "ip_application",   "fromCol": "contract_id",      "to": "contract",    "toCol": "id", "description": "知产申请关联合同"},
    {"from": "contract",         "fromCol": "second_customer_id","to": "customer",   "toCol": "id", "description": "第二甲方客户"},
]


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    schema  = yaml.safe_load((OMS / "schema.yaml").read_text(encoding="utf-8"))
    catalog = yaml.safe_load((OMS / "catalog.yaml").read_text(encoding="utf-8"))

    t = schema["tables"]

    t = fix_types(t)
    print("  Type fixes:           done")

    t = fix_fks(t)
    print("  FK upgrades:          done")

    t = fix_contract(t)
    print("  Contract cleanup:     done")

    t = unify_payment_method(t)
    print("  payment_method enum:  unified")

    t = rename_note_to_remark(t)
    print("  note→remark:          done")

    # Add relationships
    existing = {
        (r["from"], r["fromCol"], r["to"], r["toCol"])
        for r in catalog["relationships"] if isinstance(r, dict)
    }
    added = 0
    for rel in PATCH_RELS:
        key = (rel["from"], rel["fromCol"], rel["to"], rel["toCol"])
        if key not in existing:
            catalog["relationships"].append(rel)
            existing.add(key)
            added += 1
    print(f"  Relationships:        +{added} new (total {len(catalog['relationships'])})")

    schema["tables"] = t

    (OMS / "schema.yaml").write_text(
        yaml.dump(schema,  allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    (OMS / "catalog.yaml").write_text(
        yaml.dump(catalog, allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    print(f"\n  Done → {OMS}")


if __name__ == "__main__":
    main()
