"""Second-pass patch for fox-oms schema.

Changes:
  1. Convert all numeric/amount/percentage fields from string → number
  2. Add remaining uuid FK columns (contact.customer_id, customer.parent_customer_id,
     contract.handler_id / internal_contact_id, sales_task.assignee_id,
     contract_invoice.receiver_id, contract_payment.payer_id,
     bid_review.reviewer_id, bid_progress/project_progress.operator_id,
     project_cost.supplier_id / person_id)
  3. Remove redundant string name fields that now have FK counterparts
  4. Fix enum values (bid_tender.status + cancelled, project_acceptance.result + rework,
     customer_partnership.partner_type + competitor, project_team.role + leader/sales/support)
  5. Add status_changed_at to contract / bid_tender / project
  6. Add missing catalog relationships

Usage:
    python3 patch_oms_v2.py
"""
from __future__ import annotations
import copy, re
import yaml
from pathlib import Path

OMS = Path(__file__).parent.parent / "fox-oms"


# ── helpers ──────────────────────────────────────────────────────────────────

def insert_after(cols: dict, after: str, key: str, val: dict) -> dict:
    if key in cols:
        return cols
    result: dict = {}
    for k, v in cols.items():
        result[k] = v
        if k == after:
            result[key] = val
    if key not in result:
        result[key] = val
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


def remove_keys(cols: dict, keys: list[str]) -> dict:
    return {k: v for k, v in cols.items() if k not in keys}


def uuid_fk(desc: str, required: bool = False) -> dict:
    d: dict = {"type": "uuid", "description": desc}
    if required:
        d["required"] = True
    return d


# ── numeric field detection ───────────────────────────────────────────────────

# Column names that indicate numeric value
_NUMERIC_NAME_RE = re.compile(
    r"(amount|price|rate|ratio|score|count|prob|margin|capital|limit|fee|total"
    r"|hours|weight|quantity|percent|budget|cost|revenue|salary|bonus|penalty"
    r"|win_prob|gross_margin|tax_rate|transfer_ratio|specialty_ratio|recovery_rate"
    r"|insured_count|r_score|f_score|m_score|total_score|sort_order|bid_amount"
    r"|win_amount|service_fee|bid_doc_fee|plan_ratio|plan_amount|actual_amount"
    r"|invoice_amount|contract_amount|invoiced_amount|received_amount|profit_amount"
    r"|profit_rate|transfer_amount|sub_amount|sub_tax_rate|old_amount|new_amount"
    r"|unit_price|typical_discount|win_rate_vs_us|planned_hours|actual_hours"
    r"|price_score_weight|business_score_weight|technical_score_weight|win_price_ratio"
    r"|payment_amount|research_amount|estimated_amount|project_amount|emp_project"
    r"|reg_capital|paid_capital|contract_total|cost_total|follow_count|total_amount)"
)

# Description keywords that indicate numeric
_NUMERIC_DESC_KEYWORDS = ["万元", "元", "（%）", "(%)", "比例", "得分", "分数",
                           "数量", "人数", "工时", "参保"]


def is_numeric_field(col_name: str, col_def: dict) -> bool:
    if col_def.get("type") != "string":
        return False
    if col_def.get("values"):        # enum — skip
        return False
    if _NUMERIC_NAME_RE.search(col_name):
        return True
    desc = col_def.get("description", "")
    return any(kw in desc for kw in _NUMERIC_DESC_KEYWORDS)


def convert_numeric_fields(tables: dict) -> tuple[dict, int]:
    tables = copy.deepcopy(tables)
    count = 0
    for tname, tdata in tables.items():
        for cname, cdef in tdata.get("columns", {}).items():
            if is_numeric_field(cname, cdef):
                cdef["type"] = "number"
                count += 1
    return tables, count


# ── FK additions ──────────────────────────────────────────────────────────────

def add_fks(tables: dict) -> dict:
    t = tables

    # contact.customer_id (after id)
    t["contact"]["columns"] = insert_after(
        t["contact"]["columns"], "id",
        "customer_id", uuid_fk("所属客户", required=True)
    )

    # customer.parent_customer_id (after parent_company_name → replace it)
    cust = t["customer"]["columns"]
    if "parent_company_name" in cust:
        new_cust: dict = {}
        for k, v in cust.items():
            if k == "parent_company_name":
                new_cust["parent_customer_id"] = uuid_fk("上级客户")
            else:
                new_cust[k] = v
        t["customer"]["columns"] = new_cust

    # opportunity.second_customer_id (replace second_customer_name)
    opp = t["opportunity"]["columns"]
    if "second_customer_name" in opp:
        new_opp: dict = {}
        for k, v in opp.items():
            if k == "second_customer_name":
                new_opp["second_customer_id"] = uuid_fk("第二客户")
            else:
                new_opp[k] = v
        t["opportunity"]["columns"] = new_opp

    # sales_task.assignee_id (replace assignee string)
    st = t["sales_task"]["columns"]
    if "assignee" in st:
        new_st: dict = {}
        for k, v in st.items():
            if k == "assignee":
                new_st["assignee_id"] = uuid_fk("负责人")
            else:
                new_st[k] = v
        t["sales_task"]["columns"] = new_st

    # contract: add handler_id + internal_contact_id, remove string versions
    ct = t["contract"]["columns"]
    ct = insert_after(ct, "party_a_contact_id",
        "internal_contact_id", uuid_fk("内部对接人"))
    if "handler" in ct:
        new_ct: dict = {}
        for k, v in ct.items():
            if k == "handler":
                new_ct["handler_id"] = uuid_fk("商务负责人")
            elif k == "internal_contact":
                pass  # remove — replaced by internal_contact_id above
            elif k == "transfer_from_company":
                pass  # remove — replaced by transfer_from_contract_id
            else:
                new_ct[k] = v
        t["contract"]["columns"] = new_ct
    elif "internal_contact" in ct:
        t["contract"]["columns"] = remove_keys(ct, ["internal_contact", "transfer_from_company"])

    # contract_invoice.receiver_id (replace receiver)
    ci = t["contract_invoice"]["columns"]
    if "receiver" in ci:
        new_ci: dict = {}
        for k, v in ci.items():
            if k == "receiver":
                new_ci["receiver_id"] = uuid_fk("接收联系人")
            else:
                new_ci[k] = v
        t["contract_invoice"]["columns"] = new_ci

    # contract_payment.payer_id (replace payer)
    cp = t["contract_payment"]["columns"]
    if "payer" in cp:
        new_cp: dict = {}
        for k, v in cp.items():
            if k == "payer":
                new_cp["payer_id"] = uuid_fk("付款方客户")
            else:
                new_cp[k] = v
        t["contract_payment"]["columns"] = new_cp

    # bid_review.reviewer_id (replace reviewer_name)
    br = t["bid_review"]["columns"]
    if "reviewer_name" in br:
        new_br: dict = {}
        for k, v in br.items():
            if k == "reviewer_name":
                new_br["reviewer_id"] = uuid_fk("审核人")
            else:
                new_br[k] = v
        t["bid_review"]["columns"] = new_br

    # bid_progress.operator_id (replace operator)
    bp = t["bid_progress"]["columns"]
    if "operator" in bp:
        new_bp: dict = {}
        for k, v in bp.items():
            if k == "operator":
                new_bp["operator_id"] = uuid_fk("操作人")
            else:
                new_bp[k] = v
        t["bid_progress"]["columns"] = new_bp

    # project_progress.operator_id (replace operator)
    pp = t["project_progress"]["columns"]
    if "operator" in pp:
        new_pp: dict = {}
        for k, v in pp.items():
            if k == "operator":
                new_pp["operator_id"] = uuid_fk("操作人")
            else:
                new_pp[k] = v
        t["project_progress"]["columns"] = new_pp

    # project_cost.supplier_id + person_id (replace strings)
    pc = t["project_cost"]["columns"]
    new_pc: dict = {}
    for k, v in pc.items():
        if k == "supplier_name":
            new_pc["supplier_id"] = uuid_fk("供应商客户")
        elif k == "person_name":
            new_pc["person_id"] = uuid_fk("负责人员工")
        else:
            new_pc[k] = v
    t["project_cost"]["columns"] = new_pc

    return t


# ── redundant string deletions ────────────────────────────────────────────────

REDUNDANT_FIELDS: dict[str, list[str]] = {
    "project":      ["customer_name", "opportunity_name"],
    "bid_tender":   ["pre_reg_name", "opportunity_name"],
    "bid_pre_reg":  ["opportunity_name"],
}


def remove_redundant(tables: dict) -> dict:
    for tname, fields in REDUNDANT_FIELDS.items():
        if tname in tables:
            tables[tname]["columns"] = remove_keys(tables[tname]["columns"], fields)
    return tables


# ── enum fixes ────────────────────────────────────────────────────────────────

def fix_enums(tables: dict) -> dict:
    def add_value(tname: str, col: str, value: str):
        vals = tables[tname]["columns"][col].get("values", [])
        if value not in vals:
            vals.append(value)
            tables[tname]["columns"][col]["values"] = vals

    add_value("bid_tender",          "status",       "cancelled")
    add_value("project_acceptance",  "result",       "rework")
    add_value("customer_partnership","partner_type",  "competitor")
    add_value("project_team",        "role",         "leader")
    add_value("project_team",        "role",         "sales")
    add_value("project_team",        "role",         "support")
    return tables


# ── status_changed_at fields ─────────────────────────────────────────────────

STATUS_TABLES = ["contract", "bid_tender", "project"]


def add_status_timestamps(tables: dict) -> dict:
    for tname in STATUS_TABLES:
        cols = tables[tname]["columns"]
        if "status_changed_at" not in cols:
            cols = insert_before(cols, "created_at",
                "status_changed_at",
                {"type": "datetime", "description": "状态最后变更时间"})
            tables[tname]["columns"] = cols
    return tables


# ── new catalog relationships ─────────────────────────────────────────────────

PATCH_RELS: list[dict] = [
    {"from": "contact",          "fromCol": "customer_id",          "to": "customer",        "toCol": "id", "description": "联系人所属客户"},
    {"from": "customer",         "fromCol": "parent_customer_id",    "to": "customer",        "toCol": "id", "description": "上级客户"},
    {"from": "opportunity",      "fromCol": "contact_id",            "to": "contact",         "toCol": "id", "description": "商机主要联系人"},
    {"from": "opportunity",      "fromCol": "sales_person_id",       "to": "employee",        "toCol": "id", "description": "销售负责人"},
    {"from": "opportunity",      "fromCol": "handler_id",            "to": "employee",        "toCol": "id", "description": "商务负责人"},
    {"from": "opportunity",      "fromCol": "second_customer_id",    "to": "customer",        "toCol": "id", "description": "第二客户"},
    {"from": "sales_task",       "fromCol": "assignee_id",           "to": "employee",        "toCol": "id", "description": "任务负责人"},
    {"from": "contract",         "fromCol": "handler_id",            "to": "employee",        "toCol": "id", "description": "商务负责人"},
    {"from": "contract",         "fromCol": "internal_contact_id",   "to": "employee",        "toCol": "id", "description": "内部对接人"},
    {"from": "contract_invoice", "fromCol": "receiver_id",           "to": "contact",         "toCol": "id", "description": "发票接收联系人"},
    {"from": "contract_payment", "fromCol": "payer_id",              "to": "customer",        "toCol": "id", "description": "付款方客户"},
    {"from": "bid_review",       "fromCol": "reviewer_id",           "to": "employee",        "toCol": "id", "description": "审核人"},
    {"from": "bid_progress",     "fromCol": "operator_id",           "to": "employee",        "toCol": "id", "description": "操作人"},
    {"from": "project_progress", "fromCol": "operator_id",           "to": "employee",        "toCol": "id", "description": "操作人"},
    {"from": "project_cost",     "fromCol": "supplier_id",           "to": "customer",        "toCol": "id", "description": "供应商"},
    {"from": "project_cost",     "fromCol": "person_id",             "to": "employee",        "toCol": "id", "description": "负责人员工"},
]


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    schema  = yaml.safe_load((OMS / "schema.yaml").read_text(encoding="utf-8"))
    catalog = yaml.safe_load((OMS / "catalog.yaml").read_text(encoding="utf-8"))

    tables = schema["tables"]

    # 1. Convert numeric fields
    tables, n_numeric = convert_numeric_fields(tables)
    print(f"  Numeric conversions:  {n_numeric} fields → number")

    # 2. Add FK columns
    tables = add_fks(tables)
    print("  FK columns:           added")

    # 3. Remove redundant strings
    tables = remove_redundant(tables)
    print("  Redundant fields:     removed")

    # 4. Fix enums
    tables = fix_enums(tables)
    print("  Enum values:          patched")

    # 5. Add status timestamps
    tables = add_status_timestamps(tables)
    print("  status_changed_at:    added to contract/bid_tender/project")

    # 6. Append new relationships
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

    schema["tables"] = tables

    (OMS / "schema.yaml").write_text(
        yaml.dump(schema,  allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    (OMS / "catalog.yaml").write_text(
        yaml.dump(catalog, allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    print(f"\n  Tables: {len(tables)}  |  Done → {OMS}")


if __name__ == "__main__":
    main()
