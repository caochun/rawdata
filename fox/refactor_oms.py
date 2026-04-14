"""Refactor fox-oms schema based on audit findings.

Changes applied:
  P1 – Data integrity
    - Add contract_payment table (实收款)
    - Convert key string FKs → uuid FKs
    - Add bid_tender.bid_pre_reg_id
    - Add contract.customer_id, transfer_from_contract_id

  P2 – Stage handoffs
    - Remove customer_category (keep customer_level)
    - Remove customer.bid_website, customer.taxpayer_id (covered by sub-tables)
    - Remove bid_tender.is_our_win (redundant with result)
    - Move customer partner fields → customer_partnership table
    - Remove emp_project.project_name (already has project_id FK)
    - Add project.manager_id, remove project.manager (string)
    - Replace opportunity string refs → uuid FKs
    - Replace contract party_a string refs → uuid FKs

  P3 – Auditability / completeness
    - Add project_acceptance table
    - Add competitor master table
    - Add contract_line_item table (contract ↔ product)
    - Add project_team table (active member assignments)

Usage:
    python3 refactor_oms.py
"""
from __future__ import annotations
import copy
import yaml
from pathlib import Path

IN_DIR  = Path(__file__).parent.parent / "fox-oms"
OUT_DIR = IN_DIR   # overwrite in-place

# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def insert_col_after(cols: dict, after: str, new_key: str, new_val: dict) -> dict:
    """Return new ordered dict with new_key inserted after 'after'."""
    result: dict = {}
    for k, v in cols.items():
        result[k] = v
        if k == after and new_key not in result:
            result[new_key] = new_val
    if new_key not in result:
        result[new_key] = new_val
    return result


def insert_col_before(cols: dict, before: str, new_key: str, new_val: dict) -> dict:
    """Return new ordered dict with new_key inserted before 'before'."""
    result: dict = {}
    for k, v in cols.items():
        if k == before and new_key not in result:
            result[new_key] = new_val
        result[k] = v
    if new_key not in result:
        result[new_key] = new_val
    return result


def remove_cols(cols: dict, names: list[str]) -> dict:
    return {k: v for k, v in cols.items() if k not in names}


def uuid_fk(desc: str, required: bool = False) -> dict:
    d: dict = {"type": "uuid", "description": desc}
    if required:
        d["required"] = True
    return d


def make_table(cols: dict) -> dict:
    """Wrap columns dict in table structure."""
    return {"columns": cols}


def std_cols(*extra_before_created: tuple[str, dict]) -> dict:
    """Return {id, ...extras, created_at}."""
    cols: dict = {"id": {"type": "uuid", "primary": True, "auto": True, "description": "唯一标识"}}
    for name, defn in extra_before_created:
        cols[name] = defn
    cols["created_at"] = {"type": "datetime", "auto": True, "description": "创建时间"}
    return cols

# ─────────────────────────────────────────────────────────────────────────────
# New tables
# ─────────────────────────────────────────────────────────────────────────────

NEW_TABLES: dict[str, dict] = {

    # P1: actual payment receipts (合同实收款)
    "contract_payment": make_table(std_cols(
        ("contract_id",      uuid_fk("所属合同", required=True)),
        ("invoice_id",       uuid_fk("关联发票")),
        ("payment_amount",   {"type": "string", "required": True, "description": "到款金额（万元）"}),
        ("payment_date",     {"type": "datetime", "required": True, "description": "到款日期"}),
        ("payment_method",   {"type": "enum", "values": ["bank_transfer", "check", "cash", "other"], "description": "付款方式"}),
        ("receipt_ref",      {"type": "string", "description": "银行流水号/回单编号"}),
        ("payer",            {"type": "string", "description": "付款方名称"}),
        ("reconciled",       {"type": "enum", "values": ["yes", "no"], "description": "是否已对账"}),
        ("reconciled_date",  {"type": "datetime", "description": "对账日期"}),
        ("note",             {"type": "string", "description": "备注"}),
    )),

    # P3: project delivery acceptance (交付验收)
    "project_acceptance": make_table(std_cols(
        ("project_id",       uuid_fk("所属项目", required=True)),
        ("contract_id",      uuid_fk("关联合同")),
        ("acceptance_type",  {"type": "enum", "values": ["initial", "phased", "final"], "description": "验收类型"}),
        ("accepted_by",      {"type": "string", "required": True, "description": "客户验收方（联系人姓名）"}),
        ("accepted_by_id",   uuid_fk("客户验收联系人")),
        ("accepted_date",    {"type": "datetime", "required": True, "description": "验收日期"}),
        ("result",           {"type": "enum", "values": ["pass", "conditional", "fail"], "required": True, "description": "验收结论"}),
        ("sign_document_url",{"type": "string", "description": "验收文件 URL"}),
        ("note",             {"type": "string", "description": "验收说明"}),
    )),

    # P3: competitor master (竞争对手主档)
    "competitor": make_table(std_cols(
        ("name",             {"type": "string", "required": True, "description": "公司名称"}),
        ("short_name",       {"type": "string", "description": "简称"}),
        ("tier",             {"type": "enum", "values": ["tier1", "tier2", "tier3"], "description": "竞争等级"}),
        ("strength",         {"type": "string", "description": "核心优势"}),
        ("weakness",         {"type": "string", "description": "主要弱点"}),
        ("typical_discount", {"type": "string", "description": "惯常折扣率（%）"}),
        ("win_rate_vs_us",   {"type": "string", "description": "对我司胜率（%）"}),
        ("main_products",    {"type": "string", "description": "主要产品/方案"}),
        ("note",             {"type": "string", "description": "备注"}),
    )),

    # P3: contract line items (合同明细，关联产品)
    "contract_line_item": make_table(std_cols(
        ("contract_id",      uuid_fk("所属合同", required=True)),
        ("product_id",       uuid_fk("产品")),
        ("item_name",        {"type": "string", "required": True, "description": "明细名称"}),
        ("quantity",         {"type": "string", "description": "数量"}),
        ("unit_price",       {"type": "string", "description": "单价（万元）"}),
        ("amount",           {"type": "string", "description": "金额（万元）"}),
        ("note",             {"type": "string", "description": "备注"}),
    )),

    # P3: active project team assignments (项目成员分配)
    "project_team": make_table(std_cols(
        ("project_id",       uuid_fk("所属项目", required=True)),
        ("employee_id",      uuid_fk("员工", required=True)),
        ("role",             {"type": "enum", "values": ["pm", "architect", "engineer", "qa", "consultant", "other"],
                              "required": True, "description": "项目角色"}),
        ("start_date",       {"type": "datetime", "description": "加入日期"}),
        ("end_date",         {"type": "datetime", "description": "离开日期"}),
        ("planned_hours",    {"type": "string", "description": "计划工时"}),
        ("actual_hours",     {"type": "string", "description": "实际工时"}),
        ("note",             {"type": "string", "description": "备注"}),
    )),

    # P2: partner-specific fields (从 customer 拆出来)
    "customer_partnership": make_table(std_cols(
        ("customer_id",           uuid_fk("客户", required=True)),
        ("partner_type",          {"type": "enum", "values": ["supplier", "channel", "service", "consultant"],
                                   "description": "合作类型"}),
        ("cooperation_level",     {"type": "enum", "values": ["strategic", "core", "normal"],
                                   "description": "合作等级"}),
        ("partner_relation_type", {"type": "enum", "values": ["long_term", "occasional", "normal"],
                                   "description": "合作关系"}),
        ("can_accompany_bid",     {"type": "enum", "values": ["yes", "no"], "description": "可陪标"}),
        ("partner_referrer",      {"type": "string", "description": "推荐人"}),
        ("effective_from",        {"type": "datetime", "description": "合作开始日期"}),
        ("note",                  {"type": "string", "description": "备注"}),
    )),
}

NEW_TABLE_DESCS: dict[str, str] = {
    "contract_payment":     "合同实收款记录",
    "project_acceptance":   "项目交付验收",
    "competitor":           "竞争对手主档",
    "contract_line_item":   "合同产品明细",
    "project_team":         "项目成员分配",
    "customer_partnership": "客户合作伙伴关系",
}

# ─────────────────────────────────────────────────────────────────────────────
# New cross-domain relationships (additions to catalog)
# ─────────────────────────────────────────────────────────────────────────────

NEW_RELS: list[dict] = [
    {"from": "contract",            "fromCol": "customer_id",             "to": "customer",        "toCol": "id", "description": "合同甲方客户"},
    {"from": "contract",            "fromCol": "party_a_contact_id",      "to": "contact",         "toCol": "id", "description": "合同甲方联系人"},
    {"from": "contract",            "fromCol": "transfer_from_contract_id","to": "contract",       "toCol": "id", "description": "转包来源合同"},
    {"from": "opportunity",         "fromCol": "customer_id",             "to": "customer",        "toCol": "id", "description": "商机所属客户"},
    {"from": "opportunity",         "fromCol": "contact_id",              "to": "contact",         "toCol": "id", "description": "商机主要联系人"},
    {"from": "opportunity",         "fromCol": "sales_person_id",         "to": "employee",        "toCol": "id", "description": "销售负责人"},
    {"from": "opportunity",         "fromCol": "handler_id",              "to": "employee",        "toCol": "id", "description": "商务负责人"},
    {"from": "bid_tender",          "fromCol": "bid_pre_reg_id",          "to": "bid_pre_reg",     "toCol": "id", "description": "来源预投标登记"},
    {"from": "project",             "fromCol": "manager_id",              "to": "employee",        "toCol": "id", "description": "项目负责人"},
    {"from": "contract_payment",    "fromCol": "contract_id",             "to": "contract",        "toCol": "id", "description": "所属合同"},
    {"from": "contract_payment",    "fromCol": "invoice_id",              "to": "contract_invoice","toCol": "id", "description": "关联发票"},
    {"from": "project_acceptance",  "fromCol": "project_id",              "to": "project",         "toCol": "id", "description": "验收所属项目"},
    {"from": "project_acceptance",  "fromCol": "contract_id",             "to": "contract",        "toCol": "id", "description": "验收关联合同"},
    {"from": "project_acceptance",  "fromCol": "accepted_by_id",          "to": "contact",         "toCol": "id", "description": "客户验收联系人"},
    {"from": "contract_line_item",  "fromCol": "contract_id",             "to": "contract",        "toCol": "id", "description": "所属合同"},
    {"from": "contract_line_item",  "fromCol": "product_id",              "to": "product",         "toCol": "id", "description": "关联产品"},
    {"from": "project_team",        "fromCol": "project_id",              "to": "project",         "toCol": "id", "description": "所属项目"},
    {"from": "project_team",        "fromCol": "employee_id",             "to": "employee",        "toCol": "id", "description": "项目成员"},
    {"from": "customer_partnership","fromCol": "customer_id",             "to": "customer",        "toCol": "id", "description": "合作伙伴所属客户"},
]


# ─────────────────────────────────────────────────────────────────────────────
# Main refactoring
# ─────────────────────────────────────────────────────────────────────────────

def refactor(tables: dict) -> dict:
    t = copy.deepcopy(tables)

    # ── customer ──────────────────────────────────────────────────────────────
    t["customer"]["columns"] = remove_cols(t["customer"]["columns"], [
        "customer_category",        # P2: redundant with customer_level
        "bid_website",              # P2: covered by customer_bid_site table
        "taxpayer_id",              # P2: covered by customer_invoice_info.tax_id
        "partner_type",             # P2: moved to customer_partnership
        "cooperation_level",        # P2: moved to customer_partnership
        "partner_relation_type",    # P2: moved to customer_partnership
        "can_accompany_bid",        # P2: moved to customer_partnership
        "partner_referrer",         # P2: moved to customer_partnership
    ])

    # ── opportunity ───────────────────────────────────────────────────────────
    opp = t["opportunity"]["columns"]
    # Remove string refs, add uuid FKs after id
    opp = remove_cols(opp, ["customer_name", "contact_name", "sales_person", "provider", "handler"])
    opp = insert_col_after(opp, "id",
        "customer_id", uuid_fk("所属客户", required=True))
    opp = insert_col_after(opp, "customer_id",
        "contact_id",  uuid_fk("主要联系人"))
    opp = insert_col_after(opp, "contact_id",
        "sales_person_id", uuid_fk("销售负责人"))
    opp = insert_col_after(opp, "sales_person_id",
        "handler_id",  uuid_fk("商务负责人"))
    t["opportunity"]["columns"] = opp

    # ── bid_tender ────────────────────────────────────────────────────────────
    bt = t["bid_tender"]["columns"]
    bt = remove_cols(bt, ["is_our_win"])            # P2: redundant with result
    bt = insert_col_after(bt, "id",
        "bid_pre_reg_id", uuid_fk("来源预投标登记"))  # P1: handoff FK
    t["bid_tender"]["columns"] = bt

    # ── contract ──────────────────────────────────────────────────────────────
    ct = t["contract"]["columns"]
    # Remove string refs that are replaced by FKs
    ct = remove_cols(ct, ["party_a_contact", "party_a_phone", "transfer_contract_no"])
    # Add uuid FKs
    ct = insert_col_after(ct, "id",
        "customer_id",     uuid_fk("甲方客户", required=True))
    ct = insert_col_after(ct, "customer_id",
        "party_a_contact_id", uuid_fk("甲方联系人"))
    ct = insert_col_after(ct, "transfer_from_company",
        "transfer_from_contract_id", uuid_fk("转包来源合同"))
    t["contract"]["columns"] = ct

    # ── project ───────────────────────────────────────────────────────────────
    proj = t["project"]["columns"]
    proj = remove_cols(proj, ["manager"])           # replaced by manager_id
    proj = insert_col_after(proj, "dept",
        "manager_id", uuid_fk("项目负责人"))
    t["project"]["columns"] = proj

    # ── emp_project ───────────────────────────────────────────────────────────
    t["emp_project"]["columns"] = remove_cols(
        t["emp_project"]["columns"], ["project_name"]  # already has project_id FK
    )

    # ── bid_competitor ────────────────────────────────────────────────────────
    bc = t["bid_competitor"]["columns"]
    bc = insert_col_after(bc, "company_name",
        "competitor_id", uuid_fk("竞争对手主档"))
    t["bid_competitor"]["columns"] = bc

    # ── Add new tables ────────────────────────────────────────────────────────
    t.update(NEW_TABLES)

    return t


def main():
    schema_path  = IN_DIR / "schema.yaml"
    catalog_path = IN_DIR / "catalog.yaml"

    schema  = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))

    old_count = len(schema["tables"])
    schema["tables"] = refactor(schema["tables"])
    new_count = len(schema["tables"])

    # Update catalog table descriptions
    catalog["tables"].update(NEW_TABLE_DESCS)

    # Remove descriptions for deleted/merged fields (not needed, just add new)

    # Append new relationships (deduplicate)
    existing_keys = {
        (r["from"], r["fromCol"], r["to"], r["toCol"])
        for r in catalog["relationships"] if isinstance(r, dict)
    }
    added = 0
    for rel in NEW_RELS:
        key = (rel["from"], rel["fromCol"], rel["to"], rel["toCol"])
        if key not in existing_keys:
            catalog["relationships"].append(rel)
            existing_keys.add(key)
            added += 1

    # Update description
    catalog["description"] = (
        "Fox-OMS 统一数据模型（已优化）— 整合 CRM、客户管理、投标、合同、HR、知产、项目、采购八个业务域。"
        "主链：market_lead → opportunity → bid_tender → contract → project → contract_payment。"
    )

    # Update notes
    catalog["notes"] = [
        "主业务链：customer/contact → opportunity → bid_pre_reg → bid_tender → contract → project → project_acceptance → contract_payment",
        "customer_partnership 存储合作伙伴专属字段（已从 customer 表拆出）",
        "所有跨表引用均使用 uuid FK；string 型名称字段仅供展示/归档，以 _id FK 为准",
        "contract_payment 是回款核心表，与 contract_invoice 形成 invoice→payment 闭环",
        "project_team 记录当前分配（活跃），emp_project 记录员工历史项目经历",
    ]

    # Write output
    schema_path.write_text(
        yaml.dump(schema,  allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    catalog_path.write_text(
        yaml.dump(catalog, allow_unicode=True, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )

    print(f"Tables: {old_count} → {new_count} (+{new_count - old_count} new)")
    print(f"Relationships: added {added} new")
    print(f"Output → {IN_DIR}")


if __name__ == "__main__":
    main()
