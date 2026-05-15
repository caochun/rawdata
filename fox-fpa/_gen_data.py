#!/usr/bin/env python3
"""Generate realistic fox-fpa data for a power-industry IT company.

Company: 博远智能科技有限公司 — 电力行业信息化与智能化解决方案
Period: 2025-10 ~ 2026-04 (7 months of operations)
"""
import csv, uuid, random, os
from datetime import datetime, date, timedelta
from pathlib import Path

OUT = Path(__file__).parent
TS = "2026-01-01T00:00:00+08:00"
NOW = "2026-05-14T10:00:00+08:00"

def uid():
    return str(uuid.uuid4())

def write_csv(name, rows):
    if not rows:
        return
    path = OUT / f"{name}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  {name}: {len(rows)} rows")

# ══════════════════════════════════════════════════════════════════════
# 1. dim_department
# ══════════════════════════════════════════════════════════════════════
DEPT_IDS = {k: uid() for k in [
    "company", "tech", "sys_integ", "soft_dev", "market", "admin",
]}

departments = [
    {"id": DEPT_IDS["company"], "code": "HQ", "name": "博远智能科技", "parent_id": "", "level": 0,
     "leader_id": "", "dept_type": "management", "status": "active", "sort_order": 1,
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": DEPT_IDS["tech"], "code": "TECH", "name": "技术中心", "parent_id": DEPT_IDS["company"], "level": 1,
     "leader_id": "", "dept_type": "business", "status": "active", "sort_order": 10,
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": DEPT_IDS["sys_integ"], "code": "SI", "name": "系统集成部", "parent_id": DEPT_IDS["tech"], "level": 2,
     "leader_id": "", "dept_type": "business", "status": "active", "sort_order": 11,
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": DEPT_IDS["soft_dev"], "code": "SD", "name": "软件开发部", "parent_id": DEPT_IDS["tech"], "level": 2,
     "leader_id": "", "dept_type": "business", "status": "active", "sort_order": 12,
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": DEPT_IDS["market"], "code": "MKT", "name": "市场经营部", "parent_id": DEPT_IDS["company"], "level": 1,
     "leader_id": "", "dept_type": "business", "status": "active", "sort_order": 20,
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": DEPT_IDS["admin"], "code": "ADM", "name": "综合管理部", "parent_id": DEPT_IDS["company"], "level": 1,
     "leader_id": "", "dept_type": "support", "status": "active", "sort_order": 30,
     "created_at": TS, "updated_at": TS, "created_by": "system"},
]

# ══════════════════════════════════════════════════════════════════════
# 2. dim_employee
# ══════════════════════════════════════════════════════════════════════
# (name, gender, dept_key, position, rank, hire_date, status, emp_type, base_salary, social_insurance)
EMP_DEFS = [
    # 系统集成部 (7 人)
    ("赵刚",   "male",   "sys_integ", "部门经理",   "M2", "2019-03-15", "active", "full_time", 28000, 5600),
    ("钱伟",   "male",   "sys_integ", "高级工程师", "P7", "2020-06-01", "active", "full_time", 22000, 4400),
    ("孙丽华", "female", "sys_integ", "高级工程师", "P7", "2020-09-01", "active", "full_time", 21000, 4200),
    ("李明",   "male",   "sys_integ", "工程师",     "P6", "2021-07-15", "active", "full_time", 16000, 3200),
    ("周洋",   "male",   "sys_integ", "工程师",     "P5", "2023-03-01", "active", "full_time", 13000, 2600),
    ("吴佳",   "female", "sys_integ", "工程师",     "P5", "2023-08-01", "active", "full_time", 12500, 2500),
    ("郑浩",   "male",   "sys_integ", "助理工程师", "P4", "2025-07-01", "active", "full_time", 9000,  1800),

    # 软件开发部 (5 人)
    ("王建国", "male",   "soft_dev",  "部门经理",   "M2", "2018-05-01", "active", "full_time", 30000, 6000),
    ("陈晓燕", "female", "soft_dev",  "高级开发",   "P7", "2019-11-01", "active", "full_time", 25000, 5000),
    ("黄志强", "male",   "soft_dev",  "开发工程师", "P6", "2021-04-01", "active", "full_time", 18000, 3600),
    ("林小雨", "female", "soft_dev",  "开发工程师", "P5", "2023-06-01", "active", "full_time", 14000, 2800),
    ("张博文", "male",   "soft_dev",  "前端开发",   "P5", "2024-02-01", "active", "full_time", 13500, 2700),

    # 市场经营部 (3 人)
    ("刘洪涛", "male",   "market",    "部门经理",   "M2", "2017-08-01", "active", "full_time", 26000, 5200),
    ("徐静",   "female", "market",    "客户经理",   "P6", "2021-01-15", "active", "full_time", 15000, 3000),
    ("马超",   "male",   "market",    "商务专员",   "P5", "2022-09-01", "active", "full_time", 12000, 2400),

    # 综合管理部 (3 人)
    ("杨秀英", "female", "admin",     "部门经理",   "M2", "2018-01-15", "active", "full_time", 20000, 4000),
    ("何芳",   "female", "admin",     "HR主管",     "P6", "2020-03-01", "active", "full_time", 14000, 2800),
    ("许磊",   "male",   "admin",     "财务专员",   "P5", "2022-06-01", "active", "full_time", 12000, 2400),
]

EMP_IDS = {}
employees = []
for i, (name, gender, dept_key, pos, rank, hire, status, etype, salary, si) in enumerate(EMP_DEFS, 1):
    eid = uid()
    EMP_IDS[name] = eid
    employees.append({
        "id": eid, "employee_no": f"BY{i:03d}", "name": name, "gender": gender,
        "dept_id": DEPT_IDS[dept_key], "position": pos, "rank": rank,
        "hire_date": hire, "leave_date": "", "status": status, "employment_type": etype,
        "base_salary": salary, "social_insurance": si,
        "created_at": TS, "updated_at": TS, "created_by": "system",
    })

# backfill department leaders
leader_map = {
    "company": "刘洪涛", "tech": "王建国", "sys_integ": "赵刚",
    "soft_dev": "王建国", "market": "刘洪涛", "admin": "杨秀英",
}
for d in departments:
    for key, lname in leader_map.items():
        if d["id"] == DEPT_IDS[key]:
            d["leader_id"] = EMP_IDS[lname]

# ══════════════════════════════════════════════════════════════════════
# 3. dim_customer
# ══════════════════════════════════════════════════════════════════════
CUST_IDS = {k: uid() for k in [
    "sgcc", "sgcc_js", "sgcc_zj", "csg", "csg_sz", "huaneng", "datang", "xudian",
]}

customers = [
    {"id": CUST_IDS["sgcc"], "name": "国家电网有限公司", "short_name": "国网",
     "uscc": "91110000100001XXXX", "industry": "energy", "tier": "strategic",
     "parent_customer_id": "", "region": "全国",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["sgcc_js"], "name": "国网江苏省电力有限公司", "short_name": "国网江苏",
     "uscc": "91320000XXXXXXXXXX", "industry": "energy", "tier": "strategic",
     "parent_customer_id": CUST_IDS["sgcc"], "region": "江苏",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["sgcc_zj"], "name": "国网浙江省电力有限公司", "short_name": "国网浙江",
     "uscc": "91330000XXXXXXXXXX", "industry": "energy", "tier": "key",
     "parent_customer_id": CUST_IDS["sgcc"], "region": "浙江",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["csg"], "name": "中国南方电网有限责任公司", "short_name": "南方电网",
     "uscc": "91440000XXXXXXXXXX", "industry": "energy", "tier": "strategic",
     "parent_customer_id": "", "region": "华南",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["csg_sz"], "name": "南方电网深圳供电局", "short_name": "深圳供电",
     "uscc": "91440300XXXXXXXXXX", "industry": "energy", "tier": "key",
     "parent_customer_id": CUST_IDS["csg"], "region": "广东",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["huaneng"], "name": "华能国际电力股份有限公司", "short_name": "华能国际",
     "uscc": "91110000XXXXXXXXXX", "industry": "energy", "tier": "key",
     "parent_customer_id": "", "region": "北京",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["datang"], "name": "大唐国际发电股份有限公司", "short_name": "大唐发电",
     "uscc": "91110000YYYYYYYYYY", "industry": "energy", "tier": "normal",
     "parent_customer_id": "", "region": "北京",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CUST_IDS["xudian"], "name": "许继电气股份有限公司", "short_name": "许继电气",
     "uscc": "91411000ZZZZZZZZZZ", "industry": "manufacturing", "tier": "normal",
     "parent_customer_id": "", "region": "河南",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
]

# ══════════════════════════════════════════════════════════════════════
# 4. dim_project
# ══════════════════════════════════════════════════════════════════════
PROJ_IDS = {k: uid() for k in [
    "substation", "distribution", "dispatch", "energy_mgmt",
    "virt_sgcc_js", "virt_csg_sz", "presale_datang",
]}

def proj_row(**kw):
    base = {
        "id": "", "project_no": "", "name": "", "project_type": "actual",
        "dept_id": "", "customer_id": "", "pm_id": "", "start_date": "", "end_date": "",
        "status": "active", "participation_mode": "", "settlement_cycle": "",
        "settlement_amount": "", "description": "",
        "created_at": TS, "updated_at": TS, "created_by": "system",
    }
    base.update(kw)
    return base

projects = [
    proj_row(
        id=PROJ_IDS["substation"], project_no="P2025-001",
        name="江苏220kV智能变电站监控系统", project_type="actual",
        dept_id=DEPT_IDS["sys_integ"], customer_id=CUST_IDS["sgcc_js"],
        pm_id=EMP_IDS["赵刚"], start_date="2025-09-01", end_date="2026-06-30",
        status="active", description="含监控主机、保护信息子站、在线监测系统集成",
    ),
    proj_row(
        id=PROJ_IDS["distribution"], project_no="P2025-002",
        name="浙江配网自动化终端升级项目", project_type="actual",
        dept_id=DEPT_IDS["sys_integ"], customer_id=CUST_IDS["sgcc_zj"],
        pm_id=EMP_IDS["钱伟"], start_date="2025-10-15", end_date="2026-04-30",
        status="active", description="200台配网终端DTU/FTU升级改造及调试",
    ),
    proj_row(
        id=PROJ_IDS["dispatch"], project_no="P2025-003",
        name="华能电厂智能调度管理平台", project_type="actual",
        dept_id=DEPT_IDS["soft_dev"], customer_id=CUST_IDS["huaneng"],
        pm_id=EMP_IDS["王建国"], start_date="2025-11-01", end_date="2026-08-31",
        status="active", description="发电计划优化、AGC/AVC控制、经济调度算法平台",
    ),
    proj_row(
        id=PROJ_IDS["energy_mgmt"], project_no="P2026-001",
        name="深圳供电局能源管理系统", project_type="actual",
        dept_id=DEPT_IDS["soft_dev"], customer_id=CUST_IDS["csg_sz"],
        pm_id=EMP_IDS["陈晓燕"], start_date="2026-01-15", end_date="2026-09-30",
        status="active", description="园区级综合能源管理，含光伏、储能、充电桩接入",
    ),
    proj_row(
        id=PROJ_IDS["virt_sgcc_js"], project_no="V2025-001",
        name="国网江苏技术支撑驻场", project_type="virtual",
        dept_id=DEPT_IDS["sys_integ"], customer_id=CUST_IDS["sgcc_js"],
        pm_id=EMP_IDS["赵刚"], start_date="2025-10-01", end_date="2026-09-30",
        status="active", participation_mode="on_site", settlement_cycle="monthly",
        settlement_amount=3.5, description="驻场技术支撑，2人/月，单价3.5万/人月",
    ),
    proj_row(
        id=PROJ_IDS["virt_csg_sz"], project_no="V2026-001",
        name="南方电网深圳运维外派", project_type="virtual",
        dept_id=DEPT_IDS["sys_integ"], customer_id=CUST_IDS["csg_sz"],
        pm_id=EMP_IDS["孙丽华"], start_date="2026-02-01", end_date="2026-07-31",
        status="active", participation_mode="secondment", settlement_cycle="monthly",
        settlement_amount=3.0, description="运维支撑外派1人，单价3万/人月",
    ),
    proj_row(
        id=PROJ_IDS["presale_datang"], project_no="P2026-002",
        name="大唐集团智慧电厂售前", project_type="actual",
        dept_id=DEPT_IDS["market"], customer_id=CUST_IDS["datang"],
        pm_id=EMP_IDS["刘洪涛"], start_date="2026-03-01", end_date="2026-06-30",
        status="active", description="智慧电厂整体解决方案投标，含前期调研和方案编制",
    ),
]

# ══════════════════════════════════════════════════════════════════════
# 5. dim_contract
# ══════════════════════════════════════════════════════════════════════
CONT_IDS = {k: uid() for k in ["c_sub", "c_dist", "c_disp", "c_emgmt", "c_virt_js", "c_virt_sz"]}

contracts = [
    {"id": CONT_IDS["c_sub"], "contract_no": "HT2025-001", "name": "江苏220kV智能变电站监控系统采购合同",
     "project_id": PROJ_IDS["substation"], "customer_id": CUST_IDS["sgcc_js"],
     "amount": 380, "tax_rate": 13, "sign_date": "2025-08-20", "start_date": "2025-09-01",
     "end_date": "2026-06-30", "status": "executing", "payment_method": "milestone",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CONT_IDS["c_dist"], "contract_no": "HT2025-002", "name": "浙江配网终端升级改造合同",
     "project_id": PROJ_IDS["distribution"], "customer_id": CUST_IDS["sgcc_zj"],
     "amount": 165, "tax_rate": 13, "sign_date": "2025-10-10", "start_date": "2025-10-15",
     "end_date": "2026-04-30", "status": "executing", "payment_method": "installment",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CONT_IDS["c_disp"], "contract_no": "HT2025-003", "name": "华能智能调度平台开发合同",
     "project_id": PROJ_IDS["dispatch"], "customer_id": CUST_IDS["huaneng"],
     "amount": 520, "tax_rate": 6, "sign_date": "2025-10-28", "start_date": "2025-11-01",
     "end_date": "2026-08-31", "status": "executing", "payment_method": "milestone",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CONT_IDS["c_emgmt"], "contract_no": "HT2026-001", "name": "深圳综合能源管理系统合同",
     "project_id": PROJ_IDS["energy_mgmt"], "customer_id": CUST_IDS["csg_sz"],
     "amount": 286, "tax_rate": 6, "sign_date": "2026-01-10", "start_date": "2026-01-15",
     "end_date": "2026-09-30", "status": "executing", "payment_method": "installment",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CONT_IDS["c_virt_js"], "contract_no": "HT2025-V01", "name": "国网江苏技术支撑服务合同",
     "project_id": PROJ_IDS["virt_sgcc_js"], "customer_id": CUST_IDS["sgcc_js"],
     "amount": 84, "tax_rate": 6, "sign_date": "2025-09-25", "start_date": "2025-10-01",
     "end_date": "2026-09-30", "status": "executing", "payment_method": "monthly",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": CONT_IDS["c_virt_sz"], "contract_no": "HT2026-V01", "name": "深圳供电运维支撑服务合同",
     "project_id": PROJ_IDS["virt_csg_sz"], "customer_id": CUST_IDS["csg_sz"],
     "amount": 18, "tax_rate": 6, "sign_date": "2026-01-20", "start_date": "2026-02-01",
     "end_date": "2026-07-31", "status": "executing", "payment_method": "monthly",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
]

# ══════════════════════════════════════════════════════════════════════
# 6. dim_cost_category (tree)
# ══════════════════════════════════════════════════════════════════════
CC = {}  # code -> id
cc_rows = []
_cc_sort = [0]

def cc(code, name, parent_code, level, domain, is_leaf):
    _cc_sort[0] += 10
    cid = uid()
    CC[code] = cid
    cc_rows.append({
        "id": cid, "code": code, "name": name,
        "parent_id": CC.get(parent_code, ""), "level": level,
        "cost_domain": domain, "is_leaf": is_leaf, "sort_order": _cc_sort[0],
        "created_at": TS, "updated_at": TS, "created_by": "system",
    })

# 人力成本
cc("HR",      "人力成本",     "",     1, "staff",   "no")
cc("HR-REC",  "招聘成本",     "HR",   2, "staff",   "yes")
cc("HR-SAL",  "薪资福利",     "HR",   2, "staff",   "no")
cc("HR-SAL-B","基本工资",     "HR-SAL",3,"staff",   "yes")
cc("HR-SAL-P","绩效奖金",     "HR-SAL",3,"staff",   "yes")
cc("HR-SAL-S","社保公积金",   "HR-SAL",3,"staff",   "yes")
cc("HR-SAL-O","其他福利",     "HR-SAL",3,"staff",   "yes")
cc("HR-RW",   "奖惩",         "HR",   2, "staff",   "yes")
cc("HR-OFF",  "办公分摊",     "HR",   2, "shared",  "no")
cc("HR-OFF-C","电脑设备",     "HR-OFF",3,"shared",  "yes")
cc("HR-OFF-R","办公场地",     "HR-OFF",3,"shared",  "yes")
cc("HR-OFF-U","水电物业",     "HR-OFF",3,"shared",  "yes")
cc("HR-OTH",  "其他人员费用", "HR",   2, "staff",   "no")
cc("HR-OTH-E","招待费",       "HR-OTH",3,"staff",   "yes")
cc("HR-OTH-T","团建费",       "HR-OTH",3,"staff",   "yes")
cc("HR-OTH-L","培训费",       "HR-OTH",3,"staff",   "yes")
cc("HR-LEV",  "离职成本",     "HR",   2, "staff",   "no")
cc("HR-LEV-C","经济补偿金",   "HR-LEV",3,"staff",   "yes")

# 业务费用
cc("BIZ",     "业务费用",     "",     1, "project", "no")
cc("BIZ-ENT", "招待费",       "BIZ",  2, "project", "yes")
cc("BIZ-EXP", "专家费",       "BIZ",  2, "project", "yes")
cc("BIZ-TST", "测试费",       "BIZ",  2, "project", "yes")
cc("BIZ-RPT", "报告费",       "BIZ",  2, "project", "yes")
cc("BIZ-IP",  "知识产权",     "BIZ",  2, "project", "no")
cc("BIZ-IP-P","专利费用",     "BIZ-IP",3,"project", "yes")
cc("BIZ-IP-A","论文费用",     "BIZ-IP",3,"project", "yes")
cc("BIZ-IP-S","软著费用",     "BIZ-IP",3,"project", "yes")
cc("BIZ-PRE", "前期调研",     "BIZ",  2, "project", "no")
cc("BIZ-PRE-E","专家支撑",    "BIZ-PRE",3,"project","yes")
cc("BIZ-PRE-M","模型采购",    "BIZ-PRE",3,"project","yes")
cc("BIZ-EQ",  "设备采购",     "BIZ",  2, "project", "yes")
cc("BIZ-BID", "招投标费用",   "BIZ",  2, "project", "no")
cc("BIZ-BID-S","中标服务费",  "BIZ-BID",3,"project","yes")
cc("BIZ-BID-C","客户维护费",  "BIZ-BID",3,"project","yes")
cc("BIZ-LAW", "法务支持",     "BIZ",  2, "project", "yes")
# 差旅
cc("BIZ-TRV", "差旅费",       "BIZ",  2, "project", "yes")

# ══════════════════════════════════════════════════════════════════════
# 7. dim_time_period — 2025-10-01 ~ 2026-05-31
# ══════════════════════════════════════════════════════════════════════
# China public holidays 2026 (approximate)
HOLIDAYS_2026 = {
    date(2026,1,1), date(2026,1,2), date(2026,1,3),  # 元旦
    # 春节 2026: Feb 17 (除夕) ~ Feb 23
    date(2026,2,17), date(2026,2,18), date(2026,2,19), date(2026,2,20),
    date(2026,2,21), date(2026,2,22), date(2026,2,23),
    # 清明 Apr 5-7
    date(2026,4,5), date(2026,4,6), date(2026,4,7),
    # 劳动节 May 1-5
    date(2026,5,1), date(2026,5,2), date(2026,5,3), date(2026,5,4), date(2026,5,5),
}
# Workday swaps (weekend days that are workdays due to holiday schedule)
SWAP_WORKDAYS = {
    date(2026,2,14), date(2026,2,15),  # 春节调休上班
    date(2026,4,12),  # 清明调休
    date(2026,4,26),  # 劳动节调休
}

def is_workday(d):
    if d in HOLIDAYS_2026:
        return False
    if d in SWAP_WORKDAYS:
        return True
    return d.weekday() < 5  # Mon-Fri

time_periods = []
d = date(2025, 10, 1)
end_d = date(2026, 5, 31)
while d <= end_d:
    time_periods.append({
        "id": uid(), "date": d.isoformat(), "year": d.year,
        "quarter": (d.month - 1) // 3 + 1, "month": d.month,
        "week": d.isocalendar()[1],
        "year_month": f"{d.year}-{d.month:02d}",
        "year_quarter": f"{d.year}-Q{(d.month-1)//3+1}",
        "is_workday": "yes" if is_workday(d) else "no",
    })
    d += timedelta(days=1)

# ══════════════════════════════════════════════════════════════════════
# 8. fact_timesheet — 2025-10 ~ 2026-04
# ══════════════════════════════════════════════════════════════════════
# Define who works on what projects with what allocation
# Format: (employee_name, project_key, hours_per_day, work_type, months, description)
# months: list of "YYYY-MM" strings when this assignment is active

ALL_MONTHS = ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04"]

ASSIGNMENTS = [
    # ── 系统集成部 ──
    # 赵刚: PM of 变电站, also manages 驻场
    ("赵刚", "substation",    6, "normal",  ALL_MONTHS, "项目管理、客户协调、进度跟踪"),
    ("赵刚", "virt_sgcc_js",  2, "normal",  ALL_MONTHS, "驻场人员管理与工作协调"),

    # 钱伟: PM of 配网, main on substation
    ("钱伟", "distribution",  5, "normal",  ["2025-10","2025-11","2025-12","2026-01","2026-02","2026-03","2026-04"], "配网终端现场调试与方案设计"),
    ("钱伟", "substation",    3, "normal",  ["2025-10","2025-11","2025-12","2026-01","2026-02","2026-03","2026-04"], "监控系统通信协议联调"),

    # 孙丽华: PM of 深圳运维, works on 变电站
    ("孙丽华", "substation",    5, "normal",  ["2025-10","2025-11","2025-12","2026-01"], "保护信息子站系统集成"),
    ("孙丽华", "virt_csg_sz",   8, "on_site", ["2026-02","2026-03","2026-04"], "驻场运维技术支撑"),

    # 李明: 变电站 + 配网
    ("李明", "substation",    4, "normal",  ALL_MONTHS, "在线监测系统安装与配置"),
    ("李明", "distribution",  4, "normal",  ALL_MONTHS, "DTU固件升级与功能测试"),

    # 周洋: 驻场 国网江苏
    ("周洋", "virt_sgcc_js",  8, "on_site", ALL_MONTHS, "驻场技术支撑-二次系统运维"),

    # 吴佳: 驻场 国网江苏 + 配网
    ("吴佳", "virt_sgcc_js",  5, "on_site", ALL_MONTHS, "驻场技术支撑-自动化终端维护"),
    ("吴佳", "distribution",  3, "normal",  ["2025-10","2025-11","2025-12","2026-01","2026-02"], "FTU现场调试"),

    # 郑浩: 配网 + 变电站
    ("郑浩", "distribution",  5, "normal",  ALL_MONTHS, "终端入网检测与资料整理"),
    ("郑浩", "substation",    3, "normal",  ALL_MONTHS, "辅助系统调试与文档编制"),

    # ── 软件开发部 ──
    # 王建国: PM of 调度平台
    ("王建国", "dispatch",     6, "normal",  ["2025-11","2025-12","2026-01","2026-02","2026-03","2026-04"], "架构设计、技术评审、客户沟通"),
    ("王建国", "energy_mgmt",  2, "normal",  ["2026-01","2026-02","2026-03","2026-04"], "技术方案指导"),

    # 陈晓燕: PM of 能源管理 + 调度平台
    ("陈晓燕", "dispatch",     4, "normal",  ["2025-11","2025-12","2026-01","2026-02"], "调度算法核心模块开发"),
    ("陈晓燕", "energy_mgmt",  6, "normal",  ["2026-01","2026-02","2026-03","2026-04"], "能源管理后端架构与核心API开发"),

    # 黄志强: 调度平台
    ("黄志强", "dispatch",     8, "normal",  ["2025-11","2025-12","2026-01","2026-02","2026-03","2026-04"], "AGC/AVC控制算法及接口开发"),

    # 林小雨: 能源管理 + 调度
    ("林小雨", "energy_mgmt",  6, "normal",  ["2026-01","2026-02","2026-03","2026-04"], "数据采集与设备接入模块开发"),
    ("林小雨", "dispatch",     2, "normal",  ["2025-11","2025-12"], "历史数据分析模块"),

    # 张博文: 能源管理前端 + 调度前端
    ("张博文", "dispatch",     5, "normal",  ["2025-11","2025-12","2026-01","2026-02"], "调度管理平台前端页面开发"),
    ("张博文", "energy_mgmt",  5, "normal",  ["2026-02","2026-03","2026-04"], "能源管理大屏与监控前端开发"),

    # ── 市场经营部 ──
    ("刘洪涛", "presale_datang", 4, "normal", ["2026-03","2026-04"], "大唐智慧电厂方案策划与商务对接"),
    ("刘洪涛", "substation",     2, "normal", ["2025-10","2025-11","2025-12"], "江苏项目验收协调"),
    ("刘洪涛", "dispatch",       1, "normal", ["2025-11","2025-12","2026-01"], "华能客户关系维护"),

    ("徐静", "presale_datang", 5, "normal", ["2026-03","2026-04"], "大唐方案编制与投标文件准备"),
    ("徐静", "distribution",   2, "normal", ["2025-10","2025-11","2025-12","2026-01"], "浙江项目回款跟踪"),
    ("徐静", "energy_mgmt",    2, "normal", ["2026-02","2026-03","2026-04"], "深圳能源管理项目商务支持"),

    ("马超", "presale_datang", 6, "normal", ["2026-03","2026-04"], "大唐投标材料整理与资质文件准备"),
    ("马超", "distribution",   3, "normal", ["2025-10","2025-11","2025-12","2026-01","2026-02"], "浙江项目合同执行与开票"),

    # ── 综合管理部 (不计入项目工时，用内部虚拟项目也可以，这里不生成) ──
]

timesheet = []
for emp_name, proj_key, hours, wtype, months, desc in ASSIGNMENTS:
    emp_id = EMP_IDS[emp_name]
    proj_id = PROJ_IDS[proj_key]
    for ym in months:
        y, m = int(ym[:4]), int(ym[5:])
        d = date(y, m, 1)
        if m == 12:
            end = date(y+1, 1, 1)
        else:
            end = date(y, m+1, 1)
        while d < end:
            if is_workday(d):
                # Add some variance: ±1 hour on some days
                random.seed(f"{emp_name}-{proj_key}-{d}")
                h = hours + random.choice([-1, 0, 0, 0, 0, 1])
                h = max(1, min(h, 10))
                timesheet.append({
                    "id": uid(), "employee_id": emp_id, "project_id": proj_id,
                    "work_date": d.isoformat(), "hours": h, "work_type": wtype,
                    "description": desc, "recorded_by": emp_name,
                    "created_at": NOW, "updated_at": NOW, "created_by": emp_name,
                })
            d += timedelta(days=1)

# ══════════════════════════════════════════════════════════════════════
# 9. fact_staff_cost — 2025-10 ~ 2026-04
# ══════════════════════════════════════════════════════════════════════
staff_cost = []
for emp in employees:
    name = emp["name"]
    eid = emp["id"]
    salary = emp["base_salary"]
    si = emp["social_insurance"]

    for ym in ALL_MONTHS:
        # 基本工资
        staff_cost.append({
            "id": uid(), "employee_id": eid, "year_month": ym,
            "category_id": CC["HR-SAL-B"], "amount": salary,
            "source": "payroll", "notes": "",
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })
        # 社保公积金
        staff_cost.append({
            "id": uid(), "employee_id": eid, "year_month": ym,
            "category_id": CC["HR-SAL-S"], "amount": si,
            "source": "payroll", "notes": "",
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })

    # 绩效奖金 — Q4 2025 (发在 2026-01) 和 Q1 2026 (发在 2026-04)
    random.seed(f"bonus-{name}")
    q4_bonus = round(salary * random.uniform(0.8, 1.5))
    q1_bonus = round(salary * random.uniform(0.7, 1.3))
    staff_cost.append({
        "id": uid(), "employee_id": eid, "year_month": "2026-01",
        "category_id": CC["HR-SAL-P"], "amount": q4_bonus,
        "source": "payroll", "notes": "2025年Q4绩效奖金",
        "created_at": NOW, "updated_at": NOW, "created_by": "system",
    })
    staff_cost.append({
        "id": uid(), "employee_id": eid, "year_month": "2026-04",
        "category_id": CC["HR-SAL-P"], "amount": q1_bonus,
        "source": "payroll", "notes": "2026年Q1绩效奖金",
        "created_at": NOW, "updated_at": NOW, "created_by": "system",
    })

# 办公分摊 — 全员每月均摊
MONTHLY_RENT = 45000   # 办公场地
MONTHLY_UTIL = 8000    # 水电物业
MONTHLY_EQUIP = 12000  # 电脑设备折旧
headcount = len(employees)

for ym in ALL_MONTHS:
    for emp in employees:
        staff_cost.append({
            "id": uid(), "employee_id": emp["id"], "year_month": ym,
            "category_id": CC["HR-OFF-R"], "amount": round(MONTHLY_RENT / headcount, 2),
            "source": "allocation", "notes": "办公场地按人头分摊",
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })
        staff_cost.append({
            "id": uid(), "employee_id": emp["id"], "year_month": ym,
            "category_id": CC["HR-OFF-U"], "amount": round(MONTHLY_UTIL / headcount, 2),
            "source": "allocation", "notes": "水电物业按人头分摊",
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })
        staff_cost.append({
            "id": uid(), "employee_id": emp["id"], "year_month": ym,
            "category_id": CC["HR-OFF-C"], "amount": round(MONTHLY_EQUIP / headcount, 2),
            "source": "allocation", "notes": "电脑设备折旧分摊",
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })

# 培训费 — 部分员工有
training = [("黄志强", "2026-01", 3500, "AGC控制算法培训"),
            ("林小雨", "2026-02", 2800, "物联网设备接入培训"),
            ("郑浩",   "2025-12", 1500, "电力二次系统基础培训"),
            ("张博文", "2026-03", 2000, "数据可视化技术培训")]
for tname, tym, tamt, tnote in training:
    staff_cost.append({
        "id": uid(), "employee_id": EMP_IDS[tname], "year_month": tym,
        "category_id": CC["HR-OTH-L"], "amount": tamt,
        "source": "reimbursement", "notes": tnote,
        "created_at": NOW, "updated_at": NOW, "created_by": "system",
    })

# 团建费 — 全员 2025-12 年会，2026-03 春季团建
for ym, amt, note in [("2025-12", 800, "年度总结会聚餐"), ("2026-03", 600, "春季户外团建")]:
    for emp in employees:
        staff_cost.append({
            "id": uid(), "employee_id": emp["id"], "year_month": ym,
            "category_id": CC["HR-OTH-T"], "amount": amt,
            "source": "reimbursement", "notes": note,
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })

# ══════════════════════════════════════════════════════════════════════
# 10. fact_project_cost — 直接业务费用 (source=direct)
#     timesheet_alloc 由 compute pipeline 生成，这里不生成
# ══════════════════════════════════════════════════════════════════════
project_cost = []

# 变电站项目：设备采购、测试费、差旅
direct_costs = [
    (PROJ_IDS["substation"], "2025-10", CC["BIZ-EQ"],    185000, "监控主机及服务器采购"),
    (PROJ_IDS["substation"], "2025-11", CC["BIZ-EQ"],    72000,  "保护信息子站硬件"),
    (PROJ_IDS["substation"], "2025-12", CC["BIZ-TST"],   15000,  "系统联调测试费"),
    (PROJ_IDS["substation"], "2026-01", CC["BIZ-TST"],   12000,  "通信规约一致性测试"),
    (PROJ_IDS["substation"], "2025-10", CC["BIZ-TRV"],   8500,   "南京现场勘查差旅"),
    (PROJ_IDS["substation"], "2025-11", CC["BIZ-TRV"],   6200,   "现场施工指导差旅"),
    (PROJ_IDS["substation"], "2025-12", CC["BIZ-TRV"],   7800,   "系统联调差旅"),
    (PROJ_IDS["substation"], "2026-02", CC["BIZ-TRV"],   5600,   "整站调试差旅"),
    (PROJ_IDS["substation"], "2026-03", CC["BIZ-TRV"],   4200,   "客户验收协调差旅"),
    (PROJ_IDS["substation"], "2026-01", CC["BIZ-IP-S"],  3000,   "监控软件软著登记"),

    # 配网项目：设备、测试、差旅
    (PROJ_IDS["distribution"], "2025-10", CC["BIZ-EQ"],  96000,  "DTU/FTU终端批量采购"),
    (PROJ_IDS["distribution"], "2025-11", CC["BIZ-EQ"],  48000,  "通信模块采购"),
    (PROJ_IDS["distribution"], "2025-12", CC["BIZ-TST"], 8000,   "终端入网检测费"),
    (PROJ_IDS["distribution"], "2026-01", CC["BIZ-TST"], 8000,   "终端入网检测费（续）"),
    (PROJ_IDS["distribution"], "2025-10", CC["BIZ-TRV"], 12000,  "杭州现场差旅"),
    (PROJ_IDS["distribution"], "2025-11", CC["BIZ-TRV"], 9500,   "萧山站点调试差旅"),
    (PROJ_IDS["distribution"], "2025-12", CC["BIZ-TRV"], 8000,   "现场调试差旅"),
    (PROJ_IDS["distribution"], "2026-01", CC["BIZ-TRV"], 7500,   "第二批终端调试差旅"),
    (PROJ_IDS["distribution"], "2026-02", CC["BIZ-TRV"], 6000,   "收尾调试差旅"),

    # 调度平台：专家费、差旅、软著
    (PROJ_IDS["dispatch"], "2025-11", CC["BIZ-EXP"],   25000,  "调度算法外部专家评审"),
    (PROJ_IDS["dispatch"], "2026-01", CC["BIZ-EXP"],   20000,  "电力市场规则咨询"),
    (PROJ_IDS["dispatch"], "2025-11", CC["BIZ-TRV"],   6000,   "北京客户需求调研差旅"),
    (PROJ_IDS["dispatch"], "2025-12", CC["BIZ-TRV"],   5500,   "华能电厂现场考察"),
    (PROJ_IDS["dispatch"], "2026-02", CC["BIZ-TRV"],   7200,   "系统部署差旅"),
    (PROJ_IDS["dispatch"], "2026-03", CC["BIZ-TRV"],   4800,   "用户培训差旅"),
    (PROJ_IDS["dispatch"], "2026-02", CC["BIZ-IP-S"],  3000,   "调度平台软著登记"),
    (PROJ_IDS["dispatch"], "2026-03", CC["BIZ-IP-P"],  8000,   "智能调度方法发明专利申请"),
    (PROJ_IDS["dispatch"], "2025-12", CC["BIZ-TST"],   18000,  "第三方功能测试"),

    # 能源管理：设备、专家费、差旅
    (PROJ_IDS["energy_mgmt"], "2026-01", CC["BIZ-EQ"],    35000,  "边缘计算网关采购"),
    (PROJ_IDS["energy_mgmt"], "2026-02", CC["BIZ-EQ"],    28000,  "智能电表与传感器采购"),
    (PROJ_IDS["energy_mgmt"], "2026-03", CC["BIZ-EXP"],   15000,  "综合能源专家咨询"),
    (PROJ_IDS["energy_mgmt"], "2026-01", CC["BIZ-TRV"],   8500,   "深圳现场勘查差旅"),
    (PROJ_IDS["energy_mgmt"], "2026-02", CC["BIZ-TRV"],   6800,   "设备安装指导差旅"),
    (PROJ_IDS["energy_mgmt"], "2026-03", CC["BIZ-TRV"],   5200,   "系统联调差旅"),
    (PROJ_IDS["energy_mgmt"], "2026-04", CC["BIZ-TRV"],   4500,   "数据对接差旅"),

    # 大唐售前：前期调研、差旅、招投标
    (PROJ_IDS["presale_datang"], "2026-03", CC["BIZ-PRE-E"], 12000, "智慧电厂技术专家调研"),
    (PROJ_IDS["presale_datang"], "2026-04", CC["BIZ-PRE-M"], 8000,  "AI模型试用采购"),
    (PROJ_IDS["presale_datang"], "2026-03", CC["BIZ-TRV"],   9500,  "大唐总部商务差旅"),
    (PROJ_IDS["presale_datang"], "2026-04", CC["BIZ-TRV"],   7200,  "电厂现场考察差旅"),
    (PROJ_IDS["presale_datang"], "2026-04", CC["BIZ-BID-S"], 5000,  "投标代理服务费"),
    (PROJ_IDS["presale_datang"], "2026-04", CC["BIZ-ENT"],   3800,  "客户商务招待"),
]

for proj_id, ym, cat_id, amount, notes in direct_costs:
    project_cost.append({
        "id": uid(), "project_id": proj_id, "year_month": ym,
        "category_id": cat_id, "amount": amount, "source": "direct",
        "employee_id": "", "notes": notes,
        "created_at": NOW, "updated_at": NOW, "created_by": "system",
    })

# ══════════════════════════════════════════════════════════════════════
# 11. fact_project_revenue
# ══════════════════════════════════════════════════════════════════════
revenue = []

# 变电站: 合同380万, 按里程碑付款
# 里程碑: 设备到货30%, 系统联调30%, 验收30%, 质保10%
rev_items = [
    (PROJ_IDS["substation"], CONT_IDS["c_sub"], CUST_IDS["sgcc_js"], DEPT_IDS["sys_integ"],
     [("2025-10", 380, 114, 114, "milestone", "设备到货款30%已开票已回款"),
      ("2025-12", 380, 114, 0,   "milestone", "系统联调款30%已开票未回款"),
      ("2026-03", 380, 0,   0,   "milestone", "验收款30%待开票"),]),

    # 配网: 合同165万, 分期
    (PROJ_IDS["distribution"], CONT_IDS["c_dist"], CUST_IDS["sgcc_zj"], DEPT_IDS["sys_integ"],
     [("2025-10", 165, 49.5,  49.5,  "installment", "首付款30%"),
      ("2026-01", 165, 49.5,  49.5,  "installment", "进度款30%"),
      ("2026-04", 165, 49.5,  0,     "installment", "完工款30%已开票"),]),

    # 调度平台: 合同520万, 里程碑
    (PROJ_IDS["dispatch"], CONT_IDS["c_disp"], CUST_IDS["huaneng"], DEPT_IDS["soft_dev"],
     [("2025-11", 520, 156,   156,   "milestone", "预付款30%"),
      ("2026-02", 520, 104,   104,   "milestone", "需求确认款20%"),
      ("2026-04", 520, 0,     0,     "milestone", "阶段交付款20%待开票"),]),

    # 能源管理: 合同286万, 分期
    (PROJ_IDS["energy_mgmt"], CONT_IDS["c_emgmt"], CUST_IDS["csg_sz"], DEPT_IDS["soft_dev"],
     [("2026-01", 286, 85.8,  85.8,  "installment", "预付款30%"),
      ("2026-04", 286, 57.2,  0,     "installment", "进度款20%已开票"),]),

    # 驻场 国网江苏: 月结 3.5万*2人=7万/月
    (PROJ_IDS["virt_sgcc_js"], CONT_IDS["c_virt_js"], CUST_IDS["sgcc_js"], DEPT_IDS["sys_integ"],
     [("2025-10", 84, 7,  7,  "monthly", "10月驻场服务费2人"),
      ("2025-11", 84, 7,  7,  "monthly", "11月驻场服务费2人"),
      ("2025-12", 84, 7,  7,  "monthly", "12月驻场服务费2人"),
      ("2026-01", 84, 7,  7,  "monthly", "1月驻场服务费2人"),
      ("2026-02", 84, 7,  7,  "monthly", "2月驻场服务费2人"),
      ("2026-03", 84, 7,  7,  "monthly", "3月驻场服务费2人"),
      ("2026-04", 84, 7,  0,  "monthly", "4月驻场服务费已开票未回款"),]),

    # 驻场 深圳: 月结 3万*1人
    (PROJ_IDS["virt_csg_sz"], CONT_IDS["c_virt_sz"], CUST_IDS["csg_sz"], DEPT_IDS["sys_integ"],
     [("2026-02", 18, 3,  3,  "monthly", "2月运维服务费1人"),
      ("2026-03", 18, 3,  3,  "monthly", "3月运维服务费1人"),
      ("2026-04", 18, 3,  0,  "monthly", "4月运维服务费已开票未回款"),]),
]

for proj_id, cont_id, cust_id, dept_id, items in rev_items:
    for ym, contract_amt, invoiced, received, settle, notes in items:
        revenue.append({
            "id": uid(), "project_id": proj_id, "contract_id": cont_id,
            "customer_id": cust_id, "dept_id": dept_id, "year_month": ym,
            "contract_amount": contract_amt, "invoiced_amount": invoiced,
            "received_amount": received, "settlement_cycle": settle,
            "payment_method": "", "notes": notes,
            "created_at": NOW, "updated_at": NOW, "created_by": "system",
        })

# ══════════════════════════════════════════════════════════════════════
# 12. rule_cost_allocation
# ══════════════════════════════════════════════════════════════════════
rules = [
    {"id": uid(), "category_id": CC["HR-OFF-R"], "allocation_method": "headcount",
     "scope": "company", "description": "办公场地按全公司人头均摊", "is_active": "yes",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": uid(), "category_id": CC["HR-OFF-U"], "allocation_method": "headcount",
     "scope": "company", "description": "水电物业按全公司人头均摊", "is_active": "yes",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": uid(), "category_id": CC["HR-OFF-C"], "allocation_method": "headcount",
     "scope": "company", "description": "电脑设备折旧按人头均摊", "is_active": "yes",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
    {"id": uid(), "category_id": CC["HR-OTH-L"], "allocation_method": "manual",
     "scope": "department", "description": "培训费按实际参训人记录", "is_active": "yes",
     "created_at": TS, "updated_at": TS, "created_by": "system"},
]

# ══════════════════════════════════════════════════════════════════════
# Write all CSVs
# ══════════════════════════════════════════════════════════════════════
print("Generating fox-fpa data...")
write_csv("dim_department", departments)
write_csv("dim_employee", employees)
write_csv("dim_customer", customers)
write_csv("dim_project", projects)
write_csv("dim_contract", contracts)
write_csv("dim_cost_category", cc_rows)
write_csv("dim_time_period", time_periods)
write_csv("fact_timesheet", timesheet)
write_csv("fact_staff_cost", staff_cost)
write_csv("fact_project_cost", project_cost)
write_csv("fact_project_revenue", revenue)
write_csv("rule_cost_allocation", rules)
print("Done!")
