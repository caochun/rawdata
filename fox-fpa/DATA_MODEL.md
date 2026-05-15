# Fox-FPA 数据模型说明

Fox-FPA（Financial Planning & Analysis）是一套面向工程服务/系统集成类企业的 **经营分析数据模型**，完全独立建模，核心目标：算清楚每个人、每个项目到底赚不赚钱。

采用 **维度建模**（事实表 + 维度表），自建全套维度主档和事实明细，形成独立的管理会计体系。

**分析主线**：`人员成本 →（按工时归集）→ 项目成本 →（对比合同产出）→ 项目利润 →（按部门/客户/周期切分）→ 经营分析`

---

## 一、维度表

### dim_employee — 人员

分析视角下的人员档案，关注与成本和产出相关的属性。

关键字段：`employee_no`（工号）、`name`（姓名）、`dept_id`（FK → dim_department）、`position`（岗位）、`rank`（职级）、`hire_date`（入职日期）、`leave_date`（离职日期）、`status`（状态：active / probation / left）、`employment_type`（用工类型：full_time / part_time / intern / rehire）。

### dim_department — 部门

公司内部组织架构，`parent_id` 自引用支持多级层级。

关键字段：`code`（部门编码）、`name`（部门名称）、`parent_id`（上级部门）、`level`（层级深度）、`leader_id`（FK → dim_employee）、`dept_type`（部门类型：business / support / management）。

### dim_customer — 客户

分析视角下的客户档案，支持客户层级到最末级。

关键字段：`name`（客户名称）、`uscc`（统一社会信用代码）、`industry`（行业）、`tier`（客户等级：strategic / key / normal / potential）、`parent_customer_id`（上级客户，支持集团层级）、`region`（区域）。

### dim_project — 项目

分析视角下的项目档案，统一管理实际项目和虚拟项目。

关键字段：`project_no`（项目编号）、`name`（项目名称）、`project_type`（项目类型：actual / virtual）、`dept_id`（FK → dim_department，归属部门）、`customer_id`（FK → dim_customer）、`pm_id`（FK → dim_employee，项目经理）、`start_date`（开始日期）、`end_date`（结束日期）、`status`（状态：active / completed / terminated / suspended）、`participation_mode`（参与方式，虚拟项目用：outsource / secondment / on_site / remote）、`settlement_cycle`（结算周期，虚拟项目用：monthly / quarterly / milestone）。

**设计说明**：虚拟项目和实际项目统一在此表，`project_type` 区分。虚拟项目多几个专属字段（`participation_mode`、`settlement_cycle`），实际项目这些字段留空。

### dim_contract — 合同

分析视角下的合同档案，关联项目和客户，用于产出分析。

关键字段：`contract_no`（合同编号）、`name`（合同名称）、`project_id`（FK → dim_project）、`customer_id`（FK → dim_customer）、`amount`（合同金额）、`tax_rate`（税率）、`sign_date`（签署日期）、`start_date`（开始日期）、`end_date`（结束日期）、`status`（状态：draft / signed / executing / completed / terminated）、`payment_method`（付款方式）。

### dim_cost_category — 费用科目

树形费用科目维度，统一分类所有成本和费用。`parent_id` 自引用支持多级层级。

关键字段：`code`（科目编码）、`name`（科目名称）、`level`（层级深度）、`parent_id`（上级科目）、`cost_domain`（归属域：staff / project / shared）、`is_leaf`（是否末级科目）、`sort_order`（排序）。

**预置科目树**：

```
人力成本
├── 招聘成本
├── 薪资福利
│   ├── 基本工资
│   ├── 绩效奖金
│   ├── 社保公积金
│   └── 其他福利
├── 奖惩
├── 办公分摊
│   ├── 电脑设备
│   ├── 办公场地
│   └── 水电物业
├── 其他人员费用
│   ├── 招待费
│   ├── 团建费
│   └── 培训费
└── 离职成本
    └── 经济补偿金

业务费用
├── 招待费
├── 专家费
├── 测试费
├── 报告费
├── 知识产权
│   ├── 专利费用
│   ├── 论文费用
│   └── 软著费用
├── 前期调研
│   ├── 专家支撑
│   └── 模型采购
├── 设备采购
├── 招投标费用
│   ├── 中标服务费
│   └── 客户维护费
└── 法务支持
```

### dim_time_period — 时间维度

预生成的时间维度表，支持按年/季/月/周多粒度汇总。

关键字段：`date`（日期）、`year`（年份）、`quarter`（季度）、`month`（月份）、`week`（ISO周号）、`year_month`（年月，如 2026-05）、`year_quarter`（年季，如 2026-Q2）、`is_workday`（是否工作日）。

---

## 二、事实表

### fact_timesheet — 工时记录

**整个模型的基石**。记录每个人在每个项目上投入的工时，是人力成本归集到项目的唯一依据。

关键字段：`employee_id`（FK → dim_employee）、`project_id`（FK → dim_project）、`work_date`（工作日期）、`hours`（工时数）、`work_type`（工作类型：normal / overtime / on_site）、`description`（工作内容摘要）、`recorded_by`（记录人）。

**设计说明**：
- 粒度：人 × 项目 × 日
- 一个人一天可以有多条记录（多个项目），总工时应 ≤ 当日可用工时
- 虚拟项目也记录工时，用于分摊计算

### fact_staff_cost — 人员月度成本

记录每个人每月在各费用科目上的实际成本。

关键字段：`employee_id`（FK → dim_employee）、`year_month`（年月）、`category_id`（FK → dim_cost_category）、`amount`（金额）、`source`（数据来源：payroll / reimbursement / allocation / manual）、`notes`（备注）。

**设计说明**：
- 粒度：人 × 月 × 费用科目
- 办公分摊类成本按人头均摊，系统自动生成
- 招聘成本、离职补偿为一次性记录，挂在对应月份

### fact_project_cost — 项目成本

记录每个项目每月在各费用科目上的成本。人力成本由系统按工时比例从 `fact_staff_cost` 归集而来，业务费用直接记录。

关键字段：`project_id`（FK → dim_project）、`year_month`（年月）、`category_id`（FK → dim_cost_category）、`amount`（金额）、`source`（来源：timesheet_alloc / direct / manual）、`employee_id`（FK → dim_employee，可选，人力成本时关联具体人员）、`notes`（备注）。

**设计说明**：
- 粒度：项目 × 月 × 费用科目
- `source = timesheet_alloc` 的记录由系统根据工时比例自动生成，不手工录入
- `source = direct` 的记录为直接发生的业务费用
- **不存聚合字段**，项目总成本由明细汇总

### fact_project_revenue — 项目产出与结算

记录每个项目的收入侧数据，含合同金额、开票进度、回款进度。

关键字段：`project_id`（FK → dim_project）、`contract_id`（FK → dim_contract，可选）、`customer_id`（FK → dim_customer）、`dept_id`（FK → dim_department，内部归属部门）、`year_month`（结算期间）、`contract_amount`（合同金额）、`invoiced_amount`（已开票金额）、`received_amount`（已回款金额）、`settlement_cycle`（结算周期）、`payment_method`（付款方式）。

**设计说明**：
- 实际项目关联 `dim_contract` 获取合同金额
- 虚拟项目的结算金额和周期直接录入
- 支持客户到最末级，用于按客户维度分析利润

---

## 三、分摊规则

### rule_cost_allocation — 成本分摊规则

定义间接成本（办公分摊等）的分摊方式。

关键字段：`category_id`（FK → dim_cost_category，待分摊的科目）、`allocation_method`（分摊方式：headcount / timesheet_ratio / equal / manual）、`scope`（分摊范围：company / department）、`description`（规则说明）、`is_active`（是否启用）。

**预置规则**：
| 费用科目 | 分摊方式 | 说明 |
|---|---|---|
| 办公场地 | headcount | 按部门人头均摊 |
| 水电物业 | headcount | 按部门人头均摊 |
| 电脑设备 | manual | 按实际领用人记录 |
| 培训费 | manual | 按实际参训人记录 |

---

## 四、关键设计原则

**1. 完全独立**
Fox-FPA 自建全套维度主档，不依赖外部系统。数据可从业务系统导入，但模型本身是自包含的。

**2. 工时是基石**
没有工时数据，人力成本无法归集到项目，整个分析框架无法运转。工时记录的完整性和准确性是第一优先级。

**3. 不存聚合字段**
所有汇总数据（项目总成本、人员年度总成本、项目利润等）均从明细表实时计算。

**4. 分摊可追溯**
每条成本记录标注 `source` 来源，区分系统自动分摊和人工录入，确保分摊逻辑可审计。

**5. 虚拟项目统一管理**
虚拟项目和实际项目在 `dim_project` 中统一管理，用 `project_type` 区分，分析时可合并或分开，避免两套口径。

**6. 统一用 UUID 主键**
所有表使用 UUID 作为主键，跨表引用均使用 UUID 外键。

**7. 审计字段规范**
所有表包含 `created_at`（创建时间）、`updated_at`（更新时间）、`created_by`（创建人）。

---

## 五、模型总览

| 分类 | 表名 | 说明 |
|---|---|---|
| 维度表 | `dim_employee` | 人员 |
| 维度表 | `dim_department` | 部门 |
| 维度表 | `dim_customer` | 客户 |
| 维度表 | `dim_project` | 项目（含虚拟项目） |
| 维度表 | `dim_contract` | 合同 |
| 维度表 | `dim_cost_category` | 费用科目（树形） |
| 维度表 | `dim_time_period` | 时间维度 |
| 事实表 | `fact_timesheet` | 工时记录 |
| 事实表 | `fact_staff_cost` | 人员月度成本 |
| 事实表 | `fact_project_cost` | 项目成本 |
| 事实表 | `fact_project_revenue` | 项目产出与结算 |
| 规则表 | `rule_cost_allocation` | 成本分摊规则 |

共 **7 张维度表 + 4 张事实表 + 1 张规则表 = 12 张表**。
