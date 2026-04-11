---
name: loom
description: CRM 数据管理工作流 — 使用 loom 工具读写客户、公司、商机、沟通记录和任务数据。
version: 1.0.0
author: loom
license: MIT
metadata:
  hermes:
    tags: [CRM, Data, CSV, Git]
prerequisites:
  env_vars: [LOOM_ROOT]
  tools: [loom_catalog, loom_query, loom_add, loom_update, loom_delete, loom_commit, loom_sync, loom_stats]
---

# Loom — CRM 数据管理

Loom 将 CSV 文件作为数据库，用 Git 做版本控制和多人同步。你拥有一组 `loom_*` 工具来操作数据。

## 核心原则

1. **先读后写** — 任何操作前先调 `loom_catalog` 了解表结构
2. **不自动提交** — 数据变更后不要主动 commit，让用户决定何时提交
3. **退出前提醒** — 如果本次会话有未提交的变更，结束前提醒用户是否需要 `loom_commit`
4. **同步优先** — 多人协作时先 `loom_sync`，再修改数据

## 标准工作流

### 查询数据

```
1. loom_catalog          → 了解有哪些表、每列的含义
2. loom_query            → 查询数据，支持：
   - filters: 精确匹配  {"status": "active"}
   - search:  模糊搜索  "腾讯" （在所有字段中搜索）
   - sort_by: 排序      "-created_at"（- 前缀为降序）
   - limit/offset: 分页  limit=10, offset=20
   - fields:  字段选择  ["name", "email"]
3. loom_stats            → 聚合统计：
   - agg: {"id": "count", "value": "sum"}  支持 count/sum/avg/min/max
   - group_by: 分组字段（可选）
```

### 新增 / 修改 / 删除

```
1. loom_catalog          → 确认目标表的列定义和校验规则
2. loom_query            → 查看现有数据（修改/删除时先定位 id）
3. loom_add / loom_update / loom_delete  → 执行操作
4. 告知用户变更已暂存，可继续操作或运行 loom_commit 提交
```

**注意：不要在每次操作后自动调用 loom_commit。** 用户可能需要连续做多次变更后一次性提交。仅在用户明确要求保存/提交时才调用 `loom_commit`。

### 会话结束提醒

如果本次会话中执行过 add/update/delete 操作且尚未 commit，在结束对话前提醒：

> 本次会话有未提交的数据变更，是否需要运行 `loom_commit` 保存？

### 多人协作同步

```
1. loom_sync             → 拉取远程变更并合并
2. （如有冲突）loom_conflicts → 查看冲突列表
3. loom_resolve          → 逐个解决冲突
4. loom_sync             → 推送合并结果
```

## 表结构速查

| 表 | 用途 | 关键字段 |
|---|---|---|
| **contacts** | 客户联系人 | name(必填), email, phone, company, status(active/inactive/pending) |
| **companies** | 公司组织 | name(必填), industry, size(startup/sme/enterprise) |
| **deals** | 销售商机 | name(必填), stage(lead→qualified→proposal→negotiation→won/lost), value |
| **interactions** | 沟通记录(只增不改) | contact_id(必填), type(call/email/meeting/wechat/note), note(必填) |
| **tasks** | 跟进任务 | title(必填), status(todo/in_progress/done/cancelled), priority(low/medium/high) |

## 关系

- `contacts.company_id → companies.id`
- `deals.contact_id → contacts.id` / `deals.company_id → companies.id`
- `interactions.contact_id → contacts.id` / `interactions.deal_id → deals.id`
- `tasks.contact_id → contacts.id` / `tasks.deal_id → deals.id`

## 重要规则

- **interactions 只增不改** — 沟通记录是历史事实，不要 update 或 delete，只能 add
- **id 和 created_at 自动生成** — 新增数据时不需要填这两个字段
- **updated_at 自动更新** — update 操作会自动刷新此字段（contacts/companies/deals/tasks）
- **commit message 要有意义** — 不要用默认 message，描述清楚做了什么变更
- **status 枚举值固定** — 必须使用 schema 中定义的值，否则校验失败
- **deals.stage 是销售漏斗** — 正常流转：lead → qualified → proposal → negotiation → won/lost

## 常见场景示例

### 录入新客户 + 公司

```
loom_add(table="companies", data={"name": "XX科技", "industry": "互联网", "size": "sme"})
→ 拿到 company_id

loom_add(table="contacts", data={"name": "张总", "email": "zhang@xx.com", "phone": "13800000000", "title": "CEO", "company": "XX科技", "company_id": "<id>", "source": "referral", "status": "active"})

→ 告知用户：已添加 XX科技 和联系人张总，变更尚未提交。
```

### 创建商机并跟进

```
loom_add(table="deals", data={"name": "XX科技年度采购", "contact_id": "<id>", "company_id": "<id>", "stage": "lead", "value": "500000", "currency": "CNY"})

loom_add(table="interactions", data={"contact_id": "<id>", "deal_id": "<id>", "type": "meeting", "note": "首次拜访，介绍方案"})

loom_add(table="tasks", data={"title": "发送报价单给张总", "contact_id": "<id>", "deal_id": "<id>", "assignee": "小王", "due_date": "2026-04-15", "priority": "high", "status": "todo"})

→ 告知用户：已创建商机、沟通记录和任务，变更尚未提交。
```

### 推进商机阶段

```
loom_update(table="deals", id="<id>", data={"stage": "proposal"})
loom_add(table="interactions", data={"contact_id": "<id>", "deal_id": "<id>", "type": "email", "note": "已发送正式方案书"})

→ 告知用户：商机已推进至 proposal，变更尚未提交。
```

### 用户要求提交时

```
loom_commit(message="add: XX科技 + 联系人张总 + 采购商机")
→ message 应概括本次所有变更
```

### 查询和统计

```
# 模糊搜索
loom_query(table="contacts", search="腾讯")

# 最近 5 条沟通记录
loom_query(table="interactions", sort_by="-created_at", limit=5)

# 按状态统计联系人数量
loom_stats(table="contacts", group_by="status", agg={"id": "count"})

# 按商机阶段统计金额和数量
loom_stats(table="deals", group_by="stage", agg={"value": "sum", "id": "count"})

# 全表汇总：商机总金额
loom_stats(table="deals", agg={"value": "sum"})

# 按负责人统计待办任务数
loom_stats(table="tasks", group_by="assignee", agg={"id": "count"})
```
