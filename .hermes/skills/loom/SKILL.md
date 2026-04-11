---
name: loom
description: 本地 CSV 数据管理 + A2A 跨 Agent 数据查询工作流
version: 2.0.0
author: loom
license: MIT
metadata:
  hermes:
    tags: [CRM, Data, CSV, Git, A2A]
prerequisites:
  env_vars: [LOOM_ROOT]
  tools: [loom_catalog, loom_query, loom_add, loom_update, loom_delete, loom_commit, loom_sync, loom_stats, loom_discover, loom_call]
---

# Loom — 数据管理 & A2A 跨 Agent 查询

Loom 将 CSV 文件作为数据库，用 Git 做版本控制和多人同步。支持本地数据操作，以及通过 A2A 协议查询远程 Agent 的数据。

## 核心原则

1. **先读后写** — 任何操作前先调 `loom_catalog` 了解表结构
2. **不自动提交** — 数据变更后不要主动 commit，让用户决定何时提交
3. **退出前提醒** — 如果本次会话有未提交的变更，结束前提醒用户是否需要 `loom_commit`
4. **同步优先** — 多人协作时先 `loom_sync`，再修改数据

## 本地数据工作流

### 查询数据

```
1. loom_catalog          → 了解有哪些表、每列的含义
2. loom_query            → 查询数据，支持：
   - filters: 精确匹配  {"status": "active"}
   - search:  模糊搜索  "腾讯"（在所有字段中搜索）
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

## A2A 跨 Agent 查询工作流

当需要查询**其他人的数据**（例如上级查询下属数据），使用 A2A 工具：

```
1. loom_catalog          → 读本地 catalog，找 agent_url 字段（通常在 team_members 等表中）
   或由用户直接提供远程地址，如 http://192.168.1.10:8100

2. loom_discover(agent_url)
   → 获取远程 Agent Card，了解对方数据结构和能力

3. loom_call(agent_url, query)
   → 用自然语言向远程 Agent 发起查询
   例："查询所有 stage 不是 closed 的商机，按金额降序"
```

### 典型场景：上级查询下属数据

```
# 1. 从本地 team_members 表找到下属的 agent_url
loom_catalog()
loom_query(table="team_members", filters={"name": "alice"})
→ 得到 agent_url: "http://localhost:8100"

# 2. 了解对方能提供什么数据
loom_discover("http://localhost:8100")

# 3. 查询对方数据
loom_call("http://localhost:8100", "查询所有进行中的商机，显示名称、金额和阶段")
loom_call("http://localhost:8100", "统计各阶段商机数量和总金额")
loom_call("http://localhost:8100", "查询本周有哪些高优先级任务未完成")
```

### 注意事项

- `loom_discover` 返回对方的 Agent Card（JSON），包含 name、description、skills
- `loom_call` 的 query 用自然语言描述，由对方 Agent 负责解析和执行
- 对方 Agent 的访问控制由其自身的 skill/system prompt 决定，无需本地配置
- 远程查询结果为只读，不能通过 `loom_call` 修改对方数据

## 常见查询示例

```
# 本地模糊搜索
loom_query(table="contacts", search="腾讯")

# 最近 5 条记录
loom_query(table="interactions", sort_by="-created_at", limit=5)

# 按字段分组统计
loom_stats(table="contacts", group_by="status", agg={"id": "count"})
loom_stats(table="deals", group_by="stage", agg={"amount": "sum", "id": "count"})

# 跨 Agent 查询
loom_call("http://alice.local:8100", "本季度已成交的商机列表")
loom_call("http://alice.local:8100", "按客户公司统计商机总金额")
```
