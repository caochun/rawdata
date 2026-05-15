---
name: loom
description: 本地 CSV 数据管理 + A2A 跨 Agent 数据查询工作流
version: 2.1.0
author: loom
license: MIT
metadata:
  hermes:
    tags: [CRM, Data, CSV, Git, A2A]
prerequisites:
  env_vars: [LOOM_ROOT]
  tools: [lm data query, lm data add, lm data update, lm data delete, lm data commit, lm data status, lm data stats, lm data join, lm data tree, lm data validate, lm sync, lm view list, lm view query, lm compute list, lm compute run]
---

# Loom — 数据管理 & A2A 跨 Agent 查询

Loom 将 CSV 文件作为数据库，用 Git 做版本控制和多人同步。支持本地数据操作，以及通过 A2A 协议查询远程 Agent 的数据。

## CLI 入口：`lm`

实际的 CLI 命令是 `lm`（不是 `loom`）。所有操作通过 `lm` 的子命令完成。

```
lm data query <table>      — 查询数据
lm data add <table>        — 新增行
lm data update <table>     — 修改行
lm data delete <table>     — 删除行
lm data status             — 查看未提交变更
lm data commit             — 提交变更
lm data stats <table>      — 聚合统计
lm data join <left> <right> — 跨表连接查询
```

**数据文件路径：** 表格文件存放在子目录中（如 `fox-fpa/dim_employee.csv`），但 `lm data query` 自动扫描所有子目录，直接按表名查询即可，无需指定子目录路径。

**无需 catalog.yaml：** `lm data query` 直接读取 CSV 文件头作为列定义，不需要 catalog.yaml 存在。可用 `head -1 <path>` 查看列名。

## 核心原则

1. **先读后写** — 任何操作前先用 `lm data query --json --limit 3` 了解数据格式和字段值
2. **不自动提交** — 数据变更后不要主动 commit，让用户决定何时提交
3. **退出前提醒** — 如果本次会话有未提交的变更，结束前提醒用户是否需要运行 `lm data commit`
4. **同步优先** — 多人协作时先 `lm sync`，再修改数据
5. **新增数据：用 `--data` 传 JSON 字符串** — `lm data add <table> --data '{"field": "value"}'`，字段名匹配 CSV 列名。无专门的表单工具，由 agent 根据 CSV 表头向用户询问字段值

## 本地数据工作流

### 探索表结构

```
1. head -1 <repo>/*/<table>.csv     → 查看列名（最快方式）
   或
   lm data query <table> --limit 1 --json  → 查看列名和示例数据
```

CSV 文件位于子目录中（如 `fox-fpa/dim_employee.csv`），`lm data query` 自动发现。数据可能分布在多个子目录下。

### 查询数据

```
lm data query <table> [--filter field=value] [--search "keyword"] [--sort -field] [--limit N] [--offset N] [--fields col1,col2] [--json]
```

- `--filter` / `-f`：精确匹配，可重复（如 `-f status=active -f dept_id=xxx`）
- `--search` / `-s`：模糊搜索（所有字段）
- `--sort`：排序，`-field` 前缀为降序
- `--fields`：逗号分隔的返回列
- `--json`：JSON 输出（推荐 agent 使用）

### 聚合统计

```
lm data stats <table> --agg "field=count,amount=sum" [--group-by dept_id] [--json]
```

支持：count, sum, avg, min, max

### 新增数据

1. 查看 CSV 表头确认字段列表
2. 询问用户各字段值
3. 执行 `lm data add <table> --data '{"field1": "value1", "field2": "value2"}'`
4. 告知用户变更已暂存，可继续操作或运行 `lm data commit` 提交

### 修改 / 删除

```
lm data update <table> --id <row_id> --data '{"field": "new_value"}'
lm data delete <table> --id <row_id>
```

**修改/删除前：** 先 `lm data query` 查找目标行并获取 id。

### 提交变更

```
lm data commit [-m "commit message"]
```

**注意：不要在每次操作后自动提交。** 用户可能需要连续做多次变更后一次性提交。仅在用户明确要求保存/提交时调用。

### 会话结束提醒

如果本次会话中执行过 add/update/delete 操作且尚未 commit，在结束对话前提醒：

> 本次会话有未提交的数据变更，是否需要运行 `lm data commit` 保存？

### 多人协作同步

```
1. lm sync               → 拉取远程变更并合并
2. （如有冲突）使用 lm 的冲突解决机制
3. lm sync               → 推送合并结果
```

## 跨表连接查询

```
lm data join <left_table> <right_table> [--on left_col=right_col] [--type inner|left] [--filter table.field=value] [--fields "left.col1,right.col2"] [--limit N] [--json]
```

## 预定义分析视图

```
lm view list                     → 列出可用视图
lm view query <view_name>        → 执行视图
```

## 计算管线（成本分摊）

```
lm compute list                   → 列出可用管线
lm compute run <pipeline> --period 2026-05  → 执行管线
```

## A2A 跨 Agent 查询工作流

当需要查询**其他人的数据**（例如上级查询下属数据），使用终端 HTTP 工具：

```
1. 从本地 team_members 表或用户提供地址获取远程 agent_url
2. curl <agent_url>/discover → 了解对方数据结构和能力
3. curl -X POST <agent_url>/call -d '{"query": "..."}'  → 发起查询
```

### 注意事项

- 对方 Agent 的访问控制由其自身的 skill/system prompt 决定，无需本地配置
- 远程查询结果为只读，不能修改对方数据

## 常见查询示例（正确的 CLI 写法）

```
# 查看表结构
head -1 fox-fpa/dim_employee.csv

# 本地精确匹配查询
lm data query contacts --filter status=active --json

# 模糊搜索
lm data query contacts --search "腾讯" --json

# 最近 5 条记录
lm data query interactions --sort -created_at --limit 5 --json

# 按字段分组统计
lm data stats contacts --group-by status --agg "id=count" --json
lm data stats deals --group-by stage --agg "amount=sum,id=count" --json

# 跨表连接
lm data join deals contacts --on contact_id=id --json

# 查看所有表可用
find . -name "*.csv" | grep -v ".hermes"
```
