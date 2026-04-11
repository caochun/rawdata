# Loom

Spreadsheet-as-database with Git sync and AI agent integration.

Loom 将 CSV 文件作为结构化数据库，用 Git 进行版本控制和多人同步，同时提供 CLI 和 AI Agent 两种操作接口。

## 核心特性

- **CSV 即数据库** — 每张表一个 CSV 文件，schema.yaml 定义结构和校验规则
- **Git 同步** — 自动 commit/pull/push，CSV 行级三路合并冲突检测
- **AI Agent 集成** — 作为 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 插件，提供 10 个工具供 Agent 操作数据
- **查询与统计** — 模糊搜索、排序、分页、聚合统计（基于 pandas）

## 快速开始

### 安装

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

### 初始化数据仓库

```bash
lm init ./my-data -n "我的数据"
cd my-data
```

### CLI 操作

```bash
# 查询
lm data query contacts
lm data query contacts --search 张三 --sort -created_at --limit 5 --json

# 增删改
lm data add contacts -d '{"name": "张三", "status": "active"}'
lm data update contacts --id <id> -d '{"status": "inactive"}'
lm data delete contacts --id <id>

# 统计
lm data stats contacts --group-by status --agg id=count
lm data stats deals --group-by stage --agg value=sum,id=count --json

# 提交与同步
lm data commit -m "add: 新增联系人张三"
lm sync run
```

### AI Agent 模式

```bash
# 安装 Hermes Agent
.venv/bin/pip install -e vendor/hermes-agent

# 启动（需配置 .hermes/config.yaml 中的 LLM provider）
LOOM_ROOT=./my-data ./hermes.sh
```

Agent 可用工具：`loom_catalog` `loom_query` `loom_add` `loom_update` `loom_delete` `loom_commit` `loom_sync` `loom_conflicts` `loom_resolve` `loom_stats`

## 数据仓库结构

```
my-data/
├── schema.yaml       # 表结构定义（列、类型、校验、合并策略）
├── catalog.yaml      # 自然语言描述（供 Agent 理解上下文）
├── contacts.csv      # 数据表
├── companies.csv
└── ...
```

### schema.yaml 示例

```yaml
tables:
  contacts:
    columns:
      id:
        type: uuid
        primary: true
        auto: true
      name:
        type: string
        required: true
      status:
        type: enum
        values: [active, inactive, pending]
      created_at:
        type: datetime
        auto: true
    merge_strategy:
      status: last_write_wins
```

支持的列类型：`string`（可带 pattern）、`uuid`、`datetime`、`enum`（带 values）

## 项目结构

```
├── loom/                      # Python 包
│   ├── cli.py                 # CLI 入口（lm 命令）
│   ├── commands/              # 子命令（data, init, sync）
│   └── core/                  # 核心逻辑
│       ├── schema.py          # schema 加载与校验
│       ├── store.py           # CSV 读写、查询、聚合
│       └── git_ops.py         # Git 操作与冲突检测
├── .hermes/
│   ├── plugins/loom/          # Hermes Agent 插件
│   └── skills/loom/           # Agent 工作流指南
├── hermes.sh                  # Agent 启动脚本
└── vendor/hermes-agent/       # Hermes Agent（git submodule）
```

## License

MIT
