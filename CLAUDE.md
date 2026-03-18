# My Stock Research - 股票量化研究平台

## 项目概况

| 项 | 说明 |
|---|---|
| 定位 | 重型指标分析、信号验证、策略研究（脚本 + 报告） |
| 技术栈 | Python 3.10+ / pandas / SQLAlchemy / MySQL |
| 数据来源 | 读 `my_stock` 生产库，写 `stock_research` 研究库 |
| 关联项目 | `my-stock`（数据平台，负责 Tushare 同步和 Web 服务） |

## 代码规范

1. **最小实用**，只做需要的功能，不过度设计
2. **简化逻辑**，复杂的脚本拆分成简单的模块
3. **注释详细**，所有函数、关键逻辑、SQL 查询必须写清楚中文注释
4. **日志完善**，日志完整详实（开始/阶段完成/结束/耗时/数据量）

## Git 工作流

**禁止直接在 master 上提交。** 线程ID默认 `claude01`。

```
1. git pull origin master
2. git checkout -b task-{线程ID}-{内容简写}
3. 开发 commit
4. git fetch origin && git rebase origin/master
5. git checkout master && git merge task-xxx && git push origin master
6. 删除 task 分支
```

### 提交格式

格式: `[TAG] - {线程ID} - {描述}`，**一个 commit 只做一件事**。

| TAG | 含义 |
|-----|------|
| `[ADD]` | 新增功能/文件 |
| `[FIX]` | BUG修复 |
| `[MOD]` | 功能调整（调参、重构、优化） |
| `[DEL]` | 删除文件/功能 |

## 目录结构

```
my-stock-research/
├── base/                    ← 跨主题研究基础 + 方法论
│   └── constraints/         ← 强制约束（牛熊分析标准等）
├── {topic}/                 ← 按主题分目录（macd/rsi/turnover/ma/...）
│   ├── base/                ← 该主题的基础（信号定义、Schema、数据源）
│   ├── report/              ← 研究报告，命名 {日期}-简要描述.md
│   ├── scripts/             ← 脚本代码，维护 00-脚本使用说明.md
│   └── data/                ← 研究数据（CSV等）
├── la/scripts/              ← LA 选股分析脚本
├── lib/                     ← 公共基础设施（config/database/logger）
├── scripts/                 ← 工具脚本（Tushare 文档同步/数据检查）
├── tushare_docs/            ← Tushare 接口文档（离线查阅）
├── docs/                    ← 项目文档
│   ├── constraints/         ← 项目级约束（复权选型/语雀目录）
│   └── database.md          ← 数据库设计文档（双库表结构）
├── task/                    ← 研究任务（完成后归档删除）
├── CLAUDE.md
├── PROGRESS.md
└── README.md
```

## 研究规范

### 启动前必读

1. `base/constraints/` — 强制约束（牛熊分析标准等）
2. `base/` — 研究方法论、分层框架、跨主题通用经验
3. `{topic}/base/` — 该主题的基础（信号定义、Schema、数据源等）

### 脚本管理

脚本分三层存放：

| 目录 | 用途 | 说明 |
|------|------|------|
| `scripts/` | 工具脚本 | Tushare 文档爬取、数据检查，与研究主题无关 |
| `{topic}/scripts/` | 主题研究脚本 | 每个主题独立（有自己的 config/database/models） |
| `la/scripts/` | LA 选股分析脚本 | 分析选股结果、评估模型表现 |

**规则：**
- 新增或修改脚本时**必须同步更新**对应目录的 `00-脚本使用说明.md`
- 各主题 `scripts/` 有独立的 `config.py` / `database.py`，**不要跨主题引用**
- 需要新数据时，先查 `docs/database.md` 看 my_stock 库是否已有，没有则查 `tushare_docs/` 自行接入

### 文档命名

文档命名: `010-简要描述.md`（任务文档）或 `{日期}-简要描述.md`（报告）。

标题下方第一行写上建立时间：
```
# 文档标题
**创建时间**: 20260318 17:05
```

## 数据库

### 读取库（只读，由其他项目维护）

| 库 | 说明 | 数据表目录 |
|----|------|-----------|
| `my_stock` | A股全量数据：行情K线、基本面、财务、资金流向、指数、宏观经济、LA选股 | `docs/010-my_stock数据表.md` |
| `my_trend` | 舆情数据：东财热度排名、股吧情感、新闻LLM分析 | `docs/020-my_trend数据表.md` |

需要数据时先查上述文档。如需 Tushare 未同步的接口，查 `tushare_docs/interface_catalog.csv` 定位后自行调用。

### 写入库（每主题独立，自行创建）

每个研究主题创建独立数据库，命名 `stock_{topic}`：

| 已有 | 库名 |
|------|------|
| MACD | `stock_research` |
| RSI | `stock_rsi` |
| 换手率 | `stock_turnover` |
| 均线 | `stock_ma` |

**新主题接入规则：**
1. `scripts/config.py` 设置 `WRITE_DB_NAME = "stock_{topic}"`
2. `scripts/database.py` 创建引擎 + 自动建库
3. `scripts/models.py` 定义表结构

各主题连接配置在自己的 `scripts/config.py`，**不要跨主题共享写入库**。

## 项目约束

以下约束必须遵守：

- **复权选型** — 个股指标计算禁止 bfq，详见 `docs/constraints/020-复权选型规则.md`
- **牛熊分析标准** — 详见 `base/constraints/牛熊分析标准.md`
- **语雀上传** — 必须按目录结构上传，详见 `docs/constraints/040-语雀目录约束.md`

## 注意事项

1. **任务前先读 task/**：开始研究任务前，先阅读 `task/` 下对应文件
2. **研究前先读 base/**：开始任何主题研究前，先读 `base/` 和 `{topic}/base/`
3. **命令失败**：分析原因后调整方案，不要重复执行相同的失败命令
4. **网络代理**：外网操作超时时设置 `http_proxy=http://localhost:7890`
