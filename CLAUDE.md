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
├── docs/constraints/        ← 项目级约束（复权选型/语雀目录）
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

新增或修改脚本时必须同步更新对应目录的 `00-脚本使用说明.md`。

### 文档命名

文档命名: `010-简要描述.md`（任务文档）或 `{日期}-简要描述.md`（报告）。

标题下方第一行写上建立时间：
```
# 文档标题
**创建时间**: 20260318 17:05
```

## 数据库

双库设计，配置在 `lib/config.py`（或各主题 `scripts/config.py`）：

| 库 | 用途 | 引擎变量 |
|----|------|---------|
| `my_stock` | 读生产数据（K线、基本面等） | `read_engine` |
| `stock_research` | 写研究结果（信号、统计表） | `write_engine` |

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
