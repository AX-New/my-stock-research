# My Stock Research

股票量化研究平台 — 重型指标分析、信号验证、策略研究。

## 定位

- **脚本 + 报告**：跑分析脚本，产出研究报告和统计数据
- **与 my-stock 共享 MySQL**：读 `my_stock` / `my_trend` 生产库，写各主题独立研究库
- **无 Web 服务**：纯本地脚本项目
- **回测引擎**：研究脚本产出 orders.csv → my-stock 的 `OrderBasedEngine` CLI 跑回测 → `/report` 页面查看

## 研究主题

| 主题 | 目录 | 报告数 | 状态 |
|------|------|--------|------|
| MACD/DIF | `macd/` | 12 | 已完成：牛熊周期、指数/行业/个股信号、DIF极值有效性、ATR标准化 |
| RSI | `rsi/` | 6 | 已完成：四层级分析（指数→行业→个股） |
| 换手率 | `turnover/` | 7 | 已完成：四层级分析 + 方法论综合 |
| 均线 | `ma/` | 5 | 已完成：四层级分析 + 方法论 |
| 波动率 | `volatility/` | 2 | 已完成：振幅预测、波动率对比 |
| 热度排名 | `hot/` | 3 | 已完成：假说验证、多因子、涨速分析 |
| 资金流向 | `moneyflow/` | 0 | 脚本完成，**报告待产出** |
| 峰值择时 | `peak_timing/` | 2 | 已完成：信号衰减分析 |
| 共振 | `resonance/` | 0 | 脚本完成，指数级结论已出 |
| 通用 | `general/` | 7 | 综合报告：四/六指标选股、知识图谱、牛熊评估 |
| LA 选股 | `la/scripts/` | - | 选股结果分析脚本（16个） |
| PyTorch | `pytorch/` | 1 | Demo：LSTM预测、市场状态分类 |
| Qlib | `qlib/` | 1 | Demo：LightGBM预测 |

## 进行中的任务

| 任务 | 目录 | 状态 |
|------|------|------|
| 热度轮转策略回测 | `task/162-*` | 活跃：最优策略年化74.61%，待多仓位+基本面过滤 |
| 选股扫描与共振检测 | `task/170-*` | TODO：四指标信号表已就绪，待构建扫描逻辑 |
| 资金流向研究补完 | `task/171-*` | TODO：脚本已有，报告待产出 |
| 盘中热度信号回测 | `task/172-*` | BLOCKED：等待数据积累（20+交易日） |

## 核心结论

- **技术指标最优持有期 T+3~T+10**，超过 T+60 退化为噪声（base/07）
- **DIF月线谷值 T+6m 胜率 100%**，是全项目最强信号（macd/report/10）
- **MACD+换手率暴增 共振 → 收益翻倍**（resonance/）
- **热度均值回归 年化74.61%**，纯热度出场优于所有 MACD 出场变体（task/162）

## 快速上手

```bash
# 1. 配置数据库连接
cp .env.example .env
# 编辑 .env 填入 MySQL 连接信息

# 2. 运行研究脚本（以 MACD 为例）
cd macd/scripts
python compute_index_macd.py
python analyze_index_macd.py

# 3. 回测（需 my-stock 环境）
cd F:/projects/my-stock
python -m app.services.run_backtest --orders orders.csv --output ./output --strategy-name xx
```

## 目录结构

```
├── base/                跨主题研究基础（方法论 ×7、约束）
├── {topic}/             按主题分目录（macd/rsi/turnover/ma/...）
│   ├── base/            该主题基础（信号定义、Schema）
│   ├── scripts/         分析脚本（含 00-脚本使用说明.md）
│   ├── report/          研究报告
│   └── data/            统计数据（CSV/TXT）
├── general/             通用研究（跨主题报告 + 脚本）
├── la/scripts/          LA 选股分析脚本
├── backtest/            回测输出目录
├── lib/                 公共基础设施（config/database/logger）
├── task/                研究任务（完成后归档到主题目录）
├── docs/                项目文档 + 约束
└── scripts/             工具脚本（Tushare 文档爬取等）
```
