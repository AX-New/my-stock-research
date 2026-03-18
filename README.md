# My Stock Research

股票量化研究平台 — 重型指标分析、信号验证、策略研究。

## 定位

- **脚本 + 报告**：跑分析脚本，产出研究报告和统计数据
- **与 my-stock 共享 MySQL**：读 `my_stock` 生产库的 K 线和基本面数据，写 `stock_research` 研究库
- **无 Web 服务**：纯本地脚本项目

## 研究主题

| 主题 | 目录 | 状态 |
|------|------|------|
| MACD | `macd/` | 已完成牛熊周期分析、指数/行业/个股信号验证 |
| RSI | `rsi/` | 已完成分层分析（指数→行业→个股） |
| 换手率 | `turnover/` | 已完成分层分析 |
| 均线 | `ma/` | 已完成分层分析 |
| 资金流向 | `moneyflow/` | 已完成行业+个股分析 |
| 热度排名 | `hot/` | 已完成假设验证 |
| 峰值择时 | `peak_timing/` | 已完成信号衰减分析 |
| 共振 | `resonance/` | 进行中 |
| PyTorch | `pytorch/` | Demo 阶段 |
| Qlib | `qlib/` | Demo 阶段 |
| LA 选股分析 | `la/scripts/` | 选股结果分析脚本 |

## 快速上手

```bash
# 1. 配置数据库连接
cp .env.example .env
# 编辑 .env 填入 MySQL 连接信息

# 2. 运行研究脚本（以 MACD 为例）
cd macd/scripts
python compute_index_macd.py
python analyze_index_macd.py
```

## 目录结构

```
├── base/              跨主题研究基础（方法论、约束）
├── {topic}/           按主题分目录
│   ├── scripts/       分析脚本
│   ├── report/        研究报告
│   └── data/          统计数据
├── la/scripts/        LA 选股分析脚本
├── lib/               公共基础设施
├── task/              研究任务
└── docs/constraints/  项目约束
```
