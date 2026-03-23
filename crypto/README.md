# Crypto Quant - 加密货币量化交易系统

独立部署的加密货币量化交易系统。

## 与 A 股系统的核心区别

| 维度 | A 股 (my-stock) | 加密货币 (crypto) |
|------|----------------|------------------|
| 数据量 | 5000+ 股票 × 数十年 → 海量 | 数十个交易对 × 数年 → 小量 |
| 存储策略 | 增量同步 + 按需获取 | **全量存储**所有历史数据 |
| 复权 | 前复权/后复权/不复权 | **无需复权** |
| 交易时间 | 工作日 9:30-15:00 | **7×24 全年无休** |
| 数据源 | Tushare (REST API) | CCXT (Binance/OKX) |

## 快速开始

### 1. 环境配置

```bash
cd crypto
cp .env.example .env
# 编辑 .env 填写数据库和交易所 API 配置
pip install -r requirements.txt
```

### 2. 同步数据

```bash
# 全量同步所有配置的交易对
python scripts/sync_data.py --all
```

### 3. 回测验证

```bash
python scripts/run_backtest.py --symbol BTC/USDT --strategy dual_ma
```

### 4. 启动 API

```bash
python run.py
# 访问 http://localhost:8001/docs 查看 API 文档
```

### 5. Docker 部署

```bash
docker-compose up -d
```

## 项目结构

```
crypto/
├── app/                    ← 核心代码（Python 包）
│   ├── config.py           ← 配置管理（环境变量驱动）
│   ├── database.py         ← SQLAlchemy 数据库层
│   ├── logger.py           ← 自包含日志
│   ├── db_utils.py         ← 批量操作工具
│   ├── main.py             ← FastAPI 应用
│   ├── models/             ← 数据模型
│   │   ├── symbol.py       ← 交易对元数据
│   │   ├── kline.py        ← K线数据（全量存储，无需复权）
│   │   ├── signal.py       ← 策略信号
│   │   ├── trade.py        ← 交易记录
│   │   └── position.py     ← 持仓管理
│   ├── services/           ← 业务逻辑
│   │   ├── exchange_client.py  ← CCXT 交易所封装
│   │   ├── data_sync.py        ← 全量数据同步
│   │   ├── indicator.py        ← 技术指标计算
│   │   ├── backtest_engine.py  ← 回测引擎
│   │   └── trade_engine.py     ← 交易执行引擎
│   ├── strategies/         ← 策略实现
│   │   ├── base.py         ← 策略基类
│   │   ├── dual_ma.py      ← 双均线策略
│   │   ├── rsi.py          ← RSI 策略
│   │   ├── macd.py         ← MACD 策略
│   │   ├── bollinger.py    ← 布林带策略
│   │   └── composite.py    ← 组合投票策略
│   └── api/                ← REST API 端点
│       ├── data.py         ← 数据同步/查询
│       ├── strategy.py     ← 策略信号
│       ├── backtest.py     ← 回测执行
│       └── trade.py        ← 交易/持仓查询
├── scripts/                ← CLI 脚本
│   ├── sync_data.py        ← 数据同步
│   ├── run_backtest.py     ← 策略回测
│   └── run_bot.py          ← 自动交易机器人
├── base/                   ← 研究文档
├── report/                 ← 研究报告
├── logs/                   ← 日志文件
├── run.py                  ← API 服务入口
├── .env.example            ← 环境配置模板
├── requirements.txt        ← Python 依赖
├── Dockerfile              ← Docker 镜像
└── docker-compose.yml      ← 容器编排
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/data/symbols/sync` | 同步交易对元数据 |
| GET | `/api/data/klines/sync` | 同步K线数据 |
| GET | `/api/data/sync/all` | 全量同步 |
| GET | `/api/data/klines` | 查询K线数据 |
| GET | `/api/strategy/list` | 列出可用策略 |
| GET | `/api/strategy/signal` | 计算策略信号 |
| GET | `/api/backtest/run` | 执行回测 |
| GET | `/api/trade/positions` | 查询持仓 |
| GET | `/api/trade/history` | 查询交易历史 |

## 数据库

独立数据库：`stock_crypto`

| 表 | 说明 |
|----|------|
| `crypto_symbol` | 交易对元数据（精度、手续费等） |
| `crypto_kline` | K线数据（全量存储，无需复权） |
| `crypto_signal` | 策略信号记录 |
| `crypto_trade` | 交易执行记录 |
| `crypto_position` | 持仓管理 |
