# VectorBT 与 CCXT 框架深度分析报告

> 分析日期：2026-03-17
> 目的：评估两个框架的能力、适用场景，以及如何在 my-stock 项目中落地

---

## 一、VectorBT — 向量化回测与量化分析框架

### 1.1 是什么

VectorBT 是一个**高性能 Python 量化分析框架**，核心理念是用 NumPy 数组 + Numba JIT 编译替代传统事件驱动回测。它不是一个个模拟每根K线的买卖，而是把整个交易策略表达为**向量运算**，一次性处理完所有数据。

**一句话定位**：研究阶段的超级加速器——快速验证海量参数组合，找到最优策略参数。

### 1.2 核心架构

```
用户代码 (Python)
    ↓
pandas DataFrame / NumPy Array
    ↓
VectorBT 指标引擎 + Portfolio 引擎
    ↓
NumPy 向量运算 (简单计算) + Numba @jit (复杂逻辑)
    ↓
原生机器码执行
```

**关键模块**：

| 模块 | 功能 |
|------|------|
| `Indicators` | MA、RSI、MACD、布林带等，支持自定义指标 |
| `Signals` | 入场/出场信号生成（金叉、死叉等） |
| `Portfolio` | 交易模拟、订单处理、盈亏计算 |
| `Returns` | 夏普比、索提诺比、最大回撤等性能指标 |
| `Records` | 订单/交易记录管理，回撤分析 |
| `Plotting` | Plotly 交互式图表、热力图 |
| `Data` | Yahoo Finance、CCXT、Alpaca 数据接入 |

### 1.3 为什么快

传统回测框架（Backtrader、Zipline）是**事件驱动**：逐根K线遍历，每根K线判断信号、执行交易。100万根K线就需要循环100万次。

VectorBT 是**向量化**：
1. 把所有收盘价放进一个 NumPy 数组
2. 用向量运算一次算完所有 MA（不用循环）
3. 用布尔数组表示信号（`fast_ma > slow_ma` 一行代码生成所有信号）
4. Portfolio 引擎用 Numba 编译的 C 级别循环处理交易

**性能对比**：

| 框架 | 100万订单耗时 | 倍率 |
|------|-------------|------|
| **VectorBT** | **70-100ms** | **1x（基准）** |
| NautilusTrader | ~500ms | 5-7x |
| Backtrader | 5-10秒 | 50-100x |
| Zipline | 15+秒 | 150+x |

### 1.4 杀手级特性：参数网格搜索

这是 VectorBT 最强大的能力——**同时测试成千上万个参数组合**：

```python
import vectorbt as vbt

# 定义参数范围
fast_windows = list(range(5, 30))      # 25 个快线周期
slow_windows = list(range(20, 120, 5))  # 20 个慢线周期
# 总共 25 × 20 = 500 个组合

# 一行代码算完所有 MA
fast_ma = vbt.MA.run(close, fast_windows, short_name='fast')
slow_ma = vbt.MA.run(close, slow_windows, short_name='slow')

# 一行代码生成所有信号
entries = fast_ma.ma_crossed_above(slow_ma.ma)
exits = fast_ma.ma_crossed_below(slow_ma.ma)

# 一行代码回测所有 500 个组合
portfolio = vbt.Portfolio.from_signals(close, entries, exits)

# 结果是一个 DataFrame，500 行，每行一个组合的完整绩效
print(portfolio.total_return())   # 500 个收益率
print(portfolio.sharpe_ratio())   # 500 个夏普比
```

在 Backtrader 里做同样的事需要写一个嵌套循环跑 500 次，可能要几分钟。VectorBT 几秒搞定。

### 1.5 使用示例

#### 示例1：基础 MA 金叉策略

```python
import vectorbt as vbt

# 下载数据
close = vbt.YFData.download('AAPL', start='2020-01-01').get('Close')

# 计算双均线
fast_ma = vbt.MA.run(close, 10)
slow_ma = vbt.MA.run(close, 50)

# 生成信号
entries = fast_ma.ma_crossed_above(slow_ma.ma)  # 金叉买入
exits = fast_ma.ma_crossed_below(slow_ma.ma)     # 死叉卖出

# 回测
pf = vbt.Portfolio.from_signals(close, entries, exits, init_cash=100000)

# 查看结果
print(pf.stats())
# Total Return    [%]:  45.2
# Sharpe Ratio:         1.23
# Max Drawdown [%]:     -18.5
# Win Rate     [%]:     62.3
# Total Trades:         28

# 可视化
pf.plot().show()
```

#### 示例2：RSI + MA 组合策略

```python
rsi = vbt.RSI.run(close, window=14)
fast_ma = vbt.MA.run(close, 10)
slow_ma = vbt.MA.run(close, 50)

# 入场：金叉 AND RSI > 50（趋势确认）
entries = (fast_ma.ma_crossed_above(slow_ma.ma)) & (rsi.rsi > 50)
# 出场：死叉 OR RSI < 30（超卖保护）
exits = (fast_ma.ma_crossed_below(slow_ma.ma)) | (rsi.rsi < 30)

pf = vbt.Portfolio.from_signals(close, entries, exits)
```

#### 示例3：多股票同时回测

```python
# 同时回测 10 只股票
symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
           'META', 'NVDA', 'JPM', 'V', 'MA']
data = vbt.YFData.download(symbols, start='2020-01-01')
close = data.get('Close')

# 所有股票共用同一套策略参数
fast_ma = vbt.MA.run(close, 10)
slow_ma = vbt.MA.run(close, 50)
entries = fast_ma.ma_crossed_above(slow_ma.ma)
exits = fast_ma.ma_crossed_below(slow_ma.ma)

pf = vbt.Portfolio.from_signals(close, entries, exits)
# 结果自动按股票分组，可以比较哪只股票策略效果最好
```

#### 示例4：热力图找最优参数

```python
fast_windows = list(range(5, 30))
slow_windows = list(range(30, 100, 5))

fast_ma = vbt.MA.run(close, fast_windows, short_name='fast')
slow_ma = vbt.MA.run(close, slow_windows, short_name='slow')
entries = fast_ma.ma_crossed_above(slow_ma.ma)
exits = fast_ma.ma_crossed_below(slow_ma.ma)

pf = vbt.Portfolio.from_signals(close, entries, exits)

# 生成夏普比热力图：X轴快线周期，Y轴慢线周期，颜色=夏普比
fig = pf.sharpe_ratio().vbt.heatmap(
    x_level='fast_window',
    y_level='slow_window'
)
fig.show()
```

### 1.6 开源版 vs PRO 版

| 特性 | 开源版 (免费) | PRO 版 ($20/月) |
|------|-------------|----------------|
| 核心回测 | ✓ | ✓（更快） |
| 指标计算 | ✓ | ✓ |
| 参数优化 | ✓ | ✓（并行化） |
| 多时间框架 | ✓ | ✓ |
| 可视化 | ✓ | ✓ |
| 并行计算 | 有限 | 内置支持 |
| 文档质量 | 一般 | 优秀 |
| 开发状态 | 社区维护 | 活跃开发 |
| 技术支持 | GitHub Issues | 私有 Slack |

**建议**：先用开源版验证框架是否适合项目需求，确认价值后再考虑 PRO。

### 1.7 局限性

| 局限 | 影响 | 应对 |
|------|------|------|
| **无实盘交易** | 只能回测，不能自动下单 | 回测用 VectorBT，实盘用其他方案 |
| **内存消耗大** | 大参数网格 × 多股票会爆内存 | 分批处理，控制参数范围 |
| **无限价单模拟** | 所有订单按收盘价成交 | 对高频策略不精确 |
| **学习曲线陡** | 需要熟悉 NumPy/Pandas | 先掌握基础再深入 |
| **首次运行慢** | Numba JIT 编译需要 1-5 秒 | 预编译或接受首次延迟 |
| **滑点模型简单** | 只支持固定百分比滑点 | 对中低频策略影响不大 |

---

## 二、CCXT — 加密货币交易所统一接口

### 2.1 是什么

CCXT（CryptoCurrency eXchange Trading）是一个**加密货币交易所统一 API 库**，支持 109+ 家交易所，提供标准化的数据获取和交易接口。

**一句话定位**：写一套代码，连接所有加密货币交易所。

**⚠️ 重要前提：CCXT 只支持加密货币交易所，不支持 A 股市场。**

### 2.2 核心架构

```
你的代码
    ↓
CCXT 统一接口 (fetch_ticker, create_order, ...)
    ↓
Exchange 适配层 (binance.py, okx.py, ...)
    ↓
HTTP REST / WebSocket
    ↓
各交易所 API
```

**设计理念**：
- **统一抽象**：所有交易所共享相同的方法签名和返回格式
- **公私分离**：公共 API（行情）无需认证，私有 API（交易）需要 API Key
- **特性检测**：`exchange.has['fetchOHLCV']` 检查交易所是否支持某功能

### 2.3 支持的交易所（部分）

| 类别 | 交易所 |
|------|--------|
| **头部 CEX** | Binance, OKX, Bybit, Gate.io, KuCoin, Coinbase, Kraken |
| **衍生品** | Binance Futures, Bybit, OKX Futures, Deribit |
| **DEX** | Uniswap (通过聚合器), Hyperliquid |
| **区域性** | Upbit (韩国), Bitflyer (日本), Mercado (巴西) |

### 2.4 核心功能

#### 2.4.1 行情数据

```python
import ccxt

exchange = ccxt.binance({'enableRateLimit': True})

# 获取实时价格
ticker = exchange.fetch_ticker('BTC/USDT')
print(f"BTC 价格: {ticker['last']}")
print(f"24h 涨跌: {ticker['percentage']}%")
print(f"24h 成交量: {ticker['quoteVolume']}")

# 获取 K 线数据
ohlcv = exchange.fetch_ohlcv('BTC/USDT', timeframe='1h', limit=100)
# 返回: [[timestamp, open, high, low, close, volume], ...]

# 获取订单簿
orderbook = exchange.fetch_order_book('BTC/USDT')
print(f"买一: {orderbook['bids'][0]}")
print(f"卖一: {orderbook['asks'][0]}")
```

#### 2.4.2 交易操作

```python
exchange = ccxt.binance({
    'apiKey': 'xxx',
    'secret': 'yyy',
    'enableRateLimit': True
})

# 限价买入
order = exchange.create_order('BTC/USDT', 'limit', 'buy', 0.01, 60000)

# 市价卖出
order = exchange.create_order('BTC/USDT', 'market', 'sell', 0.01)

# 查询订单状态
status = exchange.fetch_order(order['id'], 'BTC/USDT')

# 撤单
exchange.cancel_order(order['id'], 'BTC/USDT')
```

#### 2.4.3 账户管理

```python
# 查询余额
balance = exchange.fetch_balance()
print(f"USDT 可用: {balance['USDT']['free']}")
print(f"BTC 总量: {balance['BTC']['total']}")

# 查询成交历史
trades = exchange.fetch_my_trades('BTC/USDT', limit=50)
```

#### 2.4.4 异步模式

```python
import asyncio
import ccxt.async_support as ccxt_async

async def monitor_prices():
    exchange = ccxt_async.binance({'enableRateLimit': True})

    symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
    tickers = await asyncio.gather(
        *[exchange.fetch_ticker(s) for s in symbols]
    )

    for ticker in tickers:
        print(f"{ticker['symbol']}: {ticker['last']}")

    await exchange.close()

asyncio.run(monitor_prices())
```

### 2.5 WebSocket 支持（原 CCXT Pro，已合并）

从 v1.95+ 开始，WebSocket 功能已合并到免费版本：

```python
import ccxt.pro as ccxtpro

async def watch_trades():
    exchange = ccxtpro.binance()
    while True:
        trades = await exchange.watch_trades('BTC/USDT')
        for trade in trades:
            print(f"{trade['datetime']} {trade['side']} {trade['amount']} @ {trade['price']}")
```

**REST vs WebSocket**：

| 对比项 | REST (轮询) | WebSocket (推送) |
|--------|------------|-----------------|
| 延迟 | 高（轮询间隔） | 低（实时推送） |
| 带宽 | 高（重复请求） | 低（持续连接） |
| 服务器压力 | 高 | 低 |
| 适合场景 | 低频查询 | 实时行情/交易 |

### 2.6 与 VectorBT 的配合

VectorBT 内置了 CCXT 数据源支持，可以直接获取加密货币数据进行回测：

```python
import vectorbt as vbt

# 通过 CCXT 获取 Binance 的 BTC 数据
data = vbt.CCXTData.download(
    symbols='BTC/USDT',
    exchange='binance',
    timeframe='1h',
    start='2024-01-01'
)
close = data.get('Close')

# 然后用 VectorBT 的策略框架回测
fast_ma = vbt.MA.run(close, 10)
slow_ma = vbt.MA.run(close, 50)
entries = fast_ma.ma_crossed_above(slow_ma.ma)
exits = fast_ma.ma_crossed_below(slow_ma.ma)

pf = vbt.Portfolio.from_signals(close, entries, exits)
print(pf.stats())
```

### 2.7 局限性

| 局限 | 说明 |
|------|------|
| **仅限加密货币** | 不支持股票、期货、外汇等传统市场 |
| **不支持 A 股** | 上交所、深交所完全不在支持范围 |
| **限速严格** | 交易所限速 1-2 req/s，高频场景受限 |
| **签名性能** | Python ECDSA 签名慢（~45ms），需用 Coincurve 优化 |
| **历史数据有限** | 部分交易所只提供近期数据 |
| **功能差异大** | 不同交易所支持的功能不同，需要逐个检查 |

---

## 三、两个框架的关系与定位

```
┌─────────────────────────────────────────────────┐
│                  量化交易系统                      │
├──────────────────┬──────────────────────────────┤
│   数据获取层      │   分析/回测层                  │
│                  │                              │
│  ┌──────────┐   │   ┌──────────────┐           │
│  │ CCXT     │───┼──→│ VectorBT     │           │
│  │ (加密货币)│   │   │ (回测引擎)    │           │
│  └──────────┘   │   └──────────────┘           │
│                  │          ↓                    │
│  ┌──────────┐   │   ┌──────────────┐           │
│  │ Tushare  │───┼──→│ 策略优化      │           │
│  │ (A股)    │   │   │ 参数网格搜索  │           │
│  └──────────┘   │   └──────────────┘           │
│                  │          ↓                    │
│  ┌──────────┐   │   ┌──────────────┐           │
│  │ AkShare  │───┼──→│ 绩效分析      │           │
│  │ (实时)   │   │   │ 可视化报告    │           │
│  └──────────┘   │   └──────────────┘           │
└──────────────────┴──────────────────────────────┘
```

**简单说**：
- **CCXT** = 加密货币的"数据管道"（类似我们项目里 Tushare 的角色）
- **VectorBT** = 高速回测引擎（比我们现有的 backtest_service.py 快 100 倍）

---

## 四、在 my-stock 项目中的落地方案

### 4.1 当前项目架构回顾

我们已有的回测能力：
- `app/services/backtest_service.py` — 自研回测引擎（事件驱动）
- `app/strategies/` — 5 个内置策略模板（MA、MACD、RSI、布林带、双MA+量）
- `research/` — 独立的指标研究模块（MACD、RSI、MA、换手率、资金流向）
- `scripts/batch_backtest.py` — 批量回测脚本

当前痛点：
1. **参数优化慢**：测试 100 个参数组合需要循环跑 100 次回测
2. **研究效率低**：每次验证一个指标假设都要写新脚本
3. **缺少加密货币数据**：项目只覆盖 A 股，没有币圈数据能力

### 4.2 VectorBT 落地方案（⭐ 高价值）

#### 方案 A：参数优化加速器

**目标**：为现有 5 个策略提供快速参数优化能力

```python
# research/vectorbt/param_optimizer.py

import vectorbt as vbt
import pandas as pd
from app.database import SessionLocal
from app.models.tushare.market_daily import MarketDaily

def optimize_ma_cross(ts_code: str, fast_range: range, slow_range: range):
    """MA 金叉策略参数网格搜索"""
    # 1. 从数据库读取前复权数据
    session = SessionLocal()
    rows = session.query(MarketDaily).filter(
        MarketDaily.ts_code == ts_code
    ).order_by(MarketDaily.trade_date).all()

    close = pd.Series(
        [r.close for r in rows],
        index=pd.to_datetime([r.trade_date for r in rows])
    )

    # 2. VectorBT 参数网格搜索（一次算完所有组合）
    fast_ma = vbt.MA.run(close, list(fast_range), short_name='fast')
    slow_ma = vbt.MA.run(close, list(slow_range), short_name='slow')
    entries = fast_ma.ma_crossed_above(slow_ma.ma)
    exits = fast_ma.ma_crossed_below(slow_ma.ma)

    pf = vbt.Portfolio.from_signals(
        close, entries, exits,
        init_cash=100000,
        fees=0.001,      # 手续费 0.1%
        slippage=0.001    # 滑点 0.1%
    )

    # 3. 返回排序后的最优参数
    results = pd.DataFrame({
        'sharpe': pf.sharpe_ratio(),
        'total_return': pf.total_return(),
        'max_drawdown': pf.max_drawdown(),
        'win_rate': pf.trades.win_rate
    }).sort_values('sharpe', ascending=False)

    return results.head(20)  # Top 20 参数组合
```

**价值**：现有的 batch_backtest.py 测 500 个组合可能要 10 分钟，VectorBT 只要几秒。

#### 方案 B：研究加速器

**目标**：加速 research/ 目录下的指标研究

```python
# research/vectorbt/signal_effectiveness.py

import vectorbt as vbt

def analyze_macd_signals(close: pd.Series):
    """MACD 信号有效性分析——VectorBT 版"""

    # 测试不同 MACD 参数组合
    # fast_period: 8-16, slow_period: 20-35, signal: 7-12
    macd = vbt.MACD.run(
        close,
        fast_window=[8, 10, 12, 14, 16],
        slow_window=[20, 25, 30, 35],
        signal_window=[7, 9, 11]
    )

    # 金叉买入、死叉卖出
    entries = macd.macd_crossed_above(macd.signal)
    exits = macd.macd_crossed_below(macd.signal)

    pf = vbt.Portfolio.from_signals(close, entries, exits)

    # 60 个参数组合的完整绩效对比
    return pf.stats()
```

#### 方案 C：全市场策略筛选

**目标**：在 5000+ 只 A 股上快速筛选策略有效的股票

```python
# research/vectorbt/market_scan.py

def scan_market_with_strategy(strategy_func, stock_list: list):
    """全市场策略扫描"""

    # 构建多股票 DataFrame（列=股票代码，行=日期）
    close_df = build_multi_stock_df(stock_list)  # 从数据库批量读取

    # VectorBT 自动对所有股票并行计算
    fast_ma = vbt.MA.run(close_df, 10)
    slow_ma = vbt.MA.run(close_df, 50)
    entries = fast_ma.ma_crossed_above(slow_ma.ma)
    exits = fast_ma.ma_crossed_below(slow_ma.ma)

    pf = vbt.Portfolio.from_signals(close_df, entries, exits)

    # 筛选夏普比 > 1 的股票
    sharpe = pf.sharpe_ratio()
    good_stocks = sharpe[sharpe > 1.0].sort_values(ascending=False)

    return good_stocks
```

### 4.3 CCXT 落地方案（⭐ 中等价值）

由于 CCXT **不支持 A 股**，它在 my-stock 项目中的价值取决于是否需要扩展加密货币能力。

#### 方案 A：加密货币数据对比分析

**目标**：研究 BTC 走势与 A 股的相关性

```python
# research/crypto/btc_astock_correlation.py

import ccxt
import pandas as pd

def fetch_btc_daily():
    """获取 BTC 日线数据"""
    exchange = ccxt.binance({'enableRateLimit': True})
    ohlcv = exchange.fetch_ohlcv('BTC/USDT', '1d', limit=365)

    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df.set_index('date')

def analyze_correlation():
    """BTC 与上证指数/创业板的相关性分析"""
    btc = fetch_btc_daily()
    # 从数据库读取指数数据
    sh_index = get_index_daily('000001.SH')

    # 计算滚动相关性
    corr = btc['close'].pct_change().rolling(30).corr(
        sh_index['close'].pct_change()
    )
    return corr
```

#### 方案 B：多市场量化框架

**目标**：将数据获取层抽象化，支持 A 股 + 加密货币

```python
# app/data_providers/base.py
class DataProvider:
    def fetch_ohlcv(self, symbol, timeframe, limit): ...
    def fetch_ticker(self, symbol): ...

# app/data_providers/tushare_provider.py
class TushareProvider(DataProvider):
    """A 股数据"""
    ...

# app/data_providers/ccxt_provider.py
class CCXTProvider(DataProvider):
    """加密货币数据"""
    def fetch_ohlcv(self, symbol, timeframe, limit):
        exchange = ccxt.binance({'enableRateLimit': True})
        return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
```

### 4.4 落地优先级建议

| 优先级 | 方案 | 预期价值 | 工作量 | 建议 |
|--------|------|---------|--------|------|
| 🔴 P0 | VectorBT 参数优化器 | 研究效率提升 10-100 倍 | 2-3 天 | **立即做** |
| 🟡 P1 | VectorBT 研究加速器 | 替代 research/ 大量脚本 | 3-5 天 | 研究阶段时做 |
| 🟡 P1 | VectorBT 全市场扫描 | 快速筛选策略有效股票 | 3-5 天 | 有具体需求时做 |
| 🟢 P2 | CCXT 数据对比 | 跨市场相关性研究 | 1-2 天 | 有兴趣时做 |
| 🔵 P3 | 多市场数据框架 | 架构完整性 | 5-7 天 | 确认需要币圈后做 |

---

## 五、VectorBT vs 现有回测引擎对比

| 对比项 | 现有 backtest_service.py | VectorBT |
|--------|------------------------|----------|
| **回测模式** | 事件驱动（逐 K 线） | 向量化（一次性） |
| **单次回测速度** | 中等 | 极快 |
| **参数优化** | 需要循环 N 次 | 一次完成 N 个 |
| **自定义策略** | Python 类继承 | 布尔表达式/Numba |
| **实盘对接** | 可以（有模拟账户） | 不支持 |
| **API 集成** | 已有 FastAPI 接口 | 需要包装 |
| **策略持久化** | 数据库存储 | 无 |
| **可视化** | 前端 ECharts | Plotly 图表 |

**结论**：不是替代关系，而是**互补**。

- **VectorBT**：用于研究阶段——快速验证想法、优化参数、全市场扫描
- **现有引擎**：用于生产阶段——API 服务、模拟交易、前端展示、策略管理

**推荐工作流**：
```
研究阶段（VectorBT）→ 找到最优参数 → 写入策略模板 → 生产回测（现有引擎）→ 模拟交易
```

---

## 六、技术实施注意事项

### 6.1 安装

```bash
# VectorBT (开源版)
pip install vectorbt

# CCXT
pip install ccxt

# 依赖
# vectorbt 需要: numpy, pandas, numba, plotly, yfinance
# ccxt 需要: certifi, cryptography (可选 aiohttp 用于异步)
```

### 6.2 与现有数据的对接

VectorBT 默认从 Yahoo Finance 获取数据，但 A 股数据在我们的 MySQL 数据库中。需要写一个数据适配器：

```python
# app/utils/vbt_data_adapter.py

import pandas as pd
import vectorbt as vbt
from app.database import SessionLocal
from app.models.tushare.market_daily import MarketDaily

def load_stock_close(ts_code: str, start_date: str = None) -> pd.Series:
    """从数据库加载收盘价，格式兼容 VectorBT"""
    session = SessionLocal()
    query = session.query(MarketDaily).filter(
        MarketDaily.ts_code == ts_code
    )
    if start_date:
        query = query.filter(MarketDaily.trade_date >= start_date)

    rows = query.order_by(MarketDaily.trade_date).all()
    session.close()

    return pd.Series(
        [r.close for r in rows],
        index=pd.DatetimeIndex([r.trade_date for r in rows]),
        name=ts_code
    )

def load_multi_stock_close(ts_codes: list, start_date: str = None) -> pd.DataFrame:
    """加载多只股票的收盘价 DataFrame"""
    series_list = [load_stock_close(code, start_date) for code in ts_codes]
    return pd.concat(series_list, axis=1)
```

### 6.3 内存管理

VectorBT 的参数网格会占用大量内存。建议：

```python
# 控制参数范围
# ❌ 不要这样（100 × 100 = 10000 组合，可能爆内存）
fast = range(1, 100)
slow = range(1, 100)

# ✅ 合理范围（20 × 15 = 300 组合）
fast = range(5, 25)
slow = range(25, 100, 5)

# 大规模扫描时分批处理
for batch in chunk_stocks(all_stocks, batch_size=50):
    results = scan_batch(batch)
    save_results(results)
    gc.collect()  # 手动释放内存
```

---

## 七、总结

### VectorBT

| 项 | 结论 |
|----|------|
| **适合我们** | ✅ 非常适合。A 股数据从数据库读取，用 VectorBT 做向量化回测，效率提升显著 |
| **最大价值** | 参数优化（秒级完成上千个组合）、全市场策略扫描 |
| **与现有系统关系** | 互补，不替代。研究用 VectorBT，生产用现有引擎 |
| **建议行动** | 安装开源版，先写一个参数优化器验证效果 |

### CCXT

| 项 | 结论 |
|----|------|
| **适合我们** | ⚠️ 有限。CCXT 不支持 A 股，只在需要加密货币数据时有价值 |
| **最大价值** | BTC/A 股相关性研究、未来扩展加密货币交易能力 |
| **与现有系统关系** | 可作为新数据源接入，但不影响核心功能 |
| **建议行动** | 暂不集成，等有具体加密货币需求时再引入 |
