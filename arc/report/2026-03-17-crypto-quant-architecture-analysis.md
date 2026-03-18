# 加密货币量化交易系统架构分析报告

> 分析日期：2026-03-17
> 核心问题：是否应在现有 my-stock（A股量化平台）上扩展加密货币交易能力，还是独立建设新项目？

---

## 一、现有 my-stock 架构盘点

### 1.1 技术栈

| 层级 | 技术选型 |
|------|---------|
| 后端 | Python 3.10+ / FastAPI / SQLAlchemy 2.0 / MySQL |
| 前端 | Vue 3 / TypeScript / Pinia / ECharts 6 / Vite (PWA) |
| 数据源 | Tushare Pro（历史）+ AkShare（实时）|
| 部署 | Nginx 反代 + 本地 FastAPI 单进程 |

### 1.2 核心架构组件

```
FastAPI 单进程多线程
├── api_executor       10线程   HTTP请求处理
├── _sync_executor      8线程   Tushare数据同步（L1: 按接口，L2: 按股票）
├── AkShare Worker      1线程   串行化AkShare调用
├── MultiSource Workers 3线程   多数据源并行获取
└── sim_scheduler       1线程   模拟账户定时评估
```

**数据管道**：Tushare/AkShare → 限速代理/优先队列 → UnifiedCache(L1内存+L2过期+L3文件) → batch_upsert → MySQL

**调度器**：JSON注册表驱动，Phase 1串行（基础数据），Phase 2-5并行（行情/财务/资金流等）

**策略框架**：BaseStrategy接口（init_indicators → generate_signals），支持模板策略、自定义代码、独立进程三种执行模式

**回测引擎**：事件驱动逐日循环，含佣金/印花税/滑点/涨跌停约束/风控

### 1.3 组件可复用性评估

#### ✅ 可直接复用（无需修改或极少修改）

| 组件 | 说明 |
|------|------|
| `app/database.py` | 通用MySQL连接层，无股票特定逻辑 |
| `app/db_utils.py` | 通用 `batch_upsert`，适用于任何SQLAlchemy模型 |
| `UnifiedCache` 三层缓存 | 通用缓存框架，换fetcher即可 |
| `AkShareQueue` 优先队列模式 | 数据源无关的串行化队列 |
| `MultiSourceQueue` 多源切换 | 将sina/eastmoney换成binance/okx即可 |
| `app/logger.py` | 通用日志 |
| `app/api/response.py` | 统一响应格式 |
| `app/api/deps.py` / `auth_service` | JWT认证，完全通用 |
| `BaseStrategy` 策略接口 | DataFrame输入信号输出，市场无关 |
| `risk_manager.py` | 仓位管理逻辑通用 |
| `scheduler_all.py` 调度模式 | JSON注册表驱动的调度器完全通用 |
| 前端: KlineChart / EquityCurve | 任何OHLCV数据均可渲染 |
| 前端: 认证流程 / Pinia stores | 通用 |

#### ⚠️ 需要适配

| 组件 | 需要修改的部分 |
|------|--------------|
| `tushare_throttle.py` | 限速概念可复用，需替换为CCXT内置限速或通用限速器 |
| `backtest_engine.py` | 去掉印花税+涨跌停，改为maker/taker费率模型 |
| `sim_trading_service.py` | 去掉15:30定时评估，加密货币24/7交易 |
| `screener_service.py` | SQL筛选逻辑是A股特有，但多因子筛选概念通用 |
| 前端 `MinuteChart.vue` | 硬编码9:30-15:00时间轴，需改为24h滚动窗口 |
| 前端配色 | A股红涨绿跌，国际加密惯例相反 |

#### ❌ 不可复用（A股特有）

| 组件 | 原因 |
|------|------|
| `app/models/tushare/` 全部119个模型 | Tushare专有表结构 |
| `app/services/tushare/` 全部同步服务 | Tushare API特定 |
| `config/sync_registry*.json` | 指向Tushare服务 |
| `app/models/la/` LLM分析模型 | A股特有指标（沪深300 PE、涨停数等）|
| `app/services/akshare_*.py` | AkShare返回A股数据 |
| 宏观经济模型（CPI/PMI/SHIBOR等）| A股宏观数据 |
| `StockScreener.vue` | A股指标（PE/ROE/市值等）|

---

## 二、加密货币量化交易的特殊需求

### 2.1 与A股系统的本质差异

| 维度 | A股 | 加密货币 |
|------|-----|---------|
| **交易时间** | 工作日 9:30-15:00 | 24/7/365 永不休市 |
| **数据源** | Tushare/AkShare（中心化数据商）| 100+交易所各自API，高度碎片化 |
| **协议** | REST为主 | REST + WebSocket 混合（行情必须WS）|
| **结算** | T+1 通过中登 | 即时链上结算或交易所内部结算 |
| **托管** | 券商代管 | 自托管钱包 或 交易所账户（有对手方风险）|
| **波动率** | 日内2%算大波动 | 日内5-20%常见，极端50%+ |
| **杠杆** | 融资融券~2x | 合约最高1000x |
| **保证金** | 有缓冲期 | 到维持保证金即刻强平 |
| **资金费率** | 无 | 永续合约每8h收取，长短方向不同 |
| **监管** | 严格监管 | 监管薄弱，操纵/刷量常见 |
| **涨跌停** | 有（10%/20%/30%）| 无，闪崩随时可能 |
| **印花税** | 卖出0.1% | 无，但有maker/taker费 |

### 2.2 加密货币特有的架构挑战

**1. WebSocket生命周期管理**
- 连接可能"静默断开"——socket保持打开但不再推送数据
- 必须实现心跳检测 + 数据新鲜度检查（N秒无更新 → 强制重连）
- 重连后需REST快照 + 增量更新序列号对齐

**2. 订单簿重建复杂度**
- A股只需看NBBO最优价，加密需要自行维护L2订单簿
- 初始REST快照 → WebSocket增量diff → 序列号缺口检测 → 自动重同步

**3. 交易所级限速管理**
- 不同端点消耗不同"权重"（如Binance: GET /ticker = 1权重，GET /depth = 5-50权重）
- 超限 → 429错误 → 反复超限 → IP封禁（2分钟到3天）
- 需要中心化限速预算管理器，跨所有并发请求追踪

**4. 无自然日结**
- 不能假设"收盘后跑批处理"
- 余额对账、仓位对账、数据清洗必须持续进行
- 维护必须滚动重启，零停机

**5. 资金费率核算**
- 永续合约每8小时收取资金费率
- 方向可正可负（多头付空头 或 空头付多头）
- 回测和实盘P&L计算必须包含此项，否则严重失真

**6. 精度规则碎片化**
- 每个交易所每个交易对都有独立的：最小下单量、数量步长、价格步长、最小名义金额
- CCXT通过 `market['limits']` 和 `market['precision']` 归一化，但需显式处理

---

## 三、主流加密货币量化框架对比

### 3.1 框架定位图谱

```
             低频信号策略 ←————————————————————→ 高频做市/套利
                  |                                    |
           Freqtrade / Jesse                    Hummingbot
                  |                                    |
           （CCXT作为连接层）                  NautilusTrader
                  |                                    |
             适合个人开发者 ←———————————————→ 适合机构/团队
```

### 3.2 详细对比

| 维度 | CCXT | Freqtrade | Hummingbot | Jesse | NautilusTrader |
|------|------|-----------|------------|-------|----------------|
| **定位** | 交易所连接层 | 信号策略框架 | 做市策略框架 | 研究→生产 | 机构级HFT |
| **语言** | Python/TS/Go/C#/PHP | Python | Python+Cython | Python | Rust+Python |
| **架构** | 库 | 单体 | 单体(时钟驱动) | 分层单体 | Rust核心+Python控制面 |
| **回测** | ❌ 无 | ⭐⭐⭐ 优秀 | ⭐ 有限 | ⭐⭐⭐ 优秀 | ⭐⭐⭐ 优秀 |
| **WebSocket** | ✅ (Pro版,付费) | 通过CCXT | 原生(最低延迟) | ✅ | 原生(tokio) |
| **订单簿** | ✅ | 基础 | ⭐⭐⭐ 核心设计 | ✅ | ⭐⭐⭐ 无锁 |
| **DEX支持** | 部分 | 实验性 | 24+ DEX | 部分 | 增长中 |
| **ML集成** | ❌ | FreqAI内置 | ❌ | 内置(2024+) | ❌ |
| **交易所数** | 100+ | CCXT全部 | 主流CEX+DEX | 主流 | 多资产多市场 |
| **性能** | — | 中等 | 高(Cython) | 中等 | 最高(Rust) |
| **社区** | 非常大 | 非常大 | 大 | 中等 | 增长中 |
| **学习曲线** | 低 | 中 | 高 | 低 | 高 |
| **适合场景** | 底层连接 | 个人量化 | 做市/套利 | 研究转生产 | 专业机构 |

### 3.3 推荐选型

**对于个人开发者做信号策略**：
- **首选 CCXT 作为连接层** — 100+交易所统一API，不需要为每个交易所写连接器
- **策略框架自建** — 现有 my-stock 的 BaseStrategy 模式已经够用，比套用 Freqtrade 更灵活
- **不推荐直接用 Freqtrade/Jesse** — 它们是完整的独立系统，与 my-stock 架构冲突

---

## 四、架构决策：合 vs 分

### 4.1 方案A：在 my-stock 上扩展（合）

```
my-stock/
├── app/
│   ├── models/
│   │   ├── tushare/       # A股数据模型（已有）
│   │   └── crypto/        # 新增：加密货币数据模型
│   ├── services/
│   │   ├── tushare/       # A股同步服务（已有）
│   │   └── crypto/        # 新增：CCXT数据同步
│   ├── strategies/        # 共享策略框架
│   ├── ccxt_connector.py  # 新增：CCXT连接器
│   └── ...
├── frontend/
│   ├── views/
│   │   ├── StockDetail.vue    # A股页面
│   │   └── CryptoDetail.vue   # 新增：加密货币页面
│   └── ...
└── config/
    ├── sync_registry.json       # A股同步注册表
    └── crypto_sync_registry.json # 新增：加密货币同步注册表
```

**优势**：
- 基础设施零成本复用（数据库层、缓存、认证、日志、调度器）
- 策略框架统一，可写跨市场策略（如A股+BTC对冲）
- 前端组件复用（K线图、资产曲线、仓位管理）
- 部署维护成本低（一套系统）
- 开发速度快（不需要重新搭建脚手架）

**劣势**：
- 24/7运行需求与A股"工作日运行"模式冲突
- 代码膨胀：当前119个Tushare模型 + 加密货币模型 → 模型层臃肿
- 数据库混杂：A股和加密货币数据共用一个MySQL实例
- 测试复杂度增加：改动基础组件可能影响两个市场
- 架构耦合风险：为适配加密货币的实时需求，可能需要修改核心组件

**风险**：
- WebSocket长连接 + FastAPI单进程多线程 → 可能产生线程竞争
- 加密货币的高频数据写入可能影响A股查询性能

### 4.2 方案B：独立新项目（分）

```
my-crypto/                    # 新项目
├── app/
│   ├── models/              # 加密货币专用数据模型
│   ├── services/            # CCXT连接器、数据同步
│   ├── strategies/          # 加密策略（可从my-stock复制BaseStrategy）
│   ├── backtest/            # 适配maker/taker费率的回测引擎
│   └── websocket/           # WebSocket行情管理
├── frontend/                # 独立前端（或复用my-stock前端组件）
└── requirements.txt         # 干净的依赖

my-stock/                    # 现有项目不变
└── ...
```

**优势**：
- 架构干净，为加密货币24/7场景量身设计
- 独立演进，不受A股需求约束
- 部署独立，可以单独扩展
- 依赖干净，不引入Tushare/AkShare

**劣势**：
- 大量基础代码重复（数据库层、缓存、认证、日志约2000行）
- 两套系统维护成本高
- 无法直接写跨市场策略
- 前端组件需要重新搭建或做成共享库

### 4.3 方案C：共享核心 + 市场模块分离（推荐）

```
my-stock/
├── app/
│   ├── core/                    # 共享核心（从现有代码重构）
│   │   ├── database.py          # 数据库连接
│   │   ├── db_utils.py          # batch_upsert
│   │   ├── cache.py             # UnifiedCache
│   │   ├── queue.py             # 优先队列
│   │   ├── logger.py            # 日志
│   │   └── config.py            # 配置
│   │
│   ├── stock/                   # A股市场模块
│   │   ├── models/              # Tushare数据模型
│   │   ├── services/            # Tushare同步服务
│   │   ├── api/                 # A股API路由
│   │   └── config/              # 同步注册表
│   │
│   ├── crypto/                  # 加密货币市场模块（新增）
│   │   ├── models/              # 交易对/K线/订单/持仓模型
│   │   ├── services/            # CCXT连接器、数据同步
│   │   ├── api/                 # 加密货币API路由
│   │   ├── websocket/           # WS行情管理
│   │   └── config/              # 交易所配置
│   │
│   ├── strategies/              # 共享策略框架
│   │   ├── base.py              # BaseStrategy（市场无关）
│   │   ├── stock/               # A股策略模板
│   │   └── crypto/              # 加密策略模板
│   │
│   ├── backtest/                # 共享回测引擎（费率模型可配置）
│   │   ├── engine.py            # 核心引擎
│   │   ├── cost_model.py        # 费率模型（印花税/maker-taker可切换）
│   │   └── risk_manager.py      # 风控
│   │
│   └── api/                     # 共享API（认证等）
│       ├── auth.py
│       └── response.py
│
├── frontend/
│   ├── src/
│   │   ├── components/          # 共享组件（K线图等）
│   │   ├── views/
│   │   │   ├── stock/           # A股页面
│   │   │   └── crypto/          # 加密货币页面
│   │   └── ...
│   └── ...
└── ...
```

**优势**：
- 核心代码零重复，一次修改两处受益
- 市场模块完全独立，互不影响
- 策略框架统一，支持跨市场策略
- 增量开发：先跑通加密货币数据 → 策略 → 回测 → 实盘
- 部署灵活：可以选择只启动某个市场模块

**劣势**：
- 需要先做一次重构（将现有代码按 core/stock 分离）
- 重构有引入bug的风险

**重构工作量评估**：
- 核心层提取：~2天（database, db_utils, cache, queue, logger, config 本身就是独立的，只需移动目录+改import）
- A股模块归整：~3天（models/services/api 按 stock/ 子目录重新组织）
- 总计约5天，一次性投入，长期收益

---

## 五、综合推荐

### 5.1 最终建议：方案A（在my-stock上直接扩展）

虽然方案C（核心分离）在架构上最优雅，但考虑到实际情况，**推荐方案A**，理由如下：

**1. 投入产出比最优**
- 方案C需要先花5天重构，且重构过程可能引入bug，对已有功能产生风险
- 方案A可以立即开始开发，新增代码放在 `crypto/` 子目录下即可
- 如果后期确实需要分离，那时候再重构也不迟（代码已经写好了，分离比从零开始容易）

**2. 加密货币交易与A股高度同构**
- 数据模型：都是OHLCV + 指标 + 信号
- 策略逻辑：同样的 BaseStrategy 接口
- 回测逻辑：只需参数化费率模型
- 前端展示：同样的K线图、资产曲线
- 你说的"所有东西都共通"是对的

**3. 渐进式开发路径清晰**

```
Phase 1: 数据层（~1周）
  ├── crypto/models/          交易对、K线、行情模型
  ├── crypto/services/ccxt_connector.py   CCXT连接器
  └── crypto/services/sync_*.py           数据同步服务

Phase 2: 展示层（~1周）
  ├── crypto/api/             API路由
  └── frontend/views/crypto/  前端页面

Phase 3: 策略+回测（~1-2周）
  ├── strategies/crypto/      加密策略模板
  └── backtest cost_model     maker/taker费率适配

Phase 4: 实盘能力（~2周）
  ├── crypto/services/trading_service.py  下单执行
  ├── crypto/websocket/       实时行情推送
  └── 风控 + 告警
```

**4. 关于24/7的担忧**
- my-stock 本来就是常驻运行的（FastAPI长期运行），不是"收盘关闭"的
- AkShare队列已经是后台线程持续运行
- 加密货币的WebSocket连接同样可以作为后台线程/协程运行
- 真正需要改的只是：sim_trading_service 里的15:30定时评估逻辑

**5. 什么情况下应该分离**
- 当加密货币代码量超过A股代码量时
- 当需要独立部署（如加密交易跑在VPS上靠近交易所）时
- 当需要微服务架构做高频交易时

### 5.2 具体实施建议

#### 技术选型
- **连接层**：CCXT（必选，100+交易所统一API）
- **数据存储**：继续用MySQL（与现有系统一致，个人量化足够）
- **实时行情**：CCXT Pro WebSocket（如果做实时策略）或 REST轮询（如果只做日线策略）
- **策略框架**：复用现有BaseStrategy
- **回测引擎**：复用现有引擎，参数化费率模型

#### 需要新增的核心组件

| 组件 | 说明 | 优先级 |
|------|------|--------|
| `app/crypto/models/` | 交易所、交易对、K线、持仓、交易记录模型 | P0 |
| `app/crypto/services/ccxt_connector.py` | CCXT统一连接器，含限速、重连、错误处理 | P0 |
| `app/crypto/services/sync_ohlcv.py` | K线数据同步（类似Tushare daily同步） | P0 |
| `app/crypto/api/` | 行情查询、交易对列表等API | P1 |
| `app/backtest/cost_model.py` | 可配置费率模型（印花税 vs maker/taker） | P1 |
| `app/crypto/services/trading_service.py` | 下单执行服务 | P2 |
| `app/crypto/websocket/` | WebSocket行情管理（心跳、重连、订单簿重建） | P2 |

#### 需要注意的坑

1. **CCXT限速**：CCXT内置`enableRateLimit`，但默认限速可能太保守或太激进，建议结合现有`tushare_throttle.py`的自适应限速思路
2. **数据质量**：加密货币历史数据有缺失K线、价格毛刺、刷量问题，同步时需要数据清洗
3. **精度处理**：不同交易对的价格/数量精度不同（BTC精确到0.00000001，USDT精确到0.01），必须用CCXT的`market['precision']`
4. **回测陷阱**：加密货币的滑点和流动性与A股差异巨大，不能直接套用A股的滑点假设
5. **API密钥安全**：交易所API密钥必须环境变量管理，绝不能入库或入代码

---

## 六、框架技术细节参考

### 6.1 CCXT 核心架构

```python
import ccxt

# 统一接口 —— 100+交易所用同样的代码
exchange = ccxt.binance({
    'apiKey': 'xxx',
    'secret': 'xxx',
    'enableRateLimit': True,  # 内置限速
})

# 获取K线
ohlcv = exchange.fetch_ohlcv('BTC/USDT', '1d', limit=100)

# 获取订单簿
orderbook = exchange.fetch_order_book('BTC/USDT', limit=20)

# 下单
order = exchange.create_limit_buy_order('BTC/USDT', 0.001, 50000)

# 查余额
balance = exchange.fetch_balance()
```

**CCXT Pro (WebSocket)**：
```python
import ccxt.pro as ccxtpro

exchange = ccxtpro.binance()
while True:
    orderbook = await exchange.watch_order_book('BTC/USDT')
    # 实时订单簿更新，自动维护本地副本
```

### 6.2 与 my-stock 数据管道的对应关系

```
my-stock A股管道:                    my-stock 加密货币管道(新增):
Tushare pro.daily()                  CCXT exchange.fetch_ohlcv()
        ↓                                    ↓
TushareProProxy (自适应限速)          CcxtConnector (CCXT内置限速)
        ↓                                    ↓
batch_upsert → MySQL                 batch_upsert → MySQL (复用)
        ↓                                    ↓
scheduler_all 调度                    同样的调度模式 (复用)
        ↓                                    ↓
API → 前端K线图                       API → 前端K线图 (复用)
```

### 6.3 各框架适用场景总结

| 你的需求 | 推荐方案 |
|---------|---------|
| 只做日线/4H级别信号策略 | CCXT + 自建（在my-stock上扩展）|
| 做市/高频/套利 | 独立项目 + Hummingbot 或 NautilusTrader |
| 快速验证策略想法 | Freqtrade（独立使用，不集成到my-stock）|
| 研究转生产，追求代码整洁 | Jesse（独立使用）|
| 机构级多资产 | NautilusTrader（独立部署）|

---

## 七、结论

**核心判断**：加密货币量化与A股量化在数据模型、策略逻辑、回测框架、前端展示上高度同构。差异主要在连接层（交易所API vs 数据商API）和运行模式（24/7 vs 工作日）上。这些差异可以通过在 my-stock 内新增 `crypto/` 模块来解决，无需独立建设新项目。

**推荐路径**：在 my-stock 项目中新增 `app/crypto/` 模块，使用 CCXT 作为交易所连接层，复用现有的数据库层、缓存、策略框架、回测引擎和前端组件。先从数据同步和展示做起，验证可行后再扩展到策略执行和实盘交易。

**长期观察**：如果未来加密货币部分的代码量和复杂度超过A股部分，或者需要独立部署（如靠近交易所的低延迟VPS），那时再考虑分离。但目前阶段，合在一起是投入产出比最优的选择。
