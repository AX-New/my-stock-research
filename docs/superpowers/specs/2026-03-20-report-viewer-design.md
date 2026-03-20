# 回测报告查看器设计文档

**创建时间**: 20260320 16:30

## 目标

在 my-stock Vue 前端新增报告查看器页面，支持浏览 CLI 生成的回测报告，并结合后端数据库实时拉取个股 K 线数据，为每只股票生成独立图表并标注买卖点。

## 背景

- `OrderBasedEngine` CLI 回测产出报告到 `backtest/output/{timestamp}_{strategy}/` 目录
- 报告包含 `report.json`（metrics、equity_curve、trades）、`report.html`、`trades.csv`
- 静态 HTML 报告无法实时查询数据库，个股 K 线展示能力缺失
- 需要一个交互式页面，把报告数据 + 数据库 K 线组合起来做可视化分析

## 架构

```
┌─────────────────────────────────────┐
│  Vue 前端 ReportViewer.vue          │
│  ┌───────────┐ ┌──────────────────┐ │
│  │ 报告列表   │ │ 报告详情         │ │
│  │           │ │ ┌──────────────┐ │ │
│  │ 扫描目录   │ │ │ MetricsCards │ │ │
│  │ 按时间排序 │ │ ├──────────────┤ │ │
│  │           │ │ │ 权益曲线      │ │ │
│  │           │ │ ├──────────────┤ │ │
│  │           │ │ │ 个股图表 ×N   │ │ │
│  │           │ │ │ K线+买卖标注  │ │ │
│  │           │ │ ├──────────────┤ │ │
│  │           │ │ │ TradeTable   │ │ │
│  │           │ │ └──────────────┘ │ │
│  └───────────┘ └──────────────────┘ │
└──────────────┬──────────────────────┘
               │ HTTP API
┌──────────────▼──────────────────────┐
│  FastAPI 后端 (my-stock)            │
│  app/api/report.py                  │
│  ┌────────────┐  ┌────────────────┐ │
│  │ 文件系统    │  │ MySQL 数据库    │ │
│  │ report.json │  │ K线 + 复权因子  │ │
│  └────────────┘  └────────────────┘ │
└─────────────────────────────────────┘
```

## 后端 API

新增 `app/api/report.py`，挂载到 `/report` 前缀。

### GET /report/list

扫描报告目录，返回报告列表。

**报告目录**: 可配置，默认 `F:/projects/my-stock-research/backtest/output/`

**目录命名规则**: `{YYYYMMDD_HHMMSS}_{strategy_name}/`

**响应**:
```json
{
  "code": 0,
  "data": [
    {
      "folder": "20260320_143000_heat_rotation",
      "strategy_name": "heat_rotation",
      "created_at": "2026-03-20 14:30:00",
      "has_report": true
    }
  ]
}
```

**逻辑**:
1. 扫描目录下所有子文件夹
2. 从文件夹名解析时间戳和策略名
3. 检查 `report.json` 是否存在
4. 按时间倒序排列

### GET /report/{folder}

读取指定报告的 `report.json` 完整内容。

**响应**: 使用标准 `ok(data)` 包装返回 `report.json` 的内容，包含 `config`、`metrics`、`equity_curve`、`trades`、`rejected_orders`。

**字段映射**: trades 中的字段需要做归一化映射，使其兼容现有前端组件：
- `actual_price` → `price`（AdvancedEquityChart、TradeTable 期望 `price`）
- `quantity` → `volume`（AdvancedEquityChart tooltip 期望 `volume`）
- 保留原始字段 `target_price`、`actual_price`、`quantity` 不删除

### GET /report/{folder}/stock-klines

根据报告中的股票代码和时间区间，从数据库拉取前复权 K 线数据。

**参数**:
- `folder`: 报告目录名（从 report.json 读取 ts_codes 和时间区间）

**响应**:
```json
{
  "code": 0,
  "data": {
    "000001.SZ": [
      {"trade_date": "20260101", "open": 10.5, "high": 10.8, "low": 10.3, "close": 10.6}
    ],
    "600519.SH": [...]
  }
}
```

**逻辑**:
1. 读取该报告的 `report.json`，从 `trades` 中提取实际交易过的 `ts_code` 集合（不用 config.ts_codes，避免拉取未交易的股票）
2. 使用 `config.start_date`、`config.end_date` 作为时间区间
3. 对每只股票调用现有的 `_load_qfq_daily()` 拉取前复权 K 线
4. 返回 `{ts_code: [{trade_date, open, high, low, close, vol}]}` 格式（包含成交量，便于未来升级为 K 线图）

## 前端

### 路由

```
/report          → ReportViewer.vue
```

### 页面结构: ReportViewer.vue

#### 报告选择区（顶部）

- 下拉选择框，列出所有报告（显示策略名 + 时间）
- 选择后自动加载报告数据和 K 线数据

#### 绩效指标

复用 `MetricsCards` 组件，传入 `report.json` 中的 `metrics`。

#### 总权益曲线

复用 `AdvancedEquityChart` 组件，传入 `equity_curve` 和 `trades`。

#### 个股图表区（核心新增）

从 `trades` 提取所有涉及的股票代码，对每只股票生成一个独立图表：

**每个图表包含**:
- 标题: 股票代码 + 名称
- 价格曲线: 该股在回测区间内的前复权收盘价（折线图）
- 买入标注: 绿色向上箭头 markPoint，标注在买入日期的价格位置
- 卖出标注: 红色向下箭头 markPoint，标注在卖出日期的价格位置
- tooltip: 悬浮显示日期、价格、交易金额、盈亏

**图表组件**: 新建 `StockTradeChart.vue`，接收 props:
```ts
{
  tsCode: string
  klineData: Array<{trade_date, open, high, low, close, vol}>
  trades: Array<{trade_date, direction, price, actual_price, target_price, amount, pnl, quantity}>
}
```

内部用 ECharts 渲染：
- series: 收盘价折线
- markPoint: 买卖标注点
- markArea (可选): 持仓区间背景色

#### 交易记录

复用 `TradeTable` 组件，传入 `trades`。

### 前端 API

新增 `frontend/src/api/report.ts`:

```ts
export function getReportList()
export function getReport(folder: string)
export function getStockKlines(folder: string)
```

## 数据流

```
1. 页面加载 → GET /report/list → 报告列表
2. 用户选择报告 → GET /report/{folder} → report.json 数据
3. 同时请求 → GET /report/{folder}/stock-klines → 各股 K 线
4. 前端组装:
   - metrics → MetricsCards
   - equity_curve + trades → AdvancedEquityChart
   - 按 ts_code 分组 trades + kline → StockTradeChart × N
   - trades → TradeTable
```

## 文件变更清单

### my-stock 后端
| 文件 | 操作 | 说明 |
|------|------|------|
| `app/api/report.py` | 新增 | 报告查看器 API（list、detail、stock-klines） |
| `app/api/__init__.py` 或主路由 | 修改 | 注册 report router |

### my-stock 前端
| 文件 | 操作 | 说明 |
|------|------|------|
| `frontend/src/api/report.ts` | 新增 | API 调用函数 |
| `frontend/src/views/ReportViewer.vue` | 新增 | 报告查看器页面 |
| `frontend/src/components/StockTradeChart.vue` | 新增 | 个股 K 线 + 买卖标注图表 |
| `frontend/src/router/index.ts` | 修改 | 添加 /report 路由 |

### 不需要变更
- `AdvancedEquityChart.vue` — 直接复用
- `MetricsCards.vue` — 直接复用
- `TradeTable.vue` — 直接复用
- `backtest_service.py` — generate_report() 不需要改动

## 约束

- K 线数据使用前复权（qfq），复用现有 `_load_qfq_daily()`
- 报告目录路径写在后端配置中，不硬编码
- API 不需要认证（报告是本地研究数据，非敏感）
