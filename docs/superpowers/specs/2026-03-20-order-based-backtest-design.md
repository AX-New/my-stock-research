# 订单驱动回测引擎设计

**创建时间**: 20260320

## 背景

my-stock 现有的 `BacktestEngine` 是信号驱动模式：接收 signal 列（1/-1/0），引擎内部决定交易执行。
my-stock-research 的研究脚本需要的是订单驱动模式：研究脚本自己控制全部策略逻辑（全市场选股、轮转换仓等），
引擎只做交易模拟器——接收明确的买卖指令，模拟真实成本，校验价格合理性，输出报告。

## 目标

1. 在 my-stock 现有模块上扩展，新增 `OrderBasedEngine` 类和 `run_backtest.py` 脚本入口
2. 研究脚本产出 `orders.csv`，通过命令行调用回测脚本
3. 引擎自动从 `my_stock` 数据库查询K线数据，校验价格合理性
4. 报告按 `{时间戳}_{策略名}/` 文件夹管理，输出到调用方指定目录
5. 现有 API 回测路径不受影响

## 整体流程

```
研究脚本产出 orders.csv
        ↓
python F:/projects/my-stock/app/services/run_backtest.py \
    --orders orders.csv \
    --output F:/projects/my-stock-research/backtest/output \
    --strategy-name heat_rotation \
    --capital 1000000
        ↓
引擎执行：
  1. 读取 orders.csv
  2. 提取所有股票代码 + 日期范围
  3. 从 my_stock 库查询K线数据：
     - 前复权数据：用于净值计算（持仓市值）
     - 原始数据（pct_chg）：用于涨跌停判断
     - 日期范围覆盖全部持仓期间，不仅是订单日期
  4. 按日期排序订单，逐笔执行：
     - 涨跌停判断：基于原始 pct_chg，涨停买不进、跌停卖不出
     - 价格校验：检查是否在当日 [low, high] 范围内
     - 滑点计算：买入价上浮、卖出价下浮（边界价附近滑点可能被吸收）
     - 佣金/印花税扣除
     - 更新持仓和资金
  5. 生成每日净值曲线（覆盖全部交易日）
  6. 计算绩效指标
  7. 输出报告到指定目录
        ↓
backtest/output/20260320_143052_heat_rotation/
    ├── report.html    (ECharts 交互图表)
    ├── report.json    (完整数据)
    └── trades.csv     (交易记录)
```

## 改动范围

### my-stock（3个文件）

| 文件 | 动作 | 说明 |
|------|------|------|
| `app/services/backtest_engine.py` | 新增类 | 新增 `OrderBasedEngine`，与 `BacktestEngine` 并列 |
| `app/services/backtest_service.py` | 抽取函数 | 抽出 `generate_report()` 支持自定义输出路径和文件夹命名 |
| `app/services/run_backtest.py` | 新增文件 | CLI 入口脚本，argparse 解析参数 |

### my-stock-research（1个目录）

| 路径 | 动作 | 说明 |
|------|------|------|
| `backtest/output/` | 新增目录 | 报告输出目录，`.gitignore` 忽略 output 内容 |

## 详细设计

### 1. OrderBasedEngine 类

新增在 `app/services/backtest_engine.py`，与现有 `BacktestEngine` 并列。

```python
class OrderBasedEngine:
    """订单驱动回测引擎 - 接收明确的买卖指令，模拟真实交易成本"""

    def __init__(self,
                 initial_capital: float = 1000000.0,
                 commission_rate: float = 0.0003,
                 min_commission: float = 5.0,
                 stamp_duty_rate: float = 0.001,
                 slippage_rate: float = 0.002):
        ...

    def run(self, orders: list[dict], kline_data: dict, trade_dates: list[str]) -> dict:
        """
        执行回测

        参数:
            orders: 订单列表，每个订单包含 date, code, action, price, amount
                    按 date 排序，同一天先卖后买
                    amount: 买入目标金额（引擎按100股整数倍计算实际数量），卖出时忽略（全部卖出）
            kline_data: K线数据字典
                    格式: {(date, code): {"open", "high", "low", "close", "pct_chg"}}
                    pct_chg: 原始涨跌幅（非复权），用于涨跌停判断
            trade_dates: 完整交易日序列（覆盖全部持仓期间），用于每日净值计算

        返回:
            {
                "trades": [...],         # 实际成交记录
                "equity_curve": [...],    # 每日净值（覆盖全部交易日）
                "metrics": {...},         # 绩效指标
                "rejected_orders": [...], # 被拒绝的订单（价格越界/涨跌停/无持仓等）
            }
        """
```

**核心逻辑**：

```python
def _execute_order(self, order, kline):
    """执行单笔订单"""
    date, code, action, target_price, amount = order
    high, low = kline["high"], kline["low"]

    # 0. 卖出前检查：是否有持仓
    if action == "sell" and code not in self.positions:
        return None  # 无持仓，拒绝，记入 rejected_orders，reason="无持仓"

    # 1. 涨跌停判断（基于原始 pct_chg，不受复权影响）
    pct_chg = kline.get("pct_chg")  # 原始涨跌幅，从 market_daily 表获取
    if pct_chg is not None:
        # 一字涨停：high == low 且涨幅 >= 9.8%，买入失败
        if action == "buy" and high == low and pct_chg >= 9.8:
            return None  # 涨停买不进
        # 一字跌停：high == low 且跌幅 <= -9.8%，卖出失败
        if action == "sell" and high == low and pct_chg <= -9.8:
            return None  # 跌停卖不出

    # 2. 价格校验：超出当日范围则按边界价
    target_price = min(max(target_price, low), high)

    # 3. 滑点（边界价附近滑点可能被部分或完全吸收）
    if action == "buy":
        actual_price = target_price * (1 + self.slippage_rate)
        actual_price = min(actual_price, high)  # 不超过当日最高价
    else:
        actual_price = target_price * (1 - self.slippage_rate)
        actual_price = max(actual_price, low)   # 不低于当日最低价

    # 4. 执行交易
    if action == "buy":
        # 按 amount 计算可买手数（100股整数倍）
        quantity = int(amount / actual_price / 100) * 100
        if quantity < 100:
            return None  # 金额不足一手
        cost = self._calculate_buy_cost(actual_price, quantity)
        if cost > self.cash:
            return None  # 资金不足
        ...
    else:
        # 卖出：全部卖出该持仓
        quantity = self.positions[code]["quantity"]
        ...
```

**每日净值计算**：

引擎遍历 `trade_dates`（完整交易日序列），对每个交易日：
1. 执行当日所有订单（先卖后买）
2. 用当日收盘价计算所有持仓市值
3. 记录 equity_curve 条目（cash + market_value = total_value）

K线数据必须覆盖全部持仓期间（不仅是订单日期），确保持仓期间每天都能用收盘价计算市值。

**持仓管理**：

```python
self.positions = {}  # {code: {"quantity": int, "avg_cost": float}}
self.cash = initial_capital
```

- 买入：按订单 `amount` 计算可买手数（100股整数倍），扣除资金 + 佣金
- 卖出：全部卖出该股持仓，释放资金，扣除佣金 + 印花税，计算单笔盈亏
- 卖出不存在的持仓：拒绝，记入 `rejected_orders`，reason="无持仓"

### 2. 报告生成函数

从 `backtest_service.py` 抽出 `generate_report()`，支持自定义输出路径：

```python
def generate_report(strategy_name: str, ts_codes: list, start_date: str,
                    end_date: str, initial_capital: float, metrics: dict,
                    equity_curve: list, trades: list, elapsed: float,
                    output_dir: str = None,
                    commission_rate: float = 0.0003,
                    stamp_duty_rate: float = 0.001,
                    slippage_rate: float = 0.002) -> str:
    """
    生成回测报告（HTML + JSON + trades.csv）

    参数:
        output_dir: 报告输出根目录，将在其下创建 {时间戳}_{策略名}/ 子目录
                    如果为 None，使用 my-stock 默认 report/ 目录

    返回:
        报告目录路径
    """
    # 创建文件夹: 20260320_143052_heat_rotation/
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"{timestamp}_{strategy_name}"

    if output_dir:
        report_path = Path(output_dir) / folder_name
    else:
        report_path = REPORT_DIR / folder_name

    report_path.mkdir(parents=True, exist_ok=True)

    # 生成 report.html（复用现有 _save_html_report 逻辑，改写出路径）
    # 生成 report.json
    # 生成 trades.csv（新增，方便研究分析）
```

**现有 API 兼容**：现有的 `_save_report()` 和 `_save_html_report()` 保持不变，
继续以 `{bt_id}.json` / `{bt_id}.html` 命名存到 `report/` 目录。
`generate_report()` 是新增的独立函数，仅供 CLI 脚本调用，不改动现有 API 调用路径。

### 3. CLI 入口脚本

新增 `app/services/run_backtest.py`：

```python
"""
订单驱动回测 CLI 入口

用法:
    python run_backtest.py --orders orders.csv --output ./output --strategy-name heat_rotation

orders.csv 格式:
    date,code,action,price,amount
    20260101,000001.SZ,buy,10.50,500000
    20260105,000001.SZ,sell,11.20,
"""
import argparse
import sys
import time
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="订单驱动回测")
    parser.add_argument("--orders", required=True, help="订单CSV文件路径")
    parser.add_argument("--output", required=True, help="报告输出目录")
    parser.add_argument("--strategy-name", default="backtest", help="策略名称")
    parser.add_argument("--capital", type=float, default=1000000, help="初始资金")
    parser.add_argument("--commission", type=float, default=0.0003, help="佣金费率")
    parser.add_argument("--min-commission", type=float, default=5.0, help="最低佣金")
    parser.add_argument("--stamp-duty", type=float, default=0.001, help="印花税率")
    parser.add_argument("--slippage", type=float, default=0.002, help="滑点率")
    args = parser.parse_args()

    # 1. 读取订单并校验
    df_orders = pd.read_csv(args.orders, dtype={"date": str, "code": str})
    _validate_orders(df_orders)  # 校验必填列、action值、日期格式
    orders = df_orders.to_dict("records")

    # 2. 提取股票代码和日期范围，从数据库加载K线
    codes = df_orders["code"].unique().tolist()
    start_date = df_orders["date"].min()
    end_date = df_orders["date"].max()
    kline_data, trade_dates = _load_kline_from_db(codes, start_date, end_date)

    # 3. 创建引擎并运行
    engine = OrderBasedEngine(
        initial_capital=args.capital,
        commission_rate=args.commission,
        min_commission=args.min_commission,
        stamp_duty_rate=args.stamp_duty,
        slippage_rate=args.slippage,
    )
    start_time = time.time()
    result = engine.run(orders, kline_data, trade_dates)
    elapsed = round(time.time() - start_time, 2)

    # 4. 生成报告
    report_dir = generate_report(
        strategy_name=args.strategy_name,
        ts_codes=codes,
        start_date=start_date,
        end_date=end_date,
        initial_capital=args.capital,
        metrics=result["metrics"],
        equity_curve=result["equity_curve"],
        trades=result["trades"],
        elapsed=elapsed,
        output_dir=args.output,
        commission_rate=args.commission,
        stamp_duty_rate=args.stamp_duty,
        slippage_rate=args.slippage,
    )

    # 5. 输出结果摘要
    print(f"回测完成，报告目录: {report_dir}")
    print(f"总收益: {result['metrics']['total_return']}%")
    print(f"年化收益: {result['metrics']['annual_return']}%")
    print(f"最大回撤: {result['metrics']['max_drawdown']}%")
    print(f"夏普比率: {result['metrics']['sharpe_ratio']}")
    if result.get("rejected_orders"):
        print(f"被拒绝订单: {len(result['rejected_orders'])} 笔")
```

**输入校验函数**：

```python
def _validate_orders(df: pd.DataFrame):
    """校验订单 CSV 格式"""
    required_cols = {"date", "code", "action", "price"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"订单CSV缺少必填列: {missing}")
    invalid_actions = df[~df["action"].isin(["buy", "sell"])]
    if not invalid_actions.empty:
        raise ValueError(f"action 只能是 buy/sell，发现: {invalid_actions['action'].unique()}")
    # 校验 date 格式为 YYYYMMDD
    if not df["date"].str.match(r"^\d{8}$").all():
        raise ValueError("date 格式必须为 YYYYMMDD")
```

**K线加载函数**：

```python
def _load_kline_from_db(codes: list, start_date: str, end_date: str) -> tuple[dict, list]:
    """
    从 my_stock 库加载K线数据

    加载两种数据：
    1. 前复权K线（open/high/low/close）：用于价格校验和净值计算
    2. 原始 pct_chg：用于涨跌停判断（避免复权导致的判断错误）

    日期范围覆盖 [start_date, end_date] 的全部交易日，
    确保持仓期间每天都能计算市值。

    返回:
        kline_data: {(date, code): {"open", "high", "low", "close", "pct_chg"}}
        trade_dates: 排序后的完整交易日列表
    """
```

### 4. 报告输出结构

```
backtest/output/20260320_143052_heat_rotation/
├── report.html     ← ECharts 交互图表（复用现有模板）
├── report.json     ← 完整数据（配置 + 指标 + 净值 + 交易）
└── trades.csv      ← 交易记录（方便 pandas 再分析）
```

**report.json 结构**：
```json
{
  "generated_at": "2026-03-20 14:30:52",
  "config": {
    "strategy_name": "heat_rotation",
    "ts_codes": ["000001.SZ", "600519.SH", ...],
    "start_date": "20240101",
    "end_date": "20241231",
    "initial_capital": 1000000,
    "commission_rate": 0.0003,
    "stamp_duty_rate": 0.001,
    "slippage_rate": 0.002
  },
  "metrics": {
    "total_return": 18.5,
    "annual_return": 15.5,
    "sharpe_ratio": 1.2,
    "max_drawdown": 8.3,
    "win_rate": 55.0,
    "profit_loss_ratio": 1.8,
    "total_trades": 20,
    "final_value": 1185000.0
  },
  "rejected_orders": [
    {"date": "20240315", "code": "000001.SZ", "action": "buy", "price": 10.5, "reason": "涨停买不进"}
  ],
  "equity_curve": [...],
  "trades": [...]
}
```

**trades.csv 列**：
```csv
trade_date,ts_code,direction,target_price,actual_price,quantity,amount,commission,stamp_duty,pnl
```

### 5. my-stock-research 目录变更

```
my-stock-research/
├── backtest/
│   ├── output/           ← 报告输出（.gitignore 忽略内容）
│   └── .gitignore        ← output/ 内容不入库
├── ...
```

## 订单 CSV 格式

```csv
date,code,action,price,amount
20260101,000001.SZ,buy,10.50,500000
20260105,000001.SZ,sell,11.20,
20260105,600519.SH,buy,1800.00,1000000
```

| 列 | 必填 | 说明 |
|----|------|------|
| `date` | 是 | 交易日期，YYYYMMDD |
| `code` | 是 | 股票代码，如 000001.SZ |
| `action` | 是 | `buy` 或 `sell` |
| `price` | 是 | 目标价格（引擎会校验并加滑点） |
| `amount` | 买入必填 | 买入目标金额，引擎按此计算可买手数（100股整数倍）；卖出时忽略（全部卖出） |

## 价格校验规则

涨跌停判断使用原始 `pct_chg`（从 `market_daily` 表获取），避免前复权导致除权日误判。

| 场景 | 处理方式 |
|------|---------|
| 指定价在 [low, high] 范围内 | 按指定价 + 滑点成交 |
| 指定价 > high | 按 high + 滑点成交（滑点后不超过 high，即滑点被吸收） |
| 指定价 < low | 按 low - 滑点成交（滑点后不低于 low，即滑点被吸收） |
| 一字涨停（high == low，pct_chg >= 9.8）且买入 | 拒绝，记入 rejected_orders |
| 一字跌停（high == low，pct_chg <= -9.8）且卖出 | 拒绝，记入 rejected_orders |
| 无K线数据（停牌等） | 拒绝，记入 rejected_orders |
| 卖出无持仓的股票 | 拒绝，记入 rejected_orders，reason="无持仓" |
| 买入金额不足一手（100股） | 拒绝，记入 rejected_orders，reason="金额不足一手" |
| 买入资金不足 | 拒绝，记入 rejected_orders，reason="资金不足" |

**注意**：当目标价接近当日价格边界时，滑点可能被部分或完全吸收（因为实际价格不会超出 [low, high] 范围）。

## 费用计算

与现有 BacktestEngine 保持一致：

| 费用 | 计算方式 | 默认值 |
|------|---------|--------|
| 买入佣金 | max(成交金额 × commission_rate, min_commission) | 万3，最低5元 |
| 卖出佣金 | max(成交金额 × commission_rate, min_commission) | 万3，最低5元 |
| 印花税 | 成交金额 × stamp_duty_rate（仅卖出） | 千1 |
| 滑点 | 买入价 × (1 + slippage_rate)，卖出价 × (1 - slippage_rate) | 千2 |

## 指标计算

复用现有 `_calc_metrics()` 逻辑：

- 总收益率、年化收益率
- 夏普比率（无风险利率 3%）
- 最大回撤
- 胜率、盈亏比
- 交易次数、最终资产

## 兼容性

- 现有 API 路径（`/api/backtest/*`）**不受影响**
- 现有 `BacktestEngine`（信号驱动）**不修改**
- 现有 `_save_report()` / `_save_html_report()` **不修改**（保持 `{bt_id}` 命名方式）
- 新增的 `OrderBasedEngine`、`generate_report()`、`run_backtest.py` 是**纯新增代码**，与现有功能无交集
