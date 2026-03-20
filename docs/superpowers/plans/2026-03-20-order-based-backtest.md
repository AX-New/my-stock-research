# 订单驱动回测引擎 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 my-stock 中新增订单驱动回测引擎，支持 CLI 脚本调用，报告输出到指定目录

**Architecture:** 在 my-stock 现有 backtest 模块中新增 `OrderBasedEngine` 类（纯计算，不含指标计算）+ `generate_report()` 函数（报告生成）+ `run_backtest.py` CLI 入口（串联引擎+指标+报告）。my-stock-research 创建 `backtest/output/` 目录存放报告。所有新增代码与现有 API 路径无交集。

**Tech Stack:** Python / pandas / SQLAlchemy / argparse / ECharts（HTML报告）

**Spec:** `docs/superpowers/specs/2026-03-20-order-based-backtest-design.md`

---

## 文件结构

### my-stock 项目（F:/projects/my-stock）

| 文件 | 动作 | 职责 |
|------|------|------|
| `app/services/backtest_engine.py` | 修改（追加） | 新增 `OrderBasedEngine` 类，纯交易模拟，返回 trades/equity_curve/rejected_orders |
| `app/services/backtest_service.py` | 修改（追加） | 新增 `generate_report()` 函数，现有函数不动 |
| `app/services/run_backtest.py` | 新建 | CLI 入口：参数解析、CSV校验、K线加载、引擎调用、`_calc_metrics` 计算指标、报告生成 |

### my-stock-research 项目（F:/projects/my-stock-research）

| 文件 | 动作 | 职责 |
|------|------|------|
| `backtest/output/.gitkeep` | 新建 | 保持 output 目录存在 |
| `backtest/.gitignore` | 新建 | 忽略 output/ 下的报告文件 |

### 关键设计决策

1. **`_calc_metrics` 在 CLI 层调用，不在引擎内调用** — 避免 `backtest_engine.py` → `backtest_service.py` 的循环导入（`backtest_service.py` 已经 import `BacktestEngine`）
2. **`pct_chg` 直接从 `_load_kline` 结果获取** — `_apply_adjustment` 只复权 PRICE_COLS，不修改 `pct_chg`，无需额外查询
3. **`trade_dates` 从上证指数获取** — 避免因个股停牌导致交易日序列不完整
4. **my-stock 的改动在 feature 分支上进行** — 遵循 git 工作流

---

## Task 1: 创建 my-stock-research 目录结构

**Files:**
- Create: `F:/projects/my-stock-research/backtest/output/.gitkeep`
- Create: `F:/projects/my-stock-research/backtest/.gitignore`

- [ ] **Step 1: 创建 backtest/output/ 目录和 .gitignore**

```
backtest/
├── output/
│   └── .gitkeep
└── .gitignore
```

`.gitignore` 内容:
```
output/*
!output/.gitkeep
```

- [ ] **Step 2: 提交**

```bash
cd F:/projects/my-stock-research
git add backtest/
git commit -m "[ADD] - claude01 - 新增 backtest/output 目录，存放回测报告"
```

---

## Task 2: 新增 OrderBasedEngine 类

**Files:**
- Modify: `F:/projects/my-stock/app/services/backtest_engine.py`（在文件末尾追加）

**前置阅读:**
- `F:/projects/my-stock/app/services/backtest_engine.py` — 现有 BacktestEngine 的费用计算逻辑

**注意:** `OrderBasedEngine.run()` 只返回原始数据（trades, equity_curve, rejected_orders），**不调用 `_calc_metrics`**。指标计算由 CLI 脚本（Task 4）在引擎外部完成，避免循环导入。

- [ ] **Step 0: 在 my-stock 创建 feature 分支**

```bash
cd F:/projects/my-stock
git pull origin master
git checkout -b task-claude01-order-based-engine
```

- [ ] **Step 1: 在 backtest_engine.py 末尾追加 OrderBasedEngine 类**

核心逻辑：
```python
class OrderBasedEngine:
    """订单驱动回测引擎 - 接收买卖指令，模拟真实交易成本"""

    def __init__(self, initial_capital=1000000.0, commission_rate=0.0003,
                 min_commission=5.0, stamp_duty_rate=0.001, slippage_rate=0.002):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}   # {code: {"quantity": int, "avg_cost": float}}
        self.trades = []
        self.equity_curve = []
        self.rejected_orders = []

        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.stamp_duty_rate = stamp_duty_rate
        self.slippage_rate = slippage_rate

    def run(self, orders, kline_data, trade_dates):
        """
        主入口：遍历 trade_dates，每天先执行订单（先卖后买），再记录净值

        参数:
            orders: list[dict] — 每个包含 date, code, action, price, amount
            kline_data: dict — {(date, code): {"open","high","low","close","pct_chg"}}
            trade_dates: list[str] — 完整交易日序列

        返回（不含 metrics，由调用方计算）:
            {"trades": [...], "equity_curve": [...], "rejected_orders": [...]}
        """
        # 1. 按日期分组订单: {date: [orders]}
        orders_by_date = {}
        for o in orders:
            orders_by_date.setdefault(o["date"], []).append(o)

        # 2. 遍历 trade_dates
        for date in trade_dates:
            day_orders = orders_by_date.get(date, [])
            # 先卖后买
            sell_orders = [o for o in day_orders if o["action"] == "sell"]
            buy_orders = [o for o in day_orders if o["action"] == "buy"]

            for order in sell_orders:
                self._process_order(order, kline_data)
            for order in buy_orders:
                self._process_order(order, kline_data)

            # 记录当日净值
            self._record_equity(date, kline_data)

        return {
            "trades": self.trades,
            "equity_curve": self.equity_curve,
            "rejected_orders": self.rejected_orders,
        }

    def _process_order(self, order, kline_data):
        """处理单笔订单：校验 → 执行 或 拒绝"""
        date, code = order["date"], order["code"]
        kline = kline_data.get((date, code))

        # 无K线数据（停牌等）
        if kline is None:
            self.rejected_orders.append({**order, "reason": "无K线数据(停牌)"})
            return

        result = self._execute_order(order, kline)
        if result is None:
            pass  # 已在 _execute_order 中记入 rejected_orders

    def _execute_order(self, order, kline):
        """执行单笔订单，成功返回 trade dict，失败返回 None 并记入 rejected"""
        date = order["date"]
        code = order["code"]
        action = order["action"]
        target_price = float(order["price"])
        amount = float(order.get("amount") or 0)
        high, low = kline["high"], kline["low"]

        # 0. 卖出检查持仓
        if action == "sell" and code not in self.positions:
            self.rejected_orders.append({**order, "reason": "无持仓"})
            return None

        # 1. 涨跌停判断（基于原始 pct_chg）
        pct_chg = kline.get("pct_chg")
        if pct_chg is not None:
            if action == "buy" and high == low and pct_chg >= 9.8:
                self.rejected_orders.append({**order, "reason": "涨停买不进"})
                return None
            if action == "sell" and high == low and pct_chg <= -9.8:
                self.rejected_orders.append({**order, "reason": "跌停卖不出"})
                return None

        # 2. 价格校验：clamp 到 [low, high]
        target_price = min(max(target_price, low), high)

        # 3. 滑点
        if action == "buy":
            actual_price = target_price * (1 + self.slippage_rate)
            actual_price = min(actual_price, high)
        else:
            actual_price = target_price * (1 - self.slippage_rate)
            actual_price = max(actual_price, low)

        # 4. 执行
        if action == "buy":
            quantity = int(amount / actual_price / 100) * 100
            if quantity < 100:
                self.rejected_orders.append({**order, "reason": "金额不足一手"})
                return None
            cost = self._calculate_buy_cost(actual_price, quantity)
            if cost > self.cash:
                self.rejected_orders.append({**order, "reason": "资金不足"})
                return None

            self.cash -= cost
            self.positions[code] = {
                "quantity": quantity,
                "avg_cost": cost / quantity,
            }
            commission = max(actual_price * quantity * self.commission_rate,
                           self.min_commission)
            trade = {
                "trade_date": date, "ts_code": code, "direction": "buy",
                "target_price": round(float(order["price"]), 2),
                "actual_price": round(actual_price, 2),
                "quantity": quantity,
                "amount": round(actual_price * quantity, 2),
                "commission": round(commission, 2),
                "stamp_duty": 0, "pnl": 0,
            }
            self.trades.append(trade)
            return trade
        else:
            pos = self.positions[code]
            quantity = pos["quantity"]
            proceeds = self._calculate_sell_proceeds(actual_price, quantity)
            pnl = proceeds - (pos["avg_cost"] * quantity)

            self.cash += proceeds
            commission = max(actual_price * quantity * self.commission_rate,
                           self.min_commission)
            stamp_duty = actual_price * quantity * self.stamp_duty_rate
            trade = {
                "trade_date": date, "ts_code": code, "direction": "sell",
                "target_price": round(float(order["price"]), 2),
                "actual_price": round(actual_price, 2),
                "quantity": quantity,
                "amount": round(proceeds, 2),
                "commission": round(commission, 2),
                "stamp_duty": round(stamp_duty, 2),
                "pnl": round(pnl, 2),
            }
            self.trades.append(trade)
            del self.positions[code]
            return trade

    def _calculate_buy_cost(self, price, quantity):
        """买入总成本 = 金额 + max(金额×佣金率, 最低佣金)"""
        raw_cost = price * quantity
        commission = max(raw_cost * self.commission_rate, self.min_commission)
        return raw_cost + commission

    def _calculate_sell_proceeds(self, price, quantity):
        """卖出净得 = 金额 - 佣金 - 印花税"""
        raw_proceeds = price * quantity
        commission = max(raw_proceeds * self.commission_rate, self.min_commission)
        stamp_duty = raw_proceeds * self.stamp_duty_rate
        return raw_proceeds - commission - stamp_duty

    def _record_equity(self, date, kline_data):
        """记录当日净值: cash + 所有持仓市值(按收盘价)"""
        market_value = 0
        for code, pos in self.positions.items():
            kline = kline_data.get((date, code))
            if kline:
                price = kline["close"]
            else:
                price = pos["avg_cost"]  # 停牌用成本价
            market_value += pos["quantity"] * price

        self.equity_curve.append({
            "trade_date": date,
            "cash": round(self.cash, 2),
            "market_value": round(market_value, 2),
            "total_value": round(self.cash + market_value, 2),
        })
```

- [ ] **Step 2: 验证现有 BacktestEngine 未被修改**

确认只在文件末尾追加了新类，原有 `BacktestEngine` 的代码行没有任何变动。

- [ ] **Step 3: 提交**

```bash
cd F:/projects/my-stock
git add app/services/backtest_engine.py
git commit -m "[ADD] - claude01 - 新增 OrderBasedEngine 订单驱动回测引擎"
```

---

## Task 3: 新增 generate_report() 函数

**Files:**
- Modify: `F:/projects/my-stock/app/services/backtest_service.py`（在文件末尾追加）

**前置阅读:**
- `F:/projects/my-stock/app/services/backtest_service.py:245-415` — 现有 `_save_html_report()` HTML 生成逻辑
- `F:/projects/my-stock/app/services/backtest_service.py:215-243` — 现有 `_save_report()` JSON 生成逻辑

- [ ] **Step 1: 在 backtest_service.py 末尾追加 generate_report() 函数**

```python
def generate_report(strategy_name, ts_codes, start_date, end_date,
                    initial_capital, metrics, equity_curve, trades, elapsed,
                    output_dir=None, rejected_orders=None,
                    commission_rate=0.0003, stamp_duty_rate=0.001,
                    slippage_rate=0.002):
    """
    生成独立回测报告（HTML + JSON + trades.csv）到指定目录
    报告目录命名: {YYYYMMDD_HHMMSS}_{strategy_name}/
    返回: 报告目录路径字符串
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = f"{timestamp}_{strategy_name}"
    if output_dir:
        report_path = Path(output_dir) / folder_name
    else:
        report_path = REPORT_DIR / folder_name
    report_path.mkdir(parents=True, exist_ok=True)

    # ---- report.json ----
    report_data = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "config": {
            "strategy_name": strategy_name,
            "ts_codes": ts_codes,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "commission_rate": commission_rate,
            "stamp_duty_rate": stamp_duty_rate,
            "slippage_rate": slippage_rate,
        },
        "metrics": metrics,
        "elapsed_seconds": elapsed,
        "rejected_orders": rejected_orders or [],
        "equity_curve": equity_curve,
        "trades": trades,
    }
    json_path = report_path / "report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    # ---- trades.csv ----
    if trades:
        import csv
        csv_path = report_path / "trades.csv"
        fieldnames = ["trade_date", "ts_code", "direction", "target_price",
                      "actual_price", "quantity", "amount", "commission",
                      "stamp_duty", "pnl"]
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(trades)

    # ---- report.html ----
    # 复用 _save_html_report 的 HTML/CSS/ECharts 模板
    # 关键修改点:
    #   1. 交易记录用 t.get("quantity") 而非 t.get("volume")
    #   2. 新增 target_price 和 actual_price 列
    #   3. 如果 rejected_orders 非空，增加一个 "被拒绝订单" card
    #   4. 输出到 report_path / "report.html"
    # （完整 HTML 模板从 _save_html_report 复制并修改上述几点）

    log.info(f"[backtest] 报告已生成: {report_path}")
    return str(report_path)
```

HTML 报告修改要点（从 `_save_html_report` 复制时需改动的地方）：
- `t.get('volume', '')` → `t.get('quantity', '')`（修正字段名）
- 表头增加 `目标价` / `实际价` 列
- 如果 `rejected_orders` 非空，在交易记录 card 下方新增一个 card 展示被拒订单
- 文件路径: `report_path / "report.html"`（不是 `REPORT_DIR / f"{bt_id}.html"`）

- [ ] **Step 2: 验证现有函数未被修改**

确认 `_save_report()`、`_save_html_report()`、`run_backtest()` 等现有函数无任何变动。

- [ ] **Step 3: 提交**

```bash
cd F:/projects/my-stock
git add app/services/backtest_service.py
git commit -m "[ADD] - claude01 - 新增 generate_report() 独立报告生成函数"
```

---

## Task 4: 新增 run_backtest.py CLI 入口

**Files:**
- Create: `F:/projects/my-stock/app/services/run_backtest.py`

**前置阅读:**
- `F:/projects/my-stock/app/services/backtest_service.py:88-97` — `_load_qfq_daily()` K线加载逻辑
- `F:/projects/my-stock/app/services/kline_adjust_service.py` — `_load_kline` 返回的列包含 pct_chg，`_apply_adjustment` 只复权 PRICE_COLS 不改 pct_chg

- [ ] **Step 1: 创建 run_backtest.py**

```python
"""
订单驱动回测 CLI 入口

用法:
    cd F:/projects/my-stock
    python -m app.services.run_backtest --orders xx.csv --output ./output --strategy-name xx

orders.csv 格式:
    date,code,action,price,amount
    20260101,000001.SZ,buy,10.50,500000
    20260105,000001.SZ,sell,11.20,
"""
import argparse
import time
import pandas as pd
from pathlib import Path

from app.services.backtest_engine import OrderBasedEngine
from app.services.backtest_service import generate_report, _calc_metrics
from app.services.kline_adjust_service import (
    _load_kline, _load_adj_factor, _load_latest_adj_factor, _apply_adjustment
)
from app.models.tushare.index.index_daily import IndexDaily
from app.database import engine as db_engine
from app.logger import get_logger
from sqlalchemy import select

log = get_logger(__name__)


def _validate_orders(df):
    """校验订单 CSV 格式"""
    # 必填列
    required_cols = {"date", "code", "action", "price"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"订单CSV缺少必填列: {missing}")

    # action 只能是 buy/sell
    invalid = df[~df["action"].isin(["buy", "sell"])]
    if not invalid.empty:
        raise ValueError(f"action 只能是 buy/sell，发现: {invalid['action'].unique()}")

    # date 格式 YYYYMMDD
    if not df["date"].str.match(r"^\d{8}$").all():
        raise ValueError("date 格式必须为 YYYYMMDD")

    # 买入订单必须有 amount
    buy_orders = df[df["action"] == "buy"]
    if "amount" not in df.columns:
        if not buy_orders.empty:
            raise ValueError("买入订单必须有 amount 列")
    else:
        bad_buys = buy_orders[buy_orders["amount"].isna() | (buy_orders["amount"] <= 0)]
        if not bad_buys.empty:
            raise ValueError(f"以下买入订单缺少 amount:\n{bad_buys[['date','code']].to_string()}")


def _load_kline_from_db(codes, start_date, end_date):
    """
    从 my_stock 库加载K线数据

    1. 前复权 K线 (open/high/low/close): 价格校验和净值计算
    2. pct_chg: _load_kline 返回的原始列，_apply_adjustment 不修改它

    trade_dates 从上证指数获取（确保完整交易日序列，不受个股停牌影响）

    返回:
        kline_data: {(date, code): {"open","high","low","close","pct_chg"}}
        trade_dates: 排序后的完整交易日列表
    """
    kline_data = {}

    with db_engine.connect() as conn:
        # 从上证指数获取完整交易日序列
        idx_df = pd.read_sql(
            select(IndexDaily.trade_date)
            .where(IndexDaily.ts_code == "000001.SH",
                   IndexDaily.trade_date >= start_date,
                   IndexDaily.trade_date <= end_date)
            .order_by(IndexDaily.trade_date.asc()),
            conn
        )
        trade_dates = idx_df["trade_date"].tolist()

        for code in codes:
            # _load_kline 返回的 df 包含 pct_chg 列
            df_raw = _load_kline(conn, "market_daily", code, start_date, end_date)
            if df_raw.empty:
                continue

            # 保存原始 pct_chg（复权不会修改它）
            pct_chg_map = dict(zip(df_raw["trade_date"], df_raw["pct_chg"]))

            # 前复权
            df_adj = _load_adj_factor(conn, code, start_date, end_date)
            latest = _load_latest_adj_factor(conn, code)
            df_qfq = _apply_adjustment(df_raw, df_adj, "qfq", latest)

            for _, row in df_qfq.iterrows():
                date = row["trade_date"]
                kline_data[(date, code)] = {
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "pct_chg": pct_chg_map.get(date),
                }

    return kline_data, trade_dates


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
    log.info(f"[backtest] 读取订单: {args.orders}")
    df_orders = pd.read_csv(args.orders, dtype={"date": str, "code": str})
    _validate_orders(df_orders)
    orders = df_orders.to_dict("records")
    log.info(f"[backtest] 订单数: {len(orders)}")

    # 2. 提取股票代码和日期范围，加载K线
    codes = df_orders["code"].unique().tolist()
    start_date = df_orders["date"].min()
    end_date = df_orders["date"].max()
    log.info(f"[backtest] 加载K线: {len(codes)} 只股票, {start_date} ~ {end_date}")
    kline_data, trade_dates = _load_kline_from_db(codes, start_date, end_date)
    log.info(f"[backtest] K线数据: {len(kline_data)} 条, 交易日: {len(trade_dates)} 天")

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
    log.info(f"[backtest] 引擎执行完成, 耗时 {elapsed}s")

    # 4. 计算绩效指标（在 CLI 层调用，避免引擎循环导入）
    metrics = _calc_metrics(result["trades"], result["equity_curve"], args.capital)

    # 5. 生成报告
    report_dir = generate_report(
        strategy_name=args.strategy_name,
        ts_codes=codes,
        start_date=start_date,
        end_date=end_date,
        initial_capital=args.capital,
        metrics=metrics,
        equity_curve=result["equity_curve"],
        trades=result["trades"],
        elapsed=elapsed,
        output_dir=args.output,
        rejected_orders=result.get("rejected_orders"),
        commission_rate=args.commission,
        stamp_duty_rate=args.stamp_duty,
        slippage_rate=args.slippage,
    )

    # 6. 输出结果摘要
    print(f"\n回测完成，报告目录: {report_dir}")
    print(f"总收益: {metrics['total_return']}%")
    print(f"年化收益: {metrics['annual_return']}%")
    print(f"最大回撤: {metrics['max_drawdown']}%")
    print(f"夏普比率: {metrics['sharpe_ratio']}")
    print(f"胜率: {metrics['win_rate']}%")
    print(f"交易次数: {metrics['total_trades']}")
    rejected = result.get("rejected_orders", [])
    if rejected:
        print(f"被拒绝订单: {len(rejected)} 笔")
        for r in rejected:
            print(f"  - {r['date']} {r['code']} {r['action']} {r.get('reason','')}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 提交**

```bash
cd F:/projects/my-stock
git add app/services/run_backtest.py
git commit -m "[ADD] - claude01 - 新增 run_backtest.py 订单驱动回测 CLI 入口"
```

---

## Task 5: 端到端验证

**前置条件:** Task 1-4 全部完成

- [ ] **Step 1: 创建测试用 orders.csv**

在 `F:/projects/my-stock-research/backtest/` 下创建 `test_orders.csv`。

先查数据库确认可用的日期和价格范围：
```sql
SELECT trade_date, open, high, low, close
FROM market_daily
WHERE ts_code = '000001.SZ' AND trade_date BETWEEN '20250102' AND '20250228'
ORDER BY trade_date LIMIT 10;
```

用查到的真实价格构造买卖：
```csv
date,code,action,price,amount
20250102,000001.SZ,buy,{当日收盘价},500000
20250115,000001.SZ,sell,{当日收盘价},
20250116,600519.SH,buy,{当日收盘价},500000
20250210,600519.SH,sell,{当日收盘价},
```

- [ ] **Step 2: 运行回测**

```bash
cd F:/projects/my-stock
python -m app.services.run_backtest \
    --orders F:/projects/my-stock-research/backtest/test_orders.csv \
    --output F:/projects/my-stock-research/backtest/output \
    --strategy-name test_e2e \
    --capital 1000000
```

**预期输出:**
- 控制台打印回测结果摘要（总收益、年化收益、最大回撤、夏普比率等）
- 生成报告目录: `backtest/output/YYYYMMDD_HHMMSS_test_e2e/`

- [ ] **Step 3: 验证报告输出**

检查生成的报告目录应包含 3 个文件: `report.html`, `report.json`, `trades.csv`

验证:
1. `report.html` — 浏览器打开，检查 ECharts 图表正常渲染、指标数据一致
2. `report.json` — 检查 config/metrics/trades/equity_curve/rejected_orders 结构完整
3. `trades.csv` — 检查列名（trade_date, ts_code, direction, target_price, actual_price, quantity, amount, commission, stamp_duty, pnl）和数据正确

- [ ] **Step 4: 验证边界情况**

构造一个包含边界情况的 orders.csv：
- 涨停日买入（应被拒绝）
- 停牌日买入（应被拒绝）
- 卖出未持仓的股票（应被拒绝）
- 金额不足一手的买入（应被拒绝）

运行并检查 rejected_orders 正确记录，控制台输出被拒原因。

- [ ] **Step 5: 合并分支，清理**

```bash
cd F:/projects/my-stock
git checkout master
git merge task-claude01-order-based-engine
git push origin master
git branch -d task-claude01-order-based-engine
```

删除 `test_orders.csv`，保留 output/.gitkeep。
