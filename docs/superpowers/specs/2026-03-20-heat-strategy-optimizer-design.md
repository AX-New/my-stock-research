# 热度策略参数优化系统设计规范

**创建时间**: 20260320

## 1. 背景

基于东财人气排名的热度轮转策略已验证年化74.61%、Sharpe 1.22（`task/162-热度策略回测/03_heat_rotation_no_timeout.py`）。
参数优化目前仅靠10个硬编码组合，缺乏系统化优化能力和实盘运行能力。

**目标**：
1. 封装可复用的 `HeatRotationStrategy` 类，参数化设计
2. 构建参数优化引擎（网格搜索 + AI代理模型）
3. 产出每日可运行的实盘信号生成脚本
4. 增强 my-stock `optimizer_service.py` 支持可调用函数（callable）接口
5. 最优回测通过 OrderBasedEngine CLI 生成报告，前端 /report 可视化

---

## 2. 架构概览

```
my-stock-research/
└── heat/
    └── scripts/
        ├── config.py          ← DB配置：my_stock:3307, my_trend:3310(SSH隧道)
        ├── database.py        ← 引擎创建
        ├── data_loader.py     ← 数据加载（两库）
        ├── strategy.py        ← HeatRotationStrategy 类（核心）
        ├── optimizer.py       ← 参数优化引擎（网格搜索 + AI代理）
        ├── run_optimization.py← CLI：运行优化 + 生成最优报告
        ├── live_signal.py     ← 实盘信号生成（每日收盘后运行）
        └── 00-脚本使用说明.md

my-stock/
└── app/services/
    └── optimizer_service.py  ← 增强：支持 run_callable_optimization()
```

---

## 3. 核心组件

### 3.1 数据库配置（heat/scripts/config.py）

```python
# my_stock: 本地/远程（生产行情库）
MY_STOCK_DB_URI = "mysql+pymysql://root:root@127.0.0.1:3307/my_stock"

# my_trend: 腾讯云 SSH 隧道（port 3310 → 腾讯云 3306）
MY_TREND_DB_URI = "mysql+pymysql://root:root@127.0.0.1:3310/my_trend"
```

### 3.2 HeatRotationStrategy（heat/scripts/strategy.py）

**接口设计**：
```python
class HeatRotationStrategy:
    DEFAULT_PARAMS = {
        'lookback': 20,           # 热度位置回看窗口
        'buy_threshold': 0.80,    # 买入阈值
        'sell_threshold': 0.20,   # 卖出阈值
        'max_hold_days': 9999,    # 最大持仓天数（9999=无超时）
        'min_deal_amount': 5e7,   # 最低日成交额（流动性过滤）
        'n_positions': 1,         # 同时持仓数量
        'sort_by': 'rank_surge',  # 选股排序：'rank_surge' | 'heat_position'
    }

    def run(self, data_bundle: dict, params: dict) -> dict:
        """
        运行回测，返回:
        {
            'trades': pd.DataFrame,
            'equity_curve': pd.DataFrame,
            'metrics': dict,     # total_return, annual_return, max_drawdown, sharpe, win_rate...
            'orders_df': pd.DataFrame,  # 用于 OrderBasedEngine 的买卖订单
        }
        """

    def generate_orders(self, params: dict, start_date: str, end_date: str) -> pd.DataFrame:
        """
        加载数据 + 运行策略 + 返回 orders.csv 格式 DataFrame
        用于传给 my-stock OrderBasedEngine CLI
        """

    def get_today_signal(self, params: dict) -> dict:
        """
        读取 my_trend 最新数据，输出今日信号
        返回: {'action': 'buy'|'sell'|'hold', 'stock_code': str, 'reason': str}
        """
```

**n_positions 支持**：
- n_positions=1：单仓位（当前最优）
- n_positions=2~3：多仓位均分，降低集中度风险
- 持仓满额时不买入新股，有空仓时选 rank_surge 最高的补仓

### 3.3 参数优化引擎（heat/scripts/optimizer.py）

**两阶段优化**：

**Phase 1：网格搜索**（~200 组合，快速扫描）
```python
param_space = {
    'lookback':        [10, 15, 20, 30, 40],
    'buy_threshold':   [0.70, 0.75, 0.80, 0.85, 0.90],
    'sell_threshold':  [0.10, 0.15, 0.20, 0.25, 0.30],
    'min_deal_amount': [1e7, 3e7, 5e7, 1e8],
    'n_positions':     [1, 2, 3],
}
# lookback 缓存 heat_position 矩阵，避免重复计算
# 约 5×5×5×4×3 = 1500 组，启用 lookback 缓存后实际约 200 次完整运算
```

**Phase 2：AI代理模型优化**（基于 MLP，精细搜索）
- 复用 my-stock `optimizer_service.py` 的 AI 优化思路
- 在网格搜索最优区间内，用代理模型预测并选择下一批候选
- 约 50 次额外评估

**优化目标**：Sharpe ratio（主）+ 约束 max_drawdown < 35%
**输出**：`best_params.json` + `optimization_results.csv`

### 3.4 CLI 入口（heat/scripts/run_optimization.py）

```
步骤：
1. 加载数据（一次性）
2. Phase 1：网格搜索 → top 20 结果
3. Phase 2：AI优化 → 最终最优参数
4. 保存 best_params.json
5. 用最优参数生成 orders.csv
6. 调用 my-stock run_backtest.py CLI → 生成 OrderBasedEngine 报告
7. 打印报告路径
```

### 3.5 实盘信号（heat/scripts/live_signal.py）

```
每日运行时机：收盘后 17:10（popularity_rank 已更新）
流程：
1. 读取 my_trend.popularity_rank 最近 lookback+5 天数据
2. 计算 heat_position、rank_surge
3. 如果有持仓（从 state.json 读取）：
   - 检查持仓股是否触发卖出条件 → 输出 SELL 信号
4. 如果无持仓：
   - 扫描候选，选出信号 → 输出 BUY 信号
5. 更新 state.json（持仓状态）
6. 输出信号到 signals/YYYYMMDD_signal.json
```

### 3.6 my-stock optimizer_service.py 增强

新增 `run_callable_optimization()` 函数：
```python
def run_callable_optimization(
    strategy_fn: callable,       # fn(params: dict) -> {'sharpe': float, 'annual_return': float, ...}
    param_grid: dict,            # {'param_name': [val1, val2, ...], ...}
    metric: str = 'sharpe',      # 优化目标指标
    n_jobs: int = 1,             # 并行数（Windows 下建议 1）
) -> list[dict]:
```
这是通用接口，任何策略函数都可以接入优化框架。

---

## 4. 数据流

```
my_trend (port 3310)          my_stock (port 3307)
  popularity_rank               market_daily + adj_factor
        |                             |
        +-----------+  +--------------+
                    |  |
              data_loader.py
                    |
              strategy.py (HeatRotationStrategy)
                    |
             optimizer.py
                    |
         best_params.json + orders.csv
                    |
    [subprocess] run_backtest.py (my-stock CLI)
                    |
         backtest/output/{timestamp}_热度轮转优化/
                    |
           /report (前端查看)
```

---

## 5. 文件约束

- my-stock-research 写入库：无（热度策略不需要写入数据库，结果存文件）
- 报告输出目录：`F:/projects/my-stock-research/backtest/output/`（通过环境变量 REPORT_OUTPUT_DIR 配置）
- 实盘状态文件：`heat/scripts/state.json`

---

## 6. 实盘运行说明

策略可直接运行（不依赖任何 API 或框架）：
```bash
# 运行参数优化
cd F:/projects/my-stock-research
python heat/scripts/run_optimization.py --start 2025-03-15 --end 2026-03-19

# 每日实盘信号（收盘后运行）
python heat/scripts/live_signal.py

# 查看信号
cat heat/scripts/signals/$(date +%Y%m%d)_signal.json
```

---

## 7. 产出清单

| 产出 | 位置 |
|------|------|
| 策略类 | heat/scripts/strategy.py |
| 优化引擎 | heat/scripts/optimizer.py |
| 优化 CLI | heat/scripts/run_optimization.py |
| 实盘信号 | heat/scripts/live_signal.py |
| 最优参数 | heat/scripts/best_params.json |
| 优化结果 | heat/scripts/optimization_results.csv |
| 最终报告 | backtest/output/ → /report 前端 |
| 研究报告 | heat/report/ → 语雀 |
| my-stock 增强 | app/services/optimizer_service.py |
