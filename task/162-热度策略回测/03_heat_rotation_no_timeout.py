"""
热度轮转策略回测 v2 - 无超时版本（单仓位全市场轮转）

基于 02_heat_rotation_backtest.py 修改：
- MAX_HOLD_DAYS = 9999（取消超时强制平仓，仅在热度回升时卖出）
- 移除参数敏感性分析以加速执行

【回测逻辑详解】
==========================================
本策略模拟一个投资者，始终满仓持有1只股票，通过全市场扫描选择"热度最低"的股票买入，
热度回升后卖出，立即换入下一只最冷门的股票。资金不间断轮转。

一、数据基础
  - 热度排名：东财人气排名（popularity_rank），每日覆盖约5,490只股票
    rank=1为最热门，rank越大越冷门
  - 行情数据：前复权收盘价（qfq_close），用于计算真实收益率
  - 基准指数：沪深300

二、信号计算
  每日对每只股票计算"热度相对位置"（heat_position）：
    heat_position = (当前rank - 过去N日最低rank) / (过去N日最高rank - 过去N日最低rank)

    · heat_position ≈ 1.0 → 当前rank处于近期最高 → 热度处于近期最低 → 买入机会
    · heat_position ≈ 0.0 → 当前rank处于近期最低 → 热度处于近期最高 → 卖出时机

三、交易规则
  买入：
    1. 当前无持仓（初始状态或刚卖出）
    2. 全市场扫描，选出heat_position最高的候选股
    3. 候选股须满足：heat_position >= 买入阈值、日成交额 >= 最低流动性要求
    4. 信号当日收盘后确定，次日收盘价执行买入

  卖出：
    1. 持仓股heat_position <= 卖出阈值（热度已回升）
    2. 或持仓天数 >= 最大持仓天数（强制止损）
    3. 信号当日收盘后确定，次日收盘价执行卖出

  轮转：
    · 卖出信号和买入信号在同一天产生，次日同时执行
    · 即：次日收盘卖出旧股 + 次日收盘买入新股
    · 效果：资金几乎无空窗期

四、资金管理
  · 初始资金100万
  · 全仓操作，每次投入全部资金
  · 无手续费（简化处理）

五、业绩评价
  · 策略净值曲线 vs 沪深300净值曲线
  · 总收益、年化收益、最大回撤、夏普比率
  · 月度超额收益、胜率、交易明细
"""

import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text

# ============================================================
# 配置
# ============================================================

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3307
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'

# 策略参数
LOOKBACK = 20           # 热度位置回看窗口（天）
BUY_THRESHOLD = 0.80    # 买入阈值：heat_position >= 此值 → 热度处于近期低谷
SELL_THRESHOLD = 0.20   # 卖出阈值：heat_position <= 此值 → 热度已回升
MAX_HOLD_DAYS = 9999    # 无超时限制，仅在热度回升时卖出
MIN_DEAL_AMOUNT = 5e7   # 最低日成交额 5000万（过滤流动性差的小票）

START_DATE = '2025-03-15'
END_DATE = '2026-03-19'
INITIAL_CAPITAL = 1_000_000

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ============================================================
# 数据加载
# ============================================================

def get_engine(db_name):
    return create_engine(
        f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4',
        pool_pre_ping=True,
    )


def load_data():
    """一次性加载所有需要的数据"""
    logger.info("=" * 60)
    logger.info("加载数据...")
    logger.info("=" * 60)

    trend_engine = get_engine('my_trend')
    stock_engine = get_engine('my_stock')

    # 1. 热度排名数据（含成交额，用于流动性过滤）
    heat_df = pd.read_sql(f"""
        SELECT stock_code, date, `rank`, deal_amount
        FROM popularity_rank
        WHERE date >= '{START_DATE}' AND date <= '{END_DATE}'
    """, trend_engine)
    heat_df['date'] = pd.to_datetime(heat_df['date']).dt.date
    logger.info(f"  热度数据: {len(heat_df):,} 条, {heat_df['stock_code'].nunique()} 只股票")

    # 2. 交易日历
    td_df = pd.read_sql(f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1
          AND cal_date >= '{START_DATE.replace('-', '')}'
          AND cal_date <= '{END_DATE.replace('-', '')}'
        ORDER BY cal_date
    """, stock_engine)
    trading_days = sorted(pd.to_datetime(td_df['cal_date'], format='%Y%m%d').dt.date.tolist())
    logger.info(f"  交易日: {len(trading_days)} 天")

    # 3. 行情数据（前复权收盘价）
    price_df = pd.read_sql(f"""
        SELECT m.ts_code, m.trade_date, m.close, a.adj_factor
        FROM market_daily m
        JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        WHERE m.trade_date >= '{START_DATE.replace('-', '')}'
          AND m.trade_date <= '{END_DATE.replace('-', '')}'
    """, stock_engine)
    price_df['date'] = pd.to_datetime(price_df['trade_date'], format='%Y%m%d').dt.date
    price_df['stock_code'] = price_df['ts_code'].str[:6]
    # 前复权：当前价格不变，历史价格按复权因子调整
    latest_adj = price_df.groupby('stock_code')['adj_factor'].transform('last')
    price_df['qfq_close'] = price_df['close'] * price_df['adj_factor'] / latest_adj
    logger.info(f"  行情数据: {len(price_df):,} 条, {price_df['stock_code'].nunique()} 只股票")

    # 4. 沪深300指数
    index_df = pd.read_sql(f"""
        SELECT trade_date, close as index_close FROM index_daily
        WHERE ts_code = '000300.SH'
          AND trade_date >= '{START_DATE.replace('-', '')}'
          AND trade_date <= '{END_DATE.replace('-', '')}'
        ORDER BY trade_date
    """, stock_engine)
    index_df['date'] = pd.to_datetime(index_df['trade_date'], format='%Y%m%d').dt.date
    logger.info(f"  沪深300: {len(index_df)} 天")

    return heat_df, trading_days, price_df, index_df


# ============================================================
# 热度位置矩阵计算
# ============================================================

def compute_heat_position(heat_df, trading_days, lookback):
    """
    计算每只股票每天的热度相对位置（向量化）

    heat_position = (当前rank - N日最低rank) / (N日最高rank - N日最低rank)
    · 值接近1 → rank处于近期最高 → 热度处于近期最低 → 买入信号
    · 值接近0 → rank处于近期最低 → 热度处于近期最高 → 卖出信号

    rank_surge = 当前rank / 过去N日平均rank
    · 值越大 → 表示该股相对于自身常态冷了越多 → 用于候选排序
    · 例如：平时rank=100的股票，当前rank=300，surge=3.0（冷了3倍）
    """
    logger.info(f"计算热度位置矩阵 (lookback={lookback})...")

    td_set = set(trading_days)
    heat_td = heat_df[heat_df['date'].isin(td_set)].copy()

    # 透视表：行=日期, 列=股票代码, 值=rank
    rank_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='rank')
    rank_pivot = rank_pivot.sort_index()

    # 成交额透视表
    deal_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='deal_amount')
    deal_pivot = deal_pivot.sort_index()

    # 滚动最高rank和最低rank
    rolling_max = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).max()
    rolling_min = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).min()

    # heat_position = (当前rank - 最低rank) / (最高rank - 最低rank)
    range_val = rolling_max - rolling_min
    range_val = range_val.replace(0, np.nan)  # 避免除以零
    heat_position = (rank_pivot - rolling_min) / range_val

    # rank_surge = 当前rank / 滚动均值rank（用于候选排序）
    # 值越大说明该股相对自身常态"冷"了越多，选股时优先选surge最大的
    rolling_mean = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).mean()
    rolling_mean = rolling_mean.replace(0, np.nan)
    rank_surge = rank_pivot / rolling_mean

    logger.info(f"  矩阵大小: {heat_position.shape[0]} 天 × {heat_position.shape[1]} 只股票")

    return heat_position, rank_pivot, deal_pivot, rank_surge


# ============================================================
# 单仓位轮转回测
# ============================================================

def simulate_rotation(heat_position, rank_pivot, deal_pivot, rank_surge,
                      price_df, trading_days,
                      index_df, buy_threshold, sell_threshold, max_hold_days,
                      min_deal_amount):
    """
    单仓位轮转回测引擎

    逐日模拟：
    1. 记录当日净值
    2. 检查持仓是否触发卖出条件
    3. 如果无持仓，全市场选最冷门的股票买入
    4. 执行买卖（次日收盘价）

    选股标准：
    - heat_position >= 阈值 作为入选条件
    - rank_surge（当前rank/近期均值rank）作为排序标准
    - rank_surge越大 → 该股相对自身常态"冷"了越多 → 优先选择
    - 这样自然偏好"平时热门、突然变冷"的股票（符合均值回归逻辑）
    """
    logger.info(f"开始轮转回测 (buy={buy_threshold}, sell={sell_threshold}, "
                f"max_hold={max_hold_days}, min_deal={min_deal_amount/1e8:.1f}亿)...")

    # 构建价格查询表: (stock_code, date) -> {qfq_close, close}
    price_lookup = {
        (row['stock_code'], row['date']): {'qfq_close': row['qfq_close'], 'close': row['close']}
        for _, row in price_df[['stock_code', 'date', 'qfq_close', 'close']].iterrows()
    }

    # 交易日列表和索引映射
    td_list = sorted(trading_days)
    td_map = {d: i for i, d in enumerate(td_list)}

    # 沪深300查询表
    idx_lookup = index_df.set_index('date')['index_close'].to_dict()

    # ---- 状态初始化 ----
    trades = []           # 交易记录
    equity_records = []   # 日净值记录
    capital = INITIAL_CAPITAL  # 可用资金（非持仓时）

    holding = False
    hold_code = None          # 持仓股票代码
    hold_entry_date = None    # 入场日期
    hold_entry_qfq = None     # 入场前复权价
    hold_entry_actual = None  # 入场实际价
    hold_start_idx = None     # 入场交易日索引
    position_capital = None   # 入场时投入的资金

    last_equity = INITIAL_CAPITAL
    no_candidate_days = 0     # 找不到候选的天数

    for i, today in enumerate(td_list):
        # ---- 1. 记录当日净值 ----
        if holding:
            p = price_lookup.get((hold_code, today))
            if p:
                current_equity = position_capital * (p['qfq_close'] / hold_entry_qfq)
                last_equity = current_equity
            else:
                current_equity = last_equity  # 停牌，沿用上一日
        else:
            current_equity = capital

        equity_records.append({
            'date': today,
            'equity': current_equity,
            'index_close': idx_lookup.get(today, np.nan),
            'holding': hold_code if holding else None,
        })

        # 预热期：跳过前LOOKBACK天（无法计算heat_position）
        if today not in heat_position.index:
            continue

        # 检查是否有次日（用于执行交易）
        if i + 1 >= len(td_list):
            # 期末：强制平仓
            if holding:
                p = price_lookup.get((hold_code, today))
                if p:
                    ret = (p['qfq_close'] - hold_entry_qfq) / hold_entry_qfq
                    capital = position_capital * (1 + ret)
                    trades.append({
                        'stock_code': hold_code,
                        'entry_date': hold_entry_date,
                        'exit_date': today,
                        'entry_price': hold_entry_actual,
                        'exit_price': p['close'],
                        'return': ret,
                        'hold_days': i - hold_start_idx,
                        'exit_reason': 'end_of_period',
                    })
                    holding = False
            break

        next_td = td_list[i + 1]

        # ---- 2. 卖出判断 ----
        should_sell = False
        sell_reason = None

        if holding:
            # 获取持仓股今日的heat_position
            hp_today = np.nan
            if hold_code in heat_position.columns:
                hp_today = heat_position.loc[today, hold_code]

            hold_days = i - hold_start_idx

            if not np.isnan(hp_today) and hp_today <= sell_threshold:
                should_sell = True
                sell_reason = 'heat_recovered'
            elif hold_days >= max_hold_days:
                should_sell = True
                sell_reason = 'max_hold'

        # ---- 3. 执行卖出 ----
        if should_sell:
            exit_price = price_lookup.get((hold_code, next_td))
            if exit_price:
                ret = (exit_price['qfq_close'] - hold_entry_qfq) / hold_entry_qfq
                capital = position_capital * (1 + ret)

                trades.append({
                    'stock_code': hold_code,
                    'entry_date': hold_entry_date,
                    'exit_date': next_td,
                    'entry_price': hold_entry_actual,
                    'exit_price': exit_price['close'],
                    'return': ret,
                    'hold_days': td_map[next_td] - hold_start_idx,
                    'exit_reason': sell_reason,
                })
                holding = False
                hold_code = None
            # 如果次日停牌无法卖出，保持持仓，下一日重试

        # ---- 4. 买入判断 ----
        if not holding:
            # 获取今日所有股票的 heat_position
            hp_series = heat_position.loc[today].dropna()

            # 条件1: heat_position >= 买入阈值
            candidates = hp_series[hp_series >= buy_threshold]

            if len(candidates) == 0:
                no_candidate_days += 1
                continue

            # 条件2: 成交额 >= 最低流动性要求（向量化）
            if min_deal_amount > 0 and today in deal_pivot.index:
                deal_today = deal_pivot.loc[today].reindex(candidates.index)
                candidates = candidates[deal_today.fillna(0) >= min_deal_amount]

            if len(candidates) == 0:
                no_candidate_days += 1
                continue

            # 条件3: 次日有价格数据（未停牌）- 使用集合查找加速
            valid_codes = [c for c in candidates.index if (c, next_td) in price_lookup]
            if not valid_codes:
                no_candidate_days += 1
                continue
            candidates = candidates.loc[valid_codes]

            # ---- 5. 选股：rank_surge 最高的（相对自身常态冷了最多的） ----
            # rank_surge = 当前rank / 近期均值rank，越大说明偏离常态越远
            # 例如：平时rank=100突然到rank=300 → surge=3.0
            # 这样自然偏好"平时热门、突然变冷"的股票（均值回归弹性大）
            if today in rank_surge.index:
                surge_today = rank_surge.loc[today].reindex(candidates.index).dropna()
                if len(surge_today) > 0:
                    best_code = surge_today.idxmax()
                else:
                    best_code = candidates.idxmax()
            else:
                best_code = candidates.idxmax()

            # ---- 6. 执行买入 ----
            buy_price = price_lookup[(best_code, next_td)]
            holding = True
            hold_code = best_code
            hold_entry_date = next_td
            hold_entry_qfq = buy_price['qfq_close']
            hold_entry_actual = buy_price['close']
            hold_start_idx = td_map[next_td]
            position_capital = capital

    trade_df = pd.DataFrame(trades) if trades else pd.DataFrame()
    equity_df = pd.DataFrame(equity_records)

    logger.info(f"  总交易笔数: {len(trade_df)}")
    logger.info(f"  无候选天数: {no_candidate_days}")
    logger.info(f"  最终资金: {capital:,.0f}")

    return trade_df, equity_df, capital


# ============================================================
# 绩效指标计算
# ============================================================

def compute_metrics(trade_df, equity_df):
    """计算策略和基准的各项绩效指标"""
    if len(trade_df) == 0 or len(equity_df) == 0:
        return {}

    eq = equity_df.copy()
    eq = eq[eq['equity'] > 0].copy()

    # 策略净值
    eq['nav'] = eq['equity'] / INITIAL_CAPITAL
    eq['daily_return'] = eq['nav'].pct_change().fillna(0)

    # 沪深300净值
    first_idx = eq[eq['index_close'].notna()].iloc[0]['index_close']
    eq['index_nav'] = eq['index_close'] / first_idx
    eq['index_daily_return'] = eq['index_nav'].pct_change().fillna(0)

    # 超额收益（日度）
    eq['excess_daily'] = eq['daily_return'] - eq['index_daily_return']

    # 总天数
    n_days = len(eq)

    # ---- 策略指标 ----
    total_return = (eq['nav'].iloc[-1] - 1) * 100
    annual_return = ((eq['nav'].iloc[-1]) ** (250 / n_days) - 1) * 100

    # 最大回撤
    peak = eq['nav'].expanding().max()
    drawdown = (eq['nav'] - peak) / peak
    max_drawdown = drawdown.min() * 100

    # 夏普比率（年化，无风险利率2%）
    rf_daily = 0.02 / 250
    daily_excess = eq['daily_return'] - rf_daily
    sharpe = (daily_excess.mean() / daily_excess.std() * np.sqrt(250)) if daily_excess.std() > 0 else 0

    # ---- 基准指标 ----
    idx_nav = eq['index_nav'].dropna()
    index_return = (idx_nav.iloc[-1] - 1) * 100
    index_annual = ((idx_nav.iloc[-1]) ** (250 / n_days) - 1) * 100

    idx_peak = idx_nav.expanding().max()
    idx_dd = (idx_nav - idx_peak) / idx_peak
    idx_max_dd = idx_dd.min() * 100

    # ---- 交易统计 ----
    total_trades = len(trade_df)
    wins = len(trade_df[trade_df['return'] > 0])
    losses = len(trade_df[trade_df['return'] < 0])
    win_rate = wins / total_trades * 100
    avg_return = trade_df['return'].mean() * 100
    avg_hold = trade_df['hold_days'].mean()
    avg_win = trade_df[trade_df['return'] > 0]['return'].mean() * 100 if wins > 0 else 0
    avg_loss = trade_df[trade_df['return'] < 0]['return'].mean() * 100 if losses > 0 else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')

    return {
        'total_return': total_return,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'sharpe': sharpe,
        'index_return': index_return,
        'index_annual': index_annual,
        'index_max_dd': idx_max_dd,
        'excess_total': total_return - index_return,
        'excess_annual': annual_return - index_annual,
        'total_trades': total_trades,
        'wins': wins,
        'losses': losses,
        'win_rate': win_rate,
        'avg_return': avg_return,
        'avg_hold': avg_hold,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'profit_factor': profit_factor,
    }


# ============================================================
# 报告生成
# ============================================================

def build_report(trade_df, equity_df, metrics, params):
    """生成详细的 Markdown 回测报告"""
    lookback, buy_th, sell_th, max_hold, min_deal = params
    lines = []

    lines.append("# 热度轮转策略回测报告（单仓位全市场轮转）")
    lines.append(f"**创建时间**: {datetime.now().strftime('%Y%m%d %H:%M')}")
    lines.append("")

    # ---- 回测逻辑说明 ----
    lines.append("## 一、回测逻辑说明")
    lines.append("")
    lines.append("### 1.1 策略思路")
    lines.append("利用东财人气排名的均值回归特性：当一只股票的热度跌到近期最低点时买入，")
    lines.append("等热度回升后卖出，然后立即从全市场选出下一只\"最冷门\"的股票买入。")
    lines.append("**资金始终满仓运转，没有空窗期。**")
    lines.append("")
    lines.append("### 1.2 交易流程")
    lines.append("```")
    lines.append("每个交易日收盘后：")
    lines.append("  1. 如果持仓中 → 检查是否触发卖出条件")
    lines.append(f"     - 条件A: 持仓股的heat_position <= {sell_th}（热度已回升到近期高点）")
    lines.append(f"     - 条件B: 持仓天数 >= {max_hold}天（超时强制平仓）")
    lines.append("  2. 如果无持仓（初始/刚卖出） → 全市场选股")
    lines.append(f"     - 筛选heat_position >= {buy_th}的股票（热度处于近期低谷）")
    lines.append(f"     - 过滤日成交额 < {min_deal/1e8:.1f}亿的小票")
    lines.append("     - 在候选中选heat_position最高的（最冷门的）")
    lines.append("  3. 次日收盘价执行买入/卖出")
    lines.append("     - 卖出和买入可以同日执行（卖旧买新）")
    lines.append("```")
    lines.append("")
    lines.append("### 1.3 heat_position 计算方法")
    lines.append(f"- 取过去{lookback}个交易日的热度排名（rank）")
    lines.append("- heat_position = (当前rank - N日最低rank) / (N日最高rank - N日最低rank)")
    lines.append("- 值域 [0, 1]，越接近1表示热度越冷（rank越大=越不受关注）")
    lines.append("")
    lines.append("### 1.4 资金管理")
    lines.append(f"- 初始资金: {INITIAL_CAPITAL/1e4:.0f}万")
    lines.append("- 每次全仓买入1只股票")
    lines.append("- 卖出后全部资金立即投入下一只")
    lines.append("- 未计入交易手续费")
    lines.append("")

    if not metrics:
        lines.append("## 回测无数据\n")
        return "\n".join(lines)

    # ---- 策略 vs 基准 ----
    lines.append("## 二、策略 vs 沪深300 业绩对比")
    lines.append("")
    lines.append("| 指标 | 策略 | 沪深300 | 超额 |")
    lines.append("|------|------|---------|------|")
    lines.append(f"| **总收益率** | **{metrics['total_return']:.2f}%** | {metrics['index_return']:.2f}% | "
                 f"**{metrics['excess_total']:.2f}%** |")
    lines.append(f"| **年化收益率** | **{metrics['annual_return']:.2f}%** | {metrics['index_annual']:.2f}% | "
                 f"**{metrics['excess_annual']:.2f}%** |")
    lines.append(f"| 最大回撤 | {metrics['max_drawdown']:.2f}% | {metrics['index_max_dd']:.2f}% | - |")
    lines.append(f"| 夏普比率 | {metrics['sharpe']:.2f} | - | - |")
    lines.append("")

    # ---- 交易统计 ----
    lines.append("## 三、交易统计")
    lines.append("")
    lines.append("| 指标 | 值 |")
    lines.append("|------|-----|")
    lines.append(f"| 总交易笔数 | {metrics['total_trades']} |")
    lines.append(f"| 盈利 / 亏损 | {metrics['wins']} / {metrics['losses']} |")
    lines.append(f"| **胜率** | **{metrics['win_rate']:.1f}%** |")
    lines.append(f"| **每笔平均收益** | **{metrics['avg_return']:.2f}%** |")
    lines.append(f"| 平均盈利 / 平均亏损 | {metrics['avg_win']:.2f}% / {metrics['avg_loss']:.2f}% |")
    lines.append(f"| **盈亏比** | **{metrics['profit_factor']:.2f}** |")
    lines.append(f"| 平均持仓天数 | {metrics['avg_hold']:.1f} |")
    lines.append("")

    # ---- 按退出原因 ----
    if len(trade_df) > 0:
        lines.append("## 四、按退出原因分析")
        lines.append("")
        lines.append("| 退出原因 | 笔数 | 胜率 | 平均收益 | 平均持仓天数 |")
        lines.append("|---------|------|------|---------|------------|")
        for reason, label in [('heat_recovered', '热度回升'), ('max_hold', '超时平仓'),
                              ('end_of_period', '期末平仓')]:
            sub = trade_df[trade_df['exit_reason'] == reason]
            if len(sub) > 0:
                wr = len(sub[sub['return'] > 0]) / len(sub) * 100
                ar = sub['return'].mean() * 100
                ah = sub['hold_days'].mean()
                lines.append(f"| {label} | {len(sub)} | {wr:.1f}% | {ar:.2f}% | {ah:.1f} |")
        lines.append("")

    # ---- 月度业绩 ----
    if len(equity_df) > 0:
        lines.append("## 五、月度业绩（策略 vs 沪深300）")
        lines.append("")
        eq = equity_df.copy()
        eq['nav'] = eq['equity'] / INITIAL_CAPITAL
        first_idx = eq[eq['index_close'].notna()].iloc[0]['index_close']
        eq['index_nav'] = eq['index_close'] / first_idx
        eq['month'] = pd.to_datetime(eq['date']).dt.to_period('M')

        monthly_data = []
        for month, group in eq.groupby('month'):
            s_start = group['nav'].iloc[0]
            s_end = group['nav'].iloc[-1]
            s_ret = (s_end / s_start - 1) * 100

            i_vals = group['index_nav'].dropna()
            if len(i_vals) >= 2:
                i_start = i_vals.iloc[0]
                i_end = i_vals.iloc[-1]
                i_ret = (i_end / i_start - 1) * 100
            else:
                i_ret = np.nan

            monthly_data.append({'month': str(month), 's_ret': s_ret, 'i_ret': i_ret})

        lines.append("| 月份 | 策略收益 | 沪深300 | 超额收益 |")
        lines.append("|------|---------|---------|---------|")
        win_months = 0
        total_months = 0
        for m in monthly_data:
            excess = m['s_ret'] - m['i_ret'] if not np.isnan(m['i_ret']) else np.nan
            i_str = f"{m['i_ret']:.2f}%" if not np.isnan(m['i_ret']) else "-"
            e_str = f"{excess:.2f}%" if not np.isnan(excess) else "-"
            lines.append(f"| {m['month']} | {m['s_ret']:.2f}% | {i_str} | {e_str} |")
            if not np.isnan(excess):
                total_months += 1
                if excess > 0:
                    win_months += 1

        if total_months > 0:
            lines.append(f"\n月度跑赢大盘比例: **{win_months}/{total_months}** ({win_months/total_months*100:.0f}%)")
        lines.append("")

    # ---- 交易明细 ----
    if len(trade_df) > 0:
        lines.append("## 六、全部交易明细")
        lines.append("")
        lines.append("| # | 股票 | 买入日期 | 卖出日期 | 持仓天数 | 收益率 | 退出原因 |")
        lines.append("|---|------|---------|---------|---------|-------|---------|")
        for idx, row in trade_df.iterrows():
            ret_str = f"{row['return']*100:+.2f}%"
            reason_map = {'heat_recovered': '热度回升', 'max_hold': '超时', 'end_of_period': '期末'}
            reason = reason_map.get(row['exit_reason'], row['exit_reason'])
            lines.append(f"| {idx+1} | {row['stock_code']} | {row['entry_date']} | "
                         f"{row['exit_date']} | {row['hold_days']} | {ret_str} | {reason} |")
        lines.append("")

    # ---- 参数 ----
    lines.append("## 七、回测参数")
    lines.append("")
    lines.append("| 参数 | 值 | 说明 |")
    lines.append("|------|-----|------|")
    lines.append(f"| LOOKBACK | {lookback} | heat_position回看窗口（天） |")
    lines.append(f"| BUY_THRESHOLD | {buy_th} | 买入阈值 |")
    lines.append(f"| SELL_THRESHOLD | {sell_th} | 卖出阈值 |")
    lines.append(f"| MAX_HOLD_DAYS | {max_hold} | 最大持仓天数 |")
    lines.append(f"| MIN_DEAL_AMOUNT | {min_deal/1e8:.1f}亿 | 最低日成交额 |")
    lines.append(f"| 回测区间 | {START_DATE} ~ {END_DATE} | |")
    lines.append("")

    return "\n".join(lines)


# ============================================================
# 参数敏感性分析
# ============================================================

def sensitivity_analysis(heat_df, price_df, trading_days, index_df):
    """对关键参数进行网格搜索"""
    logger.info("=" * 60)
    logger.info("参数敏感性分析...")
    logger.info("=" * 60)

    param_combos = [
        # (lookback, buy_threshold, sell_threshold, max_hold, min_deal_amount)
        (10, 0.80, 0.20, 15, 5e7),
        (20, 0.80, 0.20, 15, 5e7),   # 默认
        (30, 0.80, 0.20, 15, 5e7),
        (20, 0.90, 0.10, 15, 5e7),   # 更极端信号
        (20, 0.70, 0.30, 15, 5e7),   # 更宽松信号
        (20, 0.80, 0.20, 10, 5e7),   # 更短持仓
        (20, 0.80, 0.20, 5,  5e7),   # 超短持仓
        (20, 0.80, 0.20, 15, 1e7),   # 更低流动性门槛
        (20, 0.80, 0.20, 15, 1e8),   # 更高流动性门槛
        (30, 0.90, 0.10, 10, 5e7),   # 极端信号+短持仓
    ]

    results = []
    # 缓存不同lookback的heat_position
    hp_cache = {}

    for lb, bt, st, mh, md in param_combos:
        logger.info(f"  测试: lookback={lb}, buy={bt}, sell={st}, hold={mh}, deal={md/1e8:.1f}亿")

        if lb not in hp_cache:
            hp, rp, dp, rs = compute_heat_position(heat_df, trading_days, lb)
            hp_cache[lb] = (hp, rp, dp, rs)
        else:
            hp, rp, dp, rs = hp_cache[lb]

        trade_df, equity_df, final_capital = simulate_rotation(
            hp, rp, dp, rs, price_df, trading_days, index_df, bt, st, mh, md
        )

        if len(trade_df) == 0:
            continue

        m = compute_metrics(trade_df, equity_df)
        if not m:
            continue

        results.append({
            'lookback': lb, 'buy_th': bt, 'sell_th': st,
            'max_hold': mh, 'min_deal': md,
            'total_return': m['total_return'],
            'annual_return': m['annual_return'],
            'max_drawdown': m['max_drawdown'],
            'sharpe': m['sharpe'],
            'excess_total': m['excess_total'],
            'trades': m['total_trades'],
            'win_rate': m['win_rate'],
            'avg_return': m['avg_return'],
            'avg_hold': m['avg_hold'],
        })

    return pd.DataFrame(results)


# ============================================================
# 主函数
# ============================================================

def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("热度轮转策略回测 v2 - 无超时版本")
    logger.info("=" * 60)

    # 1. 加载数据
    heat_df, trading_days, price_df, index_df = load_data()

    # 2. 计算热度位置
    heat_position, rank_pivot, deal_pivot, rank_surge = compute_heat_position(
        heat_df, trading_days, LOOKBACK
    )

    # 3. 默认参数回测
    logger.info("=" * 60)
    logger.info("默认参数回测...")
    logger.info("=" * 60)

    trade_df, equity_df, final_capital = simulate_rotation(
        heat_position, rank_pivot, deal_pivot, rank_surge,
        price_df, trading_days, index_df,
        BUY_THRESHOLD, SELL_THRESHOLD, MAX_HOLD_DAYS, MIN_DEAL_AMOUNT
    )

    if len(trade_df) == 0:
        logger.error("回测无交易产生，请检查数据和参数")
        return

    # 4. 计算绩效
    metrics = compute_metrics(trade_df, equity_df)
    logger.info(f"  策略总收益: {metrics['total_return']:.2f}%")
    logger.info(f"  沪深300: {metrics['index_return']:.2f}%")
    logger.info(f"  超额: {metrics['excess_total']:.2f}%")

    # 5. 生成报告（跳过参数敏感性分析以加速执行）
    report = build_report(
        trade_df, equity_df, metrics,
        (LOOKBACK, BUY_THRESHOLD, SELL_THRESHOLD, MAX_HOLD_DAYS, MIN_DEAL_AMOUNT)
    )

    # 6. 保存结果
    output_dir = os.path.dirname(os.path.abspath(__file__))

    report_path = os.path.join(output_dir, '热度轮转策略回测报告_无超时.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"报告: {report_path}")

    trade_path = os.path.join(output_dir, 'rotation_trades_no_timeout.csv')
    trade_df.to_csv(trade_path, index=False, encoding='utf-8-sig')
    logger.info(f"交易明细: {trade_path}")

    equity_path = os.path.join(output_dir, 'rotation_equity_no_timeout.csv')
    equity_df.to_csv(equity_path, index=False, encoding='utf-8-sig')
    logger.info(f"净值曲线: {equity_path}")

    elapsed = time.time() - t0
    logger.info(f"全部完成，耗时 {elapsed:.1f} 秒")


if __name__ == '__main__':
    main()
