"""
平台突破策略回测 — 新入场条件 + 热度回升出场

【策略思路】
选择处于低波动平台整理中、连续下跌、且热度较冷的股票买入，
等待热度回升后卖出。核心假设：低波动+冷门+短期回调 = 蓄势待发。

【入场条件（三条同时满足）】
  1. 20日ATR/close < 0.03  — 低波动平台整理期
  2. 连续3天收盘价下跌     — 短期回调
  3. 热度rank > 2000        — 偏冷门（不在最热的2000只之内）

【选股排序】
  rank_surge = 当前rank / 20日均值rank，选surge最大的（偏离常态最远）

【出场条件】
  heat_position <= 0.20（热度回升到近期高位）
  无超时（MAX_HOLD_DAYS = 9999）
  期末强制平仓

【数据来源】
  - my_stock: market_daily + adj_factor（前复权close/high/low）, trade_cal, index_daily
  - my_trend: popularity_rank（rank, deal_amount）
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
LOOKBACK = 20             # 热度位置回看窗口（天）
ATR_WINDOW = 20           # ATR计算窗口（天）
ATR_THRESHOLD = 0.03      # ATR/close 阈值：< 此值 = 低波动平台
CONSEC_DOWN_DAYS = 3      # 连续下跌天数
RANK_COLD_THRESHOLD = 2000  # rank > 此值 = 偏冷门
SELL_THRESHOLD = 0.20     # 卖出阈值：heat_position <= 此值 → 热度已回升
MAX_HOLD_DAYS = 9999      # 无超时限制
MIN_DEAL_AMOUNT = 5e7     # 最低日成交额 5000万

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
    """一次性加载所有需要的数据（含 high/low 用于ATR计算）"""
    logger.info("=" * 60)
    logger.info("加载数据...")
    logger.info("=" * 60)

    trend_engine = get_engine('my_trend')
    stock_engine = get_engine('my_stock')

    # 1. 热度排名数据（含成交额）
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

    # 3. 行情数据（前复权 close/high/low）
    price_df = pd.read_sql(f"""
        SELECT m.ts_code, m.trade_date, m.close, m.high, m.low, a.adj_factor
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
    price_df['qfq_high'] = price_df['high'] * price_df['adj_factor'] / latest_adj
    price_df['qfq_low'] = price_df['low'] * price_df['adj_factor'] / latest_adj
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
# 信号矩阵计算
# ============================================================

def compute_signals(heat_df, trading_days, price_df, lookback, atr_window):
    """
    计算所有信号矩阵（向量化）：
    - heat_position: 热度相对位置（用于出场判断）
    - rank_pivot: 原始rank值（用于 rank > 2000 过滤）
    - deal_pivot: 成交额（用于流动性过滤）
    - rank_surge: 当前rank / 20日均值rank（用于选股排序）
    - atr_ratio_pivot: ATR/close（用于低波动过滤）
    - close_pivot: 前复权收盘价（用于连续下跌判断）
    """
    logger.info(f"计算信号矩阵 (lookback={lookback}, atr_window={atr_window})...")

    td_set = set(trading_days)
    heat_td = heat_df[heat_df['date'].isin(td_set)].copy()

    # ---- 热度相关矩阵 ----
    rank_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='rank')
    rank_pivot = rank_pivot.sort_index()

    deal_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='deal_amount')
    deal_pivot = deal_pivot.sort_index()

    # heat_position = (当前rank - N日最低rank) / (N日最高rank - N日最低rank)
    rolling_max = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).max()
    rolling_min = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).min()
    range_val = rolling_max - rolling_min
    range_val = range_val.replace(0, np.nan)
    heat_position = (rank_pivot - rolling_min) / range_val

    # rank_surge = 当前rank / 滚动均值rank
    rolling_mean = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).mean()
    rolling_mean = rolling_mean.replace(0, np.nan)
    rank_surge = rank_pivot / rolling_mean

    logger.info(f"  热度矩阵: {heat_position.shape[0]} 天 × {heat_position.shape[1]} 只股票")

    # ---- 行情相关矩阵 ----
    price_td = price_df[price_df['date'].isin(td_set)].copy()

    close_pivot = price_td.pivot_table(index='date', columns='stock_code', values='qfq_close')
    close_pivot = close_pivot.sort_index()

    high_pivot = price_td.pivot_table(index='date', columns='stock_code', values='qfq_high')
    high_pivot = high_pivot.sort_index()

    low_pivot = price_td.pivot_table(index='date', columns='stock_code', values='qfq_low')
    low_pivot = low_pivot.sort_index()

    # ATR = 20日滚动平均 of (high - low)
    true_range = high_pivot - low_pivot
    atr = true_range.rolling(window=atr_window, min_periods=atr_window // 2).mean()

    # ATR ratio = ATR / close
    atr_ratio_pivot = atr / close_pivot
    atr_ratio_pivot = atr_ratio_pivot.replace([np.inf, -np.inf], np.nan)

    logger.info(f"  行情矩阵: {close_pivot.shape[0]} 天 × {close_pivot.shape[1]} 只股票")

    return heat_position, rank_pivot, deal_pivot, rank_surge, atr_ratio_pivot, close_pivot


# ============================================================
# 单仓位轮转回测
# ============================================================

def simulate_rotation(heat_position, rank_pivot, deal_pivot, rank_surge,
                      atr_ratio_pivot, close_pivot,
                      price_df, trading_days, index_df,
                      sell_threshold, max_hold_days, min_deal_amount):
    """
    单仓位轮转回测引擎 — 平台入场 + 热度回升出场

    入场条件（三条同时满足）:
      1. ATR/close < 0.03 → 低波动平台整理
      2. 连续3天下跌 → 短期回调
      3. rank > 2000 → 偏冷门

    选股: rank_surge最大的（偏离自身常态最远）

    出场条件:
      - heat_position <= 0.20 → 热度已回升
      - 无超时
    """
    logger.info(f"开始轮转回测 (ATR<{ATR_THRESHOLD}, {CONSEC_DOWN_DAYS}连跌, "
                f"rank>{RANK_COLD_THRESHOLD}, sell={sell_threshold}, "
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
    trades = []
    equity_records = []
    capital = INITIAL_CAPITAL

    holding = False
    hold_code = None
    hold_entry_date = None
    hold_entry_qfq = None
    hold_entry_actual = None
    hold_start_idx = None
    position_capital = None

    last_equity = INITIAL_CAPITAL
    no_candidate_days = 0

    for i, today in enumerate(td_list):
        # ---- 1. 记录当日净值 ----
        if holding:
            p = price_lookup.get((hold_code, today))
            if p:
                current_equity = position_capital * (p['qfq_close'] / hold_entry_qfq)
                last_equity = current_equity
            else:
                current_equity = last_equity
        else:
            current_equity = capital

        equity_records.append({
            'date': today,
            'equity': current_equity,
            'index_close': idx_lookup.get(today, np.nan),
            'holding': hold_code if holding else None,
        })

        # 预热期：需要heat_position数据可用
        if today not in heat_position.index:
            continue

        # 检查是否有次日
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

        # ---- 4. 买入判断（新入场三条件） ----
        if not holding:
            # === 条件1: ATR/close < ATR_THRESHOLD（低波动平台整理） ===
            if today not in atr_ratio_pivot.index:
                no_candidate_days += 1
                continue
            atr_today = atr_ratio_pivot.loc[today].dropna()
            low_vol_stocks = set(atr_today[atr_today < ATR_THRESHOLD].index)

            if len(low_vol_stocks) == 0:
                no_candidate_days += 1
                continue

            # === 条件2: 连续3天下跌 ===
            # 需要今天和前3天的close数据（共4天），判断3次连续下跌
            today_idx_in_close = None
            if today in close_pivot.index:
                close_dates = close_pivot.index.tolist()
                today_pos = close_dates.index(today)
                if today_pos >= CONSEC_DOWN_DAYS:
                    # 取最近4天的close
                    day0 = close_dates[today_pos - 3]  # 3天前
                    day1 = close_dates[today_pos - 2]  # 2天前
                    day2 = close_dates[today_pos - 1]  # 昨天
                    day3 = close_dates[today_pos]       # 今天

                    c0 = close_pivot.loc[day0]
                    c1 = close_pivot.loc[day1]
                    c2 = close_pivot.loc[day2]
                    c3 = close_pivot.loc[day3]

                    # 连续3天下跌: c3 < c2 < c1 < c0
                    consec_down = (c3 < c2) & (c2 < c1) & (c1 < c0)
                    down_stocks = set(consec_down[consec_down].index)
                else:
                    down_stocks = set()
            else:
                down_stocks = set()

            if len(down_stocks) == 0:
                no_candidate_days += 1
                continue

            # === 条件3: rank > 2000（偏冷门） ===
            if today not in rank_pivot.index:
                no_candidate_days += 1
                continue
            rank_today = rank_pivot.loc[today].dropna()
            cold_stocks = set(rank_today[rank_today > RANK_COLD_THRESHOLD].index)

            if len(cold_stocks) == 0:
                no_candidate_days += 1
                continue

            # === 三条件交集 ===
            candidates_set = low_vol_stocks & down_stocks & cold_stocks

            if len(candidates_set) == 0:
                no_candidate_days += 1
                continue

            candidates_idx = pd.Index(list(candidates_set))

            # === 流动性过滤: deal_amount >= MIN_DEAL_AMOUNT ===
            if min_deal_amount > 0 and today in deal_pivot.index:
                deal_today = deal_pivot.loc[today].reindex(candidates_idx)
                candidates_idx = candidates_idx[deal_today.fillna(0) >= min_deal_amount]

            if len(candidates_idx) == 0:
                no_candidate_days += 1
                continue

            # === 次日有价格数据（未停牌） ===
            valid_codes = [c for c in candidates_idx if (c, next_td) in price_lookup]
            if not valid_codes:
                no_candidate_days += 1
                continue
            candidates_idx = pd.Index(valid_codes)

            # ---- 5. 选股：rank_surge 最高的 ----
            if today in rank_surge.index:
                surge_today = rank_surge.loc[today].reindex(candidates_idx).dropna()
                if len(surge_today) > 0:
                    best_code = surge_today.idxmax()
                else:
                    # fallback: 选rank最大的
                    best_code = rank_today.reindex(candidates_idx).idxmax()
            else:
                best_code = rank_today.reindex(candidates_idx).idxmax()

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
    losses = len(trade_df[trade_df['return'] <= 0])
    win_rate = wins / total_trades * 100
    avg_return = trade_df['return'].mean() * 100
    avg_hold = trade_df['hold_days'].mean()
    avg_win = trade_df[trade_df['return'] > 0]['return'].mean() * 100 if wins > 0 else 0
    avg_loss = trade_df[trade_df['return'] <= 0]['return'].mean() * 100 if losses > 0 else 0
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

def build_report(trade_df, equity_df, metrics):
    """生成详细的 Markdown 回测报告"""
    lines = []

    lines.append("# 平台突破策略回测报告（新入场+热度回升出场）")
    lines.append(f"**创建时间**: {datetime.now().strftime('%Y%m%d %H:%M')}")
    lines.append("")

    # ---- 策略说明 ----
    lines.append("## 一、策略说明")
    lines.append("")
    lines.append("### 1.1 策略思路")
    lines.append("选择处于 **低波动平台整理** 中、**连续短期下跌**、且 **热度偏冷** 的股票买入，")
    lines.append("等待热度回升后卖出。核心假设：低波动+冷门+短期回调 = 蓄势待发的均值回归机会。")
    lines.append("")
    lines.append("与之前的热度轮转策略（v2无超时版）的主要区别：")
    lines.append("- **旧入场**: heat_position >= 0.80（纯热度信号）")
    lines.append("- **新入场**: ATR/close < 0.03 + 连续3天下跌 + rank > 2000（技术面+热度面复合信号）")
    lines.append("- **出场条件相同**: heat_position <= 0.20（热度回升），无超时")
    lines.append("")
    lines.append("### 1.2 入场条件（三条件同时满足）")
    lines.append("")
    lines.append("| # | 条件 | 参数 | 含义 |")
    lines.append("|---|------|------|------|")
    lines.append(f"| 1 | 20日ATR/close < {ATR_THRESHOLD} | ATR = 20日rolling avg(high-low) | 低波动平台整理期 |")
    lines.append(f"| 2 | 连续{CONSEC_DOWN_DAYS}天收盘价下跌 | close[t] < close[t-1] < ... | 短期回调 |")
    lines.append(f"| 3 | 热度rank > {RANK_COLD_THRESHOLD} | 东财人气排名 | 偏冷门股 |")
    lines.append("")
    lines.append("### 1.3 交易流程")
    lines.append("```")
    lines.append("每个交易日收盘后：")
    lines.append("  [持仓时] 检查是否触发卖出：")
    lines.append(f"     - heat_position <= {SELL_THRESHOLD} → 热度已回升，卖出")
    lines.append(f"     - 持仓天数 >= {MAX_HOLD_DAYS} → 超时平仓（实际无限制）")
    lines.append("  [空仓时] 全市场扫描：")
    lines.append(f"     - 三条件筛选 → 流动性过滤(>= {MIN_DEAL_AMOUNT/1e8:.1f}亿)")
    lines.append("     - rank_surge最大的股票买入（偏离自身常态最远）")
    lines.append("  执行：次日收盘价买入/卖出")
    lines.append("```")
    lines.append("")
    lines.append("### 1.4 资金管理")
    lines.append(f"- 初始资金: {INITIAL_CAPITAL/1e4:.0f}万")
    lines.append("- 每次全仓买入1只股票，卖出后全额投入下一只")
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
    lines.append(f"| ATR_WINDOW | {ATR_WINDOW} | ATR计算窗口（天） |")
    lines.append(f"| ATR_THRESHOLD | {ATR_THRESHOLD} | ATR/close阈值，< 此值 = 低波动 |")
    lines.append(f"| CONSEC_DOWN_DAYS | {CONSEC_DOWN_DAYS} | 连续下跌天数 |")
    lines.append(f"| RANK_COLD_THRESHOLD | {RANK_COLD_THRESHOLD} | rank > 此值 = 偏冷门 |")
    lines.append(f"| LOOKBACK | {LOOKBACK} | heat_position回看窗口（天） |")
    lines.append(f"| SELL_THRESHOLD | {SELL_THRESHOLD} | 卖出阈值（heat_position） |")
    lines.append(f"| MAX_HOLD_DAYS | {MAX_HOLD_DAYS} | 最大持仓天数（无限制） |")
    lines.append(f"| MIN_DEAL_AMOUNT | {MIN_DEAL_AMOUNT/1e8:.1f}亿 | 最低日成交额 |")
    lines.append(f"| 回测区间 | {START_DATE} ~ {END_DATE} | |")
    lines.append(f"| 初始资金 | {INITIAL_CAPITAL/1e4:.0f}万 | |")
    lines.append("")

    # ---- 与旧策略对比说明 ----
    lines.append("## 八、与旧热度轮转策略对比说明")
    lines.append("")
    lines.append("| 维度 | 旧策略（热度轮转v2无超时） | 本策略（平台突破+热度出场） |")
    lines.append("|------|------------------------|------------------------|")
    lines.append("| 入场信号 | heat_position >= 0.80（纯热度低谷） | ATR/close < 0.03 + 3连跌 + rank>2000 |")
    lines.append("| 信号含义 | 热度突然降低 | 低波动整理+短期回调+冷门 |")
    lines.append("| 出场信号 | heat_position <= 0.20 | heat_position <= 0.20（相同） |")
    lines.append("| 选股排序 | rank_surge最大 | rank_surge最大（相同） |")
    lines.append("| 核心差异 | 只看热度面 | 技术面（波动率+趋势）+ 热度面复合 |")
    lines.append("")

    return "\n".join(lines)


# ============================================================
# 主函数
# ============================================================

def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("平台突破策略回测 — 新入场条件 + 热度回升出场")
    logger.info("=" * 60)

    # 1. 加载数据
    heat_df, trading_days, price_df, index_df = load_data()

    # 2. 计算信号矩阵
    heat_position, rank_pivot, deal_pivot, rank_surge, atr_ratio_pivot, close_pivot = compute_signals(
        heat_df, trading_days, price_df, LOOKBACK, ATR_WINDOW
    )

    # 3. 回测
    logger.info("=" * 60)
    logger.info("开始回测...")
    logger.info("=" * 60)

    trade_df, equity_df, final_capital = simulate_rotation(
        heat_position, rank_pivot, deal_pivot, rank_surge,
        atr_ratio_pivot, close_pivot,
        price_df, trading_days, index_df,
        SELL_THRESHOLD, MAX_HOLD_DAYS, MIN_DEAL_AMOUNT
    )

    if len(trade_df) == 0:
        logger.error("回测无交易产生，请检查数据和参数")
        return

    # 4. 计算绩效
    metrics = compute_metrics(trade_df, equity_df)
    logger.info(f"  策略总收益: {metrics['total_return']:.2f}%")
    logger.info(f"  沪深300: {metrics['index_return']:.2f}%")
    logger.info(f"  超额: {metrics['excess_total']:.2f}%")

    # 5. 生成报告
    report = build_report(trade_df, equity_df, metrics)

    # 6. 保存结果
    output_dir = os.path.dirname(os.path.abspath(__file__))

    report_path = os.path.join(output_dir, '平台策略回测报告_热度出场.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"报告: {report_path}")

    trade_path = os.path.join(output_dir, 'platform_trades_heat_exit.csv')
    trade_df.to_csv(trade_path, index=False, encoding='utf-8-sig')
    logger.info(f"交易明细: {trade_path}")

    equity_path = os.path.join(output_dir, 'platform_equity_heat_exit.csv')
    equity_df.to_csv(equity_path, index=False, encoding='utf-8-sig')
    logger.info(f"净值曲线: {equity_path}")

    elapsed = time.time() - t0
    logger.info(f"全部完成，耗时 {elapsed:.1f} 秒")


if __name__ == '__main__':
    main()
