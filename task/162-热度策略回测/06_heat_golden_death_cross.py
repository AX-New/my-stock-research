"""
热度轮转策略回测 - 金叉后死叉出场版本（单仓位全市场轮转）

基于 04_heat_dif_peak_exit.py 修改：
- 入场逻辑不变：heat_position >= 0.8, rank_surge排序, 成交额>=5000万
- 出场逻辑改为"金叉后等死叉"两阶段出场：
  Phase 1（等金叉）：买入后等待DIF上穿DEA（金叉）
  Phase 2（等死叉）：金叉出现后，等待DIF下穿DEA（死叉），次日卖出
- 无超时强制平仓（MAX_HOLD_DAYS = 9999）

【金叉/死叉判断】
==========================================
金叉条件：DIF_yesterday <= DEA_yesterday AND DIF_today > DEA_today
死叉条件：DIF_yesterday > DEA_yesterday AND DIF_today <= DEA_today

逻辑流程：
  买入 → 等金叉 → 金叉出现 → 等死叉 → 死叉出现 → 次日卖出
"""

import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
MIN_DEAL_AMOUNT = 5e7   # 最低日成交额 5000万（过滤流动性差的小票）

# MACD参数
MACD_SHORT = 12         # 短期EMA周期
MACD_LONG = 26          # 长期EMA周期
MACD_SIGNAL = 9         # 信号线EMA周期

# 回测区间
START_DATE = '2025-03-15'
END_DATE = '2026-03-19'
INITIAL_CAPITAL = 1_000_000

# MACD预热：需要至少60个交易日的数据来让EMA充分收敛
# 从START_DATE往前推约3个月
MACD_WARMUP_DATE = '2024-12-01'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ============================================================
# MACD计算
# ============================================================

def calc_macd(close_series, short=12, long=26, signal=9):
    """
    计算MACD指标

    参数:
        close_series: 收盘价序列（需按时间排序）
        short: 短期EMA周期（默认12）
        long: 长期EMA周期（默认26）
        signal: 信号线EMA周期（默认9）

    返回:
        dif: 快线 - 慢线
        dea: DIF的EMA（信号线）
        macd_val: (DIF - DEA) * 2（柱状图）
    """
    ema_short = close_series.ewm(span=short, adjust=False).mean()
    ema_long = close_series.ewm(span=long, adjust=False).mean()
    dif = ema_short - ema_long
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_val = (dif - dea) * 2
    return dif, dea, macd_val


# ============================================================
# 数据加载
# ============================================================

def get_engine(db_name):
    return create_engine(
        f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4',
        pool_pre_ping=True,
    )


def load_data():
    """一次性加载所有需要的数据（含MACD预热期的行情数据）"""
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

    # 2. 交易日历（回测区间）
    td_df = pd.read_sql(f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1
          AND cal_date >= '{START_DATE.replace('-', '')}'
          AND cal_date <= '{END_DATE.replace('-', '')}'
        ORDER BY cal_date
    """, stock_engine)
    trading_days = sorted(pd.to_datetime(td_df['cal_date'], format='%Y%m%d').dt.date.tolist())
    logger.info(f"  交易日: {len(trading_days)} 天")

    # 3. 行情数据（从MACD预热日期开始，用于MACD计算）
    #    回测区间的价格用于交易执行，预热期的价格仅用于MACD计算
    price_df = pd.read_sql(f"""
        SELECT m.ts_code, m.trade_date, m.close, a.adj_factor
        FROM market_daily m
        JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        WHERE m.trade_date >= '{MACD_WARMUP_DATE.replace('-', '')}'
          AND m.trade_date <= '{END_DATE.replace('-', '')}'
    """, stock_engine)
    price_df['date'] = pd.to_datetime(price_df['trade_date'], format='%Y%m%d').dt.date
    price_df['stock_code'] = price_df['ts_code'].str[:6]
    # 前复权：当前价格不变，历史价格按复权因子调整
    latest_adj = price_df.groupby('stock_code')['adj_factor'].transform('last')
    price_df['qfq_close'] = price_df['close'] * price_df['adj_factor'] / latest_adj
    logger.info(f"  行情数据: {len(price_df):,} 条, {price_df['stock_code'].nunique()} 只股票"
                f" (含MACD预热期从{MACD_WARMUP_DATE}起)")

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
    rolling_mean = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).mean()
    rolling_mean = rolling_mean.replace(0, np.nan)
    rank_surge = rank_pivot / rolling_mean

    logger.info(f"  矩阵大小: {heat_position.shape[0]} 天 × {heat_position.shape[1]} 只股票")

    return heat_position, rank_pivot, deal_pivot, rank_surge


# ============================================================
# DIF和DEA矩阵计算
# ============================================================

def compute_dif_dea_pivot(price_df):
    """
    计算所有股票的MACD DIF和DEA值，构建DIF和DEA透视表

    使用前复权收盘价（qfq_close）计算MACD，确保价格连续性。
    预热期数据用于EMA收敛，回测区间内的DIF/DEA值用于金叉死叉判断。

    返回:
        dif_pivot: DataFrame, 行=日期, 列=股票代码, 值=DIF
        dea_pivot: DataFrame, 行=日期, 列=股票代码, 值=DEA
    """
    logger.info(f"计算MACD DIF/DEA矩阵 (EMA{MACD_SHORT}/{MACD_LONG}, signal={MACD_SIGNAL})...")

    # 按股票分组计算MACD
    dif_records = []
    dea_records = []

    stock_codes = price_df['stock_code'].unique()
    logger.info(f"  计算 {len(stock_codes)} 只股票的MACD...")

    for code in stock_codes:
        stock_data = price_df[price_df['stock_code'] == code].sort_values('date')
        if len(stock_data) < MACD_LONG + MACD_SIGNAL:
            # 数据不足以计算MACD，跳过
            continue

        close_series = stock_data['qfq_close'].reset_index(drop=True)
        dif, dea, macd_val = calc_macd(close_series, MACD_SHORT, MACD_LONG, MACD_SIGNAL)

        stock_dif = pd.DataFrame({
            'date': stock_data['date'].values,
            'stock_code': code,
            'dif': dif.values,
        })
        dif_records.append(stock_dif)

        stock_dea = pd.DataFrame({
            'date': stock_data['date'].values,
            'stock_code': code,
            'dea': dea.values,
        })
        dea_records.append(stock_dea)

    dif_all = pd.concat(dif_records, ignore_index=True)
    dea_all = pd.concat(dea_records, ignore_index=True)

    # 构建DIF透视表：行=日期, 列=股票代码, 值=DIF
    dif_pivot = dif_all.pivot_table(index='date', columns='stock_code', values='dif')
    dif_pivot = dif_pivot.sort_index()

    # 构建DEA透视表：行=日期, 列=股票代码, 值=DEA
    dea_pivot = dea_all.pivot_table(index='date', columns='stock_code', values='dea')
    dea_pivot = dea_pivot.sort_index()

    logger.info(f"  DIF矩阵大小: {dif_pivot.shape[0]} 天 × {dif_pivot.shape[1]} 只股票")
    logger.info(f"  DEA矩阵大小: {dea_pivot.shape[0]} 天 × {dea_pivot.shape[1]} 只股票")

    return dif_pivot, dea_pivot


# ============================================================
# 单仓位轮转回测（金叉后死叉出场版）
# ============================================================

def simulate_rotation(heat_position, rank_pivot, deal_pivot, rank_surge,
                      dif_pivot, dea_pivot, price_df, trading_days,
                      index_df, buy_threshold, min_deal_amount):
    """
    单仓位轮转回测引擎（金叉后死叉出场版）

    逐日模拟：
    1. 记录当日净值
    2. 检查持仓是否触发死叉卖出条件（需先经历金叉）
    3. 如果无持仓，全市场选最冷门的股票买入
    4. 执行买卖（次日收盘价）

    卖出条件（金叉后死叉）：
    - Phase 1：买入后等待金叉（DIF上穿DEA）
      金叉：DIF_yesterday <= DEA_yesterday AND DIF_today > DEA_today
    - Phase 2：金叉出现后等待死叉（DIF下穿DEA）
      死叉：DIF_yesterday > DEA_yesterday AND DIF_today <= DEA_today
    - 死叉出现 → 次日卖出
    """
    logger.info(f"开始轮转回测 (buy={buy_threshold}, "
                f"min_deal={min_deal_amount/1e8:.1f}亿, "
                f"出场=金叉后死叉)...")

    # 构建价格查询表: (stock_code, date) -> {qfq_close, close}
    # 仅使用回测区间内的价格数据
    start_dt = pd.to_datetime(START_DATE).date()
    price_bt = price_df[price_df['date'] >= start_dt]
    price_lookup = {
        (row['stock_code'], row['date']): {'qfq_close': row['qfq_close'], 'close': row['close']}
        for _, row in price_bt[['stock_code', 'date', 'qfq_close', 'close']].iterrows()
    }

    # 交易日列表和索引映射
    td_list = sorted(trading_days)
    td_map = {d: i for i, d in enumerate(td_list)}

    # DIF/DEA透视表的日期集合（含预热期，用于回溯查询）
    dif_date_set = set(dif_pivot.index.tolist())
    dif_dates = sorted(dif_pivot.index.tolist())

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
    golden_cross_seen = False # 是否已经出现金叉

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

        # ---- 2. 卖出判断（金叉后死叉） ----
        should_sell = False
        sell_reason = None

        if holding:
            # 检查金叉/死叉信号：需要今天和昨天的DIF/DEA
            if (hold_code in dif_pivot.columns and hold_code in dea_pivot.columns
                    and i >= 1):
                # 获取今天的DIF/DEA
                dif_today = np.nan
                dea_today = np.nan
                if today in dif_date_set:
                    dif_today = dif_pivot.loc[today, hold_code]
                    dea_today = dea_pivot.loc[today, hold_code]

                # 获取昨天的DIF/DEA（使用回测交易日列表的前一天）
                yesterday = td_list[i - 1]
                dif_yesterday = np.nan
                dea_yesterday = np.nan
                if yesterday in dif_date_set:
                    dif_yesterday = dif_pivot.loc[yesterday, hold_code]
                    dea_yesterday = dea_pivot.loc[yesterday, hold_code]

                # 确保所有值有效
                if not any(np.isnan(x) for x in [dif_today, dea_today, dif_yesterday, dea_yesterday]):
                    if not golden_cross_seen:
                        # Phase 1：等待金叉
                        # 金叉：DIF从下方穿越DEA到上方
                        if dif_yesterday <= dea_yesterday and dif_today > dea_today:
                            golden_cross_seen = True

                    if golden_cross_seen:
                        # Phase 2：等待死叉
                        # 死叉：DIF从上方穿越DEA到下方
                        if dif_yesterday > dea_yesterday and dif_today <= dea_today:
                            should_sell = True
                            sell_reason = 'death_cross_after_golden'

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
                golden_cross_seen = False
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
            golden_cross_seen = False  # 新仓位重置金叉标志

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

def build_report(trade_df, equity_df, metrics):
    """生成详细的 Markdown 回测报告"""
    lines = []

    lines.append("# 热度轮转策略回测报告（金叉后死叉出场）")
    lines.append(f"**创建时间**: {datetime.now().strftime('%Y%m%d %H:%M')}")
    lines.append("")

    # ---- 回测逻辑说明 ----
    lines.append("## 一、回测逻辑说明")
    lines.append("")
    lines.append("### 1.1 策略思路")
    lines.append("利用东财人气排名的均值回归特性选股，用MACD金叉/死叉信号判断出场时机。")
    lines.append("当一只股票的热度跌到近期最低点时买入，等待DIF上穿DEA（金叉确认趋势启动），")
    lines.append("再等待DIF下穿DEA（死叉确认趋势结束）时卖出，然后立即轮转到下一只。")
    lines.append("**资金始终满仓运转，没有空窗期。**")
    lines.append("")
    lines.append("### 1.2 交易流程")
    lines.append("```")
    lines.append("每个交易日收盘后：")
    lines.append("  1. 如果持仓中 → 两阶段出场判断")
    lines.append("     Phase 1（等金叉）：")
    lines.append("       - DIF_yesterday <= DEA_yesterday AND DIF_today > DEA_today → 金叉确认")
    lines.append("       - 标记 golden_cross_seen = True")
    lines.append("     Phase 2（等死叉，仅在金叉后）：")
    lines.append("       - DIF_yesterday > DEA_yesterday AND DIF_today <= DEA_today → 死叉确认")
    lines.append("       - 触发卖出，次日执行")
    lines.append("     无超时强制平仓")
    lines.append("  2. 如果无持仓（初始/刚卖出） → 全市场选股")
    lines.append(f"     - 筛选heat_position >= {BUY_THRESHOLD}的股票（热度处于近期低谷）")
    lines.append(f"     - 过滤日成交额 < {MIN_DEAL_AMOUNT/1e8:.1f}亿的小票")
    lines.append("     - 在候选中选rank_surge最高的（相对自身常态冷了最多的）")
    lines.append("  3. 次日收盘价执行买入/卖出")
    lines.append("     - 卖出和买入可以同日执行（卖旧买新）")
    lines.append("```")
    lines.append("")
    lines.append("### 1.3 核心指标")
    lines.append("")
    lines.append("**heat_position（入场信号）：**")
    lines.append(f"- 取过去{LOOKBACK}个交易日的热度排名（rank）")
    lines.append("- heat_position = (当前rank - N日最低rank) / (N日最高rank - N日最低rank)")
    lines.append("- 值域 [0, 1]，越接近1表示热度越冷（rank越大=越不受关注）")
    lines.append("")
    lines.append("**MACD 金叉/死叉（出场信号）：**")
    lines.append(f"- 使用前复权收盘价计算MACD (EMA{MACD_SHORT}/{MACD_LONG}, signal={MACD_SIGNAL})")
    lines.append("- DIF = EMA_short - EMA_long，DEA = EMA(DIF, 9)")
    lines.append("- 金叉：DIF从下方穿越DEA到上方（趋势启动）")
    lines.append("- 死叉：DIF从上方穿越DEA到下方（趋势结束）")
    lines.append("- 必须先看到金叉，再等死叉出场 → 确保完整趋势周期")
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
        for reason, label in [('death_cross_after_golden', 'DIF金叉→死叉'),
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
            reason_map = {'death_cross_after_golden': 'DIF金叉→死叉', 'end_of_period': '期末'}
            reason = reason_map.get(row['exit_reason'], row['exit_reason'])
            lines.append(f"| {idx+1} | {row['stock_code']} | {row['entry_date']} | "
                         f"{row['exit_date']} | {row['hold_days']} | {ret_str} | {reason} |")
        lines.append("")

    # ---- 与其他出场策略对比 ----
    lines.append("## 七、与其他出场策略对比")
    lines.append("")
    lines.append("| 策略 | 总收益 | 年化收益 | 最大回撤 | 夏普 | 胜率 | 交易笔数 |")
    lines.append("|------|--------|---------|---------|------|------|---------|")
    lines.append(f"| **金叉后死叉出场（本策略）** | **{metrics['total_return']:.2f}%** | "
                 f"**{metrics['annual_return']:.2f}%** | **{metrics['max_drawdown']:.2f}%** | "
                 f"**{metrics['sharpe']:.2f}** | **{metrics['win_rate']:.1f}%** | "
                 f"**{metrics['total_trades']}** |")
    lines.append("| DIF见顶出场（M4） | 57.51% | 58.85% | -15.80% | 1.49 | 57.7% | 26 |")
    lines.append("| 热度出场（无超时） | 72.68% | 74.61% | -21.41% | 1.22 | 60.0% | 25 |")
    lines.append("| 热度出场（超时15天） | 31.95% | 32.70% | -39.12% | 0.76 | 66.7% | 24 |")
    lines.append("")

    # ---- 参数 ----
    lines.append("## 八、回测参数")
    lines.append("")
    lines.append("| 参数 | 值 | 说明 |")
    lines.append("|------|-----|------|")
    lines.append(f"| LOOKBACK | {LOOKBACK} | heat_position回看窗口（天） |")
    lines.append(f"| BUY_THRESHOLD | {BUY_THRESHOLD} | 买入阈值 |")
    lines.append(f"| MIN_DEAL_AMOUNT | {MIN_DEAL_AMOUNT/1e8:.1f}亿 | 最低日成交额 |")
    lines.append(f"| MACD参数 | EMA{MACD_SHORT}/{MACD_LONG}, signal={MACD_SIGNAL} | 标准MACD参数 |")
    lines.append(f"| 出场逻辑 | 金叉后死叉 | DIF上穿DEA后等DIF下穿DEA卖出 |")
    lines.append(f"| MACD预热 | 从{MACD_WARMUP_DATE}开始 | 确保EMA充分收敛 |")
    lines.append(f"| 回测区间 | {START_DATE} ~ {END_DATE} | |")
    lines.append("")

    return "\n".join(lines)


# ============================================================
# 主函数
# ============================================================

def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("热度轮转策略回测 - 金叉后死叉出场版")
    logger.info("=" * 60)

    # 1. 加载数据（含MACD预热期）
    heat_df, trading_days, price_df, index_df = load_data()

    # 2. 计算热度位置
    heat_position, rank_pivot, deal_pivot, rank_surge = compute_heat_position(
        heat_df, trading_days, LOOKBACK
    )

    # 3. 计算MACD DIF和DEA矩阵（使用含预热期的完整价格数据）
    dif_pivot, dea_pivot = compute_dif_dea_pivot(price_df)

    # 4. 运行回测
    logger.info("=" * 60)
    logger.info("运行金叉后死叉出场回测...")
    logger.info("=" * 60)

    trade_df, equity_df, final_capital = simulate_rotation(
        heat_position, rank_pivot, deal_pivot, rank_surge,
        dif_pivot, dea_pivot, price_df, trading_days, index_df,
        BUY_THRESHOLD, MIN_DEAL_AMOUNT
    )

    if len(trade_df) == 0:
        logger.error("回测无交易产生，请检查数据和参数")
        return

    # 5. 计算绩效
    metrics = compute_metrics(trade_df, equity_df)
    logger.info(f"  策略总收益: {metrics['total_return']:.2f}%")
    logger.info(f"  沪深300: {metrics['index_return']:.2f}%")
    logger.info(f"  超额: {metrics['excess_total']:.2f}%")

    # 6. 生成报告
    report = build_report(trade_df, equity_df, metrics)

    # 7. 保存结果
    output_dir = os.path.dirname(os.path.abspath(__file__))

    report_path = os.path.join(output_dir, '热度轮转策略回测报告_金叉死叉出场.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"报告: {report_path}")

    trade_path = os.path.join(output_dir, 'rotation_trades_golden_death.csv')
    trade_df.to_csv(trade_path, index=False, encoding='utf-8-sig')
    logger.info(f"交易明细: {trade_path}")

    equity_path = os.path.join(output_dir, 'rotation_equity_golden_death.csv')
    equity_df.to_csv(equity_path, index=False, encoding='utf-8-sig')
    logger.info(f"净值曲线: {equity_path}")

    elapsed = time.time() - t0
    logger.info(f"全部完成，耗时 {elapsed:.1f} 秒")


if __name__ == '__main__':
    main()
