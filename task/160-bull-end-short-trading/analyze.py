"""
牛市末期短线交易潜力分析 + 熊市操作策略研究

分析目标：
1. 牛市结束前6个月做短线是否可行（尤其震荡型牛市）
2. 不同牛市类型下个股短线收益对比
3. 熊市中的操作策略效果评估

数据范围：2016~2026（近10年，覆盖足够牛熊样本）
短线定义：持有5日/10日/20日
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from lib.database import read_engine

# ========== 1. 牛熊周期定义 ==========
# 基于 macd/report/01-a-share-bull-bear-cycles.md

BULL_BEAR_CYCLES = [
    # (名称, 类型, 开始日期, 结束日期, 子类型)
    ("第15轮牛市", "bull", "20130601", "20150601", "大牛市"),
    ("第16轮熊市", "bear", "20150601", "20160101", "急跌熊"),
    ("第17轮牛市", "bull", "20160101", "20180101", "慢牛"),
    ("第18轮熊市", "bear", "20180101", "20190101", "中等熊"),
    ("第19轮牛市", "bull", "20190101", "20210201", "结构牛"),
    ("第20轮熊市", "bear", "20210201", "20240201", "阴跌熊"),
    ("第21轮牛市", "bull", "20240201", "20260320", "当前牛市"),
]

# 短线持有天数
HOLDING_DAYS = [5, 10, 20]


def load_index_data():
    """加载上证指数日线数据"""
    print("加载上证指数日线数据...")
    sql = """
    SELECT trade_date, close, pct_chg
    FROM index_daily
    WHERE ts_code = '000001.SH'
    AND trade_date >= '20130101'
    ORDER BY trade_date
    """
    df = pd.read_sql(sql, read_engine)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    print(f"  上证指数日线: {len(df)} 条, {df['trade_date'].min()} ~ {df['trade_date'].max()}")
    return df


def load_stock_daily(start_date, end_date, extra_days=30):
    """
    加载指定区间的个股日线数据（前复权）
    extra_days: 额外多加载的交易日天数，用于计算持有期收益
    """
    # 向后多加载一些数据用于计算持有期收益
    sql = f"""
    SELECT m.ts_code, m.trade_date, m.open, m.high, m.low, m.close,
           m.pct_chg, m.vol, m.amount, a.adj_factor
    FROM market_daily m
    JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
    WHERE m.trade_date >= '{start_date}'
    AND m.trade_date <= '{end_date}'
    AND m.vol > 0
    ORDER BY m.ts_code, m.trade_date
    """
    df = pd.read_sql(sql, read_engine)
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 前复权价格
    latest_adj = df.groupby('ts_code')['adj_factor'].transform('last')
    df['adj_close'] = df['close'] * df['adj_factor'] / latest_adj

    return df


def load_daily_basic(start_date, end_date):
    """加载每日基础指标（市值、换手率）"""
    sql = f"""
    SELECT ts_code, trade_date, turnover_rate_f, total_mv, circ_mv
    FROM daily_basic
    WHERE trade_date >= '{start_date}'
    AND trade_date <= '{end_date}'
    """
    df = pd.read_sql(sql, read_engine)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df


def calc_forward_returns(df, holding_days_list):
    """
    计算前瞻N日收益率
    输入: 按ts_code, trade_date排序的日线数据
    输出: 增加 ret_5d, ret_10d, ret_20d 列
    """
    print("计算前瞻收益率...")
    results = []
    for ts_code, group in df.groupby('ts_code'):
        group = group.sort_values('trade_date').reset_index(drop=True)
        for n in holding_days_list:
            col_name = f'ret_{n}d'
            group[col_name] = group['adj_close'].shift(-n) / group['adj_close'] - 1
        results.append(group)

    result = pd.concat(results, ignore_index=True)
    print(f"  完成, 共 {len(result)} 条记录")
    return result


def get_period_dates(period_start, period_end, last_n_months=None):
    """
    获取期间日期范围
    last_n_months: 如果指定，取该区间最后N个月
    """
    start = pd.to_datetime(period_start)
    end = pd.to_datetime(period_end)
    if last_n_months:
        start = end - pd.DateOffset(months=last_n_months)
    return start, end


def analyze_short_term_returns(df, period_name):
    """
    分析特定时期的短线收益统计
    返回: dict 包含各持有期的统计数据
    """
    stats = {'period': period_name}
    stats['total_samples'] = len(df)

    for n in HOLDING_DAYS:
        col = f'ret_{n}d'
        valid = df[col].dropna()
        if len(valid) == 0:
            continue

        stats[f'{n}d_samples'] = len(valid)
        stats[f'{n}d_mean'] = valid.mean() * 100
        stats[f'{n}d_median'] = valid.median() * 100
        stats[f'{n}d_std'] = valid.std() * 100
        stats[f'{n}d_win_rate'] = (valid > 0).mean() * 100
        stats[f'{n}d_gt3pct'] = (valid > 0.03).mean() * 100  # 收益>3%的比例
        stats[f'{n}d_gt5pct'] = (valid > 0.05).mean() * 100  # 收益>5%的比例
        stats[f'{n}d_lt_neg3pct'] = (valid < -0.03).mean() * 100  # 亏损>3%的比例
        stats[f'{n}d_lt_neg5pct'] = (valid < -0.05).mean() * 100  # 亏损>5%的比例
        stats[f'{n}d_sharpe'] = valid.mean() / valid.std() if valid.std() > 0 else 0
        stats[f'{n}d_p25'] = valid.quantile(0.25) * 100
        stats[f'{n}d_p75'] = valid.quantile(0.75) * 100
        stats[f'{n}d_max'] = valid.max() * 100
        stats[f'{n}d_min'] = valid.min() * 100

    return stats


def analyze_by_stock_type(df, daily_basic_df):
    """
    按股票类型（市值大小）分组分析
    """
    # 合并市值数据
    merged = df.merge(daily_basic_df[['ts_code', 'trade_date', 'total_mv', 'turnover_rate_f']],
                      on=['ts_code', 'trade_date'], how='left')

    # 按市值分组: 小盘(<50亿) / 中盘(50-200亿) / 大盘(>200亿)
    # total_mv 单位是万元
    merged['mv_group'] = pd.cut(merged['total_mv'],
                                 bins=[0, 500000, 2000000, float('inf')],
                                 labels=['小盘(<50亿)', '中盘(50-200亿)', '大盘(>200亿)'])

    results = {}
    for group_name, group_df in merged.groupby('mv_group', observed=True):
        results[group_name] = analyze_short_term_returns(group_df, str(group_name))

    return results


def analyze_bull_end_vs_middle(df_all, cycle_name, cycle_start, cycle_end):
    """
    对比牛市中期 vs 末期的短线收益
    """
    start = pd.to_datetime(cycle_start)
    end = pd.to_datetime(cycle_end)
    mid_point = start + (end - start) / 2
    last_6m_start = end - pd.DateOffset(months=6)

    # 牛市前半段
    df_early = df_all[(df_all['trade_date'] >= start) & (df_all['trade_date'] < mid_point)]
    # 牛市后半段（不含最后6个月）
    df_middle = df_all[(df_all['trade_date'] >= mid_point) & (df_all['trade_date'] < last_6m_start)]
    # 牛市最后6个月
    df_late = df_all[(df_all['trade_date'] >= last_6m_start) & (df_all['trade_date'] <= end)]

    results = {}
    if len(df_early) > 0:
        results['前半段'] = analyze_short_term_returns(df_early, f'{cycle_name}-前半段')
    if len(df_middle) > 0:
        results['中段'] = analyze_short_term_returns(df_middle, f'{cycle_name}-中段')
    if len(df_late) > 0:
        results['末期6个月'] = analyze_short_term_returns(df_late, f'{cycle_name}-末期6个月')

    return results


def analyze_bear_market_strategies(df_bear, cycle_name):
    """
    分析熊市中不同策略的效果
    策略：
    1. 随机买入持有（基准）
    2. 大跌后反弹（前一日跌幅>3%后买入）
    3. 缩量后买入（成交量低于20日均量的50%）
    4. 超跌反弹（连续3日下跌后买入）
    """
    results = {}

    # 1. 随机买入（基准）
    results['随机买入'] = analyze_short_term_returns(df_bear, f'{cycle_name}-随机买入')

    # 2. 大跌后反弹: 前一日跌幅>3%
    df_after_drop = df_bear[df_bear['pct_chg'] < -3.0]
    if len(df_after_drop) > 0:
        results['大跌次日买入'] = analyze_short_term_returns(df_after_drop, f'{cycle_name}-大跌次日买入')

    # 3. 缩量买入: 当日成交量 < 20日均量的50%
    df_bear_sorted = df_bear.sort_values(['ts_code', 'trade_date'])
    df_bear_sorted['vol_ma20'] = df_bear_sorted.groupby('ts_code')['vol'].transform(
        lambda x: x.rolling(20, min_periods=10).mean()
    )
    df_low_vol = df_bear_sorted[df_bear_sorted['vol'] < df_bear_sorted['vol_ma20'] * 0.5]
    if len(df_low_vol) > 0:
        results['缩量买入'] = analyze_short_term_returns(df_low_vol, f'{cycle_name}-缩量买入')

    # 4. 超跌反弹: 连续3日下跌
    df_bear_sorted['down_streak'] = df_bear_sorted.groupby('ts_code')['pct_chg'].transform(
        lambda x: (x < 0).rolling(3, min_periods=3).sum()
    )
    df_oversold = df_bear_sorted[df_bear_sorted['down_streak'] >= 3]
    if len(df_oversold) > 0:
        results['连续3日下跌后买入'] = analyze_short_term_returns(df_oversold, f'{cycle_name}-连续3日下跌后买入')

    return results


def analyze_volatility_by_period(index_df):
    """
    分析各牛熊阶段的指数波动率
    """
    results = []
    for name, trend, start, end, subtype in BULL_BEAR_CYCLES:
        mask = (index_df['trade_date'] >= pd.to_datetime(start)) & \
               (index_df['trade_date'] <= pd.to_datetime(end))
        period_df = index_df[mask]
        if len(period_df) == 0:
            continue

        daily_vol = period_df['pct_chg'].std()
        annualized_vol = daily_vol * np.sqrt(252)
        max_drawdown = (period_df['close'] / period_df['close'].cummax() - 1).min()

        results.append({
            'period': name,
            'subtype': subtype,
            'days': len(period_df),
            'daily_vol': daily_vol,
            'annual_vol': annualized_vol,
            'max_drawdown': max_drawdown * 100,
            'mean_daily_ret': period_df['pct_chg'].mean(),
        })

    return pd.DataFrame(results)


def main():
    print("=" * 70)
    print("牛市末期短线交易潜力分析 + 熊市操作策略研究")
    print("=" * 70)

    # 加载指数数据
    index_df = load_index_data()

    # ========== Part 1: 各阶段波动率分析 ==========
    print("\n" + "=" * 70)
    print("Part 1: 各牛熊阶段波动率分析")
    print("=" * 70)
    vol_df = analyze_volatility_by_period(index_df)
    print(vol_df.to_string(index=False))

    # ========== Part 2: 牛市末期 vs 中期短线收益对比 ==========
    print("\n" + "=" * 70)
    print("Part 2: 牛市末期 vs 中期短线收益对比")
    print("=" * 70)

    # 分析的牛市周期（近10年内有完整结束的牛市）
    bull_cycles_to_analyze = [
        ("第17轮慢牛", "20160101", "20180101", "慢牛"),
        ("第19轮结构牛", "20190101", "20210201", "结构牛"),
    ]

    all_bull_comparisons = {}

    for cycle_name, start, end, subtype in bull_cycles_to_analyze:
        print(f"\n--- {cycle_name} ({subtype}) ---")

        # 多加载30个交易日用于计算前瞻收益
        end_extended = (pd.to_datetime(end) + pd.DateOffset(days=45)).strftime('%Y%m%d')

        print(f"加载个股数据: {start} ~ {end_extended}")
        df_stock = load_stock_daily(start, end_extended)
        print(f"  个股日线: {len(df_stock)} 条, {df_stock['ts_code'].nunique()} 只股票")

        # 计算前瞻收益
        df_stock = calc_forward_returns(df_stock, HOLDING_DAYS)

        # 只取牛市区间内的数据进行分析（前瞻收益已经包含了之后的数据）
        df_bull = df_stock[(df_stock['trade_date'] >= pd.to_datetime(start)) &
                           (df_stock['trade_date'] <= pd.to_datetime(end))]

        # 对比前半段 / 中段 / 末期6个月
        comparison = analyze_bull_end_vs_middle(df_bull, cycle_name, start, end)
        all_bull_comparisons[cycle_name] = comparison

        for phase, stats in comparison.items():
            print(f"\n  {phase}:")
            for n in HOLDING_DAYS:
                if f'{n}d_mean' in stats:
                    print(f"    {n}日持有: 均值={stats[f'{n}d_mean']:.2f}%, "
                          f"胜率={stats[f'{n}d_win_rate']:.1f}%, "
                          f"盈利>5%占比={stats[f'{n}d_gt5pct']:.1f}%, "
                          f"亏损>5%占比={stats[f'{n}d_lt_neg5pct']:.1f}%")

    # ========== Part 3: 市值分层分析（震荡牛市末期） ==========
    print("\n" + "=" * 70)
    print("Part 3: 震荡牛市末期 - 按市值分层分析")
    print("=" * 70)

    all_mv_analysis = {}
    for cycle_name, start, end, subtype in bull_cycles_to_analyze:
        last_6m_start = (pd.to_datetime(end) - pd.DateOffset(months=6)).strftime('%Y%m%d')
        end_extended = (pd.to_datetime(end) + pd.DateOffset(days=45)).strftime('%Y%m%d')

        print(f"\n--- {cycle_name} 末期6个月 ({last_6m_start} ~ {end}) ---")

        df_stock = load_stock_daily(last_6m_start, end_extended)
        df_stock = calc_forward_returns(df_stock, HOLDING_DAYS)
        df_bull_late = df_stock[(df_stock['trade_date'] >= pd.to_datetime(last_6m_start)) &
                                 (df_stock['trade_date'] <= pd.to_datetime(end))]

        # 加载市值数据
        df_basic = load_daily_basic(last_6m_start, end)

        mv_results = analyze_by_stock_type(df_bull_late, df_basic)
        all_mv_analysis[cycle_name] = mv_results

        for mv_group, stats in mv_results.items():
            print(f"\n  {mv_group}:")
            for n in HOLDING_DAYS:
                if f'{n}d_mean' in stats:
                    print(f"    {n}日持有: 均值={stats[f'{n}d_mean']:.2f}%, "
                          f"胜率={stats[f'{n}d_win_rate']:.1f}%, "
                          f"盈利>5%={stats[f'{n}d_gt5pct']:.1f}%")

    # ========== Part 4: 熊市策略分析 ==========
    print("\n" + "=" * 70)
    print("Part 4: 熊市操作策略分析")
    print("=" * 70)

    bear_cycles_to_analyze = [
        ("第18轮中等熊", "20180101", "20190101", "中等熊"),
        ("第20轮阴跌熊", "20210201", "20240201", "阴跌熊"),
    ]

    all_bear_strategies = {}

    for cycle_name, start, end, subtype in bear_cycles_to_analyze:
        print(f"\n--- {cycle_name} ({subtype}) ---")

        end_extended = (pd.to_datetime(end) + pd.DateOffset(days=45)).strftime('%Y%m%d')

        print(f"加载个股数据: {start} ~ {end_extended}")
        df_stock = load_stock_daily(start, end_extended)
        print(f"  个股日线: {len(df_stock)} 条, {df_stock['ts_code'].nunique()} 只股票")

        df_stock = calc_forward_returns(df_stock, HOLDING_DAYS)
        df_bear = df_stock[(df_stock['trade_date'] >= pd.to_datetime(start)) &
                           (df_stock['trade_date'] <= pd.to_datetime(end))]

        # 分析不同策略
        bear_results = analyze_bear_market_strategies(df_bear, cycle_name)
        all_bear_strategies[cycle_name] = bear_results

        for strategy, stats in bear_results.items():
            print(f"\n  {strategy}:")
            for n in HOLDING_DAYS:
                if f'{n}d_mean' in stats:
                    print(f"    {n}日持有: 均值={stats[f'{n}d_mean']:.2f}%, "
                          f"胜率={stats[f'{n}d_win_rate']:.1f}%, "
                          f"样本数={stats[f'{n}d_samples']}")

    # ========== Part 5: 熊市分阶段分析 ==========
    print("\n" + "=" * 70)
    print("Part 5: 阴跌熊市分阶段分析（第20轮 2021.02~2024.02）")
    print("=" * 70)

    # 将阴跌熊分为4个阶段
    bear20_phases = [
        ("初跌期(2021.02-2021.08)", "20210201", "20210801"),
        ("反弹期(2021.08-2021.12)", "20210801", "20211201"),
        ("主跌期(2022.01-2022.10)", "20220101", "20221001"),
        ("磨底期(2022.10-2024.02)", "20221001", "20240201"),
    ]

    # 加载整个熊市区间数据
    df_bear20 = load_stock_daily("20210201", "20240315")
    df_bear20 = calc_forward_returns(df_bear20, HOLDING_DAYS)

    for phase_name, p_start, p_end in bear20_phases:
        df_phase = df_bear20[(df_bear20['trade_date'] >= pd.to_datetime(p_start)) &
                              (df_bear20['trade_date'] <= pd.to_datetime(p_end))]
        if len(df_phase) == 0:
            continue

        stats = analyze_short_term_returns(df_phase, phase_name)
        print(f"\n  {phase_name}:")
        for n in HOLDING_DAYS:
            if f'{n}d_mean' in stats:
                print(f"    {n}日持有: 均值={stats[f'{n}d_mean']:.2f}%, "
                      f"胜率={stats[f'{n}d_win_rate']:.1f}%, "
                      f"盈利>3%={stats[f'{n}d_gt3pct']:.1f}%, "
                      f"亏损>3%={stats[f'{n}d_lt_neg3pct']:.1f}%")

    # ========== Part 6: 牛市末期 vs 牛转熊初期 对比 ==========
    print("\n" + "=" * 70)
    print("Part 6: 牛市末期 vs 牛转熊初期短线收益对比")
    print("=" * 70)

    transition_pairs = [
        ("第17轮慢牛末期", "20170701", "20180101",
         "第18轮熊市初期", "20180101", "20180701"),
        ("第19轮结构牛末期", "20200801", "20210201",
         "第20轮熊市初期", "20210201", "20210801"),
    ]

    for bull_name, b_start, b_end, bear_name, br_start, br_end in transition_pairs:
        print(f"\n--- {bull_name} vs {bear_name} ---")

        # 牛市末期
        br_end_ext = (pd.to_datetime(br_end) + pd.DateOffset(days=45)).strftime('%Y%m%d')
        df_all = load_stock_daily(b_start, br_end_ext)
        df_all = calc_forward_returns(df_all, HOLDING_DAYS)

        df_bull_late = df_all[(df_all['trade_date'] >= pd.to_datetime(b_start)) &
                               (df_all['trade_date'] <= pd.to_datetime(b_end))]
        df_bear_early = df_all[(df_all['trade_date'] >= pd.to_datetime(br_start)) &
                                (df_all['trade_date'] <= pd.to_datetime(br_end))]

        bull_stats = analyze_short_term_returns(df_bull_late, bull_name)
        bear_stats = analyze_short_term_returns(df_bear_early, bear_name)

        for n in HOLDING_DAYS:
            if f'{n}d_mean' in bull_stats and f'{n}d_mean' in bear_stats:
                print(f"  {n}日持有:")
                print(f"    {bull_name}: 均值={bull_stats[f'{n}d_mean']:.2f}%, "
                      f"胜率={bull_stats[f'{n}d_win_rate']:.1f}%, "
                      f"盈利>5%={bull_stats[f'{n}d_gt5pct']:.1f}%")
                print(f"    {bear_name}: 均值={bear_stats[f'{n}d_mean']:.2f}%, "
                      f"胜率={bear_stats[f'{n}d_win_rate']:.1f}%, "
                      f"盈利>5%={bear_stats[f'{n}d_gt5pct']:.1f}%")

    # ========== 汇总输出 ==========
    print("\n" + "=" * 70)
    print("汇总: 各阶段10日持有期核心指标对比")
    print("=" * 70)

    summary_rows = []

    # 牛市各阶段
    for cycle_name, comparison in all_bull_comparisons.items():
        for phase, stats in comparison.items():
            if '10d_mean' in stats:
                summary_rows.append({
                    '阶段': f'{cycle_name}-{phase}',
                    '10日均值(%)': f"{stats['10d_mean']:.2f}",
                    '10日胜率(%)': f"{stats['10d_win_rate']:.1f}",
                    '10日盈>5%(%)': f"{stats['10d_gt5pct']:.1f}",
                    '10日亏>5%(%)': f"{stats['10d_lt_neg5pct']:.1f}",
                    '10日中位数(%)': f"{stats['10d_median']:.2f}",
                })

    # 熊市策略
    for cycle_name, strategies in all_bear_strategies.items():
        for strategy, stats in strategies.items():
            if '10d_mean' in stats:
                summary_rows.append({
                    '阶段': f'{cycle_name}-{strategy}',
                    '10日均值(%)': f"{stats['10d_mean']:.2f}",
                    '10日胜率(%)': f"{stats['10d_win_rate']:.1f}",
                    '10日盈>5%(%)': f"{stats['10d_gt5pct']:.1f}",
                    '10日亏>5%(%)': f"{stats['10d_lt_neg5pct']:.1f}",
                    '10日中位数(%)': f"{stats['10d_median']:.2f}",
                })

    summary_df = pd.DataFrame(summary_rows)
    print(summary_df.to_string(index=False))

    print("\n分析完成！")


if __name__ == '__main__':
    main()
