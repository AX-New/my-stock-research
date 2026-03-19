"""
牛市末期策略选股短线分析

核心问题：上一版分析只看了全市场"随机买入"的平均胜率，没有结合选股策略。
本脚本测试：通过技术指标选股，能否在牛市末期提升短线成功率？

策略列表：
1. MACD金叉 — DIF上穿DEA，经典趋势跟随
2. RSI超卖反弹 — RSI(14)<30，均值回归
3. 放量突破MA20 — 价格站上MA20+放量，动量突破
4. 缩量回踩MA20 — 价格回踩MA20附近+缩量+收阳，趋势延续
5. 大单净流入 — 主力资金流入
6. MACD金叉+大单 — 多因子组合

对比维度：牛市前半段/中段/末期6个月，各策略 vs 随机基准
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from lib.database import read_engine
import time

# ========== 牛熊周期定义 ==========
BULL_CYCLES = [
    ("第17轮慢牛", "20160101", "20180101", "慢牛"),
    ("第19轮结构牛", "20190101", "20210201", "结构牛"),
]

HOLDING_DAYS = [5, 10, 20]


def load_stock_daily(start_date, end_date):
    """
    加载个股日线数据（前复权）
    包含OHLCV和复权因子，计算前复权价格
    """
    print(f"  加载个股日线: {start_date} ~ {end_date} ...")
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
    for col in ['open', 'high', 'low', 'close']:
        df[f'adj_{col}'] = df[col] * df['adj_factor'] / latest_adj

    print(f"    {len(df)} 条, {df['ts_code'].nunique()} 只股票")
    return df


def load_moneyflow(start_date, end_date):
    """加载资金流向数据"""
    print(f"  加载资金流向: {start_date} ~ {end_date} ...")
    sql = f"""
    SELECT ts_code, trade_date,
           buy_lg_amount, sell_lg_amount,
           buy_elg_amount, sell_elg_amount
    FROM moneyflow
    WHERE trade_date >= '{start_date}'
    AND trade_date <= '{end_date}'
    """
    df = pd.read_sql(sql, read_engine)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    # 大单净流入 = (大单买入-大单卖出) + (超大单买入-超大单卖出)
    df['big_net_inflow'] = (df['buy_lg_amount'] - df['sell_lg_amount'] +
                            df['buy_elg_amount'] - df['sell_elg_amount'])
    print(f"    {len(df)} 条")
    return df[['ts_code', 'trade_date', 'big_net_inflow']]


def calc_technical_indicators(df):
    """
    计算技术指标：MACD、RSI、MA、成交量均线
    输入：按ts_code, trade_date排序的日线数据
    输出：增加指标列
    """
    print("  计算技术指标...")
    t0 = time.time()

    results = []
    total = df['ts_code'].nunique()
    processed = 0

    for ts_code, group in df.groupby('ts_code'):
        g = group.sort_values('trade_date').copy()

        # === MACD (12, 26, 9) ===
        ema12 = g['adj_close'].ewm(span=12, adjust=False).mean()
        ema26 = g['adj_close'].ewm(span=26, adjust=False).mean()
        g['dif'] = ema12 - ema26
        g['dea'] = g['dif'].ewm(span=9, adjust=False).mean()
        g['macd_hist'] = (g['dif'] - g['dea']) * 2
        # MACD金叉信号：DIF从下方穿越DEA
        g['macd_golden'] = ((g['dif'] > g['dea']) &
                            (g['dif'].shift(1) <= g['dea'].shift(1))).astype(int)

        # === RSI(14) ===
        delta = g['adj_close'].diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        g['rsi_14'] = 100 - (100 / (1 + rs))

        # === 均线 ===
        g['ma5'] = g['adj_close'].rolling(5).mean()
        g['ma10'] = g['adj_close'].rolling(10).mean()
        g['ma20'] = g['adj_close'].rolling(20).mean()

        # === 成交量均线 ===
        g['vol_ma20'] = g['vol'].rolling(20).mean()

        # === 放量突破MA20信号 ===
        # 收盘价站上MA20 + 前一日在MA20下方 + 成交量>1.5倍20日均量
        g['break_ma20'] = ((g['adj_close'] > g['ma20']) &
                           (g['adj_close'].shift(1) <= g['ma20'].shift(1)) &
                           (g['vol'] > g['vol_ma20'] * 1.5)).astype(int)

        # === 缩量回踩MA20支撑信号 ===
        # 价格在MA20附近(±3%) + 成交量<均量70% + 当日收阳 + MA20向上
        ma20_dist = abs(g['adj_close'] - g['ma20']) / g['ma20']
        g['pullback_ma20'] = ((ma20_dist < 0.03) &
                              (g['vol'] < g['vol_ma20'] * 0.7) &
                              (g['adj_close'] > g['adj_open']) &
                              (g['ma20'] > g['ma20'].shift(1))).astype(int)

        # === RSI超卖信号 ===
        g['rsi_oversold'] = (g['rsi_14'] < 30).astype(int)

        results.append(g)
        processed += 1
        if processed % 1000 == 0:
            print(f"    已处理 {processed}/{total} 只股票...")

    result = pd.concat(results, ignore_index=True)
    print(f"    技术指标计算完成, 耗时 {time.time()-t0:.1f}s")
    return result


def calc_forward_returns(df):
    """计算前瞻N日收益率"""
    print("  计算前瞻收益率...")
    results = []
    for ts_code, group in df.groupby('ts_code'):
        g = group.sort_values('trade_date').reset_index(drop=True)
        for n in HOLDING_DAYS:
            g[f'ret_{n}d'] = g['adj_close'].shift(-n) / g['adj_close'] - 1
        results.append(g)
    return pd.concat(results, ignore_index=True)


def analyze_returns(df, label):
    """
    计算收益统计
    返回包含各持有期统计的dict
    """
    stats = {'label': label, 'samples': len(df)}
    for n in HOLDING_DAYS:
        col = f'ret_{n}d'
        valid = df[col].dropna()
        if len(valid) < 30:  # 样本太少不统计
            continue
        stats[f'{n}d_n'] = len(valid)
        stats[f'{n}d_mean'] = valid.mean() * 100
        stats[f'{n}d_median'] = valid.median() * 100
        stats[f'{n}d_win'] = (valid > 0).mean() * 100
        stats[f'{n}d_gt3'] = (valid > 0.03).mean() * 100
        stats[f'{n}d_gt5'] = (valid > 0.05).mean() * 100
        stats[f'{n}d_lt3'] = (valid < -0.03).mean() * 100
        stats[f'{n}d_lt5'] = (valid < -0.05).mean() * 100
        # 盈亏比
        avg_win = valid[valid > 0].mean() if (valid > 0).any() else 0
        avg_loss = abs(valid[valid < 0].mean()) if (valid < 0).any() else 1
        stats[f'{n}d_pl_ratio'] = avg_win / avg_loss if avg_loss > 0 else 0
    return stats


def print_comparison_table(all_stats, title, holding=10):
    """打印策略对比表格"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"  持有期: {holding}日")
    print(f"{'='*80}")
    print(f"{'策略':<25} {'样本数':>8} {'均值%':>8} {'中位数%':>8} {'胜率%':>8} "
          f"{'盈>3%':>8} {'亏>5%':>8} {'盈亏比':>8}")
    print("-" * 97)

    prefix = f'{holding}d_'
    for s in all_stats:
        n = s.get(f'{prefix}n', 0)
        if n == 0:
            print(f"{s['label']:<25} {'样本不足':>8}")
            continue
        print(f"{s['label']:<25} {n:>8} "
              f"{s[f'{prefix}mean']:>8.2f} "
              f"{s[f'{prefix}median']:>8.2f} "
              f"{s[f'{prefix}win']:>8.1f} "
              f"{s[f'{prefix}gt3']:>8.1f} "
              f"{s[f'{prefix}lt5']:>8.1f} "
              f"{s[f'{prefix}pl_ratio']:>8.2f}")


def analyze_one_cycle(cycle_name, start, end, subtype):
    """
    分析一轮牛市各阶段的策略表现
    """
    print(f"\n{'#'*80}")
    print(f"# {cycle_name} ({subtype})  {start} ~ {end}")
    print(f"{'#'*80}")

    # 需要额外往前加载数据用于计算技术指标（至少26天MACD预热）
    pre_start = (pd.to_datetime(start) - pd.DateOffset(days=60)).strftime('%Y%m%d')
    # 往后加载数据用于计算持有期收益
    post_end = (pd.to_datetime(end) + pd.DateOffset(days=45)).strftime('%Y%m%d')

    # 加载数据
    df = load_stock_daily(pre_start, post_end)
    mf = load_moneyflow(start, end)

    # 计算技术指标
    df = calc_technical_indicators(df)

    # 计算前瞻收益
    df = calc_forward_returns(df)

    # 合并资金流向
    df = df.merge(mf, on=['ts_code', 'trade_date'], how='left')
    # 大单净流入信号：当日大单净流入 > 0
    df['big_inflow_signal'] = (df['big_net_inflow'] > 0).astype(int)
    # MACD金叉 + 大单净流入组合
    df['macd_bigflow'] = ((df['macd_golden'] == 1) &
                          (df['big_inflow_signal'] == 1)).astype(int)

    # 只取牛市区间内的数据
    dt_start = pd.to_datetime(start)
    dt_end = pd.to_datetime(end)
    df_bull = df[(df['trade_date'] >= dt_start) & (df['trade_date'] <= dt_end)].copy()

    # 划分阶段
    mid_point = dt_start + (dt_end - dt_start) / 2
    last_6m = dt_end - pd.DateOffset(months=6)

    phases = {
        '前半段': df_bull[(df_bull['trade_date'] >= dt_start) & (df_bull['trade_date'] < mid_point)],
        '中段': df_bull[(df_bull['trade_date'] >= mid_point) & (df_bull['trade_date'] < last_6m)],
        '末期6月': df_bull[(df_bull['trade_date'] >= last_6m) & (df_bull['trade_date'] <= dt_end)],
    }

    # 策略定义
    strategies = {
        '基准(随机)': lambda d: d,
        'MACD金叉': lambda d: d[d['macd_golden'] == 1],
        'RSI超卖(<30)': lambda d: d[d['rsi_oversold'] == 1],
        '放量突破MA20': lambda d: d[d['break_ma20'] == 1],
        '缩量回踩MA20': lambda d: d[d['pullback_ma20'] == 1],
        '大单净流入': lambda d: d[d['big_inflow_signal'] == 1],
        'MACD金叉+大单': lambda d: d[d['macd_bigflow'] == 1],
    }

    # 收集所有结果
    all_results = {}

    for phase_name, phase_df in phases.items():
        print(f"\n--- {cycle_name} · {phase_name} ({len(phase_df)} 条日线) ---")
        phase_stats = []

        for strat_name, strat_filter in strategies.items():
            filtered = strat_filter(phase_df)
            stats = analyze_returns(filtered, strat_name)
            phase_stats.append(stats)

        all_results[phase_name] = phase_stats

        # 打印各持有期对比
        for h in HOLDING_DAYS:
            print_comparison_table(phase_stats,
                                   f"{cycle_name} · {phase_name}",
                                   holding=h)

    return all_results


def print_cross_phase_summary(cycle_name, results):
    """
    跨阶段对比：同一策略在前半段/中段/末期的变化
    重点看策略能否在末期保持优势
    """
    print(f"\n{'='*80}")
    print(f"  跨阶段对比总结: {cycle_name}")
    print(f"  关键指标: 10日持有期")
    print(f"{'='*80}")

    strategies = ['基准(随机)', 'MACD金叉', 'RSI超卖(<30)', '放量突破MA20',
                  '缩量回踩MA20', '大单净流入', 'MACD金叉+大单']

    print(f"\n{'策略':<25} {'前半段':>12} {'中段':>12} {'末期6月':>12} {'末期vs基准':>12}")
    print(f"{'':25} {'胜率/均值':>12} {'胜率/均值':>12} {'胜率/均值':>12} {'胜率差':>12}")
    print("-" * 73)

    # 找到末期基准的胜率
    base_end_win = None
    for s in results.get('末期6月', []):
        if s['label'] == '基准(随机)' and '10d_win' in s:
            base_end_win = s['10d_win']
            break

    for strat in strategies:
        row = f"{strat:<25}"
        end_win = None
        for phase in ['前半段', '中段', '末期6月']:
            found = False
            for s in results.get(phase, []):
                if s['label'] == strat and '10d_win' in s:
                    win = s['10d_win']
                    mean = s['10d_mean']
                    row += f" {win:5.1f}/{mean:+5.2f}"
                    if phase == '末期6月':
                        end_win = win
                    found = True
                    break
            if not found:
                row += f" {'--':>12}"

        # 末期策略vs末期基准
        if end_win is not None and base_end_win is not None:
            diff = end_win - base_end_win
            row += f" {diff:+8.1f}pp"
        else:
            row += f" {'--':>12}"

        print(row)


def analyze_strategy_consistency(all_cycle_results):
    """
    分析策略在不同牛市周期中的一致性
    如果一个策略在两轮牛市末期都能提升胜率，才有参考价值
    """
    print(f"\n{'#'*80}")
    print(f"# 策略一致性分析：两轮牛市末期对比")
    print(f"{'#'*80}")

    strategies = ['基准(随机)', 'MACD金叉', 'RSI超卖(<30)', '放量突破MA20',
                  '缩量回踩MA20', '大单净流入', 'MACD金叉+大单']

    print(f"\n{'策略':<25} {'第17轮慢牛末期':>18} {'第19轮结构牛末期':>18} {'两轮平均':>12} {'一致性':>8}")
    print(f"{'':25} {'胜率%  均值%':>18} {'胜率%  均值%':>18} {'胜率提升':>12} {'':>8}")
    print("-" * 95)

    # 先找各轮基准
    base_wins = {}
    for cycle_name, results in all_cycle_results.items():
        for s in results.get('末期6月', []):
            if s['label'] == '基准(随机)' and '10d_win' in s:
                base_wins[cycle_name] = s['10d_win']

    for strat in strategies:
        row = f"{strat:<25}"
        diffs = []
        for cycle_name in ['第17轮慢牛', '第19轮结构牛']:
            results = all_cycle_results.get(cycle_name, {})
            found = False
            for s in results.get('末期6月', []):
                if s['label'] == strat and '10d_win' in s:
                    win = s['10d_win']
                    mean = s['10d_mean']
                    row += f" {win:6.1f} {mean:+6.2f}  "
                    if strat != '基准(随机)' and cycle_name in base_wins:
                        diffs.append(win - base_wins[cycle_name])
                    found = True
                    break
            if not found:
                row += f" {'--':>18}"

        if len(diffs) == 2:
            avg_diff = np.mean(diffs)
            consistent = "YES" if diffs[0] > 0 and diffs[1] > 0 else "NO"
            row += f" {avg_diff:+8.1f}pp   {consistent}"
        elif strat == '基准(随机)':
            row += f" {'(基准)':>12} {'':>8}"
        else:
            row += f" {'--':>12} {'--':>8}"

        print(row)


def main():
    print("=" * 80)
    print("牛市末期策略选股短线分析")
    print("核心问题：通过技术指标选股，能否在牛市末期提升短线成功率？")
    print("=" * 80)
    t_start = time.time()

    all_cycle_results = {}

    for cycle_name, start, end, subtype in BULL_CYCLES:
        results = analyze_one_cycle(cycle_name, start, end, subtype)
        all_cycle_results[cycle_name] = results

        # 打印该周期的跨阶段对比
        print_cross_phase_summary(cycle_name, results)

    # 两轮牛市策略一致性对比
    analyze_strategy_consistency(all_cycle_results)

    elapsed = time.time() - t_start
    print(f"\n分析完成，总耗时 {elapsed:.0f}s")


if __name__ == '__main__':
    main()
