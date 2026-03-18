"""
模块三：聪明钱+拥挤度 共振验证

验证假设：
- H7: 大单流入 + 行业未过热 = 最优组合

信号分组：
- A: 聪明钱+未拥挤（MOD大单top20% + 行业3日净流入<=0）
- B: 聪明钱+已拥挤（MOD大单top20% + 行业3日净流入>0）
- C: 仅行业热（行业3日正，个股不在top20%）
- D: 仅个股强（MOD大单top20%，行业不满足连续3日条件）
- E: 对照（其余）

行业资金流由个股 moneyflow 按 stock_basic.industry 聚合生成。

输出：统计数据到终端 + CSV，不生成报告。

用法：
    python analyze_moneyflow_resonance.py [--start-date YYYYMMDD] [--end-date YYYYMMDD] [--output-dir DIR]
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import argparse
import time
import pandas as pd
import numpy as np
from moneyflow_loader import (
    load_stock_basic, load_moneyflow, load_market_daily_for_returns,
    load_index_daily, compute_future_returns, get_market_regime
)


HORIZONS = [1, 3, 5, 10, 20]
GROUPS = ['A_smart_uncrowded', 'B_smart_crowded', 'C_industry_hot',
          'D_stock_only', 'E_control']
GROUP_LABELS = {
    'A_smart_uncrowded': 'A: 聪明钱+未拥挤',
    'B_smart_crowded': 'B: 聪明钱+已拥挤',
    'C_industry_hot': 'C: 仅行业热',
    'D_stock_only': 'D: 仅个股强',
    'E_control': 'E: 对照组',
}


def build_industry_flow(moneyflow_df: pd.DataFrame,
                        stock_basic_df: pd.DataFrame) -> pd.DataFrame:
    """
    将个股资金流按行业聚合，构建行业级资金流。

    Args:
        moneyflow_df: 个股 moneyflow 数据
        stock_basic_df: 含 ts_code, industry 的映射

    Returns:
        DataFrame[industry, trade_date, ind_net_mf]
    """
    # 映射个股到行业
    mapping = stock_basic_df[['ts_code', 'industry']].dropna()
    df = moneyflow_df.merge(mapping, on='ts_code', how='inner')

    # 按行业+日期聚合
    ind_flow = df.groupby(['industry', 'trade_date'], as_index=False)['net_mf_amount'].sum()
    ind_flow.rename(columns={'net_mf_amount': 'ind_net_mf'}, inplace=True)

    print(f"[build_industry_flow] 行业数: {ind_flow['industry'].nunique()} | "
          f"记录数: {len(ind_flow)}")
    return ind_flow


def compute_industry_consecutive(ind_flow: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """
    判断行业是否连续 N 日净流入 > 0 或 <= 0。

    Returns:
        DataFrame[industry, trade_date, consec_in (bool), consec_out (bool)]
    """
    result = ind_flow.sort_values(['industry', 'trade_date']).copy()

    def _check_consecutive(group):
        net = group['ind_net_mf'].values
        consec_in = np.zeros(len(net), dtype=bool)
        consec_out = np.zeros(len(net), dtype=bool)
        for i in range(n - 1, len(net)):
            if all(net[i-j] > 0 for j in range(n)):
                consec_in[i] = True
            if all(net[i-j] <= 0 for j in range(n)):
                consec_out[i] = True
        return pd.DataFrame({
            'consec_in': consec_in,
            'consec_out': consec_out,
        }, index=group.index)

    consec = result.groupby('industry', group_keys=False).apply(_check_consecutive)
    result['consec_in'] = consec['consec_in']
    result['consec_out'] = consec['consec_out']

    return result[['industry', 'trade_date', 'consec_in', 'consec_out']]


def compute_mod_factor(moneyflow_df: pd.DataFrame,
                       pct_chg_df: pd.DataFrame) -> pd.DataFrame:
    """
    计算 MOD 修正大单因子。

    Returns:
        DataFrame[ts_code, trade_date, f_mod, is_top20]
    """
    # 只取需要的列，减少内存（原始 moneyflow 有 11 列，这里只需 4 列算 f_large）
    df = moneyflow_df[['ts_code', 'trade_date', 'buy_lg_amount', 'sell_lg_amount',
                        'buy_elg_amount', 'sell_elg_amount']].copy()
    df['f_large'] = ((df['buy_lg_amount'] - df['sell_lg_amount']) +
                     (df['buy_elg_amount'] - df['sell_elg_amount']))
    # 释放不再需要的列
    df.drop(columns=['buy_lg_amount', 'sell_lg_amount', 'buy_elg_amount', 'sell_elg_amount'],
            inplace=True)

    df = df.merge(pct_chg_df[['ts_code', 'trade_date', 'pct_chg']],
                  on=['ts_code', 'trade_date'], how='left')

    # 每日截面 OLS 回归取残差
    def mod_regression(group):
        y = group['f_large'].values
        x = group['pct_chg'].values
        mask = ~(np.isnan(y) | np.isnan(x))
        if mask.sum() < 30:
            return pd.Series(np.nan, index=group.index)
        y_clean, x_clean = y[mask], x[mask]
        beta = np.cov(x_clean, y_clean)[0, 1] / (np.var(x_clean, ddof=1) + 1e-10)
        alpha = np.mean(y_clean) - beta * np.mean(x_clean)
        residuals = np.full(len(group), np.nan)
        residuals[mask] = y[mask] - (alpha + beta * x[mask])
        return pd.Series(residuals, index=group.index)

    print("[compute_mod_factor] 计算 MOD 修正因子...")
    t0 = time.time()
    df['f_mod'] = df.groupby('trade_date', group_keys=False).apply(mod_regression)
    elapsed = time.time() - t0
    print(f"[compute_mod_factor] 完成 | {elapsed:.1f}s")

    # 每日截面 top 20%
    def _mark_top20(group):
        threshold = group['f_mod'].quantile(0.8)
        return group['f_mod'] >= threshold

    df['is_top20'] = df.groupby('trade_date', group_keys=False).apply(_mark_top20)

    return df[['ts_code', 'trade_date', 'f_mod', 'is_top20']]


def assign_signal_groups(mod_df: pd.DataFrame, ind_consec: pd.DataFrame,
                         stock_basic_df: pd.DataFrame) -> pd.DataFrame:
    """
    将每只股票每日分配到 A/B/C/D/E 信号组。

    Args:
        mod_df: 含 is_top20 的 MOD 因子数据
        ind_consec: 含 consec_in, consec_out 的行业连续性数据
        stock_basic_df: 个股-行业映射

    Returns:
        DataFrame[ts_code, trade_date, signal_group]
    """
    # 映射行业
    mapping = stock_basic_df[['ts_code', 'industry']].dropna()
    df = mod_df.merge(mapping, on='ts_code', how='inner')

    # 合并行业连续性
    df = df.merge(ind_consec, on=['industry', 'trade_date'], how='left')
    df['consec_in'] = df['consec_in'].fillna(False)
    df['consec_out'] = df['consec_out'].fillna(False)

    # 分组逻辑（向量化，避免 apply(axis=1) 在百万行上的性能问题）
    smart = df['is_top20']
    hot = df['consec_in']
    cold = df['consec_out']

    conditions = [
        smart & cold,                    # A: 聪明钱 + 行业未过热
        smart & hot,                     # B: 聪明钱 + 行业已拥挤
        hot & ~smart,                    # C: 仅行业热
        smart & ~hot & ~cold,            # D: 仅个股强，行业中性
    ]
    choices = ['A_smart_uncrowded', 'B_smart_crowded', 'C_industry_hot', 'D_stock_only']
    df['signal_group'] = np.select(conditions, choices, default='E_control')

    return df[['ts_code', 'trade_date', 'signal_group']]


def compute_group_stats(merged_df: pd.DataFrame,
                        regime_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    计算各信号组的收益统计。
    """
    results = []

    if regime_df is not None:
        merged_df = merged_df.merge(regime_df, on='trade_date', how='left')
        envs = [('all', merged_df), ('bull', merged_df[merged_df['regime'] == 'bull']),
                ('bear', merged_df[merged_df['regime'] == 'bear'])]
    else:
        envs = [('all', merged_df)]

    for env_name, env_df in envs:
        for group in GROUPS:
            g_df = env_df[env_df['signal_group'] == group]
            if len(g_df) == 0:
                continue
            row = {'group': group, 'label': GROUP_LABELS.get(group, group),
                   'regime': env_name, 'sample_count': len(g_df)}
            for h in HORIZONS:
                col = f'ret_{h}d'
                if col in g_df.columns:
                    valid = g_df[col].dropna()
                    row[f'avg_{h}d'] = valid.mean()
                    row[f'med_{h}d'] = valid.median()
                    row[f'winrate_{h}d'] = (valid > 0).mean() * 100
                    row[f'count_{h}d'] = len(valid)
            results.append(row)

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description='聪明钱+拥挤度共振验证')
    parser.add_argument('--start-date', default=None, help='开始日期 YYYYMMDD')
    parser.add_argument('--end-date', default=None, help='结束日期 YYYYMMDD')
    parser.add_argument('--output-dir', default='output', help='CSV 输出目录')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    t_start = time.time()

    print("=" * 60)
    print("模块三：聪明钱+拥挤度共振验证")
    print("=" * 60)

    # === 1. 加载数据 ===
    stock_basic = load_stock_basic()
    moneyflow = load_moneyflow(args.start_date, args.end_date)
    market = load_market_daily_for_returns(args.start_date, args.end_date)
    index_daily = load_index_daily("000001.SH")

    # === 2. 过滤 ===
    valid_codes = stock_basic[
        (stock_basic['list_status'] == 'L') &
        (~stock_basic['name'].str.contains('ST', na=False))
    ]['ts_code'].values
    moneyflow = moneyflow[moneyflow['ts_code'].isin(valid_codes)]
    market = market[(market['ts_code'].isin(valid_codes)) & (market['vol'] > 0)]
    print(f"[main] 过滤后: moneyflow {len(moneyflow)} 条, market {len(market)} 条")

    # === 3. 构建行业级资金流 ===
    ind_flow = build_industry_flow(moneyflow, stock_basic)

    # === 4. 行业连续性判断 ===
    ind_consec = compute_industry_consecutive(ind_flow, n=3)

    # === 5. 计算 MOD 因子 ===
    pct_chg_df = market[['ts_code', 'trade_date', 'pct_chg']].copy()
    mod_factors = compute_mod_factor(moneyflow, pct_chg_df)
    del moneyflow, pct_chg_df  # 释放内存
    import gc; gc.collect()

    # === 6. 分配信号组 ===
    signal_groups = assign_signal_groups(mod_factors, ind_consec, stock_basic)

    # === 7. 计算未来收益 ===
    print("[main] 计算未来 N 日收益率...")
    market = compute_future_returns(market, HORIZONS)

    # === 8. 合并信号组与收益 ===
    returns_cols = ['ts_code', 'trade_date'] + [f'ret_{h}d' for h in HORIZONS]
    merged = signal_groups.merge(market[returns_cols], on=['ts_code', 'trade_date'], how='inner')
    print(f"[main] 合并完成 | {len(merged)} 条有效记录")

    # 信号组分布
    print("\n--- 信号组分布 ---")
    for group in GROUPS:
        count = (merged['signal_group'] == group).sum()
        pct = count / len(merged) * 100
        print(f"  {GROUP_LABELS.get(group, group)}: {count:,} ({pct:.1f}%)")

    # === 9. 统计 ===
    regime = get_market_regime(index_daily)
    stats = compute_group_stats(merged, regime)

    # 打印核心对比: A vs B vs E
    print("\n--- 核心对比: A(未拥挤) vs B(已拥挤) vs E(对照) ---")
    for env in stats['regime'].unique():
        env_df = stats[stats['regime'] == env]
        print(f"\n  [{env}]")
        for group in ['A_smart_uncrowded', 'B_smart_crowded', 'E_control']:
            g_df = env_df[env_df['group'] == group]
            if len(g_df) == 0:
                continue
            parts = [f"{h}d: {g_df[f'avg_{h}d'].values[0]:+.3f}%" for h in HORIZONS]
            label = GROUP_LABELS.get(group, group)
            print(f"    {label}: {' | '.join(parts)}")

    # === 10. 输出 CSV ===
    stats_path = os.path.join(args.output_dir, 'resonance_group_stats.csv')
    stats.to_csv(stats_path, index=False, encoding='utf-8-sig')

    # 信号频率统计
    freq_stats = merged.groupby(['signal_group', 'trade_date']).size().reset_index(name='count')
    daily_freq = freq_stats.groupby('signal_group')['count'].mean().reset_index()
    daily_freq.columns = ['signal_group', 'avg_daily_count']
    freq_path = os.path.join(args.output_dir, 'resonance_signal_frequency.csv')
    daily_freq.to_csv(freq_path, index=False, encoding='utf-8-sig')

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"模块三完成 | 总耗时: {elapsed:.1f}s")
    print(f"统计输出: {stats_path}")
    print(f"频率输出: {freq_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
