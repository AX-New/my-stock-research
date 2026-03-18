"""
模块一：个股资金流分层验证

验证假设：
- H1: 大单净流入 top 分组有正向 alpha
- H2: 小单净流入 top 分组跑输（反向指标）
- H3: 全口径 net_mf_amount 无预测力或反转
- H4: MOD 修正后大单因子 alpha 增强

方法：
- 每日截面计算 4 个因子
- 按因子值分 5 档（Q1=top 20%, Q5=bottom 20%）
- 统计各档未来 1/3/5/10/20 日收益率

输出：统计数据到终端 + CSV，不生成报告。

用法：
    python analyze_stock_moneyflow.py [--start-date YYYYMMDD] [--end-date YYYYMMDD] [--output-dir DIR]
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
QUANTILE_LABELS = ['Q1_top', 'Q2', 'Q3', 'Q4', 'Q5_bot']


def filter_stocks(moneyflow_df: pd.DataFrame, market_df: pd.DataFrame,
                  stock_basic_df: pd.DataFrame) -> tuple:
    """
    过滤数据：排除 ST、停牌、非上市股。

    Returns:
        (filtered_moneyflow, filtered_market)
    """
    # 排除 ST 股和非上市股
    valid_codes = stock_basic_df[
        (stock_basic_df['list_status'] == 'L') &
        (~stock_basic_df['name'].str.contains('ST', na=False))
    ]['ts_code'].values
    print(f"[filter] 有效股票（非ST、已上市）: {len(valid_codes)} 只")

    moneyflow_df = moneyflow_df[moneyflow_df['ts_code'].isin(valid_codes)]

    # 排除停牌日（vol=0）
    suspended = market_df[market_df['vol'] == 0][['ts_code', 'trade_date']]
    if len(suspended) > 0:
        # 用 merge + indicator 排除停牌日
        moneyflow_df = moneyflow_df.merge(
            suspended, on=['ts_code', 'trade_date'], how='left', indicator=True
        )
        before = len(moneyflow_df)
        moneyflow_df = moneyflow_df[moneyflow_df['_merge'] == 'left_only'].drop(columns='_merge')
        print(f"[filter] 排除停牌日: {before} → {len(moneyflow_df)} 条")

    market_df = market_df[market_df['ts_code'].isin(valid_codes)]
    market_df = market_df[market_df['vol'] > 0]

    return moneyflow_df, market_df


def compute_factors(moneyflow_df: pd.DataFrame, pct_chg_df: pd.DataFrame) -> pd.DataFrame:
    """
    计算 4 个资金流因子。

    Args:
        moneyflow_df: 资金流数据
        pct_chg_df: DataFrame[ts_code, trade_date, pct_chg] 当日涨跌幅（MOD修正用）

    Returns:
        DataFrame[ts_code, trade_date, f_large, f_small, f_net, f_mod]
    """
    df = moneyflow_df.copy()

    # 因子1: 大单净流入 = (大单买-大单卖) + (特大单买-特大单卖)
    df['f_large'] = ((df['buy_lg_amount'] - df['sell_lg_amount']) +
                     (df['buy_elg_amount'] - df['sell_elg_amount']))

    # 因子2: 小单净流入
    df['f_small'] = df['buy_sm_amount'] - df['sell_sm_amount']

    # 因子3: 全口径净流入
    df['f_net'] = df['net_mf_amount']

    # 因子4: MOD 修正大单 — 需要 merge 涨跌幅
    df = df.merge(pct_chg_df[['ts_code', 'trade_date', 'pct_chg']],
                  on=['ts_code', 'trade_date'], how='left')

    # 每日截面回归: f_large = alpha + beta * pct_chg + epsilon
    # MOD修正因子 = epsilon（残差）
    def mod_regression(group):
        """对单日截面做 OLS 回归，返回残差"""
        y = group['f_large'].values
        x = group['pct_chg'].values
        # 过滤 NaN
        mask = ~(np.isnan(y) | np.isnan(x))
        if mask.sum() < 30:  # 样本太少跳过
            return pd.Series(np.nan, index=group.index)
        y_clean, x_clean = y[mask], x[mask]
        # OLS: beta = cov(x,y)/var(x), alpha = mean(y) - beta*mean(x)
        beta = np.cov(x_clean, y_clean)[0, 1] / (np.var(x_clean, ddof=1) + 1e-10)
        alpha = np.mean(y_clean) - beta * np.mean(x_clean)
        residuals = np.full(len(group), np.nan)
        residuals[mask] = y[mask] - (alpha + beta * x[mask])
        return pd.Series(residuals, index=group.index)

    print("[compute_factors] 计算 MOD 修正因子（每日截面回归）...")
    t0 = time.time()
    df['f_mod'] = df.groupby('trade_date', group_keys=False).apply(mod_regression)
    elapsed = time.time() - t0
    print(f"[compute_factors] MOD 回归完成 | {df['trade_date'].nunique()} 个交易日 | {elapsed:.1f}s")

    return df[['ts_code', 'trade_date', 'f_large', 'f_small', 'f_net', 'f_mod']]


def assign_quantiles(factors_df: pd.DataFrame, factor_col: str) -> pd.Series:
    """
    每日截面按因子值分 5 档。

    Q1=top 20%（因子值最大）, Q5=bottom 20%（因子值最小/最负）。

    Returns:
        Series of quantile labels
    """
    def _qcut(group):
        try:
            return pd.qcut(group[factor_col], 5, labels=QUANTILE_LABELS)
        except ValueError:
            # 数据量不足或值完全相同
            return pd.Series(np.nan, index=group.index)

    return factors_df.groupby('trade_date', group_keys=False).apply(_qcut)


def compute_quantile_stats(merged_df: pd.DataFrame, factor_name: str,
                           regime_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    计算分位组的收益统计。

    Args:
        merged_df: 含因子分位组和未来收益的 DataFrame
        factor_name: 因子名（用于输出标识）
        regime_df: DataFrame[trade_date, regime]（可选，用于分市场环境）

    Returns:
        统计结果 DataFrame
    """
    results = []

    # 如果有市场环境数据，添加 regime 列
    if regime_df is not None:
        merged_df = merged_df.merge(regime_df, on='trade_date', how='left')
        envs = [('all', merged_df), ('bull', merged_df[merged_df['regime'] == 'bull']),
                ('bear', merged_df[merged_df['regime'] == 'bear'])]
    else:
        envs = [('all', merged_df)]

    for env_name, env_df in envs:
        for q in QUANTILE_LABELS:
            q_df = env_df[env_df['quantile'] == q]
            if len(q_df) == 0:
                continue
            row = {'factor': factor_name, 'quantile': q, 'regime': env_name,
                   'sample_count': len(q_df)}
            for h in HORIZONS:
                col = f'ret_{h}d'
                if col in q_df.columns:
                    valid = q_df[col].dropna()
                    row[f'avg_{h}d'] = valid.mean()
                    row[f'med_{h}d'] = valid.median()
                    row[f'winrate_{h}d'] = (valid > 0).mean() * 100
                    row[f'count_{h}d'] = len(valid)
            results.append(row)

    return pd.DataFrame(results)


def compute_long_short_spread(stats_df: pd.DataFrame) -> pd.DataFrame:
    """
    计算 Q1-Q5 多空收益差。
    """
    results = []
    for factor in stats_df['factor'].unique():
        for regime in stats_df['regime'].unique():
            fdf = stats_df[(stats_df['factor'] == factor) & (stats_df['regime'] == regime)]
            q1 = fdf[fdf['quantile'] == 'Q1_top']
            q5 = fdf[fdf['quantile'] == 'Q5_bot']
            if len(q1) == 0 or len(q5) == 0:
                continue
            row = {'factor': factor, 'regime': regime}
            for h in HORIZONS:
                q1_avg = q1[f'avg_{h}d'].values[0] if f'avg_{h}d' in q1.columns else np.nan
                q5_avg = q5[f'avg_{h}d'].values[0] if f'avg_{h}d' in q5.columns else np.nan
                row[f'spread_{h}d'] = q1_avg - q5_avg
            results.append(row)
    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description='个股资金流分层验证')
    parser.add_argument('--start-date', default=None, help='开始日期 YYYYMMDD')
    parser.add_argument('--end-date', default=None, help='结束日期 YYYYMMDD')
    parser.add_argument('--output-dir', default='output', help='CSV 输出目录')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    t_start = time.time()

    # === 1. 加载数据 ===
    print("=" * 60)
    print("模块一：个股资金流分层验证")
    print("=" * 60)

    stock_basic = load_stock_basic()
    moneyflow = load_moneyflow(args.start_date, args.end_date)
    market = load_market_daily_for_returns(args.start_date, args.end_date)
    index_daily = load_index_daily("000001.SH")

    # === 2. 数据过滤 ===
    moneyflow, market = filter_stocks(moneyflow, market, stock_basic)

    # === 3. 计算未来收益 ===
    print("[main] 计算未来 N 日收益率...")
    market = compute_future_returns(market, HORIZONS)

    # === 4. 计算因子 ===
    pct_chg_df = market[['ts_code', 'trade_date', 'pct_chg']].copy()
    factors = compute_factors(moneyflow, pct_chg_df)

    # === 5. 合并因子与收益 ===
    returns_cols = ['ts_code', 'trade_date'] + [f'ret_{h}d' for h in HORIZONS]
    merged = factors.merge(market[returns_cols], on=['ts_code', 'trade_date'], how='inner')
    print(f"[main] 因子+收益合并完成 | {len(merged)} 条有效记录")

    # === 6. 市场环境 ===
    regime = get_market_regime(index_daily)

    # === 7. 分因子计算统计 ===
    factor_names = ['f_large', 'f_small', 'f_net', 'f_mod']
    factor_labels = {
        'f_large': '大单净流入',
        'f_small': '小单净流入',
        'f_net': '全口径净流入',
        'f_mod': 'MOD修正大单',
    }

    all_stats = []
    all_spreads = []

    for fname in factor_names:
        print(f"\n--- 因子: {factor_labels[fname]} ({fname}) ---")
        merged['quantile'] = assign_quantiles(merged, fname)
        valid = merged['quantile'].notna().sum()
        print(f"[main] 有效分档记录: {valid}/{len(merged)}")

        stats = compute_quantile_stats(merged, factor_labels[fname], regime)
        spreads = compute_long_short_spread(stats)

        all_stats.append(stats)
        all_spreads.append(spreads)

        # 打印 Q1 vs Q5 多空差
        print(f"\n{factor_labels[fname]} — Q1(top) vs Q5(bottom) 多空收益差:")
        for _, row in spreads[spreads['regime'] == 'all'].iterrows():
            parts = [f"{h}d: {row.get(f'spread_{h}d', float('nan')):+.3f}%"
                     for h in HORIZONS]
            print(f"  {' | '.join(parts)}")

    # === 8. 汇总输出 ===
    stats_all = pd.concat(all_stats, ignore_index=True)
    spreads_all = pd.concat(all_spreads, ignore_index=True)

    stats_path = os.path.join(args.output_dir, 'stock_moneyflow_quantile_stats.csv')
    spreads_path = os.path.join(args.output_dir, 'stock_moneyflow_long_short_spread.csv')
    stats_all.to_csv(stats_path, index=False, encoding='utf-8-sig')
    spreads_all.to_csv(spreads_path, index=False, encoding='utf-8-sig')

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"模块一完成 | 总耗时: {elapsed:.1f}s")
    print(f"统计输出: {stats_path}")
    print(f"多空差输出: {spreads_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
