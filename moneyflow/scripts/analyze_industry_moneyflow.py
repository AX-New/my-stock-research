"""
模块二：行业资金流双向验证

验证假设：
- H5: 行业资金持续流入后走弱（拥挤效应）
- H6: 行业资金流拐点（流出转流入）有短期正向预测力

测试内容：
1. 行业按当日 net_amount 分 5 档，统计后续收益（方向性验证）
2. 资金流拐点信号后续收益
3. 持续流入 vs 持续流出的行业后续收益对比

数据源: moneyflow_ind_dc（东财行业分类，~2023-09 起）
行业收益: pct_change 字段复合累计

输出：统计数据到终端 + CSV，不生成报告。

用法：
    python analyze_industry_moneyflow.py [--output-dir DIR]
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import argparse
import time
import pandas as pd
import numpy as np
from moneyflow_loader import load_industry_moneyflow, load_index_daily, get_market_regime


HORIZONS = [1, 3, 5, 10, 20]
QUANTILE_LABELS = ['Q1_top', 'Q2', 'Q3', 'Q4', 'Q5_bot']


def compute_industry_future_returns(df: pd.DataFrame, horizons: list) -> pd.DataFrame:
    """
    计算行业未来 N 日复合收益率。

    用 pct_change 字段复合计算: prod(1 + pct_change/100) - 1

    Args:
        df: 行业资金流数据，需含 name, trade_date, pct_change
        horizons: 观察周期列表

    Returns:
        新增 ret_Nd 列的 DataFrame
    """
    result = df.sort_values(['name', 'trade_date']).copy()

    for h in horizons:
        def _compound_return(group):
            """计算未来 h 日复合收益"""
            pct = group['pct_change'].values
            returns = np.full(len(pct), np.nan)
            for i in range(len(pct) - h):
                # 从 i+1 到 i+h（含）的复合收益
                compound = np.prod(1 + pct[i+1:i+1+h] / 100) - 1
                returns[i] = compound * 100  # 转百分比
            return pd.Series(returns, index=group.index)

        result[f'ret_{h}d'] = result.groupby('name', group_keys=False).apply(_compound_return)

    return result


def test_direction(df: pd.DataFrame, regime_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    测试一：行业资金流方向性验证（H5）

    每日将行业按 net_amount 排序分 5 档，统计后续收益。
    """
    results = []

    # 每日截面分位
    def _qcut(group):
        try:
            return pd.qcut(group['net_amount'], 5, labels=QUANTILE_LABELS)
        except ValueError:
            return pd.Series(np.nan, index=group.index)

    df = df.copy()
    df['quantile'] = df.groupby('trade_date', group_keys=False).apply(_qcut)

    if regime_df is not None:
        df = df.merge(regime_df, on='trade_date', how='left')
        envs = [('all', df), ('bull', df[df['regime'] == 'bull']),
                ('bear', df[df['regime'] == 'bear'])]
    else:
        envs = [('all', df)]

    for env_name, env_df in envs:
        for q in QUANTILE_LABELS:
            q_df = env_df[env_df['quantile'] == q]
            if len(q_df) == 0:
                continue
            row = {'test': 'direction', 'group': q, 'regime': env_name,
                   'sample_count': len(q_df)}
            for h in HORIZONS:
                col = f'ret_{h}d'
                valid = q_df[col].dropna()
                row[f'avg_{h}d'] = valid.mean()
                row[f'med_{h}d'] = valid.median()
                row[f'winrate_{h}d'] = (valid > 0).mean() * 100
                row[f'count_{h}d'] = len(valid)
            results.append(row)

    return pd.DataFrame(results)


def test_turning_points(df: pd.DataFrame) -> pd.DataFrame:
    """
    测试二：行业资金流拐点（H6）

    信号：连续 N 日流出后首日转流入（及反向）
    """
    results = []
    consecutive_days_list = [2, 3, 5]

    for industry in df['name'].unique():
        ind_df = df[df['name'] == industry].sort_values('trade_date').reset_index(drop=True)
        net = ind_df['net_amount'].values

        for n in consecutive_days_list:
            for i in range(n, len(ind_df)):
                # 流出转流入拐点: 前 N 日全为负，当日为正
                if all(net[i-j-1] < 0 for j in range(n)) and net[i] > 0:
                    for h in HORIZONS:
                        col = f'ret_{h}d'
                        if col in ind_df.columns and i < len(ind_df):
                            val = ind_df.iloc[i][col]
                            if not np.isnan(val):
                                results.append({
                                    'signal': f'out_to_in_N{n}',
                                    'industry': industry,
                                    'trade_date': ind_df.iloc[i]['trade_date'],
                                    'horizon': f'{h}d',
                                    'return': val,
                                })

                # 流入转流出拐点: 前 N 日全为正，当日为负
                if all(net[i-j-1] > 0 for j in range(n)) and net[i] < 0:
                    for h in HORIZONS:
                        col = f'ret_{h}d'
                        if col in ind_df.columns and i < len(ind_df):
                            val = ind_df.iloc[i][col]
                            if not np.isnan(val):
                                results.append({
                                    'signal': f'in_to_out_N{n}',
                                    'industry': industry,
                                    'trade_date': ind_df.iloc[i]['trade_date'],
                                    'horizon': f'{h}d',
                                    'return': val,
                                })

    if not results:
        return pd.DataFrame()

    raw = pd.DataFrame(results)

    # 按信号类型+观察周期汇总
    summary = []
    for signal in raw['signal'].unique():
        for horizon in raw['horizon'].unique():
            subset = raw[(raw['signal'] == signal) & (raw['horizon'] == horizon)]
            summary.append({
                'test': 'turning_point',
                'signal': signal,
                'horizon': horizon,
                'avg_return': subset['return'].mean(),
                'med_return': subset['return'].median(),
                'winrate': (subset['return'] > 0).mean() * 100,
                'sample_count': len(subset),
            })
    return pd.DataFrame(summary)


def test_persistence(df: pd.DataFrame) -> pd.DataFrame:
    """
    测试三：持续流入 vs 持续流出

    连续 3/5 日流入的行业 vs 连续 3/5 日流出的行业，后续收益对比。
    """
    results = []
    consecutive_days_list = [3, 5]

    for industry in df['name'].unique():
        ind_df = df[df['name'] == industry].sort_values('trade_date').reset_index(drop=True)
        net = ind_df['net_amount'].values

        for n in consecutive_days_list:
            for i in range(n - 1, len(ind_df)):
                # 连续 n 日流入（含当日）
                if all(net[i-j] > 0 for j in range(n)):
                    for h in HORIZONS:
                        col = f'ret_{h}d'
                        if col in ind_df.columns:
                            val = ind_df.iloc[i][col]
                            if not np.isnan(val):
                                results.append({
                                    'signal': f'consecutive_in_N{n}',
                                    'industry': industry,
                                    'horizon': f'{h}d',
                                    'return': val,
                                })

                # 连续 n 日流出（含当日）
                if all(net[i-j] < 0 for j in range(n)):
                    for h in HORIZONS:
                        col = f'ret_{h}d'
                        if col in ind_df.columns:
                            val = ind_df.iloc[i][col]
                            if not np.isnan(val):
                                results.append({
                                    'signal': f'consecutive_out_N{n}',
                                    'industry': industry,
                                    'horizon': f'{h}d',
                                    'return': val,
                                })

    if not results:
        return pd.DataFrame()

    raw = pd.DataFrame(results)

    summary = []
    for signal in raw['signal'].unique():
        for horizon in raw['horizon'].unique():
            subset = raw[(raw['signal'] == signal) & (raw['horizon'] == horizon)]
            summary.append({
                'test': 'persistence',
                'signal': signal,
                'horizon': horizon,
                'avg_return': subset['return'].mean(),
                'med_return': subset['return'].median(),
                'winrate': (subset['return'] > 0).mean() * 100,
                'sample_count': len(subset),
            })
    return pd.DataFrame(summary)


def main():
    parser = argparse.ArgumentParser(description='行业资金流双向验证')
    parser.add_argument('--output-dir', default='output', help='CSV 输出目录')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    t_start = time.time()

    print("=" * 60)
    print("模块二：行业资金流双向验证")
    print("=" * 60)

    # === 1. 加载数据 ===
    ind_mf = load_industry_moneyflow()
    index_daily = load_index_daily("000001.SH")
    regime = get_market_regime(index_daily)

    print(f"[main] 行业数: {ind_mf['name'].nunique()} | "
          f"日期范围: {ind_mf['trade_date'].min()} ~ {ind_mf['trade_date'].max()}")

    # === 2. 计算行业未来收益 ===
    print("[main] 计算行业未来 N 日复合收益率...")
    ind_mf = compute_industry_future_returns(ind_mf, HORIZONS)

    # === 3. 测试一：方向性验证 ===
    print("\n--- 测试一：行业资金流方向性验证 (H5) ---")
    direction_stats = test_direction(ind_mf, regime)
    if len(direction_stats) > 0:
        # 打印 Q1 vs Q5
        for env in direction_stats['regime'].unique():
            env_df = direction_stats[direction_stats['regime'] == env]
            q1 = env_df[env_df['group'] == 'Q1_top']
            q5 = env_df[env_df['group'] == 'Q5_bot']
            if len(q1) > 0 and len(q5) > 0:
                print(f"\n  [{env}] Q1(流入最多) vs Q5(流出最多) 收益差:")
                for h in HORIZONS:
                    q1_avg = q1[f'avg_{h}d'].values[0]
                    q5_avg = q5[f'avg_{h}d'].values[0]
                    print(f"    {h}d: Q1={q1_avg:+.3f}% | Q5={q5_avg:+.3f}% | 差={q1_avg-q5_avg:+.3f}%")

    # === 4. 测试二：拐点信号 ===
    print("\n--- 测试二：行业资金流拐点信号 (H6) ---")
    turning_stats = test_turning_points(ind_mf)
    if len(turning_stats) > 0:
        for signal in turning_stats['signal'].unique():
            print(f"\n  信号: {signal}")
            s_df = turning_stats[turning_stats['signal'] == signal]
            for _, row in s_df.iterrows():
                print(f"    {row['horizon']}: avg={row['avg_return']:+.3f}% | "
                      f"winrate={row['winrate']:.1f}% | n={row['sample_count']}")

    # === 5. 测试三：持续流入/流出 ===
    print("\n--- 测试三：持续流入 vs 持续流出 ---")
    persist_stats = test_persistence(ind_mf)
    if len(persist_stats) > 0:
        for signal in sorted(persist_stats['signal'].unique()):
            print(f"\n  信号: {signal}")
            s_df = persist_stats[persist_stats['signal'] == signal]
            for _, row in s_df.iterrows():
                print(f"    {row['horizon']}: avg={row['avg_return']:+.3f}% | "
                      f"winrate={row['winrate']:.1f}% | n={row['sample_count']}")

    # === 6. 输出 CSV ===
    direction_stats.to_csv(os.path.join(args.output_dir, 'industry_direction_stats.csv'),
                           index=False, encoding='utf-8-sig')
    turning_stats.to_csv(os.path.join(args.output_dir, 'industry_turning_point_stats.csv'),
                         index=False, encoding='utf-8-sig')
    persist_stats.to_csv(os.path.join(args.output_dir, 'industry_persistence_stats.csv'),
                         index=False, encoding='utf-8-sig')

    elapsed = time.time() - t_start
    print(f"\n{'=' * 60}")
    print(f"模块二完成 | 总耗时: {elapsed:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
