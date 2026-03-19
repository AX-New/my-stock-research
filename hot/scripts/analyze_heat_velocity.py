"""
热度涨速与股价关系分析脚本

核心研究问题：
  1. 个股热度排名变化速度（涨速）是否对后续股价有预测力？
  2. 热度涨速与资金流向之间的关系如何？
  3. 先验热度涨速（T-N天的热度变化）对T+1~T+5收益的预测力

数据源:
  - my_trend.popularity_rank: 东财人气排名 (2025-03-15 ~ 2026-03-19)
  - my_stock.market_daily: 个股日线行情
  - my_stock.moneyflow: 个股资金流向

用法:
  python hot/scripts/analyze_heat_velocity.py [--part 1|2|3|all]
    part 1: 热度涨速 vs 后续收益分析
    part 2: 热度涨速 vs 资金流向关联分析
    part 3: 综合多因子分析
"""

import argparse
import sys
import time
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

warnings.filterwarnings('ignore')

# === 配置 ===
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3307
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'

TREND_DB_URL = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/my_trend?charset=utf8mb4'
STOCK_DB_URL = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/my_stock?charset=utf8mb4'

# 输出目录
OUTPUT_DIR = Path(__file__).resolve().parent.parent / 'data'
OUTPUT_DIR.mkdir(exist_ok=True)


def get_engines():
    """创建数据库连接"""
    return create_engine(TREND_DB_URL), create_engine(STOCK_DB_URL)


def code_to_tscode(code: str) -> str:
    """stock_code → ts_code: 6/9开头=SH, 其他=SZ"""
    if code.startswith(('6', '9')):
        return f"{code}.SH"
    return f"{code}.SZ"


def load_heat_data(trend_engine):
    """加载热度排名数据"""
    print(">>> 加载热度排名数据...")
    t0 = time.time()
    sql = """
        SELECT stock_code, date, `rank`, new_price, change_rate,
               volume_ratio, turnover_rate, deal_amount
        FROM popularity_rank
        WHERE date >= '2025-03-15'
        ORDER BY stock_code, date
    """
    df = pd.read_sql(sql, trend_engine)
    df['date'] = pd.to_datetime(df['date'])
    # 转换为 trade_date 格式 (YYYYMMDD) 以便与行情表关联
    df['trade_date'] = df['date'].dt.strftime('%Y%m%d')
    # 转换 ts_code
    df['ts_code'] = df['stock_code'].apply(code_to_tscode)
    print(f"    加载完成: {len(df):,} 行, 覆盖 {df['stock_code'].nunique()} 只股票, "
          f"日期 {df['date'].min().date()} ~ {df['date'].max().date()}, "
          f"耗时 {time.time()-t0:.1f}s")
    return df


def load_market_data(stock_engine, start_date='20250301'):
    """加载日线行情数据"""
    print(">>> 加载日线行情数据...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, open, high, low, close, pre_close,
               pct_chg, vol, amount
        FROM market_daily
        WHERE trade_date >= '{start_date}'
        ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, stock_engine)
    print(f"    加载完成: {len(df):,} 行, 耗时 {time.time()-t0:.1f}s")
    return df


def load_moneyflow_data(stock_engine, start_date='20250301'):
    """加载资金流向数据"""
    print(">>> 加载资金流向数据...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date,
               buy_sm_amount, sell_sm_amount,
               buy_md_amount, sell_md_amount,
               buy_lg_amount, sell_lg_amount,
               buy_elg_amount, sell_elg_amount,
               net_mf_vol, net_mf_amount
        FROM moneyflow
        WHERE trade_date >= '{start_date}'
        ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, stock_engine)
    # 计算各类资金净流入
    df['net_sm'] = df['buy_sm_amount'] - df['sell_sm_amount']    # 散户净流入
    df['net_md'] = df['buy_md_amount'] - df['sell_md_amount']    # 中单净流入
    df['net_lg'] = df['buy_lg_amount'] - df['sell_lg_amount']    # 大单净流入
    df['net_elg'] = df['buy_elg_amount'] - df['sell_elg_amount']  # 超大单净流入
    df['net_main'] = df['net_lg'] + df['net_elg']                # 主力净流入(大单+超大单)
    print(f"    加载完成: {len(df):,} 行, 耗时 {time.time()-t0:.1f}s")
    return df


def calc_heat_velocity(heat_df):
    """
    计算热度涨速指标

    涨速定义：排名变化速度（rank越小=越热门，所以rank下降=热度上升）
    - velocity_1d: 1日排名变化 (负值=热度上升)
    - velocity_3d: 3日排名变化
    - velocity_5d: 5日排名变化
    - velocity_pct_1d: 1日排名变化百分比
    - rank_ma5: 5日排名均值
    - rank_std5: 5日排名波动
    """
    print(">>> 计算热度涨速指标...")
    t0 = time.time()

    df = heat_df.sort_values(['stock_code', 'date']).copy()

    # 按股票分组计算
    grp = df.groupby('stock_code')

    # 排名变化（负值=排名上升=更热门）
    df['rank_lag1'] = grp['rank'].shift(1)
    df['rank_lag3'] = grp['rank'].shift(3)
    df['rank_lag5'] = grp['rank'].shift(5)

    df['velocity_1d'] = df['rank'] - df['rank_lag1']  # 负值=热度上升
    df['velocity_3d'] = df['rank'] - df['rank_lag3']
    df['velocity_5d'] = df['rank'] - df['rank_lag5']

    # 排名变化百分比（相对变化）
    df['velocity_pct_1d'] = df['velocity_1d'] / df['rank_lag1']
    df['velocity_pct_3d'] = df['velocity_3d'] / df['rank_lag3']

    # 排名移动平均和波动率
    df['rank_ma5'] = grp['rank'].transform(lambda x: x.rolling(5, min_periods=3).mean())
    df['rank_std5'] = grp['rank'].transform(lambda x: x.rolling(5, min_periods=3).std())

    # 热度加速度（涨速的变化）
    df['acceleration'] = df['velocity_1d'] - grp['velocity_1d'].shift(1)

    print(f"    计算完成, 耗时 {time.time()-t0:.1f}s")
    return df


def calc_future_returns(market_df):
    """计算未来N日收益"""
    print(">>> 计算未来收益...")
    t0 = time.time()

    df = market_df.sort_values(['ts_code', 'trade_date']).copy()
    grp = df.groupby('ts_code')

    # 未来N日收益率
    for n in [1, 2, 3, 5, 10, 20]:
        df[f'fwd_ret_{n}d'] = grp['close'].transform(
            lambda x: x.shift(-n) / x - 1
        ) * 100  # 百分比

    # 未来N日最大回撤
    for n in [5, 10]:
        df[f'fwd_maxdd_{n}d'] = grp['close'].transform(
            lambda x: pd.Series(
                [(x.iloc[i+1:i+n+1].min() / x.iloc[i] - 1) * 100
                 if i + n < len(x) else np.nan
                 for i in range(len(x))],
                index=x.index
            )
        )

    print(f"    计算完成, 耗时 {time.time()-t0:.1f}s")
    return df


def merge_all_data(heat_df, market_df, money_df):
    """合并所有数据"""
    print(">>> 合并数据...")
    t0 = time.time()

    # 以热度数据为基准，关联行情和资金流向
    merged = heat_df.merge(
        market_df, on=['ts_code', 'trade_date'], how='inner'
    )
    merged = merged.merge(
        money_df[['ts_code', 'trade_date', 'net_sm', 'net_md', 'net_lg',
                   'net_elg', 'net_main', 'net_mf_amount']],
        on=['ts_code', 'trade_date'], how='left'
    )

    print(f"    合并完成: {len(merged):,} 行, "
          f"有资金流向 {merged['net_mf_amount'].notna().sum():,} 行, "
          f"耗时 {time.time()-t0:.1f}s")
    return merged


# ===========================================================
# Part 1: 热度涨速 vs 后续收益
# ===========================================================
def analyze_velocity_vs_returns(df):
    """分析热度涨速对后续收益的预测力"""
    print("\n" + "="*70)
    print("Part 1: 热度涨速 vs 后续收益")
    print("="*70)

    results = {}

    # --- 1.1 按热度涨速分组，看后续收益 ---
    print("\n--- 1.1 热度涨速分组 vs 后续收益 ---")
    # velocity_1d 的分位数分组
    valid = df.dropna(subset=['velocity_1d', 'fwd_ret_1d'])
    valid['vel_group'] = pd.qcut(valid['velocity_1d'], q=5,
                                  labels=['急升(Q1)', '温和升(Q2)', '平稳(Q3)', '温和降(Q4)', '急降(Q5)'],
                                  duplicates='drop')

    for period in ['1d', '3d', '5d', '10d', '20d']:
        col = f'fwd_ret_{period}'
        if col not in valid.columns:
            continue
        stats = valid.groupby('vel_group', observed=True)[col].agg(['mean', 'median', 'std', 'count'])
        stats.columns = ['均值%', '中位数%', '标准差%', '样本量']
        print(f"\n  涨速分组 vs 未来{period}收益:")
        print(stats.to_string(float_format='%.3f'))
        results[f'vel_vs_ret_{period}'] = stats

    # --- 1.2 先验涨速（T-5 ~ T-1 的热度变化）vs 后续收益 ---
    print("\n--- 1.2 先验热度涨速(T-5到T-1) vs 后续收益 ---")
    valid2 = df.dropna(subset=['velocity_5d', 'fwd_ret_5d'])
    valid2['prior_vel_group'] = pd.qcut(valid2['velocity_5d'], q=5,
                                         labels=['大幅升热(Q1)', '温和升热(Q2)', '平稳(Q3)',
                                                 '温和降热(Q4)', '大幅降热(Q5)'],
                                         duplicates='drop')

    for period in ['1d', '3d', '5d', '10d', '20d']:
        col = f'fwd_ret_{period}'
        if col not in valid2.columns:
            continue
        stats = valid2.groupby('prior_vel_group', observed=True)[col].agg(['mean', 'median', 'std', 'count'])
        stats.columns = ['均值%', '中位数%', '标准差%', '样本量']
        print(f"\n  先验涨速分组 vs 未来{period}收益:")
        print(stats.to_string(float_format='%.3f'))
        results[f'prior_vel_vs_ret_{period}'] = stats

    # --- 1.3 热度加速度 vs 后续收益 ---
    print("\n--- 1.3 热度加速度（涨速变化速度）vs 后续收益 ---")
    valid3 = df.dropna(subset=['acceleration', 'fwd_ret_5d'])
    valid3['acc_group'] = pd.qcut(valid3['acceleration'], q=5,
                                   labels=['加速升热', '缓加速', '平稳', '缓减速', '加速降热'],
                                   duplicates='drop')

    for period in ['1d', '5d', '10d']:
        col = f'fwd_ret_{period}'
        if col not in valid3.columns:
            continue
        stats = valid3.groupby('acc_group', observed=True)[col].agg(['mean', 'median', 'count'])
        stats.columns = ['均值%', '中位数%', '样本量']
        print(f"\n  加速度分组 vs 未来{period}收益:")
        print(stats.to_string(float_format='%.3f'))
        results[f'acc_vs_ret_{period}'] = stats

    # --- 1.4 绝对排名层级 + 涨速交叉分析 ---
    print("\n--- 1.4 排名层级 × 涨速 交叉分析 (未来5日收益%) ---")
    valid4 = df.dropna(subset=['rank', 'velocity_1d', 'fwd_ret_5d'])
    valid4['rank_tier'] = pd.cut(valid4['rank'],
                                  bins=[0, 100, 500, 1000, 2000, float('inf')],
                                  labels=['Top100', 'Top500', 'Top1000', 'Top2000', '2000+'])
    valid4['vel_tier'] = pd.qcut(valid4['velocity_1d'], q=3,
                                  labels=['升热', '平稳', '降热'], duplicates='drop')

    cross = valid4.groupby(['rank_tier', 'vel_tier'], observed=True)['fwd_ret_5d'].agg(['mean', 'count'])
    cross.columns = ['均值%', '样本量']
    print(cross.to_string(float_format='%.3f'))
    results['rank_vel_cross'] = cross

    # --- 1.5 热度排名波动率 vs 后续收益 ---
    print("\n--- 1.5 热度排名波动率 vs 后续收益 ---")
    valid5 = df.dropna(subset=['rank_std5', 'fwd_ret_5d'])
    valid5['vol_group'] = pd.qcut(valid5['rank_std5'], q=4,
                                   labels=['低波动', '中低波动', '中高波动', '高波动'],
                                   duplicates='drop')

    for period in ['1d', '5d', '10d']:
        col = f'fwd_ret_{period}'
        if col not in valid5.columns:
            continue
        stats = valid5.groupby('vol_group', observed=True)[col].agg(['mean', 'median', 'count'])
        stats.columns = ['均值%', '中位数%', '样本量']
        print(f"\n  排名波动率分组 vs 未来{period}收益:")
        print(stats.to_string(float_format='%.3f'))
        results[f'vol_vs_ret_{period}'] = stats

    return results


# ===========================================================
# Part 2: 热度涨速 vs 资金流向
# ===========================================================
def analyze_velocity_vs_moneyflow(df):
    """分析热度涨速与资金流向的关系"""
    print("\n" + "="*70)
    print("Part 2: 热度涨速 vs 资金流向")
    print("="*70)

    results = {}
    valid = df.dropna(subset=['velocity_1d', 'net_main'])

    # --- 2.1 热度涨速分组的资金流向特征 ---
    print("\n--- 2.1 热度涨速分组的资金流向特征 ---")
    valid['vel_group'] = pd.qcut(valid['velocity_1d'], q=5,
                                  labels=['急升(Q1)', '温和升(Q2)', '平稳(Q3)', '温和降(Q4)', '急降(Q5)'],
                                  duplicates='drop')

    flow_stats = valid.groupby('vel_group', observed=True).agg({
        'net_main': ['mean', 'median'],       # 主力净流入
        'net_sm': ['mean', 'median'],          # 散户净流入
        'net_elg': ['mean', 'median'],         # 超大单净流入
        'net_mf_amount': ['mean', 'median'],   # 总净流入
    })
    flow_stats.columns = ['主力均值', '主力中位', '散户均值', '散户中位',
                           '超大单均值', '超大单中位', '总净流入均值', '总净流入中位']
    print(flow_stats.to_string(float_format='%.1f'))
    results['vel_vs_flow'] = flow_stats

    # --- 2.2 热度涨速 × 资金方向 交叉分析 ---
    print("\n--- 2.2 热度涨速 × 主力资金方向 → 未来5日收益 ---")
    valid2 = valid.dropna(subset=['fwd_ret_5d']).copy()
    valid2['flow_dir'] = np.where(valid2['net_main'] > 0, '主力净流入', '主力净流出')
    valid2['vel_tier'] = pd.qcut(valid2['velocity_1d'], q=3,
                                  labels=['升热', '平稳', '降热'], duplicates='drop')

    cross = valid2.groupby(['vel_tier', 'flow_dir'], observed=True)['fwd_ret_5d'].agg(['mean', 'median', 'count'])
    cross.columns = ['均值%', '中位数%', '样本量']
    print(cross.to_string(float_format='%.3f'))
    results['vel_flow_cross_5d'] = cross

    print("\n--- 热度涨速 × 主力资金方向 → 未来10日收益 ---")
    valid3 = valid.dropna(subset=['fwd_ret_10d']).copy()
    valid3['flow_dir'] = np.where(valid3['net_main'] > 0, '主力净流入', '主力净流出')
    valid3['vel_tier'] = pd.qcut(valid3['velocity_1d'], q=3,
                                  labels=['升热', '平稳', '降热'], duplicates='drop')

    cross10 = valid3.groupby(['vel_tier', 'flow_dir'], observed=True)['fwd_ret_10d'].agg(['mean', 'median', 'count'])
    cross10.columns = ['均值%', '中位数%', '样本量']
    print(cross10.to_string(float_format='%.3f'))
    results['vel_flow_cross_10d'] = cross10

    # --- 2.3 资金流向领先/滞后热度变化 ---
    print("\n--- 2.3 资金流向是否领先热度变化？ ---")
    # 计算前N日资金流向 vs 当日热度变化
    valid_lag = df.sort_values(['ts_code', 'trade_date']).copy()
    grp = valid_lag.groupby('ts_code')
    valid_lag['net_main_lag1'] = grp['net_main'].shift(1)  # 前1日主力
    valid_lag['net_main_lag3'] = grp['net_main'].shift(1).rolling(3, min_periods=1).mean()  # 前3日主力均值

    # 前日主力流入/流出 → 当日热度变化相关性
    lag_valid = valid_lag.dropna(subset=['net_main_lag1', 'velocity_1d'])

    from scipy import stats as scipy_stats
    corr_1d, pval_1d = scipy_stats.pearsonr(lag_valid['net_main_lag1'], lag_valid['velocity_1d'])
    print(f"  前1日主力净流入 vs 当日热度涨速: r={corr_1d:.4f}, p={pval_1d:.2e}")

    lag_valid3 = valid_lag.dropna(subset=['net_main_lag3', 'velocity_1d'])
    corr_3d, pval_3d = scipy_stats.pearsonr(lag_valid3['net_main_lag3'], lag_valid3['velocity_1d'])
    print(f"  前3日主力均值 vs 当日热度涨速: r={corr_3d:.4f}, p={pval_3d:.2e}")

    # 反向：当日热度变化 → 后N日资金流向
    valid_lag['net_main_fwd1'] = grp['net_main'].shift(-1)
    fwd_valid = valid_lag.dropna(subset=['velocity_1d', 'net_main_fwd1'])
    corr_fwd, pval_fwd = scipy_stats.pearsonr(fwd_valid['velocity_1d'], fwd_valid['net_main_fwd1'])
    print(f"  当日热度涨速 vs 次日主力净流入: r={corr_fwd:.4f}, p={pval_fwd:.2e}")

    results['lead_lag'] = {
        'main_lag1_vs_vel': (corr_1d, pval_1d),
        'main_lag3_vs_vel': (corr_3d, pval_3d),
        'vel_vs_main_fwd1': (corr_fwd, pval_fwd),
    }

    # --- 2.4 散户 vs 主力资金与热度的差异化关系 ---
    print("\n--- 2.4 散户 vs 主力：谁更跟随热度？ ---")
    valid4 = valid.dropna(subset=['velocity_1d'])
    # 按热度涨速极端组看资金构成
    q1 = valid4['velocity_1d'].quantile(0.1)  # 热度急升
    q9 = valid4['velocity_1d'].quantile(0.9)  # 热度急降

    hot_surge = valid4[valid4['velocity_1d'] <= q1]
    hot_drop = valid4[valid4['velocity_1d'] >= q9]
    mid = valid4[(valid4['velocity_1d'] > q1) & (valid4['velocity_1d'] < q9)]

    print(f"\n  热度急升 (排名降最多10%, vel<={q1:.0f}):")
    print(f"    散户净流入均值: {hot_surge['net_sm'].mean():.1f} 万")
    print(f"    主力净流入均值: {hot_surge['net_main'].mean():.1f} 万")
    print(f"    超大单净流入均值: {hot_surge['net_elg'].mean():.1f} 万")
    print(f"    样本量: {len(hot_surge):,}")

    print(f"\n  热度急降 (排名升最多10%, vel>={q9:.0f}):")
    print(f"    散户净流入均值: {hot_drop['net_sm'].mean():.1f} 万")
    print(f"    主力净流入均值: {hot_drop['net_main'].mean():.1f} 万")
    print(f"    超大单净流入均值: {hot_drop['net_elg'].mean():.1f} 万")
    print(f"    样本量: {len(hot_drop):,}")

    print(f"\n  中间组:")
    print(f"    散户净流入均值: {mid['net_sm'].mean():.1f} 万")
    print(f"    主力净流入均值: {mid['net_main'].mean():.1f} 万")
    print(f"    超大单净流入均值: {mid['net_elg'].mean():.1f} 万")
    print(f"    样本量: {len(mid):,}")

    return results


# ===========================================================
# Part 3: 综合多因子分析
# ===========================================================
def analyze_multifactor(df):
    """综合多因子分析"""
    print("\n" + "="*70)
    print("Part 3: 综合多因子分析")
    print("="*70)

    results = {}

    # --- 3.1 各因子与未来收益的相关性排名 ---
    print("\n--- 3.1 单因子预测力排名 (与未来5日收益的相关系数) ---")
    from scipy import stats as scipy_stats

    factors = ['rank', 'velocity_1d', 'velocity_3d', 'velocity_5d',
               'acceleration', 'rank_std5', 'rank_ma5',
               'change_rate', 'volume_ratio', 'turnover_rate',
               'net_main', 'net_sm', 'net_elg', 'net_mf_amount']

    targets = ['fwd_ret_1d', 'fwd_ret_3d', 'fwd_ret_5d', 'fwd_ret_10d', 'fwd_ret_20d']

    corr_matrix = []
    for factor in factors:
        row = {'因子': factor}
        for target in targets:
            valid = df.dropna(subset=[factor, target])
            if len(valid) > 100:
                r, p = scipy_stats.pearsonr(valid[factor], valid[target])
                row[target] = f"{r:.4f}" if p < 0.05 else f"{r:.4f}(ns)"
            else:
                row[target] = 'N/A'
        corr_matrix.append(row)

    corr_df = pd.DataFrame(corr_matrix).set_index('因子')
    print(corr_df.to_string())
    results['factor_corr'] = corr_df

    # --- 3.2 热度涨速的IC值分析 ---
    print("\n--- 3.2 热度涨速IC值时序分析 (每日截面相关性) ---")
    ic_list = []
    for date_val in df['trade_date'].unique():
        daily = df[df['trade_date'] == date_val].dropna(subset=['velocity_1d', 'fwd_ret_5d'])
        if len(daily) >= 50:
            r, _ = scipy_stats.spearmanr(daily['velocity_1d'], daily['fwd_ret_5d'])
            ic_list.append({'date': date_val, 'IC': r})

    if ic_list:
        ic_df = pd.DataFrame(ic_list)
        print(f"  IC均值: {ic_df['IC'].mean():.4f}")
        print(f"  IC标准差: {ic_df['IC'].std():.4f}")
        print(f"  ICIR (IC均值/标准差): {ic_df['IC'].mean() / ic_df['IC'].std():.4f}")
        print(f"  IC>0 占比: {(ic_df['IC'] > 0).mean():.1%}")
        print(f"  IC<0 占比: {(ic_df['IC'] < 0).mean():.1%}")
        print(f"  有效天数: {len(ic_df)}")
        results['ic_stats'] = ic_df

        # 保存IC序列
        ic_df.to_csv(OUTPUT_DIR / 'heat_velocity_ic.csv', index=False)
        print(f"  IC序列已保存到 {OUTPUT_DIR / 'heat_velocity_ic.csv'}")

    # --- 3.3 热度涨速 + 资金方向 双因子组合 ---
    print("\n--- 3.3 双因子信号组合效果 ---")
    valid3 = df.dropna(subset=['velocity_5d', 'net_main', 'fwd_ret_5d', 'fwd_ret_10d']).copy()

    # 信号定义：前5日热度急升 + 主力净流入
    q20 = valid3['velocity_5d'].quantile(0.2)
    q80 = valid3['velocity_5d'].quantile(0.8)

    signals = {
        '升热+主力流入': (valid3['velocity_5d'] <= q20) & (valid3['net_main'] > 0),
        '升热+主力流出': (valid3['velocity_5d'] <= q20) & (valid3['net_main'] <= 0),
        '降热+主力流入': (valid3['velocity_5d'] >= q80) & (valid3['net_main'] > 0),
        '降热+主力流出': (valid3['velocity_5d'] >= q80) & (valid3['net_main'] <= 0),
        '平稳组(基准)': (valid3['velocity_5d'] > q20) & (valid3['velocity_5d'] < q80),
    }

    signal_results = []
    for name, mask in signals.items():
        subset = valid3[mask]
        if len(subset) > 0:
            signal_results.append({
                '信号': name,
                '样本量': len(subset),
                '未来5日均值%': subset['fwd_ret_5d'].mean(),
                '未来5日中位%': subset['fwd_ret_5d'].median(),
                '未来10日均值%': subset['fwd_ret_10d'].mean(),
                '未来10日中位%': subset['fwd_ret_10d'].median(),
                '未来5日>0占比': (subset['fwd_ret_5d'] > 0).mean(),
            })

    sig_df = pd.DataFrame(signal_results).set_index('信号')
    print(sig_df.to_string(float_format='%.3f'))
    results['dual_factor'] = sig_df

    # --- 3.4 分市值层级分析 ---
    print("\n--- 3.4 不同热度层级的涨速预测力差异 ---")
    valid4 = df.dropna(subset=['rank', 'velocity_1d', 'fwd_ret_5d']).copy()
    valid4['rank_tier'] = pd.cut(valid4['rank'],
                                  bins=[0, 200, 500, 1000, 3000, float('inf')],
                                  labels=['Top200', '200-500', '500-1000', '1000-3000', '3000+'])

    tier_ic = []
    for tier in valid4['rank_tier'].cat.categories:
        tier_data = valid4[valid4['rank_tier'] == tier]
        if len(tier_data) > 100:
            r, p = scipy_stats.spearmanr(tier_data['velocity_1d'], tier_data['fwd_ret_5d'])
            tier_ic.append({
                '排名层级': tier,
                'IC': r,
                'p值': p,
                '样本量': len(tier_data)
            })

    if tier_ic:
        tier_df = pd.DataFrame(tier_ic).set_index('排名层级')
        print(tier_df.to_string(float_format='%.4f'))
        results['tier_ic'] = tier_df

    return results


# ===========================================================
# 总结统计
# ===========================================================
def print_summary(all_results):
    """输出总结"""
    print("\n" + "="*70)
    print("分析总结")
    print("="*70)

    print("""
关键发现请查看上述详细输出。

数据已保存到 hot/data/ 目录。
报告请基于上述数据手动撰写。
""")


def main():
    parser = argparse.ArgumentParser(description='热度涨速与股价关系分析')
    parser.add_argument('--part', type=str, default='all',
                        choices=['1', '2', '3', 'all'],
                        help='分析模块: 1=涨速vs收益, 2=涨速vs资金, 3=多因子, all=全部')
    args = parser.parse_args()

    print("="*70)
    print("热度涨速与股价关系分析")
    print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)

    t_start = time.time()

    # 加载数据
    trend_engine, stock_engine = get_engines()
    heat_df = load_heat_data(trend_engine)
    market_df = load_market_data(stock_engine)
    money_df = load_moneyflow_data(stock_engine)

    # 计算衍生指标
    heat_df = calc_heat_velocity(heat_df)
    market_df = calc_future_returns(market_df)

    # 合并
    merged = merge_all_data(heat_df, market_df, money_df)

    # 保存合并数据摘要
    merged.to_csv(OUTPUT_DIR / 'heat_velocity_merged_sample.csv', index=False,
                  encoding='utf-8-sig')
    print(f"\n合并数据已保存: {OUTPUT_DIR / 'heat_velocity_merged_sample.csv'}")

    # 执行分析
    all_results = {}

    if args.part in ('1', 'all'):
        all_results['part1'] = analyze_velocity_vs_returns(merged)

    if args.part in ('2', 'all'):
        all_results['part2'] = analyze_velocity_vs_moneyflow(merged)

    if args.part in ('3', 'all'):
        all_results['part3'] = analyze_multifactor(merged)

    print_summary(all_results)

    print(f"\n总耗时: {time.time() - t_start:.1f}s")


if __name__ == '__main__':
    main()
