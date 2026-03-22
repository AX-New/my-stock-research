"""
热度 ML 特征工程模块

从热度排名数据中提取丰富的特征用于机器学习：
  1. 热度排名特征：排名位置、变化、动量、波动率
  2. 行情特征：收益率、振幅、成交量变化
  3. 市场环境特征：大盘涨跌幅、市场热度分布
  4. 交叉特征：热度 × 量价交互

所有特征在每个交易日、每只股票粒度计算。
"""
import logging
import pandas as pd
import numpy as np
from datetime import timedelta

logger = logging.getLogger(__name__)


def build_features(data_bundle: dict, forward_days: list = None) -> pd.DataFrame:
    """
    从数据包构建完整的 ML 特征矩阵

    Args:
        data_bundle: load_ml_dataset() 返回的数据包
        forward_days: 预测目标的天数列表，如 [3, 5, 10]

    Returns:
        DataFrame: 每行 = (stock_code, date)，包含所有特征和目标变量
    """
    if forward_days is None:
        forward_days = [3, 5, 10]

    heat_df = data_bundle['heat_df'].copy()
    price_df = data_bundle['price_df'].copy()
    index_df = data_bundle['index_df'].copy()
    trading_days = data_bundle['trading_days']

    logger.info("=" * 60)
    logger.info("开始特征工程...")
    logger.info("=" * 60)

    # ──────────────────── 1. 热度排名特征 ────────────────────
    logger.info("1. 计算热度排名特征...")
    heat_features = _compute_heat_features(heat_df)

    # ──────────────────── 2. 行情特征 ────────────────────
    logger.info("2. 计算行情特征...")
    market_features = _compute_market_features(price_df)

    # ──────────────────── 3. 大盘环境特征 ────────────────────
    logger.info("3. 计算大盘环境特征...")
    env_features = _compute_env_features(index_df, heat_df)

    # ──────────────────── 4. 合并所有特征 ────────────────────
    logger.info("4. 合并特征矩阵...")
    df = heat_features.merge(market_features, on=['stock_code', 'date'], how='inner')
    df = df.merge(env_features, on='date', how='left')

    # ──────────────────── 5. 计算预测目标 ────────────────────
    logger.info("5. 计算预测目标（前瞻 N 日收益率）...")
    df = _compute_targets(df, price_df, trading_days, forward_days)

    # ──────────────────── 6. 交叉特征 ────────────────────
    logger.info("6. 计算交叉特征...")
    df = _compute_cross_features(df)

    # 清理：去掉全 NaN 的行
    feature_cols = [c for c in df.columns if c not in ['stock_code', 'date', 'stock_name']
                    and not c.startswith('target_')]
    df = df.dropna(subset=feature_cols, how='all')

    logger.info(f"特征矩阵完成: {len(df):,} 行, {len(feature_cols)} 个特征, "
                f"{df['stock_code'].nunique()} 只股票")
    logger.info(f"特征列: {feature_cols}")
    return df


def get_feature_columns(df: pd.DataFrame) -> list:
    """获取特征列名（排除 stock_code, date, target_* 等非特征列）"""
    exclude = {'stock_code', 'date', 'stock_name'}
    return [c for c in df.columns if c not in exclude and not c.startswith('target_')]


# ════════════════════════════════════════════════════════════════
# 内部函数
# ════════════════════════════════════════════════════════════════


def _compute_heat_features(heat_df: pd.DataFrame) -> pd.DataFrame:
    """
    热度排名特征（核心特征组）

    特征列表：
      - rank: 原始排名
      - rank_pct: 排名百分位（0=最热, 1=最冷）
      - rank_chg_1d/3d/5d/10d: 排名变化（正=变冷/排名上升）
      - rank_ma5/10/20: 排名移动平均
      - rank_std5/10: 排名波动率（5/10日标准差）
      - heat_position_10/20/30: 热度位置（0~1，1=冷到极点）
      - rank_surge_10/20: 排名弹性（当前/均值，>1=比平均冷）
      - days_in_top100/500: 近20天进入 top100/500 的天数
      - rank_min_20d/rank_max_20d: 近20天最高/最低排名
    """
    df = heat_df[['stock_code', 'stock_name', 'date', 'rank',
                   'change_rate', 'volume_ratio', 'turnover_rate', 'deal_amount']].copy()
    df = df.sort_values(['stock_code', 'date'])

    # 每日全市场股票总数（用于归一化）
    daily_count = heat_df.groupby('date')['stock_code'].nunique().reset_index()
    daily_count.columns = ['date', 'total_stocks']
    df = df.merge(daily_count, on='date', how='left')

    # 排名百分位
    df['rank_pct'] = df['rank'] / df['total_stocks']

    grouped = df.groupby('stock_code')

    # 排名变化
    for d in [1, 3, 5, 10]:
        df[f'rank_chg_{d}d'] = grouped['rank'].diff(d)

    # 排名移动平均
    for w in [5, 10, 20]:
        df[f'rank_ma{w}'] = grouped['rank'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).mean()
        )

    # 排名波动率
    for w in [5, 10]:
        df[f'rank_std{w}'] = grouped['rank'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).std()
        )

    # 热度位置（heat_position）：近 N 天排名范围内的归一化位置
    # 1 = 冷到极点（排名接近近期最高/最冷），0 = 热到极点
    for lookback in [10, 20, 30]:
        rank_min = grouped['rank'].transform(
            lambda x: x.rolling(lookback, min_periods=max(1, lookback // 2)).min()
        )
        rank_max = grouped['rank'].transform(
            lambda x: x.rolling(lookback, min_periods=max(1, lookback // 2)).max()
        )
        df[f'heat_position_{lookback}'] = (df['rank'] - rank_min) / (rank_max - rank_min + 1e-8)

    # 排名弹性（rank_surge）：当前排名 / N日均值，>1表示比平均冷
    for lookback in [10, 20]:
        df[f'rank_surge_{lookback}'] = df['rank'] / df[f'rank_ma{lookback}'].clip(lower=1)

    # 近20天进入 top N 的天数
    for top_n in [100, 500]:
        df[f'days_in_top{top_n}'] = grouped['rank'].transform(
            lambda x: (x <= top_n).rolling(20, min_periods=1).sum()
        )

    # 近20天最高/最低排名
    df['rank_min_20d'] = grouped['rank'].transform(
        lambda x: x.rolling(20, min_periods=1).min()
    )
    df['rank_max_20d'] = grouped['rank'].transform(
        lambda x: x.rolling(20, min_periods=1).max()
    )

    # 换手率和量比的变化
    df['turnover_chg_1d'] = grouped['turnover_rate'].diff(1)
    df['volume_ratio_chg_1d'] = grouped['volume_ratio'].diff(1)

    df = df.drop(columns=['total_stocks'])
    logger.info(f"  热度特征: {len(df):,} 行")
    return df


def _compute_market_features(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    个股行情特征

    特征列表：
      - return_1d/3d/5d/10d: N 日收益率
      - amplitude: 振幅 (high-low)/pre_close
      - ma_bias_5/10/20: 均线偏离度
      - vol_ratio_5/10: 成交量相对 N日均量
      - price_pos_20/60: 价格位置（近N日范围内归一化）
      - volatility_10/20: N日收益率标准差
    """
    df = price_df[['stock_code', 'date', 'qfq_close', 'qfq_open', 'qfq_high',
                    'qfq_low', 'vol', 'pct_chg', 'pre_close', 'high', 'low']].copy()
    df = df.sort_values(['stock_code', 'date'])
    grouped = df.groupby('stock_code')

    # N 日收益率（基于前复权价格）
    for d in [1, 3, 5, 10]:
        df[f'return_{d}d'] = grouped['qfq_close'].pct_change(d)

    # 振幅
    df['amplitude'] = (df['high'] - df['low']) / df['pre_close'].clip(lower=0.01)

    # 均线偏离度
    for w in [5, 10, 20]:
        ma = grouped['qfq_close'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).mean()
        )
        df[f'ma_bias_{w}'] = (df['qfq_close'] - ma) / ma.clip(lower=0.01)

    # 成交量相对均量
    for w in [5, 10]:
        vol_ma = grouped['vol'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).mean()
        )
        df[f'vol_ratio_{w}'] = df['vol'] / vol_ma.clip(lower=1)

    # 价格位置（近N日范围归一化，0=最低, 1=最高）
    for w in [20, 60]:
        p_min = grouped['qfq_close'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).min()
        )
        p_max = grouped['qfq_close'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).max()
        )
        df[f'price_pos_{w}'] = (df['qfq_close'] - p_min) / (p_max - p_min + 1e-8)

    # 波动率
    for w in [10, 20]:
        df[f'volatility_{w}'] = grouped['return_1d'].transform(
            lambda x: x.rolling(w, min_periods=max(1, w // 2)).std()
        )

    # 只保留需要的列
    keep_cols = ['stock_code', 'date']
    keep_cols += [c for c in df.columns if c.startswith(('return_', 'amplitude', 'ma_bias_',
                                                          'vol_ratio_', 'price_pos_', 'volatility_'))]
    df = df[keep_cols]
    logger.info(f"  行情特征: {len(df):,} 行")
    return df


def _compute_env_features(index_df: pd.DataFrame, heat_df: pd.DataFrame) -> pd.DataFrame:
    """
    大盘环境特征（每日一条，所有股票共享）

    特征列表：
      - index_return_1d/5d/10d: 沪深300 N日收益率
      - index_volatility_10: 大盘10日波动率
      - index_ma_bias_20: 大盘20日均线偏离度
      - market_heat_mean/std/skew: 全市场热度排名分布统计
      - top100_avg_chg: 热度Top100股票平均涨跌幅
    """
    # 大盘指数特征
    idx = index_df[['date', 'index_close', 'index_pct_chg']].copy()
    idx = idx.sort_values('date')
    idx['index_return_1d'] = idx['index_close'].pct_change(1)
    idx['index_return_5d'] = idx['index_close'].pct_change(5)
    idx['index_return_10d'] = idx['index_close'].pct_change(10)
    idx['index_volatility_10'] = idx['index_return_1d'].rolling(10, min_periods=5).std()
    ma20 = idx['index_close'].rolling(20, min_periods=10).mean()
    idx['index_ma_bias_20'] = (idx['index_close'] - ma20) / ma20.clip(lower=0.01)

    # 全市场热度分布特征
    heat_stats = heat_df.groupby('date').agg(
        market_heat_mean=('rank', 'mean'),
        market_heat_std=('rank', 'std'),
        market_heat_skew=('rank', lambda x: x.skew()),
    ).reset_index()

    # Top100 平均涨跌幅（热门股情绪指标）
    top100 = heat_df[heat_df['rank'] <= 100].groupby('date')['change_rate'].mean().reset_index()
    top100.columns = ['date', 'top100_avg_chg']

    env = idx.merge(heat_stats, on='date', how='left')
    env = env.merge(top100, on='date', how='left')

    keep_cols = ['date', 'index_return_1d', 'index_return_5d', 'index_return_10d',
                 'index_volatility_10', 'index_ma_bias_20',
                 'market_heat_mean', 'market_heat_std', 'market_heat_skew',
                 'top100_avg_chg']
    env = env[keep_cols]
    logger.info(f"  大盘环境特征: {len(env)} 天")
    return env


def _compute_targets(df: pd.DataFrame, price_df: pd.DataFrame,
                     trading_days: list, forward_days: list) -> pd.DataFrame:
    """
    计算预测目标：未来 N 天收益率（向量化实现，避免 iterrows）

    使用交易日历确保跳过非交易日。
    target_Nd_return: 未来 N 个交易日的收益率
    target_Nd_dir: 未来 N 天涨跌方向（1=涨, 0=跌）
    """
    # 构建交易日索引映射
    td_idx = {d: i for i, d in enumerate(trading_days)}

    # 构建 (stock_code, date) → qfq_close 查询表（用 numpy 加速）
    arr = price_df[['stock_code', 'date', 'qfq_close']].to_numpy()
    price_lookup = {(str(arr[i, 0]), arr[i, 1]): float(arr[i, 2]) for i in range(len(arr))}

    # 构建 date → future_date 映射表（对每个 N，预计算偏移）
    for n in forward_days:
        col_ret = f'target_{n}d_return'
        col_dir = f'target_{n}d_dir'

        # 预建 date → future_date 映射
        future_date_map = {}
        for d, idx in td_idx.items():
            future_idx = idx + n
            if future_idx < len(trading_days):
                future_date_map[d] = trading_days[future_idx]

        # 向量化查询
        codes = df['stock_code'].values
        dates = df['date'].values
        returns = np.full(len(df), np.nan)
        for i in range(len(df)):
            cur_date = dates[i]
            code = codes[i]
            future_date = future_date_map.get(cur_date)
            if future_date is None:
                continue
            cur_price = price_lookup.get((code, cur_date))
            future_price = price_lookup.get((code, future_date))
            if cur_price and future_price and cur_price > 0:
                returns[i] = (future_price - cur_price) / cur_price

        df[col_ret] = returns
        df[col_dir] = np.where(np.isnan(returns), np.nan, (returns > 0).astype(float))

    valid_count = df[f'target_{forward_days[0]}d_return'].notna().sum()
    logger.info(f"  目标变量: {len(forward_days)} 个前瞻周期, 有效样本 {valid_count:,} 条")
    return df


def _compute_cross_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    交叉特征：热度 × 行情的交互

    特征列表：
      - heat_momentum_cross: 热度排名变化 × 价格收益率（同向=确认，反向=背离）
      - cold_with_volume: 排名变冷 × 放量（冷门放量信号）
      - heat_vol_interaction: 热度位置 × 波动率（冷门+高波动=潜在反转）
    """
    # 热度动量 × 价格动量（背离/确认信号）
    if 'rank_chg_5d' in df.columns and 'return_5d' in df.columns:
        df['heat_momentum_cross'] = df['rank_chg_5d'] * df['return_5d']

    # 冷门 + 放量
    if 'heat_position_20' in df.columns and 'vol_ratio_5' in df.columns:
        df['cold_with_volume'] = df['heat_position_20'] * df['vol_ratio_5']

    # 热度位置 × 波动率
    if 'heat_position_20' in df.columns and 'volatility_10' in df.columns:
        df['heat_vol_interaction'] = df['heat_position_20'] * df['volatility_10']

    logger.info(f"  交叉特征计算完成")
    return df
