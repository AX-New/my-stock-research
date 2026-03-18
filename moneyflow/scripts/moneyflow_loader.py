"""
资金流向研究 - 数据加载模块

加载资金流、行情、基础信息数据，供分析脚本使用。
所有数据从 my_stock 生产库（read_engine）读取。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import pandas as pd
import numpy as np
from sqlalchemy import text
from database import read_engine
import time


def load_stock_basic() -> pd.DataFrame:
    """
    加载股票基础信息（上市状态、行业、名称）

    Returns:
        DataFrame[ts_code, name, industry, list_status]
    """
    sql = "SELECT ts_code, name, industry, list_status FROM stock_basic"
    with read_engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    print(f"[moneyflow_loader] stock_basic 加载完成 | {len(df)} 只股票")
    return df


def load_moneyflow(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    加载个股资金流数据（moneyflow 表）

    只加载分析所需的列，减少内存占用。
    金额单位：万元。

    Args:
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（可选）

    Returns:
        DataFrame[ts_code, trade_date, buy_sm_amount, sell_sm_amount,
                  buy_md_amount, sell_md_amount, buy_lg_amount, sell_lg_amount,
                  buy_elg_amount, sell_elg_amount, net_mf_amount]
    """
    cols = ("ts_code, trade_date, "
            "buy_sm_amount, sell_sm_amount, "
            "buy_md_amount, sell_md_amount, "
            "buy_lg_amount, sell_lg_amount, "
            "buy_elg_amount, sell_elg_amount, "
            "net_mf_amount")
    where_clauses = []
    if start_date:
        where_clauses.append(f"trade_date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"trade_date <= '{end_date}'")
    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    sql = f"SELECT {cols} FROM moneyflow {where} ORDER BY trade_date, ts_code"

    t0 = time.time()
    with read_engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    elapsed = time.time() - t0
    print(f"[moneyflow_loader] moneyflow 加载完成 | {len(df)} 条 | {elapsed:.1f}s")
    return df


def load_market_daily_for_returns(start_date: str = None, end_date: str = None,
                                   extend_days: int = 40) -> pd.DataFrame:
    """
    加载行情数据，用于计算未来收益率和 MOD 修正。

    从 market_daily + adj_factor 计算前复权收盘价。
    同时返回 pct_chg（涨跌幅）和 vol（成交量，用于过滤停牌）。

    注意：end_date 会自动延伸 extend_days 个自然日，确保最后 20 个交易日
    也能计算未来收益率。

    Args:
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（可选）
        extend_days: end_date 向后延伸的自然日数（默认 40，覆盖 20 个交易日）

    Returns:
        DataFrame[ts_code, trade_date, close_qfq, pct_chg, vol]
    """
    where_clauses = []
    if start_date:
        where_clauses.append(f"m.trade_date >= '{start_date}'")
    if end_date:
        # 延伸 end_date 以覆盖未来收益计算所需数据
        from datetime import datetime, timedelta
        ext_date = (datetime.strptime(end_date, '%Y%m%d') + timedelta(days=extend_days)).strftime('%Y%m%d')
        where_clauses.append(f"m.trade_date <= '{ext_date}'")
    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # 前复权 = close * adj_factor / 最新adj_factor
    # 先获取原始数据，后续在 pandas 中计算 qfq
    sql = f"""
        SELECT m.ts_code, m.trade_date, m.close, m.pct_chg, m.vol,
               COALESCE(a.adj_factor, 1.0) as adj_factor
        FROM market_daily m
        LEFT JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        {where}
        ORDER BY m.ts_code, m.trade_date
    """

    t0 = time.time()
    with read_engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    elapsed = time.time() - t0
    print(f"[moneyflow_loader] market_daily 加载完成 | {len(df)} 条 | {elapsed:.1f}s")

    # 计算前复权价: close_qfq = close * adj_factor / 该股票最新的 adj_factor
    if len(df) > 0:
        latest_factor = df.groupby('ts_code')['adj_factor'].transform('last')
        df['close_qfq'] = (df['close'] * df['adj_factor'] / latest_factor).round(2)
    else:
        df['close_qfq'] = pd.Series(dtype=float)

    df = df[['ts_code', 'trade_date', 'close_qfq', 'pct_chg', 'vol']]
    return df


def load_industry_moneyflow(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    加载行业资金流数据（moneyflow_ind_dc 表）

    仅加载 content_type='行业' 的记录。
    同名行业多个DC代码会按名称聚合。
    金额单位：元。

    Args:
        start_date: 开始日期 YYYYMMDD（可选）
        end_date: 结束日期 YYYYMMDD（可选）

    Returns:
        DataFrame[name, trade_date, net_amount, net_amount_rate, pct_change]
        已按 (name, trade_date) 聚合去重。
    """
    where_clauses = ["content_type = '行业'"]
    if start_date:
        where_clauses.append(f"trade_date >= '{start_date}'")
    if end_date:
        where_clauses.append(f"trade_date <= '{end_date}'")
    where = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT name, trade_date, net_amount, net_amount_rate, pct_change
        FROM moneyflow_ind_dc
        {where}
        ORDER BY trade_date, name
    """

    t0 = time.time()
    with read_engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    elapsed = time.time() - t0
    print(f"[moneyflow_loader] moneyflow_ind_dc 加载完成 | {len(df)} 条 | {elapsed:.1f}s")

    # 同名行业聚合（如"银行"有两个DC代码）
    before = len(df)
    df = df.groupby(['name', 'trade_date'], as_index=False).agg({
        'net_amount': 'sum',
        'net_amount_rate': 'mean',  # 占比取均值
        'pct_change': 'mean',       # 涨跌幅取均值（同名行业涨幅应一致或接近）
    })
    after = len(df)
    if before != after:
        print(f"[moneyflow_loader] 行业名称聚合: {before} → {after} 条（合并重名行业）")

    return df


def load_index_daily(ts_code: str = "000001.SH") -> pd.DataFrame:
    """
    加载指数日线（用于市场环境判断 MA60）

    Args:
        ts_code: 指数代码，默认上证指数

    Returns:
        DataFrame[trade_date, close]
    """
    sql = f"SELECT trade_date, close FROM index_daily WHERE ts_code = '{ts_code}' ORDER BY trade_date"
    with read_engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    print(f"[moneyflow_loader] index_daily({ts_code}) 加载完成 | {len(df)} 条")
    return df


def compute_future_returns(price_df: pd.DataFrame, horizons: list = [1, 3, 5, 10, 20]) -> pd.DataFrame:
    """
    计算每只股票每日的未来 N 日收益率。

    Args:
        price_df: DataFrame[ts_code, trade_date, close_qfq]，需按 ts_code+trade_date 排序
        horizons: 观察周期列表

    Returns:
        原 DataFrame 新增 ret_1d, ret_3d, ... 列（百分比）
    """
    # 确保按 ts_code + trade_date 排序
    result = price_df.sort_values(['ts_code', 'trade_date']).copy()
    grouped = result.groupby('ts_code')['close_qfq']
    for h in horizons:
        # 用 shift(-h) 取未来第 h 天的价格，避免 lambda transform 的内存开销
        future_price = grouped.shift(-h)
        result[f'ret_{h}d'] = (future_price / result['close_qfq'] - 1) * 100
    return result


def get_market_regime(index_df: pd.DataFrame, ma_period: int = 60) -> pd.DataFrame:
    """
    判断市场环境（牛/熊）。

    用指数收盘价 vs MA60 判断：
    - close > MA60 → 'bull'
    - close <= MA60 → 'bear'

    Args:
        index_df: DataFrame[trade_date, close]
        ma_period: 均线周期，默认 60

    Returns:
        DataFrame[trade_date, regime]  regime: 'bull' | 'bear'
    """
    df = index_df.copy()
    df['ma'] = df['close'].rolling(ma_period, min_periods=ma_period).mean()
    df['regime'] = np.where(df['close'] > df['ma'], 'bull', 'bear')
    return df[['trade_date', 'regime']].dropna()
