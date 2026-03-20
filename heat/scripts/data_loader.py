"""
热度策略数据加载模块

提供加载热度排名、行情价格、交易日历、指数等所需数据的函数。
所有数据均为只读，热度数据来自 my_trend（port 3310），行情数据来自 my_stock（port 3307）。
"""
import logging
import pandas as pd
import numpy as np
from datetime import date

from heat.scripts.database import get_stock_engine, get_trend_engine

logger = logging.getLogger(__name__)


def load_heat_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    从 my_trend.popularity_rank 加载热度排名数据

    字段：stock_code, date, rank, deal_amount
    date 已转为 Python date 对象，方便与 trading_days 对齐。

    Args:
        start_date: 起始日期，格式 'YYYY-MM-DD'
        end_date:   结束日期，格式 'YYYY-MM-DD'

    Returns:
        DataFrame，columns: [stock_code, date, rank, deal_amount]
    """
    logger.info(f"加载热度排名数据 [{start_date} ~ {end_date}]...")
    engine = get_trend_engine()
    sql = f"""
        SELECT stock_code, date, `rank`, deal_amount
        FROM popularity_rank
        WHERE date >= '{start_date}' AND date <= '{end_date}'
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['date']).dt.date
    logger.info(f"  热度数据: {len(df):,} 条, {df['stock_code'].nunique()} 只股票")
    return df


def load_trading_days(start_date: str, end_date: str) -> list:
    """
    从 my_stock.trade_cal 加载 A股交易日历

    Args:
        start_date: 起始日期，格式 'YYYY-MM-DD'
        end_date:   结束日期，格式 'YYYY-MM-DD'

    Returns:
        list of datetime.date，已排序
    """
    logger.info(f"加载交易日历 [{start_date} ~ {end_date}]...")
    engine = get_stock_engine()
    start_int = start_date.replace('-', '')
    end_int = end_date.replace('-', '')
    sql = f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1
          AND cal_date >= '{start_int}'
          AND cal_date <= '{end_int}'
        ORDER BY cal_date
    """
    df = pd.read_sql(sql, engine)
    trading_days = sorted(pd.to_datetime(df['cal_date'], format='%Y%m%d').dt.date.tolist())
    logger.info(f"  交易日: {len(trading_days)} 天")
    return trading_days


def load_price_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    从 my_stock.market_daily + adj_factor 加载前复权收盘价

    前复权计算方式：
        qfq_close = close * adj_factor / latest_adj_factor
    使得最新价格不变，历史价格按复权因子调整，涨跌幅连续可比。

    Args:
        start_date: 起始日期，格式 'YYYY-MM-DD'
        end_date:   结束日期，格式 'YYYY-MM-DD'

    Returns:
        DataFrame，columns: [ts_code, stock_code, date, close, adj_factor, qfq_close]
    """
    logger.info(f"加载行情数据（前复权）[{start_date} ~ {end_date}]...")
    engine = get_stock_engine()
    start_int = start_date.replace('-', '')
    end_int = end_date.replace('-', '')
    sql = f"""
        SELECT m.ts_code, m.trade_date, m.close, a.adj_factor
        FROM market_daily m
        JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        WHERE m.trade_date >= '{start_int}'
          AND m.trade_date <= '{end_int}'
    """
    df = pd.read_sql(sql, engine)
    # 日期和股票代码格式转换
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
    df['stock_code'] = df['ts_code'].str[:6]  # '000001.SZ' → '000001'

    # 前复权：当前价格不变，历史价格按复权因子调整
    # latest_adj 取每只股票最新的 adj_factor
    latest_adj = df.groupby('stock_code')['adj_factor'].transform('last')
    df['qfq_close'] = df['close'] * df['adj_factor'] / latest_adj

    logger.info(f"  行情数据: {len(df):,} 条, {df['stock_code'].nunique()} 只股票")
    return df


def load_index_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    从 my_stock.index_daily 加载沪深300指数数据

    Args:
        start_date: 起始日期，格式 'YYYY-MM-DD'
        end_date:   结束日期，格式 'YYYY-MM-DD'

    Returns:
        DataFrame，columns: [trade_date, index_close, date]
    """
    logger.info(f"加载沪深300指数 [{start_date} ~ {end_date}]...")
    engine = get_stock_engine()
    start_int = start_date.replace('-', '')
    end_int = end_date.replace('-', '')
    sql = f"""
        SELECT trade_date, close as index_close FROM index_daily
        WHERE ts_code = '000300.SH'
          AND trade_date >= '{start_int}'
          AND trade_date <= '{end_int}'
        ORDER BY trade_date
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
    logger.info(f"  沪深300: {len(df)} 天")
    return df


def load_data_bundle(start_date: str, end_date: str) -> dict:
    """
    一次性加载策略所需全部数据，供策略和优化器复用

    Args:
        start_date: 起始日期，格式 'YYYY-MM-DD'
        end_date:   结束日期，格式 'YYYY-MM-DD'

    Returns:
        dict，包含:
            'heat_df':       热度排名 DataFrame
            'trading_days':  交易日列表（list of date）
            'price_df':      前复权行情 DataFrame
            'index_df':      沪深300指数 DataFrame
    """
    logger.info("=" * 60)
    logger.info("开始加载数据包...")
    logger.info("=" * 60)

    heat_df = load_heat_data(start_date, end_date)
    trading_days = load_trading_days(start_date, end_date)
    price_df = load_price_data(start_date, end_date)
    index_df = load_index_data(start_date, end_date)

    # 预构建价格查询表（避免每次参数评估时重建，加速优化器 10x+）
    # 用 numpy 数组索引代替 iterrows，速度提升 ~20x
    logger.info("预构建价格查询表...")
    arr = price_df[['stock_code', 'date', 'qfq_close', 'close']].to_numpy()
    price_lookup = {
        (str(arr[i, 0]), arr[i, 1]): {
            'qfq_close': float(arr[i, 2]),
            'close': float(arr[i, 3]),
        }
        for i in range(len(arr))
    }
    # 预构建指数查询表
    idx_lookup = index_df.set_index('date')['index_close'].to_dict()
    logger.info(f"  价格查询表: {len(price_lookup):,} 条，指数查询表: {len(idx_lookup)} 天")

    logger.info("数据包加载完成")
    return {
        'heat_df': heat_df,
        'trading_days': trading_days,
        'price_df': price_df,
        'index_df': index_df,
        'price_lookup': price_lookup,   # 预构建，优化器直接复用
        'idx_lookup': idx_lookup,        # 预构建，优化器直接复用
    }
