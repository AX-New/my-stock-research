"""
ML 分析数据加载模块

加载热度排名、行情数据、交易日历，合并为统一的分析 DataFrame。
所有数据均为只读，热度数据来自 my_trend，行情数据来自 my_stock。
"""
import logging
import pandas as pd
import numpy as np

from ml.scripts.database import get_stock_engine, get_trend_engine

logger = logging.getLogger(__name__)


def load_heat_data(start_date: str, end_date: str, max_rank: int = 500) -> pd.DataFrame:
    """
    从 my_trend.popularity_rank 加载热度排名数据

    只加载 rank <= max_rank 的热门股票，避免加载全量 200万+ 条数据（SSH 隧道太慢）。
    默认 max_rank=500 约 18万条，覆盖绝大部分有分析价值的股票。

    Args:
        start_date: 起始日期
        end_date: 结束日期
        max_rank: 最大排名（默认500，即每天Top500热门股）
    """
    logger.info(f"加载热度数据 [{start_date} ~ {end_date}], rank<={max_rank}...")
    engine = get_trend_engine()
    sql = f"""
        SELECT stock_code, stock_name, date, `rank`,
               new_price, change_rate, volume_ratio,
               turnover_rate, volume, deal_amount
        FROM popularity_rank
        WHERE date >= '{start_date}' AND date <= '{end_date}'
          AND `rank` <= {max_rank}
        ORDER BY date, `rank`
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['date']).dt.date
    logger.info(f"  热度数据: {len(df):,} 条, {df['stock_code'].nunique()} 只股票, "
                f"{df['date'].nunique()} 天")
    return df


def load_price_data(start_date: str, end_date: str, stock_codes: list = None) -> pd.DataFrame:
    """
    从 my_stock 加载前复权日线行情

    Args:
        start_date: 起始日期
        end_date: 结束日期
        stock_codes: 只加载这些股票（6位代码），None=全量加载
    """
    logger.info(f"加载行情数据 [{start_date} ~ {end_date}]"
                f"{f', {len(stock_codes)} 只股票' if stock_codes else ''}...")
    engine = get_stock_engine()
    start_int = start_date.replace('-', '')
    end_int = end_date.replace('-', '')

    # 如果有股票过滤列表，构建 IN 条件（转为 ts_code 格式需要匹配 LIKE）
    stock_filter = ''
    if stock_codes:
        # stock_code 是 6位数字，ts_code 是 000001.SZ 格式
        codes_str = ','.join(f"'{c}'" for c in stock_codes)
        stock_filter = f"AND SUBSTRING(m.ts_code, 1, 6) IN ({codes_str})"

    sql = f"""
        SELECT m.ts_code, m.trade_date, m.open, m.high, m.low, m.close,
               m.vol, m.amount, m.pct_chg, m.pre_close,
               a.adj_factor
        FROM market_daily m
        JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        WHERE m.trade_date >= '{start_int}' AND m.trade_date <= '{end_int}'
        {stock_filter}
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
    df['stock_code'] = df['ts_code'].str[:6]

    # 前复权计算：使最新价格不变，历史价格按复权因子调整
    latest_adj = df.groupby('stock_code')['adj_factor'].transform('last')
    df['qfq_close'] = df['close'] * df['adj_factor'] / latest_adj
    df['qfq_open'] = df['open'] * df['adj_factor'] / latest_adj
    df['qfq_high'] = df['high'] * df['adj_factor'] / latest_adj
    df['qfq_low'] = df['low'] * df['adj_factor'] / latest_adj

    logger.info(f"  行情数据: {len(df):,} 条, {df['stock_code'].nunique()} 只股票")
    return df


def load_index_data(start_date: str, end_date: str) -> pd.DataFrame:
    """加载沪深300指数数据，用于计算市场环境特征"""
    logger.info(f"加载沪深300指数 [{start_date} ~ {end_date}]...")
    engine = get_stock_engine()
    start_int = start_date.replace('-', '')
    end_int = end_date.replace('-', '')
    sql = f"""
        SELECT trade_date, close as index_close, pct_chg as index_pct_chg
        FROM index_daily
        WHERE ts_code = '000300.SH'
          AND trade_date >= '{start_int}' AND trade_date <= '{end_int}'
        ORDER BY trade_date
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
    logger.info(f"  沪深300: {len(df)} 天")
    return df


def load_trading_days(start_date: str, end_date: str) -> list:
    """从 trade_cal 加载交易日历"""
    engine = get_stock_engine()
    start_int = start_date.replace('-', '')
    end_int = end_date.replace('-', '')
    sql = f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1
          AND cal_date >= '{start_int}' AND cal_date <= '{end_int}'
        ORDER BY cal_date
    """
    df = pd.read_sql(sql, engine)
    return sorted(pd.to_datetime(df['cal_date'], format='%Y%m%d').dt.date.tolist())


def load_ml_dataset(start_date: str, end_date: str, max_rank: int = 500) -> dict:
    """
    一次性加载 ML 分析所需全部数据

    Args:
        start_date: 起始日期
        end_date: 结束日期
        max_rank: 热度排名过滤（默认500，只加载热门股）

    Returns:
        dict: {
            'heat_df': 热度排名 DataFrame,
            'price_df': 前复权行情 DataFrame（仅含热度股票）,
            'index_df': 沪深300指数 DataFrame,
            'trading_days': 交易日列表,
        }
    """
    logger.info("=" * 60)
    logger.info("加载 ML 分析数据包...")
    logger.info("=" * 60)

    heat_df = load_heat_data(start_date, end_date, max_rank=max_rank)
    # 只加载热度数据中出现的股票的行情，大幅减少数据量
    heat_stocks = heat_df['stock_code'].unique().tolist()
    price_df = load_price_data(start_date, end_date, stock_codes=heat_stocks)
    index_df = load_index_data(start_date, end_date)
    trading_days = load_trading_days(start_date, end_date)

    logger.info(f"数据包加载完成: 热度 {len(heat_df):,} 条, 行情 {len(price_df):,} 条, "
                f"交易日 {len(trading_days)} 天")
    return {
        'heat_df': heat_df,
        'price_df': price_df,
        'index_df': index_df,
        'trading_days': trading_days,
    }
