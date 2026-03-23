"""
加密货币量化交易 - 数据获取模块

功能：
  1. 从交易所获取历史K线数据并存入MySQL
  2. 增量更新K线数据
  3. 从数据库读取K线用于策略计算
"""
import sys
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(__file__))
from config import KLINE_FETCH_LIMIT
from database import engine, Session, init_tables
from models import CryptoKline
from exchange_client import create_exchange, fetch_ohlcv, get_exchange_name

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from logger import get_logger

logger = get_logger('crypto.data')


def fetch_and_store_klines(symbol: str, timeframe: str, exchange_name: str = None,
                           days: int = 365, exchange=None) -> int:
    """
    获取历史K线数据并存入数据库

    Args:
        symbol: 交易对（如 BTC/USDT）
        timeframe: K线周期
        exchange_name: 交易所名称
        days: 获取最近多少天的数据
        exchange: 已有的交易所实例（可选，避免重复创建）

    Returns:
        新增条数
    """
    init_tables()

    if exchange is None:
        exchange = create_exchange(exchange_name)
    ex_name = get_exchange_name(exchange)

    # 计算起始时间
    since = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    all_ohlcv = []
    fetch_since = since

    logger.info(f"开始获取 {symbol} {timeframe} K线，起始: {datetime.fromtimestamp(since/1000)}")

    # 分页获取所有历史数据
    while True:
        try:
            ohlcv = fetch_ohlcv(exchange, symbol, timeframe,
                                since=fetch_since, limit=KLINE_FETCH_LIMIT)
        except Exception as e:
            logger.error(f"获取数据出错，已获取 {len(all_ohlcv)} 条: {e}")
            break

        if not ohlcv:
            break

        all_ohlcv.extend(ohlcv)

        # 如果返回数据少于 limit，说明已到最新
        if len(ohlcv) < KLINE_FETCH_LIMIT:
            break

        # 下一页从最后一条的时间+1ms开始
        fetch_since = ohlcv[-1][0] + 1

        # 限流：避免触发交易所频率限制
        time.sleep(exchange.rateLimit / 1000)

    if not all_ohlcv:
        logger.warning(f"未获取到任何K线数据: {symbol} {timeframe}")
        return 0

    logger.info(f"共获取 {len(all_ohlcv)} 条K线数据")

    # 转为 DataFrame
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['open_time'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['exchange'] = ex_name
    df['symbol'] = symbol
    df['timeframe'] = timeframe
    df = df.drop(columns=['timestamp'])

    # 去重：按唯一键去重
    df = df.drop_duplicates(subset=['exchange', 'symbol', 'timeframe', 'open_time'])

    # 写入数据库（使用 REPLACE INTO 实现 upsert）
    session = Session()
    inserted = 0
    try:
        for _, row in df.iterrows():
            # 检查是否已存在
            existing = session.query(CryptoKline).filter_by(
                exchange=row['exchange'],
                symbol=row['symbol'],
                timeframe=row['timeframe'],
                open_time=row['open_time'],
            ).first()

            if existing:
                # 更新
                existing.open = row['open']
                existing.high = row['high']
                existing.low = row['low']
                existing.close = row['close']
                existing.volume = row['volume']
            else:
                # 新增
                kline = CryptoKline(
                    exchange=row['exchange'],
                    symbol=row['symbol'],
                    timeframe=row['timeframe'],
                    open_time=row['open_time'],
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume'],
                )
                session.add(kline)
                inserted += 1

        session.commit()
        logger.info(f"数据入库完成: 新增 {inserted} 条，总计 {len(df)} 条")
    except Exception as e:
        session.rollback()
        logger.error(f"数据入库失败: {e}")
        raise
    finally:
        session.close()

    return inserted


def update_klines(symbol: str, timeframe: str, exchange_name: str = None) -> int:
    """
    增量更新K线数据：从数据库中最新一条之后开始获取

    Args:
        symbol: 交易对
        timeframe: K线周期
        exchange_name: 交易所名称

    Returns:
        新增条数
    """
    init_tables()
    exchange = create_exchange(exchange_name)
    ex_name = get_exchange_name(exchange)

    # 查询数据库中最新时间
    session = Session()
    try:
        latest = session.query(CryptoKline).filter_by(
            exchange=ex_name, symbol=symbol, timeframe=timeframe,
        ).order_by(CryptoKline.open_time.desc()).first()
    finally:
        session.close()

    if latest:
        since = int(latest.open_time.timestamp() * 1000) + 1
        logger.info(f"增量更新 {symbol} {timeframe}，从 {latest.open_time} 之后开始")
        # 计算天数差
        days = (datetime.now() - latest.open_time).days + 1
    else:
        logger.info(f"数据库无历史数据，将获取全量: {symbol} {timeframe}")
        days = 365

    return fetch_and_store_klines(symbol, timeframe, exchange_name=exchange_name,
                                  days=days, exchange=exchange)


def load_klines(symbol: str, timeframe: str, exchange_name: str = None,
                start_date: str = None, end_date: str = None,
                limit: int = None) -> pd.DataFrame:
    """
    从数据库加载K线数据

    Args:
        symbol: 交易对
        timeframe: K线周期
        exchange_name: 交易所名称（默认全部）
        start_date: 起始日期（YYYY-MM-DD）
        end_date: 结束日期（YYYY-MM-DD）
        limit: 最大条数

    Returns:
        DataFrame: open_time, open, high, low, close, volume
    """
    conditions = ["symbol = :symbol", "timeframe = :timeframe"]
    params = {'symbol': symbol, 'timeframe': timeframe}

    if exchange_name:
        conditions.append("exchange = :exchange")
        params['exchange'] = exchange_name

    if start_date:
        conditions.append("open_time >= :start_date")
        params['start_date'] = start_date

    if end_date:
        conditions.append("open_time <= :end_date")
        params['end_date'] = end_date

    where_clause = " AND ".join(conditions)
    sql = f"SELECT open_time, open, high, low, close, volume FROM crypto_kline WHERE {where_clause} ORDER BY open_time"

    if limit:
        sql += f" LIMIT {limit}"

    df = pd.read_sql(text(sql), engine, params=params)
    if not df.empty:
        df['open_time'] = pd.to_datetime(df['open_time'])
        df = df.set_index('open_time')

    logger.info(f"加载K线: {symbol} {timeframe}, {len(df)} 条")
    return df


if __name__ == '__main__':
    """命令行用法: python data_fetcher.py [symbol] [timeframe] [days]"""
    import argparse

    parser = argparse.ArgumentParser(description='获取加密货币K线数据')
    parser.add_argument('--symbol', default='BTC/USDT', help='交易对')
    parser.add_argument('--timeframe', default='1h', help='K线周期')
    parser.add_argument('--days', type=int, default=365, help='获取天数')
    parser.add_argument('--exchange', default=None, help='交易所(binance/okx)')
    parser.add_argument('--update', action='store_true', help='增量更新模式')
    args = parser.parse_args()

    if args.update:
        count = update_klines(args.symbol, args.timeframe, args.exchange)
    else:
        count = fetch_and_store_klines(args.symbol, args.timeframe,
                                       exchange_name=args.exchange, days=args.days)
    print(f"完成: 新增 {count} 条K线数据")
