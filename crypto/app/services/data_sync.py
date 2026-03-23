"""
数据同步服务

加密货币数据量小，采用全量存储策略：
  - 所有交易对的完整历史K线
  - 所有周期（1h/4h/1d 等）
  - 无需复权，价格直接使用原始值

与 A 股不同：
  - 数据量级：单个交易对全历史 ~ 几万条（vs A股几千支 × 每支几千条）
  - 无复权：加密货币不存在除权除息
  - 7×24 交易：无休市概念
"""
import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from app.config import Config
from app.database import engine, SessionLocal, init_db
from app.db_utils import batch_upsert
from app.models.kline import CryptoKline
from app.models.symbol import CryptoSymbol
from app.services.exchange_client import (
    create_exchange, fetch_ohlcv, fetch_markets, get_exchange_name,
)
from app.logger import get_logger

logger = get_logger("crypto.data_sync")


def sync_symbols(exchange_name: str = None) -> int:
    """
    同步交易对元数据

    从交易所获取所有 USDT 交易对信息并存入数据库。
    """
    init_db()
    exchange = create_exchange(exchange_name)
    ex_name = get_exchange_name(exchange)

    markets = fetch_markets(exchange)
    if not markets:
        logger.warning("未获取到交易对信息")
        return 0

    records = []
    for m in markets:
        records.append({
            "exchange": ex_name,
            "symbol": m["symbol"],
            "base_currency": m["base"],
            "quote_currency": m["quote"],
            "price_precision": m.get("price_precision", 8),
            "amount_precision": m.get("amount_precision", 8),
            "min_amount": m.get("min_amount", 0) or 0,
            "min_cost": m.get("min_cost", 0) or 0,
            "maker_fee": m.get("maker_fee", 0.001) or 0.001,
            "taker_fee": m.get("taker_fee", 0.001) or 0.001,
            "status": "active",
        })

    batch_upsert(CryptoSymbol, records, ["exchange", "symbol"])
    logger.info(f"同步交易对完成: {ex_name}, {len(records)} 个")
    return len(records)


def sync_klines(symbol: str, timeframe: str, exchange_name: str = None,
                days: int = None, exchange=None) -> int:
    """
    全量同步K线数据

    加密货币数据量小，默认获取全部历史数据。
    自动分页获取，支持增量更新（从最新一条之后开始）。

    Args:
        symbol: 交易对（如 BTC/USDT）
        timeframe: K线周期
        exchange_name: 交易所名称
        days: 获取天数（默认 Config.FULL_SYNC_DAYS=3650，约10年全量）
        exchange: 已有的交易所实例（可选）

    Returns:
        入库条数
    """
    init_db()

    if exchange is None:
        exchange = create_exchange(exchange_name)
    ex_name = get_exchange_name(exchange)

    if days is None:
        days = Config.FULL_SYNC_DAYS

    # 检查数据库中已有的最新时间，实现增量更新
    session = SessionLocal()
    try:
        latest = session.query(CryptoKline).filter_by(
            exchange=ex_name, symbol=symbol, timeframe=timeframe,
        ).order_by(CryptoKline.open_time.desc()).first()
    finally:
        session.close()

    if latest:
        since = int(latest.open_time.timestamp() * 1000) + 1
        logger.info(f"增量同步 {symbol} {timeframe}，从 {latest.open_time} 之后开始")
    else:
        since = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
        logger.info(f"全量同步 {symbol} {timeframe}，起始: "
                    f"{datetime.utcfromtimestamp(since / 1000)}")

    # 分页获取所有历史数据
    all_ohlcv = []
    fetch_since = since

    while True:
        try:
            ohlcv = fetch_ohlcv(exchange, symbol, timeframe,
                                since=fetch_since, limit=Config.KLINE_FETCH_LIMIT)
        except Exception as e:
            logger.error(f"获取数据出错，已获取 {len(all_ohlcv)} 条: {e}")
            break

        if not ohlcv:
            break

        all_ohlcv.extend(ohlcv)

        # 返回数据少于 limit，说明已到最新
        if len(ohlcv) < Config.KLINE_FETCH_LIMIT:
            break

        # 下一页从最后一条的时间+1ms开始
        fetch_since = ohlcv[-1][0] + 1

        # 限流
        time.sleep(exchange.rateLimit / 1000)

    if not all_ohlcv:
        logger.warning(f"未获取到K线数据: {symbol} {timeframe}")
        return 0

    logger.info(f"共获取 {len(all_ohlcv)} 条K线数据: {symbol} {timeframe}")

    # 转为记录列表
    records = []
    seen = set()
    for row in all_ohlcv:
        key = (ex_name, symbol, timeframe, row[0])
        if key in seen:
            continue
        seen.add(key)
        records.append({
            "exchange": ex_name,
            "symbol": symbol,
            "timeframe": timeframe,
            "open_time": datetime.utcfromtimestamp(row[0] / 1000),
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "quote_volume": 0,
        })

    # 批量入库
    batch_upsert(CryptoKline, records,
                 ["exchange", "symbol", "timeframe", "open_time"])

    logger.info(f"K线入库完成: {symbol} {timeframe}, {len(records)} 条")
    return len(records)


def sync_all(exchange_name: str = None,
             symbols: list[str] = None,
             timeframes: list[str] = None) -> dict:
    """
    全量同步：所有交易对 × 所有周期

    Args:
        exchange_name: 交易所名称
        symbols: 交易对列表（默认从配置读取）
        timeframes: K线周期列表（默认从配置读取）

    Returns:
        同步统计 {"symbol_count": ..., "kline_count": ...}
    """
    if symbols is None:
        symbols = [s.strip() for s in Config.DEFAULT_SYMBOLS.split(",")]
    if timeframes is None:
        timeframes = [t.strip() for t in Config.DEFAULT_TIMEFRAMES.split(",")]

    exchange = create_exchange(exchange_name)

    # 1. 同步交易对元数据
    symbol_count = sync_symbols(exchange_name)

    # 2. 逐个同步K线
    total_klines = 0
    for symbol in symbols:
        for tf in timeframes:
            try:
                count = sync_klines(symbol, tf, exchange=exchange)
                total_klines += count
            except Exception as e:
                logger.error(f"同步失败 {symbol} {tf}: {e}")

    logger.info(f"全量同步完成: {len(symbols)} 个交易对, "
                f"{len(timeframes)} 个周期, 共 {total_klines} 条K线")

    return {
        "symbol_count": symbol_count,
        "kline_count": total_klines,
        "symbols": symbols,
        "timeframes": timeframes,
    }


def load_klines(symbol: str, timeframe: str, exchange_name: str = None,
                start_date: str = None, end_date: str = None,
                limit: int = None) -> pd.DataFrame:
    """
    从数据库加载K线数据

    Returns:
        DataFrame: index=open_time, columns=[open, high, low, close, volume]
    """
    conditions = ["symbol = :symbol", "timeframe = :timeframe"]
    params = {"symbol": symbol, "timeframe": timeframe}

    if exchange_name:
        conditions.append("exchange = :exchange")
        params["exchange"] = exchange_name

    if start_date:
        conditions.append("open_time >= :start_date")
        params["start_date"] = start_date

    if end_date:
        conditions.append("open_time <= :end_date")
        params["end_date"] = end_date

    where_clause = " AND ".join(conditions)
    sql = (f"SELECT open_time, open, high, low, close, volume "
           f"FROM crypto_kline WHERE {where_clause} ORDER BY open_time")

    if limit:
        sql += f" LIMIT {limit}"

    df = pd.read_sql(text(sql), engine, params=params)
    if not df.empty:
        df["open_time"] = pd.to_datetime(df["open_time"])
        df = df.set_index("open_time")

    logger.info(f"加载K线: {symbol} {timeframe}, {len(df)} 条")
    return df
