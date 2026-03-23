"""
K线数据（OHLCV）

加密货币无需复权，价格直接使用原始值。
全量存储所有历史K线数据（数据量小，可承受）。
"""
from datetime import datetime

from sqlalchemy import (
    Column, BigInteger, String, Float, DateTime,
    Index, UniqueConstraint,
)

from app.database import Base


class CryptoKline(Base):
    """K线数据"""
    __tablename__ = "crypto_kline"
    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "timeframe", "open_time",
                         name="uq_kline"),
        Index("idx_kline_query", "symbol", "timeframe", "open_time"),
        {"comment": "加密货币K线数据（无需复权，全量存储）"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="主键")
    exchange = Column(String(20), nullable=False, comment="交易所(binance/okx)")
    symbol = Column(String(30), nullable=False, comment="交易对(BTC/USDT)")
    timeframe = Column(String(10), nullable=False, comment="K线周期(1h/4h/1d)")
    open_time = Column(DateTime, nullable=False, comment="开盘时间(UTC)")
    open = Column(Float, nullable=False, comment="开盘价")
    high = Column(Float, nullable=False, comment="最高价")
    low = Column(Float, nullable=False, comment="最低价")
    close = Column(Float, nullable=False, comment="收盘价")
    volume = Column(Float, nullable=False, comment="成交量(基础货币)")
    quote_volume = Column(Float, default=0, comment="成交额(计价货币)")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now,
                        comment="更新时间")
