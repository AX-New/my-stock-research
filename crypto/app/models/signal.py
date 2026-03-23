"""策略信号记录"""
from datetime import datetime

from sqlalchemy import (
    Column, BigInteger, String, Float, DateTime, Text, Index,
)

from app.database import Base


class CryptoSignal(Base):
    """策略信号"""
    __tablename__ = "crypto_signal"
    __table_args__ = (
        Index("idx_signal_time", "symbol", "signal_time"),
        Index("idx_signal_strategy", "strategy", "signal_time"),
        {"comment": "策略交易信号"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="主键")
    exchange = Column(String(20), nullable=False, comment="交易所")
    symbol = Column(String(30), nullable=False, comment="交易对")
    timeframe = Column(String(10), nullable=False, comment="K线周期")
    strategy = Column(String(50), nullable=False, comment="策略名称")
    signal = Column(String(10), nullable=False, comment="信号类型(BUY/SELL/HOLD)")
    signal_time = Column(DateTime, nullable=False, comment="信号产生时间")
    price = Column(Float, nullable=False, comment="信号价格")
    reason = Column(Text, comment="信号原因说明")
    indicators = Column(Text, comment="指标快照JSON")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
