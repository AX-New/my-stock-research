"""交易执行记录"""
from datetime import datetime

from sqlalchemy import (
    Column, BigInteger, String, Float, DateTime, Integer, Text, Index,
)

from app.database import Base


class CryptoTrade(Base):
    """交易记录"""
    __tablename__ = "crypto_trade"
    __table_args__ = (
        Index("idx_trade_time", "symbol", "trade_time"),
        Index("idx_trade_strategy", "strategy", "trade_time"),
        {"comment": "交易执行记录"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="主键")
    exchange = Column(String(20), nullable=False, comment="交易所")
    symbol = Column(String(30), nullable=False, comment="交易对")
    side = Column(String(10), nullable=False, comment="方向(buy/sell)")
    order_type = Column(String(20), nullable=False, comment="订单类型(market/limit)")
    amount = Column(Float, nullable=False, comment="数量")
    price = Column(Float, nullable=False, comment="成交价格")
    cost = Column(Float, nullable=False, comment="总成本(USDT)")
    fee = Column(Float, default=0, comment="手续费")
    strategy = Column(String(50), comment="策略名称")
    signal_id = Column(BigInteger, comment="关联信号ID")
    order_id = Column(String(100), comment="交易所订单ID")
    status = Column(String(20), default="filled", comment="状态(filled/canceled/failed)")
    trade_time = Column(DateTime, nullable=False, comment="交易时间")
    is_paper = Column(Integer, default=1, comment="是否模拟交易(1=模拟/0=实盘)")
    note = Column(Text, comment="备注")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
