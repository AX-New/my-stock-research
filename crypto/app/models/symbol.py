"""
交易对元数据

记录所有已同步的交易对信息，用于管理同步范围和查询。
加密货币数据量小，存储全量数据（所有交易对、所有周期的完整历史K线）。
"""
from datetime import datetime

from sqlalchemy import (
    Column, BigInteger, String, Float, DateTime, Integer,
    Index, UniqueConstraint,
)

from app.database import Base


class CryptoSymbol(Base):
    """交易对元数据"""
    __tablename__ = "crypto_symbol"
    __table_args__ = (
        UniqueConstraint("exchange", "symbol", name="uq_symbol"),
        Index("idx_symbol_status", "status"),
        {"comment": "交易对元数据"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="主键")
    exchange = Column(String(20), nullable=False, comment="交易所(binance/okx)")
    symbol = Column(String(30), nullable=False, comment="交易对(BTC/USDT)")
    base_currency = Column(String(20), nullable=False, comment="基础货币(BTC)")
    quote_currency = Column(String(20), nullable=False, comment="计价货币(USDT)")
    price_precision = Column(Integer, default=8, comment="价格精度(小数位)")
    amount_precision = Column(Integer, default=8, comment="数量精度(小数位)")
    min_amount = Column(Float, default=0, comment="最小交易数量")
    min_cost = Column(Float, default=0, comment="最小交易金额(USDT)")
    maker_fee = Column(Float, default=0.001, comment="Maker手续费率")
    taker_fee = Column(Float, default=0.001, comment="Taker手续费率")
    status = Column(String(20), default="active", comment="状态(active/inactive)")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now,
                        comment="更新时间")
