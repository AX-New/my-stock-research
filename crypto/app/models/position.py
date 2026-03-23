"""持仓记录"""
from datetime import datetime

from sqlalchemy import (
    Column, BigInteger, String, Float, DateTime,
    Index, UniqueConstraint,
)

from app.database import Base


class CryptoPosition(Base):
    """当前持仓"""
    __tablename__ = "crypto_position"
    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "strategy", name="uq_position"),
        Index("idx_position_status", "status"),
        {"comment": "当前持仓"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment="主键")
    exchange = Column(String(20), nullable=False, comment="交易所")
    symbol = Column(String(30), nullable=False, comment="交易对")
    strategy = Column(String(50), nullable=False, comment="策略名称")
    side = Column(String(10), default="long", comment="方向(long/short)")
    amount = Column(Float, default=0, comment="持仓数量")
    avg_price = Column(Float, default=0, comment="平均持仓成本")
    current_price = Column(Float, default=0, comment="当前价格")
    unrealized_pnl = Column(Float, default=0, comment="未实现盈亏")
    realized_pnl = Column(Float, default=0, comment="已实现盈亏")
    stop_loss = Column(Float, comment="止损价")
    take_profit = Column(Float, comment="止盈价")
    status = Column(String(20), default="open", comment="状态(open/closed)")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now,
                        comment="更新时间")
