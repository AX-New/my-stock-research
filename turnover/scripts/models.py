"""换手率研究 — 数据模型定义"""
from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, DateTime, Index, func,
)
from database import TurnoverBase


class IndexTurnoverSignal(TurnoverBase):
    """指数换手率信号明细表（L1/L2/L2.5 共用）"""
    __tablename__ = "index_turnover_signal"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="指数代码")
    trade_date = Column(String(10), nullable=False, comment="交易日期 YYYYMMDD")
    freq = Column(String(10), nullable=False, comment="周期: daily/weekly/monthly")
    signal_type = Column(String(20), nullable=False, comment="信号大类: extreme/divergence/zone/persistent/surge/cross")
    signal_name = Column(String(40), nullable=False, comment="具体信号名")
    direction = Column(String(10), comment="方向: buy/sell")
    signal_value = Column(Float, comment="换手率数值")
    # 后续收益（日线窗口，周线/月线窗口在写入时映射到同名字段）
    ret_5 = Column(Float, comment="后续收益 T+5")
    ret_10 = Column(Float, comment="后续收益 T+10")
    ret_20 = Column(Float, comment="后续收益 T+20")
    ret_60 = Column(Float, comment="后续收益 T+60")
    bull_bear = Column(String(10), comment="牛熊: bull/bear")
    bull_bear_sub = Column(String(30), comment="牛熊子类型")
    created_at = Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间")

    __table_args__ = (
        Index("uk_signal", "ts_code", "trade_date", "freq", "signal_name", unique=True),
        Index("idx_type", "signal_type"),
        Index("idx_date", "trade_date"),
    )


# 信号表唯一键（供 batch_upsert 使用）
SIGNAL_UNIQUE_KEYS = ["ts_code", "trade_date", "freq", "signal_name"]


class StockTurnoverSignal(TurnoverBase):
    """个股换手率信号明细表"""
    __tablename__ = "stock_turnover_signal"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    trade_date = Column(String(10), nullable=False, comment="交易日期 YYYYMMDD")
    freq = Column(String(10), nullable=False, comment="周期: daily/weekly/monthly")
    signal_type = Column(String(20), nullable=False, comment="信号大类: extreme/divergence/zone/persistent/surge/cross")
    signal_name = Column(String(40), nullable=False, comment="具体信号名")
    direction = Column(String(10), comment="方向: buy/sell")
    signal_value = Column(Float, comment="换手率数值")
    close = Column(Float, comment="信号发出时收盘价")
    ret_5 = Column(Float, comment="后续收益 T+5")
    ret_10 = Column(Float, comment="后续收益 T+10")
    ret_20 = Column(Float, comment="后续收益 T+20")
    ret_60 = Column(Float, comment="后续收益 T+60")
    bull_bear = Column(String(10), comment="牛熊: bull/bear")
    bull_bear_sub = Column(String(30), comment="牛熊子类型")
    created_at = Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间")

    __table_args__ = (
        Index("uk_stk_turnover_signal", "ts_code", "trade_date", "freq", "signal_name", unique=True),
        Index("idx_stk_turn_sig_type", "signal_type"),
        Index("idx_stk_turn_sig_date", "trade_date"),
    )


STOCK_SIGNAL_UNIQUE_KEYS = ["ts_code", "trade_date", "freq", "signal_name"]
