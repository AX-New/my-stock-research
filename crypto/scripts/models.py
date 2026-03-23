"""
加密货币量化交易 - 数据模型

表结构：
  - crypto_kline: K线数据（OHLCV）
  - crypto_trade: 交易记录
  - crypto_position: 持仓记录
  - crypto_signal: 策略信号
"""
from datetime import datetime
from sqlalchemy import (
    Column, BigInteger, String, Float, DateTime,
    Integer, Text, Index, UniqueConstraint,
)
from database import CryptoBase


class CryptoKline(CryptoBase):
    """K线数据（OHLCV）"""
    __tablename__ = 'crypto_kline'
    __table_args__ = (
        UniqueConstraint('exchange', 'symbol', 'timeframe', 'open_time',
                         name='uq_kline'),
        Index('idx_kline_query', 'symbol', 'timeframe', 'open_time'),
        {'comment': '加密货币K线数据'},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键')
    exchange = Column(String(20), nullable=False, comment='交易所(binance/okx)')
    symbol = Column(String(30), nullable=False, comment='交易对(BTC/USDT)')
    timeframe = Column(String(10), nullable=False, comment='K线周期(1h/4h/1d)')
    open_time = Column(DateTime, nullable=False, comment='开盘时间')
    open = Column(Float, nullable=False, comment='开盘价')
    high = Column(Float, nullable=False, comment='最高价')
    low = Column(Float, nullable=False, comment='最低价')
    close = Column(Float, nullable=False, comment='收盘价')
    volume = Column(Float, nullable=False, comment='成交量')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now,
                        comment='更新时间')


class CryptoSignal(CryptoBase):
    """策略信号"""
    __tablename__ = 'crypto_signal'
    __table_args__ = (
        Index('idx_signal_time', 'symbol', 'signal_time'),
        {'comment': '策略交易信号'},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键')
    exchange = Column(String(20), nullable=False, comment='交易所')
    symbol = Column(String(30), nullable=False, comment='交易对')
    timeframe = Column(String(10), nullable=False, comment='K线周期')
    strategy = Column(String(50), nullable=False, comment='策略名称')
    signal = Column(String(10), nullable=False, comment='信号类型(BUY/SELL/HOLD)')
    signal_time = Column(DateTime, nullable=False, comment='信号产生时间')
    price = Column(Float, nullable=False, comment='信号价格')
    reason = Column(Text, comment='信号原因说明')
    indicators = Column(Text, comment='指标快照JSON')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')


class CryptoTrade(CryptoBase):
    """交易记录"""
    __tablename__ = 'crypto_trade'
    __table_args__ = (
        Index('idx_trade_time', 'symbol', 'trade_time'),
        {'comment': '交易执行记录'},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键')
    exchange = Column(String(20), nullable=False, comment='交易所')
    symbol = Column(String(30), nullable=False, comment='交易对')
    side = Column(String(10), nullable=False, comment='方向(buy/sell)')
    order_type = Column(String(20), nullable=False, comment='订单类型(market/limit)')
    amount = Column(Float, nullable=False, comment='数量')
    price = Column(Float, nullable=False, comment='成交价格')
    cost = Column(Float, nullable=False, comment='总成本(USDT)')
    fee = Column(Float, default=0, comment='手续费')
    strategy = Column(String(50), comment='策略名称')
    signal_id = Column(BigInteger, comment='关联信号ID')
    order_id = Column(String(100), comment='交易所订单ID')
    status = Column(String(20), default='filled', comment='状态(filled/canceled/failed)')
    trade_time = Column(DateTime, nullable=False, comment='交易时间')
    is_paper = Column(Integer, default=1, comment='是否模拟交易(1=模拟/0=实盘)')
    note = Column(Text, comment='备注')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')


class CryptoPosition(CryptoBase):
    """持仓记录"""
    __tablename__ = 'crypto_position'
    __table_args__ = (
        UniqueConstraint('exchange', 'symbol', 'strategy', name='uq_position'),
        {'comment': '当前持仓'},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True, comment='主键')
    exchange = Column(String(20), nullable=False, comment='交易所')
    symbol = Column(String(30), nullable=False, comment='交易对')
    strategy = Column(String(50), nullable=False, comment='策略名称')
    side = Column(String(10), default='long', comment='方向(long/short)')
    amount = Column(Float, default=0, comment='持仓数量')
    avg_price = Column(Float, default=0, comment='平均持仓成本')
    current_price = Column(Float, default=0, comment='当前价格')
    unrealized_pnl = Column(Float, default=0, comment='未实现盈亏')
    realized_pnl = Column(Float, default=0, comment='已实现盈亏')
    stop_loss = Column(Float, comment='止损价')
    take_profit = Column(Float, comment='止盈价')
    status = Column(String(20), default='open', comment='状态(open/closed)')
    created_at = Column(DateTime, default=datetime.now, comment='创建时间')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now,
                        comment='更新时间')
