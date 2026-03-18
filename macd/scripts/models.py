"""16张 MACD 表模型定义（工厂函数生成）

股票: 4周期 × 3复权 = 12张
指数: 4周期 × 1(无复权) = 4张
"""
from sqlalchemy import Column, BigInteger, Integer, String, Float, DateTime, UniqueConstraint, func

from database import ResearchBase


def _make_macd_model(table_name: str) -> type:
    """生成 MACD 表模型类"""
    class_name = "".join(word.capitalize() for word in table_name.split("_"))
    attrs = {
        "__tablename__": table_name,
        "__table_args__": (
            UniqueConstraint("ts_code", "trade_date", name=f"uk_{table_name}"),
            {"comment": f"MACD指标 - {table_name}"},
        ),
        "id": Column(BigInteger, primary_key=True, autoincrement=True),
        "ts_code": Column(String(20), nullable=False, comment="股票/指数代码"),
        "trade_date": Column(String(10), nullable=False, comment="交易日期"),
        "open": Column(Float, comment="开盘价"),
        "high": Column(Float, comment="最高价"),
        "low": Column(Float, comment="最低价"),
        "close": Column(Float, comment="收盘价"),
        "vol": Column(Float, comment="成交量"),
        "pct_chg": Column(Float, comment="涨跌幅(%)"),
        "dif": Column(Float, comment="DIF (EMA12 - EMA26)"),
        "dea": Column(Float, comment="DEA (DIF的EMA9)"),
        "macd": Column(Float, comment="MACD柱 (DIF - DEA) × 2"),
        "created_at": Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间"),
        "updated_at": Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间"),
    }
    return type(class_name, (ResearchBase,), attrs)


_FREQS = ("daily", "weekly", "monthly", "yearly")
_ADJS = ("bfq", "qfq", "hfq")

# 查找表: 按 (类型, 周期, 复权) 获取模型类
MODEL_MAP = {}

# 股票 MACD — 12张表
for _freq in _FREQS:
    for _adj in _ADJS:
        _table = f"stock_macd_{_freq}_{_adj}"
        MODEL_MAP[("stock", _freq, _adj)] = _make_macd_model(_table)

# 指数 MACD — 4张表
for _freq in _FREQS:
    _table = f"index_macd_{_freq}"
    MODEL_MAP[("index", _freq, "none")] = _make_macd_model(_table)


def _make_macd_signal_model(table_name: str) -> type:
    """生成 MACD 信号事件表模型（个股/指数共用结构）

    存储各类 MACD 信号（交叉/零轴穿越/背离/DIF极值），
    附带信号发出后 5/10/20/60 日收益率
    """
    class_name = "".join(word.capitalize() for word in table_name.split("_"))
    attrs = {
        "__tablename__": table_name,
        "__table_args__": (
            UniqueConstraint("ts_code", "trade_date", "freq", "signal_name",
                             name=f"uk_{table_name}"),
            {"comment": f"MACD信号 - {table_name}"},
        ),
        "id": Column(BigInteger, primary_key=True, autoincrement=True),
        "ts_code": Column(String(20), nullable=False, comment="股票/指数代码"),
        "trade_date": Column(String(10), nullable=False, comment="交易日期"),
        "freq": Column(String(10), nullable=False, comment="周期: daily/weekly/monthly"),
        # 信号信息
        "signal_type": Column(String(30), nullable=False,
                              comment="信号大类: cross/zero_cross/divergence/dif_extreme"),
        "signal_name": Column(String(50), nullable=False,
                              comment="具体信号名: golden_cross/dif_peak 等"),
        "direction": Column(String(10), comment="方向: buy/sell"),
        "signal_value": Column(Float, comment="信号关联数值（DIF值）"),
        "close": Column(Float, comment="信号发出时收盘价"),
        "dif": Column(Float, comment="DIF值"),
        "dea": Column(Float, comment="DEA值"),
        # 信号后收益率
        "ret_5": Column(Float, comment="信号后5日收益率(%)"),
        "ret_10": Column(Float, comment="信号后10日收益率(%)"),
        "ret_20": Column(Float, comment="信号后20日收益率(%)"),
        "ret_60": Column(Float, comment="信号后60日收益率(%)"),
        # 牛熊标注（可后续填充）
        "trend": Column(String(10), comment="牛熊: bull/bear/unknown"),
        "phase_id": Column(Integer, comment="牛熊周期编号"),
        "phase_label": Column(String(50), comment="牛熊周期标签"),
        # 时间戳
        "created_at": Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间"),
        "updated_at": Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间"),
    }
    return type(class_name, (ResearchBase,), attrs)


# 信号表
SIGNAL_MAP = {
    "stock": _make_macd_signal_model("stock_macd_signal"),
}
