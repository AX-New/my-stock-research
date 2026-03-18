"""MA 均线表模型定义（工厂函数生成）

仿照 MACD 模式:
- 指数: 4周期(日/周/月/年) × 1(无复权) = 4张
- 申万行业: 4周期(日/周/月/年) × 1(无复权) = 4张
- 股票: 4周期(日/周/月/年) × 1(仅bfq，MACD研究已证明复权差异<2%) = 4张
- 信号: 3张(指数/申万/股票)

总计: 15张表 (12张数据表 + 3张信号表)
基于 MACD 经验，复权模式差异<2%，MA研究只保留 bfq，不做多复权对比。
"""
from sqlalchemy import Column, BigInteger, String, Float, Integer, DateTime, Index, UniqueConstraint, func

from database import MABase


def _make_ma_model(table_name: str) -> type:
    """生成 MA 数据表模型类

    存储 K线行情 + 7条均线值，用于后续信号检测和收益分析
    """
    class_name = "".join(word.capitalize() for word in table_name.split("_"))
    attrs = {
        "__tablename__": table_name,
        "__table_args__": (
            UniqueConstraint("ts_code", "trade_date", name=f"uk_{table_name}"),
            Index(f"ix_{table_name}_trade_date", "trade_date"),
            {"comment": f"MA均线指标 - {table_name}"},
        ),
        "id": Column(BigInteger, primary_key=True, autoincrement=True),
        "ts_code": Column(String(20), nullable=False, comment="股票/指数代码"),
        "trade_date": Column(String(10), nullable=False, comment="交易日期"),
        # K线行情
        "open": Column(Float, comment="开盘价"),
        "high": Column(Float, comment="最高价"),
        "low": Column(Float, comment="最低价"),
        "close": Column(Float, comment="收盘价"),
        "vol": Column(Float, comment="成交量"),
        "pct_chg": Column(Float, comment="涨跌幅(%)"),
        # 7条均线
        "ma5": Column(Float, comment="MA5均线"),
        "ma10": Column(Float, comment="MA10均线"),
        "ma20": Column(Float, comment="MA20均线"),
        "ma30": Column(Float, comment="MA30均线"),
        "ma60": Column(Float, comment="MA60均线"),
        "ma90": Column(Float, comment="MA90均线"),
        "ma250": Column(Float, comment="MA250均线(年线)"),
        # 乖离率（价格偏离均线的百分比）
        "bias5": Column(Float, comment="MA5乖离率(%)"),
        "bias10": Column(Float, comment="MA10乖离率(%)"),
        "bias20": Column(Float, comment="MA20乖离率(%)"),
        "bias60": Column(Float, comment="MA60乖离率(%)"),
        # 时间戳
        "created_at": Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间"),
        "updated_at": Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间"),
    }
    return type(class_name, (MABase,), attrs)


def _make_signal_model(table_name: str) -> type:
    """生成 MA 信号表模型类

    存储各类 MA 信号（乖离率极值、方向突破、假突破、支撑压力、多线排列、收敛、金死叉等），
    附带信号发出后 5/10/20/60 日收益率及牛熊周期标注
    """
    class_name = "".join(word.capitalize() for word in table_name.split("_"))
    attrs = {
        "__tablename__": table_name,
        "__table_args__": (
            UniqueConstraint("ts_code", "trade_date", "freq", "signal_name",
                             name=f"uk_{table_name}"),
            Index(f"ix_{table_name}_trade_date", "trade_date"),
            Index(f"ix_{table_name}_signal_type", "signal_type"),
            {"comment": f"MA信号 - {table_name}"},
        ),
        "id": Column(BigInteger, primary_key=True, autoincrement=True),
        "ts_code": Column(String(20), nullable=False, comment="股票/指数代码"),
        "trade_date": Column(String(10), nullable=False, comment="交易日期"),
        "freq": Column(String(10), nullable=False, comment="周期: daily/weekly/monthly/yearly"),
        # 信号信息
        "signal_type": Column(String(30), nullable=False, comment="信号大类: bias_extreme/direction_break/fake_break/support_resist/alignment/convergence/ma_cross"),
        "signal_name": Column(String(50), nullable=False, comment="具体信号名: bias20_extreme_low/ma5_cross_ma20_golden 等"),
        "direction": Column(String(10), comment="方向: buy/sell"),
        "signal_value": Column(Float, comment="信号关联数值（如乖离率百分比）"),
        "close": Column(Float, comment="信号发出时收盘价"),
        "ma_values": Column(String(200), comment="信号时刻相关均线值(JSON字符串)"),
        # 信号后收益率
        "ret_5": Column(Float, comment="信号后5日收益率(%)"),
        "ret_10": Column(Float, comment="信号后10日收益率(%)"),
        "ret_20": Column(Float, comment="信号后20日收益率(%)"),
        "ret_60": Column(Float, comment="信号后60日收益率(%)"),
        # 牛熊标注
        "trend": Column(String(10), comment="牛熊阶段: bull/bear/unknown"),
        "phase_id": Column(Integer, comment="牛熊周期编号"),
        "phase_label": Column(String(50), comment="牛熊周期标签"),
        # 时间戳
        "created_at": Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间"),
        "updated_at": Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间"),
    }
    return type(class_name, (MABase,), attrs)


# ── 数据表查找表: 按 (类型, 周期) 获取模型类 ──────────────────────

MODEL_MAP = {}

# 指数 MA — 4张表
for _freq in ("daily", "weekly", "monthly", "yearly"):
    _table = f"index_ma_{_freq}"
    MODEL_MAP[("index", _freq)] = _make_ma_model(_table)

# 申万行业 MA — 4张表
for _freq in ("daily", "weekly", "monthly", "yearly"):
    _table = f"sw_ma_{_freq}"
    MODEL_MAP[("sw", _freq)] = _make_ma_model(_table)

# 股票 MA — 4张表（仅 bfq）
for _freq in ("daily", "weekly", "monthly", "yearly"):
    _table = f"stock_ma_{_freq}"
    MODEL_MAP[("stock", _freq)] = _make_ma_model(_table)


# ── 信号表查找表: 按类型获取模型类 ──────────────────────────────

SIGNAL_MAP = {
    "index": _make_signal_model("index_ma_signal"),
    "sw": _make_signal_model("sw_ma_signal"),
    "stock": _make_signal_model("stock_ma_signal"),
}
