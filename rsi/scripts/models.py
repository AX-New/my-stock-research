"""RSI 指标表模型定义（工厂函数生成）

表结构:
- 指数: 4周期(日/周/月/年) = 4张
- 申万行业: 4周期(日/周/月/年) = 4张
- 股票(无复权): 4周期 = 4张（L1-3用）
- 股票(含复权): 4周期×3复权 = 12张（L4用，RSI基于close.diff()，复权影响结果）
- 信号: 3张(指数/申万/股票)
- 统计: 1张(个股信号汇总)

总计: 28张表
"""
from sqlalchemy import Column, BigInteger, String, Float, Integer, DateTime, Index, UniqueConstraint, func

from database import RSIBase


def _make_rsi_model(table_name: str) -> type:
    """生成 RSI 数据表模型类

    存储 K线行情 + 4个周期的 RSI 值（6/12/14/24）
    """
    class_name = "".join(word.capitalize() for word in table_name.split("_"))
    attrs = {
        "__tablename__": table_name,
        "__table_args__": (
            UniqueConstraint("ts_code", "trade_date", name=f"uk_{table_name}"),
            Index(f"ix_{table_name}_trade_date", "trade_date"),
            {"comment": f"RSI指标 - {table_name}"},
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
        # 4个周期的 RSI
        "rsi_6": Column(Float, comment="RSI(6)"),
        "rsi_12": Column(Float, comment="RSI(12)"),
        "rsi_14": Column(Float, comment="RSI(14)"),
        "rsi_24": Column(Float, comment="RSI(24)"),
        # 时间戳
        "created_at": Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间"),
        "updated_at": Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间"),
    }
    return type(class_name, (RSIBase,), attrs)


def _make_signal_model(table_name: str) -> type:
    """生成 RSI 信号表模型类

    存储各类 RSI 信号（极端值、背离、失败摆动、中轴穿越等），
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
            {"comment": f"RSI信号 - {table_name}"},
        ),
        "id": Column(BigInteger, primary_key=True, autoincrement=True),
        "ts_code": Column(String(20), nullable=False, comment="股票/指数代码"),
        "trade_date": Column(String(10), nullable=False, comment="交易日期"),
        "freq": Column(String(10), nullable=False, comment="周期: daily/weekly/monthly/yearly"),
        # 信号信息
        "signal_type": Column(String(30), nullable=False, comment="信号大类: extreme/divergence/failure_swing/centerline"),
        "signal_name": Column(String(50), nullable=False, comment="具体信号名: rsi14_overbought/rsi14_bull_divergence 等"),
        "direction": Column(String(10), comment="方向: buy/sell"),
        "signal_value": Column(Float, comment="信号关联数值（如RSI值）"),
        "close": Column(Float, comment="信号发出时收盘价"),
        "rsi_values": Column(String(200), comment="信号时刻各周期RSI值(JSON字符串)"),
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
    return type(class_name, (RSIBase,), attrs)


# ── 数据表查找表: 按 (类型, 周期[, 复权]) 获取模型类 ────────────────

MODEL_MAP = {}

# 指数 RSI — 4张表
for _freq in ("daily", "weekly", "monthly", "yearly"):
    _table = f"index_rsi_{_freq}"
    MODEL_MAP[("index", _freq)] = _make_rsi_model(_table)

# 申万行业 RSI — 4张表
for _freq in ("daily", "weekly", "monthly", "yearly"):
    _table = f"sw_rsi_{_freq}"
    MODEL_MAP[("sw", _freq)] = _make_rsi_model(_table)

# 股票 RSI（无复权）— 4张表（L1-3 用，保持兼容）
for _freq in ("daily", "weekly", "monthly", "yearly"):
    _table = f"stock_rsi_{_freq}"
    MODEL_MAP[("stock", _freq)] = _make_rsi_model(_table)

# 股票 RSI（含复权）— 12张表（L4 个股验证用）
# RSI 基于 close.diff() 计算，不同复权方式的 close 不同，RSI 结果也不同
ADJS = ("bfq", "qfq", "hfq")
for _freq in ("daily", "weekly", "monthly", "yearly"):
    for _adj in ADJS:
        _table = f"stock_rsi_{_freq}_{_adj}"
        MODEL_MAP[("stock", _freq, _adj)] = _make_rsi_model(_table)


# ── 信号表查找表: 按类型获取模型类 ──────────────────────────────

SIGNAL_MAP = {
    "index": _make_signal_model("index_rsi_signal"),
    "sw": _make_signal_model("sw_rsi_signal"),
    "stock": _make_signal_model("stock_rsi_signal"),
}


# ── 个股信号统计表 ──────────────────────────────────────────────

# 18种RSI信号: (列名前缀, 信号检测器返回的 signal_name, 是否买入信号)
SIGNAL_NAME_MAP = [
    ("rsi6_ob",      "rsi6_overbought",           False),
    ("rsi6_os",      "rsi6_oversold",              True),
    ("rsi12_ob",     "rsi12_overbought",           False),
    ("rsi12_os",     "rsi12_oversold",              True),
    ("rsi14_ob",     "rsi14_overbought",            False),
    ("rsi14_os",     "rsi14_oversold",              True),
    ("rsi24_ob",     "rsi24_overbought",            False),
    ("rsi24_os",     "rsi24_oversold",              True),
    ("r14_str_ob",   "rsi14_strong_overbought",     False),
    ("r14_str_os",   "rsi14_strong_oversold",       True),
    ("r14_adp_hi",   "rsi14_adaptive_high",         False),
    ("r14_adp_lo",   "rsi14_adaptive_low",          True),
    ("r14_bear_div", "rsi14_bear_divergence",       False),
    ("r14_bull_div", "rsi14_bull_divergence",       True),
    ("r14_bull_fs",  "rsi14_bull_failure_swing",    True),
    ("r14_bear_fs",  "rsi14_bear_failure_swing",    False),
    ("r14_xup50",    "rsi14_cross_above_50",        True),
    ("r14_xdn50",    "rsi14_cross_below_50",        False),
]


def _make_signal_stats_model():
    """生成个股RSI信号统计表模型

    每行 = (ts_code, freq, adj)，总计 ~65,676 行
    18种信号 × (cnt + avg_ret + win_rate) = 54 指标列
    """
    attrs = {
        "__tablename__": "stock_rsi_signal_stats",
        "__table_args__": (
            UniqueConstraint("ts_code", "freq", "adj", name="uk_srss"),
            Index("ix_srss_ts_code", "ts_code"),
            {"comment": "个股RSI信号统计（每只股票×周期×复权一行）"},
        ),
        "id": Column(BigInteger, primary_key=True, autoincrement=True),
        "ts_code": Column(String(20), nullable=False, comment="股票代码"),
        "freq": Column(String(10), nullable=False, comment="K线周期"),
        "adj": Column(String(5), nullable=False, comment="复权类型"),
        # 基础信息
        "kline_count": Column(Integer, comment="K线总数"),
        "date_start": Column(String(10), comment="数据起始日期"),
        "date_end": Column(String(10), comment="数据结束日期"),
        # 时间戳
        "created_at": Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间"),
        "updated_at": Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间"),
    }

    # 动态生成 18 种信号的 cnt/avg_ret/win_rate 列
    for short, _full_name, _is_buy in SIGNAL_NAME_MAP:
        attrs[f"{short}_cnt"] = Column(Integer, default=0, comment=f"{_full_name} 次数")
        attrs[f"{short}_avg_ret"] = Column(Float, comment=f"{_full_name} 平均收益率(%)")
        attrs[f"{short}_win_rate"] = Column(Float, comment=f"{_full_name} 胜率(%)")

    return type("StockRsiSignalStats", (RSIBase,), attrs)


StockRsiSignalStats = _make_signal_stats_model()
