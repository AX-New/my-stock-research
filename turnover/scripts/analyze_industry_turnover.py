"""Layer 3: 行业换手率信号分析

申万行业指数在 index_dailybasic 中无换手率数据，
从个股 daily_basic 按行业聚合（市值加权平均换手率）。

分析内容:
1. 31个申万一级行业的换手率水平对比
2. 各行业信号检测 + 后续收益统计
3. 5大类别聚合对比（周期/金融地产/大消费/科技成长/稳定制造）
4. 例外行业清单

用法:
  python turnover/research/analyze_industry_turnover.py --save-signals
  python turnover/research/analyze_industry_turnover.py --codes 801780.SI,801790.SI --save-signals
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text, Column, BigInteger, String, Float, DateTime, Index as SAIndex, func

from database import read_engine, write_engine, init_turnover_tables, TurnoverBase
from bull_bear_phases import get_phase, tag_trend
from signal_detector_turnover import (
    detect_all_signals, SIGNAL_NAMES_CN, SIGNAL_TYPE_NAMES_CN,
    BUY_SIGNALS, SELL_SIGNALS,
)
from db_utils import batch_upsert
from app.logger import get_logger

log = get_logger(__name__)

# ── 常量 ──────────────────────────────────────────────────

FREQS = ["daily", "weekly", "monthly"]

RETURN_HORIZONS = {
    "daily": [5, 10, 20, 60],
    "weekly": [2, 4, 8, 13],
    "monthly": [1, 3, 6, 12],
}

PRIMARY_HORIZON = {"daily": 20, "weekly": 4, "monthly": 3}
SIGNAL_WINDOW = {"daily": 250, "weekly": 52, "monthly": 24}
FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

# 申万5大类别分组
INDUSTRY_CATEGORIES = {
    "周期": ["801010.SI", "801030.SI", "801040.SI", "801050.SI", "801710.SI", "801950.SI", "801960.SI"],
    "金融地产": ["801780.SI", "801790.SI", "801180.SI"],
    "大消费": ["801110.SI", "801120.SI", "801130.SI", "801140.SI", "801150.SI",
               "801200.SI", "801210.SI", "801880.SI", "801980.SI"],
    "科技成长": ["801080.SI", "801730.SI", "801740.SI", "801750.SI", "801760.SI", "801770.SI"],
    "稳定制造": ["801160.SI", "801170.SI", "801720.SI", "801890.SI", "801970.SI", "801230.SI"],
}


# ── 信号表模型 ──────────────────────────────────────────────

class SwTurnoverSignal(TurnoverBase):
    """申万行业换手率信号明细表"""
    __tablename__ = "sw_turnover_signal"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="行业代码")
    trade_date = Column(String(10), nullable=False, comment="交易日期")
    freq = Column(String(10), nullable=False, comment="周期")
    signal_type = Column(String(20), nullable=False, comment="信号大类")
    signal_name = Column(String(40), nullable=False, comment="信号名")
    direction = Column(String(10), comment="方向")
    signal_value = Column(Float, comment="换手率数值")
    ret_5 = Column(Float, comment="后续收益窗口1")
    ret_10 = Column(Float, comment="后续收益窗口2")
    ret_20 = Column(Float, comment="后续收益窗口3")
    ret_60 = Column(Float, comment="后续收益窗口4")
    bull_bear = Column(String(10), comment="牛熊")
    bull_bear_sub = Column(String(30), comment="牛熊子类型")
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        SAIndex("uk_sw_signal", "ts_code", "trade_date", "freq", "signal_name", unique=True),
        SAIndex("idx_sw_type", "signal_type"),
        SAIndex("idx_sw_date", "trade_date"),
    )


SW_SIGNAL_UNIQUE_KEYS = ["ts_code", "trade_date", "freq", "signal_name"]


# ── 数据加载 ──────────────────────────────────────────────────

def load_industry_list() -> pd.DataFrame:
    """加载申万一级行业列表"""
    sql = text(
        "SELECT DISTINCT l1_code, l1_name FROM index_member_all "
        "WHERE is_new = 'Y' ORDER BY l1_code"
    )
    with read_engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df


def load_industry_members(l1_code: str) -> list:
    """加载行业成分股列表（当前成分）"""
    sql = text(
        "SELECT ts_code FROM index_member_all "
        "WHERE l1_code = :code AND is_new = 'Y'"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql, {"code": l1_code}).fetchall()
    return [r[0] for r in result]


def load_industry_turnover(members: list) -> pd.DataFrame:
    """批量加载行业成分股的日换手率和收盘价

    用 daily_basic 的 turnover_rate_f 和 total_mv（总市值，用于加权）
    收盘价用 sw_daily 的行业指数收盘价
    """
    if not members:
        return pd.DataFrame()

    # 批量读取成分股换手率 + 市值
    placeholders = ",".join([f":c{i}" for i in range(len(members))])
    sql = text(
        f"SELECT ts_code, trade_date, turnover_rate_f, total_mv "
        f"FROM daily_basic "
        f"WHERE ts_code IN ({placeholders}) AND trade_date >= '20131030' "
        f"ORDER BY trade_date"
    )
    params = {f"c{i}": code for i, code in enumerate(members)}
    with read_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    return df


def load_sw_index_close(l1_code: str) -> pd.DataFrame:
    """加载申万行业指数收盘价（用于计算后续收益）"""
    sql = text(
        "SELECT trade_date, close FROM sw_daily "
        "WHERE ts_code = :code AND trade_date >= '20131030' "
        "ORDER BY trade_date"
    )
    with read_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": l1_code})
    return df


def aggregate_industry_turnover(df_stocks: pd.DataFrame) -> pd.DataFrame:
    """将个股换手率按市值加权聚合为行业日换手率

    行业日换手率 = Σ(个股换手率 × 个股市值) / Σ(个股市值)
    """
    if df_stocks.empty:
        return pd.DataFrame()

    # 去掉缺失值
    df = df_stocks.dropna(subset=["turnover_rate_f", "total_mv"])

    # 按日期分组，市值加权平均
    def weighted_avg(group):
        weights = group["total_mv"]
        values = group["turnover_rate_f"]
        return np.average(values, weights=weights) if weights.sum() > 0 else np.nan

    result = df.groupby("trade_date").apply(weighted_avg).reset_index()
    result.columns = ["trade_date", "turnover_rate_f"]
    result = result.sort_values("trade_date").reset_index(drop=True)

    return result


# ── 分析函数（复用 analyze_index_turnover 的逻辑）──────────────

def aggregate_to_freq(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """日线聚合为周线/月线"""
    if freq == "daily":
        return df.copy()

    df = df.copy()
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    if freq == "weekly":
        df["period"] = (df["trade_date_dt"].dt.isocalendar().year.astype(str) + "W" +
                        df["trade_date_dt"].dt.isocalendar().week.astype(str).str.zfill(2))
    elif freq == "monthly":
        df["period"] = df["trade_date"].str[:6]

    agg = df.groupby("period").agg(
        trade_date=("trade_date", "last"),
        close=("close", "last"),
        turnover_rate_f=("turnover_rate_f", "mean"),
    ).reset_index(drop=True)

    return agg.sort_values("trade_date").reset_index(drop=True)


def calc_forward_returns(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """计算后续收益率"""
    df = df.copy()
    close = df["close"].values
    for h in RETURN_HORIZONS[freq]:
        ret = np.full(len(close), np.nan)
        for i in range(len(close) - h):
            ret[i] = (close[i + h] / close[i] - 1) * 100
        df[f"ret_{h}"] = ret
    return df


def analyze_single_industry(l1_code: str, l1_name: str, freqs: list, save_signals: bool):
    """分析单个行业"""
    log.info(f"  分析行业: {l1_name} ({l1_code})")
    t0 = time.time()

    # 1. 加载成分股换手率
    members = load_industry_members(l1_code)
    if not members:
        log.warning(f"    {l1_code} 无成分股，跳过")
        return {}

    df_stocks = load_industry_turnover(members)
    if df_stocks.empty:
        log.warning(f"    {l1_code} 无换手率数据，跳过")
        return {}

    # 2. 聚合为行业日换手率
    df_turnover = aggregate_industry_turnover(df_stocks)
    log.info(f"    成分股: {len(members)}只, 行业换手率: {len(df_turnover)}天")

    # 3. 加载行业指数收盘价
    df_close = load_sw_index_close(l1_code)
    if df_close.empty:
        log.warning(f"    {l1_code} 无指数收盘价，跳过")
        return {}

    # 合并换手率和收盘价
    df = pd.merge(df_turnover, df_close[["trade_date", "close"]], on="trade_date", how="inner")

    all_stats = {}
    for freq in freqs:
        freq_name = FREQ_NAMES[freq]
        df_freq = aggregate_to_freq(df, freq)
        if len(df_freq) < 30:
            continue

        df_freq = calc_forward_returns(df_freq, freq)

        # 信号检测
        window = SIGNAL_WINDOW[freq]
        signals = detect_all_signals(
            df_freq, col="turnover_rate_f", price_col="close",
            extreme_window=window, zone_window=window,
        )

        if not signals:
            continue

        # 补充后续收益和牛熊标注
        horizons = RETURN_HORIZONS[freq]
        for sig in signals:
            idx_list = df_freq.index[df_freq["trade_date"] == sig["trade_date"]].tolist()
            if not idx_list:
                continue
            idx = idx_list[0]
            for h in horizons:
                sig[f"ret_{h}"] = df_freq.at[idx, f"ret_{h}"]
            phase = get_phase(sig["trade_date"])
            sig["bull_bear"] = phase["trend"] if phase else "unknown"
            sig["bull_bear_sub"] = phase["label"] if phase else "unknown"

        # 统计
        signals_df = pd.DataFrame(signals)
        primary_h = PRIMARY_HORIZON[freq]
        ret_col = f"ret_{primary_h}"

        stats = {}
        for signal_name in signals_df["signal_name"].unique():
            sig = signals_df[signals_df["signal_name"] == signal_name]
            rets = sig[ret_col].dropna()
            if len(rets) == 0:
                continue
            direction = sig["direction"].iloc[0]
            if direction == "sell":
                win_rate = (rets < 0).mean() * 100
            else:
                win_rate = (rets > 0).mean() * 100
            stats[signal_name] = {
                "count": len(rets),
                "avg_ret": rets.mean(),
                "win_rate": win_rate,
            }

        all_stats[freq] = stats

        # 保存信号
        if save_signals:
            save_records = []
            for sig in signals:
                rec = {
                    "ts_code": l1_code,
                    "trade_date": sig["trade_date"],
                    "freq": freq,
                    "signal_type": sig["signal_type"],
                    "signal_name": sig["signal_name"],
                    "direction": sig["direction"],
                    "signal_value": sig.get("signal_value"),
                    "bull_bear": sig.get("bull_bear"),
                    "bull_bear_sub": sig.get("bull_bear_sub"),
                }
                horizons_list = RETURN_HORIZONS[freq]
                ret_fields = ["ret_5", "ret_10", "ret_20", "ret_60"]
                for i, h in enumerate(horizons_list):
                    if i < len(ret_fields):
                        rec[ret_fields[i]] = sig.get(f"ret_{h}")
                save_records.append(rec)
            batch_upsert(SwTurnoverSignal, save_records, SW_SIGNAL_UNIQUE_KEYS)

    elapsed = time.time() - t0
    log.info(f"    完成，耗时 {elapsed:.1f}s")
    return all_stats


# ── 主流程 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="行业换手率信号分析")
    parser.add_argument("--codes", help="行业代码，逗号分隔，默认全部31个")
    parser.add_argument("--freq", default="daily,weekly,monthly",
                        help="周期，默认 daily,weekly,monthly")
    parser.add_argument("--start-date", help="增量起始日期 YYYYMMDD")
    parser.add_argument("--end-date", help="截止日期 YYYYMMDD")
    parser.add_argument("--save-signals", action="store_true", help="信号写入DB")
    parser.add_argument("--all", action="store_true", help="全部周期")
    args = parser.parse_args()

    # 创建表
    TurnoverBase.metadata.create_all(bind=write_engine)

    freqs = [f.strip() for f in args.freq.split(",")]

    # 加载行业列表
    industry_df = load_industry_list()

    if args.codes:
        target_codes = [c.strip() for c in args.codes.split(",")]
        industry_df = industry_df[industry_df["l1_code"].isin(target_codes)]

    # 构建行业→类别映射
    code_to_category = {}
    for cat, codes in INDUSTRY_CATEGORIES.items():
        for code in codes:
            code_to_category[code] = cat

    log.info(f"行业换手率分析: {len(industry_df)}个行业, 频率={freqs}")
    t_start = time.time()

    all_results = {}
    for _, row in industry_df.iterrows():
        l1_code = row["l1_code"]
        l1_name = row["l1_name"]
        category = code_to_category.get(l1_code, "其他")

        stats = analyze_single_industry(l1_code, l1_name, freqs, args.save_signals)
        all_results[l1_code] = {
            "name": l1_name,
            "category": category,
            "stats": stats,
        }

    elapsed = time.time() - t_start
    log.info(f"全部行业分析完成，总耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()
