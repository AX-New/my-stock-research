"""个股 MACD 信号统计分析

对每只股票在每个 freq×adj 组合下:
1. 从 stock_research 读取已计算的 MACD 数据
2. 检测所有信号类型 + DIF 极值
3. 计算信号后续收益率和胜率
4. 汇总写入 stock_macd_signal_stats 表

结果表: 每行 = (ts_code, freq, adj)，最多 5300×12 行
信号计数: 10种（金叉/死叉各2 + DIF零轴穿越2 + 背离2 + DIF极值2）
关键表现: 8组信号的平均收益+胜率 + 零轴均值回归统计

用法:
  python research/macd/scripts/analyze_stock_macd.py --codes "300750.SZ,600406.SH"
  python research/macd/scripts/analyze_stock_macd.py  # 全市场
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, DateTime,
    Index, UniqueConstraint, func, text,
)

from app.logger import get_logger
from database import ResearchBase, write_engine
from signal_detector import (
    detect_all_signals, FREQ_ORDER,
    _find_local_peaks, _find_local_troughs,
)
from db_utils import batch_upsert

log = get_logger("research.analyze_stock_macd")

FREQS = ("daily", "weekly", "monthly", "yearly")
ADJS = ("bfq", "qfq", "hfq")

# 各周期的"主"评估窗口（K线根数）
# daily T+20≈1月, weekly T+4≈1月, monthly T+3≈1季, yearly T+1≈1年
MAIN_HORIZON = {
    "daily": 20,
    "weekly": 4,
    "monthly": 3,
    "yearly": 1,
}


# ── 结果表模型 ──────────────────────────────────────────────────

class StockMacdSignalStats(ResearchBase):
    __tablename__ = "stock_macd_signal_stats"
    __table_args__ = (
        UniqueConstraint("ts_code", "freq", "adj", name="uk_smss"),
        Index("ix_smss_ts_code", "ts_code"),
        {"comment": "个股MACD信号统计（每只股票×周期×复权一行）"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    freq = Column(String(10), nullable=False, comment="K线周期: daily/weekly/monthly/yearly")
    adj = Column(String(5), nullable=False, comment="复权类型: bfq/qfq/hfq")

    # 基础信息
    kline_count = Column(Integer, comment="K线总数")
    date_start = Column(String(10), comment="数据起始日期")
    date_end = Column(String(10), comment="数据结束日期")

    # 信号计数（10种）
    gc_cnt = Column(Integer, default=0, comment="金叉(零轴下)次数")
    zgc_cnt = Column(Integer, default=0, comment="金叉(零轴上)次数")
    dc_cnt = Column(Integer, default=0, comment="死叉(零轴上)次数")
    zdc_cnt = Column(Integer, default=0, comment="死叉(零轴下)次数")
    dif_up_cnt = Column(Integer, default=0, comment="DIF上穿零轴次数")
    dif_down_cnt = Column(Integer, default=0, comment="DIF下穿零轴次数")
    top_div_cnt = Column(Integer, default=0, comment="顶背离次数")
    bot_div_cnt = Column(Integer, default=0, comment="底背离次数")
    dif_peak_cnt = Column(Integer, default=0, comment="DIF局部极大值次数")
    dif_trough_cnt = Column(Integer, default=0, comment="DIF局部极小值次数")

    # 关键信号表现（主窗口: daily T+20, weekly T+4, monthly T+3, yearly T+1）
    gc_avg_ret = Column(Float, comment="金叉(合并)平均收益率(%)")
    gc_win_rate = Column(Float, comment="金叉(合并)胜率(%) - 后续上涨为胜")
    dc_avg_ret = Column(Float, comment="死叉(合并)平均收益率(%)")
    dc_win_rate = Column(Float, comment="死叉(合并)胜率(%) - 后续下跌为胜")
    peak_avg_ret = Column(Float, comment="DIF极大值后平均收益率(%)")
    peak_down_rate = Column(Float, comment="DIF极大值后下跌率(%)")
    trough_avg_ret = Column(Float, comment="DIF极小值后平均收益率(%)")
    trough_up_rate = Column(Float, comment="DIF极小值后上涨率(%)")

    # 背离信号表现
    top_div_avg_ret = Column(Float, comment="顶背离平均收益率(%)")
    top_div_win_rate = Column(Float, comment="顶背离胜率(%) - 后续下跌为胜")
    bot_div_avg_ret = Column(Float, comment="底背离平均收益率(%)")
    bot_div_win_rate = Column(Float, comment="底背离胜率(%) - 后续上涨为胜")

    # DIF零轴穿越表现
    dif_up_avg_ret = Column(Float, comment="DIF上穿零轴平均收益率(%)")
    dif_up_win_rate = Column(Float, comment="DIF上穿零轴胜率(%) - 后续上涨为胜")
    dif_down_avg_ret = Column(Float, comment="DIF下穿零轴平均收益率(%)")
    dif_down_win_rate = Column(Float, comment="DIF下穿零轴胜率(%) - 后续下跌为胜")

    # 零轴均值回归
    zero_high_cnt = Column(Integer, comment="DIF>Q75的K线数")
    zero_high_avg_ret = Column(Float, comment="DIF>Q75时主窗口平均收益率(%)")
    zero_low_cnt = Column(Integer, comment="DIF<Q25的K线数")
    zero_low_avg_ret = Column(Float, comment="DIF<Q25时主窗口平均收益率(%)")

    created_at = Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间")


# ── 数据加载 ──────────────────────────────────────────────────

def load_stock_macd(ts_code: str, freq: str, adj: str, conn) -> pd.DataFrame:
    """从 stock_research 读取已计算的个股 MACD 数据"""
    table = f"stock_macd_{freq}_{adj}"
    sql = text(
        f"SELECT trade_date, close, vol, pct_chg, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


# ── 分析核心 ──────────────────────────────────────────────────

def _calc_return(df: pd.DataFrame, idx: int, horizon: int) -> float | None:
    """计算第 idx 根K线后 horizon 根的收益率(%)"""
    target = idx + horizon
    if target >= len(df):
        return None
    base = float(df.iloc[idx]["close"])
    future = float(df.iloc[target]["close"])
    if base == 0:
        return None
    return round((future - base) / base * 100, 2)


def _signal_perf(df: pd.DataFrame, signals_df: pd.DataFrame,
                 signal_types: list[str], horizon: int,
                 is_buy: bool) -> tuple[float | None, float | None]:
    """计算指定信号类型的平均收益和胜率"""
    if signals_df.empty:
        return None, None
    subset = signals_df[signals_df["signal"].isin(signal_types)]
    if subset.empty:
        return None, None

    rets = []
    for _, row in subset.iterrows():
        r = _calc_return(df, int(row["idx"]), horizon)
        if r is not None:
            rets.append(r)
    if not rets:
        return None, None

    avg_ret = round(np.mean(rets), 2)
    if is_buy:
        win_rate = round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1)
    else:
        win_rate = round(sum(1 for r in rets if r < 0) / len(rets) * 100, 1)
    return avg_ret, win_rate


def _extreme_perf(df: pd.DataFrame, indices: list[int],
                  horizon: int, expect_down: bool) -> tuple[float | None, float | None]:
    """计算 DIF 极值后的平均收益和方向率"""
    rets = []
    for idx in indices:
        r = _calc_return(df, idx, horizon)
        if r is not None:
            rets.append(r)
    if not rets:
        return None, None

    avg_ret = round(np.mean(rets), 2)
    if expect_down:
        rate = round(sum(1 for r in rets if r < 0) / len(rets) * 100, 1)
    else:
        rate = round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1)
    return avg_ret, rate


def analyze_one(ts_code: str, freq: str, adj: str, conn) -> dict | None:
    """分析单只股票单个 freq×adj 组合，返回一行统计数据"""
    df = load_stock_macd(ts_code, freq, adj, conn)
    if df.empty or len(df) < 30:
        return None

    horizon = MAIN_HORIZON[freq]
    order = FREQ_ORDER.get(freq, 20)

    # 1. 检测标准信号（金叉/死叉/零轴穿越/背离）
    signals_df = detect_all_signals(df, freq=freq)
    sig_counts = signals_df["signal"].value_counts().to_dict() if not signals_df.empty else {}

    # 2. 检测 DIF 局部极值
    dif_values = df["dif"].values
    peaks = _find_local_peaks(dif_values, order)
    troughs = _find_local_troughs(dif_values, order)

    # 3. 计算关键信号表现（8组）
    gc_avg, gc_win = _signal_perf(
        df, signals_df, ["golden_cross", "zero_golden_cross"], horizon, is_buy=True
    )
    dc_avg, dc_win = _signal_perf(
        df, signals_df, ["death_cross", "zero_death_cross"], horizon, is_buy=False
    )
    peak_avg, peak_down = _extreme_perf(df, peaks, horizon, expect_down=True)
    trough_avg, trough_up = _extreme_perf(df, troughs, horizon, expect_down=False)
    # 背离信号
    tdiv_avg, tdiv_win = _signal_perf(
        df, signals_df, ["top_divergence"], horizon, is_buy=False
    )
    bdiv_avg, bdiv_win = _signal_perf(
        df, signals_df, ["bottom_divergence"], horizon, is_buy=True
    )
    # DIF零轴穿越
    dup_avg, dup_win = _signal_perf(
        df, signals_df, ["dif_cross_zero_up"], horizon, is_buy=True
    )
    ddn_avg, ddn_win = _signal_perf(
        df, signals_df, ["dif_cross_zero_down"], horizon, is_buy=False
    )

    # 4. 零轴均值回归（DIF分位数区间的未来收益）
    valid_dif = dif_values[~np.isnan(dif_values)]
    zero_high_cnt, zero_high_avg = 0, None
    zero_low_cnt, zero_low_avg = 0, None
    if len(valid_dif) > 10:
        q75 = np.percentile(valid_dif, 75)
        q25 = np.percentile(valid_dif, 25)
        # DIF > Q75 的K线，计算主窗口后收益
        high_rets = []
        low_rets = []
        for idx in range(len(df) - horizon):
            dv = dif_values[idx]
            if np.isnan(dv):
                continue
            r = _calc_return(df, idx, horizon)
            if r is None:
                continue
            if dv > q75:
                high_rets.append(r)
            elif dv < q25:
                low_rets.append(r)
        zero_high_cnt = len(high_rets)
        zero_high_avg = round(np.mean(high_rets), 2) if high_rets else None
        zero_low_cnt = len(low_rets)
        zero_low_avg = round(np.mean(low_rets), 2) if low_rets else None

    return {
        "ts_code": ts_code,
        "freq": freq,
        "adj": adj,
        "kline_count": len(df),
        "date_start": str(df["trade_date"].iloc[0]),
        "date_end": str(df["trade_date"].iloc[-1]),
        # 信号计数
        "gc_cnt": sig_counts.get("golden_cross", 0),
        "zgc_cnt": sig_counts.get("zero_golden_cross", 0),
        "dc_cnt": sig_counts.get("death_cross", 0),
        "zdc_cnt": sig_counts.get("zero_death_cross", 0),
        "dif_up_cnt": sig_counts.get("dif_cross_zero_up", 0),
        "dif_down_cnt": sig_counts.get("dif_cross_zero_down", 0),
        "top_div_cnt": sig_counts.get("top_divergence", 0),
        "bot_div_cnt": sig_counts.get("bottom_divergence", 0),
        "dif_peak_cnt": len(peaks),
        "dif_trough_cnt": len(troughs),
        # 原有4组表现
        "gc_avg_ret": gc_avg,
        "gc_win_rate": gc_win,
        "dc_avg_ret": dc_avg,
        "dc_win_rate": dc_win,
        "peak_avg_ret": peak_avg,
        "peak_down_rate": peak_down,
        "trough_avg_ret": trough_avg,
        "trough_up_rate": trough_up,
        # 背离信号表现
        "top_div_avg_ret": tdiv_avg,
        "top_div_win_rate": tdiv_win,
        "bot_div_avg_ret": bdiv_avg,
        "bot_div_win_rate": bdiv_win,
        # DIF零轴穿越表现
        "dif_up_avg_ret": dup_avg,
        "dif_up_win_rate": dup_win,
        "dif_down_avg_ret": ddn_avg,
        "dif_down_win_rate": ddn_win,
        # 零轴均值回归
        "zero_high_cnt": zero_high_cnt,
        "zero_high_avg_ret": zero_high_avg,
        "zero_low_cnt": zero_low_cnt,
        "zero_low_avg_ret": zero_low_avg,
    }


# ── 批量分析 ──────────────────────────────────────────────────

def analyze_stocks(ts_codes: list[str], freqs: tuple = FREQS, adjs: tuple = ADJS):
    """批量分析所有股票的 MACD 信号统计"""
    total = len(ts_codes)
    combos = len(freqs) * len(adjs)
    log.info(f"[analyze] 开始 | 股票: {total} | 组合: {combos}/只 | 总计: {total * combos}")

    start = time.time()
    results = []
    processed = 0

    with write_engine.connect() as conn:
        for i, ts_code in enumerate(ts_codes, 1):
            for freq in freqs:
                for adj in adjs:
                    try:
                        row = analyze_one(ts_code, freq, adj, conn)
                        if row:
                            results.append(row)
                            processed += 1
                    except Exception as e:
                        log.error(f"[analyze] 失败 | {ts_code}/{freq}/{adj} | {e}")

            # 每10只股票打印进度 + 写入数据库
            if i % 10 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[analyze] 进度: {i}/{total} | 产出: {processed}行 | 耗时: {elapsed}s")
                if results:
                    batch_upsert(StockMacdSignalStats, results,
                                 unique_keys=["ts_code", "freq", "adj"])
                    results = []

    # 剩余写入
    if results:
        batch_upsert(StockMacdSignalStats, results, unique_keys=["ts_code", "freq", "adj"])

    elapsed = round(time.time() - start, 1)
    log.info(f"[analyze] 完成 | 产出: {processed}行 | 总耗时: {elapsed}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股 MACD 信号统计分析")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔")
    parser.add_argument("--freq", default=None, help="指定周期，逗号分隔 (如: daily,weekly)")
    parser.add_argument("--adj", default=None, help="指定复权，逗号分隔 (如: bfq,qfq)")
    args = parser.parse_args()

    # 建表
    ResearchBase.metadata.create_all(bind=write_engine)

    if args.codes:
        codes = args.codes.split(",")
    else:
        from kline_loader import get_all_stock_codes
        codes = get_all_stock_codes()

    freqs = tuple(args.freq.split(",")) if args.freq else FREQS
    adjs = tuple(args.adj.split(",")) if args.adj else ADJS

    analyze_stocks(codes, freqs=freqs, adjs=adjs)
