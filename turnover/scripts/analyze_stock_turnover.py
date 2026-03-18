"""Layer 4: 全市场个股换手率信号统计分析

对约5500只A股个股:
1. 从 my_stock.daily_basic 读取换手率数据（无需compute, 无需复权）
2. 信号检测（lite模式, 全向量化）
3. 统计信号后续收益率和胜率
4. 汇总写入 stock_turnover_signal_stats

特点:
- 不写信号明细表（省百万行 I/O），只写统计汇总
- 100只/批读取，单SQL
- 全向量化信号检测
- 无复权维度，比RSI L4快10倍

用法:
  python turnover/research/analyze_stock_turnover.py                     # 全市场
  python turnover/research/analyze_stock_turnover.py --codes "300750.SZ" # 指定股票
  python turnover/research/analyze_stock_turnover.py --freq daily        # 指定频率
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import (
    text, Column, BigInteger, Integer, String, Float, DateTime,
    Index as SAIndex, func,
)

from database import read_engine, write_engine, TurnoverBase
from signal_detector_turnover import detect_all_signals, SIGNAL_NAMES_CN
from db_utils import batch_upsert
from app.logger import get_logger

log = get_logger("research.analyze_stock_turnover")

# ── 常量 ──────────────────────────────────────────────────────────
FREQS = ("daily", "weekly", "monthly")

# 各周期的主评估窗口（K线根数）
MAIN_HORIZON = {"daily": 20, "weekly": 4, "monthly": 3}

# 信号检测滚动窗口
SIGNAL_WINDOW = {"daily": 250, "weekly": 52, "monthly": 24}

FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

# 信号名映射: (短名, 完整名, 是否买入方向)
# 买入方向: 后续涨为胜; 卖出方向: 后续跌为胜
SIGNAL_NAME_MAP = [
    ("exh", "extreme_high", False),       # 超高换手 — sell
    ("exl", "extreme_low", True),         # 超低换手 — buy
    ("puvd", "price_up_vol_down", False), # 价涨量缩 — sell
    ("pdvu", "price_down_vol_up", True),  # 价跌量增 — buy
    ("znh", "zone_high", False),          # 高换手区 — sell
    ("znl", "zone_low", True),            # 低换手区 — buy
    ("suh", "sustained_high", False),     # 连续放量 — sell
    ("sul", "sustained_low", True),       # 连续缩量 — buy
    ("srg", "surge", False),              # 暴增 — 个股级为卖出信号(跌为胜)
    ("plg", "plunge", False),             # 骤降 — 个股级为卖出信号(跌为胜)
    ("mxu", "ma_cross_up", True),         # MA上穿 — buy
    ("mxd", "ma_cross_down", False),      # MA下穿 — sell
]

# ── 数据模型 ──────────────────────────────────────────────────────

class StockTurnoverSignalStats(TurnoverBase):
    """个股换手率信号统计汇总表（L4）"""
    __tablename__ = "stock_turnover_signal_stats"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts_code = Column(String(20), nullable=False, comment="股票代码")
    freq = Column(String(10), nullable=False, comment="周期: daily/weekly/monthly")
    kline_count = Column(Integer, comment="K线总数")
    date_start = Column(String(10), comment="数据起始日期")
    date_end = Column(String(10), comment="数据截止日期")
    avg_turnover = Column(Float, comment="平均日换手率(%)")
    avg_total_mv = Column(Float, comment="平均总市值(万元)")

    # 12种信号 × 3列(cnt, avg_ret, win_rate) = 36列
    exh_cnt = Column(Integer, default=0, comment="超高换手次数")
    exh_avg_ret = Column(Float, comment="超高换手平均收益(%)")
    exh_win_rate = Column(Float, comment="超高换手胜率(%)")

    exl_cnt = Column(Integer, default=0, comment="超低换手次数")
    exl_avg_ret = Column(Float, comment="超低换手平均收益(%)")
    exl_win_rate = Column(Float, comment="超低换手胜率(%)")

    puvd_cnt = Column(Integer, default=0, comment="价涨量缩次数")
    puvd_avg_ret = Column(Float, comment="价涨量缩平均收益(%)")
    puvd_win_rate = Column(Float, comment="价涨量缩胜率(%)")

    pdvu_cnt = Column(Integer, default=0, comment="价跌量增次数")
    pdvu_avg_ret = Column(Float, comment="价跌量增平均收益(%)")
    pdvu_win_rate = Column(Float, comment="价跌量增胜率(%)")

    znh_cnt = Column(Integer, default=0, comment="高换手区次数")
    znh_avg_ret = Column(Float, comment="高换手区平均收益(%)")
    znh_win_rate = Column(Float, comment="高换手区胜率(%)")

    znl_cnt = Column(Integer, default=0, comment="低换手区次数")
    znl_avg_ret = Column(Float, comment="低换手区平均收益(%)")
    znl_win_rate = Column(Float, comment="低换手区胜率(%)")

    suh_cnt = Column(Integer, default=0, comment="连续放量次数")
    suh_avg_ret = Column(Float, comment="连续放量平均收益(%)")
    suh_win_rate = Column(Float, comment="连续放量胜率(%)")

    sul_cnt = Column(Integer, default=0, comment="连续缩量次数")
    sul_avg_ret = Column(Float, comment="连续缩量平均收益(%)")
    sul_win_rate = Column(Float, comment="连续缩量胜率(%)")

    srg_cnt = Column(Integer, default=0, comment="换手率暴增次数")
    srg_avg_ret = Column(Float, comment="换手率暴增平均收益(%)")
    srg_win_rate = Column(Float, comment="换手率暴增胜率(%)")

    plg_cnt = Column(Integer, default=0, comment="换手率骤降次数")
    plg_avg_ret = Column(Float, comment="换手率骤降平均收益(%)")
    plg_win_rate = Column(Float, comment="换手率骤降胜率(%)")

    mxu_cnt = Column(Integer, default=0, comment="MA5上穿MA20次数")
    mxu_avg_ret = Column(Float, comment="MA5上穿MA20平均收益(%)")
    mxu_win_rate = Column(Float, comment="MA5上穿MA20胜率(%)")

    mxd_cnt = Column(Integer, default=0, comment="MA5下穿MA20次数")
    mxd_avg_ret = Column(Float, comment="MA5下穿MA20平均收益(%)")
    mxd_win_rate = Column(Float, comment="MA5下穿MA20胜率(%)")

    created_at = Column(DateTime, server_default=func.now(), nullable=False, comment="创建时间")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False, comment="更新时间")

    __table_args__ = (
        SAIndex("uk_stock_turnover_stats", "ts_code", "freq", unique=True),
        SAIndex("idx_stk_turnover_ts", "ts_code"),
    )


STATS_UNIQUE_KEYS = ["ts_code", "freq"]


# ── 数据加载 ──────────────────────────────────────────────────────

def load_stock_codes() -> list[str]:
    """从 stock_basic 获取全部上市A股代码"""
    sql = text("SELECT ts_code FROM stock_basic WHERE list_status='L' ORDER BY ts_code")
    with read_engine.connect() as conn:
        result = conn.execute(sql).fetchall()
    codes = [r[0] for r in result]
    log.info(f"加载上市A股: {len(codes)} 只")
    return codes


def load_daily_basic_batch(codes: list, conn) -> pd.DataFrame:
    """批量加载个股日线数据（100只/批, 单SQL）

    从 daily_basic 读取: ts_code, trade_date, close, turnover_rate_f, total_mv
    数据起始: 2013.10.30（与指数研究一致）
    """
    if not codes:
        return pd.DataFrame()

    placeholders = ",".join([f":c{i}" for i in range(len(codes))])
    sql = text(
        f"SELECT ts_code, trade_date, close, turnover_rate_f, total_mv "
        f"FROM daily_basic "
        f"WHERE ts_code IN ({placeholders}) AND trade_date >= '20131030' "
        f"ORDER BY ts_code, trade_date"
    )
    params = {f"c{i}": code for i, code in enumerate(codes)}
    return pd.read_sql(sql, conn, params=params)


# ── 聚合 ──────────────────────────────────────────────────────────

def aggregate_to_freq(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """将日线聚合为周线/月线

    换手率用均值法（与日线量级一致，阈值可复用）
    """
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


# ── 单只分析 ──────────────────────────────────────────────────────

def analyze_one(ts_code: str, df_stock: pd.DataFrame, freq: str,
                signal_after: str = None) -> dict | None:
    """分析单只股票单个频率, 返回一行统计数据

    流程:
    1. 聚合为目标频率
    2. detect_all_signals(lite=True) 检测12种信号
    3. 按 signal_name 分组计算后续收益率和胜率
    4. 返回统计字典（对应 StockTurnoverSignalStats 列）

    signal_after: 只统计该日期之后的信号（数据仍全量加载，保证检测窗口完整）
    """
    # 聚合
    df = aggregate_to_freq(df_stock, freq)

    # 数据不足则跳过（需要窗口 + 足够信号样本）
    min_rows = SIGNAL_WINDOW[freq] + 50
    if len(df) < min_rows:
        return None

    horizon = MAIN_HORIZON[freq]
    close_arr = df["close"].values

    # 信号检测（lite模式: 返回元组列表）
    window = SIGNAL_WINDOW[freq]
    signals = detect_all_signals(
        df, col="turnover_rate_f", price_col="close",
        extreme_window=window, zone_window=window,
        lite=True,
    )

    # 日期过滤：只保留指定日期之后的信号（检测仍用全量数据保证窗口正确）
    if signal_after:
        dates = df["trade_date"].values
        signals = [s for s in signals if str(dates[s[0]]) >= signal_after]

    # 按 signal_name 分组
    sig_by_name = {}
    for sig in signals:
        # lite模式: (idx, signal_type, signal_name, direction)
        idx, _, sig_name, _ = sig
        if sig_name not in sig_by_name:
            sig_by_name[sig_name] = []
        sig_by_name[sig_name].append(idx)

    # 构建结果行
    result = {
        "ts_code": ts_code,
        "freq": freq,
        "kline_count": len(df),
        "date_start": str(df["trade_date"].iloc[0]),
        "date_end": str(df["trade_date"].iloc[-1]),
        "avg_turnover": round(float(df_stock["turnover_rate_f"].mean()), 4)
            if not df_stock["turnover_rate_f"].isna().all() else None,
        "avg_total_mv": round(float(df_stock["total_mv"].mean()), 2)
            if "total_mv" in df_stock.columns and not df_stock["total_mv"].isna().all() else None,
    }

    # 对每种信号计算 cnt / avg_ret / win_rate
    for short, full_name, is_buy in SIGNAL_NAME_MAP:
        indices = sig_by_name.get(full_name, [])
        cnt = len(indices)
        result[f"{short}_cnt"] = cnt

        if cnt == 0:
            result[f"{short}_avg_ret"] = None
            result[f"{short}_win_rate"] = None
            continue

        # 计算后续收益率
        rets = []
        for idx in indices:
            target = idx + horizon
            if target < len(close_arr):
                base = float(close_arr[idx])
                future = float(close_arr[target])
                if base > 0:
                    rets.append(round((future / base - 1) * 100, 2))

        if not rets:
            result[f"{short}_avg_ret"] = None
            result[f"{short}_win_rate"] = None
            continue

        result[f"{short}_avg_ret"] = round(np.mean(rets), 2)
        if is_buy:
            # 买入方向: 后续涨为胜
            result[f"{short}_win_rate"] = round(
                sum(1 for r in rets if r > 0) / len(rets) * 100, 1)
        else:
            # 卖出方向: 后续跌为胜
            result[f"{short}_win_rate"] = round(
                sum(1 for r in rets if r < 0) / len(rets) * 100, 1)

    return result


# ── 批量分析 ──────────────────────────────────────────────────────

def analyze_stocks(ts_codes: list, freqs: tuple = FREQS, batch_size: int = 100,
                   signal_after: str = None) -> list | None:
    """批量分析所有股票的换手率信号统计

    signal_after: 只统计该日期之后的信号。设置时不写DB（避免覆盖全量统计），返回结果列表。
    """
    total = len(ts_codes)
    freq_count = len(freqs)
    mode = f"signal_after={signal_after}" if signal_after else "全量"
    log.info(f"[analyze] 开始 | 股票: {total} | 频率: {freq_count} | 模式: {mode}")

    start = time.time()
    results = []
    all_results = []  # signal_after 模式下保留全部结果
    processed = 0
    skipped = 0

    # 分批读取
    batches = [ts_codes[i:i + batch_size] for i in range(0, total, batch_size)]

    with read_engine.connect() as read_conn:
        for batch_idx, batch_codes in enumerate(batches):
            # 批量加载数据
            df_batch = load_daily_basic_batch(batch_codes, read_conn)
            if df_batch.empty:
                skipped += len(batch_codes)
                continue

            # 按股票分组
            grouped = {code: group.copy() for code, group in df_batch.groupby("ts_code")}

            for ts_code in batch_codes:
                df_stock = grouped.get(ts_code)
                if df_stock is None or df_stock.empty:
                    skipped += 1
                    continue

                # 去除换手率或收盘价为空的行
                df_stock = df_stock.dropna(subset=["turnover_rate_f", "close"])
                if len(df_stock) < 300:
                    skipped += 1
                    continue

                for freq in freqs:
                    try:
                        row = analyze_one(ts_code, df_stock, freq,
                                          signal_after=signal_after)
                        if row:
                            results.append(row)
                            processed += 1
                    except Exception as e:
                        log.error(f"[analyze] 失败 | {ts_code}/{freq} | {e}")

            # 每批处理
            batch_end = min((batch_idx + 1) * batch_size, total)
            elapsed = round(time.time() - start, 1)
            log.info(f"[analyze] 进度: {batch_end}/{total} | "
                     f"产出: {processed}行 | 跳过: {skipped} | 耗时: {elapsed}s")

            if signal_after:
                # 不写DB，保留在内存
                all_results.extend(results)
                results = []
            else:
                if results:
                    batch_upsert(StockTurnoverSignalStats, results, STATS_UNIQUE_KEYS)
                    results = []

    # 剩余处理
    if signal_after:
        all_results.extend(results)
    elif results:
        batch_upsert(StockTurnoverSignalStats, results, STATS_UNIQUE_KEYS)

    elapsed = round(time.time() - start, 1)
    log.info(f"[analyze] 完成 | 产出: {processed}行 | 跳过: {skipped} | 总耗时: {elapsed}s")

    if signal_after:
        return all_results





# ── 主入口 ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="个股换手率信号统计分析 (L4)")
    parser.add_argument("--codes", default=None,
                        help="股票代码，逗号分隔（默认全市场）")
    parser.add_argument("--freq", default="daily,weekly,monthly",
                        help="周期，逗号分隔（默认 daily,weekly,monthly）")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="批量读取大小（默认100）")
    parser.add_argument("--signal-after", default=None,
                        help="只统计此日期之后的信号 (YYYYMMDD)，不写DB")
    args = parser.parse_args()

    # 建表
    TurnoverBase.metadata.create_all(bind=write_engine)

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    else:
        codes = load_stock_codes()

    freqs = tuple(f.strip() for f in args.freq.split(","))
    analyze_stocks(codes, freqs=freqs, batch_size=args.batch_size,
                   signal_after=args.signal_after)


if __name__ == "__main__":
    main()
