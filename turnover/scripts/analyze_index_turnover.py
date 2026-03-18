"""Layer 1-2.5: 指数换手率信号全维度分析

分析内容:
1. 各频率换手率信号统计
2. 信号后续收益分析（平均收益 + 胜率）
3. 牛熊阶段 × 信号类型交叉分析（核心）
4. 牛熊子类型细分分析
5. 各频率对比

用法:
  # L1: 上证指数
  python turnover/research/analyze_index_turnover.py --save-signals
  # L2: 三大指数
  python turnover/research/analyze_index_turnover.py --codes 000001.SH,399001.SZ,399006.SZ --save-signals
  # L2.5: 宽基
  python turnover/research/analyze_index_turnover.py --codes 000016.SH,000300.SH,000905.SH --save-signals
"""
import argparse
import os
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import read_engine, write_engine, init_turnover_tables
from bull_bear_phases import get_phase, tag_trend, SH_PHASES
from signal_detector_turnover import (
    detect_all_signals, SIGNAL_NAMES_CN, SIGNAL_TYPE_NAMES_CN,
    BUY_SIGNALS, SELL_SIGNALS, NEUTRAL_SIGNALS,
)
from models import IndexTurnoverSignal, SIGNAL_UNIQUE_KEYS
from db_utils import batch_upsert
from app.logger import get_logger

log = get_logger(__name__)

# ── 常量 ──────────────────────────────────────────────────
FREQS = ["daily", "weekly", "monthly"]

# 各周期的后续收益计算窗口
RETURN_HORIZONS = {
    "daily": [5, 10, 20, 60],
    "weekly": [2, 4, 8, 13],
    "monthly": [1, 3, 6, 12],
}

HORIZON_LABELS = {
    "daily": {5: "T+5(1周)", 10: "T+10(2周)", 20: "T+20(1月)", 60: "T+60(3月)"},
    "weekly": {2: "T+2(2周)", 4: "T+4(1月)", 8: "T+8(2月)", 13: "T+13(1季)"},
    "monthly": {1: "T+1(1月)", 3: "T+3(1季)", 6: "T+6(半年)", 12: "T+12(1年)"},
}

# 主窗口（用于核心结论判定）
PRIMARY_HORIZON = {"daily": 20, "weekly": 4, "monthly": 3}

# 各频率的信号检测滚动窗口（日线250=1年, 周线52=1年, 月线24=2年）
SIGNAL_WINDOW = {"daily": 250, "weekly": 52, "monthly": 24}

FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

INDEX_NAMES = {
    "000001.SH": "上证指数", "399001.SZ": "深证成指", "399006.SZ": "创业板指",
    "000016.SH": "上证50", "000300.SH": "沪深300", "000905.SH": "中证500",
}


# ── 数据加载 ──────────────────────────────────────────────────

def load_index_data(ts_code: str) -> pd.DataFrame:
    """从 my_stock 加载指数日线数据（收盘价 + 换手率）"""
    # 合并 index_daily 和 index_dailybasic
    sql = text(
        "SELECT a.trade_date, a.open, a.high, a.low, a.close, a.pct_chg, "
        "       b.turnover_rate, b.turnover_rate_f "
        "FROM index_daily a "
        "JOIN index_dailybasic b ON a.ts_code = b.ts_code AND a.trade_date = b.trade_date "
        "WHERE a.ts_code = :code "
        "ORDER BY a.trade_date"
    )
    with read_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})
    log.info(f"加载 {ts_code} 数据: {len(df)} 条, "
             f"{df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}")
    return df


def aggregate_to_freq(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """将日线数据聚合为周线/月线

    换手率用均值法（与日线量级一致，阈值可复用）
    """
    if freq == "daily":
        return df.copy()

    df = df.copy()
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    if freq == "weekly":
        # 按 ISO 周聚合
        df["period"] = (df["trade_date_dt"].dt.isocalendar().year.astype(str) + "W" +
                        df["trade_date_dt"].dt.isocalendar().week.astype(str).str.zfill(2))
    elif freq == "monthly":
        df["period"] = df["trade_date"].str[:6]

    agg = df.groupby("period").agg(
        trade_date=("trade_date", "last"),      # 取最后一个交易日
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        pct_chg=("pct_chg", lambda x: ((1 + x/100).prod() - 1) * 100),  # 复合收益
        turnover_rate=("turnover_rate", "mean"),       # 均值法
        turnover_rate_f=("turnover_rate_f", "mean"),   # 均值法
    ).reset_index(drop=True)

    agg = agg.sort_values("trade_date").reset_index(drop=True)
    return agg


# ── 后续收益计算 ──────────────────────────────────────────

def calc_forward_returns(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """计算各窗口的后续收益率(%)"""
    df = df.copy()
    close = df["close"].values
    horizons = RETURN_HORIZONS[freq]

    for h in horizons:
        ret = np.full(len(close), np.nan)
        for i in range(len(close) - h):
            ret[i] = (close[i + h] / close[i] - 1) * 100
        df[f"ret_{h}"] = ret

    return df


# ── 信号统计 ──────────────────────────────────────────────

def compute_signal_stats(signals_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """按信号名统计胜率和收益率

    三行统计: 全量 / 牛市 / 熊市
    """
    primary_h = PRIMARY_HORIZON[freq]
    ret_col = f"ret_{primary_h}"
    results = []

    for signal_name in signals_df["signal_name"].unique():
        sig = signals_df[signals_df["signal_name"] == signal_name]
        direction = sig["direction"].iloc[0]

        for scope, subset in [("全量", sig),
                              ("牛市", sig[sig["bull_bear"] == "bull"]),
                              ("熊市", sig[sig["bull_bear"] == "bear"])]:
            if len(subset) == 0:
                continue

            rets = subset[ret_col].dropna()
            if len(rets) == 0:
                continue

            # 胜率定义: buy信号→后续涨为胜, sell信号→后续跌为胜, neutral→后续涨为胜
            if direction == "sell":
                win_rate = (rets < 0).mean() * 100
            else:
                win_rate = (rets > 0).mean() * 100

            results.append({
                "signal_name": signal_name,
                "signal_type": subset["signal_type"].iloc[0],
                "direction": direction,
                "scope": scope,
                "count": len(rets),
                "avg_ret": rets.mean(),
                "win_rate": win_rate,
                "median_ret": rets.median(),
            })

    return pd.DataFrame(results)


def compute_bull_bear_sub_stats(signals_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """按牛熊子类型 × 信号名 交叉统计"""
    primary_h = PRIMARY_HORIZON[freq]
    ret_col = f"ret_{primary_h}"
    results = []

    for signal_name in signals_df["signal_name"].unique():
        sig = signals_df[signals_df["signal_name"] == signal_name]
        direction = sig["direction"].iloc[0]

        for sub_label in sig["bull_bear_sub"].unique():
            subset = sig[sig["bull_bear_sub"] == sub_label]
            rets = subset[ret_col].dropna()
            if len(rets) == 0:
                continue

            if direction == "sell":
                win_rate = (rets < 0).mean() * 100
            else:
                win_rate = (rets > 0).mean() * 100

            results.append({
                "signal_name": signal_name,
                "bull_bear_sub": sub_label,
                "count": len(rets),
                "avg_ret": rets.mean(),
                "win_rate": win_rate,
            })

    return pd.DataFrame(results)


# ── 主流程 ──────────────────────────────────────────────────

def analyze_single_index(ts_code: str, freqs: list, save_signals: bool):
    """分析单个指数的换手率信号"""
    name = INDEX_NAMES.get(ts_code, ts_code)
    log.info(f"{'='*60}")
    log.info(f"开始分析: {name} ({ts_code})")
    t0 = time.time()

    # 加载日线数据
    df_daily = load_index_data(ts_code)
    if df_daily.empty:
        log.warning(f"{ts_code} 无数据，跳过")
        return {}, {}

    all_stats = {}
    all_sub_stats = {}

    for freq in freqs:
        freq_name = FREQ_NAMES[freq]
        log.info(f"  处理 {freq_name}...")

        # 聚合
        df = aggregate_to_freq(df_daily, freq)
        log.info(f"    数据量: {len(df)} 条")

        # 计算后续收益
        df = calc_forward_returns(df, freq)

        # 检测信号（滚动窗口按频率调整）
        window = SIGNAL_WINDOW[freq]
        signals = detect_all_signals(
            df, col="turnover_rate_f", price_col="close",
            extreme_window=window, zone_window=window,
        )
        log.info(f"    检测到 {len(signals)} 个信号")

        if not signals:
            continue

        # 构造信号 DataFrame
        signals_df = pd.DataFrame(signals)
        # 合并后续收益和牛熊标注
        horizons = RETURN_HORIZONS[freq]
        for sig in signals:
            idx = df.index[df["trade_date"] == sig["trade_date"]].tolist()
            if not idx:
                continue
            idx = idx[0]
            for h in horizons:
                sig[f"ret_{h}"] = df.at[idx, f"ret_{h}"]
            # 牛熊标注
            phase = get_phase(sig["trade_date"])
            sig["bull_bear"] = phase["trend"] if phase else "unknown"
            sig["bull_bear_sub"] = phase["label"] if phase else "unknown"

        signals_df = pd.DataFrame(signals)

        # 统计
        stats = compute_signal_stats(signals_df, freq)
        sub_stats = compute_bull_bear_sub_stats(signals_df, freq)
        all_stats[freq] = stats
        all_sub_stats[freq] = sub_stats

        # 打印核心统计
        if not stats.empty:
            full_stats = stats[stats["scope"] == "全量"]
            log.info(f"    {freq_name}信号统计 (主窗口T+{PRIMARY_HORIZON[freq]}):")
            for _, row in full_stats.iterrows():
                cn = SIGNAL_NAMES_CN.get(row["signal_name"], row["signal_name"])
                log.info(f"      {cn}: n={row['count']}, 收益={row['avg_ret']:.2f}%, "
                         f"胜率={row['win_rate']:.1f}%")

        # 保存信号到数据库
        if save_signals and signals:
            save_records = []
            for sig in signals:
                rec = {
                    "ts_code": ts_code,
                    "trade_date": sig["trade_date"],
                    "freq": freq,
                    "signal_type": sig["signal_type"],
                    "signal_name": sig["signal_name"],
                    "direction": sig["direction"],
                    "signal_value": sig.get("signal_value"),
                    "bull_bear": sig.get("bull_bear"),
                    "bull_bear_sub": sig.get("bull_bear_sub"),
                }
                # 将不同频率的收益窗口映射到统一的 ret_5/10/20/60 字段
                horizons_list = RETURN_HORIZONS[freq]
                ret_fields = ["ret_5", "ret_10", "ret_20", "ret_60"]
                for i, h in enumerate(horizons_list):
                    if i < len(ret_fields):
                        rec[ret_fields[i]] = sig.get(f"ret_{h}")
                save_records.append(rec)

            batch_upsert(IndexTurnoverSignal, save_records, SIGNAL_UNIQUE_KEYS)
            log.info(f"    已保存 {len(save_records)} 条信号到数据库")

    elapsed = time.time() - t0
    log.info(f"  {name} 分析完成，耗时 {elapsed:.1f}s")

    return all_stats, all_sub_stats


def main():
    parser = argparse.ArgumentParser(description="指数换手率信号分析")
    parser.add_argument("--codes", default="000001.SH",
                        help="指数代码，逗号分隔，默认 000001.SH")
    parser.add_argument("--freq", default="daily,weekly,monthly",
                        help="周期，逗号分隔，默认 daily,weekly,monthly")
    parser.add_argument("--start-date", help="增量起始日期 YYYYMMDD")
    parser.add_argument("--end-date", help="截止日期 YYYYMMDD")
    parser.add_argument("--save-signals", action="store_true",
                        help="信号写入数据库")
    parser.add_argument("--all", action="store_true",
                        help="全部周期")
    args = parser.parse_args()

    # 初始化表
    init_turnover_tables()

    codes = [c.strip() for c in args.codes.split(",")]
    freqs = [f.strip() for f in args.freq.split(",")]

    log.info(f"换手率信号分析: 指数={codes}, 频率={freqs}, 保存信号={args.save_signals}")

    for ts_code in codes:
        all_stats, all_sub_stats = analyze_single_index(ts_code, freqs, args.save_signals)

    log.info("全部分析完成")


if __name__ == "__main__":
    main()
