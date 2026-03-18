"""Layer 3.5: 换手率信号时间稳定性验证

将上证指数换手率信号按 4 年分段，检查:
1. 跨时代稳定性：信号方向是否各时段一致（胜率始终>50%或始终<50%）
2. 幅度衰减：收益幅度是否在缩小（市场效率提升→alpha缩小）
3. 同环境稳定性：同一牛熊环境内信号是否一致（近1年 vs 历史同环境）

数据范围: 2013.10~2026.03（只有约12年，比MACD/RSI的30年短）

MACD经验: 趋势指标跨时代方向稳定，幅度衰减(-8.95%→-2.64%)
RSI经验: 反转指标跨时代方向不稳定(22%)，但同环境内100%稳定

用法:
  python turnover/research/analyze_time_stability_turnover.py
  python turnover/research/analyze_time_stability_turnover.py --ts_code 399001.SZ --name 深证成指
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import read_engine
from bull_bear_phases import get_phase, tag_trend
from signal_detector_turnover import (
    detect_all_signals, SIGNAL_NAMES_CN, BUY_SIGNALS, SELL_SIGNALS,
)
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

INDEX_NAMES = {
    "000001.SH": "上证指数", "399001.SZ": "深证成指", "399006.SZ": "创业板指",
}

# 按年分段（数据2013.10~2026.03，逐年分析趋势更清晰）
TIME_PERIODS = [
    ("20140101", "20141231", "2014"),
    ("20150101", "20151231", "2015"),
    ("20160101", "20161231", "2016"),
    ("20170101", "20171231", "2017"),
    ("20180101", "20181231", "2018"),
    ("20190101", "20191231", "2019"),
    ("20200101", "20201231", "2020"),
    ("20210101", "20211231", "2021"),
    ("20220101", "20221231", "2022"),
    ("20230101", "20231231", "2023"),
    ("20240101", "20241231", "2024"),
    ("20250101", "20261231", "2025"),
]

# 近1年（同环境稳定性验证）
RECENT_1Y = ("20250301", "20260312", "近1年")

# 核心信号（报告重点关注）
CORE_SIGNALS = [
    "extreme_high", "extreme_low",
    "price_up_vol_down", "price_down_vol_up",
    "zone_high", "zone_low",
    "sustained_high", "sustained_low",
    "surge", "plunge",
    "ma_cross_up", "ma_cross_down",
]


# ── 数据加载 ──────────────────────────────────────────────────

def load_index_data(ts_code: str) -> pd.DataFrame:
    """从 my_stock 加载指数日线数据"""
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
    log.info(f"加载 {ts_code}: {len(df)} 条, {df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}")
    return df


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
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        turnover_rate_f=("turnover_rate_f", "mean"),
    ).reset_index(drop=True)

    return agg.sort_values("trade_date").reset_index(drop=True)


def calc_forward_returns(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """计算后续收益"""
    df = df.copy()
    close = df["close"].values
    for h in RETURN_HORIZONS[freq]:
        ret = np.full(len(close), np.nan)
        for i in range(len(close) - h):
            ret[i] = (close[i + h] / close[i] - 1) * 100
        df[f"ret_{h}"] = ret
    return df


# ── 分析函数 ──────────────────────────────────────────────────

def detect_and_enrich(df: pd.DataFrame, freq: str):
    """在全量数据上检测信号，并补充收益和牛熊标注"""
    window = SIGNAL_WINDOW[freq]
    signals = detect_all_signals(
        df, col="turnover_rate_f", price_col="close",
        extreme_window=window, zone_window=window,
    )

    horizons = RETURN_HORIZONS[freq]
    for sig in signals:
        idx_list = df.index[df["trade_date"] == sig["trade_date"]].tolist()
        if not idx_list:
            continue
        idx = idx_list[0]
        for h in horizons:
            sig[f"ret_{h}"] = df.at[idx, f"ret_{h}"]
        phase = get_phase(sig["trade_date"])
        sig["bull_bear"] = phase["trend"] if phase else "unknown"
        sig["bull_bear_sub"] = phase["label"] if phase else "unknown"

    return signals


def stats_for_signals(signals: list, freq: str) -> dict:
    """计算信号统计（按signal_name分组）"""
    if not signals:
        return {}

    primary_h = PRIMARY_HORIZON[freq]
    ret_col = f"ret_{primary_h}"
    result = {}

    df = pd.DataFrame(signals)
    for signal_name in df["signal_name"].unique():
        sub = df[df["signal_name"] == signal_name]
        rets = sub[ret_col].dropna()
        if len(rets) == 0:
            continue

        direction = sub["direction"].iloc[0]
        if direction == "sell":
            win_rate = (rets < 0).mean() * 100
        else:
            win_rate = (rets > 0).mean() * 100

        result[signal_name] = {
            "count": len(rets),
            "avg_ret": rets.mean(),
            "win_rate": win_rate,
            "direction": direction,
        }

    return result


def analyze_by_period(all_signals: list, freq: str) -> dict:
    """将全量信号按时间分段统计"""
    period_results = {}
    for start, end, label in TIME_PERIODS:
        period_signals = [s for s in all_signals if start <= s["trade_date"] <= end]
        stats = stats_for_signals(period_signals, freq)
        period_results[label] = stats

    # 近1年
    r1y_start, r1y_end, r1y_label = RECENT_1Y
    recent_signals = [s for s in all_signals if r1y_start <= s["trade_date"] <= r1y_end]
    period_results[r1y_label] = stats_for_signals(recent_signals, freq)

    return period_results


def analyze_bull_bear_consistency(all_signals: list, freq: str) -> dict:
    """同环境稳定性：按牛熊子类型分组，验证同一环境内信号方向是否一致"""
    if not all_signals:
        return {}

    primary_h = PRIMARY_HORIZON[freq]
    ret_col = f"ret_{primary_h}"
    df = pd.DataFrame(all_signals)
    result = {}

    for signal_name in df["signal_name"].unique():
        sub = df[df["signal_name"] == signal_name]
        direction = sub["direction"].iloc[0]
        env_stats = {}

        for env in sub["bull_bear_sub"].unique():
            env_sub = sub[sub["bull_bear_sub"] == env]
            rets = env_sub[ret_col].dropna()
            if len(rets) < 3:
                continue

            if direction == "sell":
                wr = (rets < 0).mean() * 100
            else:
                wr = (rets > 0).mean() * 100

            env_stats[env] = {"count": len(rets), "win_rate": wr, "avg_ret": rets.mean()}

        result[signal_name] = env_stats

    return result


# ── 主流程 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="换手率信号时间稳定性验证 (L3.5)")
    parser.add_argument("--ts_code", default="000001.SH", help="指数代码")
    parser.add_argument("--name", default=None, help="指数名称")
    parser.add_argument("--start-date", help="起始日期")
    parser.add_argument("--end-date", help="截止日期")
    args = parser.parse_args()

    name = args.name or INDEX_NAMES.get(args.ts_code, args.ts_code)
    log.info(f"时间稳定性验证: {name} ({args.ts_code})")
    t0 = time.time()

    # 加载全量日线数据
    df_daily = load_index_data(args.ts_code)

    all_period_results = {}
    all_bb_results = {}

    for freq in FREQS:
        freq_name = FREQ_NAMES[freq]
        log.info(f"  处理 {freq_name}...")

        # 聚合
        df = aggregate_to_freq(df_daily, freq)
        df = calc_forward_returns(df, freq)
        log.info(f"    数据量: {len(df)} 条")

        # 在全量数据上检测信号（保证上下文完整）
        signals = detect_and_enrich(df, freq)
        log.info(f"    全量信号: {len(signals)} 个")

        # 按时段分组统计
        period_results = analyze_by_period(signals, freq)
        all_period_results[freq] = period_results

        # 同环境稳定性
        bb_results = analyze_bull_bear_consistency(signals, freq)
        all_bb_results[freq] = bb_results

        # 打印核心信号的各时段胜率
        primary_h = PRIMARY_HORIZON[freq]
        log.info(f"    {freq_name} 时段对比 (T+{primary_h}):")
        for signal_name in ["extreme_high", "surge", "extreme_low", "ma_cross_up"]:
            cn = SIGNAL_NAMES_CN.get(signal_name, signal_name)
            parts = []
            for start, end, label in TIME_PERIODS:
                stats = period_results.get(label, {}).get(signal_name)
                if stats:
                    parts.append(f"{label}: {stats['win_rate']:.0f}%({stats['count']})")
            if parts:
                log.info(f"      {cn}: {' | '.join(parts)}")

    elapsed = time.time() - t0
    log.info(f"时间稳定性验证完成，耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    main()
