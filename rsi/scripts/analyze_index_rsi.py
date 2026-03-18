"""Layer 1-2: 指数 RSI 信号全维度分析

分析内容:
1. RSI 分布特征（A股RSI的实际分布，验证70/30阈值是否合适）
2. 各周期 RSI 信号统计
3. 信号后续收益分析（平均收益 + 胜率）
4. 牛熊阶段 × 信号类型交叉分析（核心）
5. 各频率对比
6. 自动生成核心结论

用法:
  python rsi/research/analyze_index_rsi.py                            # 默认上证指数
  python rsi/research/analyze_index_rsi.py --ts_code 399001.SZ --name 深证成指
  python rsi/research/analyze_index_rsi.py --ts_code 399006.SZ --name 创业板指
  python rsi/research/analyze_index_rsi.py --save-signals             # 同时将信号写入数据库
"""
import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import write_engine, init_rsi_tables
from bull_bear_phases import get_phase, tag_trend
from signal_detector_rsi import detect_all_signals
from models import SIGNAL_MAP
from db_utils import batch_upsert
from app.logger import get_logger

log = get_logger(__name__)

# ── 默认值 ────────────────────────────────────────────────────
TS_CODE = "000001.SH"
INDEX_NAME = "上证指数"
FREQS = ["daily", "weekly", "monthly"]  # 年线数据点太少，不做信号统计

# ── 各周期的后续收益计算窗口 ─────────────────────────────────
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

FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

# ── 信号中文名映射 ──────────────────────────────────────────
SIGNAL_NAMES = {
    # extreme — 极端值
    "rsi6_overbought": "RSI6超买(>70)", "rsi6_oversold": "RSI6超卖(<30)",
    "rsi12_overbought": "RSI12超买(>70)", "rsi12_oversold": "RSI12超卖(<30)",
    "rsi14_overbought": "RSI14超买(>70)", "rsi14_oversold": "RSI14超卖(<30)",
    "rsi24_overbought": "RSI24超买(>70)", "rsi24_oversold": "RSI24超卖(<30)",
    "rsi14_strong_overbought": "RSI14强超买(>80)", "rsi14_strong_oversold": "RSI14强超卖(<20)",
    "rsi14_adaptive_high": "RSI14自适应高(>Q90)", "rsi14_adaptive_low": "RSI14自适应低(<Q10)",
    # divergence — 背离
    "rsi14_bull_divergence": "RSI14底背离", "rsi14_bear_divergence": "RSI14顶背离",
    # failure_swing — 失败摆动
    "rsi14_bull_failure_swing": "RSI14多头失败摆动(W底)", "rsi14_bear_failure_swing": "RSI14空头失败摆动(M头)",
    # centerline — 中轴穿越
    "rsi14_cross_above_50": "RSI14上穿50", "rsi14_cross_below_50": "RSI14下穿50",
}

SIGNAL_TYPE_NAMES = {
    "extreme": "极端值",
    "divergence": "背离",
    "failure_swing": "失败摆动",
    "centerline": "中轴穿越",
}

# buy 方向的信号集合
BUY_SIGNALS = {
    "rsi6_oversold", "rsi12_oversold", "rsi14_oversold", "rsi24_oversold",
    "rsi14_strong_oversold", "rsi14_adaptive_low",
    "rsi14_bull_divergence", "rsi14_bull_failure_swing",
    "rsi14_cross_above_50",
}


# ── 数据加载 ──────────────────────────────────────────────────

def load_rsi_data(freq: str, ts_code: str = None) -> pd.DataFrame:
    """从 stock_rsi 库加载指数 RSI 数据"""
    ts_code = ts_code or TS_CODE
    table = f"index_rsi_{freq}"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, "
        f"rsi_6, rsi_12, rsi_14, rsi_24 "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


# ── 信号分析 ──────────────────────────────────────────────────

def analyze_signals(df: pd.DataFrame, freq: str, ts_code: str) -> list[dict]:
    """对指定周期的数据检测信号 + 标注牛熊 + 计算后续收益"""
    signals = detect_all_signals(df, freq=freq)
    if not signals:
        return []

    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])

    enriched = []
    for sig in signals:
        # 标注牛熊阶段
        sig["trend"] = tag_trend(sig["trade_date"])
        phase = get_phase(sig["trade_date"])
        sig["phase_id"] = phase["id"] if phase else None
        sig["phase_label"] = phase["label"] if phase else None

        # 计算信号后各窗口收益率
        db_ret_cols = ["ret_5", "ret_10", "ret_20", "ret_60"]
        idx = sig["idx"]
        base_close = df.iloc[idx]["close"]
        for i, h in enumerate(horizons):
            target = idx + h
            col_name = db_ret_cols[i] if i < len(db_ret_cols) else f"ret_{h}"
            if target < len(df):
                future_close = df.iloc[target]["close"]
                sig[col_name] = round((future_close - base_close) / base_close * 100, 2)
            else:
                sig[col_name] = None
            if col_name != f"ret_{h}":
                sig[f"ret_{h}"] = sig[col_name]

        sig["freq"] = freq
        sig["ts_code"] = ts_code
        enriched.append(sig)

    return enriched


# ── 信号写入数据库 ────────────────────────────────────────────

def save_signals(signals: list[dict], source_type: str = "index"):
    """将信号写入信号表"""
    if not signals:
        return

    init_rsi_tables()
    model = SIGNAL_MAP[source_type]

    write_cols = [
        "ts_code", "trade_date", "freq", "signal_type", "signal_name",
        "direction", "signal_value", "close", "rsi_values",
        "ret_5", "ret_10", "ret_20", "ret_60",
        "trend", "phase_id", "phase_label",
    ]
    records = [{k: sig.get(k) for k in write_cols} for sig in signals]
    batch_upsert(model, records, unique_keys=["ts_code", "trade_date", "freq", "signal_name"])
    log.info("写入信号 %d 条 → %s", len(records), model.__tablename__)


# ── 统计计算 ──────────────────────────────────────────────────

def _is_buy_signal(signal_name: str) -> bool:
    return signal_name in BUY_SIGNALS


def calc_signal_stats(signals: list[dict], horizons: list[int]) -> list[dict]:
    """按 signal_name 统计: 数量、各窗口平均收益、胜率"""
    groups = defaultdict(list)
    for sig in signals:
        groups[sig["signal_name"]].append(sig)

    rows = []
    for sig_name in sorted(groups.keys()):
        group = groups[sig_name]
        is_buy = _is_buy_signal(sig_name)
        sig_type = group[0]["signal_type"]

        row = {
            "signal_name": sig_name,
            "signal_name_cn": SIGNAL_NAMES.get(sig_name, sig_name),
            "signal_type": sig_type,
            "signal_type_cn": SIGNAL_TYPE_NAMES.get(sig_type, sig_type),
            "direction": "buy" if is_buy else "sell",
            "count": len(group),
        }

        for h in horizons:
            col = f"ret_{h}"
            valid = [s[col] for s in group if s.get(col) is not None]
            if valid:
                row[f"avg_{h}"] = round(np.mean(valid), 2)
                if is_buy:
                    row[f"win_{h}"] = round(sum(1 for v in valid if v > 0) / len(valid) * 100, 1)
                else:
                    row[f"win_{h}"] = round(sum(1 for v in valid if v < 0) / len(valid) * 100, 1)
            else:
                row[f"avg_{h}"] = None
                row[f"win_{h}"] = None
        rows.append(row)

    return rows


def calc_trend_signal_matrix(signals: list[dict], horizons: list[int]) -> list[dict]:
    """牛熊 × 信号类型交叉统计 (三行: 全量/牛市/熊市)"""
    groups = defaultdict(list)
    for sig in signals:
        groups[sig["signal_name"]].append(sig)

    main_h = horizons[-1]
    col = f"ret_{main_h}"
    rows = []

    for sig_name in sorted(groups.keys()):
        group = groups[sig_name]
        is_buy = _is_buy_signal(sig_name)
        sig_type = group[0]["signal_type"]

        subsets = {
            "全量": group,
            "牛市": [s for s in group if s.get("trend") == "bull"],
            "熊市": [s for s in group if s.get("trend") == "bear"],
        }

        for trend_label, subset in subsets.items():
            if not subset:
                continue

            valid = [s[col] for s in subset if s.get(col) is not None]

            row = {
                "trend": trend_label,
                "signal_name": sig_name,
                "signal_name_cn": SIGNAL_NAMES.get(sig_name, sig_name),
                "signal_type": sig_type,
                "signal_type_cn": SIGNAL_TYPE_NAMES.get(sig_type, sig_type),
                "direction": "buy" if is_buy else "sell",
                "count": len(subset),
            }

            if valid:
                row["avg_ret"] = round(np.mean(valid), 2)
                if is_buy:
                    row["win_rate"] = round(sum(1 for v in valid if v > 0) / len(valid) * 100, 1)
                else:
                    row["win_rate"] = round(sum(1 for v in valid if v < 0) / len(valid) * 100, 1)
                row["max_ret"] = round(max(valid), 2)
                row["min_ret"] = round(min(valid), 2)
            else:
                row["avg_ret"] = None
                row["win_rate"] = None
                row["max_ret"] = None
                row["min_ret"] = None

            rows.append(row)

    return rows


# ── RSI 分布统计 ──────────────────────────────────────────────

def calc_rsi_distribution(df: pd.DataFrame) -> dict:
    """计算 RSI14 的分布特征: 均值、中位数、各区间占比

    用于验证 A 股 RSI 分布是否偏斜，70/30 阈值是否合理。
    """
    rsi14 = df["rsi_14"].dropna()
    if rsi14.empty:
        return {}

    return {
        "count": len(rsi14),
        "mean": round(rsi14.mean(), 2),
        "median": round(rsi14.median(), 2),
        "std": round(rsi14.std(), 2),
        "min": round(rsi14.min(), 2),
        "max": round(rsi14.max(), 2),
        "q10": round(rsi14.quantile(0.1), 2),
        "q25": round(rsi14.quantile(0.25), 2),
        "q75": round(rsi14.quantile(0.75), 2),
        "q90": round(rsi14.quantile(0.9), 2),
        # 各区间占比
        "pct_below_20": round((rsi14 < 20).sum() / len(rsi14) * 100, 2),
        "pct_below_30": round((rsi14 < 30).sum() / len(rsi14) * 100, 2),
        "pct_30_50": round(((rsi14 >= 30) & (rsi14 < 50)).sum() / len(rsi14) * 100, 2),
        "pct_50_70": round(((rsi14 >= 50) & (rsi14 < 70)).sum() / len(rsi14) * 100, 2),
        "pct_above_70": round((rsi14 >= 70).sum() / len(rsi14) * 100, 2),
        "pct_above_80": round((rsi14 >= 80).sum() / len(rsi14) * 100, 2),
    }


# ── 报告生成 ──────────────────────────────────────────────────

def _fmt_pct(val, show_sign=True):
    if val is None:
        return "-"
    if show_sign:
        return f"{val:+.2f}%"
    return f"{val:.1f}%"


def _auto_conclusions(all_results: dict) -> list[str]:
    """基于统计数据自动生成核心结论"""
    conclusions = []

    for freq in FREQS:
        if freq not in all_results or not all_results[freq]["signals"]:
            continue

        freq_cn = FREQ_NAMES[freq]
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]

        matrix = all_results[freq]["trend_matrix"]

        # 找牛熊分化最大的信号
        sig_diffs = {}
        for row in matrix:
            name = row["signal_name_cn"]
            trend = row["trend"]
            avg = row.get("avg_ret")
            if avg is not None:
                sig_diffs.setdefault(name, {})[trend] = avg

        max_diff = 0
        max_diff_name = None
        for name, trends in sig_diffs.items():
            if "牛市" in trends and "熊市" in trends:
                diff = abs(trends["牛市"] - trends["熊市"])
                if diff > max_diff:
                    max_diff = diff
                    max_diff_name = name

        if max_diff_name and max_diff > 3:
            bull_val = sig_diffs[max_diff_name].get("牛市", 0)
            bear_val = sig_diffs[max_diff_name].get("熊市", 0)
            conclusions.append(
                f"- **{freq_cn}牛熊分化最大**: {max_diff_name}，"
                f"牛市{h_label}平均{bull_val:+.2f}% vs 熊市{bear_val:+.2f}%，"
                f"差异{max_diff:.1f}个百分点"
            )

        # 找胜率最高的 buy 信号
        stats = all_results[freq]["stats"]
        best_buy = None
        best_buy_win = 0
        for row in stats:
            if row["direction"] == "buy":
                win = row.get(f"win_{main_h}")
                if win is not None and win > best_buy_win and row["count"] >= 3:
                    best_buy_win = win
                    best_buy = row

        if best_buy and best_buy_win > 50:
            conclusions.append(
                f"- **{freq_cn}最佳买入信号**: {best_buy['signal_name_cn']}，"
                f"{h_label}胜率{best_buy_win:.1f}%，"
                f"平均收益{best_buy.get(f'avg_{main_h}', 0):+.2f}%，"
                f"样本{best_buy['count']}次"
            )

        # 找胜率最高的 sell 信号
        best_sell = None
        best_sell_win = 0
        for row in stats:
            if row["direction"] == "sell":
                win = row.get(f"win_{main_h}")
                if win is not None and win > best_sell_win and row["count"] >= 3:
                    best_sell_win = win
                    best_sell = row

        if best_sell and best_sell_win > 50:
            conclusions.append(
                f"- **{freq_cn}最佳卖出信号**: {best_sell['signal_name_cn']}，"
                f"{h_label}胜率{best_sell_win:.1f}%，"
                f"平均收益{best_sell.get(f'avg_{main_h}', 0):+.2f}%，"
                f"样本{best_sell['count']}次"
            )

    return conclusions


def generate_report(all_results: dict, output_path: str, ts_code: str = None, index_name: str = None):
    """生成 Markdown 分析报告"""
    ts_code = ts_code or TS_CODE
    index_name = index_name or INDEX_NAME
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append(f"# {index_name} RSI 信号分析报告")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append(f"> 分析标的: {ts_code} ({index_name})")
    lines.append("> 牛熊阶段基准: 上证指数")
    lines.append("> RSI周期: 6/12/14/24 | 信号类别: 极端值 / 背离 / 失败摆动 / 中轴穿越")
    lines.append("")

    # ── 一、RSI 分布特征 ──────────────────────────────────────
    lines.append("## 一、RSI(14) 分布特征")
    lines.append("")
    lines.append("> 验证 A 股 RSI 分布是否偏斜，经典 70/30 阈值是否合适")
    lines.append("")

    for freq in FREQS:
        if freq not in all_results:
            continue
        dist = all_results[freq].get("rsi_dist", {})
        if not dist:
            continue

        lines.append(f"### {FREQ_NAMES[freq]}")
        lines.append("")
        lines.append(f"- 数据量: {dist['count']} 条 | 均值: {dist['mean']} | 中位数: {dist['median']} | 标准差: {dist['std']}")
        lines.append(f"- 范围: [{dist['min']}, {dist['max']}] | Q10: {dist['q10']} | Q90: {dist['q90']}")
        lines.append("")
        lines.append("| 区间 | 占比 | 说明 |")
        lines.append("|------|------|------|")
        lines.append(f"| RSI < 20 | {dist['pct_below_20']}% | 强超卖区 |")
        lines.append(f"| RSI < 30 | {dist['pct_below_30']}% | 超卖区 |")
        lines.append(f"| 30 ≤ RSI < 50 | {dist['pct_30_50']}% | 偏空区 |")
        lines.append(f"| 50 ≤ RSI < 70 | {dist['pct_50_70']}% | 偏多区 |")
        lines.append(f"| RSI ≥ 70 | {dist['pct_above_70']}% | 超买区 |")
        lines.append(f"| RSI ≥ 80 | {dist['pct_above_80']}% | 强超买区 |")
        lines.append("")

    # ── 二、数据概览 ──────────────────────────────────────────
    lines.append("## 二、数据概览")
    lines.append("")
    lines.append("| 周期 | K线数量 | 起始日期 | 结束日期 | 信号总数 |")
    lines.append("|------|--------|---------|---------|---------|")
    for freq in FREQS:
        if freq not in all_results:
            continue
        r = all_results[freq]
        df = r["data"]
        sig_count = len(r["signals"])
        lines.append(
            f"| {FREQ_NAMES[freq]} | {len(df)} | {df['trade_date'].iloc[0]} | "
            f"{df['trade_date'].iloc[-1]} | {sig_count} |"
        )
    lines.append("")

    # 各周期各信号类型数量分布
    lines.append("### 各周期信号数量分布")
    lines.append("")

    all_sig_names = set()
    for freq in FREQS:
        if freq in all_results:
            for sig in all_results[freq]["signals"]:
                all_sig_names.add(sig["signal_name"])
    sig_names_sorted = sorted(all_sig_names)

    header = "| 信号类型 | " + " | ".join(FREQ_NAMES[f] for f in FREQS if f in all_results) + " |"
    sep = "|---------|" + "|".join("------" for f in FREQS if f in all_results) + "|"
    lines.append(header)
    lines.append(sep)
    for sn in sig_names_sorted:
        cn = SIGNAL_NAMES.get(sn, sn)
        counts = []
        for freq in FREQS:
            if freq not in all_results:
                continue
            c = sum(1 for s in all_results[freq]["signals"] if s["signal_name"] == sn)
            counts.append(str(c))
        lines.append(f"| {cn} | " + " | ".join(counts) + " |")
    lines.append("")

    # ── 三、信号后续收益统计 ──────────────────────────────────
    lines.append("## 三、信号后续收益统计")
    lines.append("")
    lines.append("> 胜率定义: buy信号后续上涨为胜; sell信号后续下跌为胜")
    lines.append("")

    for freq in FREQS:
        if freq not in all_results or not all_results[freq]["signals"]:
            continue
        r = all_results[freq]
        horizons = RETURN_HORIZONS[freq]
        h_labels = HORIZON_LABELS[freq]

        lines.append(f"### {FREQ_NAMES[freq]}信号")
        lines.append("")

        stats = r["stats"]

        type_groups = defaultdict(list)
        for row in stats:
            type_groups[row["signal_type"]].append(row)

        h_cols = []
        for h in horizons:
            label = h_labels[h]
            h_cols.append(f"{label}均值")
            h_cols.append(f"{label}胜率")
        header = "| 信号 | 方向 | 数量 | " + " | ".join(h_cols) + " |"
        sep = "|------|------|------|" + "|".join("------" for _ in h_cols) + "|"

        for type_name in ["extreme", "divergence", "failure_swing", "centerline"]:
            if type_name not in type_groups:
                continue
            type_cn = SIGNAL_TYPE_NAMES[type_name]
            lines.append(f"**{type_cn}**")
            lines.append("")
            lines.append(header)
            lines.append(sep)

            for row in type_groups[type_name]:
                dir_cn = "买" if row["direction"] == "buy" else "卖"
                vals = [row["signal_name_cn"], dir_cn, str(row["count"])]
                for h in horizons:
                    vals.append(_fmt_pct(row.get(f"avg_{h}")))
                    vals.append(_fmt_pct(row.get(f"win_{h}"), show_sign=False))
                lines.append("| " + " | ".join(vals) + " |")
            lines.append("")

    # ── 四、牛熊 × 信号交叉分析 ─────────────────────────────
    lines.append("## 四、牛熊阶段 × 信号类型交叉分析")
    lines.append("")
    lines.append("> **最重要的章节** — 每个信号必须看全量/牛市/熊市三行")
    lines.append("")

    for freq in FREQS:
        if freq not in all_results or not all_results[freq]["signals"]:
            continue
        r = all_results[freq]
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]

        lines.append(f"### {FREQ_NAMES[freq]} (评估窗口: {h_label})")
        lines.append("")

        matrix = r["trend_matrix"]

        type_groups = defaultdict(list)
        for row in matrix:
            type_groups[row["signal_type"]].append(row)

        for type_name in ["extreme", "divergence", "failure_swing", "centerline"]:
            if type_name not in type_groups:
                continue
            type_cn = SIGNAL_TYPE_NAMES[type_name]
            lines.append(f"**{type_cn}**")
            lines.append("")
            lines.append("| 阶段 | 信号 | 方向 | 数量 | 平均收益 | 胜率 | 最大收益 | 最大亏损 |")
            lines.append("|------|------|------|------|---------|------|---------|---------|")

            sig_order = []
            seen = set()
            for row in type_groups[type_name]:
                if row["signal_name"] not in seen:
                    sig_order.append(row["signal_name"])
                    seen.add(row["signal_name"])

            trend_order = {"全量": 0, "牛市": 1, "熊市": 2}
            for sn in sorted(sig_order):
                rows_for_sig = [r for r in type_groups[type_name] if r["signal_name"] == sn]
                rows_for_sig.sort(key=lambda x: trend_order.get(x["trend"], 9))
                for row in rows_for_sig:
                    dir_cn = "买" if row["direction"] == "buy" else "卖"
                    lines.append(
                        f"| {row['trend']} | {row['signal_name_cn']} | {dir_cn} | "
                        f"{row['count']} | {_fmt_pct(row.get('avg_ret'))} | "
                        f"{_fmt_pct(row.get('win_rate'), show_sign=False)} | "
                        f"{_fmt_pct(row.get('max_ret'))} | {_fmt_pct(row.get('min_ret'))} |"
                    )
            lines.append("")

    # ── 五、各频率对比 ───────────────────────────────────────
    lines.append("## 五、各频率对比")
    lines.append("")

    freq_compare = defaultdict(dict)
    for freq in FREQS:
        if freq not in all_results or not all_results[freq]["stats"]:
            continue
        main_h = RETURN_HORIZONS[freq][-1]
        for row in all_results[freq]["stats"]:
            sn = row["signal_name"]
            freq_compare[sn][freq] = {
                "avg": row.get(f"avg_{main_h}"),
                "win": row.get(f"win_{main_h}"),
                "count": row["count"],
                "direction": row["direction"],
                "signal_name_cn": row["signal_name_cn"],
            }

    multi_freq_sigs = {sn: data for sn, data in freq_compare.items() if len(data) >= 2}

    if multi_freq_sigs:
        lines.append("| 信号 | 方向 | " + " | ".join(
            f"{FREQ_NAMES[f]}(数量/均值/胜率)" for f in FREQS) + " |")
        lines.append("|------|------|" + "|".join("------" for _ in FREQS) + "|")

        for sn in sorted(multi_freq_sigs.keys()):
            data = multi_freq_sigs[sn]
            sample = next(iter(data.values()))
            cn = sample["signal_name_cn"]
            dir_cn = "买" if sample["direction"] == "buy" else "卖"

            cols = [cn, dir_cn]
            for freq in FREQS:
                if freq in data:
                    d = data[freq]
                    avg_str = _fmt_pct(d["avg"])
                    win_str = _fmt_pct(d["win"], show_sign=False)
                    cols.append(f"{d['count']} / {avg_str} / {win_str}")
                else:
                    cols.append("-")
            lines.append("| " + " | ".join(cols) + " |")
        lines.append("")

    # ── 六、核心结论 ──────────────────────────────────────────
    lines.append("## 六、核心结论")
    lines.append("")

    conclusions = _auto_conclusions(all_results)
    if conclusions:
        for c in conclusions:
            lines.append(c)
    else:
        lines.append("- 数据不足，暂无自动结论")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("> 本报告由 `analyze_index_rsi.py` 自动生成，所有结论需结合牛熊周期判读")
    lines.append("")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"报告已生成: {output_path}")


# ── 主函数 ──────────────────────────────────────────────────

def main():
    global TS_CODE, INDEX_NAME

    parser = argparse.ArgumentParser(description="指数 RSI 信号分析")
    parser.add_argument("--ts_code", default="000001.SH",
                        help="指数代码 (如 000001.SH, 399001.SZ, 399006.SZ)")
    parser.add_argument("--name", default=None,
                        help="指数名称 (如 上证指数, 深证成指, 创业板指)")
    parser.add_argument("--save-signals", action="store_true",
                        help="将信号写入数据库")
    parser.add_argument("--start-date", default=None,
                        help="增量起始日期 YYYYMMDD（仅影响信号写入，报告仍覆盖全量）")
    parser.add_argument("--end-date", default=None,
                        help="截止日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()

    TS_CODE = args.ts_code
    INDEX_NAME = args.name or {
        "000001.SH": "上证指数",
        "399001.SZ": "深证成指",
        "399006.SZ": "创业板指",
    }.get(TS_CODE, TS_CODE)

    start_time = time.time()
    log.info("=" * 60)
    log.info("RSI Signal Analysis")
    log.info("Index: %s (%s)", TS_CODE, INDEX_NAME)
    log.info("Start: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    all_results = {}

    for freq in FREQS:
        log.info("--- %s ---", FREQ_NAMES[freq])
        df = load_rsi_data(freq, TS_CODE)
        if df.empty:
            log.warning("  无数据，跳过（请先运行 compute_index_rsi.py --freq %s）", freq)
            continue

        log.info("  数据量: %d 条", len(df))
        log.info("  时间范围: %s ~ %s", df['trade_date'].iloc[0], df['trade_date'].iloc[-1])

        # RSI 分布统计
        rsi_dist = calc_rsi_distribution(df)

        # 检测信号并标注牛熊+计算收益
        signals = analyze_signals(df, freq, TS_CODE)
        log.info("  信号数量: %d", len(signals))

        if signals:
            type_counts = defaultdict(int)
            for sig in signals:
                type_counts[sig["signal_type"]] += 1
            for sig_type, count in sorted(type_counts.items()):
                log.info("    %s: %d", SIGNAL_TYPE_NAMES.get(sig_type, sig_type), count)

        horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
        stats = calc_signal_stats(signals, horizons) if signals else []
        trend_matrix = calc_trend_signal_matrix(signals, horizons) if signals else []

        all_results[freq] = {
            "data": df,
            "signals": signals,
            "stats": stats,
            "trend_matrix": trend_matrix,
            "rsi_dist": rsi_dist,
        }

    log.info("")

    # 写入信号（支持增量: 只写入指定日期范围内的信号）
    if args.save_signals:
        log.info("--- 写入信号到数据库 ---")
        for freq in FREQS:
            if freq in all_results and all_results[freq]["signals"]:
                sigs = all_results[freq]["signals"]
                if args.start_date:
                    sigs = [s for s in sigs if s["trade_date"] >= args.start_date]
                if args.end_date:
                    sigs = [s for s in sigs if s["trade_date"] <= args.end_date]
                if sigs:
                    save_signals(sigs, source_type="index")
        log.info("")

    # 生成报告
    report_map = {
        "000001.SH": "01-index-rsi-layer1-shanghai.md",
        "399001.SZ": "02-index-rsi-layer2-shenzhen.md",
        "399006.SZ": "02-index-rsi-layer2-chinext.md",
    }
    code_prefix = TS_CODE.split(".")[0]
    report_name = report_map.get(TS_CODE, f"index-rsi-{code_prefix}.md")
    output_path = os.path.join(
        os.path.dirname(__file__), '..', 'report', report_name
    )
    output_path = os.path.abspath(output_path)
    generate_report(all_results, output_path, TS_CODE, INDEX_NAME)

    elapsed = round(time.time() - start_time, 1)
    log.info("")
    log.info("=" * 60)
    log.info("分析完成 | 总耗时: %.1fs", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
