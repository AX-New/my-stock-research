"""Layer 1: 上证指数 MACD 全维度分析

分析内容:
1. 各周期 MACD 信号统计
2. 信号后续收益分析（平均收益 + 胜率）
3. 牛熊阶段 × 信号类型交叉分析
4. 已知顶底的 MACD 特征逆向研究
5. 生成完整分析报告

用法:
  python research/macd/scripts/analyze_index_macd.py                          # 默认上证指数
  python research/macd/scripts/analyze_index_macd.py --ts_code 399001.SZ --name 深证成指
  python research/macd/scripts/analyze_index_macd.py --ts_code 399006.SZ --name 创业板指
"""
import argparse
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import pandas as pd
import numpy as np
from sqlalchemy import text

from database import write_engine
from bull_bear_phases import SH_PHASES, SH_TOPS, SH_BOTTOMS, get_phase, tag_trend, find_nearest_date
from signal_detector import detect_all_signals, FREQ_ORDER

# 默认值，可通过命令行参数覆盖
TS_CODE = "000001.SH"
INDEX_NAME = "上证指数"
FREQS = ["daily", "weekly", "monthly"]  # 年线数据点太少，不做信号统计

# 各周期的后续收益计算窗口（单位: K线根数）
RETURN_HORIZONS = {
    "daily": [5, 10, 20, 60],       # 1周 / 2周 / 1月 / 3月
    "weekly": [2, 4, 8, 13],        # 2周 / 1月 / 2月 / 1季
    "monthly": [1, 3, 6, 12],       # 1月 / 1季 / 半年 / 1年
}

# 各周期收益窗口的可读名称
HORIZON_LABELS = {
    "daily": {5: "T+5(1周)", 10: "T+10(2周)", 20: "T+20(1月)", 60: "T+60(3月)"},
    "weekly": {2: "T+2(2周)", 4: "T+4(1月)", 8: "T+8(2月)", 13: "T+13(1季)"},
    "monthly": {1: "T+1(1月)", 3: "T+3(1季)", 6: "T+6(半年)", 12: "T+12(1年)"},
}

# 信号中文名
SIGNAL_NAMES = {
    "golden_cross": "金叉(零轴下)",
    "zero_golden_cross": "金叉(零轴上)",
    "death_cross": "死叉(零轴上)",
    "zero_death_cross": "死叉(零轴下)",
    "dif_cross_zero_up": "DIF上穿零轴",
    "dif_cross_zero_down": "DIF下穿零轴",
    "top_divergence": "顶背离",
    "bottom_divergence": "底背离",
}

FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}


# ── 数据加载 ──────────────────────────────────────────────────

def load_macd_data(freq: str, ts_code: str = None) -> pd.DataFrame:
    """从 stock_research 库加载指数 MACD 数据"""
    ts_code = ts_code or TS_CODE
    table = f"index_macd_{freq}"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


# ── 信号分析 ──────────────────────────────────────────────────

def calc_post_returns(df: pd.DataFrame, signal_idx: int, horizons: list[int]) -> dict:
    """计算信号发出后各窗口的收益率(%)"""
    base_close = df.iloc[signal_idx]["close"]
    returns = {}
    for h in horizons:
        target = signal_idx + h
        if target < len(df):
            future_close = df.iloc[target]["close"]
            returns[f"ret_{h}"] = round((future_close - base_close) / base_close * 100, 2)
        else:
            returns[f"ret_{h}"] = None
    return returns


def analyze_signals(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """对指定周期的数据检测信号 + 标注牛熊 + 计算后续收益"""
    signals_df = detect_all_signals(df, freq=freq)
    if signals_df.empty:
        return signals_df

    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])

    enriched = []
    for _, row in signals_df.iterrows():
        record = row.to_dict()
        # 标注牛熊阶段
        record["trend"] = tag_trend(row["trade_date"])
        phase = get_phase(row["trade_date"])
        record["phase_id"] = phase["id"] if phase else None
        record["phase_label"] = phase["label"] if phase else None
        # 后续收益
        idx = int(record["idx"])
        record.update(calc_post_returns(df, idx, horizons))
        enriched.append(record)

    return pd.DataFrame(enriched)


# ── 统计计算 ──────────────────────────────────────────────────

def calc_signal_stats(signals_df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    """按信号类型统计: 数量、各窗口平均收益、胜率

    胜率定义:
    - 买入信号(金叉/底背离/DIF上穿零轴): 后续上涨为胜
    - 卖出信号(死叉/顶背离/DIF下穿零轴): 后续下跌为胜
    """
    buy_signals = {"golden_cross", "zero_golden_cross", "bottom_divergence", "dif_cross_zero_up"}
    rows = []

    for sig_type, group in signals_df.groupby("signal"):
        row = {"signal": sig_type, "signal_name": SIGNAL_NAMES.get(sig_type, sig_type), "count": len(group)}
        is_buy = sig_type in buy_signals

        for h in horizons:
            col = f"ret_{h}"
            valid = group[col].dropna()
            if len(valid) > 0:
                row[f"avg_{h}"] = round(valid.mean(), 2)
                if is_buy:
                    row[f"win_{h}"] = round((valid > 0).sum() / len(valid) * 100, 1)
                else:
                    row[f"win_{h}"] = round((valid < 0).sum() / len(valid) * 100, 1)
            else:
                row[f"avg_{h}"] = None
                row[f"win_{h}"] = None
        rows.append(row)

    return pd.DataFrame(rows)


def calc_trend_signal_matrix(signals_df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    """牛熊 × 信号类型交叉统计"""
    buy_signals = {"golden_cross", "zero_golden_cross", "bottom_divergence", "dif_cross_zero_up"}
    rows = []

    for (trend, sig_type), group in signals_df.groupby(["trend", "signal"]):
        is_buy = sig_type in buy_signals
        # 取最长窗口做主要判断
        main_h = horizons[-1]
        col = f"ret_{main_h}"
        valid = group[col].dropna()

        row = {
            "trend": trend,
            "signal": sig_type,
            "signal_name": SIGNAL_NAMES.get(sig_type, sig_type),
            "count": len(group),
        }

        if len(valid) > 0:
            row["avg_ret"] = round(valid.mean(), 2)
            if is_buy:
                row["win_rate"] = round((valid > 0).sum() / len(valid) * 100, 1)
            else:
                row["win_rate"] = round((valid < 0).sum() / len(valid) * 100, 1)
            row["max_ret"] = round(valid.max(), 2)
            row["min_ret"] = round(valid.min(), 2)
        else:
            row["avg_ret"] = None
            row["win_rate"] = None
            row["max_ret"] = None
            row["min_ret"] = None
        rows.append(row)

    return pd.DataFrame(rows)


# ── 顶底特征分析 ──────────────────────────────────────────────

def analyze_tops_bottoms(df: pd.DataFrame, freq: str, signals_df: pd.DataFrame) -> dict:
    """在已知顶底附近提取 MACD 特征

    对每个已知顶/底:
    1. 找到最近的交易日
    2. 提取该日的 DIF/DEA/MACD 值
    3. 检查附近是否有背离信号
    4. 检查最近一次金叉/死叉的距离
    """
    order = FREQ_ORDER.get(freq, 20)

    def _extract_features(target_ym: str, point: float, label: str, is_top: bool) -> dict | None:
        date = find_nearest_date(df, target_ym)
        if date is None:
            return None

        idx = df[df["trade_date"] == date].index[0]
        row = df.iloc[idx]

        features = {
            "label": label,
            "target_ym": target_ym,
            "actual_date": date,
            "point": point,
            "close": float(row["close"]),
            "dif": float(row["dif"]),
            "dea": float(row["dea"]),
            "macd": float(row["macd"]),
            "dif_above_zero": row["dif"] > 0,
            "macd_positive": row["macd"] > 0,
        }

        # 检查最近的信号
        if not signals_df.empty:
            # 该日期之前（含）的信号
            prev_signals = signals_df[signals_df["trade_date"] <= date]

            # 最近一次金叉距离
            last_gc = prev_signals[prev_signals["signal"].isin(["golden_cross", "zero_golden_cross"])]
            if not last_gc.empty:
                gc_idx = int(last_gc.iloc[-1]["idx"])
                features["bars_since_last_gc"] = idx - gc_idx
            else:
                features["bars_since_last_gc"] = None

            # 最近一次死叉距离
            last_dc = prev_signals[prev_signals["signal"].isin(["death_cross", "zero_death_cross"])]
            if not last_dc.empty:
                dc_idx = int(last_dc.iloc[-1]["idx"])
                features["bars_since_last_dc"] = idx - dc_idx
            else:
                features["bars_since_last_dc"] = None

            # 附近是否有背离信号（前后 order 根K线内）
            div_type = "top_divergence" if is_top else "bottom_divergence"
            nearby_div = signals_df[
                (signals_df["signal"] == div_type) &
                (signals_df["idx"].between(idx - order * 2, idx + order))
            ]
            features["has_divergence"] = len(nearby_div) > 0
            if len(nearby_div) > 0:
                features["divergence_date"] = nearby_div.iloc[-1]["trade_date"]
        else:
            features["bars_since_last_gc"] = None
            features["bars_since_last_dc"] = None
            features["has_divergence"] = False

        return features

    top_features = []
    for top in SH_TOPS:
        f = _extract_features(top["ym"], top["point"], top["label"], is_top=True)
        if f:
            top_features.append(f)

    bottom_features = []
    for bottom in SH_BOTTOMS:
        f = _extract_features(bottom["ym"], bottom["point"], bottom["label"], is_top=False)
        if f:
            bottom_features.append(f)

    return {"tops": top_features, "bottoms": bottom_features}


def summarize_tb_features(tb_analysis: dict) -> dict:
    """汇总顶底特征的共性统计"""
    summary = {}

    # 顶部特征统计
    tops = tb_analysis["tops"]
    if tops:
        n = len(tops)
        summary["top_count"] = n
        summary["top_dif_above_zero"] = sum(1 for t in tops if t["dif_above_zero"])
        summary["top_macd_positive"] = sum(1 for t in tops if t["macd_positive"])
        summary["top_has_divergence"] = sum(1 for t in tops if t["has_divergence"])
        # 顶部时最近死叉的平均距离
        dc_distances = [t["bars_since_last_dc"] for t in tops if t["bars_since_last_dc"] is not None]
        summary["top_avg_bars_since_dc"] = round(np.mean(dc_distances), 1) if dc_distances else None

    # 底部特征统计
    bottoms = tb_analysis["bottoms"]
    if bottoms:
        n = len(bottoms)
        summary["bottom_count"] = n
        summary["bottom_dif_above_zero"] = sum(1 for b in bottoms if b["dif_above_zero"])
        summary["bottom_macd_positive"] = sum(1 for b in bottoms if b["macd_positive"])
        summary["bottom_has_divergence"] = sum(1 for b in bottoms if b["has_divergence"])
        gc_distances = [b["bars_since_last_gc"] for b in bottoms if b["bars_since_last_gc"] is not None]
        summary["bottom_avg_bars_since_gc"] = round(np.mean(gc_distances), 1) if gc_distances else None

    return summary


# ── 报告生成 ──────────────────────────────────────────────────

def generate_report(all_results: dict, output_path: str, ts_code: str = None, index_name: str = None):
    """生成 Markdown 分析报告"""
    ts_code = ts_code or TS_CODE
    index_name = index_name or INDEX_NAME
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append(f"# {index_name} MACD 信号分析报告")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append(f"> 分析标的: {ts_code} ({index_name})")
    lines.append("> 牛熊阶段基准: 上证指数（验证同一市场环境下不同指数的 MACD 表现是否一致）")
    lines.append("> 分析方法: 信号检测 + 后续收益统计 + 已知顶底逆向特征提取")
    lines.append("")

    # === 一、数据概览 ===
    lines.append("## 一、数据概览")
    lines.append("")
    lines.append("| 周期 | K线数量 | 起始日期 | 结束日期 | 信号总数 |")
    lines.append("|------|--------|---------|---------|---------|")
    for freq in FREQS:
        if freq not in all_results:
            continue
        r = all_results[freq]
        df = r["data"]
        sig_count = len(r["signals"]) if not r["signals"].empty else 0
        lines.append(f"| {FREQ_NAMES[freq]} | {len(df)} | {df['trade_date'].iloc[0]} | "
                      f"{df['trade_date'].iloc[-1]} | {sig_count} |")
    lines.append("")

    # 各周期信号数量分布
    lines.append("### 各周期信号数量分布")
    lines.append("")
    # 收集所有信号类型
    all_sig_types = set()
    for freq in FREQS:
        if freq in all_results and not all_results[freq]["signals"].empty:
            all_sig_types.update(all_results[freq]["signals"]["signal"].unique())
    sig_types_sorted = sorted(all_sig_types)

    header = "| 信号类型 | " + " | ".join(FREQ_NAMES[f] for f in FREQS) + " |"
    sep = "|---------|" + "|".join("-----" for _ in FREQS) + "|"
    lines.append(header)
    lines.append(sep)
    for st in sig_types_sorted:
        name = SIGNAL_NAMES.get(st, st)
        counts = []
        for freq in FREQS:
            if freq in all_results and not all_results[freq]["signals"].empty:
                c = (all_results[freq]["signals"]["signal"] == st).sum()
                counts.append(str(c))
            else:
                counts.append("-")
        lines.append(f"| {name} | " + " | ".join(counts) + " |")
    lines.append("")

    # === 二、信号后续收益统计 ===
    lines.append("## 二、信号后续收益统计")
    lines.append("")
    lines.append("> 胜率定义: 买入信号(金叉/底背离/DIF上穿)后续上涨为胜; 卖出信号(死叉/顶背离/DIF下穿)后续下跌为胜")
    lines.append("")

    for freq in FREQS:
        if freq not in all_results or all_results[freq]["signals"].empty:
            continue
        r = all_results[freq]
        horizons = RETURN_HORIZONS[freq]
        h_labels = HORIZON_LABELS[freq]

        lines.append(f"### {FREQ_NAMES[freq]}信号")
        lines.append("")

        stats = calc_signal_stats(r["signals"], horizons)
        # 表头
        h_cols = []
        for h in horizons:
            label = h_labels[h]
            h_cols.append(f"{label}均值")
            h_cols.append(f"{label}胜率")
        header = "| 信号 | 数量 | " + " | ".join(h_cols) + " |"
        sep = "|------|------|" + "|".join("------" for _ in h_cols) + "|"
        lines.append(header)
        lines.append(sep)

        for _, row in stats.iterrows():
            vals = [row["signal_name"], str(row["count"])]
            for h in horizons:
                avg = row.get(f"avg_{h}")
                win = row.get(f"win_{h}")
                vals.append(f"{avg:+.2f}%" if avg is not None else "-")
                vals.append(f"{win:.1f}%" if win is not None else "-")
            lines.append("| " + " | ".join(str(v) for v in vals) + " |")
        lines.append("")

    # === 三、牛熊 × 信号交叉分析 ===
    lines.append("## 三、牛熊阶段 × 信号类型交叉分析")
    lines.append("")
    lines.append("> 使用各周期最长窗口的收益率作为判断依据")
    lines.append("")

    for freq in FREQS:
        if freq not in all_results or all_results[freq]["signals"].empty:
            continue
        r = all_results[freq]
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]

        lines.append(f"### {FREQ_NAMES[freq]} (评估窗口: {h_label})")
        lines.append("")

        matrix = calc_trend_signal_matrix(r["signals"], horizons)
        lines.append("| 趋势 | 信号 | 数量 | 平均收益 | 胜率 | 最大收益 | 最大亏损 |")
        lines.append("|------|------|------|---------|------|---------|---------|")

        for _, row in matrix.sort_values(["trend", "signal"]).iterrows():
            trend_cn = "牛市" if row["trend"] == "bull" else ("熊市" if row["trend"] == "bear" else row["trend"])
            avg = f"{row['avg_ret']:+.2f}%" if row["avg_ret"] is not None else "-"
            win = f"{row['win_rate']:.1f}%" if row["win_rate"] is not None else "-"
            mx = f"{row['max_ret']:+.2f}%" if row["max_ret"] is not None else "-"
            mn = f"{row['min_ret']:+.2f}%" if row["min_ret"] is not None else "-"
            lines.append(f"| {trend_cn} | {row['signal_name']} | {row['count']} | "
                          f"{avg} | {win} | {mx} | {mn} |")
        lines.append("")

    # === 四、顶底特征分析 ===
    lines.append("## 四、已知顶底的 MACD 特征分析")
    lines.append("")
    lines.append("> 从已确认的10个顶部和10个底部出发，逆向提取各周期 MACD 状态")
    lines.append("")

    for freq in FREQS:
        if freq not in all_results or "tops_bottoms" not in all_results[freq]:
            continue
        tb = all_results[freq]["tops_bottoms"]
        lines.append(f"### {FREQ_NAMES[freq]} - 顶部特征")
        lines.append("")
        lines.append("| 顶部 | 日期 | 点位 | DIF | DEA | MACD柱 | DIF>0 | 有背离 | 距最近死叉(根) |")
        lines.append("|------|------|------|-----|-----|--------|-------|--------|---------------|")
        for t in tb["tops"]:
            dif_z = "是" if t["dif_above_zero"] else "否"
            div = "是" if t["has_divergence"] else "否"
            dc_dist = str(t["bars_since_last_dc"]) if t["bars_since_last_dc"] is not None else "-"
            lines.append(f"| {t['label']} | {t['actual_date']} | {t['close']:.0f} | "
                          f"{t['dif']:.2f} | {t['dea']:.2f} | {t['macd']:.2f} | "
                          f"{dif_z} | {div} | {dc_dist} |")
        lines.append("")

        lines.append(f"### {FREQ_NAMES[freq]} - 底部特征")
        lines.append("")
        lines.append("| 底部 | 日期 | 点位 | DIF | DEA | MACD柱 | DIF>0 | 有背离 | 距最近金叉(根) |")
        lines.append("|------|------|------|-----|-----|--------|-------|--------|---------------|")
        for b in tb["bottoms"]:
            dif_z = "是" if b["dif_above_zero"] else "否"
            div = "是" if b["has_divergence"] else "否"
            gc_dist = str(b["bars_since_last_gc"]) if b["bars_since_last_gc"] is not None else "-"
            lines.append(f"| {b['label']} | {b['actual_date']} | {b['close']:.0f} | "
                          f"{b['dif']:.2f} | {b['dea']:.2f} | {b['macd']:.2f} | "
                          f"{dif_z} | {div} | {gc_dist} |")
        lines.append("")

    # 顶底特征共性汇总
    lines.append("### 顶底特征共性汇总")
    lines.append("")
    for freq in FREQS:
        if freq not in all_results or "tops_bottoms" not in all_results[freq]:
            continue
        summary = summarize_tb_features(all_results[freq]["tops_bottoms"])
        if not summary:
            continue

        lines.append(f"**{FREQ_NAMES[freq]}:**")
        lines.append("")
        tc = summary.get("top_count", 0)
        bc = summary.get("bottom_count", 0)
        if tc > 0:
            lines.append(f"- 顶部样本: {tc} 个")
            lines.append(f"  - DIF在零轴上方: {summary['top_dif_above_zero']}/{tc} "
                          f"({summary['top_dif_above_zero']/tc*100:.0f}%)")
            lines.append(f"  - MACD柱为正: {summary['top_macd_positive']}/{tc} "
                          f"({summary['top_macd_positive']/tc*100:.0f}%)")
            lines.append(f"  - 出现顶背离: {summary['top_has_divergence']}/{tc} "
                          f"({summary['top_has_divergence']/tc*100:.0f}%)")
            if summary.get("top_avg_bars_since_dc") is not None:
                lines.append(f"  - 距最近死叉平均: {summary['top_avg_bars_since_dc']} 根K线")
        if bc > 0:
            lines.append(f"- 底部样本: {bc} 个")
            lines.append(f"  - DIF在零轴上方: {summary['bottom_dif_above_zero']}/{bc} "
                          f"({summary['bottom_dif_above_zero']/bc*100:.0f}%)")
            lines.append(f"  - MACD柱为正: {summary['bottom_macd_positive']}/{bc} "
                          f"({summary['bottom_macd_positive']/bc*100:.0f}%)")
            lines.append(f"  - 出现底背离: {summary['bottom_has_divergence']}/{bc} "
                          f"({summary['bottom_has_divergence']/bc*100:.0f}%)")
            if summary.get("bottom_avg_bars_since_gc") is not None:
                lines.append(f"  - 距最近金叉平均: {summary['bottom_avg_bars_since_gc']} 根K线")
        lines.append("")

    # === 五、写入文件 ===
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"报告已生成: {output_path}")


# ── 主函数 ──────────────────────────────────────────────────

def main():
    global TS_CODE, INDEX_NAME

    parser = argparse.ArgumentParser(description="指数 MACD 信号分析")
    parser.add_argument("--ts_code", default="000001.SH", help="指数代码 (如 000001.SH, 399001.SZ, 399006.SZ)")
    parser.add_argument("--name", default=None, help="指数名称 (如 上证指数, 深证成指, 创业板指)")
    args = parser.parse_args()

    TS_CODE = args.ts_code
    INDEX_NAME = args.name or {
        "000001.SH": "上证指数",
        "399001.SZ": "深证成指",
        "399006.SZ": "创业板指",
    }.get(TS_CODE, TS_CODE)

    start_time = time.time()
    print(f"{'='*60}")
    print(f"MACD Signal Analysis")
    print(f"Index: {TS_CODE} ({INDEX_NAME})")
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print()

    all_results = {}

    # Step 1: 加载数据 + 检测信号
    for freq in FREQS:
        print(f"--- {FREQ_NAMES[freq]} ---")
        df = load_macd_data(freq, TS_CODE)
        if df.empty:
            print(f"  [WARN] 无数据，跳过（请先运行 compute_index_macd.py --freq {freq}）")
            continue

        print(f"  数据量: {len(df)} 条")
        print(f"  时间范围: {df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")

        # 检测信号并标注
        signals_df = analyze_signals(df, freq)
        sig_count = len(signals_df) if not signals_df.empty else 0
        print(f"  信号数量: {sig_count}")

        if not signals_df.empty:
            # 各信号类型数量
            for sig_type, count in signals_df["signal"].value_counts().items():
                print(f"    {SIGNAL_NAMES.get(sig_type, sig_type)}: {count}")

        all_results[freq] = {"data": df, "signals": signals_df}

    print()

    # Step 2: 顶底特征分析
    print("--- 顶底特征分析 ---")
    for freq in FREQS:
        if freq in all_results:
            r = all_results[freq]
            tb = analyze_tops_bottoms(r["data"], freq, r["signals"])
            all_results[freq]["tops_bottoms"] = tb
            print(f"  {FREQ_NAMES[freq]}: 顶部 {len(tb['tops'])} 个, 底部 {len(tb['bottoms'])} 个")

    print()

    # Step 3: 生成报告
    # 文件名按指数代码区分
    code_prefix = TS_CODE.split(".")[0]
    report_name = f"02-{code_prefix}-macd-analysis.md"
    output_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'report', report_name
    )
    output_path = os.path.abspath(output_path)
    generate_report(all_results, output_path, TS_CODE, INDEX_NAME)

    elapsed = round(time.time() - start_time, 1)
    print(f"\n{'='*60}")
    print(f"分析完成 | 总耗时: {elapsed}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
