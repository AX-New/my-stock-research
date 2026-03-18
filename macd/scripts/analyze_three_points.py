"""MACD 三个关键位置分析: DIF极大值 / DIF极小值 / DIF零轴

核心问题:
- DIF 达到极大值后，价格还会涨多少？DIF 峰值是否领先于价格峰值？
- DIF 达到极小值后，价格还会跌多少？DIF 谷值是否领先于价格谷值？
- DIF 在零轴附近时，趋势处于什么状态？

三指数对比: 000001.SH / 399001.SZ / 399006.SZ

用法:
  python research/macd/scripts/analyze_three_points.py                          # 默认三大指数
  python research/macd/scripts/analyze_three_points.py --indices 000016.SH:上证50,000300.SH:沪深300,000905.SH:中证500,000852.SH:中证1000 --output 03-broad-base-three-points.md
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
from bull_bear_phases import SH_TOPS, SH_BOTTOMS, get_phase, find_nearest_date
from signal_detector import _find_local_peaks, _find_local_troughs, FREQ_ORDER

INDICES = [
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
]

FREQS = ["daily", "weekly", "monthly"]
FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

RETURN_HORIZONS = {
    "daily": [5, 10, 20, 60],
    "weekly": [2, 4, 8, 13],
    "monthly": [1, 3, 6, 12],
}
HORIZON_LABELS = {
    "daily": {5: "T+5", 10: "T+10", 20: "T+20", 60: "T+60"},
    "weekly": {2: "T+2w", 4: "T+4w", 8: "T+8w", 13: "T+13w"},
    "monthly": {1: "T+1m", 3: "T+3m", 6: "T+6m", 12: "T+12m"},
}


def load_macd_data(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 库加载指数 MACD 数据"""
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


def calc_returns(df: pd.DataFrame, idx: int, horizons: list[int]) -> dict:
    """计算指定位置后续各窗口的收益率"""
    base = df.iloc[idx]["close"]
    ret = {}
    for h in horizons:
        target = idx + h
        if target < len(df):
            ret[f"ret_{h}"] = round((df.iloc[target]["close"] - base) / base * 100, 2)
        else:
            ret[f"ret_{h}"] = None
    return ret


def find_price_extreme_near(df: pd.DataFrame, dif_idx: int, window: int, find_max: bool) -> dict:
    """在 DIF 极值点附近找价格极值，计算领先/滞后关系

    dif_idx: DIF 极值点的索引
    window: 搜索窗口（前后各 window 根K线）
    find_max: True=找价格最高点(对应DIF极大值), False=找价格最低点
    """
    start = max(0, dif_idx - window)
    end = min(len(df), dif_idx + window + 1)
    subset = df.iloc[start:end]

    if find_max:
        price_idx = start + subset["close"].values.argmax()
    else:
        price_idx = start + subset["close"].values.argmin()

    # 正值=DIF领先(DIF先到极值)，负值=DIF滞后
    lead_bars = price_idx - dif_idx

    return {
        "price_extreme_idx": price_idx,
        "price_extreme_date": df.iloc[price_idx]["trade_date"],
        "price_extreme_value": float(df.iloc[price_idx]["close"]),
        "lead_bars": lead_bars,  # DIF极值到价格极值的距离(正=DIF领先)
    }


def analyze_dif_extremes(df: pd.DataFrame, freq: str) -> dict:
    """分析 DIF 极大值和极小值

    返回: {
        "peaks": [...],   # DIF 极大值点列表
        "troughs": [...], # DIF 极小值点列表
    }
    """
    order = FREQ_ORDER.get(freq, 20)
    horizons = RETURN_HORIZONS[freq]
    dif_values = df["dif"].values
    search_window = order * 3  # 在更大窗口内搜索价格极值

    # DIF 极大值
    peak_indices = _find_local_peaks(dif_values, order)
    peaks = []
    for idx in peak_indices:
        row = df.iloc[idx]
        record = {
            "idx": idx,
            "trade_date": row["trade_date"],
            "close": float(row["close"]),
            "dif": float(row["dif"]),
            "dea": float(row["dea"]),
            "macd": float(row["macd"]),
            "dif_above_zero": row["dif"] > 0,
            "trend": get_phase(row["trade_date"])["trend"] if get_phase(row["trade_date"]) else "unknown",
        }
        # 后续收益
        record.update(calc_returns(df, idx, horizons))
        # 价格极值的领先/滞后
        price_info = find_price_extreme_near(df, idx, search_window, find_max=True)
        record["lead_bars"] = price_info["lead_bars"]
        peaks.append(record)

    # DIF 极小值
    trough_indices = _find_local_troughs(dif_values, order)
    troughs = []
    for idx in trough_indices:
        row = df.iloc[idx]
        record = {
            "idx": idx,
            "trade_date": row["trade_date"],
            "close": float(row["close"]),
            "dif": float(row["dif"]),
            "dea": float(row["dea"]),
            "macd": float(row["macd"]),
            "dif_above_zero": row["dif"] > 0,
            "trend": get_phase(row["trade_date"])["trend"] if get_phase(row["trade_date"]) else "unknown",
        }
        record.update(calc_returns(df, idx, horizons))
        price_info = find_price_extreme_near(df, idx, search_window, find_max=False)
        record["lead_bars"] = price_info["lead_bars"]
        troughs.append(record)

    return {"peaks": peaks, "troughs": troughs}


def analyze_zero_zone(df: pd.DataFrame, freq: str) -> dict:
    """分析 DIF 在零轴附近的行为

    统计:
    - DIF > 0 时的平均后续收益（持有在多头区域的收益）
    - DIF < 0 时的平均后续收益（持有在空头区域的收益）
    - DIF 绝对值大小与后续收益的关系
    """
    horizons = RETURN_HORIZONS[freq]
    dif = df["dif"].values

    # DIF > 0 的K线
    above_zero = []
    below_zero = []

    # 按 DIF 绝对值分段
    # 取 DIF 绝对值的分位数作为分段标准
    abs_dif = np.abs(dif[~np.isnan(dif)])
    if len(abs_dif) == 0:
        return {"above_zero": {}, "below_zero": {}, "zones": []}

    q25, q50, q75 = np.percentile(abs_dif, [25, 50, 75])
    zones = [
        ("极弱(0~Q25)", 0, q25),
        ("弱(Q25~Q50)", q25, q50),
        ("强(Q50~Q75)", q50, q75),
        ("极强(>Q75)", q75, float("inf")),
    ]

    # 对多头/空头区域分别统计
    main_h = horizons[-1]  # 最长周期
    for i in range(len(df)):
        if np.isnan(dif[i]) or i + main_h >= len(df):
            continue
        ret = (df.iloc[i + main_h]["close"] - df.iloc[i]["close"]) / df.iloc[i]["close"] * 100
        if dif[i] > 0:
            above_zero.append({"dif": dif[i], "abs_dif": abs(dif[i]), "ret": ret})
        elif dif[i] < 0:
            below_zero.append({"dif": dif[i], "abs_dif": abs(dif[i]), "ret": ret})

    def _zone_stats(data_list):
        if not data_list:
            return {}
        df_tmp = pd.DataFrame(data_list)
        stats = {
            "count": len(df_tmp),
            "avg_ret": round(df_tmp["ret"].mean(), 2),
            "win_rate": round((df_tmp["ret"] > 0).sum() / len(df_tmp) * 100, 1),
        }
        # 按强度分区
        zone_stats = []
        for name, lo, hi in zones:
            mask = (df_tmp["abs_dif"] >= lo) & (df_tmp["abs_dif"] < hi)
            subset = df_tmp[mask]
            if len(subset) > 0:
                zone_stats.append({
                    "zone": name,
                    "count": len(subset),
                    "avg_ret": round(subset["ret"].mean(), 2),
                    "win_rate": round((subset["ret"] > 0).sum() / len(subset) * 100, 1),
                })
        stats["zones"] = zone_stats
        return stats

    return {
        "above_zero": _zone_stats(above_zero),
        "below_zero": _zone_stats(below_zero),
        "horizon": main_h,
        "quantiles": {"q25": round(q25, 2), "q50": round(q50, 2), "q75": round(q75, 2)},
    }


def analyze_top_bottom_lead(df: pd.DataFrame, freq: str) -> dict:
    """分析已知顶底处 DIF 极值的领先性

    对每个已知顶/底，找到最近的 DIF 极值点，计算领先了多少根K线
    """
    order = FREQ_ORDER.get(freq, 20)
    dif_values = df["dif"].values
    peak_indices = _find_local_peaks(dif_values, order)
    trough_indices = _find_local_troughs(dif_values, order)

    top_leads = []
    for top in SH_TOPS:
        date = find_nearest_date(df, top["ym"])
        if date is None:
            continue
        top_idx = df[df["trade_date"] == date].index[0]

        # 在顶部附近找最近的 DIF 极大值
        best_peak = None
        best_dist = float("inf")
        for pi in peak_indices:
            dist = top_idx - pi  # 正值=DIF先到峰值（领先）
            if abs(dist) < best_dist and abs(dist) <= order * 4:
                best_dist = abs(dist)
                best_peak = pi
                lead = dist

        if best_peak is not None:
            top_leads.append({
                "label": top["label"],
                "top_date": date,
                "top_close": float(df.iloc[top_idx]["close"]),
                "dif_peak_date": df.iloc[best_peak]["trade_date"],
                "dif_peak_value": float(dif_values[best_peak]),
                "lead_bars": lead,  # 正=DIF领先
            })

    bottom_leads = []
    for bottom in SH_BOTTOMS:
        date = find_nearest_date(df, bottom["ym"])
        if date is None:
            continue
        bot_idx = df[df["trade_date"] == date].index[0]

        best_trough = None
        best_dist = float("inf")
        for ti in trough_indices:
            dist = bot_idx - ti
            if abs(dist) < best_dist and abs(dist) <= order * 4:
                best_dist = abs(dist)
                best_trough = ti
                lead = dist

        if best_trough is not None:
            bottom_leads.append({
                "label": bottom["label"],
                "bottom_date": date,
                "bottom_close": float(df.iloc[bot_idx]["close"]),
                "dif_trough_date": df.iloc[best_trough]["trade_date"],
                "dif_trough_value": float(dif_values[best_trough]),
                "lead_bars": lead,
            })

    return {"top_leads": top_leads, "bottom_leads": bottom_leads}


# ── 报告生成 ──────────────────────────────────────────────────

def generate_report(all_data: dict, output_path: str, indices: list[tuple[str, str]] = None):
    """生成多指数对比报告"""
    indices = indices or INDICES
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    index_names = " / ".join(n for _, n in indices)
    lines.append("# MACD 三个关键位置分析: DIF极大值 / DIF极小值 / 零轴")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append(f"> 指数对比: {index_names}")
    lines.append("> 核心问题: DIF 的极值点是否领先于价格极值点？零轴位置的趋势含义？")
    lines.append("")

    # === 一、DIF 极大值分析 ===
    lines.append("## 一、DIF 极大值（动能峰值）")
    lines.append("")
    lines.append("DIF 达到局部最大值 = 上涨动能到达顶峰。之后即使价格继续涨，动能已在衰减。")
    lines.append("")

    for freq in FREQS:
        lines.append(f"### {FREQ_NAMES[freq]}")
        lines.append("")
        horizons = RETURN_HORIZONS[freq]
        h_labels = HORIZON_LABELS[freq]

        # 表头
        h_cols = [f"{h_labels[h]}均值" for h in horizons] + [f"{h_labels[h]}胜率" for h in horizons]
        lines.append("| 指数 | DIF峰值数 | 牛市占比 | DIF>0占比 | 领先价格(均值) | "
                      + " | ".join(f"{h_labels[h]}均值" for h in horizons) + " | "
                      + " | ".join(f"{h_labels[h]}胜率" for h in horizons) + " |")
        sep_count = 5 + len(horizons) * 2
        lines.append("|" + "|".join("------" for _ in range(sep_count)) + "|")

        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data:
                continue
            peaks = all_data[key]["extremes"]["peaks"]
            if not peaks:
                continue

            n = len(peaks)
            bull_pct = round(sum(1 for p in peaks if p["trend"] == "bull") / n * 100, 0)
            above_zero_pct = round(sum(1 for p in peaks if p["dif_above_zero"]) / n * 100, 0)
            leads = [p["lead_bars"] for p in peaks if p["lead_bars"] is not None]
            avg_lead = round(np.mean(leads), 1) if leads else "-"

            vals = [name, str(n), f"{bull_pct:.0f}%", f"{above_zero_pct:.0f}%", str(avg_lead)]
            for h in horizons:
                rets = [p[f"ret_{h}"] for p in peaks if p.get(f"ret_{h}") is not None]
                vals.append(f"{np.mean(rets):+.2f}%" if rets else "-")
            for h in horizons:
                rets = [p[f"ret_{h}"] for p in peaks if p.get(f"ret_{h}") is not None]
                if rets:
                    # DIF极大值后价格下跌为"正确"（动能见顶 → 价格应跌）
                    win = round(sum(1 for r in rets if r < 0) / len(rets) * 100, 1)
                    vals.append(f"{win}%")
                else:
                    vals.append("-")
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

        # 牛熊分拆
        lines.append(f"**{FREQ_NAMES[freq]} - 牛熊分拆（DIF极大值后价格走势）:**")
        lines.append("")
        main_h = horizons[-1]
        lines.append(f"| 指数 | 趋势 | 数量 | {h_labels[main_h]}平均收益 | {h_labels[main_h]}下跌率 |")
        lines.append("|------|------|------|---------|---------|")
        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data:
                continue
            peaks = all_data[key]["extremes"]["peaks"]
            for trend in ["bull", "bear"]:
                subset = [p for p in peaks if p["trend"] == trend]
                rets = [p[f"ret_{main_h}"] for p in subset if p.get(f"ret_{main_h}") is not None]
                if rets:
                    avg = np.mean(rets)
                    down_rate = sum(1 for r in rets if r < 0) / len(rets) * 100
                    trend_cn = "牛市" if trend == "bull" else "熊市"
                    lines.append(f"| {name} | {trend_cn} | {len(rets)} | {avg:+.2f}% | {down_rate:.1f}% |")
        lines.append("")

    # === 二、DIF 极小值分析 ===
    lines.append("## 二、DIF 极小值（动能谷值）")
    lines.append("")
    lines.append("DIF 达到局部最小值 = 下跌动能到达顶峰。之后即使价格继续跌，动能已在收敛。")
    lines.append("")

    for freq in FREQS:
        lines.append(f"### {FREQ_NAMES[freq]}")
        lines.append("")
        horizons = RETURN_HORIZONS[freq]
        h_labels = HORIZON_LABELS[freq]

        lines.append("| 指数 | DIF谷值数 | 熊市占比 | DIF<0占比 | 领先价格(均值) | "
                      + " | ".join(f"{h_labels[h]}均值" for h in horizons) + " | "
                      + " | ".join(f"{h_labels[h]}胜率" for h in horizons) + " |")
        sep_count = 5 + len(horizons) * 2
        lines.append("|" + "|".join("------" for _ in range(sep_count)) + "|")

        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data:
                continue
            troughs = all_data[key]["extremes"]["troughs"]
            if not troughs:
                continue

            n = len(troughs)
            bear_pct = round(sum(1 for t in troughs if t["trend"] == "bear") / n * 100, 0)
            below_zero_pct = round(sum(1 for t in troughs if not t["dif_above_zero"]) / n * 100, 0)
            leads = [t["lead_bars"] for t in troughs if t["lead_bars"] is not None]
            avg_lead = round(np.mean(leads), 1) if leads else "-"

            vals = [name, str(n), f"{bear_pct:.0f}%", f"{below_zero_pct:.0f}%", str(avg_lead)]
            for h in horizons:
                rets = [t[f"ret_{h}"] for t in troughs if t.get(f"ret_{h}") is not None]
                vals.append(f"{np.mean(rets):+.2f}%" if rets else "-")
            for h in horizons:
                rets = [t[f"ret_{h}"] for t in troughs if t.get(f"ret_{h}") is not None]
                if rets:
                    # DIF极小值后价格上涨为"正确"
                    win = round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1)
                    vals.append(f"{win}%")
                else:
                    vals.append("-")
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")

        # 牛熊分拆
        lines.append(f"**{FREQ_NAMES[freq]} - 牛熊分拆（DIF极小值后价格走势）:**")
        lines.append("")
        main_h = horizons[-1]
        lines.append(f"| 指数 | 趋势 | 数量 | {h_labels[main_h]}平均收益 | {h_labels[main_h]}上涨率 |")
        lines.append("|------|------|------|---------|---------|")
        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data:
                continue
            troughs = all_data[key]["extremes"]["troughs"]
            for trend in ["bull", "bear"]:
                subset = [t for t in troughs if t["trend"] == trend]
                rets = [t[f"ret_{main_h}"] for t in subset if t.get(f"ret_{main_h}") is not None]
                if rets:
                    avg = np.mean(rets)
                    up_rate = sum(1 for r in rets if r > 0) / len(rets) * 100
                    trend_cn = "牛市" if trend == "bull" else "熊市"
                    lines.append(f"| {name} | {trend_cn} | {len(rets)} | {avg:+.2f}% | {up_rate:.1f}% |")
        lines.append("")

    # === 三、零轴位置分析 ===
    lines.append("## 三、零轴位置分析")
    lines.append("")
    lines.append("DIF > 0 = 短期均线在长期均线之上（多头排列）; DIF < 0 = 空头排列。")
    lines.append("按 DIF 绝对值强度分区，分析持有收益。")
    lines.append("")

    for freq in FREQS:
        lines.append(f"### {FREQ_NAMES[freq]}")
        lines.append("")
        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data or "zero_zone" not in all_data[key]:
                continue
            zz = all_data[key]["zero_zone"]
            horizon = zz["horizon"]
            h_label = HORIZON_LABELS[freq].get(horizon, f"T+{horizon}")

            lines.append(f"**{name}** (评估窗口: {h_label}):")
            lines.append("")

            above = zz.get("above_zero", {})
            below = zz.get("below_zero", {})

            if above and below:
                lines.append(f"| 区域 | K线数 | 平均收益 | 上涨率 |")
                lines.append(f"|------|------|---------|-------|")
                lines.append(f"| DIF > 0 (多头) | {above['count']} | {above['avg_ret']:+.2f}% | {above['win_rate']:.1f}% |")
                lines.append(f"| DIF < 0 (空头) | {below['count']} | {below['avg_ret']:+.2f}% | {below['win_rate']:.1f}% |")
                lines.append("")

                # 强度分区
                lines.append(f"按 DIF 强度分区 (|DIF| 分位: Q25={zz['quantiles']['q25']}, Q50={zz['quantiles']['q50']}, Q75={zz['quantiles']['q75']}):")
                lines.append("")
                lines.append("| 位置 | 强度 | K线数 | 平均收益 | 上涨率 |")
                lines.append("|------|------|------|---------|-------|")
                for zone in above.get("zones", []):
                    lines.append(f"| 多头 | {zone['zone']} | {zone['count']} | {zone['avg_ret']:+.2f}% | {zone['win_rate']:.1f}% |")
                for zone in below.get("zones", []):
                    lines.append(f"| 空头 | {zone['zone']} | {zone['count']} | {zone['avg_ret']:+.2f}% | {zone['win_rate']:.1f}% |")
                lines.append("")

    # === 四、DIF 极值领先性分析 ===
    lines.append("## 四、DIF 极值对已知顶底的领先性")
    lines.append("")
    lines.append("> 正值 = DIF极值领先于价格极值（DIF先见顶/底），负值 = DIF滞后")
    lines.append("")

    for freq in FREQS:
        lines.append(f"### {FREQ_NAMES[freq]}")
        lines.append("")

        # 顶部领先性
        lines.append("**DIF极大值 vs 已知顶部:**")
        lines.append("")
        lines.append("| 指数 | 顶部 | 价格见顶日 | DIF见顶日 | 领先(根) |")
        lines.append("|------|------|----------|----------|---------|")
        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data or "tb_lead" not in all_data[key]:
                continue
            for t in all_data[key]["tb_lead"]["top_leads"]:
                lead_str = f"+{t['lead_bars']}" if t["lead_bars"] > 0 else str(t["lead_bars"])
                lines.append(f"| {name} | {t['label']} | {t['top_date']} | {t['dif_peak_date']} | {lead_str} |")
        lines.append("")

        # 计算平均领先
        lines.append("**平均领先根数:**")
        lines.append("")
        lines.append("| 指数 | 顶部平均领先 | 底部平均领先 |")
        lines.append("|------|-----------|-----------|")
        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data or "tb_lead" not in all_data[key]:
                continue
            tb = all_data[key]["tb_lead"]
            top_leads = [t["lead_bars"] for t in tb["top_leads"]]
            bot_leads = [b["lead_bars"] for b in tb["bottom_leads"]]
            top_avg = f"{np.mean(top_leads):+.1f}" if top_leads else "-"
            bot_avg = f"{np.mean(bot_leads):+.1f}" if bot_leads else "-"
            lines.append(f"| {name} | {top_avg} | {bot_avg} |")
        lines.append("")

        # 底部领先性
        lines.append("**DIF极小值 vs 已知底部:**")
        lines.append("")
        lines.append("| 指数 | 底部 | 价格见底日 | DIF见底日 | 领先(根) |")
        lines.append("|------|------|----------|----------|---------|")
        for ts_code, name in indices:
            key = (ts_code, freq)
            if key not in all_data or "tb_lead" not in all_data[key]:
                continue
            for b in all_data[key]["tb_lead"]["bottom_leads"]:
                lead_str = f"+{b['lead_bars']}" if b["lead_bars"] > 0 else str(b["lead_bars"])
                lines.append(f"| {name} | {b['label']} | {b['bottom_date']} | {b['dif_trough_date']} | {lead_str} |")
        lines.append("")

    # 写入文件
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"报告已生成: {output_path}")


def parse_indices(s: str) -> list[tuple[str, str]]:
    """解析指数参数，格式: 000016.SH:上证50,000300.SH:沪深300"""
    result = []
    for item in s.split(","):
        parts = item.strip().split(":")
        if len(parts) == 2:
            result.append((parts[0].strip(), parts[1].strip()))
        else:
            result.append((parts[0].strip(), parts[0].strip()))
    return result


def main():
    parser = argparse.ArgumentParser(description="MACD DIF极值/零轴 多指数对比分析")
    parser.add_argument("--indices", default=None,
                        help="指数列表，格式: 代码:名称,代码:名称 (默认三大指数)")
    parser.add_argument("--output", default=None,
                        help="输出文件名 (默认 03-three-points-analysis.md)")
    args = parser.parse_args()

    indices = parse_indices(args.indices) if args.indices else INDICES
    output_name = args.output or "03-three-points-analysis.md"

    start_time = time.time()
    print(f"{'='*60}")
    print(f"MACD Three Points Analysis")
    print(f"Indices: {', '.join(f'{c}({n})' for c, n in indices)}")
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print()

    all_data = {}

    for ts_code, name in indices:
        print(f"--- {name} ({ts_code}) ---")
        for freq in FREQS:
            df = load_macd_data(ts_code, freq)
            if df.empty:
                print(f"  {FREQ_NAMES[freq]}: 无数据")
                continue

            print(f"  {FREQ_NAMES[freq]}: {len(df)} 条 ({df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]})")

            key = (ts_code, freq)
            all_data[key] = {}

            # 1. DIF 极值分析
            extremes = analyze_dif_extremes(df, freq)
            all_data[key]["extremes"] = extremes
            print(f"    DIF极大值: {len(extremes['peaks'])} 个, DIF极小值: {len(extremes['troughs'])} 个")

            # 2. 零轴区域分析
            zero_zone = analyze_zero_zone(df, freq)
            all_data[key]["zero_zone"] = zero_zone

            # 3. 已知顶底领先性
            tb_lead = analyze_top_bottom_lead(df, freq)
            all_data[key]["tb_lead"] = tb_lead
            print(f"    顶部匹配: {len(tb_lead['top_leads'])} 个, 底部匹配: {len(tb_lead['bottom_leads'])} 个")

        print()

    # 生成报告
    output_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'report', output_name
    )
    output_path = os.path.abspath(output_path)
    generate_report(all_data, output_path, indices)

    elapsed = round(time.time() - start_time, 1)
    print(f"\n{'='*60}")
    print(f"Done | {elapsed}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
