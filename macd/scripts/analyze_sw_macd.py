"""Layer 3: 申万一级行业 MACD 信号分析

31个行业按5大类别分组，批量分析MACD信号，验证指数层面结论的行业适用性。

数据来源: stock_research.sw_macd_{daily|weekly|monthly}
牛熊标注: 复用上证指数周期（bull_bear_phases.py）

用法:
  python research/macd/scripts/analyze_sw_macd.py
  python research/macd/scripts/analyze_sw_macd.py --freq monthly    # 只跑月线
  python research/macd/scripts/analyze_sw_macd.py --category cyclical  # 只跑周期股
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
from bull_bear_phases import get_phase, tag_trend
from signal_detector import detect_all_signals, FREQ_ORDER, _find_local_peaks, _find_local_troughs

# ── 行业分类 ──────────────────────────────────────────────────

# 申万一级行业 → 5大类别
INDUSTRY_CATEGORIES = {
    "cyclical": {
        "name": "周期",
        "codes": {
            "801040.SI": "钢铁", "801950.SI": "煤炭", "801050.SI": "有色金属",
            "801030.SI": "基础化工", "801960.SI": "石油石化",
            "801710.SI": "建筑材料", "801720.SI": "建筑装饰",
        },
    },
    "financial": {
        "name": "金融地产",
        "codes": {
            "801780.SI": "银行", "801790.SI": "非银金融", "801180.SI": "房地产",
        },
    },
    "consumer": {
        "name": "大消费",
        "codes": {
            "801120.SI": "食品饮料", "801110.SI": "家用电器",
            "801200.SI": "商贸零售", "801210.SI": "社会服务",
            "801130.SI": "纺织服饰", "801980.SI": "美容护理",
            "801140.SI": "轻工制造", "801010.SI": "农林牧渔",
        },
    },
    "growth": {
        "name": "科技成长",
        "codes": {
            "801080.SI": "电子", "801750.SI": "计算机", "801770.SI": "通信",
            "801760.SI": "传媒", "801730.SI": "电力设备", "801740.SI": "国防军工",
        },
    },
    "defensive": {
        "name": "稳定制造",
        "codes": {
            "801160.SI": "公用事业", "801170.SI": "交通运输",
            "801150.SI": "医药生物", "801970.SI": "环保",
            "801880.SI": "汽车", "801890.SI": "机械设备",
        },
    },
}

# 反向映射: ts_code → (category_key, category_name, industry_name)
CODE_TO_INFO = {}
for _cat_key, _cat in INDUSTRY_CATEGORIES.items():
    for _code, _name in _cat["codes"].items():
        CODE_TO_INFO[_code] = (_cat_key, _cat["name"], _name)

ALL_CODES = list(CODE_TO_INFO.keys())  # 30个（排除综合）

FREQS = ["daily", "weekly", "monthly"]
FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

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

SIGNAL_NAMES = {
    "golden_cross": "金叉(零轴下)", "zero_golden_cross": "金叉(零轴上)",
    "death_cross": "死叉(零轴上)", "zero_death_cross": "死叉(零轴下)",
    "dif_cross_zero_up": "DIF上穿零轴", "dif_cross_zero_down": "DIF下穿零轴",
    "top_divergence": "顶背离", "bottom_divergence": "底背离",
}


# ── 数据加载 ──────────────────────────────────────────────────

def load_sw_macd(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 库加载申万行业 MACD 数据"""
    table = f"sw_macd_{freq}"
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


def analyze_industry_signals(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """对单个行业检测信号 + 标注牛熊 + 计算后续收益"""
    signals_df = detect_all_signals(df, freq=freq)
    if signals_df.empty:
        return signals_df

    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
    enriched = []
    for _, row in signals_df.iterrows():
        record = row.to_dict()
        record["trend"] = tag_trend(row["trade_date"])
        record.update(calc_post_returns(df, int(record["idx"]), horizons))
        enriched.append(record)

    return pd.DataFrame(enriched)


def analyze_dif_extremes(df: pd.DataFrame, freq: str) -> dict:
    """分析 DIF 极大值和极小值的反转率和后续收益"""
    order = FREQ_ORDER.get(freq, 20)
    horizons = RETURN_HORIZONS[freq]
    dif_values = df["dif"].values

    peak_indices = _find_local_peaks(dif_values, order)
    peaks = []
    for idx in peak_indices:
        row = df.iloc[idx]
        phase = get_phase(row["trade_date"])
        record = {
            "idx": idx, "trade_date": row["trade_date"],
            "close": float(row["close"]), "dif": float(row["dif"]),
            "trend": phase["trend"] if phase else "unknown",
        }
        record.update(calc_post_returns(df, idx, horizons))
        peaks.append(record)

    trough_indices = _find_local_troughs(dif_values, order)
    troughs = []
    for idx in trough_indices:
        row = df.iloc[idx]
        phase = get_phase(row["trade_date"])
        record = {
            "idx": idx, "trade_date": row["trade_date"],
            "close": float(row["close"]), "dif": float(row["dif"]),
            "trend": phase["trend"] if phase else "unknown",
        }
        record.update(calc_post_returns(df, idx, horizons))
        troughs.append(record)

    return {"peaks": peaks, "troughs": troughs}


def analyze_zero_zone(df: pd.DataFrame, freq: str) -> dict:
    """分析 DIF 零轴位置与后续收益的关系（均值回归验证）"""
    horizons = RETURN_HORIZONS[freq]
    main_h = horizons[-1]
    dif = df["dif"].values

    abs_dif = np.abs(dif[~np.isnan(dif)])
    if len(abs_dif) == 0:
        return {}

    q25, q50, q75 = np.percentile(abs_dif, [25, 50, 75])
    zones = [
        ("极弱(0~Q25)", 0, q25),
        ("弱(Q25~Q50)", q25, q50),
        ("强(Q50~Q75)", q50, q75),
        ("极强(>Q75)", q75, float("inf")),
    ]

    above_data, below_data = [], []
    for i in range(len(df)):
        if np.isnan(dif[i]) or i + main_h >= len(df):
            continue
        ret = (df.iloc[i + main_h]["close"] - df.iloc[i]["close"]) / df.iloc[i]["close"] * 100
        entry = {"dif": dif[i], "abs_dif": abs(dif[i]), "ret": ret}
        if dif[i] > 0:
            above_data.append(entry)
        elif dif[i] < 0:
            below_data.append(entry)

    def _calc_zone_stats(data_list):
        if not data_list:
            return {}
        df_tmp = pd.DataFrame(data_list)
        stats = {
            "count": len(df_tmp),
            "avg_ret": round(df_tmp["ret"].mean(), 2),
            "win_rate": round((df_tmp["ret"] > 0).sum() / len(df_tmp) * 100, 1),
        }
        zone_stats = []
        for name, lo, hi in zones:
            mask = (df_tmp["abs_dif"] >= lo) & (df_tmp["abs_dif"] < hi)
            subset = df_tmp[mask]
            if len(subset) > 0:
                zone_stats.append({
                    "zone": name, "count": len(subset),
                    "avg_ret": round(subset["ret"].mean(), 2),
                    "win_rate": round((subset["ret"] > 0).sum() / len(subset) * 100, 1),
                })
        stats["zones"] = zone_stats
        return stats

    return {
        "above_zero": _calc_zone_stats(above_data),
        "below_zero": _calc_zone_stats(below_data),
        "horizon": main_h,
        "quantiles": {"q25": round(q25, 2), "q50": round(q50, 2), "q75": round(q75, 2)},
    }


# ── 批量分析 ──────────────────────────────────────────────────

def run_all_analysis(codes: list[str], freqs: list[str]) -> dict:
    """对指定行业列表批量运行分析

    返回: {
        ts_code: {
            freq: {
                "data": DataFrame,
                "signals": DataFrame,
                "extremes": {"peaks": [...], "troughs": [...]},
                "zero_zone": {...},
            }
        }
    }
    """
    results = {}
    total = len(codes) * len(freqs)
    done = 0

    for ts_code in codes:
        info = CODE_TO_INFO.get(ts_code, ("unknown", "未知", ts_code))
        results[ts_code] = {}

        for freq in freqs:
            done += 1
            df = load_sw_macd(ts_code, freq)
            if df.empty:
                print(f"  [{done}/{total}] {info[2]} {FREQ_NAMES[freq]}: 无数据")
                continue

            # 信号分析
            signals = analyze_industry_signals(df, freq)
            # DIF极值分析
            extremes = analyze_dif_extremes(df, freq)
            # 零轴分析（月线和周线）
            zero_zone = analyze_zero_zone(df, freq) if freq in ("monthly", "weekly") else {}

            results[ts_code][freq] = {
                "data": df,
                "signals": signals,
                "extremes": extremes,
                "zero_zone": zero_zone,
            }

            sig_count = len(signals) if not signals.empty else 0
            peak_count = len(extremes["peaks"])
            trough_count = len(extremes["troughs"])
            print(f"  [{done}/{total}] {info[2]} {FREQ_NAMES[freq]}: "
                  f"{len(df)}条, {sig_count}信号, {peak_count}峰/{trough_count}谷")

    return results


def aggregate_by_category(results: dict, freq: str) -> dict:
    """按行业类别聚合分析结果"""
    horizons = RETURN_HORIZONS[freq]
    main_h = horizons[-1]
    buy_signals = {"golden_cross", "zero_golden_cross", "bottom_divergence", "dif_cross_zero_up"}

    agg = {}
    for cat_key, cat in INDUSTRY_CATEGORIES.items():
        all_peaks, all_troughs = [], []
        all_signals = []
        cat_industries = []

        for code in cat["codes"]:
            if code not in results or freq not in results[code]:
                continue
            r = results[code][freq]
            cat_industries.append(code)
            all_peaks.extend(r["extremes"]["peaks"])
            all_troughs.extend(r["extremes"]["troughs"])
            if not r["signals"].empty:
                all_signals.append(r["signals"])

        # DIF极大值聚合统计
        peak_rets = [p[f"ret_{main_h}"] for p in all_peaks if p.get(f"ret_{main_h}") is not None]
        peak_stats = {}
        if peak_rets:
            peak_stats = {
                "count": len(peak_rets),
                "avg_ret": round(np.mean(peak_rets), 2),
                "down_rate": round(sum(1 for r in peak_rets if r < 0) / len(peak_rets) * 100, 1),
            }

        # DIF极小值聚合统计
        trough_rets = [t[f"ret_{main_h}"] for t in all_troughs if t.get(f"ret_{main_h}") is not None]
        trough_stats = {}
        if trough_rets:
            trough_stats = {
                "count": len(trough_rets),
                "avg_ret": round(np.mean(trough_rets), 2),
                "up_rate": round(sum(1 for r in trough_rets if r > 0) / len(trough_rets) * 100, 1),
            }

        # 信号聚合统计
        sig_stats = {}
        if all_signals:
            merged = pd.concat(all_signals, ignore_index=True)
            for sig_type, group in merged.groupby("signal"):
                col = f"ret_{main_h}"
                valid = group[col].dropna()
                if len(valid) > 0:
                    is_buy = sig_type in buy_signals
                    if is_buy:
                        win = round((valid > 0).sum() / len(valid) * 100, 1)
                    else:
                        win = round((valid < 0).sum() / len(valid) * 100, 1)
                    sig_stats[sig_type] = {
                        "count": len(valid),
                        "avg_ret": round(valid.mean(), 2),
                        "win_rate": win,
                    }

        # 零轴聚合（月线）— 从原始数据重新计算
        zero_agg = {}
        if freq == "monthly":
            all_above, all_below = [], []
            for code in cat_industries:
                if code in results and freq in results[code]:
                    df = results[code][freq]["data"]
                    dif = df["dif"].values
                    for i in range(len(df)):
                        if np.isnan(dif[i]) or i + main_h >= len(df):
                            continue
                        ret = (df.iloc[i + main_h]["close"] - df.iloc[i]["close"]) / df.iloc[i]["close"] * 100
                        if dif[i] > 0:
                            all_above.append(ret)
                        elif dif[i] < 0:
                            all_below.append(ret)
            if all_above:
                zero_agg["above"] = {
                    "count": len(all_above),
                    "avg_ret": round(np.mean(all_above), 2),
                    "win_rate": round(sum(1 for r in all_above if r > 0) / len(all_above) * 100, 1),
                }
            if all_below:
                zero_agg["below"] = {
                    "count": len(all_below),
                    "avg_ret": round(np.mean(all_below), 2),
                    "win_rate": round(sum(1 for r in all_below if r > 0) / len(all_below) * 100, 1),
                }

        agg[cat_key] = {
            "name": cat["name"],
            "industry_count": len(cat_industries),
            "dif_peak_stats": peak_stats,
            "dif_trough_stats": trough_stats,
            "signal_stats": sig_stats,
            "zero_zone": zero_agg,
        }

    return agg


# ── 报告生成 ──────────────────────────────────────────────────

def generate_report(results: dict, output_path: str, freqs: list[str]):
    """生成行业MACD分析报告"""
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append("# 申万一级行业 MACD 信号分析报告 (Layer 3)")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append("> 分析范围: 30个申万一级行业（排除综合），按5大类别对比")
    lines.append("> 牛熊标注: 复用上证指数周期")
    lines.append("> 目的: 验证Layer 1/2的5个核心结论在行业层面的适用性，发现行业差异")
    lines.append("")

    # === 一、数据概览 ===
    lines.append("## 一、数据概览")
    lines.append("")
    for freq in freqs:
        lines.append(f"### {FREQ_NAMES[freq]}")
        lines.append("")
        lines.append("| 类别 | 行业 | K线数 | 时间范围 | 信号数 | DIF峰值 | DIF谷值 |")
        lines.append("|------|------|-------|---------|--------|---------|---------|")
        for cat_key, cat in INDUSTRY_CATEGORIES.items():
            for code, name in cat["codes"].items():
                if code not in results or freq not in results[code]:
                    continue
                r = results[code][freq]
                df = r["data"]
                sig_count = len(r["signals"]) if not r["signals"].empty else 0
                peaks = len(r["extremes"]["peaks"])
                troughs = len(r["extremes"]["troughs"])
                lines.append(f"| {cat['name']} | {name} | {len(df)} | "
                             f"{df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]} | "
                             f"{sig_count} | {peaks} | {troughs} |")
        lines.append("")

    # === 二、DIF极值信号 ===
    lines.append("## 二、DIF极值信号 — 按行业类别对比")
    lines.append("")
    lines.append("> Layer 1/2结论: DIF极大值后下跌率85~100%，极小值后上涨率89~100%")
    lines.append("> 本节验证该结论在不同行业类别中是否成立")
    lines.append("")

    for freq in freqs:
        agg = aggregate_by_category(results, freq)
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]

        lines.append(f"### {FREQ_NAMES[freq]} (评估窗口: {h_label})")
        lines.append("")

        # DIF极大值
        lines.append("**DIF极大值后价格走势（动能见顶 → 价格应跌）:**")
        lines.append("")
        lines.append("| 类别 | 样本数 | 平均收益 | 下跌率 |")
        lines.append("|------|--------|---------|--------|")
        for cat_key in INDUSTRY_CATEGORIES:
            a = agg[cat_key]
            ps = a["dif_peak_stats"]
            if ps:
                lines.append(f"| {a['name']} | {ps['count']} | {ps['avg_ret']:+.2f}% | {ps['down_rate']:.1f}% |")
        lines.append("")

        # DIF极小值
        lines.append("**DIF极小值后价格走势（动能见底 → 价格应涨）:**")
        lines.append("")
        lines.append("| 类别 | 样本数 | 平均收益 | 上涨率 |")
        lines.append("|------|--------|---------|--------|")
        for cat_key in INDUSTRY_CATEGORIES:
            a = agg[cat_key]
            ts = a["dif_trough_stats"]
            if ts:
                lines.append(f"| {a['name']} | {ts['count']} | {ts['avg_ret']:+.2f}% | {ts['up_rate']:.1f}% |")
        lines.append("")

        # 各行业单独的DIF极值数据
        lines.append(f"**{FREQ_NAMES[freq]} - 各行业DIF极值详情:**")
        lines.append("")
        lines.append("| 类别 | 行业 | 峰值数 | 峰后均值 | 峰后下跌率 | 谷值数 | 谷后均值 | 谷后上涨率 |")
        lines.append("|------|------|--------|---------|-----------|--------|---------|-----------|")
        for cat_key, cat in INDUSTRY_CATEGORIES.items():
            for code, name in cat["codes"].items():
                if code not in results or freq not in results[code]:
                    continue
                r = results[code][freq]
                # 峰值统计
                p_rets = [p[f"ret_{main_h}"] for p in r["extremes"]["peaks"]
                          if p.get(f"ret_{main_h}") is not None]
                # 谷值统计
                t_rets = [t[f"ret_{main_h}"] for t in r["extremes"]["troughs"]
                          if t.get(f"ret_{main_h}") is not None]

                p_avg = f"{np.mean(p_rets):+.2f}%" if p_rets else "-"
                p_down = f"{sum(1 for x in p_rets if x < 0) / len(p_rets) * 100:.1f}%" if p_rets else "-"
                t_avg = f"{np.mean(t_rets):+.2f}%" if t_rets else "-"
                t_up = f"{sum(1 for x in t_rets if x > 0) / len(t_rets) * 100:.1f}%" if t_rets else "-"

                lines.append(f"| {cat['name']} | {name} | {len(p_rets)} | {p_avg} | {p_down} | "
                             f"{len(t_rets)} | {t_avg} | {t_up} |")
        lines.append("")

    # === 三、月线零轴均值回归 ===
    lines.append("## 三、月线零轴位置 — 均值回归验证")
    lines.append("")
    lines.append("> Layer 1/2结论: 月线DIF极强多头(>Q75) → 1年亏14~15%; 极强空头 → 1年赚")
    lines.append("")

    if "monthly" in freqs:
        agg_m = aggregate_by_category(results, "monthly")

        # 类别聚合
        lines.append("### 按类别聚合（月线DIF位置 → 未来1年收益）")
        lines.append("")
        lines.append("| 类别 | DIF>0 样本 | DIF>0 1年均值 | DIF>0 上涨率 | DIF<0 样本 | DIF<0 1年均值 | DIF<0 上涨率 |")
        lines.append("|------|----------|-------------|------------|----------|-------------|------------|")
        for cat_key in INDUSTRY_CATEGORIES:
            a = agg_m[cat_key]
            zz = a.get("zero_zone", {})
            ab = zz.get("above", {})
            bl = zz.get("below", {})
            ab_str = f"{ab['count']} | {ab['avg_ret']:+.2f}% | {ab['win_rate']:.1f}%" if ab else "- | - | -"
            bl_str = f"{bl['count']} | {bl['avg_ret']:+.2f}% | {bl['win_rate']:.1f}%" if bl else "- | - | -"
            lines.append(f"| {a['name']} | {ab_str} | {bl_str} |")
        lines.append("")

        # 各行业零轴强度分区
        lines.append("### 各行业月线零轴强度分区详情")
        lines.append("")
        lines.append("> 按DIF绝对值分位(Q25/Q50/Q75)分区，验证极强多头/空头的均值回归")
        lines.append("")

        for cat_key, cat in INDUSTRY_CATEGORIES.items():
            lines.append(f"**{cat['name']}:**")
            lines.append("")
            lines.append("| 行业 | 位置 | 强度 | 样本 | 1年均值 | 上涨率 |")
            lines.append("|------|------|------|------|---------|--------|")
            for code, name in cat["codes"].items():
                if code not in results or "monthly" not in results[code]:
                    continue
                zz = results[code]["monthly"].get("zero_zone", {})
                for side, label in [("above_zero", "多头"), ("below_zero", "空头")]:
                    side_data = zz.get(side, {})
                    for zone in side_data.get("zones", []):
                        lines.append(f"| {name} | {label} | {zone['zone']} | "
                                     f"{zone['count']} | {zone['avg_ret']:+.2f}% | {zone['win_rate']:.1f}% |")
            lines.append("")

    # === 四、传统信号有效性 ===
    lines.append("## 四、传统信号有效性 — 行业类别对比")
    lines.append("")
    lines.append("> Layer 1/2结论: 金叉死叉胜率≈随机(46~54%)，但牛熊环境显著影响有效性")
    lines.append("")

    for freq in ["daily", "monthly"]:
        if freq not in freqs:
            continue
        agg = aggregate_by_category(results, freq)
        main_h = RETURN_HORIZONS[freq][-1]
        h_label = HORIZON_LABELS[freq][main_h]

        lines.append(f"### {FREQ_NAMES[freq]} (评估窗口: {h_label})")
        lines.append("")
        lines.append("| 类别 | 信号 | 数量 | 平均收益 | 胜率 |")
        lines.append("|------|------|------|---------|------|")
        for cat_key in INDUSTRY_CATEGORIES:
            a = agg[cat_key]
            for sig_type in ["golden_cross", "death_cross", "zero_golden_cross", "zero_death_cross"]:
                if sig_type in a["signal_stats"]:
                    s = a["signal_stats"][sig_type]
                    sig_name = SIGNAL_NAMES.get(sig_type, sig_type)
                    lines.append(f"| {a['name']} | {sig_name} | {s['count']} | "
                                 f"{s['avg_ret']:+.2f}% | {s['win_rate']:.1f}% |")
        lines.append("")

    # === 五、行业排名 ===
    lines.append("## 五、行业MACD信号质量排名")
    lines.append("")
    lines.append("> 按月线DIF极值反转率排名（综合得分 = (峰后下跌率 + 谷后上涨率) / 2）")
    lines.append("")

    if "monthly" in freqs:
        main_h = RETURN_HORIZONS["monthly"][-1]
        rankings = []
        for code in ALL_CODES:
            if code not in results or "monthly" not in results[code]:
                continue
            info = CODE_TO_INFO[code]
            r = results[code]["monthly"]

            peak_rets = [p[f"ret_{main_h}"] for p in r["extremes"]["peaks"]
                         if p.get(f"ret_{main_h}") is not None]
            trough_rets = [t[f"ret_{main_h}"] for t in r["extremes"]["troughs"]
                           if t.get(f"ret_{main_h}") is not None]

            if peak_rets and trough_rets:
                down_rate = sum(1 for x in peak_rets if x < 0) / len(peak_rets) * 100
                up_rate = sum(1 for x in trough_rets if x > 0) / len(trough_rets) * 100
                avg_score = (down_rate + up_rate) / 2
                rankings.append({
                    "code": code, "name": info[2], "category": info[1],
                    "peak_count": len(peak_rets),
                    "peak_down_rate": round(down_rate, 1),
                    "peak_avg_ret": round(np.mean(peak_rets), 2),
                    "trough_count": len(trough_rets),
                    "trough_up_rate": round(up_rate, 1),
                    "trough_avg_ret": round(np.mean(trough_rets), 2),
                    "score": round(avg_score, 1),
                })

        rankings.sort(key=lambda x: x["score"], reverse=True)
        lines.append("| 排名 | 行业 | 类别 | DIF峰值数 | 峰后下跌率 | 峰后均值 | "
                     "DIF谷值数 | 谷后上涨率 | 谷后均值 | 综合得分 |")
        lines.append("|------|------|------|---------|-----------|---------|---------|-----------|---------|---------|")
        for i, r in enumerate(rankings, 1):
            lines.append(f"| {i} | {r['name']} | {r['category']} | {r['peak_count']} | "
                         f"{r['peak_down_rate']:.1f}% | {r['peak_avg_ret']:+.2f}% | "
                         f"{r['trough_count']} | {r['trough_up_rate']:.1f}% | "
                         f"{r['trough_avg_ret']:+.2f}% | {r['score']:.1f} |")
        lines.append("")

    # === 六、结论 ===
    lines.append("## 六、关键发现与Layer 1/2对照")
    lines.append("")
    lines.append("（本节由脚本输出数据后人工撰写）")
    lines.append("")

    # 写入文件
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n报告已生成: {output_path}")


# ── 主函数 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="申万行业 MACD 信号分析 (Layer 3)")
    parser.add_argument("--freq", default=None,
                        help="只分析指定周期: daily/weekly/monthly (默认全部)")
    parser.add_argument("--category", default=None,
                        help="只分析指定类别: cyclical/financial/consumer/growth/defensive")
    args = parser.parse_args()

    freqs = [args.freq] if args.freq else FREQS
    if args.category:
        if args.category not in INDUSTRY_CATEGORIES:
            print(f"未知类别: {args.category}, 可选: {list(INDUSTRY_CATEGORIES.keys())}")
            return
        codes = list(INDUSTRY_CATEGORIES[args.category]["codes"].keys())
    else:
        codes = ALL_CODES

    start_time = time.time()
    print(f"{'='*60}")
    print(f"Layer 3: 申万行业 MACD 信号分析")
    print(f"行业数: {len(codes)}, 周期: {', '.join(freqs)}")
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print()

    # 批量分析
    results = run_all_analysis(codes, freqs)

    # 生成报告
    output_path = os.path.join(
        os.path.dirname(__file__), '..', '..', 'report',
        '05-sw-industry-macd-analysis.md'
    )
    output_path = os.path.abspath(output_path)
    generate_report(results, output_path, freqs)

    elapsed = round(time.time() - start_time, 1)
    print(f"\n{'='*60}")
    print(f"分析完成 | 行业: {len(codes)} | 周期: {len(freqs)} | 耗时: {elapsed}s")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
