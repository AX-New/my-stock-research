"""Layer 3: 申万行业 RSI 信号分析

分析内容:
1. 30个行业分别检测RSI信号、计算收益、标注牛熊
2. 按5大行业类别聚合对比
3. 产出行业差异报告和例外清单

用法:
  python rsi/research/analyze_sw_rsi.py                    # 分析所有行业
  python rsi/research/analyze_sw_rsi.py --save-signals     # 同时写入信号表
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

# ── 行业分类 ──────────────────────────────────────────────────

SW_CATEGORIES = {
    "周期": ["钢铁", "煤炭", "有色金属", "石油石化", "基础化工", "建筑材料"],
    "金融地产": ["银行", "非银金融", "房地产"],
    "大消费": ["食品饮料", "家用电器", "医药生物", "美容护理", "社会服务", "商贸零售", "纺织服饰", "轻工制造", "农林牧渔"],
    "科技成长": ["电子", "计算机", "通信", "传媒", "电力设备"],
    "稳定制造": ["公用事业", "交通运输", "机械设备", "建筑装饰", "国防军工", "汽车", "环保"],
}

FREQS = ["daily", "weekly", "monthly"]
FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

RETURN_HORIZONS = {
    "daily": [5, 10, 20, 60],
    "weekly": [2, 4, 8, 13],
    "monthly": [1, 3, 6, 12],
}

HORIZON_LABELS = {
    "daily": {5: "T+5", 10: "T+10", 20: "T+20", 60: "T+60"},
    "weekly": {2: "T+2", 4: "T+4", 8: "T+8", 13: "T+13"},
    "monthly": {1: "T+1", 3: "T+3", 6: "T+6", 12: "T+12"},
}

SIGNAL_TYPE_NAMES = {
    "extreme": "极端值",
    "divergence": "背离",
    "failure_swing": "失败摆动",
    "centerline": "中轴穿越",
}

SIGNAL_NAMES = {
    "rsi6_overbought": "RSI6超买", "rsi6_oversold": "RSI6超卖",
    "rsi12_overbought": "RSI12超买", "rsi12_oversold": "RSI12超卖",
    "rsi14_overbought": "RSI14超买", "rsi14_oversold": "RSI14超卖",
    "rsi24_overbought": "RSI24超买", "rsi24_oversold": "RSI24超卖",
    "rsi14_strong_overbought": "RSI14强超买", "rsi14_strong_oversold": "RSI14强超卖",
    "rsi14_adaptive_high": "RSI14自适应高", "rsi14_adaptive_low": "RSI14自适应低",
    "rsi14_bull_divergence": "RSI14底背离", "rsi14_bear_divergence": "RSI14顶背离",
    "rsi14_bull_failure_swing": "RSI14多头摆动", "rsi14_bear_failure_swing": "RSI14空头摆动",
    "rsi14_cross_above_50": "RSI14上穿50", "rsi14_cross_below_50": "RSI14下穿50",
}

BUY_SIGNALS = {
    "rsi6_oversold", "rsi12_oversold", "rsi14_oversold", "rsi24_oversold",
    "rsi14_strong_oversold", "rsi14_adaptive_low",
    "rsi14_bull_divergence", "rsi14_bull_failure_swing",
    "rsi14_cross_above_50",
}


def _is_buy_signal(name):
    return name in BUY_SIGNALS


# ── 数据加载 ──────────────────────────────────────────────────

def load_sw_rsi(freq: str, ts_code: str) -> pd.DataFrame:
    """从 stock_rsi 库加载申万行业 RSI 数据"""
    table = f"sw_rsi_{freq}"
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


def get_sw_industry_map() -> dict:
    """获取申万行业代码→名称映射"""
    sql = text(
        "SELECT ts_code, name FROM sw_rsi_daily "
        "GROUP BY ts_code, name"
    )
    # 使用 kline_loader 的接口获取行业列表
    from kline_loader import get_sw_l1_codes
    codes = get_sw_l1_codes()

    # 从 my_stock.sw_daily 获取名称映射
    sql2 = text("SELECT ts_code, name FROM my_stock.sw_daily GROUP BY ts_code, name")
    from database import read_engine
    with read_engine.connect() as conn:
        result = conn.execute(sql2)
        rows = result.fetchall()
    name_map = {row[0]: row[1] for row in rows}

    return {code: name_map.get(code, code) for code in codes}


def _get_category(industry_name: str) -> str:
    """根据行业名称查找所属类别"""
    for cat, industries in SW_CATEGORIES.items():
        if industry_name in industries:
            return cat
    return "其他"


# ── 信号分析 ──────────────────────────────────────────────────

def analyze_industry(df, freq, ts_code):
    """对单个行业检测信号 + 标注牛熊 + 计算收益"""
    signals = detect_all_signals(df, freq=freq)
    if not signals:
        return []

    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
    db_ret_cols = ["ret_5", "ret_10", "ret_20", "ret_60"]

    for sig in signals:
        sig["trend"] = tag_trend(sig["trade_date"])
        phase = get_phase(sig["trade_date"])
        sig["phase_id"] = phase["id"] if phase else None
        sig["phase_label"] = phase["label"] if phase else None

        idx = sig["idx"]
        base_close = df.iloc[idx]["close"]
        for i, h in enumerate(horizons):
            target = idx + h
            col_name = db_ret_cols[i]
            if target < len(df):
                future_close = df.iloc[target]["close"]
                sig[col_name] = round((future_close - base_close) / base_close * 100, 2)
            else:
                sig[col_name] = None
            sig[f"ret_{h}"] = sig.get(col_name)

        sig["freq"] = freq
        sig["ts_code"] = ts_code

    return signals


# ── 统计汇总 ──────────────────────────────────────────────────

def aggregate_stats(all_signals: dict, freq: str) -> list[dict]:
    """按行业×信号类型聚合统计

    all_signals: {ts_code: [signals]}
    返回: [{industry, category, signal_name, count, avg_ret, win_rate}]
    """
    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
    main_h = horizons[-1]
    col = f"ret_{main_h}"

    rows = []
    for ts_code, signals in all_signals.items():
        if not signals:
            continue

        # 按 signal_name 分组
        groups = defaultdict(list)
        for sig in signals:
            groups[sig["signal_name"]].append(sig)

        for sig_name, group in groups.items():
            is_buy = _is_buy_signal(sig_name)
            valid = [s[col] for s in group if s.get(col) is not None]

            row = {
                "ts_code": ts_code,
                "signal_name": sig_name,
                "signal_name_cn": SIGNAL_NAMES.get(sig_name, sig_name),
                "signal_type": group[0]["signal_type"],
                "direction": "buy" if is_buy else "sell",
                "count": len(group),
            }

            if valid:
                row["avg_ret"] = round(np.mean(valid), 2)
                if is_buy:
                    row["win_rate"] = round(sum(1 for v in valid if v > 0) / len(valid) * 100, 1)
                else:
                    row["win_rate"] = round(sum(1 for v in valid if v < 0) / len(valid) * 100, 1)
            else:
                row["avg_ret"] = None
                row["win_rate"] = None

            rows.append(row)

    return rows


# ── 报告生成 ──────────────────────────────────────────────────

def _fmt_pct(val, show_sign=True):
    if val is None:
        return "-"
    if show_sign:
        return f"{val:+.2f}%"
    return f"{val:.1f}%"


def generate_report(industry_results: dict, output_path: str, industry_map: dict):
    """生成 Layer 3 行业分析报告"""
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append("# 申万行业 RSI 信号分析报告 (Layer 3)")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append(f"> 行业数量: {len(industry_map)}")
    lines.append("> 牛熊阶段基准: 上证指数")
    lines.append("")

    # 聚焦周线（MACD/MA经验: 周线最佳）
    focus_freq = "weekly"
    freq_cn = FREQ_NAMES[focus_freq]

    # ── 一、数据概览 ─────────────────────────────────────────
    lines.append("## 一、数据概览")
    lines.append("")
    lines.append("| 行业 | 类别 | 日线信号数 | 周线信号数 | 月线信号数 |")
    lines.append("|------|------|----------|----------|----------|")

    for ts_code, name in sorted(industry_map.items(), key=lambda x: x[1]):
        cat = _get_category(name)
        counts = []
        for freq in FREQS:
            key = (ts_code, freq)
            sigs = industry_results.get(key, [])
            counts.append(str(len(sigs)))
        lines.append(f"| {name} | {cat} | {' | '.join(counts)} |")
    lines.append("")

    # ── 二、周线核心信号行业对比 ────────────────────────────
    lines.append(f"## 二、{freq_cn}核心信号 — 行业对比")
    lines.append("")
    lines.append("> 使用最长评估窗口的平均收益和胜率")
    lines.append("")

    # 收集周线统计
    weekly_signals = {}
    for ts_code in industry_map:
        key = (ts_code, focus_freq)
        weekly_signals[ts_code] = industry_results.get(key, [])

    stats = aggregate_stats(weekly_signals, focus_freq)

    # 按信号类型分组输出
    sig_types = defaultdict(list)
    for row in stats:
        sig_types[row["signal_type"]].append(row)

    # 核心信号对比表（每个信号类型一张表）
    for type_name in ["extreme", "divergence", "failure_swing", "centerline"]:
        if type_name not in sig_types:
            continue
        type_cn = SIGNAL_TYPE_NAMES[type_name]
        type_rows = sig_types[type_name]

        # 找出这个类型下的所有信号名
        sig_names = sorted(set(r["signal_name"] for r in type_rows))

        for sig_name in sig_names:
            cn = SIGNAL_NAMES.get(sig_name, sig_name)
            sig_rows = [r for r in type_rows if r["signal_name"] == sig_name]

            if not sig_rows:
                continue

            direction = sig_rows[0]["direction"]
            dir_cn = "买" if direction == "buy" else "卖"

            lines.append(f"### {cn} ({dir_cn})")
            lines.append("")
            lines.append("| 行业 | 类别 | 数量 | 平均收益 | 胜率 |")
            lines.append("|------|------|------|---------|------|")

            # 按胜率排序
            sorted_rows = sorted(sig_rows, key=lambda r: r.get("win_rate") or 0, reverse=True)
            for row in sorted_rows:
                name = industry_map.get(row["ts_code"], row["ts_code"])
                cat = _get_category(name)
                lines.append(
                    f"| {name} | {cat} | {row['count']} | "
                    f"{_fmt_pct(row.get('avg_ret'))} | "
                    f"{_fmt_pct(row.get('win_rate'), show_sign=False)} |"
                )
            lines.append("")

    # ── 三、按行业类别聚合 ────────────────────────────────────
    lines.append("## 三、按行业类别聚合")
    lines.append("")

    # 按类别聚合统计
    cat_stats = defaultdict(lambda: defaultdict(list))  # cat -> signal_name -> [rows]
    for row in stats:
        name = industry_map.get(row["ts_code"], "")
        cat = _get_category(name)
        cat_stats[cat][row["signal_name"]].append(row)

    # 核心信号的类别对比
    core_signals = ["rsi14_oversold", "rsi14_overbought", "rsi14_bull_divergence",
                    "rsi14_bear_divergence", "rsi14_cross_above_50", "rsi14_cross_below_50"]

    for sig_name in core_signals:
        cn = SIGNAL_NAMES.get(sig_name, sig_name)
        is_buy = _is_buy_signal(sig_name)
        dir_cn = "买" if is_buy else "卖"

        lines.append(f"### {cn} ({dir_cn}) — 类别对比")
        lines.append("")
        lines.append("| 类别 | 行业数 | 总信号数 | 平均收益 | 平均胜率 |")
        lines.append("|------|--------|---------|---------|---------|")

        for cat in ["周期", "金融地产", "大消费", "科技成长", "稳定制造"]:
            rows = cat_stats[cat].get(sig_name, [])
            if not rows:
                lines.append(f"| {cat} | 0 | 0 | - | - |")
                continue

            total_cnt = sum(r["count"] for r in rows)
            avg_rets = [r["avg_ret"] for r in rows if r.get("avg_ret") is not None]
            win_rates = [r["win_rate"] for r in rows if r.get("win_rate") is not None]

            avg_ret = round(np.mean(avg_rets), 2) if avg_rets else None
            avg_win = round(np.mean(win_rates), 1) if win_rates else None

            lines.append(
                f"| {cat} | {len(rows)} | {total_cnt} | "
                f"{_fmt_pct(avg_ret)} | {_fmt_pct(avg_win, show_sign=False)} |"
            )
        lines.append("")

    # ── 四、例外清单 ──────────────────────────────────────────
    lines.append("## 四、例外清单")
    lines.append("")
    lines.append("> 与通用结论差异显著的行业（胜率偏离全行业均值 >15个百分点）")
    lines.append("")

    # 计算每个信号的全行业平均胜率
    all_win_rates = defaultdict(list)
    for row in stats:
        if row.get("win_rate") is not None and row["count"] >= 3:
            all_win_rates[row["signal_name"]].append(row["win_rate"])

    avg_win_by_signal = {sig: np.mean(rates) for sig, rates in all_win_rates.items() if rates}

    exceptions = []
    for row in stats:
        if row.get("win_rate") is None or row["count"] < 3:
            continue
        sig_avg = avg_win_by_signal.get(row["signal_name"])
        if sig_avg is None:
            continue
        diff = row["win_rate"] - sig_avg
        if abs(diff) > 15:
            name = industry_map.get(row["ts_code"], row["ts_code"])
            exceptions.append({
                "industry": name,
                "category": _get_category(name),
                "signal": SIGNAL_NAMES.get(row["signal_name"], row["signal_name"]),
                "direction": "买" if _is_buy_signal(row["signal_name"]) else "卖",
                "win_rate": row["win_rate"],
                "avg_win_rate": round(sig_avg, 1),
                "diff": round(diff, 1),
                "count": row["count"],
            })

    if exceptions:
        exceptions.sort(key=lambda x: abs(x["diff"]), reverse=True)
        lines.append("| 行业 | 类别 | 信号 | 方向 | 胜率 | 全行业均值 | 偏差 | 样本数 |")
        lines.append("|------|------|------|------|------|----------|------|--------|")
        for e in exceptions[:30]:  # 最多30条
            lines.append(
                f"| {e['industry']} | {e['category']} | {e['signal']} | {e['direction']} | "
                f"{e['win_rate']:.1f}% | {e['avg_win_rate']:.1f}% | "
                f"{e['diff']:+.1f}pp | {e['count']} |"
            )
    else:
        lines.append("无显著例外")
    lines.append("")

    # ── 五、核心结论 ──────────────────────────────────────────
    lines.append("## 五、核心结论")
    lines.append("")

    # 自动总结
    # 1. 信号数量最多/最少的行业
    industry_total = defaultdict(int)
    for (ts_code, freq), sigs in industry_results.items():
        if freq == focus_freq:
            industry_total[ts_code] += len(sigs)

    if industry_total:
        most = max(industry_total, key=industry_total.get)
        least = min(industry_total, key=industry_total.get)
        lines.append(f"- **周线信号最活跃行业**: {industry_map.get(most, most)} ({industry_total[most]}次)")
        lines.append(f"- **周线信号最稀少行业**: {industry_map.get(least, least)} ({industry_total[least]}次)")

    # 2. 例外行业数
    if exceptions:
        lines.append(f"- **例外行业数**: {len(set(e['industry'] for e in exceptions))}个行业存在显著偏差")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("> 本报告由 `analyze_sw_rsi.py` 自动生成")
    lines.append("")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"报告已生成: {output_path}")


# ── 主函数 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="申万行业 RSI 信号分析 (Layer 3)")
    parser.add_argument("--save-signals", action="store_true", help="将信号写入数据库")
    parser.add_argument("--codes", default=None, help="行业代码，逗号分隔")
    parser.add_argument("--start-date", default=None,
                        help="增量起始日期 YYYYMMDD（仅影响信号写入，报告仍覆盖全量）")
    parser.add_argument("--end-date", default=None,
                        help="截止日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()

    start_time = time.time()
    log.info("=" * 60)
    log.info("RSI Signal Analysis - Layer 3 (SW Industries)")
    log.info("Start: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    industry_map = get_sw_industry_map()
    # 支持 --codes 过滤
    if args.codes:
        filter_codes = set(args.codes.split(","))
        industry_map = {k: v for k, v in industry_map.items() if k in filter_codes}
    log.info("行业数量: %d", len(industry_map))

    industry_results = {}  # (ts_code, freq) -> [signals]

    for ts_code, name in sorted(industry_map.items(), key=lambda x: x[1]):
        log.info("--- %s (%s) ---", name, ts_code)

        for freq in FREQS:
            df = load_sw_rsi(freq, ts_code)
            if df.empty:
                continue

            signals = analyze_industry(df, freq, ts_code)
            industry_results[(ts_code, freq)] = signals

            if signals:
                log.info("  %s: %d 条信号", FREQ_NAMES[freq], len(signals))

    log.info("")

    # 写入信号（支持增量）
    if args.save_signals:
        log.info("--- 写入信号到数据库 ---")
        init_rsi_tables()
        model = SIGNAL_MAP["sw"]
        write_cols = [
            "ts_code", "trade_date", "freq", "signal_type", "signal_name",
            "direction", "signal_value", "close", "rsi_values",
            "ret_5", "ret_10", "ret_20", "ret_60",
            "trend", "phase_id", "phase_label",
        ]
        total_written = 0
        for (ts_code, freq), signals in industry_results.items():
            if signals:
                sigs = signals
                if args.start_date:
                    sigs = [s for s in sigs if s["trade_date"] >= args.start_date]
                if args.end_date:
                    sigs = [s for s in sigs if s["trade_date"] <= args.end_date]
                if sigs:
                    records = [{k: sig.get(k) for k in write_cols} for sig in sigs]
                    batch_upsert(model, records, unique_keys=["ts_code", "trade_date", "freq", "signal_name"])
                    total_written += len(records)
        log.info("总计写入 %d 条信号", total_written)
        log.info("")

    # 生成报告
    output_path = os.path.join(
        os.path.dirname(__file__), '..', 'report', '03-sw-rsi-layer3-industry.md'
    )
    output_path = os.path.abspath(output_path)
    generate_report(industry_results, output_path, industry_map)

    elapsed = round(time.time() - start_time, 1)
    log.info("")
    log.info("=" * 60)
    log.info("分析完成 | 总耗时: %.1fs", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
