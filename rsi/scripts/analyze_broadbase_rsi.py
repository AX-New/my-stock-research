"""Layer 2.5: 四大宽基指数 RSI 市值分层验证

标的: 上证50(大盘) / 沪深300(中盘) / 中证500(小盘) / 中证1000(微盘)
目的: 验证 RSI 信号有效性是否随市值变化（MACD 经验: 方向一致但幅度递增）

前置: 先运行 compute_index_rsi.py 计算这 4 个指数的 RSI 数据
  python rsi/research/compute_index_rsi.py --codes "000016.SH,000300.SH,000905.SH,000852.SH" --all-freqs

用法:
  python rsi/research/analyze_broadbase_rsi.py
  python rsi/research/analyze_broadbase_rsi.py --save-signals
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

from analyze_index_rsi import (
    load_rsi_data, analyze_signals, calc_signal_stats,
    calc_trend_signal_matrix, calc_rsi_distribution, save_signals,
    FREQS, RETURN_HORIZONS, HORIZON_LABELS, FREQ_NAMES,
    SIGNAL_NAMES, SIGNAL_TYPE_NAMES, BUY_SIGNALS,
)
from app.logger import get_logger

log = get_logger("research.analyze_broadbase_rsi")

# ── 四大宽基指数 ─────────────────────────────────────────────
BROADBASE_INDICES = [
    ("000016.SH", "上证50", "大盘"),
    ("000300.SH", "沪深300", "中大盘"),
    ("000905.SH", "中证500", "中小盘"),
    ("000852.SH", "中证1000", "小微盘"),
]

# 报告中重点关注的核心信号（排除噪音信号如 RSI6）
CORE_SIGNALS = [
    "rsi14_oversold", "rsi14_overbought",
    "rsi14_strong_oversold", "rsi14_strong_overbought",
    "rsi14_bull_divergence", "rsi14_bear_divergence",
    "rsi14_bull_failure_swing", "rsi14_bear_failure_swing",
    "rsi14_cross_above_50", "rsi14_cross_below_50",
    "rsi24_oversold", "rsi24_overbought",
]


def _fmt_pct(val, show_sign=True):
    if val is None:
        return "-"
    return f"{val:+.2f}%" if show_sign else f"{val:.1f}%"


def _is_buy_signal(signal_name: str) -> bool:
    return signal_name in BUY_SIGNALS


def generate_broadbase_report(all_index_results: dict, output_path: str):
    """生成四大宽基指数 RSI 对比报告"""
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append("# 四大宽基指数 RSI 市值分层验证报告 (Layer 2.5)")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append("> 标的: 上证50(大盘) / 沪深300(中大盘) / 中证500(中小盘) / 中证1000(小微盘)")
    lines.append("> 目的: 验证 RSI 信号有效性是否随市值变化")
    lines.append("> RSI周期: 14(主) / 24(辅) | 信号类别: 极端值 / 背离 / 失败摆动 / 中轴穿越")
    lines.append("")

    # ── 一、数据概览 ──────────────────────────────────────────
    lines.append("## 一、数据概览")
    lines.append("")

    for freq in FREQS:
        freq_cn = FREQ_NAMES[freq]
        lines.append(f"### {freq_cn}")
        lines.append("")
        lines.append("| 指数 | 市值层 | K线数量 | 起始日期 | 结束日期 | 信号总数 |")
        lines.append("|------|--------|--------|---------|---------|---------|")

        for ts_code, name, cap_label in BROADBASE_INDICES:
            key = (ts_code, freq)
            if key not in all_index_results:
                lines.append(f"| {name} | {cap_label} | - | - | - | - |")
                continue
            r = all_index_results[key]
            df = r["data"]
            sig_count = len(r["signals"])
            lines.append(
                f"| {name} | {cap_label} | {len(df)} | {df['trade_date'].iloc[0]} | "
                f"{df['trade_date'].iloc[-1]} | {sig_count} |"
            )
        lines.append("")

    # ── 二、RSI 分布对比 ─────────────────────────────────────
    lines.append("## 二、RSI(14) 分布对比")
    lines.append("")
    lines.append("> 检查不同市值指数的 RSI 分布差异")
    lines.append("")

    for freq in FREQS:
        freq_cn = FREQ_NAMES[freq]
        lines.append(f"### {freq_cn}")
        lines.append("")
        lines.append("| 指数 | 均值 | 中位数 | 标准差 | RSI<30占比 | RSI>70占比 | Q10 | Q90 |")
        lines.append("|------|------|--------|--------|-----------|-----------|-----|-----|")

        for ts_code, name, _ in BROADBASE_INDICES:
            key = (ts_code, freq)
            if key not in all_index_results:
                continue
            dist = all_index_results[key].get("rsi_dist", {})
            if not dist:
                continue
            lines.append(
                f"| {name} | {dist['mean']} | {dist['median']} | {dist['std']} | "
                f"{dist['pct_below_30']}% | {dist['pct_above_70']}% | "
                f"{dist['q10']} | {dist['q90']} |"
            )
        lines.append("")

    # ── 三、核心信号胜率对比（按市值段）─────────────────────
    lines.append("## 三、核心信号胜率对比（按市值段）")
    lines.append("")
    lines.append("> 同一信号在不同市值指数上的胜率和收益率对比")
    lines.append("> 使用各频率主窗口: 日线T+60(3月) / 周线T+13(1季) / 月线T+12(1年)")
    lines.append("")

    for freq in FREQS:
        freq_cn = FREQ_NAMES[freq]
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]

        lines.append(f"### {freq_cn} (评估窗口: {h_label})")
        lines.append("")

        # 收集各指数的 stats
        index_stats = {}
        for ts_code, name, _ in BROADBASE_INDICES:
            key = (ts_code, freq)
            if key not in all_index_results:
                continue
            stats = all_index_results[key]["stats"]
            for row in stats:
                index_stats.setdefault(row["signal_name"], {})[name] = row

        # 筛选核心信号
        core_in_data = [sn for sn in CORE_SIGNALS if sn in index_stats]
        if not core_in_data:
            lines.append("无数据")
            lines.append("")
            continue

        # 表头: 信号 | 方向 | 上证50(次数/胜率) | 沪深300 | 中证500 | 中证1000
        idx_names = [name for _, name, _ in BROADBASE_INDICES]
        header = "| 信号 | 方向 | " + " | ".join(f"{n}(次/胜率/均值)" for n in idx_names) + " |"
        sep = "|------|------|" + "|".join("------" for _ in idx_names) + "|"
        lines.append(header)
        lines.append(sep)

        for sn in core_in_data:
            cn = SIGNAL_NAMES.get(sn, sn)
            is_buy = _is_buy_signal(sn)
            dir_cn = "买" if is_buy else "卖"

            cols = [cn, dir_cn]
            for _, name, _ in BROADBASE_INDICES:
                row = index_stats[sn].get(name)
                if row:
                    cnt = row["count"]
                    win = row.get(f"win_{main_h}")
                    avg = row.get(f"avg_{main_h}")
                    win_s = _fmt_pct(win, show_sign=False)
                    avg_s = _fmt_pct(avg)
                    cols.append(f"{cnt} / {win_s} / {avg_s}")
                else:
                    cols.append("-")
            lines.append("| " + " | ".join(cols) + " |")
        lines.append("")

    # ── 四、牛熊分组对比 ─────────────────────────────────────
    lines.append("## 四、牛熊分组对比（周线主窗口）")
    lines.append("")
    lines.append("> 检查同一信号在牛熊中的表现是否跨市值一致")
    lines.append("")

    freq = "weekly"
    if any((ts, freq) in all_index_results for ts, _, _ in BROADBASE_INDICES):
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]

        # 收集各指数的 trend_matrix
        index_matrix = {}
        for ts_code, name, _ in BROADBASE_INDICES:
            key = (ts_code, freq)
            if key not in all_index_results:
                continue
            matrix = all_index_results[key]["trend_matrix"]
            for row in matrix:
                k = (row["signal_name"], row["trend"])
                index_matrix.setdefault(k, {})[name] = row

        # 输出核心信号的牛熊三行
        for sn in CORE_SIGNALS:
            cn = SIGNAL_NAMES.get(sn, sn)
            has_data = any((sn, t) in index_matrix for t in ["全量", "牛市", "熊市"])
            if not has_data:
                continue

            is_buy = _is_buy_signal(sn)
            dir_cn = "买" if is_buy else "卖"

            lines.append(f"**{cn}** ({dir_cn})")
            lines.append("")
            idx_names = [name for _, name, _ in BROADBASE_INDICES]
            header = "| 阶段 | " + " | ".join(f"{n}(次/胜率/均值)" for n in idx_names) + " |"
            sep = "|------|" + "|".join("------" for _ in idx_names) + "|"
            lines.append(header)
            lines.append(sep)

            for trend in ["全量", "牛市", "熊市"]:
                k = (sn, trend)
                cols = [trend]
                for _, name, _ in BROADBASE_INDICES:
                    row = index_matrix.get(k, {}).get(name)
                    if row:
                        cnt = row["count"]
                        win = row.get("win_rate")
                        avg = row.get("avg_ret")
                        win_s = _fmt_pct(win, show_sign=False)
                        avg_s = _fmt_pct(avg)
                        cols.append(f"{cnt} / {win_s} / {avg_s}")
                    else:
                        cols.append("-")
                lines.append("| " + " | ".join(cols) + " |")
            lines.append("")

    # ── 五、核心结论 ──────────────────────────────────────────
    lines.append("## 五、核心结论")
    lines.append("")

    # 自动生成结论: 检查方向一致性和幅度差异
    freq = "weekly"
    main_h = RETURN_HORIZONS[freq][-1]

    direction_consistent = 0
    direction_total = 0
    amplitude_larger_small = 0
    amplitude_total = 0

    for sn in CORE_SIGNALS:
        # 检查方向一致性: 所有指数的胜率是否都 >50% 或都 <50%
        wins = []
        avgs = []
        for ts_code, name, _ in BROADBASE_INDICES:
            key = (ts_code, freq)
            if key not in all_index_results:
                continue
            for row in all_index_results[key]["stats"]:
                if row["signal_name"] == sn:
                    w = row.get(f"win_{main_h}")
                    a = row.get(f"avg_{main_h}")
                    if w is not None:
                        wins.append(w)
                    if a is not None:
                        avgs.append(a)

        if len(wins) >= 3:
            direction_total += 1
            # 方向一致 = 都>50 或都<50
            if all(w > 50 for w in wins) or all(w < 50 for w in wins):
                direction_consistent += 1

        if len(avgs) >= 3:
            amplitude_total += 1
            # 小盘幅度更大: 最后一个(中证1000)的绝对值 > 第一个(上证50)
            if abs(avgs[-1]) > abs(avgs[0]):
                amplitude_larger_small += 1

    if direction_total > 0:
        pct = round(direction_consistent / direction_total * 100, 1)
        lines.append(f"1. **方向一致性**: {direction_consistent}/{direction_total} 个核心信号在4大指数中方向一致 ({pct}%)")
    if amplitude_total > 0:
        pct = round(amplitude_larger_small / amplitude_total * 100, 1)
        lines.append(f"2. **幅度递增**: {amplitude_larger_small}/{amplitude_total} 个信号符合\"小盘幅度更大\"规律 ({pct}%)")

    lines.append("3. **与 MACD/MA 对比**: MACD 发现方向一致但幅度递增（小盘弹性更大），RSI 结论见上")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("> 本报告由 `analyze_broadbase_rsi.py` 自动生成，所有结论需结合牛熊周期判读")
    lines.append("")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"报告已生成: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Layer 2.5: 四大宽基指数 RSI 市值分层验证")
    parser.add_argument("--save-signals", action="store_true", help="将信号写入数据库")
    parser.add_argument("--start-date", default=None,
                        help="增量起始日期 YYYYMMDD（仅影响信号写入，报告仍覆盖全量）")
    parser.add_argument("--end-date", default=None,
                        help="截止日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()

    start_time = time.time()
    log.info("=" * 60)
    log.info("Layer 2.5: 四大宽基指数 RSI 市值分层验证")
    log.info("标的: %s", " / ".join(f"{n}({c})" for c, n, _ in BROADBASE_INDICES))
    log.info("Start: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    all_index_results = {}

    for ts_code, name, cap_label in BROADBASE_INDICES:
        log.info("")
        log.info("━━━ %s (%s) [%s] ━━━", name, ts_code, cap_label)

        for freq in FREQS:
            freq_cn = FREQ_NAMES[freq]
            df = load_rsi_data(freq, ts_code)
            if df.empty:
                log.warning("  %s 无数据（请先运行 compute_index_rsi.py）", freq_cn)
                continue

            log.info("  %s: %d 条 (%s ~ %s)", freq_cn, len(df),
                     df['trade_date'].iloc[0], df['trade_date'].iloc[-1])

            # RSI 分布
            rsi_dist = calc_rsi_distribution(df)

            # 信号检测 + 牛熊标注 + 收益计算
            signals = analyze_signals(df, freq, ts_code)
            log.info("    信号: %d 条", len(signals))

            # 统计
            horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
            stats = calc_signal_stats(signals, horizons) if signals else []
            trend_matrix = calc_trend_signal_matrix(signals, horizons) if signals else []

            all_index_results[(ts_code, freq)] = {
                "data": df,
                "signals": signals,
                "stats": stats,
                "trend_matrix": trend_matrix,
                "rsi_dist": rsi_dist,
            }

            # 写入信号（支持增量）
            if args.save_signals and signals:
                sigs = signals
                if args.start_date:
                    sigs = [s for s in sigs if s["trade_date"] >= args.start_date]
                if args.end_date:
                    sigs = [s for s in sigs if s["trade_date"] <= args.end_date]
                if sigs:
                    save_signals(sigs, source_type="index")

    # 生成对比报告
    output_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'report',
        '04-broadbase-rsi-layer2.5-market-cap.md'
    ))
    generate_broadbase_report(all_index_results, output_path)

    elapsed = round(time.time() - start_time, 1)
    log.info("")
    log.info("=" * 60)
    log.info("Layer 2.5 完成 | 总耗时: %.1fs", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
