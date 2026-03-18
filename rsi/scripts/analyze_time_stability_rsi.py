"""Layer 3.5: RSI 信号时间稳定性验证

将上证指数 RSI 信号按 5 年分段，检查:
1. 信号方向是否 30 年稳定（胜率始终 >50% 或始终 <50%）
2. 收益幅度是否在衰减（市场效率提升 → alpha 缩小）

MACD 经验: 方向 30 年稳定，但幅度从 -8.95% 衰减到 -2.64%

用法:
  python rsi/research/analyze_time_stability_rsi.py
  python rsi/research/analyze_time_stability_rsi.py --ts_code 399001.SZ --name 深证成指
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
    FREQS, RETURN_HORIZONS, HORIZON_LABELS, FREQ_NAMES,
    SIGNAL_NAMES, SIGNAL_TYPE_NAMES, BUY_SIGNALS,
)
from app.logger import get_logger

log = get_logger("research.analyze_time_stability_rsi")

# ── 时间分段 ─────────────────────────────────────────────────
TIME_PERIODS = [
    ("19930101", "20001231", "1993-2000"),
    ("20010101", "20071231", "2001-2007"),
    ("20080101", "20141231", "2008-2014"),
    ("20150101", "20201231", "2015-2020"),
    ("20210101", "20261231", "2021-2026"),
]

# 报告重点关注的核心信号
CORE_SIGNALS = [
    "rsi14_oversold", "rsi14_overbought",
    "rsi14_strong_oversold", "rsi14_strong_overbought",
    "rsi24_oversold", "rsi24_overbought",
    "rsi14_bull_divergence", "rsi14_bear_divergence",
    "rsi14_bull_failure_swing", "rsi14_bear_failure_swing",
    "rsi14_cross_above_50", "rsi14_cross_below_50",
]


def _fmt_pct(val, show_sign=True):
    if val is None:
        return "-"
    return f"{val:+.2f}%" if show_sign else f"{val:.1f}%"


def _is_buy_signal(signal_name: str) -> bool:
    return signal_name in BUY_SIGNALS


def analyze_by_period(df, freq: str, ts_code: str) -> dict:
    """对整段数据检测信号，然后按时间分段统计

    注意: 信号检测在全量数据上进行（保证上下文完整），
    然后按 trade_date 分配到各时段统计。
    """
    # 在全量数据上检测信号
    all_signals = analyze_signals(df, freq, ts_code)

    # 按时段分组
    period_results = {}
    for start, end, label in TIME_PERIODS:
        period_signals = [
            s for s in all_signals
            if start <= s["trade_date"].replace("-", "") <= end
        ]
        if not period_signals:
            continue

        horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
        stats = calc_signal_stats(period_signals, horizons)

        period_results[label] = {
            "signals": period_signals,
            "stats": stats,
            "count": len(period_signals),
        }

    return period_results


def generate_stability_report(all_freq_results: dict, output_path: str,
                              ts_code: str, index_name: str):
    """生成时间稳定性分析报告"""
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append("# RSI 信号时间稳定性验证报告 (Layer 3.5)")
    lines.append("")
    lines.append(f"> 生成时间: {now}")
    lines.append(f"> 分析标的: {ts_code} ({index_name})")
    lines.append(f"> 时间分段: {' / '.join(label for _, _, label in TIME_PERIODS)}")
    lines.append("> 目的: 检查信号方向是否稳定、收益幅度是否衰减")
    lines.append("")

    # ── 一、分析方法 ──────────────────────────────────────────
    lines.append("## 一、分析方法")
    lines.append("")
    lines.append("1. 在全量历史数据上检测 RSI 信号（保证信号检测的上下文完整性）")
    lines.append("2. 按交易日期将信号分配到 5 个时段")
    lines.append("3. 每个时段独立统计胜率和平均收益")
    lines.append("4. 对比各时段的方向一致性和幅度变化趋势")
    lines.append("")

    # ── 二、各时段信号概览 ────────────────────────────────────
    lines.append("## 二、各时段信号数量概览")
    lines.append("")

    for freq in FREQS:
        if freq not in all_freq_results:
            continue
        freq_cn = FREQ_NAMES[freq]
        period_results = all_freq_results[freq]

        lines.append(f"### {freq_cn}")
        lines.append("")

        periods_with_data = [label for _, _, label in TIME_PERIODS if label in period_results]
        if not periods_with_data:
            lines.append("无数据")
            lines.append("")
            continue

        header = "| 信号 | 方向 | " + " | ".join(periods_with_data) + " | 合计 |"
        sep = "|------|------|" + "|".join("------" for _ in periods_with_data) + "|------|"
        lines.append(header)
        lines.append(sep)

        for sn in CORE_SIGNALS:
            cn = SIGNAL_NAMES.get(sn, sn)
            is_buy = _is_buy_signal(sn)
            dir_cn = "买" if is_buy else "卖"

            counts = []
            total = 0
            for period_label in periods_with_data:
                pr = period_results[period_label]
                cnt = sum(1 for s in pr["signals"] if s["signal_name"] == sn)
                counts.append(str(cnt))
                total += cnt

            if total == 0:
                continue
            lines.append(f"| {cn} | {dir_cn} | " + " | ".join(counts) + f" | {total} |")
        lines.append("")

    # ── 三、核心信号时段胜率对比 ──────────────────────────────
    lines.append("## 三、核心信号时段胜率与收益对比")
    lines.append("")
    lines.append("> 关键观察: 同一信号的胜率在各时段是否稳定（方向一致性），平均收益是否递减（幅度衰减）")
    lines.append("")

    for freq in FREQS:
        if freq not in all_freq_results:
            continue
        freq_cn = FREQ_NAMES[freq]
        horizons = RETURN_HORIZONS[freq]
        main_h = horizons[-1]
        h_label = HORIZON_LABELS[freq][main_h]
        period_results = all_freq_results[freq]

        periods_with_data = [label for _, _, label in TIME_PERIODS if label in period_results]
        if not periods_with_data:
            continue

        lines.append(f"### {freq_cn} (评估窗口: {h_label})")
        lines.append("")

        # 收集各时段各信号的 stats
        period_stats = {}
        for period_label in periods_with_data:
            for row in period_results[period_label]["stats"]:
                period_stats.setdefault(row["signal_name"], {})[period_label] = row

        # 胜率表
        lines.append("**胜率对比**")
        lines.append("")
        header = "| 信号 | 方向 | " + " | ".join(periods_with_data) + " | 稳定性 |"
        sep = "|------|------|" + "|".join("------" for _ in periods_with_data) + "|--------|"
        lines.append(header)
        lines.append(sep)

        for sn in CORE_SIGNALS:
            if sn not in period_stats:
                continue
            cn = SIGNAL_NAMES.get(sn, sn)
            is_buy = _is_buy_signal(sn)
            dir_cn = "买" if is_buy else "卖"

            wins = []
            cols = [cn, dir_cn]
            for period_label in periods_with_data:
                row = period_stats[sn].get(period_label)
                if row:
                    w = row.get(f"win_{main_h}")
                    cols.append(_fmt_pct(w, show_sign=False))
                    if w is not None:
                        wins.append(w)
                else:
                    cols.append("-")

            # 稳定性判断: 所有时段方向一致(都>50或都<50) = 稳定
            if len(wins) >= 3:
                all_high = all(w > 50 for w in wins)
                all_low = all(w < 50 for w in wins)
                if all_high or all_low:
                    stability = "✅ 稳定"
                else:
                    stability = "⚠️ 不稳定"
            else:
                stability = "样本少"
            cols.append(stability)
            lines.append("| " + " | ".join(cols) + " |")
        lines.append("")

        # 平均收益表
        lines.append("**平均收益对比**")
        lines.append("")
        header = "| 信号 | 方向 | " + " | ".join(periods_with_data) + " | 趋势 |"
        sep = "|------|------|" + "|".join("------" for _ in periods_with_data) + "|------|"
        lines.append(header)
        lines.append(sep)

        for sn in CORE_SIGNALS:
            if sn not in period_stats:
                continue
            cn = SIGNAL_NAMES.get(sn, sn)
            is_buy = _is_buy_signal(sn)
            dir_cn = "买" if is_buy else "卖"

            avgs = []
            cols = [cn, dir_cn]
            for period_label in periods_with_data:
                row = period_stats[sn].get(period_label)
                if row:
                    a = row.get(f"avg_{main_h}")
                    cols.append(_fmt_pct(a))
                    if a is not None:
                        avgs.append(a)
                else:
                    cols.append("-")

            # 趋势判断: 绝对值在递减 = 衰减
            if len(avgs) >= 3:
                abs_avgs = [abs(a) for a in avgs]
                # 简单判断: 最后一段的绝对值 < 第一段的 50% → 显著衰减
                if abs_avgs[-1] < abs_avgs[0] * 0.5:
                    trend = "📉 显著衰减"
                elif abs_avgs[-1] < abs_avgs[0] * 0.8:
                    trend = "📉 轻微衰减"
                elif abs_avgs[-1] > abs_avgs[0] * 1.2:
                    trend = "📈 增强"
                else:
                    trend = "→ 平稳"
            else:
                trend = "样本少"
            cols.append(trend)
            lines.append("| " + " | ".join(cols) + " |")
        lines.append("")

    # ── 四、核心结论 ──────────────────────────────────────────
    lines.append("## 四、核心结论")
    lines.append("")

    # 自动统计
    total_stable = 0
    total_unstable = 0
    total_decaying = 0
    total_tested = 0

    freq = "weekly"  # 用周线做结论判断（MACD 经验: 周线最佳）
    if freq in all_freq_results:
        main_h = RETURN_HORIZONS[freq][-1]
        period_results = all_freq_results[freq]
        periods_with_data = [label for _, _, label in TIME_PERIODS if label in period_results]

        for sn in CORE_SIGNALS:
            wins = []
            avgs = []
            for period_label in periods_with_data:
                for row in period_results[period_label]["stats"]:
                    if row["signal_name"] == sn:
                        w = row.get(f"win_{main_h}")
                        a = row.get(f"avg_{main_h}")
                        if w is not None:
                            wins.append(w)
                        if a is not None:
                            avgs.append(a)

            if len(wins) >= 3:
                total_tested += 1
                if all(w > 50 for w in wins) or all(w < 50 for w in wins):
                    total_stable += 1
                else:
                    total_unstable += 1

            if len(avgs) >= 3:
                abs_avgs = [abs(a) for a in avgs]
                if abs_avgs[-1] < abs_avgs[0] * 0.8:
                    total_decaying += 1

    if total_tested > 0:
        stable_pct = round(total_stable / total_tested * 100, 1)
        lines.append(f"1. **方向稳定性**: {total_stable}/{total_tested} 个信号方向稳定 ({stable_pct}%)，{total_unstable} 个不稳定")
        lines.append(f"2. **幅度衰减**: {total_decaying}/{total_tested} 个信号出现收益幅度衰减")
        lines.append("3. **与 MACD 对比**: MACD 方向 30 年稳定，幅度从 -8.95% 衰减到 -2.64%")
    else:
        lines.append("- 周线数据不足，无法得出时间稳定性结论")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("> 本报告由 `analyze_time_stability_rsi.py` 自动生成")
    lines.append("")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"报告已生成: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Layer 3.5: RSI 信号时间稳定性验证")
    parser.add_argument("--ts_code", default="000001.SH", help="指数代码")
    parser.add_argument("--name", default=None, help="指数名称")
    parser.add_argument("--start-date", default=None,
                        help="增量起始日期 YYYYMMDD（限制分析数据范围）")
    parser.add_argument("--end-date", default=None,
                        help="截止日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()

    ts_code = args.ts_code
    index_name = args.name or {
        "000001.SH": "上证指数",
        "399001.SZ": "深证成指",
        "399006.SZ": "创业板指",
    }.get(ts_code, ts_code)

    start_time = time.time()
    log.info("=" * 60)
    log.info("Layer 3.5: RSI 信号时间稳定性验证")
    log.info("标的: %s (%s)", ts_code, index_name)
    log.info("时段: %s", " / ".join(label for _, _, label in TIME_PERIODS))
    log.info("Start: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    all_freq_results = {}

    for freq in FREQS:
        freq_cn = FREQ_NAMES[freq]
        df = load_rsi_data(freq, ts_code)
        if df.empty:
            log.warning("%s 无数据，跳过", freq_cn)
            continue

        log.info("")
        log.info("--- %s ---", freq_cn)
        log.info("  数据量: %d 条 (%s ~ %s)", len(df),
                 df['trade_date'].iloc[0], df['trade_date'].iloc[-1])

        # 全量信号检测 → 按时段分组统计
        period_results = analyze_by_period(df, freq, ts_code)

        for label, pr in sorted(period_results.items()):
            log.info("  %s: %d 条信号", label, pr["count"])

        all_freq_results[freq] = period_results

    # 生成报告
    output_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'report',
        '05-time-stability-rsi-layer3.5.md'
    ))
    generate_stability_report(all_freq_results, output_path, ts_code, index_name)

    elapsed = round(time.time() - start_time, 1)
    log.info("")
    log.info("=" * 60)
    log.info("Layer 3.5 完成 | 总耗时: %.1fs", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
