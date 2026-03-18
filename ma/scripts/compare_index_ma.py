"""三大指数 MA 信号对比分析

从 index_ma_signal 表读取上证/深证/创业板的信号数据，
交叉对比各信号在不同指数上的表现差异，生成对比报告。

核心问题:
1. 哪些 MA 信号在三大指数上普遍有效（普适性结论）？
2. 哪些信号只在部分指数上有效（条件性结论）？
3. 创业板 vs 主板有哪些显著差异？

用法:
  python research/ma/scripts/compare_index_ma.py                # 默认日线（唯一有收益数据的周期）
  python research/ma/scripts/compare_index_ma.py --freq daily
"""
import argparse
import os
import sys
from datetime import datetime
from collections import defaultdict

import numpy as np
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from database import write_engine
from analyze_index_ma import SIGNAL_NAMES, SIGNAL_TYPE_NAMES, BUY_SIGNALS
from app.logger import get_logger

log = get_logger(__name__)

# ── 三大指数 ────────────────────────────────────────────────────
INDICES = [
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
]

# ── 各周期的主收益列和标签 ────────────────────────────────────────
# 注意: 由于 analyze_index_ma.py 的 save_signals 使用固定列名 ret_5/10/20/60，
# 而 weekly/monthly 的实际 horizon 值不匹配这些列名（weekly 用 [2,4,8,13]，monthly 用 [1,3,6,12]），
# 导致 weekly/monthly 的收益列在数据库中全为 NULL。
# 因此目前仅 daily 频率的收益数据可用。
FREQ_CONFIG = {
    "daily": {
        "ret_col": "ret_60",       # T+60 (3个月)
        "ret_label": "T+60(3月)",
        "all_ret_cols": ["ret_5", "ret_10", "ret_20", "ret_60"],
        "ret_labels": {"ret_5": "T+5(1周)", "ret_10": "T+10(2周)",
                       "ret_20": "T+20(1月)", "ret_60": "T+60(3月)"},
    },
    "weekly": {
        "ret_col": "ret_60",       # 实际为 NULL（已知 bug）
        "ret_label": "T+13(1季)",
        "all_ret_cols": ["ret_5", "ret_10", "ret_20", "ret_60"],
        "ret_labels": {"ret_5": "T+2(2周)", "ret_10": "T+4(1月)",
                       "ret_20": "T+8(2月)", "ret_60": "T+13(1季)"},
    },
    "monthly": {
        "ret_col": "ret_60",       # 实际为 NULL（已知 bug）
        "ret_label": "T+12(1年)",
        "all_ret_cols": ["ret_5", "ret_10", "ret_20", "ret_60"],
        "ret_labels": {"ret_5": "T+1(1月)", "ret_10": "T+3(1季)",
                       "ret_20": "T+6(半年)", "ret_60": "T+12(1年)"},
    },
}

FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

# ── 一致性判断阈值 ─────────────────────────────────────────────
WIN_RATE_DIFF_THRESHOLD = 10.0  # 胜率差异 < 10% 视为一致


# ── 数据加载 ──────────────────────────────────────────────────

def load_signals(ts_code: str, freq: str) -> list[dict]:
    """从 index_ma_signal 加载指定指数/周期的信号"""
    sql = text(
        "SELECT signal_type, signal_name, direction, signal_value, close, "
        "ret_5, ret_10, ret_20, ret_60, trend, phase_id, phase_label "
        "FROM index_ma_signal "
        "WHERE ts_code = :ts_code AND freq = :freq ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code, "freq": freq})
        rows = result.fetchall()
        columns = result.keys()
    return [dict(zip(columns, row)) for row in rows]


# ── 统计计算 ──────────────────────────────────────────────────

def _is_buy_signal(signal_name: str) -> bool:
    """判断信号方向: True=买入信号, False=卖出信号"""
    return signal_name in BUY_SIGNALS


def calc_stats_by_signal(signals: list[dict], ret_col: str, trend_filter: str = None) -> dict:
    """按 signal_name 统计: 数量、胜率、平均收益

    Args:
        signals: 信号列表
        ret_col: 用于统计的收益列名 (如 "ret_60")
        trend_filter: 可选，"bull" / "bear" / None(全量)

    Returns:
        {signal_name: {"count": N, "win_rate": X, "avg_ret": Y, "direction": "buy/sell"}}
    """
    # 按 trend 过滤
    if trend_filter:
        signals = [s for s in signals if s.get("trend") == trend_filter]

    groups = defaultdict(list)
    for sig in signals:
        groups[sig["signal_name"]].append(sig)

    stats = {}
    for sig_name, group in groups.items():
        is_buy = _is_buy_signal(sig_name)
        valid_rets = [s[ret_col] for s in group if s.get(ret_col) is not None]

        stat = {
            "count": len(group),
            "signal_type": group[0]["signal_type"],
            "direction": "buy" if is_buy else "sell",
            "valid_count": len(valid_rets),
        }

        if valid_rets:
            stat["avg_ret"] = round(np.mean(valid_rets), 2)
            if is_buy:
                stat["win_rate"] = round(
                    sum(1 for v in valid_rets if v > 0) / len(valid_rets) * 100, 1
                )
            else:
                stat["win_rate"] = round(
                    sum(1 for v in valid_rets if v < 0) / len(valid_rets) * 100, 1
                )
        else:
            stat["avg_ret"] = None
            stat["win_rate"] = None

        stats[sig_name] = stat

    return stats


def compare_signal_across_indices(
    all_data: dict, ret_col: str, trend_filter: str = None
) -> list[dict]:
    """对比同一信号在三大指数上的表现

    Args:
        all_data: {ts_code: signals_list}
        ret_col: 收益列名
        trend_filter: "bull" / "bear" / None

    Returns:
        比较结果列表，每行是一个信号在三个指数上的统计
    """
    # 逐指数计算统计
    index_stats = {}
    for ts_code, signals in all_data.items():
        index_stats[ts_code] = calc_stats_by_signal(signals, ret_col, trend_filter)

    # 收集所有出现过的信号名
    all_sig_names = set()
    for stats in index_stats.values():
        all_sig_names.update(stats.keys())

    rows = []
    for sig_name in sorted(all_sig_names):
        row = {
            "signal_name": sig_name,
            "signal_name_cn": SIGNAL_NAMES.get(sig_name, sig_name),
            "signal_type": None,
            "direction": None,
        }

        win_rates = []
        for ts_code, idx_name in INDICES:
            stat = index_stats.get(ts_code, {}).get(sig_name)
            if stat:
                row["signal_type"] = row["signal_type"] or stat["signal_type"]
                row["direction"] = row["direction"] or stat["direction"]
                row[f"{ts_code}_count"] = stat["count"]
                row[f"{ts_code}_win_rate"] = stat["win_rate"]
                row[f"{ts_code}_avg_ret"] = stat["avg_ret"]
                if stat["win_rate"] is not None:
                    win_rates.append(stat["win_rate"])
            else:
                row[f"{ts_code}_count"] = 0
                row[f"{ts_code}_win_rate"] = None
                row[f"{ts_code}_avg_ret"] = None

        # 判断一致性: 所有有数据的指数胜率差异 < 阈值
        if len(win_rates) >= 2:
            max_diff = max(win_rates) - min(win_rates)
            row["consistency"] = "universal" if max_diff < WIN_RATE_DIFF_THRESHOLD else "conditional"
            row["win_rate_spread"] = round(max_diff, 1)
        elif len(win_rates) == 1:
            row["consistency"] = "single"  # 仅一个指数有数据
            row["win_rate_spread"] = None
        else:
            row["consistency"] = "no_data"
            row["win_rate_spread"] = None

        rows.append(row)

    return rows



# ── 主函数 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="三大指数 MA 信号对比分析")
    parser.add_argument("--freq", default="daily", choices=["daily", "weekly", "monthly"],
                        help="分析周期 (默认 daily，因 weekly/monthly 收益列为空)")
    args = parser.parse_args()

    freq = args.freq
    freq_cn = FREQ_NAMES[freq]

    log.info("=" * 60)
    log.info("三大指数 MA 信号对比分析")
    log.info("周期: %s (%s)", freq, freq_cn)
    log.info("开始: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # 加载三大指数信号
    all_data = {}
    for ts_code, idx_name in INDICES:
        signals = load_signals(ts_code, freq)
        log.info("  %s (%s): %d 条信号", idx_name, ts_code, len(signals))
        all_data[ts_code] = signals

    total = sum(len(v) for v in all_data.values())
    log.info("信号总计: %d 条", total)

    if total == 0:
        log.warning("无信号数据，请先运行 analyze_index_ma.py --save-signals")
        return

    # 检查收益数据可用性
    ret_col = FREQ_CONFIG[freq]["ret_col"]
    has_ret_data = False
    for signals in all_data.values():
        for sig in signals:
            if sig.get(ret_col) is not None:
                has_ret_data = True
                break
        if has_ret_data:
            break

    if not has_ret_data:
        log.warning("%s 周期的收益列 %s 全为空，对比结果将不包含胜率和收益数据", freq_cn, ret_col)
        log.warning("这是已知问题: weekly/monthly 的 horizon 值与 DB 列名不匹配")

    # 计算三组对比: 全量 / 牛市 / 熊市
    log.info("计算全量对比...")
    comparison_all = compare_signal_across_indices(all_data, ret_col, trend_filter=None)
    log.info("计算牛市对比...")
    comparison_bull = compare_signal_across_indices(all_data, ret_col, trend_filter="bull")
    log.info("计算熊市对比...")
    comparison_bear = compare_signal_across_indices(all_data, ret_col, trend_filter="bear")

    # 输出统计摘要到日志
    universal_all = [r for r in comparison_all if r["consistency"] == "universal"]
    conditional_all = [r for r in comparison_all if r["consistency"] == "conditional"]
    log.info("全量: %d 个普适信号, %d 个条件性信号", len(universal_all), len(conditional_all))

    universal_bull = [r for r in comparison_bull if r["consistency"] == "universal"]
    universal_bear = [r for r in comparison_bear if r["consistency"] == "universal"]
    log.info("牛市: %d 个普适信号 | 熊市: %d 个普适信号", len(universal_bull), len(universal_bear))

    log.info("")
    log.info("=" * 60)
    log.info("对比分析完成")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
