"""个股 RSI 信号统计分析 (Phase 2)

对每只股票在每个 freq×adj 组合下:
1. 从 stock_rsi 读取已计算的 RSI 数据
2. 用 detect_all_signals_fast 在内存中检测 18 种信号
3. 计算信号后续收益率和胜率
4. 汇总写入 stock_rsi_signal_stats 表
5. 完成后生成 Layer 4 分析报告

信号不写明细表（省百万行 I/O），只写统计汇总。

用法:
  python rsi/research/analyze_stock_rsi.py                          # 全市场
  python rsi/research/analyze_stock_rsi.py --codes "300750.SZ"      # 指定股票
  python rsi/research/analyze_stock_rsi.py --freq daily --adj qfq   # 指定组合
  python rsi/research/analyze_stock_rsi.py --report-only            # 仅生成报告
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.logger import get_logger
from database import write_engine
from signal_detector_rsi import detect_all_signals_fast
from models import StockRsiSignalStats, SIGNAL_NAME_MAP
from db_utils import batch_upsert

log = get_logger("research.analyze_stock_rsi")

FREQS = ("daily", "weekly", "monthly", "yearly")
ADJS = ("bfq", "qfq", "hfq")

# 各周期的主评估窗口（K线根数）
MAIN_HORIZON = {
    "daily": 20,    # T+20 ≈ 1月
    "weekly": 4,    # T+4 ≈ 1月
    "monthly": 3,   # T+3 ≈ 1季
    "yearly": 1,    # T+1 ≈ 1年
}


# ── 数据加载 ──────────────────────────────────────────────────

def load_stock_rsi(ts_code: str, freq: str, adj: str, conn) -> pd.DataFrame:
    """从 stock_rsi 库读取已计算的个股 RSI 数据"""
    table = f"stock_rsi_{freq}_{adj}"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, "
        f"rsi_6, rsi_12, rsi_14, rsi_24 "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


# ── 收益率计算 ────────────────────────────────────────────────

def _calc_return(close_arr, idx: int, horizon: int):
    """计算第 idx 根K线后 horizon 根的收益率(%)

    使用 numpy 数组访问，避免 df.iloc 开销
    """
    target = idx + horizon
    if target >= len(close_arr):
        return None
    base = float(close_arr[idx])
    future = float(close_arr[target])
    if base == 0:
        return None
    return round((future - base) / base * 100, 2)


# ── 单只股票分析 ──────────────────────────────────────────────

def analyze_one(ts_code: str, freq: str, adj: str, conn) -> dict | None:
    """分析单只股票单个 freq×adj 组合，返回一行统计数据

    流程:
    1. 从 stock_rsi_{freq}_{adj} 读取 RSI 数据
    2. detect_all_signals_fast 检测 18 种信号（轻量模式）
    3. 按 signal_name 分组计算信号后续收益率和胜率
    4. 返回统计字典（直接对应 StockRsiSignalStats 列）
    """
    df = load_stock_rsi(ts_code, freq, adj, conn)
    if df.empty or len(df) < 30:
        return None

    horizon = MAIN_HORIZON[freq]
    close_arr = df["close"].values

    # 检测所有信号（轻量模式: 含 idx, signal_type, signal_name, direction）
    signals = detect_all_signals_fast(df, freq=freq)

    # 按 signal_name 分组
    sig_by_name = {}
    for s in signals:
        name = s["signal_name"]
        if name not in sig_by_name:
            sig_by_name[name] = []
        sig_by_name[name].append(s["idx"])

    # 构建结果
    result = {
        "ts_code": ts_code,
        "freq": freq,
        "adj": adj,
        "kline_count": len(df),
        "date_start": str(df["trade_date"].iloc[0]),
        "date_end": str(df["trade_date"].iloc[-1]),
    }

    # 对每种信号计算 cnt / avg_ret / win_rate
    for short, full_name, is_buy in SIGNAL_NAME_MAP:
        indices = sig_by_name.get(full_name, [])
        cnt = len(indices)
        result[f"{short}_cnt"] = cnt

        if cnt == 0:
            result[f"{short}_avg_ret"] = None
            result[f"{short}_win_rate"] = None
            continue

        # 计算收益率
        rets = []
        for idx in indices:
            r = _calc_return(close_arr, idx, horizon)
            if r is not None:
                rets.append(r)

        if not rets:
            result[f"{short}_avg_ret"] = None
            result[f"{short}_win_rate"] = None
            continue

        result[f"{short}_avg_ret"] = round(np.mean(rets), 2)
        if is_buy:
            # 买入信号: 后续上涨为胜
            result[f"{short}_win_rate"] = round(
                sum(1 for r in rets if r > 0) / len(rets) * 100, 1)
        else:
            # 卖出信号: 后续下跌为胜
            result[f"{short}_win_rate"] = round(
                sum(1 for r in rets if r < 0) / len(rets) * 100, 1)

    return result


# ── 批量分析 ──────────────────────────────────────────────────

def analyze_stocks(ts_codes: list[str], freqs: tuple = FREQS, adjs: tuple = ADJS):
    """批量分析所有股票的 RSI 信号统计"""
    total = len(ts_codes)
    combos = len(freqs) * len(adjs)
    log.info(f"[analyze] 开始 | 股票: {total} | 组合: {combos}/只 | 总计: {total * combos}")

    start = time.time()
    results = []
    processed = 0

    with write_engine.connect() as conn:
        for i, ts_code in enumerate(ts_codes, 1):
            for freq in freqs:
                for adj in adjs:
                    try:
                        row = analyze_one(ts_code, freq, adj, conn)
                        if row:
                            results.append(row)
                            processed += 1
                    except Exception as e:
                        log.error(f"[analyze] 失败 | {ts_code}/{freq}/{adj} | {e}")

            # 每10只股票打印进度 + 写入数据库（避免内存积压）
            if i % 10 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[analyze] 进度: {i}/{total} | 产出: {processed}行 | 耗时: {elapsed}s")
                if results:
                    batch_upsert(StockRsiSignalStats, results,
                                 unique_keys=["ts_code", "freq", "adj"])
                    results = []

    # 剩余写入
    if results:
        batch_upsert(StockRsiSignalStats, results, unique_keys=["ts_code", "freq", "adj"])

    elapsed = round(time.time() - start, 1)
    log.info(f"[analyze] 完成 | 产出: {processed}行 | 总耗时: {elapsed}s")


# ── 报告生成 ──────────────────────────────────────────────────

def _load_stats_df() -> pd.DataFrame:
    """从 stock_rsi_signal_stats 表读取全量统计数据"""
    with write_engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM stock_rsi_signal_stats"))
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def _load_industry_map() -> dict:
    """从 my_stock.stock_basic 读取 ts_code → industry 映射"""
    from database import read_engine
    with read_engine.connect() as conn:
        result = conn.execute(text(
            "SELECT ts_code, industry FROM stock_basic WHERE list_status='L'"
        ))
        return {row[0]: row[1] for row in result.fetchall()}


def generate_report():
    """生成 Layer 4 分析报告"""
    log.info("[report] 开始生成 Layer 4 报告")

    df = _load_stats_df()
    if df.empty:
        log.error("[report] stock_rsi_signal_stats 表为空，无法生成报告")
        return

    industry_map = _load_industry_map()
    df["industry"] = df["ts_code"].map(industry_map)

    lines = []
    lines.append("# RSI Layer 4: 全市场个股验证报告\n")
    lines.append(f"**统计行数**: {len(df)} | **涉及股票**: {df['ts_code'].nunique()} 只\n")
    lines.append(f"**频率**: {', '.join(sorted(df['freq'].unique()))} | "
                 f"**复权**: {', '.join(sorted(df['adj'].unique()))}\n")

    # ── 1. 全市场信号分布 ──────────────────────────────────
    lines.append("\n## 1. 全市场信号分布\n")
    lines.append("18种RSI信号在个股层面的平均触发频率：\n")
    lines.append("| 信号 | 方向 | 平均触发次数 | 中位数 | 最大值 | 触发股数占比 |")
    lines.append("|------|------|-------------|--------|--------|-------------|")

    # 用 daily+qfq 作为代表频率
    daily_qfq = df[(df["freq"] == "daily") & (df["adj"] == "qfq")]
    if daily_qfq.empty:
        daily_qfq = df[df["freq"] == "daily"]

    for short, full_name, is_buy in SIGNAL_NAME_MAP:
        col = f"{short}_cnt"
        if col not in daily_qfq.columns:
            continue
        vals = daily_qfq[col].dropna()
        if vals.empty:
            continue
        direction = "买入" if is_buy else "卖出"
        mean_v = round(vals.mean(), 1)
        median_v = round(vals.median(), 1)
        max_v = int(vals.max())
        has_signal_pct = round((vals > 0).sum() / len(vals) * 100, 1)
        lines.append(f"| {full_name} | {direction} | {mean_v} | {median_v} | "
                     f"{max_v} | {has_signal_pct}% |")

    # ── 2. 信号有效性验证 ──────────────────────────────────
    lines.append("\n## 2. 信号有效性（日线+前复权）\n")
    lines.append("与 L1-3 指数级结论对比，看个股层面是否一致：\n")
    lines.append("| 信号 | 方向 | 平均收益率(%) | 中位胜率(%) | Q25胜率 | Q75胜率 | L1-3结论 |")
    lines.append("|------|------|-------------|-----------|---------|---------|---------|")

    # L1-3 关键结论参考（手动注入核心结论作为对比基准）
    l13_ref = {
        "rsi14_oversold": "牛市86.8%",
        "rsi14_overbought": "熊市71.1%",
        "rsi14_strong_oversold": "77.8%",
        "rsi14_strong_overbought": "极强",
        "rsi14_bull_divergence": "T+5 100%",
        "rsi14_bear_divergence": "熊市70.6%",
        "rsi14_bull_failure_swing": "牛市80%",
        "rsi14_bear_failure_swing": "熊市74.3%",
        "rsi14_cross_above_50": "牛市76.4%",
        "rsi14_cross_below_50": "~50%",
    }

    for short, full_name, is_buy in SIGNAL_NAME_MAP:
        ret_col = f"{short}_avg_ret"
        wr_col = f"{short}_win_rate"
        if ret_col not in daily_qfq.columns:
            continue
        rets = daily_qfq[ret_col].dropna()
        wrs = daily_qfq[wr_col].dropna()
        if rets.empty:
            continue
        direction = "买入" if is_buy else "卖出"
        avg_ret = round(rets.mean(), 2)
        med_wr = round(wrs.median(), 1) if not wrs.empty else "-"
        q25_wr = round(wrs.quantile(0.25), 1) if not wrs.empty else "-"
        q75_wr = round(wrs.quantile(0.75), 1) if not wrs.empty else "-"
        ref = l13_ref.get(full_name, "-")
        lines.append(f"| {full_name} | {direction} | {avg_ret} | {med_wr} | "
                     f"{q25_wr} | {q75_wr} | {ref} |")

    # ── 3. 频率对比 ──────────────────────────────────────
    lines.append("\n## 3. 频率对比\n")
    lines.append("日/周/月/年线信号的稳定性对比（前复权，核心信号）：\n")
    lines.append("| 频率 | RSI14超卖胜率 | RSI14超买胜率 | RSI14底背离胜率 | RSI14上穿50胜率 |")
    lines.append("|------|-------------|-------------|--------------|--------------|")

    qfq_df = df[df["adj"] == "qfq"]
    for freq in FREQS:
        freq_df = qfq_df[qfq_df["freq"] == freq]
        if freq_df.empty:
            continue
        os_wr = freq_df["rsi14_os_win_rate"].dropna()
        ob_wr = freq_df["rsi14_ob_win_rate"].dropna()
        div_wr = freq_df["r14_bull_div_win_rate"].dropna()
        xup_wr = freq_df["r14_xup50_win_rate"].dropna()

        os_med = f"{round(os_wr.median(), 1)}%" if not os_wr.empty else "-"
        ob_med = f"{round(ob_wr.median(), 1)}%" if not ob_wr.empty else "-"
        div_med = f"{round(div_wr.median(), 1)}%" if not div_wr.empty else "-"
        xup_med = f"{round(xup_wr.median(), 1)}%" if not xup_wr.empty else "-"
        lines.append(f"| {freq} | {os_med} | {ob_med} | {div_med} | {xup_med} |")

    # ── 4. 行业分组 ──────────────────────────────────────
    lines.append("\n## 4. 行业差异（日线+前复权）\n")
    lines.append("RSI14超卖信号按行业的胜率差异：\n")
    lines.append("| 行业 | 股票数 | RSI14超卖中位胜率 | RSI14超买中位胜率 | 判定 |")
    lines.append("|------|--------|-----------------|-----------------|------|")

    if not daily_qfq.empty and "industry" in daily_qfq.columns:
        ind_groups = daily_qfq.groupby("industry")
        ind_stats = []
        for ind, group in ind_groups:
            if pd.isna(ind) or len(group) < 5:
                continue
            os_wr = group["rsi14_os_win_rate"].dropna()
            ob_wr = group["rsi14_ob_win_rate"].dropna()
            os_med = round(os_wr.median(), 1) if not os_wr.empty else None
            ob_med = round(ob_wr.median(), 1) if not ob_wr.empty else None
            ind_stats.append({
                "industry": ind,
                "count": len(group),
                "os_med": os_med,
                "ob_med": ob_med,
            })

        # 按超卖胜率排序
        ind_stats.sort(key=lambda x: x["os_med"] or 0, reverse=True)
        for s in ind_stats:
            os_str = f"{s['os_med']}%" if s["os_med"] is not None else "-"
            ob_str = f"{s['ob_med']}%" if s["ob_med"] is not None else "-"
            # 判定
            if s["os_med"] and s["os_med"] >= 60:
                judge = "RSI效果好"
            elif s["os_med"] and s["os_med"] <= 40:
                judge = "RSI效果差"
            else:
                judge = "一般"
            lines.append(f"| {s['industry']} | {s['count']} | {os_str} | {ob_str} | {judge} |")

    # ── 5. 复权对比 ──────────────────────────────────────
    lines.append("\n## 5. 复权类型对比\n")
    lines.append("不同复权方式对RSI信号有效性的影响（日线）：\n")
    lines.append("| 复权 | RSI14超卖中位胜率 | RSI14超买中位胜率 | RSI14底背离中位胜率 |")
    lines.append("|------|-----------------|-----------------|-------------------|")

    daily_df = df[df["freq"] == "daily"]
    for adj in ADJS:
        adj_df = daily_df[daily_df["adj"] == adj]
        if adj_df.empty:
            continue
        os_wr = adj_df["rsi14_os_win_rate"].dropna()
        ob_wr = adj_df["rsi14_ob_win_rate"].dropna()
        div_wr = adj_df["r14_bull_div_win_rate"].dropna()
        adj_label = {"bfq": "不复权", "qfq": "前复权", "hfq": "后复权"}[adj]
        os_med = f"{round(os_wr.median(), 1)}%" if not os_wr.empty else "-"
        ob_med = f"{round(ob_wr.median(), 1)}%" if not ob_wr.empty else "-"
        div_med = f"{round(div_wr.median(), 1)}%" if not div_wr.empty else "-"
        lines.append(f"| {adj_label}({adj}) | {os_med} | {ob_med} | {div_med} |")

    # ── 6. 异常股票清单 ──────────────────────────────────
    lines.append("\n## 6. 异常股票清单（日线+前复权）\n")

    if not daily_qfq.empty:
        # RSI信号异常有效的股票（超卖胜率 > 80%）
        lines.append("### RSI超卖信号异常有效（胜率>80%）\n")
        high_os = daily_qfq[daily_qfq["rsi14_os_win_rate"] > 80].sort_values(
            "rsi14_os_win_rate", ascending=False).head(20)
        if not high_os.empty:
            lines.append("| 股票 | 行业 | 超卖次数 | 胜率(%) | 平均收益(%) |")
            lines.append("|------|------|---------|---------|-----------|")
            for _, row in high_os.iterrows():
                ind = row.get("industry", "-") or "-"
                lines.append(f"| {row['ts_code']} | {ind} | {int(row['rsi14_os_cnt'])} | "
                             f"{row['rsi14_os_win_rate']} | {row['rsi14_os_avg_ret']} |")
        else:
            lines.append("无\n")

        # RSI信号异常失效的股票（超卖胜率 < 30%）
        lines.append("\n### RSI超卖信号严重失效（胜率<30%）\n")
        low_os = daily_qfq[
            (daily_qfq["rsi14_os_win_rate"] < 30) & (daily_qfq["rsi14_os_cnt"] >= 3)
        ].sort_values("rsi14_os_win_rate").head(20)
        if not low_os.empty:
            lines.append("| 股票 | 行业 | 超卖次数 | 胜率(%) | 平均收益(%) |")
            lines.append("|------|------|---------|---------|-----------|")
            for _, row in low_os.iterrows():
                ind = row.get("industry", "-") or "-"
                lines.append(f"| {row['ts_code']} | {ind} | {int(row['rsi14_os_cnt'])} | "
                             f"{row['rsi14_os_win_rate']} | {row['rsi14_os_avg_ret']} |")
        else:
            lines.append("无\n")

    # ── 7. 核心结论 ──────────────────────────────────────
    lines.append("\n## 7. 核心结论\n")

    # 自动提取核心统计
    if not daily_qfq.empty:
        os_wr_all = daily_qfq["rsi14_os_win_rate"].dropna()
        ob_wr_all = daily_qfq["rsi14_ob_win_rate"].dropna()
        div_wr_all = daily_qfq["r14_bull_div_win_rate"].dropna()
        xup_wr_all = daily_qfq["r14_xup50_win_rate"].dropna()

        lines.append(f"1. **RSI14超卖**: 个股中位胜率 {round(os_wr_all.median(), 1)}%，"
                     f"与L1-3指数级86.8%(牛市)的差距说明个股噪声更大")
        lines.append(f"2. **RSI14超买**: 个股中位胜率 {round(ob_wr_all.median(), 1)}%，"
                     f"对比L1-3指数级71.1%(熊市)")
        if not div_wr_all.empty:
            lines.append(f"3. **底背离**: 个股中位胜率 {round(div_wr_all.median(), 1)}%，"
                         f"背离信号在个股上的有效性")
        if not xup_wr_all.empty:
            lines.append(f"4. **上穿50**: 个股中位胜率 {round(xup_wr_all.median(), 1)}%，"
                         f"趋势确认信号在个股的表现")

        lines.append(f"\n**总计分析**: {df['ts_code'].nunique()} 只股票 × "
                     f"{len(FREQS)} 频率 × {len(ADJS)} 复权 = {len(df)} 行统计数据")

    lines.append("\n---\n")
    lines.append("*报告由 analyze_stock_rsi.py 自动生成*\n")

    # 写入报告文件
    report_path = os.path.join(os.path.dirname(__file__), '..', 'report',
                               '06-stock-rsi-layer4.md')
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    log.info(f"[report] Layer 4 报告已生成: {os.path.abspath(report_path)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股 RSI 信号统计分析")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔")
    parser.add_argument("--freq", default=None, help="指定周期，逗号分隔 (如: daily,weekly)")
    parser.add_argument("--adj", default=None, help="指定复权，逗号分隔 (如: bfq,qfq)")
    parser.add_argument("--report-only", action="store_true", help="仅生成报告（不重新分析）")
    args = parser.parse_args()

    # 建表
    from database import init_rsi_tables
    init_rsi_tables()

    if args.report_only:
        generate_report()
    else:
        if args.codes:
            codes = args.codes.split(",")
        else:
            from kline_loader import get_all_stock_codes
            codes = get_all_stock_codes()

        freqs = tuple(args.freq.split(",")) if args.freq else FREQS
        adjs = tuple(args.adj.split(",")) if args.adj else ADJS

        analyze_stocks(codes, freqs=freqs, adjs=adjs)
        generate_report()
