"""L1 前置：上证指数换手率探索性分析

分析内容:
1. turnover_rate vs turnover_rate_f 对比，选择主力字段
2. 整体分布特征（均值/中位数/标准差/各分位数）
3. 牛熊分组统计（牛市均值 vs 熊市均值）
4. 牛熊子类型统计
5. 周线/月线聚合方式验证（均值 vs 求和）
6. 滚动窗口选择（120日 vs 250日滚动分位数稳定性）
7. 确定极端值阈值和突变倍数

用法:
  python turnover/research/explore_turnover.py
  python turnover/research/explore_turnover.py --ts_code 399001.SZ --name 深证成指
"""
import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import read_engine
from bull_bear_phases import get_phase, tag_trend, SH_PHASES
from app.logger import get_logger

log = get_logger(__name__)

TS_CODE = "000001.SH"
INDEX_NAME = "上证指数"


# ── 数据加载 ──────────────────────────────────────────────────

def load_index_turnover(ts_code: str) -> pd.DataFrame:
    """从 my_stock.index_dailybasic 加载指数换手率数据"""
    sql = text(
        "SELECT trade_date, turnover_rate, turnover_rate_f "
        "FROM index_dailybasic "
        "WHERE ts_code = :code "
        "ORDER BY trade_date"
    )
    with read_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})
    log.info(f"加载 {ts_code} 换手率数据: {len(df)} 条, {df['trade_date'].iloc[0]}~{df['trade_date'].iloc[-1]}")
    return df


def load_index_close(ts_code: str) -> pd.DataFrame:
    """从 my_stock.index_daily 加载指数收盘价（用于周线/月线聚合验证）"""
    sql = text(
        "SELECT trade_date, close, pct_chg "
        "FROM index_daily "
        "WHERE ts_code = :code "
        "ORDER BY trade_date"
    )
    with read_engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})
    return df


# ── 分析函数 ──────────────────────────────────────────────────

def analyze_distribution(df: pd.DataFrame, col: str, label: str):
    """分析单列分布特征"""
    s = df[col].dropna()
    print(f"\n{'='*60}")
    print(f" {label} 分布特征 ({col})")
    print(f"{'='*60}")
    print(f"  数据量:   {len(s)}")
    print(f"  均值:     {s.mean():.4f}")
    print(f"  中位数:   {s.median():.4f}")
    print(f"  标准差:   {s.std():.4f}")
    print(f"  偏度:     {s.skew():.4f}")
    print(f"  峰度:     {s.kurtosis():.4f}")
    print(f"  最小值:   {s.min():.4f}")
    print(f"  最大值:   {s.max():.4f}")
    print(f"\n  分位数:")
    for q in [1, 5, 10, 20, 25, 50, 75, 80, 90, 95, 99]:
        val = np.percentile(s, q)
        print(f"    {q:3d}%:  {val:.4f}")
    return s


def analyze_bull_bear(df: pd.DataFrame, col: str, label: str):
    """按牛熊阶段分组统计换手率"""
    # 标注牛熊
    df = df.copy()
    df["trend"] = df["trade_date"].apply(tag_trend)
    df["phase_info"] = df["trade_date"].apply(get_phase)
    df["phase_label"] = df["phase_info"].apply(lambda x: x["label"] if x else "unknown")
    df["phase_sub"] = df["phase_info"].apply(
        lambda x: f"{x['trend']}_{x['label']}" if x else "unknown"
    )

    print(f"\n{'='*60}")
    print(f" {label} 牛熊分组统计 ({col})")
    print(f"{'='*60}")

    # 牛/熊总体
    for trend in ["bull", "bear"]:
        sub = df[df["trend"] == trend][col].dropna()
        if len(sub) == 0:
            continue
        print(f"\n  {trend.upper()} ({len(sub)}天):")
        print(f"    均值:   {sub.mean():.4f}")
        print(f"    中位数: {sub.median():.4f}")
        print(f"    5%:     {np.percentile(sub, 5):.4f}")
        print(f"    95%:    {np.percentile(sub, 95):.4f}")

    # 各阶段明细
    print(f"\n  各牛熊阶段明细:")
    print(f"  {'阶段':<20s} {'趋势':<6s} {'天数':>5s} {'均值':>8s} {'中位数':>8s} {'5%':>8s} {'95%':>8s}")
    print(f"  {'-'*70}")

    # 只统计2013年后的阶段（数据范围内）
    for phase in SH_PHASES:
        if phase["start_ym"] < "201310":
            continue
        mask = df["phase_label"] == phase["label"]
        sub = df[mask][col].dropna()
        if len(sub) == 0:
            continue
        trend_str = "牛" if phase["trend"] == "bull" else "熊"
        print(f"  {phase['label']:<18s} {trend_str:<6s} {len(sub):>5d} "
              f"{sub.mean():>8.4f} {sub.median():>8.4f} "
              f"{np.percentile(sub, 5):>8.4f} {np.percentile(sub, 95):>8.4f}")

    return df


def compare_fields(df: pd.DataFrame):
    """对比 turnover_rate 和 turnover_rate_f"""
    print(f"\n{'='*60}")
    print(f" turnover_rate vs turnover_rate_f 对比")
    print(f"{'='*60}")

    tr = df["turnover_rate"].dropna()
    trf = df["turnover_rate_f"].dropna()

    print(f"\n  {'指标':<20s} {'turnover_rate':>15s} {'turnover_rate_f':>15s} {'倍数':>8s}")
    print(f"  {'-'*60}")
    for name, func_name in [("均值", "mean"), ("中位数", "median"), ("标准差", "std"),
                             ("最小值", "min"), ("最大值", "max")]:
        v1 = getattr(tr, func_name)()
        v2 = getattr(trf, func_name)()
        ratio = v2 / v1 if v1 > 0 else 0
        print(f"  {name:<20s} {v1:>15.4f} {v2:>15.4f} {ratio:>8.2f}x")

    # 相关性
    corr = df[["turnover_rate", "turnover_rate_f"]].corr().iloc[0, 1]
    print(f"\n  相关系数: {corr:.6f}")
    print(f"  结论: turnover_rate_f ≈ turnover_rate × {(trf.mean()/tr.mean()):.1f}，"
          f"两者高度相关(r={corr:.4f})，选 turnover_rate_f（自由流通更能反映真实交易热度）")


def analyze_rolling_windows(df: pd.DataFrame, col: str):
    """对比不同滚动窗口的分位数稳定性"""
    print(f"\n{'='*60}")
    print(f" 滚动窗口对比 ({col})")
    print(f"{'='*60}")

    s = df[col].dropna()
    for window in [60, 120, 250]:
        q95 = s.rolling(window, min_periods=window).quantile(0.95)
        q05 = s.rolling(window, min_periods=window).quantile(0.05)
        valid = q95.dropna()
        if len(valid) == 0:
            continue
        # 阈值变异系数（越小越稳定）
        cv95 = q95.std() / q95.mean() if q95.mean() > 0 else 0
        cv05 = q05.std() / q05.mean() if q05.mean() > 0 else 0
        print(f"\n  窗口 {window} 日:")
        print(f"    95th 分位 — 均值: {q95.mean():.4f}, 标准差: {q95.std():.4f}, 变异系数: {cv95:.4f}")
        print(f"     5th 分位 — 均值: {q05.mean():.4f}, 标准差: {q05.std():.4f}, 变异系数: {cv05:.4f}")
        print(f"    有效数据点: {len(valid)}")


def analyze_weekly_monthly_aggregation(df_turnover: pd.DataFrame, df_close: pd.DataFrame, col: str):
    """验证周线/月线聚合方式"""
    print(f"\n{'='*60}")
    print(f" 周线/月线聚合方式验证 ({col})")
    print(f"{'='*60}")

    # 合并换手率和收盘价
    df = pd.merge(df_turnover[["trade_date", col]], df_close[["trade_date", "close"]], on="trade_date", how="inner")
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    # 周线聚合
    df["week"] = df["trade_date_dt"].dt.isocalendar().year.astype(str) + "-W" + df["trade_date_dt"].dt.isocalendar().week.astype(str).str.zfill(2)
    weekly_mean = df.groupby("week")[col].mean()
    weekly_sum = df.groupby("week")[col].sum()

    print(f"\n  周线聚合（{len(weekly_mean)} 周）:")
    print(f"    均值法 — 均值: {weekly_mean.mean():.4f}, 中位数: {weekly_mean.median():.4f}")
    print(f"    求和法 — 均值: {weekly_sum.mean():.4f}, 中位数: {weekly_sum.median():.4f}")
    print(f"    均值法保持量级一致（与日线可比），求和法反映周内总交易量")
    print(f"    建议: 用均值法（与日线量级一致，阈值可复用）")

    # 月线聚合
    df["month"] = df["trade_date"].str[:6]
    monthly_mean = df.groupby("month")[col].mean()
    monthly_sum = df.groupby("month")[col].sum()

    print(f"\n  月线聚合（{len(monthly_mean)} 月）:")
    print(f"    均值法 — 均值: {monthly_mean.mean():.4f}, 中位数: {monthly_mean.median():.4f}")
    print(f"    求和法 — 均值: {monthly_sum.mean():.4f}, 中位数: {monthly_sum.median():.4f}")
    print(f"    建议: 同上，用均值法")


def analyze_surge_thresholds(df: pd.DataFrame, col: str):
    """分析突变信号的合理倍数阈值"""
    print(f"\n{'='*60}")
    print(f" 突变倍数阈值分析 ({col})")
    print(f"{'='*60}")

    s = df[col].dropna()
    ma5 = s.rolling(5, min_periods=5).mean()
    ratio = s / ma5

    valid = ratio.dropna()
    print(f"\n  当日/MA5 比值分布:")
    print(f"    数据量:  {len(valid)}")
    print(f"    均值:    {valid.mean():.4f}")
    print(f"    中位数:  {valid.median():.4f}")
    for q in [1, 5, 10, 90, 95, 99]:
        val = np.percentile(valid, q)
        print(f"    {q:3d}%:   {val:.4f}")

    # 不同阈值对应的信号频率
    print(f"\n  不同倍数阈值的信号频率:")
    print(f"  {'倍数':>6s} {'暴增次数':>8s} {'年均':>6s} {'骤降次数':>8s} {'年均':>6s}")
    print(f"  {'-'*40}")
    years = len(valid) / 250
    for mult in [1.5, 2.0, 2.5, 3.0]:
        surge = (valid > mult).sum()
        plunge = (valid < 1/mult).sum()
        print(f"  {mult:>6.1f} {surge:>8d} {surge/years:>6.1f} {plunge:>8d} {plunge/years:>6.1f}")


# ── 主流程 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="换手率探索性分析")
    parser.add_argument("--ts_code", default=TS_CODE, help="指数代码")
    parser.add_argument("--name", default=INDEX_NAME, help="指数名称")
    args = parser.parse_args()

    print(f"\n{'#'*60}")
    print(f"# 换手率探索性分析: {args.name} ({args.ts_code})")
    print(f"{'#'*60}")

    # 加载数据
    df = load_index_turnover(args.ts_code)
    df_close = load_index_close(args.ts_code)

    # 1. 两个字段对比
    compare_fields(df)

    # 2. 分布分析（两个字段都看）
    analyze_distribution(df, "turnover_rate", f"{args.name} 总换手率")
    analyze_distribution(df, "turnover_rate_f", f"{args.name} 自由流通换手率")

    # 3. 牛熊分组（用 turnover_rate_f）
    analyze_bull_bear(df, "turnover_rate_f", f"{args.name} 自由流通换手率")

    # 4. 滚动窗口对比
    analyze_rolling_windows(df, "turnover_rate_f")

    # 5. 周线/月线聚合方式
    analyze_weekly_monthly_aggregation(df, df_close, "turnover_rate_f")

    # 6. 突变倍数阈值
    analyze_surge_thresholds(df, "turnover_rate_f")

    print(f"\n{'='*60}")
    print(f" 探索完成，下一步: 根据以上结果确定信号阈值，编写 signal_detector_turnover.py")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
