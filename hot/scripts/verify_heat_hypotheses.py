"""
热度排名与股价关系验证脚本

数据源:
  - my_trend.em_hot_rank_detail: 东方财富人气排名 (2025-03-15 ~ 2026-03-15, 全量A股)
  - my_stock.market_daily: 日线行情 (OHLC + 涨跌幅)

验证假说:
  H1: 热度飙升 → 短涨后反转 (短期正收益, 中期反转)
  H2: 持续高热 vs 昙花一现 → 后续表现差异
  H3: 热度是滞后指标 (价格先动, 散户后追)
  H4: 极端冷门 → 反向机会
  H5: 排名变化(Δrank)比绝对排名更有预测力

用法:
  python hot/research/verify_heat_hypotheses.py [--hypothesis H1|H2|H3|H4|H5|all]
"""

import argparse
import sys
import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path

# === 配置 ===
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
from app.config import Config

STOCK_DB_URL = Config.SQLALCHEMY_DATABASE_URI
TREND_DB_URL = STOCK_DB_URL.replace("/my_stock", "/my_trend")


def get_engines():
    """创建数据库连接"""
    trend_engine = create_engine(TREND_DB_URL)
    stock_engine = create_engine(STOCK_DB_URL)
    return trend_engine, stock_engine


def code_to_tscode(code: str) -> str:
    """stock_code → ts_code: 6开头=SH, 其他=SZ"""
    if code.startswith("6"):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


def load_data(trend_engine, stock_engine):
    """加载并合并热度 + 行情数据"""
    t0 = time.time()

    # 1. 加载热度数据 (只取交易日)
    print("加载热度数据...")
    heat_df = pd.read_sql(
        "SELECT stock_code, DATE(timestamp) as trade_date, `rank` FROM em_hot_rank_detail",
        trend_engine,
    )
    heat_df["trade_date"] = pd.to_datetime(heat_df["trade_date"])
    heat_df["ts_code"] = heat_df["stock_code"].apply(code_to_tscode)
    print(f"  热度: {len(heat_df):,} 行, {heat_df['trade_date'].nunique()} 天")

    # 2. 加载行情数据
    print("加载行情数据...")
    price_df = pd.read_sql(
        "SELECT ts_code, trade_date, close, pct_chg FROM market_daily "
        "WHERE trade_date >= '20250315' AND trade_date <= '20260315'",
        stock_engine,
    )
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"], format="%Y%m%d")
    print(f"  行情: {len(price_df):,} 行, {price_df['trade_date'].nunique()} 天")

    # 3. 获取交易日历
    trading_dates = sorted(price_df["trade_date"].unique())
    trading_dates_set = set(trading_dates)

    # 4. 合并 (inner join, 只保留交易日)
    print("合并数据...")
    df = heat_df.merge(price_df, on=["ts_code", "trade_date"], how="inner")
    print(f"  合并后: {len(df):,} 行, {df['trade_date'].nunique()} 天, {df['ts_code'].nunique()} 股")

    # 5. 计算前向收益 (T+5, T+10, T+20, T+60) — 向量化
    print("计算前向收益...")
    windows = [5, 10, 20, 60, 120, 150]
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    for w in windows:
        col = f"fwd_ret_{w}"
        # groupby + shift(-w) 对齐未来第 w 个交易日的 close
        df[f"_fwd_close_{w}"] = df.groupby("ts_code")["close"].shift(-w)
        df[col] = (df[f"_fwd_close_{w}"] / df["close"] - 1) * 100
        df.drop(columns=[f"_fwd_close_{w}"], inplace=True)

    # 6. 计算排名变化 (Δrank, 需要前一交易日排名)
    print("计算排名变化...")
    df = df.sort_values(["ts_code", "trade_date"])
    df["prev_rank"] = df.groupby("ts_code")["rank"].shift(1)
    df["delta_rank"] = df["prev_rank"] - df["rank"]  # 正值=排名上升(数字变小)

    elapsed = time.time() - t0
    print(f"数据准备完成, 耗时 {elapsed:.1f}s\n")
    return df, trading_dates


def print_stats(group_name, sub_df, windows=[5, 10, 20, 60, 120, 150]):
    """打印一组统计结果"""
    n = len(sub_df)
    if n == 0:
        print(f"  {group_name}: 无数据")
        return
    line = f"  {group_name} (n={n:,})"
    for w in windows:
        col = f"fwd_ret_{w}"
        valid = sub_df[col].dropna()
        if len(valid) == 0:
            line += f"  | T+{w}: --"
            continue
        avg = valid.mean()
        win_rate = (valid > 0).mean() * 100
        line += f"  | T+{w}: {avg:+.2f}% (胜率{win_rate:.1f}%)"
    print(line)


def h1_rank_spike(df):
    """
    H1: 热度飙升 → 短涨后反转
    定义: 排名单日上升 >= 500 名 (delta_rank >= 500)
    对照: 排名无明显变化 (|delta_rank| < 50)
    """
    print("=" * 80)
    print("H1: 热度飙升 → 短期正收益 → 中期反转?")
    print("=" * 80)

    valid = df.dropna(subset=["delta_rank"])

    # 不同飙升幅度分组
    thresholds = [
        ("排名飙升≥2000", valid[valid["delta_rank"] >= 2000]),
        ("排名飙升≥1000", valid[valid["delta_rank"] >= 1000]),
        ("排名飙升≥500", valid[valid["delta_rank"] >= 500]),
        ("排名小幅变动|Δ|<50", valid[valid["delta_rank"].abs() < 50]),
        ("排名暴跌≥500", valid[valid["delta_rank"] <= -500]),
        ("排名暴跌≥1000", valid[valid["delta_rank"] <= -1000]),
    ]

    for name, sub in thresholds:
        print_stats(name, sub)

    print()


def h2_sustained_heat(df):
    """
    H2: 持续高热 vs 昙花一现
    连续 N 天排名 < 100 为"持续热门", 仅 1 天为"昙花一现"
    """
    print("=" * 80)
    print("H2: 持续高热 vs 昙花一现, 后续表现差异?")
    print("=" * 80)

    # 标记每天是否在 top 100
    hot = df[df["rank"] <= 100].copy()
    hot = hot.sort_values(["ts_code", "trade_date"])

    # 计算连续天数: 用 trade_date 的日期差分组
    results = []
    for ts, grp in hot.groupby("ts_code"):
        dates = grp["trade_date"].sort_values().values
        # 找连续交易日段
        streaks = []
        streak_start = 0
        for i in range(1, len(dates)):
            gap = (dates[i] - dates[i - 1]) / np.timedelta64(1, "D")
            if gap > 4:  # 超过4天间隔(周末+1), 认为不连续
                streaks.append((streak_start, i - 1))
                streak_start = i
        streaks.append((streak_start, len(dates) - 1))

        for s, e in streaks:
            streak_len = e - s + 1
            # 取这段热门期的最后一天作为信号点
            last_date = dates[e]
            row = grp[grp["trade_date"] == last_date].iloc[0]
            results.append({
                "ts_code": ts,
                "trade_date": last_date,
                "streak_days": streak_len,
                "rank": row["rank"],
                "fwd_ret_5": row.get("fwd_ret_5", np.nan),
                "fwd_ret_10": row.get("fwd_ret_10", np.nan),
                "fwd_ret_20": row.get("fwd_ret_20", np.nan),
                "fwd_ret_60": row.get("fwd_ret_60", np.nan),
                "fwd_ret_120": row.get("fwd_ret_120", np.nan),
                "fwd_ret_150": row.get("fwd_ret_150", np.nan),
            })

    if not results:
        print("  无 top100 连续数据")
        return

    streak_df = pd.DataFrame(results)
    groups = [
        ("昙花一现(1天)", streak_df[streak_df["streak_days"] == 1]),
        ("短期热门(2-3天)", streak_df[streak_df["streak_days"].between(2, 3)]),
        ("中期热门(4-10天)", streak_df[streak_df["streak_days"].between(4, 10)]),
        ("持续热门(>10天)", streak_df[streak_df["streak_days"] > 10]),
    ]

    for name, sub in groups:
        print_stats(name, sub)

    print()


def h3_lead_lag(df):
    """
    H3: 热度是滞后指标? (价格先动 → 散户后追)
    方法: 看大涨当天 vs 大涨后 1-3 天的排名变化
    """
    print("=" * 80)
    print("H3: 价格先动还是热度先动? (领先/滞后分析)")
    print("=" * 80)

    valid = df.dropna(subset=["delta_rank", "pct_chg"]).copy()

    # 方向1: 大涨(>5%)当天, 排名是否已经开始上升?
    big_up = valid[valid["pct_chg"] >= 5]
    big_down = valid[valid["pct_chg"] <= -5]

    print(f"\n  大涨日(涨幅≥5%, n={len(big_up):,}):")
    if len(big_up) > 0:
        avg_delta = big_up["delta_rank"].mean()
        pct_up = (big_up["delta_rank"] > 0).mean() * 100
        print(f"    当天排名平均变化: {avg_delta:+.0f} ({pct_up:.1f}%的股票排名上升)")
        # 中位数排名
        print(f"    当天排名中位数: {big_up['rank'].median():.0f}")

    print(f"\n  大跌日(跌幅≥5%, n={len(big_down):,}):")
    if len(big_down) > 0:
        avg_delta = big_down["delta_rank"].mean()
        pct_up = (big_down["delta_rank"] > 0).mean() * 100
        print(f"    当天排名平均变化: {avg_delta:+.0f} ({pct_up:.1f}%的股票排名上升)")
        print(f"    当天排名中位数: {big_down['rank'].median():.0f}")

    # 方向2: 排名突然飙升(delta_rank>=500), 当天涨跌幅如何?
    print(f"\n  反向验证 - 排名飙升日(Δrank≥500):")
    spike = valid[valid["delta_rank"] >= 500]
    if len(spike) > 0:
        print(f"    样本数: {len(spike):,}")
        print(f"    当天平均涨跌幅: {spike['pct_chg'].mean():+.2f}%")
        print(f"    当天涨幅中位数: {spike['pct_chg'].median():+.2f}%")
        print(f"    涨幅>0的比例: {(spike['pct_chg'] > 0).mean() * 100:.1f}%")

    # 方向3: 排名飙升日, 前一天的涨跌幅 (需要 lag)
    valid = valid.sort_values(["ts_code", "trade_date"])
    valid["prev_pct_chg"] = valid.groupby("ts_code")["pct_chg"].shift(1)
    spike2 = valid[valid["delta_rank"] >= 500].dropna(subset=["prev_pct_chg"])
    if len(spike2) > 0:
        print(f"\n  排名飙升日的前一天涨跌幅:")
        print(f"    前一天平均涨跌幅: {spike2['prev_pct_chg'].mean():+.2f}%")
        print(f"    前一天涨幅>0的比例: {(spike2['prev_pct_chg'] > 0).mean() * 100:.1f}%")

    print()


def h4_cold_stocks(df):
    """
    H4: 极端冷门股 → 反向机会?
    按排名分区统计前向收益
    """
    print("=" * 80)
    print("H4: 热门区 vs 冷门区, 后续收益分布")
    print("=" * 80)

    zones = [
        ("超级热门(1-50)", df[df["rank"] <= 50]),
        ("热门(51-200)", df[df["rank"].between(51, 200)]),
        ("温热(201-500)", df[df["rank"].between(201, 500)]),
        ("中等(501-1000)", df[df["rank"].between(501, 1000)]),
        ("偏冷(1001-2000)", df[df["rank"].between(1001, 2000)]),
        ("冷门(2001-3500)", df[df["rank"].between(2001, 3500)]),
        ("极冷(3501-5000)", df[df["rank"].between(3501, 5000)]),
        ("冰封(>5000)", df[df["rank"] > 5000]),
    ]

    for name, sub in zones:
        print_stats(name, sub)

    print()


def h5_delta_vs_absolute(df):
    """
    H5: 排名变化(Δrank)比绝对排名更有预测力?
    按 delta_rank 分组 vs 按 rank 分组, 对比收益离散度
    """
    print("=" * 80)
    print("H5: 排名变化(Δrank) vs 绝对排名, 谁的预测力更强?")
    print("=" * 80)

    valid = df.dropna(subset=["delta_rank"]).copy()

    # A: 按 delta_rank 分组
    print("\n  [A] 按排名变化幅度分组:")
    delta_groups = [
        ("暴升(Δ≥1500)", valid[valid["delta_rank"] >= 1500]),
        ("大升(500~1499)", valid[valid["delta_rank"].between(500, 1499)]),
        ("小升(100~499)", valid[valid["delta_rank"].between(100, 499)]),
        ("平稳(|Δ|<100)", valid[valid["delta_rank"].abs() < 100]),
        ("小降(-499~-100)", valid[valid["delta_rank"].between(-499, -100)]),
        ("大降(-1499~-500)", valid[valid["delta_rank"].between(-1499, -500)]),
        ("暴降(Δ≤-1500)", valid[valid["delta_rank"] <= -1500]),
    ]
    for name, sub in delta_groups:
        print_stats(name, sub)

    # B: 按绝对排名分组 (简化版)
    print("\n  [B] 按绝对排名分组:")
    abs_groups = [
        ("Top 100", valid[valid["rank"] <= 100]),
        ("101-500", valid[valid["rank"].between(101, 500)]),
        ("501-2000", valid[valid["rank"].between(501, 2000)]),
        ("2001-4000", valid[valid["rank"].between(2001, 4000)]),
        (">4000", valid[valid["rank"] > 4000]),
    ]
    for name, sub in abs_groups:
        print_stats(name, sub)

    # C: 预测力对比 - 用 T+20 收益的分组间标准差来衡量
    print("\n  [C] 预测力对比 (T+20 收益的组间离散度):")
    delta_means = [sub["fwd_ret_20"].mean() for _, sub in delta_groups if len(sub) > 100]
    abs_means = [sub["fwd_ret_20"].mean() for _, sub in abs_groups if len(sub) > 100]

    delta_std = np.nanstd(delta_means) if delta_means else 0
    abs_std = np.nanstd(abs_means) if abs_means else 0

    print(f"    Δrank 分组的 T+20 均值离散度: {delta_std:.3f}")
    print(f"    绝对排名分组的 T+20 均值离散度: {abs_std:.3f}")
    winner = "Δrank" if delta_std > abs_std else "绝对排名"
    print(f"    → {winner} 的区分度更强")

    print()


def main():
    parser = argparse.ArgumentParser(description="热度排名与股价关系验证")
    parser.add_argument("--hypothesis", "-H", default="all",
                        choices=["H1", "H2", "H3", "H4", "H5", "all"],
                        help="验证哪个假说 (默认: all)")
    args = parser.parse_args()

    trend_engine, stock_engine = get_engines()
    df, trading_dates = load_data(trend_engine, stock_engine)

    runners = {
        "H1": h1_rank_spike,
        "H2": h2_sustained_heat,
        "H3": h3_lead_lag,
        "H4": h4_cold_stocks,
        "H5": h5_delta_vs_absolute,
    }

    if args.hypothesis == "all":
        for key in ["H1", "H2", "H3", "H4", "H5"]:
            runners[key](df)
    else:
        runners[args.hypothesis](df)


if __name__ == "__main__":
    main()
