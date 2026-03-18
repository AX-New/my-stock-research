"""
计算热度分组相对三大指数的超额收益
对比基准: 上证(000001.SH), 深证(399001.SZ), 创业板(399006.SZ)
"""
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))
from app.config import Config

STOCK_DB_URL = Config.SQLALCHEMY_DATABASE_URI
TREND_DB_URL = STOCK_DB_URL.replace("/my_stock", "/my_trend")

WINDOWS = [5, 10, 20, 60, 120, 150]


def calc_index_fwd_returns():
    """计算三大指数每个交易日的前向收益"""
    engine = create_engine(STOCK_DB_URL)
    idx_df = pd.read_sql(
        "SELECT ts_code, trade_date, close FROM index_daily "
        "WHERE ts_code IN ('000001.SH','399001.SZ','399006.SZ') "
        "AND trade_date >= '20250315' AND trade_date <= '20260315' "
        "ORDER BY ts_code, trade_date",
        engine,
    )
    idx_df["trade_date"] = pd.to_datetime(idx_df["trade_date"], format="%Y%m%d")
    idx_df = idx_df.sort_values(["ts_code", "trade_date"])

    for w in WINDOWS:
        idx_df[f"idx_ret_{w}"] = idx_df.groupby("ts_code")["close"].shift(-w)
        idx_df[f"idx_ret_{w}"] = (idx_df[f"idx_ret_{w}"] / idx_df["close"] - 1) * 100

    return idx_df


def main():
    # 1. 指数前向收益
    print("计算指数基准收益...")
    idx_df = calc_index_fwd_returns()

    # 每个指数在各窗口的全期平均前向收益
    idx_names = {"000001.SH": "上证", "399001.SZ": "深证", "399006.SZ": "创业板"}
    print("\n=== 三大指数各窗口平均前向收益（基准线）===")
    header = f"{'指数':<8}"
    for w in WINDOWS:
        header += f"  | T+{w:>3}"
    print(header)

    idx_avgs = {}
    for code, name in idx_names.items():
        sub = idx_df[idx_df["ts_code"] == code]
        line = f"{name:<8}"
        avgs = {}
        for w in WINDOWS:
            col = f"idx_ret_{w}"
            avg = sub[col].dropna().mean()
            avgs[w] = avg
            line += f"  | {avg:>+6.2f}%"
        idx_avgs[code] = avgs
        print(line)

    # 2. 加载热度分组的收益（直接从脚本结果解析太麻烦，重新算H4关键分组）
    print("\n加载热度+行情数据...")
    trend_engine = create_engine(TREND_DB_URL)
    stock_engine = create_engine(STOCK_DB_URL)

    heat_df = pd.read_sql(
        "SELECT stock_code, DATE(timestamp) as trade_date, `rank` FROM em_hot_rank_detail",
        trend_engine,
    )
    heat_df["trade_date"] = pd.to_datetime(heat_df["trade_date"])
    heat_df["ts_code"] = heat_df["stock_code"].apply(
        lambda c: f"{c}.SH" if c.startswith("6") else f"{c}.SZ"
    )

    price_df = pd.read_sql(
        "SELECT ts_code, trade_date, close FROM market_daily "
        "WHERE trade_date >= '20250315' AND trade_date <= '20260315'",
        stock_engine,
    )
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"], format="%Y%m%d")

    df = heat_df.merge(price_df, on=["ts_code", "trade_date"], how="inner")
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    for w in WINDOWS:
        df[f"fwd_ret_{w}"] = df.groupby("ts_code")["close"].shift(-w)
        df[f"fwd_ret_{w}"] = (df[f"fwd_ret_{w}"] / df["close"] - 1) * 100

    # 3. 分组统计 + 超额收益
    zones = [
        ("超热(1-50)", (1, 50)),
        ("热门(51-200)", (51, 200)),
        ("温热(201-500)", (201, 500)),
        ("中等(501-1000)", (501, 1000)),
        ("偏冷(1001-2000)", (1001, 2000)),
        ("冷门(2001-3500)", (2001, 3500)),
        ("极冷(3501-5000)", (3501, 5000)),
        ("冰封(>5000)", (5001, 99999)),
    ]

    # 用上证作为主基准
    sh_avgs = idx_avgs["000001.SH"]

    print("\n" + "=" * 100)
    print("H4 各热度区间 vs 上证指数 超额收益")
    print("=" * 100)
    header = f"{'区间':<16} {'样本':>7}"
    for w in WINDOWS:
        header += f"  | T+{w}: 绝对→超额(胜率)"
    print(header)
    print("-" * 100)

    for name, (lo, hi) in zones:
        if hi == 99999:
            sub = df[df["rank"] >= lo]
        else:
            sub = df[df["rank"].between(lo, hi)]
        n = len(sub)
        line = f"{name:<16} {n:>7,}"
        for w in WINDOWS:
            col = f"fwd_ret_{w}"
            valid = sub[col].dropna()
            if len(valid) == 0:
                line += f"  | T+{w}: --"
                continue
            avg = valid.mean()
            win = (valid > 0).mean() * 100
            excess = avg - sh_avgs[w]
            line += f"  | {avg:+.1f}% → {excess:+.1f}%({win:.0f}%)"
        print(line)

    # 上证基准行
    print("-" * 100)
    line = f"{'上证基准':<16} {'--':>7}"
    for w in WINDOWS:
        line += f"  | {sh_avgs[w]:+.1f}% → +0.0%"
    print(line)

    # 4. 简化表格输出（纯超额收益）
    print("\n\n=== 纯超额收益表（vs 上证）===")
    print(f"{'区间':<16}", end="")
    for w in WINDOWS:
        print(f"  | T+{w:>3}", end="")
    print()
    for name, (lo, hi) in zones:
        if hi == 99999:
            sub = df[df["rank"] >= lo]
        else:
            sub = df[df["rank"].between(lo, hi)]
        print(f"{name:<16}", end="")
        for w in WINDOWS:
            col = f"fwd_ret_{w}"
            valid = sub[col].dropna()
            avg = valid.mean() if len(valid) > 0 else 0
            excess = avg - sh_avgs[w]
            print(f"  | {excess:>+6.1f}%", end="")
        print()

    # 5. vs 深证和创业板
    for idx_code, idx_name in [("399001.SZ", "深证"), ("399006.SZ", "创业板")]:
        avgs = idx_avgs[idx_code]
        print(f"\n=== 纯超额收益表（vs {idx_name}）===")
        print(f"{'区间':<16}", end="")
        for w in WINDOWS:
            print(f"  | T+{w:>3}", end="")
        print()
        for name, (lo, hi) in zones:
            if hi == 99999:
                sub = df[df["rank"] >= lo]
            else:
                sub = df[df["rank"].between(lo, hi)]
            print(f"{name:<16}", end="")
            for w in WINDOWS:
                col = f"fwd_ret_{w}"
                valid = sub[col].dropna()
                avg = valid.mean() if len(valid) > 0 else 0
                excess = avg - avgs[w]
                print(f"  | {excess:>+6.1f}%", end="")
            print()


if __name__ == "__main__":
    main()
