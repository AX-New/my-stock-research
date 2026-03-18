"""信号到峰值时间分析 — 各指标信号出现后多少天达到极值

分析目标:
- 对 MACD / RSI / MA / 换手率 / 资金流向 五大指标的各类信号
- 统计信号发出后 1~60 个交易日内，股价何时达到峰值/谷值
- 计算近一年（日线）的平均峰值天数、中位数、峰值收益率等

数据来源:
- 信号: stock_research(MACD), stock_rsi(RSI), stock_ma(MA), stock_turnover(换手率)
- 价格: my_stock.stk_factor_pro (close_qfq)
- 资金: my_stock.moneyflow (net_mf_amount)
"""
import os
import sys
import time
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# ── 数据库配置 ─────────────────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")


def _uri(db_name: str) -> str:
    return (f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4")


# 各研究库引擎
eng_stock = create_engine(_uri("my_stock"), pool_pre_ping=True)
eng_macd = create_engine(_uri("stock_research"), pool_pre_ping=True)
eng_rsi = create_engine(_uri("stock_rsi"), pool_pre_ping=True)
eng_ma = create_engine(_uri("stock_ma"), pool_pre_ping=True)
eng_turnover = create_engine(_uri("stock_turnover"), pool_pre_ping=True)

# ── 时间范围 ─────────────────────────────────────────────────────
# 近一年信号（截止日期前一年）
END_DATE = "20260317"  # 当前日期
START_DATE = "20250317"  # 一年前
# 价格数据需额外加载60个交易日用于计算峰值
PRICE_START = "20250201"  # 多留一些余量
FORWARD_DAYS = 60  # 信号后观察窗口（交易日）


def load_signals():
    """从各研究库加载近一年的日线买入/卖出信号"""
    signals_all = []

    # 1. MACD 信号
    print("[1/5] 加载 MACD 信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction,
               signal_value, close, ret_5, ret_10, ret_20, ret_60
        FROM stock_macd_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_macd)
    df["indicator"] = "MACD"
    signals_all.append(df)
    print(f"  MACD: {len(df)} 条信号, 耗时 {time.time()-t0:.1f}s")

    # 2. RSI 信号
    print("[2/5] 加载 RSI 信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction,
               signal_value, close, ret_5, ret_10, ret_20, ret_60
        FROM stock_rsi_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_rsi)
    df["indicator"] = "RSI"
    signals_all.append(df)
    print(f"  RSI: {len(df)} 条信号, 耗时 {time.time()-t0:.1f}s")

    # 3. MA 信号
    print("[3/5] 加载 MA 信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction,
               signal_value, close, ret_5, ret_10, ret_20, ret_60
        FROM stock_ma_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_ma)
    df["indicator"] = "MA"
    signals_all.append(df)
    print(f"  MA: {len(df)} 条信号, 耗时 {time.time()-t0:.1f}s")

    # 4. 换手率信号
    print("[4/5] 加载换手率信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction,
               signal_value, close, ret_5, ret_10, ret_20, ret_60
        FROM stock_turnover_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_turnover)
    df["indicator"] = "换手率"
    signals_all.append(df)
    print(f"  换手率: {len(df)} 条信号, 耗时 {time.time()-t0:.1f}s")

    # 5. 资金流向信号 — 从原始数据定义极端信号
    print("[5/5] 加载资金流向并生成信号...")
    t0 = time.time()
    df_mf = _build_moneyflow_signals()
    signals_all.append(df_mf)
    print(f"  资金流向: {len(df_mf)} 条信号, 耗时 {time.time()-t0:.1f}s")

    all_signals = pd.concat(signals_all, ignore_index=True)
    print(f"\n总信号数: {len(all_signals)}")
    return all_signals


def _build_moneyflow_signals():
    """从 moneyflow 表构建资金流向极端信号

    定义:
    - 主力净流入(buy): net_mf_amount > 该股过去60日均值 + 2倍标准差
    - 主力净流出(sell): net_mf_amount < 该股过去60日均值 - 2倍标准差
    """
    sql = f"""
        SELECT ts_code, trade_date, net_mf_amount
        FROM moneyflow
        WHERE trade_date >= '{PRICE_START}' AND trade_date <= '{END_DATE}'
          AND net_mf_amount IS NOT NULL
        ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, eng_stock)
    if df.empty:
        return pd.DataFrame()

    # 计算每只股票的滚动均值和标准差(60日窗口)
    df = df.sort_values(["ts_code", "trade_date"])
    df["roll_mean"] = df.groupby("ts_code")["net_mf_amount"].transform(
        lambda x: x.rolling(60, min_periods=20).mean()
    )
    df["roll_std"] = df.groupby("ts_code")["net_mf_amount"].transform(
        lambda x: x.rolling(60, min_periods=20).std()
    )

    # 筛选极端信号（仅近一年范围内）
    df = df[(df["trade_date"] >= START_DATE) & (df["trade_date"] <= END_DATE)]
    df = df.dropna(subset=["roll_mean", "roll_std"])
    df = df[df["roll_std"] > 0]

    # 强流入信号
    buy_mask = df["net_mf_amount"] > (df["roll_mean"] + 2 * df["roll_std"])
    buy_df = df[buy_mask].copy()
    buy_df["direction"] = "buy"
    buy_df["signal_name"] = "extreme_net_inflow"
    buy_df["signal_type"] = "extreme"

    # 强流出信号
    sell_mask = df["net_mf_amount"] < (df["roll_mean"] - 2 * df["roll_std"])
    sell_df = df[sell_mask].copy()
    sell_df["direction"] = "sell"
    sell_df["signal_name"] = "extreme_net_outflow"
    sell_df["signal_type"] = "extreme"

    result = pd.concat([buy_df, sell_df], ignore_index=True)
    result["indicator"] = "资金流向"
    result["signal_value"] = result["net_mf_amount"]
    result["close"] = None
    result["ret_5"] = None
    result["ret_10"] = None
    result["ret_20"] = None
    result["ret_60"] = None

    return result[["ts_code", "trade_date", "signal_type", "signal_name",
                    "direction", "signal_value", "close", "ret_5", "ret_10",
                    "ret_20", "ret_60", "indicator"]]


def load_price_data(ts_codes: list):
    """加载所有相关股票的 qfq 收盘价

    使用 stk_factor_pro 的 close_qfq 字段
    分批加载避免 IN 子句过长
    """
    print(f"\n加载价格数据 ({len(ts_codes)} 只股票)...")
    t0 = time.time()

    batch_size = 500
    dfs = []
    for i in range(0, len(ts_codes), batch_size):
        batch = ts_codes[i:i + batch_size]
        codes_str = ",".join(f"'{c}'" for c in batch)
        sql = f"""
            SELECT ts_code, trade_date, close_qfq
            FROM stk_factor_pro
            WHERE ts_code IN ({codes_str})
              AND trade_date >= '{PRICE_START}'
            ORDER BY ts_code, trade_date
        """
        df = pd.read_sql(sql, eng_stock)
        dfs.append(df)
        print(f"  批次 {i // batch_size + 1}: {len(df)} 行")

    prices = pd.concat(dfs, ignore_index=True)
    print(f"价格数据: {len(prices)} 行, 耗时 {time.time()-t0:.1f}s")
    return prices


def compute_peak_timing(signals: pd.DataFrame, prices: pd.DataFrame):
    """计算每个信号后的峰值/谷值到达天数

    买入信号(buy): 找后续N日最高价出现的天数
    卖出信号(sell): 找后续N日最低价出现的天数

    Returns:
        DataFrame: 每行一个信号，附加 peak_day, peak_ret 列
    """
    print(f"\n计算峰值时间 ({len(signals)} 个信号)...")
    t0 = time.time()

    # 为价格数据构建按(ts_code)分组的字典，加速查找
    prices = prices.sort_values(["ts_code", "trade_date"])
    price_groups = {}
    for ts_code, grp in prices.groupby("ts_code"):
        # 转为 numpy 数组加速
        dates = grp["trade_date"].values
        closes = grp["close_qfq"].values
        price_groups[ts_code] = (dates, closes)

    results = []
    processed = 0
    skipped = 0

    for _, row in signals.iterrows():
        ts_code = row["ts_code"]
        signal_date = row["trade_date"]
        direction = row.get("direction", "buy")

        if ts_code not in price_groups:
            skipped += 1
            continue

        dates, closes = price_groups[ts_code]

        # 找到信号日在价格数组中的位置
        idx = np.searchsorted(dates, signal_date)
        if idx >= len(dates) or dates[idx] != signal_date:
            skipped += 1
            continue

        # 信号日收盘价
        signal_close = closes[idx]
        if signal_close is None or signal_close <= 0 or np.isnan(signal_close):
            skipped += 1
            continue

        # 取后续 FORWARD_DAYS 个交易日的收盘价
        end_idx = min(idx + FORWARD_DAYS + 1, len(dates))
        if end_idx <= idx + 1:
            skipped += 1
            continue

        forward_closes = closes[idx + 1:end_idx]
        if len(forward_closes) < 5:  # 至少需要5个交易日数据
            skipped += 1
            continue

        # 计算各日收益率(%)
        forward_rets = (forward_closes / signal_close - 1) * 100

        if direction == "buy":
            # 买入信号: 找最高点
            peak_idx_in_forward = np.nanargmax(forward_rets)
            peak_ret = forward_rets[peak_idx_in_forward]
        else:
            # 卖出信号: 找最低点
            peak_idx_in_forward = np.nanargmin(forward_rets)
            peak_ret = forward_rets[peak_idx_in_forward]

        peak_day = peak_idx_in_forward + 1  # +1 因为第0天是信号后第1天

        results.append({
            "ts_code": ts_code,
            "trade_date": signal_date,
            "indicator": row["indicator"],
            "signal_type": row["signal_type"],
            "signal_name": row["signal_name"],
            "direction": direction,
            "peak_day": peak_day,
            "peak_ret": peak_ret,
            "forward_days_available": len(forward_closes),
        })

        processed += 1
        if processed % 50000 == 0:
            print(f"  已处理 {processed} / {len(signals)}, 跳过 {skipped}")

    print(f"完成: 有效 {processed}, 跳过 {skipped}, 耗时 {time.time()-t0:.1f}s")

    return pd.DataFrame(results)


def aggregate_statistics(df: pd.DataFrame):
    """按指标和信号名聚合统计峰值天数"""
    if df.empty:
        print("无数据可聚合！")
        return pd.DataFrame()

    # 按 (indicator, signal_type, signal_name, direction) 分组
    group_cols = ["indicator", "signal_type", "signal_name", "direction"]
    stats = df.groupby(group_cols).agg(
        count=("peak_day", "size"),
        avg_peak_day=("peak_day", "mean"),
        median_peak_day=("peak_day", "median"),
        std_peak_day=("peak_day", "std"),
        p25_peak_day=("peak_day", lambda x: np.percentile(x, 25)),
        p75_peak_day=("peak_day", lambda x: np.percentile(x, 75)),
        avg_peak_ret=("peak_ret", "mean"),
        median_peak_ret=("peak_ret", "median"),
    ).reset_index()

    # 四舍五入
    for col in ["avg_peak_day", "median_peak_day", "std_peak_day",
                 "p25_peak_day", "p75_peak_day", "avg_peak_ret", "median_peak_ret"]:
        stats[col] = stats[col].round(2)

    # 排序: 按指标、方向、信号数量降序
    stats = stats.sort_values(["indicator", "direction", "count"], ascending=[True, True, False])

    return stats


def aggregate_by_indicator(df: pd.DataFrame):
    """按指标大类聚合（指标级汇总）"""
    if df.empty:
        return pd.DataFrame()

    group_cols = ["indicator", "direction"]
    stats = df.groupby(group_cols).agg(
        signal_count=("peak_day", "size"),
        signal_types=("signal_name", "nunique"),
        avg_peak_day=("peak_day", "mean"),
        median_peak_day=("peak_day", "median"),
        std_peak_day=("peak_day", "std"),
        avg_peak_ret=("peak_ret", "mean"),
    ).reset_index()

    for col in ["avg_peak_day", "median_peak_day", "std_peak_day", "avg_peak_ret"]:
        stats[col] = stats[col].round(2)

    stats = stats.sort_values(["indicator", "direction"])
    return stats


def compute_peak_distribution(df: pd.DataFrame):
    """计算各指标的峰值天数分布（按5日窗口分桶）"""
    if df.empty:
        return pd.DataFrame()

    # 定义时间窗口
    bins = [0, 3, 5, 10, 15, 20, 30, 45, 60]
    labels = ["1-3日", "4-5日", "6-10日", "11-15日", "16-20日", "21-30日", "31-45日", "46-60日"]

    df["peak_bucket"] = pd.cut(df["peak_day"], bins=bins, labels=labels, right=True)

    dist = df.groupby(["indicator", "direction", "peak_bucket"], observed=False).agg(
        count=("peak_day", "size"),
    ).reset_index()

    # 计算占比
    totals = df.groupby(["indicator", "direction"]).size().reset_index(name="total")
    dist = dist.merge(totals, on=["indicator", "direction"])
    dist["pct"] = (dist["count"] / dist["total"] * 100).round(1)

    return dist


def main():
    """主流程"""
    print("=" * 60)
    print("  信号到峰值时间分析 — 五大指标日线信号")
    print(f"  数据范围: {START_DATE} ~ {END_DATE}")
    print("=" * 60)

    # 1. 加载信号
    signals = load_signals()
    if signals.empty:
        print("无信号数据!")
        return

    # 过滤: 只保留有方向的信号
    signals = signals[signals["direction"].isin(["buy", "sell"])]
    print(f"\n有效信号(buy/sell): {len(signals)}")

    # 信号概览
    print("\n各指标信号分布:")
    overview = signals.groupby(["indicator", "direction"]).size().reset_index(name="count")
    print(overview.to_string(index=False))

    # 2. 加载价格数据
    ts_codes = signals["ts_code"].unique().tolist()
    prices = load_price_data(ts_codes)

    # 3. 计算峰值时间
    peak_df = compute_peak_timing(signals, prices)

    if peak_df.empty:
        print("无法计算峰值时间！")
        return

    # 4. 聚合统计
    print("\n" + "=" * 60)
    print("  统计结果")
    print("=" * 60)

    # 4.1 指标级汇总
    indicator_stats = aggregate_by_indicator(peak_df)
    print("\n── 各指标峰值天数汇总 ──")
    print(indicator_stats.to_string(index=False))

    # 4.2 信号级汇总
    signal_stats = aggregate_statistics(peak_df)
    print("\n── 各信号峰值天数明细 ──")
    # 只打印信号数量>100的
    sig_filtered = signal_stats[signal_stats["count"] >= 100]
    print(sig_filtered.to_string(index=False))

    # 4.3 峰值天数分布
    dist = compute_peak_distribution(peak_df)

    # 5. 保存结果
    output_dir = os.path.dirname(os.path.abspath(__file__))

    indicator_stats.to_csv(os.path.join(output_dir, "peak_timing_by_indicator.csv"),
                           index=False, encoding="utf-8-sig")
    signal_stats.to_csv(os.path.join(output_dir, "peak_timing_by_signal.csv"),
                        index=False, encoding="utf-8-sig")
    dist.to_csv(os.path.join(output_dir, "peak_timing_distribution.csv"),
                index=False, encoding="utf-8-sig")
    peak_df.to_csv(os.path.join(output_dir, "peak_timing_raw.csv"),
                   index=False, encoding="utf-8-sig")

    print(f"\n结果已保存到 {output_dir}/")
    print(f"  - peak_timing_by_indicator.csv (指标级汇总)")
    print(f"  - peak_timing_by_signal.csv (信号级明细)")
    print(f"  - peak_timing_distribution.csv (天数分布)")
    print(f"  - peak_timing_raw.csv (原始明细)")


if __name__ == "__main__":
    main()
