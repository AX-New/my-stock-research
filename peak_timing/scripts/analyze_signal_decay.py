"""信号有效期衰减分析 — 各指标信号在不同持有周期的胜率变化

核心问题:
- 之前研究结论"金叉是噪声信号"是否因为评估周期太长（T+60）导致的？
- 如果信号峰值在5~8天，那用T+60来评估当然会看起来像噪声
- 每个指标/信号的"有效期"到底是多长？

方法:
- 对五大指标(MACD/RSI/MA/换手率/资金流向)的所有信号
- 在T+1,2,3,5,7,10,15,20,30,45,60多个窗口计算胜率和平均收益
- 找出每个信号的"最佳窗口"和"失效窗口"

数据来源: 与 analyze_peak_timing.py 相同
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
END_DATE = "20260317"
START_DATE = "20250317"
PRICE_START = "20250201"  # 多留余量

# 评估窗口（交易日）
WINDOWS = [1, 2, 3, 5, 7, 10, 15, 20, 30, 45, 60]


def load_signals():
    """加载五大指标的近一年日线信号"""
    signals_all = []

    # 1. MACD
    print("[1/5] 加载 MACD 信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction
        FROM stock_macd_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_macd)
    df["indicator"] = "MACD"
    signals_all.append(df)
    print(f"  MACD: {len(df)} 条, {time.time()-t0:.1f}s")

    # 2. RSI
    print("[2/5] 加载 RSI 信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction
        FROM stock_rsi_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_rsi)
    df["indicator"] = "RSI"
    signals_all.append(df)
    print(f"  RSI: {len(df)} 条, {time.time()-t0:.1f}s")

    # 3. MA
    print("[3/5] 加载 MA 信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction
        FROM stock_ma_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_ma)
    df["indicator"] = "MA"
    signals_all.append(df)
    print(f"  MA: {len(df)} 条, {time.time()-t0:.1f}s")

    # 4. 换手率
    print("[4/5] 加载换手率信号...")
    t0 = time.time()
    sql = f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction
        FROM stock_turnover_signal
        WHERE freq = 'daily'
          AND trade_date >= '{START_DATE}' AND trade_date <= '{END_DATE}'
    """
    df = pd.read_sql(sql, eng_turnover)
    df["indicator"] = "换手率"
    signals_all.append(df)
    print(f"  换手率: {len(df)} 条, {time.time()-t0:.1f}s")

    # 5. 资金流向
    print("[5/5] 加载资金流向信号...")
    t0 = time.time()
    df_mf = _build_moneyflow_signals()
    signals_all.append(df_mf)
    print(f"  资金流向: {len(df_mf)} 条, {time.time()-t0:.1f}s")

    all_signals = pd.concat(signals_all, ignore_index=True)
    all_signals = all_signals[all_signals["direction"].isin(["buy", "sell"])]
    print(f"\n总有效信号: {len(all_signals)}")
    return all_signals


def _build_moneyflow_signals():
    """从 moneyflow 表构建资金流向极端信号（同 peak_timing 定义）"""
    sql = f"""
        SELECT ts_code, trade_date, net_mf_amount
        FROM moneyflow
        WHERE trade_date >= '{PRICE_START}' AND trade_date <= '{END_DATE}'
          AND net_mf_amount IS NOT NULL
        ORDER BY ts_code, trade_date
    """
    df = pd.read_sql(sql, eng_stock)
    if df.empty:
        return pd.DataFrame(columns=["ts_code", "trade_date", "signal_type",
                                      "signal_name", "direction", "indicator"])

    df = df.sort_values(["ts_code", "trade_date"])
    df["roll_mean"] = df.groupby("ts_code")["net_mf_amount"].transform(
        lambda x: x.rolling(60, min_periods=20).mean()
    )
    df["roll_std"] = df.groupby("ts_code")["net_mf_amount"].transform(
        lambda x: x.rolling(60, min_periods=20).std()
    )

    df = df[(df["trade_date"] >= START_DATE) & (df["trade_date"] <= END_DATE)]
    df = df.dropna(subset=["roll_mean", "roll_std"])
    df = df[df["roll_std"] > 0]

    buy_mask = df["net_mf_amount"] > (df["roll_mean"] + 2 * df["roll_std"])
    buy_df = df[buy_mask].copy()
    buy_df["direction"] = "buy"
    buy_df["signal_name"] = "极端净流入"
    buy_df["signal_type"] = "extreme"

    sell_mask = df["net_mf_amount"] < (df["roll_mean"] - 2 * df["roll_std"])
    sell_df = df[sell_mask].copy()
    sell_df["direction"] = "sell"
    sell_df["signal_name"] = "极端净流出"
    sell_df["signal_type"] = "extreme"

    result = pd.concat([buy_df, sell_df], ignore_index=True)
    result["indicator"] = "资金流向"

    return result[["ts_code", "trade_date", "signal_type", "signal_name",
                    "direction", "indicator"]]


def load_price_data(ts_codes: list):
    """加载 qfq 收盘价，分批避免 IN 过长"""
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
        if (i // batch_size + 1) % 10 == 0:
            print(f"  批次 {i // batch_size + 1}")

    prices = pd.concat(dfs, ignore_index=True)
    print(f"价格数据: {len(prices)} 行, {time.time()-t0:.1f}s")
    return prices


def compute_multi_window_returns(signals: pd.DataFrame, prices: pd.DataFrame):
    """计算每个信号在多个窗口的收益率

    对每个信号, 计算 T+1, T+2, ..., T+60 的收益率(%)
    买入信号: ret > 0 则"胜"
    卖出信号: ret < 0 则"胜"（跌了 = 预判正确）
    """
    print(f"\n计算多窗口收益 ({len(signals)} 个信号, {len(WINDOWS)} 个窗口)...")
    t0 = time.time()

    # 构建价格查找字典
    prices = prices.sort_values(["ts_code", "trade_date"])
    price_groups = {}
    for ts_code, grp in prices.groupby("ts_code"):
        dates = grp["trade_date"].values
        closes = grp["close_qfq"].values
        price_groups[ts_code] = (dates, closes)

    # 结果列
    result_cols = {f"ret_{w}": [] for w in WINDOWS}
    result_cols.update({f"win_{w}": [] for w in WINDOWS})
    valid_mask = []

    processed = 0
    skipped = 0

    for _, row in signals.iterrows():
        ts_code = row["ts_code"]
        signal_date = row["trade_date"]
        direction = row["direction"]

        if ts_code not in price_groups:
            valid_mask.append(False)
            for w in WINDOWS:
                result_cols[f"ret_{w}"].append(np.nan)
                result_cols[f"win_{w}"].append(np.nan)
            skipped += 1
            continue

        dates, closes = price_groups[ts_code]
        idx = np.searchsorted(dates, signal_date)

        if idx >= len(dates) or dates[idx] != signal_date:
            valid_mask.append(False)
            for w in WINDOWS:
                result_cols[f"ret_{w}"].append(np.nan)
                result_cols[f"win_{w}"].append(np.nan)
            skipped += 1
            continue

        signal_close = closes[idx]
        if signal_close is None or signal_close <= 0 or np.isnan(signal_close):
            valid_mask.append(False)
            for w in WINDOWS:
                result_cols[f"ret_{w}"].append(np.nan)
                result_cols[f"win_{w}"].append(np.nan)
            skipped += 1
            continue

        has_any = False
        for w in WINDOWS:
            target_idx = idx + w
            if target_idx < len(dates):
                ret = (closes[target_idx] / signal_close - 1) * 100
                result_cols[f"ret_{w}"].append(ret)
                # 买入信号: ret > 0 为胜; 卖出信号: ret < 0 为胜
                if direction == "buy":
                    result_cols[f"win_{w}"].append(1 if ret > 0 else 0)
                else:
                    result_cols[f"win_{w}"].append(1 if ret < 0 else 0)
                has_any = True
            else:
                result_cols[f"ret_{w}"].append(np.nan)
                result_cols[f"win_{w}"].append(np.nan)

        valid_mask.append(has_any)
        if has_any:
            processed += 1
        else:
            skipped += 1

        if processed % 50000 == 0 and processed > 0:
            print(f"  已处理 {processed}, 跳过 {skipped}")

    print(f"完成: 有效 {processed}, 跳过 {skipped}, {time.time()-t0:.1f}s")

    # 合并结果
    for col, vals in result_cols.items():
        signals[col] = vals
    signals["valid"] = valid_mask

    return signals[signals["valid"]].copy()


def aggregate_decay_stats(df: pd.DataFrame):
    """按(指标, 信号名, 方向)聚合各窗口的胜率和平均收益

    输出: 每行一个信号类型, 列包含各窗口的胜率和平均收益
    """
    group_cols = ["indicator", "signal_name", "direction"]
    results = []

    for keys, grp in df.groupby(group_cols):
        indicator, signal_name, direction = keys
        row = {
            "indicator": indicator,
            "signal_name": signal_name,
            "direction": direction,
            "count": len(grp),
        }

        for w in WINDOWS:
            win_col = f"win_{w}"
            ret_col = f"ret_{w}"
            valid = grp[win_col].dropna()
            if len(valid) >= 30:  # 至少30个有效样本
                row[f"winrate_T{w}"] = round(valid.mean() * 100, 2)
                row[f"avgret_T{w}"] = round(grp[ret_col].dropna().mean(), 3)
                row[f"n_T{w}"] = len(valid)
            else:
                row[f"winrate_T{w}"] = np.nan
                row[f"avgret_T{w}"] = np.nan
                row[f"n_T{w}"] = len(valid)

        results.append(row)

    return pd.DataFrame(results)


def aggregate_indicator_level(df: pd.DataFrame):
    """按(指标, 方向)聚合 — 指标大类的衰减曲线"""
    group_cols = ["indicator", "direction"]
    results = []

    for keys, grp in df.groupby(group_cols):
        indicator, direction = keys
        row = {
            "indicator": indicator,
            "direction": direction,
            "count": len(grp),
        }

        for w in WINDOWS:
            win_col = f"win_{w}"
            ret_col = f"ret_{w}"
            valid = grp[win_col].dropna()
            if len(valid) >= 30:
                row[f"winrate_T{w}"] = round(valid.mean() * 100, 2)
                row[f"avgret_T{w}"] = round(grp[ret_col].dropna().mean(), 3)
            else:
                row[f"winrate_T{w}"] = np.nan
                row[f"avgret_T{w}"] = np.nan

        results.append(row)

    return pd.DataFrame(results)


def find_optimal_window(decay_df: pd.DataFrame):
    """找出每个信号的最佳窗口和失效窗口

    最佳窗口: 胜率最高的窗口
    失效窗口: 胜率首次降至50%±2%的窗口（买入信号<=52%或卖出信号<=52%）
    """
    winrate_cols = [f"winrate_T{w}" for w in WINDOWS]
    results = []

    for _, row in decay_df.iterrows():
        winrates = {}
        for w in WINDOWS:
            wr = row.get(f"winrate_T{w}")
            if pd.notna(wr):
                winrates[w] = wr

        if not winrates:
            continue

        # 最佳窗口
        best_window = max(winrates, key=winrates.get)
        best_winrate = winrates[best_window]

        # 失效窗口: 从最佳窗口之后, 胜率首次<=52%
        decay_window = None
        for w in sorted(winrates.keys()):
            if w > best_window and winrates[w] <= 52.0:
                decay_window = w
                break

        # 如果最佳窗口胜率本身就<=52%, 标记为"一直无效"
        always_noise = best_winrate <= 52.0

        results.append({
            "indicator": row["indicator"],
            "signal_name": row["signal_name"],
            "direction": row["direction"],
            "count": row["count"],
            "best_window": best_window,
            "best_winrate": best_winrate,
            "decay_window": decay_window,
            "decay_winrate": winrates.get(decay_window) if decay_window else None,
            "T60_winrate": winrates.get(60),
            "always_noise": always_noise,
        })

    return pd.DataFrame(results)


def print_decay_table(decay_df: pd.DataFrame, title: str):
    """打印衰减表格"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

    display_windows = [1, 3, 5, 7, 10, 15, 20, 30, 60]
    has_signal_name = "signal_name" in decay_df.columns

    if has_signal_name:
        header = f"{'指标':<8} {'信号':<20} {'方向':<5} {'数量':>6}"
    else:
        header = f"{'指标':<8} {'方向':<5} {'数量':>6}"
    for w in display_windows:
        header += f" {'T+'+str(w):>7}"
    print(header)
    print("-" * len(header))

    for _, row in decay_df.iterrows():
        if has_signal_name:
            line = f"{row['indicator']:<8} {row['signal_name']:<20} {row['direction']:<5} {row['count']:>6}"
        else:
            line = f"{row['indicator']:<8} {row['direction']:<5} {row['count']:>6}"
        for w in display_windows:
            wr = row.get(f"winrate_T{w}")
            if pd.notna(wr):
                line += f" {wr:>6.1f}%"
            else:
                line += f" {'N/A':>7}"
        print(line)


def main():
    """主流程"""
    print("=" * 60)
    print("  信号有效期衰减分析")
    print(f"  评估窗口: {WINDOWS}")
    print(f"  数据范围: {START_DATE} ~ {END_DATE}")
    print("=" * 60)

    # 1. 加载信号
    signals = load_signals()

    # 2. 加载价格
    ts_codes = signals["ts_code"].unique().tolist()
    prices = load_price_data(ts_codes)

    # 3. 计算多窗口收益
    df = compute_multi_window_returns(signals, prices)

    # 4. 聚合统计
    # 4.1 信号级别
    signal_decay = aggregate_decay_stats(df)
    signal_decay = signal_decay.sort_values(
        ["indicator", "direction", "count"], ascending=[True, True, False]
    )

    # 4.2 指标级别
    indicator_decay = aggregate_indicator_level(df)
    indicator_decay = indicator_decay.sort_values(["indicator", "direction"])

    # 4.3 找最佳窗口和失效窗口
    # 过滤掉样本太少的信号
    signal_decay_filtered = signal_decay[signal_decay["count"] >= 100]
    optimal = find_optimal_window(signal_decay_filtered)
    optimal = optimal.sort_values(["indicator", "direction", "count"],
                                   ascending=[True, True, False])

    # 5. 打印结果
    print_decay_table(indicator_decay, "指标级别胜率衰减 (买入=ret>0为胜, 卖出=ret<0为胜)")
    print()
    print_decay_table(signal_decay_filtered, "信号级别胜率衰减 (样本≥100)")

    print(f"\n{'='*80}")
    print(f"  最佳窗口与失效窗口")
    print(f"{'='*80}")
    print(f"{'指标':<8} {'信号':<20} {'方向':<5} {'数量':>6} {'最佳窗口':>8} {'最佳胜率':>8} {'失效窗口':>8} {'T+60胜率':>8} {'始终噪声':>8}")
    print("-" * 90)
    for _, row in optimal.iterrows():
        decay_str = f"T+{int(row['decay_window'])}" if pd.notna(row.get('decay_window')) else "未失效"
        noise_str = "是" if row["always_noise"] else "否"
        t60_str = f"{row['T60_winrate']:.1f}%" if pd.notna(row.get('T60_winrate')) else "N/A"
        print(f"{row['indicator']:<8} {row['signal_name']:<20} {row['direction']:<5} "
              f"{row['count']:>6} {'T+'+str(int(row['best_window'])):>8} "
              f"{row['best_winrate']:>7.1f}% {decay_str:>8} {t60_str:>8} {noise_str:>8}")

    # 6. 保存结果
    output_dir = os.path.dirname(os.path.abspath(__file__))

    signal_decay.to_csv(os.path.join(output_dir, "signal_decay_by_signal.csv"),
                        index=False, encoding="utf-8-sig")
    indicator_decay.to_csv(os.path.join(output_dir, "signal_decay_by_indicator.csv"),
                           index=False, encoding="utf-8-sig")
    optimal.to_csv(os.path.join(output_dir, "signal_optimal_window.csv"),
                   index=False, encoding="utf-8-sig")

    print(f"\n结果已保存:")
    print(f"  - signal_decay_by_signal.csv")
    print(f"  - signal_decay_by_indicator.csv")
    print(f"  - signal_optimal_window.csv")


if __name__ == "__main__":
    main()
