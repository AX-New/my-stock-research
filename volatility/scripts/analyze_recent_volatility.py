"""近一年 vs 全历史 波动率对比分析

核心问题:
1. 近一年市场的 ATR 预测能力与 30 年全历史有何差异？
2. 买卖偏离 K 值在近一年数据下是否需要调整？
3. 输出可直接落地的限价策略参数

分析范围:
- 7 大指数 + 50 只抽样个股
- 时间窗口: 近 1 年（约 250 个交易日）vs 全历史
- 输出: CSV 数据 + 终端摘要

用法:
  python analyze_recent_volatility.py
  python analyze_recent_volatility.py --stock 600519.SH  # 分析单只股票
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import read_engine
from kline_loader import load_index_kline, load_stock_kline, get_all_stock_codes
from volatility_calc import calc_atr, calc_atr_ratio, calc_daily_range_pct, calc_tr

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# 7 大指数
INDEX_CODES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
}

# 近一年起始日期（取 14 个月数据，前 2 个月用于 ATR 热身）
WARMUP_START = (datetime.now() - timedelta(days=430)).strftime("%Y%m%d")
# 分析起始日期（近 1 年）
ANALYSIS_START = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")


def _calc_stats(df: pd.DataFrame, atr_period: int = 14) -> dict:
    """统一计算一段数据的 ATR 预测统计

    返回: coverage, K 值分布, MAE, bias 等
    需要 df 包含完整的 ATR 热身期数据
    """
    atr_ratio = calc_atr_ratio(df, atr_period)
    range_pct = calc_daily_range_pct(df)
    atr = calc_atr(df, atr_period)

    # ATR 预测次日振幅
    pred = atr_ratio.shift(1)
    actual = range_pct
    mask = pred.notna() & actual.notna() & (actual > 0)
    pred_v = pred[mask]
    actual_v = actual[mask]

    if len(pred_v) < 20:
        return None

    # 覆盖率
    coverage = {}
    for f in [0.5, 0.8, 1.0, 1.2, 1.5, 2.0]:
        coverage[f] = float((pred_v * f >= actual_v).mean() * 100)

    # 误差
    error = pred_v - actual_v
    mae = float(error.abs().mean())
    bias = float(error.mean())
    corr = float(pred_v.corr(actual_v))

    # 买卖偏离 K 值
    open_price = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    prev_atr = atr.shift(1)

    buy_dev = (open_price - low) / prev_atr
    sell_dev = (high - open_price) / prev_atr
    dev_mask = prev_atr.notna() & (prev_atr > 0)
    buy_dev = buy_dev[dev_mask]
    sell_dev = sell_dev[dev_mask]

    k_values = {}
    if len(buy_dev) > 20:
        for pct in [0.50, 0.60, 0.70, 0.80, 0.90]:
            k_values[f"K_buy_{int(pct*100)}"] = float(buy_dev.quantile(pct))
            k_values[f"K_sell_{int(pct*100)}"] = float(sell_dev.quantile(pct))

    return {
        "n": len(pred_v),
        "mae": mae,
        "bias": bias,
        "corr": corr,
        "coverage": coverage,
        "k_values": k_values,
        "atr_ratio_mean": float(atr_ratio.dropna().mean()),
        "range_pct_mean": float(range_pct.dropna().mean()),
    }


def analyze_index_comparison():
    """指数层面: 近一年 vs 全历史对比"""
    log.info("=" * 70)
    log.info("指数波动率 — 近一年 vs 全历史 对比分析")
    log.info("=" * 70)

    rows = []

    for ts_code, name in INDEX_CODES.items():
        log.info(f"分析: {name} ({ts_code})")

        # 加载全量数据
        df_full = load_index_kline(ts_code, "daily")
        if df_full.empty or len(df_full) < 100:
            log.warning(f"  {name} 数据不足，跳过")
            continue

        # 全历史统计
        stats_full = _calc_stats(df_full)
        if stats_full is None:
            continue

        # 近一年数据（取含热身期的数据，确保 ATR 有效）
        df_recent_raw = df_full[df_full["trade_date"] >= WARMUP_START].copy()
        if len(df_recent_raw) < 60:
            log.warning(f"  {name} 近一年数据不足，跳过")
            continue

        # 计算指标用全部热身数据，但统计只取近一年
        stats_recent_raw = _calc_stats(df_recent_raw)

        # 更精确: 在全量数据上算好 ATR，再截取近一年做统计
        atr_ratio_full = calc_atr_ratio(df_full, 14)
        range_pct_full = calc_daily_range_pct(df_full)
        atr_full = calc_atr(df_full, 14)

        recent_mask = df_full["trade_date"] >= ANALYSIS_START

        # 近一年的 ATR 预测统计
        pred = atr_ratio_full.shift(1)
        actual = range_pct_full
        valid = pred.notna() & actual.notna() & (actual > 0) & recent_mask
        pred_v = pred[valid]
        actual_v = actual[valid]

        n_recent = len(pred_v)
        if n_recent < 20:
            continue

        # 覆盖率
        cov_recent = {}
        for f in [0.5, 0.8, 1.0, 1.2, 1.5, 2.0]:
            cov_recent[f] = float((pred_v * f >= actual_v).mean() * 100)

        error = pred_v - actual_v
        mae_recent = float(error.abs().mean())
        bias_recent = float(error.mean())

        # 近一年买卖偏离
        open_price = df_full["open"].astype(float)
        high = df_full["high"].astype(float)
        low = df_full["low"].astype(float)
        prev_atr = atr_full.shift(1)

        buy_dev = (open_price - low) / prev_atr
        sell_dev = (high - open_price) / prev_atr
        dev_valid = prev_atr.notna() & (prev_atr > 0) & recent_mask
        buy_dev_recent = buy_dev[dev_valid]
        sell_dev_recent = sell_dev[dev_valid]

        k_recent = {}
        for pct in [0.50, 0.60, 0.70, 0.80, 0.90]:
            k_recent[f"K_buy_{int(pct*100)}"] = float(buy_dev_recent.quantile(pct))
            k_recent[f"K_sell_{int(pct*100)}"] = float(sell_dev_recent.quantile(pct))

        # 全历史 K 值
        buy_dev_all = (open_price - low) / prev_atr
        sell_dev_all = (high - open_price) / prev_atr
        dev_valid_all = prev_atr.notna() & (prev_atr > 0)
        buy_dev_full = buy_dev_all[dev_valid_all]
        sell_dev_full = sell_dev_all[dev_valid_all]

        k_full = {}
        for pct in [0.50, 0.60, 0.70, 0.80, 0.90]:
            k_full[f"K_buy_{int(pct*100)}"] = float(buy_dev_full.quantile(pct))
            k_full[f"K_sell_{int(pct*100)}"] = float(sell_dev_full.quantile(pct))

        # 近一年振幅均值
        range_recent = range_pct_full[recent_mask].dropna()
        atr_ratio_recent = atr_ratio_full[recent_mask].dropna()

        row = {
            "指数": name,
            "ts_code": ts_code,
            # 全历史
            "全历史_样本量": stats_full["n"],
            "全历史_ATR_ratio均值": round(stats_full["atr_ratio_mean"], 4),
            "全历史_振幅均值": round(stats_full["range_pct_mean"], 4),
            "全历史_1.0x覆盖率": round(stats_full["coverage"][1.0], 1),
            "全历史_1.5x覆盖率": round(stats_full["coverage"][1.5], 1),
            "全历史_MAE": round(stats_full["mae"], 4),
            # 近一年
            "近一年_样本量": n_recent,
            "近一年_ATR_ratio均值": round(float(atr_ratio_recent.mean()), 4),
            "近一年_振幅均值": round(float(range_recent.mean()), 4),
            "近一年_1.0x覆盖率": round(cov_recent[1.0], 1),
            "近一年_1.5x覆盖率": round(cov_recent[1.5], 1),
            "近一年_MAE": round(mae_recent, 4),
        }

        # K 值对比
        for pct in [50, 60, 70, 80, 90]:
            row[f"全历史_K_buy_{pct}"] = round(k_full[f"K_buy_{pct}"], 4)
            row[f"近一年_K_buy_{pct}"] = round(k_recent[f"K_buy_{pct}"], 4)
            row[f"全历史_K_sell_{pct}"] = round(k_full[f"K_sell_{pct}"], 4)
            row[f"近一年_K_sell_{pct}"] = round(k_recent[f"K_sell_{pct}"], 4)

        rows.append(row)

    df_result = pd.DataFrame(rows)

    # 保存
    out_path = os.path.join(DATA_DIR, "recent_vs_full_index.csv")
    df_result.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"指数对比结果 → {out_path}")

    return df_result


def analyze_stock_comparison(sample_size: int = 50):
    """个股层面: 近一年 vs 全历史对比（抽样）"""
    log.info("=" * 70)
    log.info(f"个股波动率 — 近一年 vs 全历史 对比分析（抽样 {sample_size} 只）")
    log.info("=" * 70)

    all_codes = get_all_stock_codes()
    if len(all_codes) > sample_size:
        indices = np.linspace(0, len(all_codes) - 1, sample_size, dtype=int)
        codes = [all_codes[i] for i in indices]
    else:
        codes = all_codes

    rows = []
    for i, ts_code in enumerate(codes):
        if (i + 1) % 10 == 0:
            log.info(f"  进度: {i + 1}/{len(codes)}")

        df = load_stock_kline(ts_code, "daily", "qfq")
        if df.empty or len(df) < 100:
            continue

        # 全历史
        stats_full = _calc_stats(df)
        if stats_full is None or "K_buy_50" not in stats_full.get("k_values", {}):
            continue

        # 近一年
        atr_ratio = calc_atr_ratio(df, 14)
        range_pct = calc_daily_range_pct(df)
        atr = calc_atr(df, 14)
        recent_mask = df["trade_date"] >= ANALYSIS_START

        if recent_mask.sum() < 50:
            continue

        pred = atr_ratio.shift(1)
        actual = range_pct
        valid = pred.notna() & actual.notna() & (actual > 0) & recent_mask
        pred_v = pred[valid]
        actual_v = actual[valid]

        if len(pred_v) < 30:
            continue

        # 覆盖率
        cov_recent_1x = float((pred_v >= actual_v).mean() * 100)

        # K 值
        open_price = df["open"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        prev_atr = atr.shift(1)

        buy_dev = (open_price - low) / prev_atr
        sell_dev = (high - open_price) / prev_atr
        dev_valid = prev_atr.notna() & (prev_atr > 0) & recent_mask
        buy_recent = buy_dev[dev_valid]
        sell_recent = sell_dev[dev_valid]

        if len(buy_recent) < 30:
            continue

        row = {
            "ts_code": ts_code,
            # 全历史
            "全历史_样本量": stats_full["n"],
            "全历史_1.0x覆盖率": round(stats_full["coverage"][1.0], 1),
            "全历史_K_buy_50": round(stats_full["k_values"]["K_buy_50"], 4),
            "全历史_K_buy_80": round(stats_full["k_values"]["K_buy_80"], 4),
            "全历史_K_sell_50": round(stats_full["k_values"]["K_sell_50"], 4),
            "全历史_K_sell_80": round(stats_full["k_values"]["K_sell_80"], 4),
            # 近一年
            "近一年_样本量": len(pred_v),
            "近一年_1.0x覆盖率": round(cov_recent_1x, 1),
            "近一年_K_buy_50": round(float(buy_recent.quantile(0.50)), 4),
            "近一年_K_buy_80": round(float(buy_recent.quantile(0.80)), 4),
            "近一年_K_sell_50": round(float(sell_recent.quantile(0.50)), 4),
            "近一年_K_sell_80": round(float(sell_recent.quantile(0.80)), 4),
        }
        rows.append(row)

    df_result = pd.DataFrame(rows)
    out_path = os.path.join(DATA_DIR, "recent_vs_full_stock.csv")
    df_result.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"个股对比结果 → {out_path}")

    return df_result


def analyze_single_stock_recent(ts_code: str):
    """单只股票的近一年 vs 全历史详细分析"""
    df = load_stock_kline(ts_code, "daily", "qfq")
    if df.empty or len(df) < 100:
        log.error(f"{ts_code} 数据不足")
        return None

    atr_ratio = calc_atr_ratio(df, 14)
    range_pct = calc_daily_range_pct(df)
    atr = calc_atr(df, 14)
    recent_mask = df["trade_date"] >= ANALYSIS_START

    # 全历史统计
    stats_full = _calc_stats(df)

    # 近一年
    pred = atr_ratio.shift(1)
    actual = range_pct
    valid_recent = pred.notna() & actual.notna() & (actual > 0) & recent_mask
    pred_r = pred[valid_recent]
    actual_r = actual[valid_recent]

    open_price = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    prev_atr = atr.shift(1)

    buy_dev = (open_price - low) / prev_atr
    sell_dev = (high - open_price) / prev_atr
    dev_valid = prev_atr.notna() & (prev_atr > 0) & recent_mask
    buy_recent = buy_dev[dev_valid]
    sell_recent = sell_dev[dev_valid]

    # 最新 ATR
    latest_close = float(df["close"].iloc[-1])
    latest_atr = float(atr.iloc[-1]) if atr.notna().any() else 0
    latest_date = df["trade_date"].iloc[-1]

    print(f"\n{'=' * 70}")
    print(f"  {ts_code} — 近一年 vs 全历史 波动率对比")
    print(f"{'=' * 70}")
    print(f"  最新日期: {latest_date}  收盘价: {latest_close:.2f}  ATR(14): {latest_atr:.2f}")

    # 覆盖率对比
    print(f"\n--- ATR(14) 覆盖率对比 ---")
    print(f"{'倍数':>8} {'全历史':>10} {'近一年':>10} {'差异':>10}")
    print("-" * 42)
    for f in [0.8, 1.0, 1.2, 1.5]:
        if stats_full:
            cov_full = stats_full["coverage"][f]
        else:
            cov_full = 0
        if len(pred_r) > 0:
            cov_recent = float((pred_r * f >= actual_r).mean() * 100)
        else:
            cov_recent = 0
        diff = cov_recent - cov_full
        print(f"  {f}x   {cov_full:>8.1f}%  {cov_recent:>8.1f}%  {diff:>+8.1f}%")

    # K 值对比
    print(f"\n--- 买入 K 值对比 (开盘价 - K*ATR = 限价) ---")
    print(f"{'置信度':>8} {'全历史':>10} {'近一年':>10} {'差异':>10}")
    print("-" * 42)

    dev_valid_all = prev_atr.notna() & (prev_atr > 0)
    buy_all = buy_dev[dev_valid_all]
    sell_all = sell_dev[dev_valid_all]

    for pct in [0.50, 0.60, 0.70, 0.80, 0.90]:
        k_full = float(buy_all.quantile(pct)) if len(buy_all) > 0 else 0
        k_recent = float(buy_recent.quantile(pct)) if len(buy_recent) > 0 else 0
        diff = k_recent - k_full
        print(f"  {int(pct*100)}%   {k_full:>10.4f} {k_recent:>10.4f} {diff:>+10.4f}")

    print(f"\n--- 卖出 K 值对比 (开盘价 + K*ATR = 限价) ---")
    print(f"{'置信度':>8} {'全历史':>10} {'近一年':>10} {'差异':>10}")
    print("-" * 42)
    for pct in [0.50, 0.60, 0.70, 0.80, 0.90]:
        k_full = float(sell_all.quantile(pct)) if len(sell_all) > 0 else 0
        k_recent = float(sell_dev[dev_valid].quantile(pct)) if len(sell_recent) > 0 else 0
        diff = k_recent - k_full
        print(f"  {int(pct*100)}%   {k_full:>10.4f} {k_recent:>10.4f} {diff:>+10.4f}")

    # 实际限价示例
    if latest_atr > 0 and len(buy_recent) > 20:
        print(f"\n--- 基于近一年 K 值的限价示例 ---")
        print(f"  假设次日开盘价 ≈ {latest_close:.2f}")
        print(f"\n  买入:")
        for pct, label in [(0.50, "50%"), (0.70, "70%"), (0.80, "80%")]:
            k = float(buy_recent.quantile(pct))
            price = latest_close - k * latest_atr
            print(f"    {label}概率成交: {price:.2f} (K={k:.4f}, 偏移={k*latest_atr:.2f})")
        print(f"\n  卖出:")
        for pct, label in [(0.50, "50%"), (0.70, "70%"), (0.80, "80%")]:
            k = float(sell_recent.quantile(pct))
            price = latest_close + k * latest_atr
            print(f"    {label}概率成交: {price:.2f} (K={k:.4f}, 偏移={k*latest_atr:.2f})")


def print_index_summary(df: pd.DataFrame):
    """打印指数对比摘要"""
    if df.empty:
        return

    print(f"\n{'=' * 80}")
    print("一、ATR(14) 覆盖率 — 近一年 vs 全历史")
    print(f"{'=' * 80}")
    print(f"\n{'指数':<10} {'全历史(1.0x)':>12} {'近一年(1.0x)':>12} {'差异':>8} "
          f"{'全历史(1.5x)':>12} {'近一年(1.5x)':>12}")
    print("-" * 72)
    for _, row in df.iterrows():
        diff_1x = row["近一年_1.0x覆盖率"] - row["全历史_1.0x覆盖率"]
        print(f"{row['指数']:<10} {row['全历史_1.0x覆盖率']:>10.1f}%  {row['近一年_1.0x覆盖率']:>10.1f}%  "
              f"{diff_1x:>+6.1f}%  {row['全历史_1.5x覆盖率']:>10.1f}%  {row['近一年_1.5x覆盖率']:>10.1f}%")

    print(f"\n{'=' * 80}")
    print("二、买入 K 值对比（K_buy: 开盘价 - K×ATR = 限价）")
    print(f"{'=' * 80}")
    print(f"\n{'指数':<10} {'全历史K50':>10} {'近一年K50':>10} {'全历史K70':>10} {'近一年K70':>10} "
          f"{'全历史K80':>10} {'近一年K80':>10}")
    print("-" * 72)
    for _, row in df.iterrows():
        print(f"{row['指数']:<10} "
              f"{row['全历史_K_buy_50']:>10.4f} {row['近一年_K_buy_50']:>10.4f} "
              f"{row['全历史_K_buy_70']:>10.4f} {row['近一年_K_buy_70']:>10.4f} "
              f"{row['全历史_K_buy_80']:>10.4f} {row['近一年_K_buy_80']:>10.4f}")

    print(f"\n{'=' * 80}")
    print("三、卖出 K 值对比（K_sell: 开盘价 + K×ATR = 限价）")
    print(f"{'=' * 80}")
    print(f"\n{'指数':<10} {'全历史K50':>10} {'近一年K50':>10} {'全历史K70':>10} {'近一年K70':>10} "
          f"{'全历史K80':>10} {'近一年K80':>10}")
    print("-" * 72)
    for _, row in df.iterrows():
        print(f"{row['指数']:<10} "
              f"{row['全历史_K_sell_50']:>10.4f} {row['近一年_K_sell_50']:>10.4f} "
              f"{row['全历史_K_sell_70']:>10.4f} {row['近一年_K_sell_70']:>10.4f} "
              f"{row['全历史_K_sell_80']:>10.4f} {row['近一年_K_sell_80']:>10.4f}")

    # 近一年均值（实用参数）
    print(f"\n{'=' * 80}")
    print("四、近一年推荐 K 值（7 大指数均值）— 可直接用于限价策略")
    print(f"{'=' * 80}")
    for pct in [50, 60, 70, 80, 90]:
        k_buy = df[f"近一年_K_buy_{pct}"].mean()
        k_sell = df[f"近一年_K_sell_{pct}"].mean()
        print(f"  {pct}% 置信度:  K_buy = {k_buy:.4f}  K_sell = {k_sell:.4f}")


def print_stock_summary(df: pd.DataFrame):
    """打印个股对比摘要"""
    if df.empty:
        return

    print(f"\n{'=' * 70}")
    print("五、个股 K 值对比（抽样统计均值）")
    print(f"{'=' * 70}")

    print(f"\n{'指标':>20} {'全历史均值':>12} {'近一年均值':>12} {'差异':>10}")
    print("-" * 56)
    for col_pair in [
        ("全历史_K_buy_50", "近一年_K_buy_50", "K_buy_50"),
        ("全历史_K_buy_80", "近一年_K_buy_80", "K_buy_80"),
        ("全历史_K_sell_50", "近一年_K_sell_50", "K_sell_50"),
        ("全历史_K_sell_80", "近一年_K_sell_80", "K_sell_80"),
        ("全历史_1.0x覆盖率", "近一年_1.0x覆盖率", "1.0x覆盖率"),
    ]:
        full_mean = df[col_pair[0]].mean()
        recent_mean = df[col_pair[1]].mean()
        diff = recent_mean - full_mean
        print(f"{col_pair[2]:>20} {full_mean:>12.4f} {recent_mean:>12.4f} {diff:>+10.4f}")


def main():
    parser = argparse.ArgumentParser(description="近一年 vs 全历史波动率对比")
    parser.add_argument("--stock", type=str, default=None,
                        help="分析单只股票（如 600519.SH）")
    parser.add_argument("--sample", type=int, default=50,
                        help="个股抽样数量")
    args = parser.parse_args()

    start_time = time.time()

    if args.stock:
        analyze_single_stock_recent(args.stock)
    else:
        # 指数分析
        df_index = analyze_index_comparison()
        print_index_summary(df_index)

        # 个股抽样分析
        df_stock = analyze_stock_comparison(args.sample)
        print_stock_summary(df_stock)

    elapsed = time.time() - start_time
    log.info(f"\n总耗时: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
