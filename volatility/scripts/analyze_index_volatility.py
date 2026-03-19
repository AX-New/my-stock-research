"""指数波动率分析 — Layer 1-2 研究

核心问题:
1. ATR 能否准确预测次日价格振幅？
2. 哪个 ATR 周期预测最准？
3. 不同波动率指标的预测能力对比
4. 牛熊市场下波动率特征差异
5. ATR 用于买卖点定价的置信区间

分析维度:
- 7大指数: 上证/深证/创业板/上证50/沪深300/中证500/中证1000
- 3个频率: 日线/周线/月线
- 4个ATR周期: 5/10/14/20
- 牛熊分组

实用输出:
- ATR预测次日振幅的准确率（覆盖率）
- 不同置信系数下的覆盖率（ATR*0.5/0.8/1.0/1.2/1.5）
- 买入日低点偏离开盘价的ATR倍数分布
- 卖出日高点偏离开盘价的ATR倍数分布

用法:
  python analyze_index_volatility.py
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import read_engine
from kline_loader import load_index_kline
from volatility_calc import (
    calc_tr, calc_atr, calc_atr_ratio, calc_daily_range_pct,
    calc_adr, calc_historical_volatility, calc_bbw,
    calc_all_volatility_indicators,
)

# 导入牛熊标记
_macd_scripts = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'macd', 'scripts'))
sys.path.insert(0, _macd_scripts)
from bull_bear_phases import tag_trend

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)


# ── 常量 ──────────────────────────────────────────────────────

# 7大指数
INDEX_CODES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
}

# ATR 置信系数 — 用于评估覆盖率
# 例如 factor=1.0 表示: 次日振幅 <= ATR 的概率
ATR_FACTORS = [0.5, 0.8, 1.0, 1.2, 1.5, 2.0]

# ATR 周期
ATR_PERIODS = [5, 10, 14, 20]

# 输出目录
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)


# ── 分析函数 ──────────────────────────────────────────────────

def analyze_atr_prediction(df: pd.DataFrame, atr_period: int = 14) -> dict:
    """分析 ATR 对次日振幅的预测能力

    返回:
    - coverage: 各 factor 下 ATR*factor >= 次日振幅 的比例
    - mae: ATR_ratio 与次日振幅% 的均值绝对误差
    - mape: 平均绝对百分比误差
    - bias: ATR_ratio - 次日振幅% 的均值（正=高估，负=低估）
    - stats: 次日振幅的描述统计
    """
    atr_ratio = calc_atr_ratio(df, atr_period)
    range_pct = calc_daily_range_pct(df)

    # 用当日ATR预测次日振幅
    pred = atr_ratio.shift(1)  # T日ATR → 预测T+1日振幅
    actual = range_pct  # T+1日实际振幅

    # 对齐有效数据
    mask = pred.notna() & actual.notna() & (actual > 0)
    pred = pred[mask]
    actual = actual[mask]

    if len(pred) == 0:
        return None

    # 覆盖率: pred * factor >= actual 的比例
    coverage = {}
    for f in ATR_FACTORS:
        coverage[f] = (pred * f >= actual).mean() * 100

    # 误差统计
    error = pred - actual
    mae = error.abs().mean()
    mape = (error.abs() / actual).mean() * 100
    bias = error.mean()

    # 实际振幅描述统计
    stats = {
        "count": len(actual),
        "mean": actual.mean(),
        "std": actual.std(),
        "median": actual.median(),
        "p25": actual.quantile(0.25),
        "p75": actual.quantile(0.75),
        "p90": actual.quantile(0.90),
        "p95": actual.quantile(0.95),
        "max": actual.max(),
    }

    return {
        "coverage": coverage,
        "mae": mae,
        "mape": mape,
        "bias": bias,
        "stats": stats,
        "n": len(pred),
    }


def analyze_buy_sell_deviation(df: pd.DataFrame, atr_period: int = 14) -> dict:
    """分析买入日低点、卖出日高点偏离开盘价的 ATR 倍数

    买入场景: 低点 = 开盘价 - X * ATR, 求X的分布
    卖出场景: 高点 = 开盘价 + Y * ATR, 求Y的分布
    """
    atr = calc_atr(df, atr_period)

    open_price = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # 用前一日ATR作为预测值
    prev_atr = atr.shift(1)

    # 低点偏离开盘价（买入侧）: (Open - Low) / ATR
    buy_dev = (open_price - low) / prev_atr
    # 高点偏离开盘价（卖出侧）: (High - Open) / ATR
    sell_dev = (high - open_price) / prev_atr

    mask = prev_atr.notna() & (prev_atr > 0)
    buy_dev = buy_dev[mask]
    sell_dev = sell_dev[mask]

    def _stats(s):
        return {
            "mean": s.mean(),
            "median": s.median(),
            "std": s.std(),
            "p25": s.quantile(0.25),
            "p75": s.quantile(0.75),
            "p80": s.quantile(0.80),
            "p90": s.quantile(0.90),
            "p95": s.quantile(0.95),
        }

    return {
        "buy_deviation": _stats(buy_dev),
        "sell_deviation": _stats(sell_dev),
        "n": len(buy_dev),
    }


def analyze_multi_indicator_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """对比多个波动率指标的预测能力

    比较: ATR(5/10/14/20), ADR(14/20), HV(10/20/60), BBW(20)
    对次日振幅的预测准确度
    """
    range_pct = calc_daily_range_pct(df)
    actual_next = range_pct.shift(-1)  # 次日实际振幅

    results = []

    # ATR ratio 各周期
    for p in ATR_PERIODS:
        pred = calc_atr_ratio(df, p)
        mask = pred.notna() & actual_next.notna() & (actual_next > 0)
        if mask.sum() < 50:
            continue
        error = pred[mask] - actual_next[mask]
        corr = pred[mask].corr(actual_next[mask])
        coverage_1x = (pred[mask] >= actual_next[mask]).mean() * 100
        results.append({
            "indicator": f"ATR_ratio({p})",
            "MAE": error.abs().mean(),
            "MAPE%": (error.abs() / actual_next[mask]).mean() * 100,
            "bias": error.mean(),
            "corr": corr,
            "coverage_1x%": coverage_1x,
            "n": mask.sum(),
        })

    # ADR
    for p in [14, 20]:
        pred = calc_adr(df, p)
        mask = pred.notna() & actual_next.notna() & (actual_next > 0)
        if mask.sum() < 50:
            continue
        error = pred[mask] - actual_next[mask]
        corr = pred[mask].corr(actual_next[mask])
        coverage_1x = (pred[mask] >= actual_next[mask]).mean() * 100
        results.append({
            "indicator": f"ADR({p})",
            "MAE": error.abs().mean(),
            "MAPE%": (error.abs() / actual_next[mask]).mean() * 100,
            "bias": error.mean(),
            "corr": corr,
            "coverage_1x%": coverage_1x,
            "n": mask.sum(),
        })

    # HV → 日波动率（年化 / sqrt(252)）
    for p in [10, 20, 60]:
        hv = calc_historical_volatility(df, p)
        # 年化HV → 日波动率: HV / sqrt(252)
        daily_hv = hv / np.sqrt(252)
        mask = daily_hv.notna() & actual_next.notna() & (actual_next > 0)
        if mask.sum() < 50:
            continue
        error = daily_hv[mask] - actual_next[mask]
        corr = daily_hv[mask].corr(actual_next[mask])
        coverage_1x = (daily_hv[mask] >= actual_next[mask]).mean() * 100
        results.append({
            "indicator": f"HV_daily({p})",
            "MAE": error.abs().mean(),
            "MAPE%": (error.abs() / actual_next[mask]).mean() * 100,
            "bias": error.mean(),
            "corr": corr,
            "coverage_1x%": coverage_1x,
            "n": mask.sum(),
        })

    return pd.DataFrame(results)


def analyze_bull_bear_volatility(df: pd.DataFrame, atr_period: int = 14) -> dict:
    """牛熊市分组分析波动率特征"""
    # 逐行标记牛熊趋势（tag_trend接收单个日期字符串）
    df_tagged = df.copy()
    df_tagged["trend"] = df_tagged["trade_date"].apply(tag_trend)

    atr_ratio = calc_atr_ratio(df, atr_period)
    range_pct = calc_daily_range_pct(df)

    df_tagged["atr_ratio"] = atr_ratio
    df_tagged["range_pct"] = range_pct

    mask = df_tagged["atr_ratio"].notna() & df_tagged["range_pct"].notna()
    df_valid = df_tagged[mask].copy()

    results = {}
    for trend_name in ["全量", "牛市", "熊市"]:
        if trend_name == "全量":
            subset = df_valid
        elif trend_name == "牛市":
            subset = df_valid[df_valid["trend"] == "bull"]
        else:
            subset = df_valid[df_valid["trend"] == "bear"]

        if len(subset) < 30:
            continue

        # ATR预测（shift后）
        pred = subset["atr_ratio"].shift(1)
        actual = subset["range_pct"]
        valid = pred.notna() & actual.notna() & (actual > 0)
        pred_v = pred[valid]
        actual_v = actual[valid]

        coverage = {}
        for f in ATR_FACTORS:
            coverage[f] = (pred_v * f >= actual_v).mean() * 100

        results[trend_name] = {
            "n": len(actual_v),
            "atr_ratio_mean": subset["atr_ratio"].mean(),
            "range_pct_mean": subset["range_pct"].mean(),
            "range_pct_median": subset["range_pct"].median(),
            "range_pct_p90": subset["range_pct"].quantile(0.90),
            "coverage": coverage,
        }

    return results


# ── 主分析流程 ──────────────────────────────────────────────────

def run_analysis():
    """执行完整的指数波动率分析"""
    start_time = time.time()
    log.info("=" * 70)
    log.info("指数波动率分析 - Layer 1-2")
    log.info("=" * 70)

    all_results = []
    all_coverage = []
    all_deviation = []
    all_indicator_cmp = []
    all_bull_bear = []

    for ts_code, name in INDEX_CODES.items():
        log.info(f"\n{'─' * 50}")
        log.info(f"分析: {name} ({ts_code})")

        # 加载日线数据
        df = load_index_kline(ts_code, "daily")
        if df.empty or len(df) < 100:
            log.warning(f"  {name} 数据不足，跳过")
            continue

        log.info(f"  数据量: {len(df)} 根K线, {df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")

        # 1. ATR 预测准确度（各周期）
        for period in ATR_PERIODS:
            result = analyze_atr_prediction(df, period)
            if result:
                row = {
                    "index": name,
                    "ts_code": ts_code,
                    "atr_period": period,
                    "n": result["n"],
                    "mae": round(result["mae"], 4),
                    "mape": round(result["mape"], 2),
                    "bias": round(result["bias"], 4),
                    "range_mean": round(result["stats"]["mean"], 4),
                    "range_median": round(result["stats"]["median"], 4),
                    "range_p90": round(result["stats"]["p90"], 4),
                    "range_p95": round(result["stats"]["p95"], 4),
                }
                # 覆盖率
                for f in ATR_FACTORS:
                    row[f"cov_{f}x"] = round(result["coverage"][f], 1)
                all_results.append(row)

        # 2. 买卖偏离分析
        dev = analyze_buy_sell_deviation(df, 14)
        if dev:
            all_deviation.append({
                "index": name,
                "ts_code": ts_code,
                "n": dev["n"],
                **{f"buy_{k}": round(v, 4) for k, v in dev["buy_deviation"].items()},
                **{f"sell_{k}": round(v, 4) for k, v in dev["sell_deviation"].items()},
            })

        # 3. 多指标对比
        cmp_df = analyze_multi_indicator_comparison(df)
        if not cmp_df.empty:
            cmp_df.insert(0, "index", name)
            all_indicator_cmp.append(cmp_df)

        # 4. 牛熊分组
        bb = analyze_bull_bear_volatility(df, 14)
        if bb:
            for trend_name, data in bb.items():
                row = {
                    "index": name,
                    "trend": trend_name,
                    "n": data["n"],
                    "atr_ratio_mean": round(data["atr_ratio_mean"], 4),
                    "range_pct_mean": round(data["range_pct_mean"], 4),
                    "range_pct_median": round(data["range_pct_median"], 4),
                    "range_pct_p90": round(data["range_pct_p90"], 4),
                }
                for f in ATR_FACTORS:
                    row[f"cov_{f}x"] = round(data["coverage"][f], 1)
                all_bull_bear.append(row)

    # ── 保存结果 ──────────────────────────────────────────────

    # ATR预测准确度
    df_results = pd.DataFrame(all_results)
    out_path = os.path.join(DATA_DIR, "index_atr_prediction.csv")
    df_results.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"\nATR预测结果 → {out_path}")

    # 买卖偏离
    df_dev = pd.DataFrame(all_deviation)
    out_path = os.path.join(DATA_DIR, "index_buy_sell_deviation.csv")
    df_dev.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"买卖偏离结果 → {out_path}")

    # 多指标对比
    if all_indicator_cmp:
        df_cmp = pd.concat(all_indicator_cmp, ignore_index=True)
        out_path = os.path.join(DATA_DIR, "index_indicator_comparison.csv")
        df_cmp.to_csv(out_path, index=False, encoding="utf-8-sig")
        log.info(f"指标对比结果 → {out_path}")

    # 牛熊分组
    df_bb = pd.DataFrame(all_bull_bear)
    out_path = os.path.join(DATA_DIR, "index_bull_bear_volatility.csv")
    df_bb.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"牛熊分组结果 → {out_path}")

    # ── 打印摘要 ──────────────────────────────────────────────
    _print_summary(df_results, df_dev, df_bb)

    elapsed = time.time() - start_time
    log.info(f"\n分析完成，耗时 {elapsed:.1f}s")


def _print_summary(df_results, df_dev, df_bb):
    """打印分析摘要"""
    print("\n" + "=" * 70)
    print("一、ATR 预测次日振幅的覆盖率（ATR(14) 日线）")
    print("=" * 70)

    atr14 = df_results[df_results["atr_period"] == 14]
    if not atr14.empty:
        print(f"\n{'指数':<10} {'样本量':>6} {'振幅均值%':>8} {'MAE':>6} "
              f"{'0.5x':>6} {'0.8x':>6} {'1.0x':>6} {'1.2x':>6} {'1.5x':>6} {'2.0x':>6}")
        print("-" * 78)
        for _, row in atr14.iterrows():
            print(f"{row['index']:<10} {row['n']:>6} {row['range_mean']:>8.4f} {row['mae']:>6.4f} "
                  f"{row['cov_0.5x']:>5.1f}% {row['cov_0.8x']:>5.1f}% {row['cov_1.0x']:>5.1f}% "
                  f"{row['cov_1.2x']:>5.1f}% {row['cov_1.5x']:>5.1f}% {row['cov_2.0x']:>5.1f}%")

    print("\n" + "=" * 70)
    print("二、不同 ATR 周期对比（上证指数）")
    print("=" * 70)

    sh = df_results[df_results["ts_code"] == "000001.SH"]
    if not sh.empty:
        print(f"\n{'ATR周期':>8} {'MAE':>8} {'MAPE%':>8} {'1.0x覆盖':>10} {'1.5x覆盖':>10}")
        print("-" * 48)
        for _, row in sh.iterrows():
            print(f"ATR({row['atr_period']:>2}) {row['mae']:>8.4f} {row['mape']:>7.2f}% "
                  f"{row['cov_1.0x']:>9.1f}% {row['cov_1.5x']:>9.1f}%")

    print("\n" + "=" * 70)
    print("三、买入日低点 / 卖出日高点 偏离开盘价的 ATR(14) 倍数")
    print("=" * 70)

    if not df_dev.empty:
        print(f"\n{'指数':<10} {'买入侧':>38} {'卖出侧':>38}")
        print(f"{'':10} {'均值':>8} {'中位':>6} {'P80':>6} {'P90':>6} {'P95':>6}"
              f"   {'均值':>6} {'中位':>6} {'P80':>6} {'P90':>6} {'P95':>6}")
        print("-" * 90)
        for _, row in df_dev.iterrows():
            print(f"{row['index']:<10} "
                  f"{row['buy_mean']:>8.4f} {row['buy_median']:>6.4f} "
                  f"{row['buy_p80']:>6.4f} {row['buy_p90']:>6.4f} {row['buy_p95']:>6.4f}"
                  f"   {row['sell_mean']:>6.4f} {row['sell_median']:>6.4f} "
                  f"{row['sell_p80']:>6.4f} {row['sell_p90']:>6.4f} {row['sell_p95']:>6.4f}")

    print("\n" + "=" * 70)
    print("四、牛熊市波动率差异（ATR(14) 日线）")
    print("=" * 70)

    if not df_bb.empty:
        # 只看上证指数的牛熊对比
        sh_bb = df_bb[df_bb["index"] == "上证指数"]
        if not sh_bb.empty:
            print(f"\n上证指数:")
            print(f"{'趋势':>6} {'样本':>6} {'ATR_ratio均值':>12} {'振幅均值%':>10} {'1.0x覆盖':>10} {'1.5x覆盖':>10}")
            print("-" * 60)
            for _, row in sh_bb.iterrows():
                print(f"{row['trend']:>6} {row['n']:>6} {row['atr_ratio_mean']:>12.4f} "
                      f"{row['range_pct_mean']:>10.4f} {row['cov_1.0x']:>9.1f}% {row['cov_1.5x']:>9.1f}%")


if __name__ == "__main__":
    run_analysis()
