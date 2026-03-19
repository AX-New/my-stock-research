"""个股波动率分析 — Layer 4 研究

核心问题:
1. ATR 预测个股次日振幅的准确度如何？
2. 不同市值/行业的个股波动特征差异
3. 个股ATR作为买卖价格偏移量的可靠性
4. 实用工具: 给定股票代码，输出振幅预测和建议买卖价

分析模式:
  --mode batch     全市场批量统计（抽样500只）
  --mode single    单只股票详细分析
  --mode predict   实时预测模式（给出具体买卖建议价）

用法:
  python analyze_stock_volatility.py --mode batch
  python analyze_stock_volatility.py --mode single --code 600519.SH
  python analyze_stock_volatility.py --mode predict --code 600519.SH
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
from sqlalchemy import text

from database import read_engine
from kline_loader import load_stock_kline, get_all_stock_codes
from volatility_calc import (
    calc_tr, calc_atr, calc_atr_ratio, calc_daily_range_pct,
    calc_adr, calc_historical_volatility,
)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

ATR_FACTORS = [0.5, 0.8, 1.0, 1.2, 1.5, 2.0]


# ── 行业分类 ──────────────────────────────────────────────────

def load_stock_industry() -> pd.DataFrame:
    """读取股票的申万一级行业归属"""
    with read_engine.connect() as conn:
        sql = text("""
            SELECT ts_code, l1_name as industry_name
            FROM index_member_all
            WHERE (out_date IS NULL OR out_date = '')
        """)
        result = conn.execute(sql)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def load_stock_basic_info() -> pd.DataFrame:
    """读取股票基本信息（市值等）"""
    with read_engine.connect() as conn:
        # 获取最新一天的市值数据
        sql = text("""
            SELECT ts_code, total_mv, circ_mv, turnover_rate
            FROM daily_basic
            WHERE trade_date = (SELECT MAX(trade_date) FROM daily_basic)
        """)
        result = conn.execute(sql)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


# ── 单股分析 ──────────────────────────────────────────────────

def analyze_single_stock(ts_code: str, adj: str = "qfq") -> dict:
    """分析单只股票的波动率特征

    返回各ATR周期的预测指标 + 买卖偏离统计
    """
    df = load_stock_kline(ts_code, "daily", adj)
    if df.empty or len(df) < 60:
        return None

    results = {}

    for period in [5, 10, 14, 20]:
        atr_ratio = calc_atr_ratio(df, period)
        range_pct = calc_daily_range_pct(df)

        # 用当日ATR预测次日振幅
        pred = atr_ratio.shift(1)
        actual = range_pct

        mask = pred.notna() & actual.notna() & (actual > 0)
        pred_v = pred[mask]
        actual_v = actual[mask]

        if len(pred_v) < 30:
            continue

        # 覆盖率
        coverage = {}
        for f in ATR_FACTORS:
            coverage[f] = (pred_v * f >= actual_v).mean() * 100

        # 误差
        error = pred_v - actual_v
        mae = error.abs().mean()

        results[f"atr_{period}"] = {
            "mae": mae,
            "coverage": coverage,
            "atr_ratio_latest": atr_ratio.iloc[-1] if atr_ratio.notna().any() else None,
        }

    # 买卖偏离（ATR(14)）
    atr = calc_atr(df, 14)
    open_price = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    prev_atr = atr.shift(1)

    buy_dev = (open_price - low) / prev_atr
    sell_dev = (high - open_price) / prev_atr

    mask = prev_atr.notna() & (prev_atr > 0)
    buy_dev = buy_dev[mask]
    sell_dev = sell_dev[mask]

    if len(buy_dev) > 30:
        results["buy_deviation"] = {
            "mean": buy_dev.mean(), "median": buy_dev.median(),
            "p80": buy_dev.quantile(0.80), "p90": buy_dev.quantile(0.90),
        }
        results["sell_deviation"] = {
            "mean": sell_dev.mean(), "median": sell_dev.median(),
            "p80": sell_dev.quantile(0.80), "p90": sell_dev.quantile(0.90),
        }

    # 最新数据
    results["latest"] = {
        "close": float(df["close"].iloc[-1]),
        "trade_date": df["trade_date"].iloc[-1],
        "data_count": len(df),
    }

    return results


# ── 批量分析 ──────────────────────────────────────────────────

def run_batch_analysis(sample_size: int = 500):
    """全市场抽样分析"""
    start_time = time.time()
    log.info("=" * 70)
    log.info(f"个股波动率批量分析 — 抽样 {sample_size} 只")
    log.info("=" * 70)

    # 获取所有股票代码
    all_codes = get_all_stock_codes()
    log.info(f"全市场股票: {len(all_codes)} 只")

    # 等间距抽样
    if len(all_codes) > sample_size:
        indices = np.linspace(0, len(all_codes) - 1, sample_size, dtype=int)
        codes = [all_codes[i] for i in indices]
    else:
        codes = all_codes

    # 加载行业信息
    df_industry = load_stock_industry()
    industry_map = dict(zip(df_industry["ts_code"], df_industry["industry_name"])) if not df_industry.empty else {}

    # 加载市值信息
    df_basic = load_stock_basic_info()
    mv_map = dict(zip(df_basic["ts_code"], df_basic["total_mv"])) if not df_basic.empty else {}

    batch_results = []
    success = 0
    fail = 0

    for i, ts_code in enumerate(codes):
        if (i + 1) % 50 == 0:
            log.info(f"  进度: {i + 1}/{len(codes)}")

        result = analyze_single_stock(ts_code)
        if result is None or "atr_14" not in result:
            fail += 1
            continue

        success += 1
        atr14 = result["atr_14"]

        row = {
            "ts_code": ts_code,
            "industry": industry_map.get(ts_code, ""),
            "total_mv": mv_map.get(ts_code, None),
            "close": result["latest"]["close"],
            "data_count": result["latest"]["data_count"],
            "atr_ratio_latest": round(atr14["atr_ratio_latest"], 4) if atr14["atr_ratio_latest"] else None,
            "mae": round(atr14["mae"], 4),
        }
        for f in ATR_FACTORS:
            row[f"cov_{f}x"] = round(atr14["coverage"][f], 1)

        if "buy_deviation" in result:
            row["buy_dev_median"] = round(result["buy_deviation"]["median"], 4)
            row["buy_dev_p80"] = round(result["buy_deviation"]["p80"], 4)
            row["sell_dev_median"] = round(result["sell_deviation"]["median"], 4)
            row["sell_dev_p80"] = round(result["sell_deviation"]["p80"], 4)

        batch_results.append(row)

    df_batch = pd.DataFrame(batch_results)

    # 保存
    out_path = os.path.join(DATA_DIR, "stock_batch_volatility.csv")
    df_batch.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"\n批量结果 → {out_path}")
    log.info(f"成功: {success}, 失败: {fail}")

    # 打印摘要
    _print_batch_summary(df_batch)

    elapsed = time.time() - start_time
    log.info(f"\n分析完成，耗时 {elapsed:.1f}s")


def _print_batch_summary(df: pd.DataFrame):
    """打印批量分析摘要"""
    print("\n" + "=" * 70)
    print("一、个股 ATR(14) 预测次日振幅 — 全样本统计")
    print("=" * 70)

    if df.empty:
        print("无有效数据")
        return

    print(f"\n样本量: {len(df)} 只股票")
    print(f"\nATR_ratio 最新值分布:")
    print(f"  均值: {df['atr_ratio_latest'].mean():.4f}%")
    print(f"  中位: {df['atr_ratio_latest'].median():.4f}%")
    print(f"  P25:  {df['atr_ratio_latest'].quantile(0.25):.4f}%")
    print(f"  P75:  {df['atr_ratio_latest'].quantile(0.75):.4f}%")

    print(f"\n覆盖率均值:")
    for f in ATR_FACTORS:
        col = f"cov_{f}x"
        if col in df.columns:
            print(f"  ATR*{f}: {df[col].mean():.1f}%")

    # 按行业分组
    if "industry" in df.columns and df["industry"].notna().any():
        print(f"\n" + "=" * 70)
        print("二、按行业分组 — ATR(14) 1.0x 覆盖率")
        print("=" * 70)

        grp = df.groupby("industry").agg({
            "cov_1.0x": "mean",
            "atr_ratio_latest": "mean",
            "ts_code": "count",
        }).rename(columns={"ts_code": "count"}).sort_values("cov_1.0x", ascending=False)

        print(f"\n{'行业':<12} {'数量':>4} {'ATR_ratio均值':>12} {'1.0x覆盖率':>10}")
        print("-" * 42)
        for ind, row in grp.iterrows():
            if row["count"] >= 3:
                print(f"{ind:<12} {row['count']:>4} {row['atr_ratio_latest']:>12.4f} {row['cov_1.0x']:>9.1f}%")

    # 按市值分组
    if "total_mv" in df.columns and df["total_mv"].notna().any():
        print(f"\n" + "=" * 70)
        print("三、按市值分组 — ATR(14) 覆盖率")
        print("=" * 70)

        df_mv = df[df["total_mv"].notna()].copy()
        # 市值单位: 万元 → 亿元
        df_mv["mv_billion"] = df_mv["total_mv"] / 10000

        bins = [0, 30, 100, 500, 1000, float("inf")]
        labels = ["<30亿", "30-100亿", "100-500亿", "500-1000亿", ">1000亿"]
        df_mv["mv_group"] = pd.cut(df_mv["mv_billion"], bins=bins, labels=labels)

        grp = df_mv.groupby("mv_group", observed=True).agg({
            "cov_1.0x": "mean",
            "atr_ratio_latest": "mean",
            "ts_code": "count",
        }).rename(columns={"ts_code": "count"})

        print(f"\n{'市值区间':<12} {'数量':>4} {'ATR_ratio均值':>12} {'1.0x覆盖率':>10}")
        print("-" * 42)
        for label, row in grp.iterrows():
            print(f"{label:<12} {row['count']:>4} {row['atr_ratio_latest']:>12.4f} {row['cov_1.0x']:>9.1f}%")

    # 买卖偏离统计
    if "buy_dev_median" in df.columns:
        print(f"\n" + "=" * 70)
        print("四、买卖偏离 ATR(14) 倍数 — 全样本统计")
        print("=" * 70)

        print(f"\n{'指标':>18} {'均值':>8} {'中位':>8} {'P25':>8} {'P75':>8}")
        print("-" * 50)
        for col, label in [
            ("buy_dev_median", "买入侧偏离(中位)"),
            ("buy_dev_p80", "买入侧偏离(P80)"),
            ("sell_dev_median", "卖出侧偏离(中位)"),
            ("sell_dev_p80", "卖出侧偏离(P80)"),
        ]:
            if col in df.columns:
                s = df[col].dropna()
                print(f"{label:>18} {s.mean():>8.4f} {s.median():>8.4f} "
                      f"{s.quantile(0.25):>8.4f} {s.quantile(0.75):>8.4f}")


# ── 单股详细分析 ──────────────────────────────────────────────

def run_single_analysis(ts_code: str):
    """单只股票详细波动率分析"""
    log.info(f"分析: {ts_code}")

    result = analyze_single_stock(ts_code)
    if result is None:
        log.error(f"  数据不足，无法分析")
        return

    print(f"\n{'=' * 60}")
    print(f"股票: {ts_code}")
    print(f"最新收盘: {result['latest']['close']}")
    print(f"最新日期: {result['latest']['trade_date']}")
    print(f"数据量: {result['latest']['data_count']} 根K线")
    print(f"{'=' * 60}")

    # ATR 各周期
    print(f"\n--- ATR 预测次日振幅覆盖率 ---")
    print(f"{'ATR周期':>8} {'MAE':>8} {'ATR_ratio%':>10} {'0.8x':>8} {'1.0x':>8} {'1.2x':>8} {'1.5x':>8}")
    print("-" * 60)
    for period in [5, 10, 14, 20]:
        key = f"atr_{period}"
        if key in result:
            d = result[key]
            atr_r = d["atr_ratio_latest"]
            atr_str = f"{atr_r:.4f}" if atr_r else "N/A"
            print(f"ATR({period:>2}) {d['mae']:>8.4f} {atr_str:>10} "
                  f"{d['coverage'][0.8]:>7.1f}% {d['coverage'][1.0]:>7.1f}% "
                  f"{d['coverage'][1.2]:>7.1f}% {d['coverage'][1.5]:>7.1f}%")

    # 买卖偏离
    if "buy_deviation" in result:
        print(f"\n--- 买卖偏离 ATR(14) 倍数 ---")
        bd = result["buy_deviation"]
        sd = result["sell_deviation"]
        print(f"买入侧(开盘价-低点): 均值={bd['mean']:.4f} 中位={bd['median']:.4f} P80={bd['p80']:.4f} P90={bd['p90']:.4f}")
        print(f"卖出侧(高点-开盘价): 均值={sd['mean']:.4f} 中位={sd['median']:.4f} P80={sd['p80']:.4f} P90={sd['p90']:.4f}")


# ── 实时预测模式 ──────────────────────────────────────────────

def run_predict(ts_code: str):
    """实时预测: 给出具体买卖建议价

    基于ATR(14)和历史偏离分布，计算不同置信度的建议价格
    """
    df = load_stock_kline(ts_code, "daily", "qfq")
    if df.empty or len(df) < 30:
        log.error(f"  {ts_code} 数据不足")
        return

    close = float(df["close"].iloc[-1])
    open_price = float(df["open"].iloc[-1])
    high = float(df["high"].iloc[-1])
    low = float(df["low"].iloc[-1])
    trade_date = df["trade_date"].iloc[-1]

    # 计算ATR
    atr_values = {}
    for p in [5, 10, 14, 20]:
        atr = calc_atr(df, p)
        if atr.notna().any():
            atr_values[p] = float(atr.iloc[-1])

    atr_14 = atr_values.get(14, None)
    if atr_14 is None:
        log.error(f"  ATR(14) 计算失败")
        return

    # ATR ratio
    atr_ratio = atr_14 / close * 100

    # 历史买卖偏离
    atr_series = calc_atr(df, 14)
    prev_atr = atr_series.shift(1)
    open_s = df["open"].astype(float)
    high_s = df["high"].astype(float)
    low_s = df["low"].astype(float)

    mask = prev_atr.notna() & (prev_atr > 0)
    buy_dev = ((open_s - low_s) / prev_atr)[mask]
    sell_dev = ((high_s - open_s) / prev_atr)[mask]

    print(f"\n{'=' * 60}")
    print(f"  波动率预测 — {ts_code}")
    print(f"{'=' * 60}")
    print(f"  最新日期: {trade_date}")
    print(f"  收盘价:   {close:.2f}")
    print(f"  今日OHLC: O={open_price:.2f} H={high:.2f} L={low:.2f} C={close:.2f}")

    print(f"\n--- ATR 指标 ---")
    for p in [5, 10, 14, 20]:
        if p in atr_values:
            r = atr_values[p] / close * 100
            print(f"  ATR({p:>2}): {atr_values[p]:.2f} ({r:.2f}%)")

    print(f"\n--- 次日振幅预测 ---")
    print(f"  预测日振幅: {atr_14:.2f} (±{atr_ratio:.2f}% of 收盘价)")
    print(f"  预测振幅区间:")
    for factor, label in [(0.8, "保守(80%)"), (1.0, "标准(100%)"), (1.2, "宽松(120%)"), (1.5, "极限(150%)")]:
        range_val = atr_14 * factor
        print(f"    {label}: ±{range_val:.2f} → [{close - range_val:.2f}, {close + range_val:.2f}]")

    # 买入建议价（以ATR为偏移量，基于开盘价估算）
    print(f"\n--- 买入建议价（基于 ATR(14) 偏离分布）---")
    print(f"  假设次日开盘价 ≈ 今日收盘价 {close:.2f}")

    if len(buy_dev) > 30:
        # 不同置信度的买入偏移
        for pct, label in [(0.50, "50%概率能买到"), (0.60, "60%概率能买到"),
                           (0.70, "70%概率能买到"), (0.80, "80%概率能买到")]:
            dev = buy_dev.quantile(pct)
            price = close - dev * atr_14
            print(f"  {label}: {price:.2f} (偏移 {dev:.4f}*ATR = {dev * atr_14:.2f})")

    # 卖出建议价
    print(f"\n--- 卖出建议价（基于 ATR(14) 偏离分布）---")
    if len(sell_dev) > 30:
        for pct, label in [(0.50, "50%概率能卖到"), (0.60, "60%概率能卖到"),
                           (0.70, "70%概率能卖到"), (0.80, "80%概率能卖到")]:
            dev = sell_dev.quantile(pct)
            price = close + dev * atr_14
            print(f"  {label}: {price:.2f} (偏移 {dev:.4f}*ATR = {dev * atr_14:.2f})")

    print(f"\n--- 注意事项 ---")
    print(f"  1. 以上价格基于 ATR(14) 的历史统计，不是精确预测")
    print(f"  2. 开盘价可能与昨收有跳空，实际应以次日开盘价为基准")
    print(f"  3. 牛市波动一般偏大（向上），熊市向下偏移更大")
    print(f"  4. 个股事件（财报、重组等）会导致ATR失效")


# ── 主入口 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="个股波动率分析")
    parser.add_argument("--mode", choices=["batch", "single", "predict"],
                        default="batch", help="分析模式")
    parser.add_argument("--code", type=str, default=None,
                        help="股票代码（single/predict模式必须）")
    parser.add_argument("--sample", type=int, default=500,
                        help="batch模式抽样数量")
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch_analysis(args.sample)
    elif args.mode == "single":
        if not args.code:
            print("请指定 --code 股票代码")
            return
        run_single_analysis(args.code)
    elif args.mode == "predict":
        if not args.code:
            print("请指定 --code 股票代码")
            return
        run_predict(args.code)


if __name__ == "__main__":
    main()
