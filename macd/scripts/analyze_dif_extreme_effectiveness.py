"""DIF极值信号有效性深度分析

核心问题:
1. DIF需要多大才算有意义的"极值"？（幅度阈值 vs 信号质量）
2. 各周期DIF极值信号的有效时间窗口是多长？（衰减曲线）

三层级分析:
  --level index     大盘指数（7大指数逐个 + 汇总）
  --level industry  行业指数（31个申万一级行业，按5大类别聚合）
  --level stock     个股（全市场前复权，汇总统计 + 离散度）

用法:
  python analyze_dif_extreme_effectiveness.py --level index
  python analyze_dif_extreme_effectiveness.py --level industry
  python analyze_dif_extreme_effectiveness.py --level stock
  python analyze_dif_extreme_effectiveness.py --level index --codes 000001.SH:上证指数  # 单指数测试
"""
import argparse
import os
import sys
import time
import math
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from sqlalchemy import text

from database import write_engine, read_engine
from signal_detector import _find_local_peaks, _find_local_troughs, FREQ_ORDER
from bull_bear_phases import tag_trend


# ── 常量定义 ──────────────────────────────────────────────────

FREQS = ["daily", "weekly", "monthly"]
FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

# 前瞻收益窗口（按K线根数）
RETURN_HORIZONS = {
    "daily":   [1, 3, 5, 10, 20, 60],
    "weekly":  [1, 2, 4, 8, 13, 26],
    "monthly": [1, 3, 6, 12],
}

HORIZON_LABELS = {
    "daily":   {1: "T+1", 3: "T+3", 5: "T+5", 10: "T+10", 20: "T+20", 60: "T+60"},
    "weekly":  {1: "T+1w", 2: "T+2w", 4: "T+4w", 8: "T+8w", 13: "T+13w", 26: "T+26w"},
    "monthly": {1: "T+1m", 3: "T+3m", 6: "T+6m", 12: "T+12m"},
}

# 7大指数
INDEX_CODES = [
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000016.SH", "上证50"),
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("000852.SH", "中证1000"),
]

# 申万一级行业5大类别聚合
SW_CATEGORIES = {
    "上游资源": ["801010.SI", "801050.SI", "801950.SI", "801960.SI"],
    "中游制造": ["801040.SI", "801030.SI", "801730.SI", "801890.SI", "801740.SI",
                  "801880.SI", "801710.SI", "801720.SI", "801140.SI"],
    "下游消费": ["801120.SI", "801150.SI", "801110.SI", "801130.SI", "801170.SI",
                  "801200.SI", "801210.SI", "801980.SI"],
    "TMT": ["801750.SI", "801080.SI", "801760.SI", "801770.SI"],
    "金融地产+公用": ["801780.SI", "801790.SI", "801180.SI", "801160.SI",
                       "801970.SI", "801230.SI"],
}

# 时段划分
PERIOD_BINS = [
    ("2016-2018", "20160101", "20181231"),
    ("2019-2021", "20190101", "20211231"),
    ("2022-2023", "20220101", "20231231"),
    ("2024-2026", "20240101", "20261231"),
]

START_DATE = "20160101"


# ── 数据加载 ──────────────────────────────────────────────────

def load_index_macd(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 加载指数 MACD 数据"""
    table = f"index_macd_{freq}"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def load_sw_daily(ts_code: str) -> pd.DataFrame:
    """从 my_stock.sw_daily 加载申万行业日线数据"""
    sql = text(
        "SELECT trade_date, open, high, low, close, vol, pct_change as pct_chg "
        "FROM sw_daily WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def load_stock_macd(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 加载个股前复权 MACD 数据"""
    table = f"stock_macd_{freq}_qfq"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def get_all_stock_codes() -> list[str]:
    """获取全市场在市股票代码"""
    sql = text("SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code")
    with read_engine.connect() as conn:
        result = conn.execute(sql)
        return [r[0] for r in result]


def get_sw_industry_name(ts_code: str) -> str:
    """获取申万行业名称"""
    sql = text(
        "SELECT industry_name FROM index_classify "
        "WHERE index_code = :code AND level = 'L1' AND src = 'SW2021'"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql, {"code": ts_code})
        row = result.fetchone()
        return row[0] if row else ts_code


# ── DIF极值检测 ──────────────────────────────────────────────

def detect_extremes(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """检测DIF极值点，返回包含极值信息的DataFrame

    筛选 start_date 之后的极值点，但极值检测基于全量数据（避免边界效应）
    """
    if df.empty or len(df) < 10:
        return pd.DataFrame()

    order = FREQ_ORDER.get(freq, 20)
    dif_values = df["dif"].values

    # 在全量数据上检测极值
    peak_indices = _find_local_peaks(dif_values, order)
    trough_indices = _find_local_troughs(dif_values, order)

    records = []

    for idx in peak_indices:
        row = df.iloc[idx]
        if row["trade_date"] < START_DATE:
            continue
        records.append({
            "idx": idx,
            "trade_date": row["trade_date"],
            "close": float(row["close"]),
            "dif": float(row["dif"]),
            "abs_dif": abs(float(row["dif"])),
            "type": "peak",  # DIF极大值
            "trend": tag_trend(row["trade_date"]),
        })

    for idx in trough_indices:
        row = df.iloc[idx]
        if row["trade_date"] < START_DATE:
            continue
        records.append({
            "idx": idx,
            "trade_date": row["trade_date"],
            "close": float(row["close"]),
            "dif": float(row["dif"]),
            "abs_dif": abs(float(row["dif"])),
            "type": "trough",  # DIF极小值
            "trend": tag_trend(row["trade_date"]),
        })

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


# ── 前瞻收益计算 ──────────────────────────────────────────────

def compute_returns(df: pd.DataFrame, extremes: pd.DataFrame, freq: str) -> pd.DataFrame:
    """为每个极值点计算各窗口的前瞻收益率

    收益 = (close[idx+h] - close[idx]) / close[idx] * 100
    """
    horizons = RETURN_HORIZONS[freq]
    close = df["close"].values
    n = len(close)

    ret_cols = {}
    for h in horizons:
        col_name = f"ret_{h}"
        ret_vals = []
        for _, ext in extremes.iterrows():
            idx = ext["idx"]
            target = idx + h
            if target < n:
                base = close[idx]
                if base != 0:
                    ret_vals.append(round((close[target] - base) / base * 100, 3))
                else:
                    ret_vals.append(None)
            else:
                ret_vals.append(None)
        ret_cols[col_name] = ret_vals

    for col, vals in ret_cols.items():
        extremes = extremes.copy()
        extremes[col] = vals

    return extremes


# ── 幅度分位数分档 ──────────────────────────────────────────────

def assign_magnitude_band(extremes: pd.DataFrame, freq: str) -> pd.DataFrame:
    """按DIF绝对值分位数分档

    峰/谷分别计算，避免正负混合
    4档: <Q25 / Q25-Q50 / Q50-Q75 / >Q75
    月线样本<20时降为2档（中位数上下）
    """
    extremes = extremes.copy()
    extremes["mag_band"] = ""

    for ext_type in ["peak", "trough"]:
        mask = extremes["type"] == ext_type
        subset = extremes.loc[mask, "abs_dif"]
        if len(subset) == 0:
            continue

        if freq == "monthly" and len(subset) < 20:
            # 样本太少，用2档
            q50 = subset.quantile(0.5)
            bands = subset.apply(lambda x: "下半(<Q50)" if x < q50 else "上半(>=Q50)")
        else:
            q25 = subset.quantile(0.25)
            q50 = subset.quantile(0.50)
            q75 = subset.quantile(0.75)
            def _band(x):
                if x < q25:
                    return "<Q25"
                elif x < q50:
                    return "Q25-Q50"
                elif x < q75:
                    return "Q50-Q75"
                else:
                    return ">Q75"
            bands = subset.apply(_band)

        extremes.loc[mask, "mag_band"] = bands

    return extremes


# ── 时间衰减权重 + 时段标签 ──────────────────────────────────────

def assign_weights_and_period(extremes: pd.DataFrame) -> pd.DataFrame:
    """分配衰减权重和时段标签

    weight = exp(-0.2 * years_ago)，半衰期约3.5年
    """
    extremes = extremes.copy()
    now_year = 2026.0

    # 衰减权重
    years_ago = extremes["trade_date"].apply(
        lambda d: now_year - (int(d[:4]) + int(d[4:6]) / 12)
    )
    extremes["weight"] = np.exp(-0.2 * years_ago)

    # 时段标签
    def _period(d):
        for label, start, end in PERIOD_BINS:
            if start <= d <= end:
                return label
        return "other"
    extremes["period"] = extremes["trade_date"].apply(_period)

    return extremes


# ── 统计输出 ──────────────────────────────────────────────────

def _calc_group_stats(group: pd.DataFrame, horizons: list[int], freq: str) -> dict:
    """计算一组极值的统计指标"""
    h_labels = HORIZON_LABELS[freq]
    n = len(group)
    if n == 0:
        return None

    stats = {"N": n}

    for h in horizons:
        col = f"ret_{h}"
        label = h_labels.get(h, f"T+{h}")
        valid = group[col].dropna()
        if len(valid) == 0:
            stats[f"{label}_胜率"] = "-"
            stats[f"{label}_均值"] = "-"
            stats[f"{label}_w胜率"] = "-"
            stats[f"{label}_w均值"] = "-"
            continue

        # 峰→跌=赢，谷→涨=赢
        is_peak = (group["type"] == "peak").iloc[0] if len(group) > 0 else True
        if is_peak:
            wins = valid < 0
        else:
            wins = valid > 0

        # 等权
        win_rate = wins.sum() / len(valid) * 100
        avg_ret = valid.mean()
        stats[f"{label}_胜率"] = f"{win_rate:.1f}%"
        stats[f"{label}_均值"] = f"{avg_ret:+.2f}%"

        # 加权
        weights = group.loc[valid.index, "weight"]
        w_total = weights.sum()
        if w_total > 0:
            w_win_rate = (wins * weights).sum() / w_total * 100
            w_avg_ret = (valid * weights).sum() / w_total
            stats[f"{label}_w胜率"] = f"{w_win_rate:.1f}%"
            stats[f"{label}_w均值"] = f"{w_avg_ret:+.2f}%"
        else:
            stats[f"{label}_w胜率"] = "-"
            stats[f"{label}_w均值"] = "-"

    return stats


def print_section(title: str):
    """打印分隔标题"""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")


def print_table(headers: list[str], rows: list[list[str]], min_widths: list[int] = None):
    """打印对齐表格"""
    if not rows:
        print("  (无数据)")
        return

    # 计算列宽
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(str(cell)))
    if min_widths:
        for i, mw in enumerate(min_widths):
            if i < len(widths):
                widths[i] = max(widths[i], mw)

    # 打印表头
    header_line = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    print(f"  {header_line}")
    sep_line = "-+-".join("-" * widths[i] for i in range(len(headers)))
    print(f"  {sep_line}")

    # 打印数据行
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            if i < len(widths):
                cells.append(str(cell).ljust(widths[i]))
            else:
                cells.append(str(cell))
        print(f"  {' | '.join(cells)}")


def print_stats_by_group(extremes: pd.DataFrame, group_col: str, freq: str,
                          ext_type: str, title: str):
    """按指定维度分组输出统计表"""
    horizons = RETURN_HORIZONS[freq]
    h_labels = HORIZON_LABELS[freq]
    subset = extremes[extremes["type"] == ext_type]
    if subset.empty:
        return

    type_cn = "DIF极大值(峰)" if ext_type == "peak" else "DIF极小值(谷)"
    print(f"\n  [{type_cn}] {title}")

    # 构建表头：分组 | N | 各窗口(等权胜率 等权均值 加权胜率 加权均值)
    headers = [group_col, "N"]
    for h in horizons:
        label = h_labels.get(h, f"T+{h}")
        headers.extend([f"{label}_胜率", f"{label}_均值", f"{label}_w胜率", f"{label}_w均值"])

    rows = []
    for gname, gdata in subset.groupby(group_col, sort=True):
        stats = _calc_group_stats(gdata, horizons, freq)
        if stats is None:
            continue
        row = [str(gname), str(stats["N"])]
        for h in horizons:
            label = h_labels.get(h, f"T+{h}")
            row.extend([
                stats.get(f"{label}_胜率", "-"),
                stats.get(f"{label}_均值", "-"),
                stats.get(f"{label}_w胜率", "-"),
                stats.get(f"{label}_w均值", "-"),
            ])
        rows.append(row)

    # 全量汇总
    stats = _calc_group_stats(subset, horizons, freq)
    if stats:
        row = ["全量", str(stats["N"])]
        for h in horizons:
            label = h_labels.get(h, f"T+{h}")
            row.extend([
                stats.get(f"{label}_胜率", "-"),
                stats.get(f"{label}_均值", "-"),
                stats.get(f"{label}_w胜率", "-"),
                stats.get(f"{label}_w均值", "-"),
            ])
        rows.append(row)

    print_table(headers, rows)


def print_magnitude_quantiles(extremes: pd.DataFrame, freq: str):
    """输出幅度分位数信息"""
    for ext_type in ["peak", "trough"]:
        subset = extremes[extremes["type"] == ext_type]
        if subset.empty:
            continue
        type_cn = "峰" if ext_type == "peak" else "谷"
        abs_vals = subset["abs_dif"]
        qs = abs_vals.quantile([0.25, 0.5, 0.75])
        print(f"  {type_cn} |abs(DIF)| 分位: Q25={qs[0.25]:.2f}, Q50={qs[0.5]:.2f}, "
              f"Q75={qs[0.75]:.2f}, min={abs_vals.min():.2f}, max={abs_vals.max():.2f}, N={len(subset)}")


# ── 单标的完整分析流程 ──────────────────────────────────────────

def analyze_one(ts_code: str, name: str, freq: str, df: pd.DataFrame,
                verbose: bool = True) -> pd.DataFrame | None:
    """分析单个标的单个周期的DIF极值有效性

    返回带完整标注的extremes DataFrame
    """
    if df.empty:
        if verbose:
            print(f"  {name} {FREQ_NAMES[freq]}: 无数据，跳过")
        return None

    # 1. 检测极值
    extremes = detect_extremes(df, freq)
    if extremes.empty:
        if verbose:
            print(f"  {name} {FREQ_NAMES[freq]}: 无极值点")
        return None

    # 2. 计算前瞻收益
    extremes = compute_returns(df, extremes, freq)

    # 3. 幅度分档
    extremes = assign_magnitude_band(extremes, freq)

    # 4. 衰减权重 + 时段标签
    extremes = assign_weights_and_period(extremes)

    # 添加标识
    extremes["ts_code"] = ts_code
    extremes["name"] = name

    if verbose:
        n_peak = (extremes["type"] == "peak").sum()
        n_trough = (extremes["type"] == "trough").sum()
        print(f"  {name} {FREQ_NAMES[freq]}: 峰={n_peak}, 谷={n_trough}")

    return extremes


# ── Level 1: 大盘指数 ──────────────────────────────────────────

def run_index_level(codes: list[tuple[str, str]] = None):
    """大盘指数层级分析"""
    codes = codes or INDEX_CODES
    print_section("Level 1: 大盘指数 DIF极值有效性分析")
    print(f"  指数: {', '.join(f'{n}({c})' for c, n in codes)}")
    print(f"  数据范围: {START_DATE}~至今")

    for freq in FREQS:
        print_section(f"大盘指数 - {FREQ_NAMES[freq]}")

        all_extremes = []

        for ts_code, name in codes:
            df = load_index_macd(ts_code, freq)
            extremes = analyze_one(ts_code, name, freq, df)
            if extremes is not None:
                all_extremes.append(extremes)

                # 每个指数输出详细统计
                print(f"\n  --- {name} ({ts_code}) {FREQ_NAMES[freq]} ---")
                print_magnitude_quantiles(extremes, freq)

                for ext_type in ["peak", "trough"]:
                    print_stats_by_group(extremes, "mag_band", freq, ext_type,
                                          f"按幅度分层 ({name})")
                    print_stats_by_group(extremes, "period", freq, ext_type,
                                          f"按时段 ({name})")
                    print_stats_by_group(extremes, "trend", freq, ext_type,
                                          f"按牛熊 ({name})")

        # 汇总所有指数
        if all_extremes:
            combined = pd.concat(all_extremes, ignore_index=True)
            # 汇总时重新计算全局幅度分档
            combined = assign_magnitude_band(combined, freq)

            print_section(f"大盘指数汇总 - {FREQ_NAMES[freq]} (N={len(combined)})")
            print_magnitude_quantiles(combined, freq)

            for ext_type in ["peak", "trough"]:
                print_stats_by_group(combined, "mag_band", freq, ext_type,
                                      "按幅度分层 (全部指数)")
                print_stats_by_group(combined, "period", freq, ext_type,
                                      "按时段 (全部指数)")
                print_stats_by_group(combined, "trend", freq, ext_type,
                                      "按牛熊 (全部指数)")


# ── Level 2: 行业指数 ──────────────────────────────────────────

def calc_macd_from_kline(df: pd.DataFrame) -> pd.DataFrame:
    """从K线数据计算MACD指标"""
    from macd_calc import calc_macd
    if df.empty or len(df) < 30:
        return pd.DataFrame()
    macd_df = calc_macd(df["close"])
    df = df.copy()
    df["dif"] = macd_df["dif"].values
    df["dea"] = macd_df["dea"].values
    df["macd"] = macd_df["macd"].values
    return df


def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """日线重采样为周线"""
    if df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.set_index("date")

    weekly = df.resample("W-FRI").agg({
        "trade_date": "last",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "vol": "sum",
        "pct_chg": lambda x: ((1 + x / 100).prod() - 1) * 100 if len(x) > 0 else 0,
    }).dropna(subset=["trade_date"])

    weekly = weekly.reset_index(drop=True)
    return weekly


def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """日线重采样为月线"""
    if df.empty:
        return df
    df = df.copy()
    df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.set_index("date")

    monthly = df.resample("M").agg({
        "trade_date": "last",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "vol": "sum",
        "pct_chg": lambda x: ((1 + x / 100).prod() - 1) * 100 if len(x) > 0 else 0,
    }).dropna(subset=["trade_date"])

    monthly = monthly.reset_index(drop=True)
    return monthly


def run_industry_level():
    """行业指数层级分析（按5大类别聚合）"""
    print_section("Level 2: 行业指数 DIF极值有效性分析")
    print(f"  分类: {', '.join(SW_CATEGORIES.keys())}")

    for freq in FREQS:
        print_section(f"行业指数 - {FREQ_NAMES[freq]}")

        category_extremes = {}  # 类别 -> extremes列表

        for cat_name, cat_codes in SW_CATEGORIES.items():
            cat_data = []
            for sw_code in cat_codes:
                ind_name = get_sw_industry_name(sw_code)

                # 加载日线数据
                daily_df = load_sw_daily(sw_code)
                if daily_df.empty:
                    continue

                # 根据freq重采样
                if freq == "daily":
                    kline_df = daily_df
                elif freq == "weekly":
                    kline_df = resample_to_weekly(daily_df)
                else:
                    kline_df = resample_to_monthly(daily_df)

                # 计算MACD
                macd_df = calc_macd_from_kline(kline_df)
                if macd_df.empty:
                    continue

                # 分析极值
                extremes = analyze_one(sw_code, ind_name, freq, macd_df, verbose=False)
                if extremes is not None:
                    cat_data.append(extremes)

            if cat_data:
                combined = pd.concat(cat_data, ignore_index=True)
                combined = assign_magnitude_band(combined, freq)
                category_extremes[cat_name] = combined
                n_peak = (combined["type"] == "peak").sum()
                n_trough = (combined["type"] == "trough").sum()
                print(f"  {cat_name}: {len(cat_codes)}行业, 峰={n_peak}, 谷={n_trough}")

        # 输出各类别统计
        for cat_name, combined in category_extremes.items():
            print(f"\n  === {cat_name} ===")
            print_magnitude_quantiles(combined, freq)

            for ext_type in ["peak", "trough"]:
                print_stats_by_group(combined, "mag_band", freq, ext_type,
                                      f"按幅度分层 ({cat_name})")
                print_stats_by_group(combined, "trend", freq, ext_type,
                                      f"按牛熊 ({cat_name})")

        # 全行业汇总
        if category_extremes:
            all_industry = pd.concat(category_extremes.values(), ignore_index=True)
            all_industry = assign_magnitude_band(all_industry, freq)

            print_section(f"行业指数全量汇总 - {FREQ_NAMES[freq]} (N={len(all_industry)})")
            print_magnitude_quantiles(all_industry, freq)

            for ext_type in ["peak", "trough"]:
                print_stats_by_group(all_industry, "mag_band", freq, ext_type,
                                      "按幅度分层 (全行业)")
                print_stats_by_group(all_industry, "period", freq, ext_type,
                                      "按时段 (全行业)")
                print_stats_by_group(all_industry, "trend", freq, ext_type,
                                      "按牛熊 (全行业)")

            # 类别间对比表
            print(f"\n  [类别间对比 - {FREQ_NAMES[freq]}]")
            horizons = RETURN_HORIZONS[freq]
            h_labels = HORIZON_LABELS[freq]
            # 取最长窗口作为对比
            main_h = horizons[-1]
            main_label = h_labels[main_h]
            for ext_type in ["peak", "trough"]:
                type_cn = "峰" if ext_type == "peak" else "谷"
                print(f"\n  {type_cn} - {main_label} 各类别对比:")
                headers = ["类别", "N", f"{main_label}_胜率", f"{main_label}_均值",
                           f"{main_label}_w胜率", f"{main_label}_w均值"]
                cat_rows = []
                for cat_name, combined in category_extremes.items():
                    subset = combined[combined["type"] == ext_type]
                    stats = _calc_group_stats(subset, [main_h], freq)
                    if stats:
                        cat_rows.append([
                            cat_name, str(stats["N"]),
                            stats.get(f"{main_label}_胜率", "-"),
                            stats.get(f"{main_label}_均值", "-"),
                            stats.get(f"{main_label}_w胜率", "-"),
                            stats.get(f"{main_label}_w均值", "-"),
                        ])
                print_table(headers, cat_rows)


# ── Level 3: 个股 ──────────────────────────────────────────────

def run_stock_level():
    """个股层级分析（全市场汇总统计 + 离散度）"""
    print_section("Level 3: 个股 DIF极值有效性分析（全市场前复权）")

    all_stock_codes = get_all_stock_codes()
    print(f"  全市场股票数: {len(all_stock_codes)}")

    for freq in FREQS:
        print_section(f"个股 - {FREQ_NAMES[freq]}")

        # 收集所有个股各窗口统计（胜率和均值收益）
        horizons = RETURN_HORIZONS[freq]
        h_labels = HORIZON_LABELS[freq]

        # 每只股票的汇总统计（胜率/均值），用于计算离散度
        stock_stats_peak = []   # 每股的统计值列表
        stock_stats_trough = []
        total_extremes_peak = 0
        total_extremes_trough = 0
        processed = 0
        skipped = 0

        # 汇总用：收集所有极值点的收益
        all_rets_peak = {h: [] for h in horizons}
        all_rets_trough = {h: [] for h in horizons}
        # 带权重
        all_weighted_peak = {h: {"rets": [], "weights": [], "wins": []} for h in horizons}
        all_weighted_trough = {h: {"rets": [], "weights": [], "wins": []} for h in horizons}
        # 牛熊分组
        bull_rets_peak = {h: [] for h in horizons}
        bear_rets_peak = {h: [] for h in horizons}
        bull_rets_trough = {h: [] for h in horizons}
        bear_rets_trough = {h: [] for h in horizons}
        # 幅度分组（4档各自收集）
        mag_rets_peak = {band: {h: [] for h in horizons} for band in ["<Q25", "Q25-Q50", "Q50-Q75", ">Q75"]}
        mag_rets_trough = {band: {h: [] for h in horizons} for band in ["<Q25", "Q25-Q50", "Q50-Q75", ">Q75"]}

        start_time = time.time()
        batch_size = 100

        for i, ts_code in enumerate(all_stock_codes):
            df = load_stock_macd(ts_code, freq)
            if df.empty or len(df) < 50:
                skipped += 1
                continue

            extremes = detect_extremes(df, freq)
            if extremes.empty:
                skipped += 1
                continue

            extremes = compute_returns(df, extremes, freq)
            extremes = assign_magnitude_band(extremes, freq)
            extremes = assign_weights_and_period(extremes)

            processed += 1

            # 分峰/谷收集
            for ext_type, all_rets, all_weighted, bull_rets, bear_rets, mag_rets, stock_stats, total_ref in [
                ("peak", all_rets_peak, all_weighted_peak, bull_rets_peak, bear_rets_peak, mag_rets_peak, stock_stats_peak, "peak"),
                ("trough", all_rets_trough, all_weighted_trough, bull_rets_trough, bear_rets_trough, mag_rets_trough, stock_stats_trough, "trough"),
            ]:
                subset = extremes[extremes["type"] == ext_type]
                if subset.empty:
                    continue

                if ext_type == "peak":
                    total_extremes_peak += len(subset)
                else:
                    total_extremes_trough += len(subset)

                # 每股统计
                stock_h_stats = {}
                for h in horizons:
                    col = f"ret_{h}"
                    valid = subset[col].dropna()
                    if len(valid) >= 3:  # 至少3个极值才有统计意义
                        is_peak = ext_type == "peak"
                        wins = (valid < 0) if is_peak else (valid > 0)
                        stock_h_stats[h] = {
                            "win_rate": wins.sum() / len(valid) * 100,
                            "avg_ret": valid.mean(),
                        }

                        # 汇总收集
                        all_rets[h].extend(valid.tolist())
                        w = subset.loc[valid.index, "weight"]
                        all_weighted[h]["rets"].extend(valid.tolist())
                        all_weighted[h]["weights"].extend(w.tolist())
                        all_weighted[h]["wins"].extend(wins.tolist())

                        # 牛熊
                        for idx_val in valid.index:
                            trend = subset.loc[idx_val, "trend"]
                            ret_val = valid.loc[idx_val]
                            if trend == "bull":
                                bull_rets[h].append(ret_val)
                            elif trend == "bear":
                                bear_rets[h].append(ret_val)

                        # 幅度分组
                        for idx_val in valid.index:
                            band = subset.loc[idx_val, "mag_band"]
                            ret_val = valid.loc[idx_val]
                            if band in mag_rets:
                                mag_rets[band][h].append(ret_val)

                if stock_h_stats:
                    stock_stats.append(stock_h_stats)

            # 进度报告
            if (i + 1) % batch_size == 0:
                elapsed = time.time() - start_time
                speed = (i + 1) / elapsed
                eta = (len(all_stock_codes) - i - 1) / speed
                print(f"  进度: {i+1}/{len(all_stock_codes)} "
                      f"({processed}有效, {skipped}跳过) "
                      f"峰={total_extremes_peak}, 谷={total_extremes_trough} "
                      f"速度={speed:.0f}只/秒 ETA={eta:.0f}秒")

        elapsed = time.time() - start_time
        print(f"\n  完成: {processed}只有效 / {skipped}只跳过, 耗时{elapsed:.1f}秒")
        print(f"  极值总数: 峰={total_extremes_peak}, 谷={total_extremes_trough}")

        # 输出汇总统计
        for ext_type, all_rets, all_weighted, bull_rets, bear_rets, mag_rets, stock_stats in [
            ("peak", all_rets_peak, all_weighted_peak, bull_rets_peak, bear_rets_peak, mag_rets_peak, stock_stats_peak),
            ("trough", all_rets_trough, all_weighted_trough, bull_rets_trough, bear_rets_trough, mag_rets_trough, stock_stats_trough),
        ]:
            type_cn = "DIF极大值(峰)" if ext_type == "peak" else "DIF极小值(谷)"
            is_peak = ext_type == "peak"

            print(f"\n  ======== {type_cn} 全市场汇总 ========")

            # 1. 各窗口全量统计
            headers = ["窗口", "N", "等权胜率", "等权均值", "加权胜率", "加权均值"]
            rows = []
            for h in horizons:
                label = h_labels.get(h, f"T+{h}")
                rets = all_rets[h]
                if not rets:
                    continue
                rets_arr = np.array(rets)
                wins = (rets_arr < 0) if is_peak else (rets_arr > 0)
                win_rate = wins.sum() / len(rets_arr) * 100
                avg_ret = rets_arr.mean()

                # 加权
                w = np.array(all_weighted[h]["weights"])
                w_rets = np.array(all_weighted[h]["rets"])
                w_wins = np.array(all_weighted[h]["wins"])
                w_total = w.sum()
                w_win_rate = (w_wins * w).sum() / w_total * 100 if w_total > 0 else 0
                w_avg_ret = (w_rets * w).sum() / w_total if w_total > 0 else 0

                rows.append([label, str(len(rets)), f"{win_rate:.1f}%", f"{avg_ret:+.2f}%",
                             f"{w_win_rate:.1f}%", f"{w_avg_ret:+.2f}%"])
            print_table(headers, rows)

            # 2. 牛熊分组
            print(f"\n  [{type_cn}] 按牛熊分组:")
            headers = ["趋势", "窗口", "N", "胜率", "均值"]
            rows = []
            for trend_label, trend_rets in [("牛市", bull_rets), ("熊市", bear_rets)]:
                for h in horizons:
                    label = h_labels.get(h, f"T+{h}")
                    rets = trend_rets[h]
                    if not rets:
                        continue
                    rets_arr = np.array(rets)
                    wins = (rets_arr < 0) if is_peak else (rets_arr > 0)
                    win_rate = wins.sum() / len(rets_arr) * 100
                    avg_ret = rets_arr.mean()
                    rows.append([trend_label, label, str(len(rets)),
                                 f"{win_rate:.1f}%", f"{avg_ret:+.2f}%"])
            print_table(headers, rows)

            # 3. 幅度分组
            print(f"\n  [{type_cn}] 按幅度分层:")
            headers = ["幅度档", "窗口", "N", "胜率", "均值"]
            rows = []
            for band in ["<Q25", "Q25-Q50", "Q50-Q75", ">Q75"]:
                for h in horizons:
                    label = h_labels.get(h, f"T+{h}")
                    rets = mag_rets[band][h]
                    if not rets:
                        continue
                    rets_arr = np.array(rets)
                    wins = (rets_arr < 0) if is_peak else (rets_arr > 0)
                    win_rate = wins.sum() / len(rets_arr) * 100
                    avg_ret = rets_arr.mean()
                    rows.append([band, label, str(len(rets)),
                                 f"{win_rate:.1f}%", f"{avg_ret:+.2f}%"])
            print_table(headers, rows)

            # 4. 个股离散度
            print(f"\n  [{type_cn}] 个股离散度（每股胜率分布）:")
            for h in horizons:
                label = h_labels.get(h, f"T+{h}")
                win_rates = [s[h]["win_rate"] for s in stock_stats if h in s]
                if len(win_rates) < 10:
                    continue
                wr_arr = np.array(win_rates)
                print(f"    {label}: N={len(wr_arr)}只, "
                      f"均值={wr_arr.mean():.1f}%, 中位={np.median(wr_arr):.1f}%, "
                      f"标准差={wr_arr.std():.1f}%, "
                      f"[Q25={np.percentile(wr_arr, 25):.1f}%, Q75={np.percentile(wr_arr, 75):.1f}%]")


# ── 主入口 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DIF极值信号有效性深度分析")
    parser.add_argument("--level", required=True, choices=["index", "industry", "stock"],
                        help="分析层级: index=大盘指数, industry=行业指数, stock=个股")
    parser.add_argument("--codes", default=None,
                        help="指定代码，格式: 000001.SH:上证指数,399001.SZ:深证成指 (仅level=index有效)")
    args = parser.parse_args()

    start_time = time.time()
    print(f"{'#'*80}")
    print(f"  DIF极值信号有效性深度分析")
    print(f"  层级: {args.level}")
    print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  数据范围: {START_DATE}~至今 (衰减半衰期≈3.5年)")
    print(f"{'#'*80}")

    if args.level == "index":
        codes = None
        if args.codes:
            codes = []
            for item in args.codes.split(","):
                parts = item.strip().split(":")
                if len(parts) == 2:
                    codes.append((parts[0].strip(), parts[1].strip()))
                else:
                    codes.append((parts[0].strip(), parts[0].strip()))
        run_index_level(codes)
    elif args.level == "industry":
        run_industry_level()
    elif args.level == "stock":
        run_stock_level()

    elapsed = time.time() - start_time
    print(f"\n{'#'*80}")
    print(f"  完成 | 总耗时: {elapsed:.1f}秒")
    print(f"{'#'*80}")


if __name__ == "__main__":
    main()
