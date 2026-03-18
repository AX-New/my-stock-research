"""DIF极值接近度分析 — 不偷看未来的实时特征

核心思路:
  每根K线收盘时，只用过去数据计算:
    - DIF 相对于过去N日最大值的接近度（高位接近度）
    - DIF 相对于过去N日最小值的接近度（低位接近度）
  然后统计各接近度区间的未来收益和胜率。

用法:
  python research/macd/scripts/analyze_dif_proximity.py
  python research/macd/scripts/analyze_dif_proximity.py --ts_code 000001.SH --freq daily --window 250
  python research/macd/scripts/analyze_dif_proximity.py --freq weekly --window 52
"""
import argparse
import sys
import pandas as pd
import numpy as np
from sqlalchemy import text

from database import write_engine


# ── 各周期默认滚动窗口和评估窗口 ──
FREQ_CONFIG = {
    "daily": {
        "window": 250,       # 约1年
        "horizons": [5, 10, 20, 60],  # T+5/10/20/60
        "main_horizon": 20,  # 主评估窗口
        "label": "日线",
    },
    "weekly": {
        "window": 52,        # 约1年
        "horizons": [2, 4, 8, 13],
        "main_horizon": 4,
        "label": "周线",
    },
    "monthly": {
        "window": 12,        # 约1年
        "horizons": [1, 3, 6, 12],
        "main_horizon": 3,
        "label": "月线",
    },
}

# 接近度分段（高位: DIF接近历史最大值; 低位: DIF接近历史最小值）
HIGH_BANDS = [
    (">95%", 0.95, 1.01),
    ("80~95%", 0.80, 0.95),
    ("60~80%", 0.60, 0.80),
    ("40~60%", 0.40, 0.60),
    ("<40%", -0.01, 0.40),
]

LOW_BANDS = [
    (">95%", 0.95, 1.01),
    ("80~95%", 0.80, 0.95),
    ("60~80%", 0.60, 0.80),
    ("40~60%", 0.40, 0.60),
    ("<40%", -0.01, 0.40),
]


def load_dif_data(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 加载 DIF 数据"""
    # 指数表名
    if ts_code.endswith(".SH") or ts_code.endswith(".SZ"):
        if "." in ts_code and len(ts_code) == 9:
            # 判断是指数还是股票
            code_num = ts_code.split(".")[0]
            if code_num.startswith("0000") or code_num.startswith("399") or code_num.startswith("930"):
                table = f"index_macd_{freq}"
            else:
                table = f"stock_macd_{freq}_bfq"
        else:
            table = f"index_macd_{freq}"
    else:
        table = f"stock_macd_{freq}_bfq"

    sql = text(f"""
        SELECT trade_date, close, dif, dea, macd
        FROM {table}
        WHERE ts_code = :code
        ORDER BY trade_date
    """)
    df = pd.read_sql(sql, write_engine, params={"code": ts_code})
    print(f"[INFO] 加载 {ts_code} {freq} 数据: {len(df)} 行, "
          f"{df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")
    return df


def compute_proximity_features(df: pd.DataFrame, window: int) -> pd.DataFrame:
    """计算DIF极值接近度特征（只看过去，不偷看未来）

    对于每根K线，计算:
    - rolling_max: 过去 window 根K线的 DIF 最大值
    - rolling_min: 过去 window 根K线的 DIF 最小值
    - high_proximity: 当前 DIF / rolling_max（高位接近度，越接近1说明DIF越接近历史高位）
    - low_proximity: 当前 DIF / rolling_min（低位接近度，越接近1说明DIF越接近历史低位）
    - percentile: 当前 DIF 在过去 window 期的分位数（0~1）
    """
    dif = df["dif"].values
    n = len(dif)

    rolling_max = np.full(n, np.nan)
    rolling_min = np.full(n, np.nan)
    percentile = np.full(n, np.nan)

    for i in range(window, n):
        past = dif[i - window: i]  # 不含当前（严格只看过去）
        valid = past[~np.isnan(past)]
        if len(valid) < window * 0.5:
            continue
        rolling_max[i] = np.max(valid)
        rolling_min[i] = np.min(valid)
        # 当前DIF在历史分布中的分位数
        percentile[i] = np.sum(valid <= dif[i]) / len(valid)

    df = df.copy()
    df["rolling_max"] = rolling_max
    df["rolling_min"] = rolling_min
    df["percentile"] = percentile

    # 高位接近度: 当DIF>0时，DIF/rolling_max 越接近1越危险
    # 低位接近度: 当DIF<0时，DIF/rolling_min 越接近1越危险
    # 用分位数作为统一指标更直观
    # 但也算 proximity 供参考
    df["high_proximity"] = np.where(
        rolling_max > 0,
        np.clip(dif / rolling_max, 0, 1.5),
        np.nan,
    )
    df["low_proximity"] = np.where(
        rolling_min < 0,
        np.clip(dif / rolling_min, 0, 1.5),
        np.nan,
    )

    return df


def compute_forward_returns(df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    """计算未来收益率（从下一根K线开始算，不含当天）"""
    df = df.copy()
    close = df["close"].values
    for h in horizons:
        ret = np.full(len(close), np.nan)
        for i in range(len(close) - h):
            if close[i] > 0:
                ret[i] = (close[i + h] - close[i]) / close[i] * 100
        df[f"ret_{h}"] = ret
    return df


def analyze_by_percentile(df: pd.DataFrame, horizons: list[int], main_horizon: int) -> pd.DataFrame:
    """按DIF分位数分段统计收益和胜率"""
    # 分位数分段
    bands = [
        (">P90 (极高)", 0.90, 1.01),
        ("P75~P90", 0.75, 0.90),
        ("P50~P75", 0.50, 0.75),
        ("P25~P50", 0.25, 0.50),
        ("P10~P25", 0.10, 0.25),
        ("<P10 (极低)", -0.01, 0.10),
    ]

    results = []
    ret_col = f"ret_{main_horizon}"

    for label, lo, hi in bands:
        mask = (df["percentile"] >= lo) & (df["percentile"] < hi) & df[ret_col].notna()
        subset = df[mask]
        if len(subset) == 0:
            continue

        row = {
            "分位区间": label,
            "样本数": len(subset),
        }

        for h in horizons:
            col = f"ret_{h}"
            valid = subset[col].dropna()
            if len(valid) > 0:
                row[f"T+{h}均值"] = round(valid.mean(), 2)
                # 对于高位（>P50），"胜"=下跌（做空逻辑）；低位（<P50），"胜"=上涨（做多逻辑）
                # 但实际操作中，我们统一用"做多胜率"和"做空胜率"两种视角
                row[f"T+{h}上涨率"] = round((valid > 0).mean() * 100, 1)
            else:
                row[f"T+{h}均值"] = None
                row[f"T+{h}上涨率"] = None

        results.append(row)

    return pd.DataFrame(results)


def analyze_high_proximity(df: pd.DataFrame, horizons: list[int], main_horizon: int) -> pd.DataFrame:
    """高位接近度分析 — DIF接近历史最大值时的后续表现"""
    ret_col = f"ret_{main_horizon}"
    results = []

    for label, lo, hi in HIGH_BANDS:
        mask = (df["high_proximity"] >= lo) & (df["high_proximity"] < hi) & df[ret_col].notna()
        subset = df[mask]
        if len(subset) == 0:
            continue

        row = {"高位接近度": label, "样本数": len(subset)}
        for h in horizons:
            col = f"ret_{h}"
            valid = subset[col].dropna()
            if len(valid) > 0:
                row[f"T+{h}均值"] = round(valid.mean(), 2)
                row[f"T+{h}下跌率"] = round((valid < 0).mean() * 100, 1)
        results.append(row)

    return pd.DataFrame(results)


def analyze_low_proximity(df: pd.DataFrame, horizons: list[int], main_horizon: int) -> pd.DataFrame:
    """低位接近度分析 — DIF接近历史最小值时的后续表现"""
    ret_col = f"ret_{main_horizon}"
    results = []

    for label, lo, hi in LOW_BANDS:
        mask = (df["low_proximity"] >= lo) & (df["low_proximity"] < hi) & df[ret_col].notna()
        subset = df[mask]
        if len(subset) == 0:
            continue

        row = {"低位接近度": label, "样本数": len(subset)}
        for h in horizons:
            col = f"ret_{h}"
            valid = subset[col].dropna()
            if len(valid) > 0:
                row[f"T+{h}均值"] = round(valid.mean(), 2)
                row[f"T+{h}上涨率"] = round((valid > 0).mean() * 100, 1)
        results.append(row)

    return pd.DataFrame(results)


def analyze_momentum_decay(df: pd.DataFrame, horizons: list[int], main_horizon: int) -> pd.DataFrame:
    """动量衰减信号 — DIF从高位连续下降时的后续表现

    信号条件（全部实时可判断）:
    - DIF 处于高分位（>P75）
    - DIF 连续下降 N 天（N=1,2,3,4,5）
    对称地，DIF从低位连续上升也分析。
    """
    df = df.copy()
    dif = df["dif"].values

    # 计算DIF连续下降/上升天数
    consec_down = np.zeros(len(dif), dtype=int)
    consec_up = np.zeros(len(dif), dtype=int)
    for i in range(1, len(dif)):
        if not np.isnan(dif[i]) and not np.isnan(dif[i - 1]):
            if dif[i] < dif[i - 1]:
                consec_down[i] = consec_down[i - 1] + 1
                consec_up[i] = 0
            elif dif[i] > dif[i - 1]:
                consec_up[i] = consec_up[i - 1] + 1
                consec_down[i] = 0
            else:
                consec_down[i] = 0
                consec_up[i] = 0

    df["consec_down"] = consec_down
    df["consec_up"] = consec_up

    ret_col = f"ret_{main_horizon}"
    results = []

    # 高位连续下降（卖出信号）
    for n_days in [1, 2, 3, 4, 5]:
        mask = (
            (df["percentile"] > 0.75) &
            (df["consec_down"] >= n_days) &
            df[ret_col].notna()
        )
        subset = df[mask]
        if len(subset) == 0:
            continue
        row = {"信号": f"高位(>P75)+连跌{n_days}天", "样本数": len(subset)}
        for h in horizons:
            col = f"ret_{h}"
            valid = subset[col].dropna()
            if len(valid) > 0:
                row[f"T+{h}均值"] = round(valid.mean(), 2)
                row[f"T+{h}下跌率"] = round((valid < 0).mean() * 100, 1)
        results.append(row)

    # 低位连续上升（买入信号）
    for n_days in [1, 2, 3, 4, 5]:
        mask = (
            (df["percentile"] < 0.25) &
            (df["consec_up"] >= n_days) &
            df[ret_col].notna()
        )
        subset = df[mask]
        if len(subset) == 0:
            continue
        row = {"信号": f"低位(<P25)+连涨{n_days}天", "样本数": len(subset)}
        for h in horizons:
            col = f"ret_{h}"
            valid = subset[col].dropna()
            if len(valid) > 0:
                row[f"T+{h}均值"] = round(valid.mean(), 2)
                row[f"T+{h}上涨率"] = round((valid > 0).mean() * 100, 1)
        results.append(row)

    return pd.DataFrame(results)


def print_table(title: str, df: pd.DataFrame):
    """格式化打印表格"""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")
    if df.empty:
        print("  (无数据)")
        return
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 120)
    pd.set_option("display.colheader_justify", "right")
    print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="DIF极值接近度分析（不偷看未来）")
    parser.add_argument("--ts_code", default="000001.SH", help="标的代码")
    parser.add_argument("--freq", default="daily", choices=["daily", "weekly", "monthly"])
    parser.add_argument("--window", type=int, default=None, help="滚动窗口大小（默认按周期自动选择）")
    args = parser.parse_args()

    freq_cfg = FREQ_CONFIG[args.freq]
    window = args.window or freq_cfg["window"]
    horizons = freq_cfg["horizons"]
    main_horizon = freq_cfg["main_horizon"]

    print(f"\n{'#' * 80}")
    print(f"  DIF 极值接近度分析（不偷看未来）")
    print(f"  标的: {args.ts_code} | 周期: {freq_cfg['label']} | 滚动窗口: {window}")
    print(f"  评估窗口: {horizons} | 主窗口: T+{main_horizon}")
    print(f"{'#' * 80}")

    # 1. 加载数据
    df = load_dif_data(args.ts_code, args.freq)
    if len(df) < window + max(horizons):
        print(f"[ERROR] 数据不足: 需要至少 {window + max(horizons)} 行，实际 {len(df)} 行")
        sys.exit(1)

    # 2. 计算特征（只看过去）
    print(f"\n[INFO] 计算特征（滚动窗口={window}）...")
    df = compute_proximity_features(df, window)

    # 3. 计算未来收益
    print(f"[INFO] 计算未来收益 {horizons}...")
    df = compute_forward_returns(df, horizons)

    # 有效数据行数（有特征 + 有收益的行）
    valid = df.dropna(subset=["percentile", f"ret_{main_horizon}"])
    print(f"[INFO] 有效样本数: {len(valid)}（总 {len(df)} - 窗口预热 {window} - 尾部 {max(horizons)}）")

    # 4. 分析1: 按DIF分位数分段
    result1 = analyze_by_percentile(df, horizons, main_horizon)
    print_table(f"分析1: DIF 历史分位数 × 未来收益（{freq_cfg['label']}）", result1)

    # 5. 分析2: 高位接近度
    result2 = analyze_high_proximity(df, horizons, main_horizon)
    print_table(f"分析2: DIF 高位接近度（接近历史最大值）× 未来收益", result2)

    # 6. 分析3: 低位接近度
    result3 = analyze_low_proximity(df, horizons, main_horizon)
    print_table(f"分析3: DIF 低位接近度（接近历史最小值）× 未来收益", result3)

    # 7. 分析4: 动量衰减信号
    result4 = analyze_momentum_decay(df, horizons, main_horizon)
    print_table(f"分析4: 动量衰减/回升信号（位置+方向组合）× 未来收益", result4)

    # 8. 汇总
    print(f"\n{'#' * 80}")
    print(f"  分析完成 | {args.ts_code} {freq_cfg['label']}")
    print(f"{'#' * 80}")


if __name__ == "__main__":
    main()
