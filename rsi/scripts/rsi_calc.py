"""RSI 计算引擎 — Wilder 平滑法

RSI = 100 - 100 / (1 + RS)
RS  = 平均涨幅 / 平均跌幅

使用 Wilder 平滑（等价于 EMA alpha=1/N）:
- 初始值: 前 N 个涨跌幅的简单平均
- 后续: avg = (prev_avg * (N-1) + current) / N

输入: pandas Series (收盘价)
输出: pandas Series (RSI值 0~100)
"""
import numpy as np
import pandas as pd


def calc_rsi_series(close: pd.Series, period: int = 14) -> pd.Series:
    """计算单个周期的 RSI

    使用 pandas ewm 实现 Wilder 平滑，性能最优。
    ewm(com=period-1, adjust=False) 等价于 alpha=1/period 的指数平滑。

    close: 收盘价序列
    period: RSI 周期（6/12/14/24）
    Returns: RSI 序列，前 period 个值为 NaN
    """
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    # Wilder 平滑 = EMA(alpha=1/period) = EMA(com=period-1)
    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()

    # RS 计算（avg_loss=0 时 RSI=100）
    rs = avg_gain / avg_loss
    rsi = 100 - 100 / (1 + rs)

    return rsi.round(2)


def calc_all_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """计算 4 个周期的 RSI 并添加到 DataFrame

    要求 df 包含 close 列，按 trade_date 升序排列。
    添加列: rsi_6, rsi_12, rsi_14, rsi_24
    """
    close = df["close"]

    for period in [6, 12, 14, 24]:
        df[f"rsi_{period}"] = calc_rsi_series(close, period)

    return df
