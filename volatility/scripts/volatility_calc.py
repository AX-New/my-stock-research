"""波动率指标计算模块

提供以下指标的计算:
- TR (True Range): 真实波动幅度
- ATR (Average True Range): 平均真实波动幅度
- ATR Ratio: ATR占收盘价百分比，消除价格量纲
- HV (Historical Volatility): 历史波动率（收益率标准差）
- ADR (Average Daily Range): 平均日内振幅
- BBW (Bollinger Band Width): 布林带宽度
"""
import numpy as np
import pandas as pd


def calc_tr(df: pd.DataFrame) -> pd.Series:
    """计算 True Range

    TR = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    第一根K线的TR = High - Low（无前收盘价）

    参数:
        df: 必须包含 high, low, close 列，按日期升序

    返回:
        TR 序列（与 df 等长）
    """
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)

    # 三个分量
    hl = high - low
    hpc = (high - prev_close).abs()
    lpc = (low - prev_close).abs()

    tr = pd.concat([hl, hpc, lpc], axis=1).max(axis=1)
    # 第一行没有prev_close，用 high-low
    tr.iloc[0] = hl.iloc[0]
    return tr


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算 ATR (Average True Range)

    使用 Wilder's smoothing（指数移动平均的变体）:
    ATR(1) = TR(1)的period日简单均值
    ATR(t) = (ATR(t-1) * (period-1) + TR(t)) / period

    参数:
        df: K线数据
        period: ATR周期，默认14

    返回:
        ATR 序列
    """
    tr = calc_tr(df)

    # Wilder's smoothing
    atr = pd.Series(np.nan, index=tr.index)
    # 前 period 根的简单均值作为初始值
    if len(tr) >= period:
        atr.iloc[period - 1] = tr.iloc[:period].mean()
        for i in range(period, len(tr)):
            atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + tr.iloc[i]) / period
    return atr


def calc_atr_ratio(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算 ATR Ratio = ATR / Close * 100（百分比）

    消除价格量纲，使不同价位的股票可比。
    例如 ATR_ratio=3.0 表示 ATR 为收盘价的 3%。
    """
    atr = calc_atr(df, period)
    close = df["close"].astype(float)
    return (atr / close * 100).round(4)


def calc_daily_range_pct(df: pd.DataFrame) -> pd.Series:
    """计算日内振幅百分比 = (High - Low) / Close * 100"""
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    return ((high - low) / close * 100).round(4)


def calc_adr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """计算 ADR (Average Daily Range) 百分比

    ADR = SMA(日内振幅%, period)
    """
    range_pct = calc_daily_range_pct(df)
    return range_pct.rolling(window=period, min_periods=period).mean().round(4)


def calc_historical_volatility(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """计算历史波动率 (Historical Volatility)

    HV = std(ln(Close/PrevClose)) * sqrt(252) * 100
    年化波动率，单位百分比
    """
    close = df["close"].astype(float)
    log_returns = np.log(close / close.shift(1))
    hv = log_returns.rolling(window=period, min_periods=period).std() * np.sqrt(252) * 100
    return hv.round(4)


def calc_bbw(df: pd.DataFrame, period: int = 20, num_std: float = 2.0) -> pd.Series:
    """计算布林带宽度 BBW = (Upper - Lower) / Mid * 100

    Upper = SMA(Close, N) + num_std * std(Close, N)
    Lower = SMA(Close, N) - num_std * std(Close, N)
    Mid = SMA(Close, N)
    """
    close = df["close"].astype(float)
    mid = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    bbw = ((upper - lower) / mid * 100).round(4)
    return bbw


def calc_all_volatility_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算所有波动率指标，返回完整DataFrame

    新增列:
    - tr: True Range
    - atr_5/10/14/20: 不同周期ATR
    - atr_ratio_5/10/14/20: ATR百分比
    - range_pct: 日内振幅%
    - adr_14/20: 平均日内振幅%
    - hv_10/20/60: 历史波动率
    - bbw_20: 布林带宽度
    """
    result = df.copy()

    # True Range
    result["tr"] = calc_tr(df)
    result["tr_pct"] = (result["tr"] / df["close"].astype(float) * 100).round(4)

    # ATR 多周期
    for p in [5, 10, 14, 20]:
        result[f"atr_{p}"] = calc_atr(df, p)
        result[f"atr_ratio_{p}"] = calc_atr_ratio(df, p)

    # 日内振幅
    result["range_pct"] = calc_daily_range_pct(df)

    # ADR
    for p in [14, 20]:
        result[f"adr_{p}"] = calc_adr(df, p)

    # 历史波动率
    for p in [10, 20, 60]:
        result[f"hv_{p}"] = calc_historical_volatility(df, p)

    # 布林带宽度
    result["bbw_20"] = calc_bbw(df)

    return result
