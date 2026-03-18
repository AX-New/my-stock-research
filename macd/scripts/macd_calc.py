"""MACD 计算核心逻辑（EMA + DIF/DEA/MACD）"""
import pandas as pd


def calc_macd(close_series: pd.Series, short=12, long=26, signal=9) -> pd.DataFrame:
    """
    计算 MACD 指标

    输入: 收盘价序列（按时间升序排列）
    输出: DataFrame [dif, dea, macd]

    计算逻辑:
    - EMA_short = EMA(close, 12)
    - EMA_long  = EMA(close, 26)
    - DIF = EMA_short - EMA_long
    - DEA = EMA(DIF, 9)
    - MACD = (DIF - DEA) × 2
    """
    ema_short = close_series.ewm(span=short, adjust=False).mean()
    ema_long = close_series.ewm(span=long, adjust=False).mean()
    dif = ema_short - ema_long
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_val = (dif - dea) * 2

    return pd.DataFrame({
        "dif": dif.round(4),
        "dea": dea.round(4),
        "macd": macd_val.round(4),
    })
