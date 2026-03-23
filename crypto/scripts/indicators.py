"""
加密货币量化交易 - 技术指标计算模块

所有指标基于 pandas 计算，不依赖 talib。
输入：包含 open/high/low/close/volume 列的 DataFrame
输出：在原 DataFrame 上新增指标列
"""
import numpy as np
import pandas as pd


def add_ma(df: pd.DataFrame, periods: list = None) -> pd.DataFrame:
    """添加移动平均线"""
    if periods is None:
        periods = [7, 25, 99]
    for p in periods:
        df[f'ma_{p}'] = df['close'].rolling(window=p).mean()
    return df


def add_ema(df: pd.DataFrame, periods: list = None) -> pd.DataFrame:
    """添加指数移动平均线"""
    if periods is None:
        periods = [12, 26]
    for p in periods:
        df[f'ema_{p}'] = df['close'].ewm(span=p, adjust=False).mean()
    return df


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """添加 RSI 指标"""
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['rsi'] = 100 - (100 / (1 + rs))
    return df


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26,
             signal: int = 9) -> pd.DataFrame:
    """添加 MACD 指标"""
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    df['macd_dif'] = ema_fast - ema_slow
    df['macd_dea'] = df['macd_dif'].ewm(span=signal, adjust=False).mean()
    df['macd_hist'] = 2 * (df['macd_dif'] - df['macd_dea'])
    return df


def add_bollinger(df: pd.DataFrame, period: int = 20,
                  std_dev: float = 2.0) -> pd.DataFrame:
    """添加布林带"""
    df['bb_mid'] = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()
    df['bb_upper'] = df['bb_mid'] + std_dev * rolling_std
    df['bb_lower'] = df['bb_mid'] - std_dev * rolling_std
    return df


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """添加 ATR（平均真实波幅）"""
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift(1)).abs()
    low_close = (df['low'] - df['close'].shift(1)).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = true_range.rolling(window=period).mean()
    return df


def add_volume_ma(df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """添加成交量均线"""
    df['volume_ma'] = df['volume'].rolling(window=period).mean()
    df['volume_ratio'] = df['volume'] / df['volume_ma']
    return df


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """添加所有常用指标"""
    df = add_ma(df, [7, 25, 99])
    df = add_ema(df, [12, 26])
    df = add_rsi(df)
    df = add_macd(df)
    df = add_bollinger(df)
    df = add_atr(df)
    df = add_volume_ma(df)
    return df
