"""
布林带策略

规则：
  - 价格触及下轨 + RSI 较低 → BUY
  - 价格触及上轨 + RSI 较高 → SELL
"""
import pandas as pd

from app.strategies.base import BaseStrategy
from app.services.indicator import add_bollinger, add_rsi


class BollingerStrategy(BaseStrategy):
    """布林带策略"""

    name = "bollinger"

    def __init__(self, period: int = 20, std_dev: float = 2.0):
        self.period = period
        self.std_dev = std_dev

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_bollinger(df, self.period, self.std_dev)
        df = add_rsi(df)
        if len(df) < self.period + 2:
            return "HOLD", "数据不足"

        close = df["close"].iloc[-1]
        upper = df["bb_upper"].iloc[-1]
        lower = df["bb_lower"].iloc[-1]
        mid = df["bb_mid"].iloc[-1]
        rsi = df["rsi"].iloc[-1]

        # 触及下轨 + RSI 较低
        if close <= lower and rsi < 40:
            return "BUY", (f"触及布林下轨: close={close:.2f}, "
                           f"lower={lower:.2f}, RSI={rsi:.1f}")

        # 触及上轨 + RSI 较高
        if close >= upper and rsi > 60:
            return "SELL", (f"触及布林上轨: close={close:.2f}, "
                            f"upper={upper:.2f}, RSI={rsi:.1f}")

        return "HOLD", (f"close={close:.2f}, "
                        f"BB=[{lower:.2f}, {mid:.2f}, {upper:.2f}], RSI={rsi:.1f}")
