"""
RSI 策略

规则：
  - RSI < oversold → BUY（超卖反弹）
  - RSI > overbought → SELL（超买回落）
"""
import pandas as pd

from app.strategies.base import BaseStrategy
from app.services.indicator import add_rsi


class RSIStrategy(BaseStrategy):
    """RSI 超买超卖策略"""

    name = "rsi"

    def __init__(self, period: int = 14, oversold: int = 30, overbought: int = 70):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_rsi(df, self.period)
        if len(df) < self.period + 2:
            return "HOLD", f"数据不足（需要至少 {self.period + 2} 条）"

        rsi = df["rsi"].iloc[-1]
        prev_rsi = df["rsi"].iloc[-2]

        # 从超卖区回升
        if prev_rsi < self.oversold and rsi >= self.oversold:
            return "BUY", f"RSI从超卖区回升: {prev_rsi:.1f} → {rsi:.1f}"

        # 仍在超卖区
        if rsi < self.oversold:
            return "BUY", f"RSI超卖: {rsi:.1f} < {self.oversold}"

        # 从超买区回落
        if prev_rsi > self.overbought and rsi <= self.overbought:
            return "SELL", f"RSI从超买区回落: {prev_rsi:.1f} → {rsi:.1f}"

        # 仍在超买区
        if rsi > self.overbought:
            return "SELL", f"RSI超买: {rsi:.1f} > {self.overbought}"

        return "HOLD", f"RSI={rsi:.1f}"
