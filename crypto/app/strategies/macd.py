"""
MACD 策略

规则：
  - DIF 上穿 DEA（金叉）→ BUY
  - DIF 下穿 DEA（死叉）→ SELL
  - MACD 柱翻正/负 → 辅助信号
"""
import pandas as pd

from app.strategies.base import BaseStrategy
from app.services.indicator import add_macd


class MACDStrategy(BaseStrategy):
    """MACD 金叉死叉策略"""

    name = "macd"

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_macd(df, self.fast, self.slow, self.signal)
        if len(df) < self.slow + self.signal + 2:
            return "HOLD", "数据不足"

        curr_dif = df["macd_dif"].iloc[-1]
        curr_dea = df["macd_dea"].iloc[-1]
        prev_dif = df["macd_dif"].iloc[-2]
        prev_dea = df["macd_dea"].iloc[-2]
        curr_hist = df["macd_hist"].iloc[-1]
        prev_hist = df["macd_hist"].iloc[-2]

        # 金叉
        if prev_dif <= prev_dea and curr_dif > curr_dea:
            return "BUY", f"MACD金叉: DIF={curr_dif:.4f}, DEA={curr_dea:.4f}"

        # 死叉
        if prev_dif >= prev_dea and curr_dif < curr_dea:
            return "SELL", f"MACD死叉: DIF={curr_dif:.4f}, DEA={curr_dea:.4f}"

        # 柱状图翻正
        if prev_hist < 0 and curr_hist > 0:
            return "BUY", f"MACD柱翻正: {prev_hist:.4f} → {curr_hist:.4f}"

        # 柱状图翻负
        if prev_hist > 0 and curr_hist < 0:
            return "SELL", f"MACD柱翻负: {prev_hist:.4f} → {curr_hist:.4f}"

        return "HOLD", f"DIF={curr_dif:.4f}, DEA={curr_dea:.4f}, HIST={curr_hist:.4f}"
