"""
双均线策略

规则：
  - 快线上穿慢线（金叉）→ BUY
  - 快线下穿慢线（死叉）→ SELL
"""
import pandas as pd

from app.strategies.base import BaseStrategy
from app.services.indicator import add_ma


class DualMAStrategy(BaseStrategy):
    """双均线交叉策略"""

    name = "dual_ma"

    def __init__(self, fast: int = 7, slow: int = 25):
        self.fast = fast
        self.slow = slow

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_ma(df, [self.fast, self.slow])
        if len(df) < self.slow + 2:
            return "HOLD", f"数据不足（需要至少 {self.slow + 2} 条）"

        fast_col = f"ma_{self.fast}"
        slow_col = f"ma_{self.slow}"

        curr_fast = df[fast_col].iloc[-1]
        curr_slow = df[slow_col].iloc[-1]
        prev_fast = df[fast_col].iloc[-2]
        prev_slow = df[slow_col].iloc[-2]

        # 金叉
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return "BUY", (f"MA{self.fast}上穿MA{self.slow}（金叉），"
                           f"MA{self.fast}={curr_fast:.2f}, MA{self.slow}={curr_slow:.2f}")

        # 死叉
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            return "SELL", (f"MA{self.fast}下穿MA{self.slow}（死叉），"
                            f"MA{self.fast}={curr_fast:.2f}, MA{self.slow}={curr_slow:.2f}")

        return "HOLD", f"MA{self.fast}={curr_fast:.2f}, MA{self.slow}={curr_slow:.2f}"
