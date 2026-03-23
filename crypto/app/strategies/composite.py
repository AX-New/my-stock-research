"""
组合策略：多策略投票

规则：
  - 统计所有子策略的信号
  - BUY 票数 >= 阈值 → BUY
  - SELL 票数 >= 阈值 → SELL
"""
import pandas as pd

from app.strategies.base import BaseStrategy


class CompositeStrategy(BaseStrategy):
    """多策略投票组合"""

    name = "composite"

    def __init__(self, strategies: list = None,
                 buy_threshold: int = 2, sell_threshold: int = 2):
        if strategies is None:
            # 延迟导入，避免循环引用
            from app.strategies.dual_ma import DualMAStrategy
            from app.strategies.rsi import RSIStrategy
            from app.strategies.macd import MACDStrategy
            strategies = [DualMAStrategy(), RSIStrategy(), MACDStrategy()]
        self.strategies = strategies
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        votes = {"BUY": [], "SELL": [], "HOLD": []}

        for s in self.strategies:
            signal, reason = s.compute_signal(df.copy())
            votes[signal].append(f"{s.name}: {reason}")

        buy_count = len(votes["BUY"])
        sell_count = len(votes["SELL"])

        details = []
        for signal_type in ["BUY", "SELL", "HOLD"]:
            if votes[signal_type]:
                details.append(
                    f"{signal_type}({len(votes[signal_type])}): "
                    + "; ".join(votes[signal_type])
                )
        detail_str = " | ".join(details)

        if buy_count >= self.buy_threshold:
            return "BUY", f"组合信号BUY({buy_count}票) - {detail_str}"

        if sell_count >= self.sell_threshold:
            return "SELL", f"组合信号SELL({sell_count}票) - {detail_str}"

        return "HOLD", f"未达阈值 - {detail_str}"
