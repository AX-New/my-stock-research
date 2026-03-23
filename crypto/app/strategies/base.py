"""策略基类"""
import json
from abc import ABC, abstractmethod

import pandas as pd


class BaseStrategy(ABC):
    """所有策略继承此基类"""

    name: str = "base"

    @abstractmethod
    def compute_signal(self, df: pd.DataFrame) -> tuple:
        """
        计算交易信号

        Args:
            df: K线数据 DataFrame（含 OHLCV）

        Returns:
            (signal, reason): signal 为 'BUY'/'SELL'/'HOLD'，reason 为信号说明
        """
        pass

    def get_indicators_snapshot(self, df: pd.DataFrame) -> str:
        """获取最新一行指标的 JSON 快照"""
        if df.empty:
            return "{}"
        last = df.iloc[-1]
        snapshot = {}
        for col in df.columns:
            if col not in ("open", "high", "low", "close", "volume"):
                val = last.get(col)
                if pd.notna(val):
                    snapshot[col] = round(float(val), 6)
        return json.dumps(snapshot, ensure_ascii=False)
