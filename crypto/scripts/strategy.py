"""
加密货币量化交易 - 策略模块

策略框架：
  - 所有策略继承 BaseStrategy
  - compute_signal() 返回 BUY / SELL / HOLD
  - 内置策略：双均线、RSI、MACD、布林带

用法：
  strategy = DualMAStrategy(fast=7, slow=25)
  signal, reason = strategy.compute_signal(df)
"""
import json
from abc import ABC, abstractmethod

import pandas as pd

from indicators import (
    add_ma, add_rsi, add_macd, add_bollinger, add_atr, add_volume_ma,
)


class BaseStrategy(ABC):
    """策略基类"""

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
            return '{}'
        last = df.iloc[-1]
        snapshot = {}
        for col in df.columns:
            if col not in ('open', 'high', 'low', 'close', 'volume'):
                val = last.get(col)
                if pd.notna(val):
                    snapshot[col] = round(float(val), 6)
        return json.dumps(snapshot, ensure_ascii=False)


class DualMAStrategy(BaseStrategy):
    """
    双均线策略

    规则：
      - 快线上穿慢线 → BUY
      - 快线下穿慢线 → SELL
      - 其他 → HOLD
    """
    name = "dual_ma"

    def __init__(self, fast: int = 7, slow: int = 25):
        self.fast = fast
        self.slow = slow

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_ma(df, [self.fast, self.slow])
        if len(df) < self.slow + 2:
            return 'HOLD', f'数据不足（需要至少 {self.slow + 2} 条）'

        fast_col = f'ma_{self.fast}'
        slow_col = f'ma_{self.slow}'

        curr_fast = df[fast_col].iloc[-1]
        curr_slow = df[slow_col].iloc[-1]
        prev_fast = df[fast_col].iloc[-2]
        prev_slow = df[slow_col].iloc[-2]

        # 金叉：快线从下方穿越慢线
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return 'BUY', f'MA{self.fast}上穿MA{self.slow}（金叉），MA{self.fast}={curr_fast:.2f}, MA{self.slow}={curr_slow:.2f}'

        # 死叉：快线从上方穿越慢线
        if prev_fast >= prev_slow and curr_fast < curr_slow:
            return 'SELL', f'MA{self.fast}下穿MA{self.slow}（死叉），MA{self.fast}={curr_fast:.2f}, MA{self.slow}={curr_slow:.2f}'

        return 'HOLD', f'MA{self.fast}={curr_fast:.2f}, MA{self.slow}={curr_slow:.2f}'


class RSIStrategy(BaseStrategy):
    """
    RSI 策略

    规则：
      - RSI < oversold → BUY（超卖反弹）
      - RSI > overbought → SELL（超买回落）
      - 其他 → HOLD
    """
    name = "rsi"

    def __init__(self, period: int = 14, oversold: int = 30, overbought: int = 70):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_rsi(df, self.period)
        if len(df) < self.period + 2:
            return 'HOLD', f'数据不足（需要至少 {self.period + 2} 条）'

        rsi = df['rsi'].iloc[-1]
        prev_rsi = df['rsi'].iloc[-2]

        # 从超卖区回升
        if prev_rsi < self.oversold and rsi >= self.oversold:
            return 'BUY', f'RSI从超卖区回升: {prev_rsi:.1f} → {rsi:.1f}'

        # 仍在超卖区
        if rsi < self.oversold:
            return 'BUY', f'RSI超卖: {rsi:.1f} < {self.oversold}'

        # 从超买区回落
        if prev_rsi > self.overbought and rsi <= self.overbought:
            return 'SELL', f'RSI从超买区回落: {prev_rsi:.1f} → {rsi:.1f}'

        # 仍在超买区
        if rsi > self.overbought:
            return 'SELL', f'RSI超买: {rsi:.1f} > {self.overbought}'

        return 'HOLD', f'RSI={rsi:.1f}'


class MACDStrategy(BaseStrategy):
    """
    MACD 策略

    规则：
      - DIF 上穿 DEA（金叉）+ MACD柱由负转正 → BUY
      - DIF 下穿 DEA（死叉）+ MACD柱由正转负 → SELL
    """
    name = "macd"

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_macd(df, self.fast, self.slow, self.signal)
        if len(df) < self.slow + self.signal + 2:
            return 'HOLD', '数据不足'

        curr_dif = df['macd_dif'].iloc[-1]
        curr_dea = df['macd_dea'].iloc[-1]
        prev_dif = df['macd_dif'].iloc[-2]
        prev_dea = df['macd_dea'].iloc[-2]
        curr_hist = df['macd_hist'].iloc[-1]
        prev_hist = df['macd_hist'].iloc[-2]

        # 金叉
        if prev_dif <= prev_dea and curr_dif > curr_dea:
            return 'BUY', f'MACD金叉: DIF={curr_dif:.4f}, DEA={curr_dea:.4f}'

        # 死叉
        if prev_dif >= prev_dea and curr_dif < curr_dea:
            return 'SELL', f'MACD死叉: DIF={curr_dif:.4f}, DEA={curr_dea:.4f}'

        # 柱状图翻正
        if prev_hist < 0 and curr_hist > 0:
            return 'BUY', f'MACD柱翻正: {prev_hist:.4f} → {curr_hist:.4f}'

        # 柱状图翻负
        if prev_hist > 0 and curr_hist < 0:
            return 'SELL', f'MACD柱翻负: {prev_hist:.4f} → {curr_hist:.4f}'

        return 'HOLD', f'DIF={curr_dif:.4f}, DEA={curr_dea:.4f}, HIST={curr_hist:.4f}'


class BollingerStrategy(BaseStrategy):
    """
    布林带策略

    规则：
      - 价格触及下轨 + RSI超卖 → BUY
      - 价格触及上轨 + RSI超买 → SELL
    """
    name = "bollinger"

    def __init__(self, period: int = 20, std_dev: float = 2.0):
        self.period = period
        self.std_dev = std_dev

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        df = add_bollinger(df, self.period, self.std_dev)
        df = add_rsi(df)
        if len(df) < self.period + 2:
            return 'HOLD', '数据不足'

        close = df['close'].iloc[-1]
        upper = df['bb_upper'].iloc[-1]
        lower = df['bb_lower'].iloc[-1]
        mid = df['bb_mid'].iloc[-1]
        rsi = df['rsi'].iloc[-1]

        # 触及下轨 + RSI较低
        if close <= lower and rsi < 40:
            return 'BUY', f'触及布林下轨: close={close:.2f}, lower={lower:.2f}, RSI={rsi:.1f}'

        # 触及上轨 + RSI较高
        if close >= upper and rsi > 60:
            return 'SELL', f'触及布林上轨: close={close:.2f}, upper={upper:.2f}, RSI={rsi:.1f}'

        return 'HOLD', f'close={close:.2f}, BB=[{lower:.2f}, {mid:.2f}, {upper:.2f}], RSI={rsi:.1f}'


class CompositeStrategy(BaseStrategy):
    """
    组合策略：多策略投票

    规则：
      - 统计所有子策略的信号
      - BUY票数 >= 阈值 → BUY
      - SELL票数 >= 阈值 → SELL
      - 否则 → HOLD
    """
    name = "composite"

    def __init__(self, strategies: list = None, buy_threshold: int = 2,
                 sell_threshold: int = 2):
        if strategies is None:
            strategies = [
                DualMAStrategy(),
                RSIStrategy(),
                MACDStrategy(),
            ]
        self.strategies = strategies
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def compute_signal(self, df: pd.DataFrame) -> tuple:
        votes = {'BUY': [], 'SELL': [], 'HOLD': []}

        for s in self.strategies:
            # 每个策略用数据的副本，避免互相干扰
            signal, reason = s.compute_signal(df.copy())
            votes[signal].append(f"{s.name}: {reason}")

        buy_count = len(votes['BUY'])
        sell_count = len(votes['SELL'])

        details = []
        for signal_type in ['BUY', 'SELL', 'HOLD']:
            if votes[signal_type]:
                details.append(f"{signal_type}({len(votes[signal_type])}): " +
                               "; ".join(votes[signal_type]))

        detail_str = " | ".join(details)

        if buy_count >= self.buy_threshold:
            return 'BUY', f'组合信号BUY({buy_count}票) - {detail_str}'

        if sell_count >= self.sell_threshold:
            return 'SELL', f'组合信号SELL({sell_count}票) - {detail_str}'

        return 'HOLD', f'未达阈值 - {detail_str}'


# 策略注册表
STRATEGY_REGISTRY = {
    'dual_ma': DualMAStrategy,
    'rsi': RSIStrategy,
    'macd': MACDStrategy,
    'bollinger': BollingerStrategy,
    'composite': CompositeStrategy,
}


def get_strategy(name: str, **kwargs) -> BaseStrategy:
    """根据名称获取策略实例"""
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"未知策略: {name}，可用: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[name](**kwargs)
