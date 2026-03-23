"""
策略注册表

所有策略在此注册，通过 get_strategy() 获取实例。
"""
from app.strategies.base import BaseStrategy  # noqa: F401
from app.strategies.dual_ma import DualMAStrategy
from app.strategies.rsi import RSIStrategy
from app.strategies.macd import MACDStrategy
from app.strategies.bollinger import BollingerStrategy
from app.strategies.composite import CompositeStrategy

# 策略注册表
STRATEGY_REGISTRY = {
    "dual_ma": DualMAStrategy,
    "rsi": RSIStrategy,
    "macd": MACDStrategy,
    "bollinger": BollingerStrategy,
    "composite": CompositeStrategy,
}


def get_strategy(name: str, **kwargs) -> BaseStrategy:
    """根据名称获取策略实例"""
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"未知策略: {name}，可用: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[name](**kwargs)


def list_strategies() -> list[str]:
    """列出所有可用策略"""
    return list(STRATEGY_REGISTRY.keys())
