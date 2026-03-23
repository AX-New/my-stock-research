"""策略相关 API"""
from fastapi import APIRouter

from app.api.response import ok, fail
from app.strategies import list_strategies, get_strategy
from app.services.data_sync import load_klines

router = APIRouter(prefix="/strategy")


@router.get("/list")
def api_list_strategies():
    """列出所有可用策略"""
    return ok(list_strategies())


@router.get("/signal")
def api_compute_signal(symbol: str = "BTC/USDT", timeframe: str = "1h",
                       strategy: str = "dual_ma", exchange: str = None):
    """计算当前策略信号"""
    try:
        df = load_klines(symbol, timeframe, exchange)
        if df.empty or len(df) < 50:
            return fail(f"K线数据不足({len(df)}条)，请先同步数据")

        s = get_strategy(strategy)
        signal, reason = s.compute_signal(df.copy())
        indicators = s.get_indicators_snapshot(df)

        return ok({
            "symbol": symbol,
            "timeframe": timeframe,
            "strategy": strategy,
            "signal": signal,
            "reason": reason,
            "indicators": indicators,
            "price": float(df["close"].iloc[-1]),
            "time": str(df.index[-1]),
        })
    except Exception as e:
        return fail(str(e))
