"""回测相关 API"""
from fastapi import APIRouter

from app.api.response import ok, fail
from app.services.backtest_engine import BacktestEngine
from app.services.data_sync import load_klines

router = APIRouter(prefix="/backtest")


@router.get("/run")
def api_run_backtest(symbol: str = "BTC/USDT", timeframe: str = "1h",
                     strategy: str = "dual_ma", exchange: str = None,
                     capital: float = 100000,
                     stop_loss: float = 0.05, take_profit: float = 0.10):
    """执行回测"""
    try:
        df = load_klines(symbol, timeframe, exchange)
        if df.empty or len(df) < 100:
            return fail(f"K线数据不足({len(df)}条)")

        engine = BacktestEngine(
            initial_capital=capital,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        result = engine.run(df, strategy)

        # trades 中的时间转字符串
        for t in result.get("trades", []):
            if hasattr(t["time"], "strftime"):
                t["time"] = t["time"].strftime("%Y-%m-%d %H:%M:%S")

        return ok(result)
    except Exception as e:
        return fail(str(e))
