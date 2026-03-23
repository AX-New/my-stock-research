"""数据相关 API"""
from fastapi import APIRouter, Query

from app.api.response import ok, fail
from app.services.data_sync import sync_klines, sync_symbols, sync_all, load_klines

router = APIRouter(prefix="/data")


@router.get("/symbols/sync")
def api_sync_symbols(exchange: str = None):
    """同步交易对元数据"""
    try:
        count = sync_symbols(exchange)
        return ok({"count": count})
    except Exception as e:
        return fail(str(e))


@router.get("/klines/sync")
def api_sync_klines(symbol: str = "BTC/USDT", timeframe: str = "1h",
                    exchange: str = None, days: int = None):
    """同步指定交易对的K线数据"""
    try:
        count = sync_klines(symbol, timeframe, exchange, days)
        return ok({"count": count, "symbol": symbol, "timeframe": timeframe})
    except Exception as e:
        return fail(str(e))


@router.get("/sync/all")
def api_sync_all(exchange: str = None):
    """全量同步所有配置的交易对和周期"""
    try:
        result = sync_all(exchange)
        return ok(result)
    except Exception as e:
        return fail(str(e))


@router.get("/klines")
def api_get_klines(symbol: str = "BTC/USDT", timeframe: str = "1h",
                   exchange: str = None,
                   start_date: str = None, end_date: str = None,
                   limit: int = Query(default=500, le=5000)):
    """查询K线数据"""
    try:
        df = load_klines(symbol, timeframe, exchange, start_date, end_date, limit)
        if df.empty:
            return ok([])
        # 转为列表
        df = df.reset_index()
        df["open_time"] = df["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
        return ok(df.to_dict(orient="records"))
    except Exception as e:
        return fail(str(e))
