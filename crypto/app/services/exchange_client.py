"""
交易所客户端

统一封装 Binance / OKX 的连接和操作，基于 ccxt 库。
支持：获取K线、查询余额、下单、查询订单、获取交易对信息。
"""
import ccxt

from app.config import Config
from app.logger import get_logger

logger = get_logger("crypto.exchange")


def create_exchange(exchange_name: str = None, sandbox: bool = False) -> ccxt.Exchange:
    """
    创建交易所客户端实例

    Args:
        exchange_name: 交易所名称（binance/okx），默认从配置读取
        sandbox: 是否使用沙盒/测试网
    """
    name = (exchange_name or Config.DEFAULT_EXCHANGE).lower()

    # 代理配置
    proxies = {}
    if Config.HTTP_PROXY:
        proxies["http"] = Config.HTTP_PROXY
        proxies["https"] = Config.HTTPS_PROXY or Config.HTTP_PROXY

    if name == "binance":
        exchange = ccxt.binance({
            "apiKey": Config.BINANCE_API_KEY or None,
            "secret": Config.BINANCE_API_SECRET or None,
            "enableRateLimit": True,
            "proxies": proxies,
            "options": {
                "defaultType": "spot",
                "adjustForTimeDifference": True,
            },
        })
    elif name == "okx":
        exchange = ccxt.okx({
            "apiKey": Config.OKX_API_KEY or None,
            "secret": Config.OKX_API_SECRET or None,
            "password": Config.OKX_PASSPHRASE or None,
            "enableRateLimit": True,
            "proxies": proxies,
            "options": {
                "defaultType": "spot",
            },
        })
    else:
        raise ValueError(f"不支持的交易所: {name}，目前仅支持 binance / okx")

    if sandbox:
        exchange.set_sandbox_mode(True)
        logger.info(f"交易所 {name} 已启用沙盒模式")

    logger.info(f"交易所客户端已创建: {name}")
    return exchange


def fetch_ohlcv(exchange: ccxt.Exchange, symbol: str, timeframe: str,
                since: int = None, limit: int = None) -> list:
    """获取K线数据"""
    try:
        ohlcv = exchange.fetch_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            since=since,
            limit=limit or 500,
        )
        logger.info(f"获取K线: {symbol} {timeframe}, 共 {len(ohlcv)} 条")
        return ohlcv
    except Exception as e:
        logger.error(f"获取K线失败: {symbol} {timeframe} - {e}")
        raise


def fetch_ticker(exchange: ccxt.Exchange, symbol: str) -> dict:
    """获取最新行情"""
    try:
        return exchange.fetch_ticker(symbol)
    except Exception as e:
        logger.error(f"获取行情失败: {symbol} - {e}")
        raise


def fetch_balance(exchange: ccxt.Exchange) -> dict:
    """查询账户余额（只返回有余额的币种）"""
    try:
        balance = exchange.fetch_balance()
        result = {}
        for currency, amount in balance["total"].items():
            if amount and amount > 0:
                result[currency] = {
                    "total": amount,
                    "free": balance["free"].get(currency, 0),
                    "used": balance["used"].get(currency, 0),
                }
        return result
    except Exception as e:
        logger.error(f"查询余额失败: {e}")
        raise


def place_order(exchange: ccxt.Exchange, symbol: str, side: str,
                amount: float, price: float = None,
                order_type: str = "market") -> dict:
    """下单"""
    try:
        if order_type == "market":
            order = exchange.create_order(symbol, "market", side, amount)
        else:
            if price is None:
                raise ValueError("限价单必须指定价格")
            order = exchange.create_order(symbol, "limit", side, amount, price)

        logger.info(f"下单成功: {side} {amount} {symbol} @ {price or 'market'}, "
                    f"订单ID: {order.get('id')}")
        return order
    except Exception as e:
        logger.error(f"下单失败: {side} {amount} {symbol} - {e}")
        raise


def fetch_markets(exchange: ccxt.Exchange) -> list[dict]:
    """获取交易所所有交易对信息"""
    try:
        markets = exchange.load_markets()
        result = []
        for symbol, info in markets.items():
            if info.get("quote") == "USDT" and info.get("active"):
                result.append({
                    "symbol": symbol,
                    "base": info.get("base", ""),
                    "quote": info.get("quote", ""),
                    "price_precision": info.get("precision", {}).get("price", 8),
                    "amount_precision": info.get("precision", {}).get("amount", 8),
                    "min_amount": info.get("limits", {}).get("amount", {}).get("min", 0),
                    "min_cost": info.get("limits", {}).get("cost", {}).get("min", 0),
                    "maker_fee": info.get("maker", 0.001),
                    "taker_fee": info.get("taker", 0.001),
                })
        logger.info(f"获取交易对: {len(result)} 个 USDT 交易对")
        return result
    except Exception as e:
        logger.error(f"获取交易对失败: {e}")
        raise


def get_exchange_name(exchange: ccxt.Exchange) -> str:
    """获取交易所名称"""
    return exchange.id
