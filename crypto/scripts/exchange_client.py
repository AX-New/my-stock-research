"""
加密货币量化交易 - 交易所客户端

统一封装 Binance / OKX 的连接和操作，基于 ccxt 库。
支持：获取K线、查询余额、下单、查询订单等。
"""
import sys
import os
import ccxt

sys.path.insert(0, os.path.dirname(__file__))
from config import (
    BINANCE_API_KEY, BINANCE_API_SECRET,
    OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE,
    DEFAULT_EXCHANGE, HTTP_PROXY, HTTPS_PROXY,
)

# 添加 lib 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from logger import get_logger

logger = get_logger('crypto.exchange')


def create_exchange(exchange_name: str = None, sandbox: bool = False) -> ccxt.Exchange:
    """
    创建交易所客户端实例

    Args:
        exchange_name: 交易所名称（binance/okx），默认从配置读取
        sandbox: 是否使用沙盒/测试网

    Returns:
        ccxt.Exchange 实例
    """
    name = (exchange_name or DEFAULT_EXCHANGE).lower()

    # 代理配置
    proxies = {}
    if HTTP_PROXY:
        proxies['http'] = HTTP_PROXY
        proxies['https'] = HTTPS_PROXY or HTTP_PROXY

    if name == 'binance':
        exchange = ccxt.binance({
            'apiKey': BINANCE_API_KEY or None,
            'secret': BINANCE_API_SECRET or None,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {
                'defaultType': 'spot',  # 现货
                'adjustForTimeDifference': True,
            },
        })
    elif name == 'okx':
        exchange = ccxt.okx({
            'apiKey': OKX_API_KEY or None,
            'secret': OKX_API_SECRET or None,
            'password': OKX_PASSPHRASE or None,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {
                'defaultType': 'spot',
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
    """
    获取K线数据

    Args:
        exchange: 交易所实例
        symbol: 交易对（如 BTC/USDT）
        timeframe: K线周期（1m/5m/15m/1h/4h/1d）
        since: 起始时间戳（毫秒）
        limit: 数量限制

    Returns:
        [[timestamp, open, high, low, close, volume], ...]
    """
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
        ticker = exchange.fetch_ticker(symbol)
        return ticker
    except Exception as e:
        logger.error(f"获取行情失败: {symbol} - {e}")
        raise


def fetch_balance(exchange: ccxt.Exchange) -> dict:
    """查询账户余额"""
    try:
        balance = exchange.fetch_balance()
        # 只返回有余额的币种
        result = {}
        for currency, amount in balance['total'].items():
            if amount and amount > 0:
                result[currency] = {
                    'total': amount,
                    'free': balance['free'].get(currency, 0),
                    'used': balance['used'].get(currency, 0),
                }
        return result
    except Exception as e:
        logger.error(f"查询余额失败: {e}")
        raise


def place_order(exchange: ccxt.Exchange, symbol: str, side: str,
                amount: float, price: float = None,
                order_type: str = 'market') -> dict:
    """
    下单

    Args:
        exchange: 交易所实例
        symbol: 交易对
        side: 方向（buy/sell）
        amount: 数量
        price: 价格（限价单必填）
        order_type: 订单类型（market/limit）

    Returns:
        订单信息字典
    """
    try:
        if order_type == 'market':
            order = exchange.create_order(symbol, 'market', side, amount)
        else:
            if price is None:
                raise ValueError("限价单必须指定价格")
            order = exchange.create_order(symbol, 'limit', side, amount, price)

        logger.info(f"下单成功: {side} {amount} {symbol} @ {price or 'market'}, "
                     f"订单ID: {order.get('id')}")
        return order
    except Exception as e:
        logger.error(f"下单失败: {side} {amount} {symbol} - {e}")
        raise


def get_exchange_name(exchange: ccxt.Exchange) -> str:
    """获取交易所名称"""
    return exchange.id
