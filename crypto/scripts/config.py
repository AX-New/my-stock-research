"""
加密货币量化交易 - 配置模块

支持交易所：Binance / OKX
数据库：stock_crypto（本地 MySQL）
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# MySQL 数据库配置
# ============================================================
MYSQL_HOST = os.getenv('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3307))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root')

# 写入库：stock_crypto
WRITE_DB_NAME = "stock_crypto"
WRITE_DB_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{WRITE_DB_NAME}?charset=utf8mb4"
)

# ============================================================
# 交易所 API 配置
# 从环境变量读取，不在代码中硬编码密钥
# ============================================================

# Binance 配置
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', '')

# OKX 配置
OKX_API_KEY = os.getenv('OKX_API_KEY', '')
OKX_API_SECRET = os.getenv('OKX_API_SECRET', '')
OKX_PASSPHRASE = os.getenv('OKX_PASSPHRASE', '')

# 默认交易所（binance / okx）
DEFAULT_EXCHANGE = os.getenv('CRYPTO_EXCHANGE', 'binance')

# ============================================================
# 交易参数
# ============================================================
# 默认交易对
DEFAULT_SYMBOL = 'BTC/USDT'

# K线周期映射
TIMEFRAME_MAP = {
    '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
    '1h': '1h', '4h': '4h', '1d': '1d', '1w': '1w',
}

# 默认K线周期
DEFAULT_TIMEFRAME = '1h'

# 每次获取K线数量上限
KLINE_FETCH_LIMIT = 1000

# ============================================================
# 策略默认参数
# ============================================================
# 双均线策略
MA_FAST = 7
MA_SLOW = 25

# RSI 策略
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70

# MACD 策略
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# ============================================================
# 风控参数
# ============================================================
# 单笔最大仓位比例
MAX_POSITION_RATIO = 0.3

# 止损比例
STOP_LOSS_PCT = 0.05

# 止盈比例
TAKE_PROFIT_PCT = 0.10

# 最大持仓数
MAX_POSITIONS = 3

# ============================================================
# 网络代理（用于访问 Binance/OKX 等境外交易所）
# ============================================================
HTTP_PROXY = os.getenv('HTTP_PROXY', os.getenv('http_proxy', 'http://localhost:7890'))
HTTPS_PROXY = os.getenv('HTTPS_PROXY', os.getenv('https_proxy', 'http://localhost:7890'))
