"""
加密货币量化交易系统 - 配置模块

所有配置项通过环境变量管理，支持 .env 文件加载。
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# 项目根目录（crypto/）
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


class Config:
    """系统配置"""

    # ========== MySQL 数据库 ==========
    MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "stock_crypto")

    SQLALCHEMY_DATABASE_URI = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
    )

    # ========== 交易所 API ==========
    # Binance
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

    # OKX
    OKX_API_KEY = os.getenv("OKX_API_KEY", "")
    OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
    OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")

    # 默认交易所
    DEFAULT_EXCHANGE = os.getenv("CRYPTO_EXCHANGE", "binance")

    # ========== 数据同步 ==========
    # 默认交易对列表（逗号分隔）
    DEFAULT_SYMBOLS = os.getenv(
        "CRYPTO_SYMBOLS",
        "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT,XRP/USDT"
    )

    # 默认同步的K线周期列表
    DEFAULT_TIMEFRAMES = os.getenv(
        "CRYPTO_TIMEFRAMES",
        "1h,4h,1d"
    )

    # 每次获取K线数量上限
    KLINE_FETCH_LIMIT = int(os.getenv("KLINE_FETCH_LIMIT", "1000"))

    # 全量同步：获取多少天的历史数据（加密货币数据量小，默认全量）
    FULL_SYNC_DAYS = int(os.getenv("FULL_SYNC_DAYS", "3650"))

    # ========== 策略默认参数 ==========
    MA_FAST = int(os.getenv("MA_FAST", "7"))
    MA_SLOW = int(os.getenv("MA_SLOW", "25"))

    RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
    RSI_OVERSOLD = int(os.getenv("RSI_OVERSOLD", "30"))
    RSI_OVERBOUGHT = int(os.getenv("RSI_OVERBOUGHT", "70"))

    MACD_FAST = int(os.getenv("MACD_FAST", "12"))
    MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
    MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))

    # ========== 风控参数 ==========
    # 单笔最大仓位比例
    MAX_POSITION_RATIO = float(os.getenv("MAX_POSITION_RATIO", "0.3"))
    # 止损比例
    STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.05"))
    # 止盈比例
    TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.10"))
    # 最大持仓数
    MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "3"))

    # ========== 网络代理 ==========
    HTTP_PROXY = os.getenv("HTTP_PROXY", os.getenv("http_proxy", ""))
    HTTPS_PROXY = os.getenv("HTTPS_PROXY", os.getenv("https_proxy", ""))

    # ========== 服务端口 ==========
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8001"))

    # ========== 日志 ==========
    LOG_DIR = str(BASE_DIR / "logs")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
