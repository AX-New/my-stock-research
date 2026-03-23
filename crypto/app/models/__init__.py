"""模型注册：导入所有模型以触发 SQLAlchemy 元数据注册"""
from app.models.symbol import CryptoSymbol  # noqa: F401
from app.models.kline import CryptoKline  # noqa: F401
from app.models.signal import CryptoSignal  # noqa: F401
from app.models.trade import CryptoTrade  # noqa: F401
from app.models.position import CryptoPosition  # noqa: F401
