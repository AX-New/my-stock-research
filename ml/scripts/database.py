"""
ML 分析数据库引擎

提供 my_stock 和 my_trend 两个只读引擎。
my_trend 通过本地 SSH 隧道访问（port 3310 → 腾讯云 3306）。
"""
from sqlalchemy import create_engine
from ml.scripts.config import MY_STOCK_DB_URI, MY_TREND_DB_URI


def get_stock_engine():
    """获取 my_stock 引擎（只读），port 3307"""
    return create_engine(
        MY_STOCK_DB_URI,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def get_trend_engine():
    """获取 my_trend 引擎（只读），port 3310（SSH 隧道）"""
    return create_engine(
        MY_TREND_DB_URI,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
