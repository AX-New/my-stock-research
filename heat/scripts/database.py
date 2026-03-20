"""
热度策略数据库引擎

提供 my_stock 和 my_trend 两个只读引擎。
my_trend 通过本地 SSH 隧道访问（port 3310 → 腾讯云 3306）。
"""
from sqlalchemy import create_engine
from heat.scripts.config import MY_STOCK_DB_URI, MY_TREND_DB_URI


def get_stock_engine():
    """
    获取 my_stock 引擎（只读）

    连接本地 MySQL，port 3307，包含 A股行情/基本面数据。
    pool_pre_ping 确保连接池中连接有效，pool_recycle 防止 MySQL 超时断连。
    """
    return create_engine(
        MY_STOCK_DB_URI,
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def get_trend_engine():
    """
    获取 my_trend 引擎（只读）

    通过本地 SSH 隧道（port 3310）访问腾讯云 MySQL，包含舆情/热度数据。
    前提：Desktop/Tssh-tunnel.bat 已启动。
    """
    return create_engine(
        MY_TREND_DB_URI,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
