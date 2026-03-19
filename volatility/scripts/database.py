"""双引擎: read_engine(my_stock) + write_engine(stock_volatility)"""
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase

from config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD,
    READ_DB_URI, WRITE_DB_URI, WRITE_DB_NAME,
)


def _ensure_volatility_db():
    """创建 stock_volatility 数据库（如不存在）"""
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{WRITE_DB_NAME}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
            )
        conn.commit()
    finally:
        conn.close()


_ensure_volatility_db()

# 读引擎: my_stock
read_engine = create_engine(
    READ_DB_URI,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=10,
    max_overflow=20,
    echo=False,
)

# 写引擎: stock_volatility
write_engine = create_engine(
    WRITE_DB_URI,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=10,
    max_overflow=20,
    echo=False,
    isolation_level="READ COMMITTED",
)


class VolatilityBase(DeclarativeBase):
    pass


def init_volatility_tables():
    """创建所有波动率研究表"""
    import models  # noqa: F401
    VolatilityBase.metadata.create_all(bind=write_engine)
