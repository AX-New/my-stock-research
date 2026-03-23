"""
加密货币量化交易 - 数据库模块

写入库：stock_crypto
"""
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD,
    WRITE_DB_NAME, WRITE_DB_URI,
)


def _ensure_db():
    """创建 stock_crypto 数据库（如不存在）"""
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


_ensure_db()

# 写引擎
engine = create_engine(
    WRITE_DB_URI,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=10,
    max_overflow=20,
    echo=False,
)

Session = sessionmaker(bind=engine)


class CryptoBase(DeclarativeBase):
    pass


def init_tables():
    """创建所有加密货币相关表"""
    import models  # noqa: F401 — 触发模型注册
    CryptoBase.metadata.create_all(bind=engine)
