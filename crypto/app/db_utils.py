"""
加密货币量化交易系统 - 数据库工具

批量 upsert、NaN 清洗、死锁重试。
"""
import time
import math

from sqlalchemy.dialects.mysql import insert
from sqlalchemy import text

from app.database import engine
from app.logger import get_logger

logger = get_logger("crypto.db_utils")

BATCH_SIZE = 1000
DEADLOCK_RETRIES = 3


def _clean_nan(records: list[dict]):
    """将 float NaN 替换为 None（MySQL 不支持 NaN）"""
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, float) and math.isnan(v):
                rec[k] = None


def batch_upsert(model_class, records: list[dict], unique_keys: list[str]):
    """
    批量 upsert（INSERT ... ON DUPLICATE KEY UPDATE）

    Args:
        model_class: SQLAlchemy 模型类
        records: 数据字典列表
        unique_keys: 唯一键字段列表（用于判断哪些列需要更新）
    """
    if not records:
        return

    _clean_nan(records)

    # 需要更新的列 = 所有列 - 唯一键列 - 主键
    update_cols = [
        c for c in records[0].keys()
        if c not in unique_keys and c != "id"
    ]

    if len(records) <= BATCH_SIZE:
        _do_upsert(model_class, records, update_cols)
    else:
        chunks = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
        for chunk in chunks:
            _do_upsert(model_class, chunk, update_cols)

    logger.info(f"batch_upsert: {model_class.__tablename__}, {len(records)} 条")


def _do_upsert(model_class, records: list[dict], update_cols: list[str]):
    """执行单批 upsert，含死锁重试"""
    stmt = insert(model_class).values(records)
    if update_cols:
        stmt = stmt.on_duplicate_key_update(
            **{c: stmt.inserted[c] for c in update_cols}
        )

    for attempt in range(DEADLOCK_RETRIES):
        try:
            with engine.connect() as conn:
                conn.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                conn.execute(stmt)
                conn.commit()
            return
        except Exception as e:
            if "Deadlock" in str(e) and attempt < DEADLOCK_RETRIES - 1:
                time.sleep(0.5 * (attempt + 1))
                logger.warning(f"死锁重试 {attempt + 1}: {e}")
            else:
                raise
