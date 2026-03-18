"""batch_upsert — 绑定 write_engine，写入 stock_research 库"""
import math
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from sqlalchemy import text, func
from sqlalchemy.dialects.mysql import insert

from database import write_engine
from app.logger import get_logger

log = get_logger(__name__)

BATCH_SIZE = 1000
DEADLOCK_RETRIES = 3


def _do_upsert(model_class, records: list[dict], update_cols: list[str]):
    stmt = insert(model_class).values(records)
    # 确保 updated_at 在 ON DUPLICATE KEY UPDATE 中刷新
    update_dict = {c: stmt.inserted[c] for c in update_cols}
    update_dict["updated_at"] = func.now()
    stmt = stmt.on_duplicate_key_update(**update_dict)

    for attempt in range(DEADLOCK_RETRIES):
        try:
            with write_engine.connect() as conn:
                conn.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                conn.execute(stmt)
                conn.commit()
            return
        except Exception as e:
            if "Deadlock" in str(e) and attempt < DEADLOCK_RETRIES - 1:
                log.warning(f"[research_db] 死锁重试 {attempt + 1}/{DEADLOCK_RETRIES}")
                time.sleep(0.5 * (attempt + 1))
            else:
                raise


def _clean_nan(records: list[dict]):
    """将 records 中的 float NaN 替换为 None，避免 MySQL 报错"""
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and math.isnan(v):
                r[k] = None


def batch_upsert(model_class, records: list[dict], unique_keys: list[str]):
    """批量 upsert 到 stock_research 库，>1000条时分片写入"""
    if not records:
        return

    _clean_nan(records)
    update_cols = [c for c in records[0].keys() if c not in unique_keys]

    if len(records) <= BATCH_SIZE:
        _do_upsert(model_class, records, update_cols)
        return

    chunks = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    for chunk in chunks:
        _do_upsert(model_class, chunk, update_cols)
