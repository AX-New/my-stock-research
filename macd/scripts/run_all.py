"""一键执行全部 MACD 计算

执行顺序:
1. 创建 stock_research 数据库和所有表
2. 股票 MACD（12组: 4周期 × 3复权）
3. 指数 MACD（4组: 4周期）
"""
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.logger import get_logger
from database import init_research_tables

log = get_logger("research.run_all")


def main():
    start = time.time()

    # 1. 创建数据库和表
    log.info("[run_all] 初始化 stock_research 数据库和表...")
    init_research_tables()
    log.info("[run_all] 表创建完成（16张 MACD 表）")

    from compute_stock_macd import compute_stock_macd, FREQS, ADJS
    from compute_index_macd import compute_index_macd

    # 2. 股票 MACD — 12组
    for freq in FREQS:
        for adj in ADJS:
            log.info(f"[run_all] === 股票 MACD: freq={freq} adj={adj} ===")
            compute_stock_macd(freq=freq, adj=adj)

    # 3. 指数 MACD — 4组
    for freq in FREQS:
        log.info(f"[run_all] === 指数 MACD: freq={freq} ===")
        compute_index_macd(freq=freq)

    elapsed = round(time.time() - start, 1)
    log.info(f"[run_all] 全部完成 | 总耗时: {elapsed}s")


if __name__ == "__main__":
    main()
