"""指数 MACD 全市场计算入口"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.logger import get_logger
from kline_loader import load_index_kline, get_all_index_codes
from macd_calc import calc_macd
from models import MODEL_MAP
from db_utils import batch_upsert

log = get_logger("research.compute_index_macd")

FREQS = ("daily", "weekly", "monthly", "yearly")

# 写入字段
_COLS = ["ts_code", "trade_date", "open", "high", "low", "close",
         "vol", "pct_chg", "dif", "dea", "macd"]


def compute_index_macd(freq: str = "daily", ts_codes: list[str] | None = None):
    """
    计算指定周期的指数 MACD（无复权维度）

    流程:
    1. 获取全部指数代码（或指定列表）
    2. 逐只指数: 加载K线 → 计算MACD → batch_upsert写入
    3. 每100只打印进度，结束打印总耗时
    """
    model = MODEL_MAP.get(("index", freq, "none"))
    if not model:
        log.error(f"[index_macd] 找不到模型: index/{freq}")
        return

    codes = ts_codes or get_all_index_codes()
    total = len(codes)
    log.info(f"[index_macd] 开始计算 | freq={freq} | 指数数: {total}")

    start = time.time()
    processed = 0
    total_rows = 0

    for i, ts_code in enumerate(codes, 1):
        try:
            df = load_index_kline(ts_code, freq)
            if df.empty or len(df) < 2:
                continue

            # 计算 MACD
            macd_df = calc_macd(df["close"])
            df = df.reset_index(drop=True)
            df["dif"] = macd_df["dif"].values
            df["dea"] = macd_df["dea"].values
            df["macd"] = macd_df["macd"].values

            # 写入
            records = df[_COLS].to_dict(orient="records")
            batch_upsert(model, records, unique_keys=["ts_code", "trade_date"])
            processed += 1
            total_rows += len(records)
        except Exception as e:
            log.error(f"[index_macd] 计算失败 | ts_code={ts_code} | {e}")

        if i % 100 == 0:
            elapsed = round(time.time() - start, 1)
            log.info(f"[index_macd] 进度: {i}/{total} | 已处理: {processed} | "
                     f"数据量: {total_rows} | 耗时: {elapsed}s")

    elapsed = round(time.time() - start, 1)
    log.info(f"[index_macd] 完成 | freq={freq} | "
             f"处理: {processed}/{total} | 数据量: {total_rows} | 总耗时: {elapsed}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="计算指数 MACD 指标")
    parser.add_argument("--freq", default="daily", choices=FREQS, help="K线周期")
    parser.add_argument("--codes", default=None, help="指数代码，逗号分隔 (如: 000001.SH,399001.SZ)")
    args = parser.parse_args()

    # 初始化表
    from database import init_research_tables
    init_research_tables()

    codes = args.codes.split(",") if args.codes else None
    compute_index_macd(freq=args.freq, ts_codes=codes)
