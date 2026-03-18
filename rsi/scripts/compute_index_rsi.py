"""指数/申万行业 RSI 全市场计算入口

读取 my_stock 库 K线数据，计算 4 个周期的 RSI（6/12/14/24），写入 stock_rsi 库。
参照 ma/research/compute_index_ma.py 模式编写。
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.logger import get_logger
from database import read_engine
from kline_loader import load_index_kline, get_all_index_codes, load_sw_kline, get_sw_l1_codes
from models import MODEL_MAP
from db_utils import batch_upsert
from rsi_calc import calc_all_rsi

log = get_logger("research.compute_index_rsi")

FREQS = ("daily", "weekly", "monthly", "yearly")
SW_FREQS = ("daily", "weekly", "monthly")

# 写入字段: K线行情 + 4个RSI
_COLS = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "pct_chg",
         "rsi_6", "rsi_12", "rsi_14", "rsi_24"]


def compute_index_rsi(freq: str = "daily", ts_codes: list[str] | None = None,
                      start_date: str | None = None, end_date: str | None = None):
    """
    计算指定周期的指数 RSI

    流程:
    1. 获取全部指数代码（或指定列表）
    2. 逐只指数: 加载K线 → 计算RSI → batch_upsert写入
    3. 每100只打印进度，结束打印总耗时

    增量模式: 始终加载全量K线计算RSI（保证EWM准确），只写入 [start_date, end_date] 范围内的数据
    """
    model = MODEL_MAP.get(("index", freq))
    if not model:
        log.error(f"[index_rsi] 找不到模型: index/{freq}")
        return

    codes = ts_codes or get_all_index_codes()
    total = len(codes)
    date_info = ""
    if start_date:
        date_info += f" | start_date={start_date}"
    if end_date:
        date_info += f" | end_date={end_date}"
    log.info(f"[index_rsi] 开始计算 | freq={freq} | 指数数: {total}{date_info}")

    start = time.time()
    processed = 0
    total_rows = 0

    with read_engine.connect() as conn:
        for i, ts_code in enumerate(codes, 1):
            try:
                df = load_index_kline(ts_code, freq, conn=conn)
                if df.empty or len(df) < 2:
                    continue

                # 计算 4 个周期的 RSI
                df = df.reset_index(drop=True)
                df = calc_all_rsi(df)

                # 增量过滤: 只写入指定日期范围的数据
                write_df = df
                if start_date:
                    write_df = write_df[write_df["trade_date"] >= start_date]
                if end_date:
                    write_df = write_df[write_df["trade_date"] <= end_date]
                if write_df.empty:
                    continue

                records = write_df[_COLS].to_dict(orient="records")
                batch_upsert(model, records, unique_keys=["ts_code", "trade_date"])
                processed += 1
                total_rows += len(records)
            except Exception as e:
                log.error(f"[index_rsi] 计算失败 | ts_code={ts_code} | {e}")

            if i % 100 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[index_rsi] 进度: {i}/{total} | 已处理: {processed} | "
                         f"数据量: {total_rows} | 耗时: {elapsed}s")

    elapsed = round(time.time() - start, 1)
    log.info(f"[index_rsi] 完成 | freq={freq} | "
             f"处理: {processed}/{total} | 数据量: {total_rows} | 总耗时: {elapsed}s")


def compute_sw_rsi(freq: str = "daily", ts_codes: list[str] | None = None,
                   start_date: str | None = None, end_date: str | None = None):
    """
    计算申万行业指数 RSI

    数据来源: my_stock.sw_daily（日线自动聚合周/月线）
    写入目标: stock_rsi.sw_rsi_{freq}
    增量模式: 始终加载全量K线计算RSI，只写入 [start_date, end_date] 范围内的数据
    """
    model = MODEL_MAP.get(("sw", freq))
    if not model:
        log.error(f"[sw_rsi] 找不到模型: sw/{freq}")
        return

    codes = ts_codes or get_sw_l1_codes()
    total = len(codes)
    date_info = ""
    if start_date:
        date_info += f" | start_date={start_date}"
    if end_date:
        date_info += f" | end_date={end_date}"
    log.info(f"[sw_rsi] 开始计算 | freq={freq} | 行业数: {total}{date_info}")

    start = time.time()
    processed = 0
    total_rows = 0

    with read_engine.connect() as conn:
        for i, ts_code in enumerate(codes, 1):
            try:
                df = load_sw_kline(ts_code, freq, conn=conn)
                if df.empty or len(df) < 2:
                    continue

                df = df.reset_index(drop=True)
                df = calc_all_rsi(df)

                # 增量过滤
                write_df = df
                if start_date:
                    write_df = write_df[write_df["trade_date"] >= start_date]
                if end_date:
                    write_df = write_df[write_df["trade_date"] <= end_date]
                if write_df.empty:
                    continue

                records = write_df[_COLS].to_dict(orient="records")
                batch_upsert(model, records, unique_keys=["ts_code", "trade_date"])
                processed += 1
                total_rows += len(records)
            except Exception as e:
                log.error(f"[sw_rsi] 计算失败 | ts_code={ts_code} | {e}")

            if i % 10 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[sw_rsi] 进度: {i}/{total} | 已处理: {processed} | "
                         f"数据量: {total_rows} | 耗时: {elapsed}s")

    elapsed = round(time.time() - start, 1)
    log.info(f"[sw_rsi] 完成 | freq={freq} | "
             f"处理: {processed}/{total} | 数据量: {total_rows} | 总耗时: {elapsed}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="计算指数/申万行业 RSI 指标")
    parser.add_argument("--freq", default="daily", choices=FREQS, help="K线周期")
    parser.add_argument("--codes", default=None, help="代码列表，逗号分隔")
    parser.add_argument("--source", default="index", choices=["index", "sw"],
                        help="数据来源: index(指数) / sw(申万行业)")
    parser.add_argument("--all-freqs", action="store_true",
                        help="一次性计算所有周期（忽略 --freq）")
    parser.add_argument("--start-date", default=None, help="增量起始日期 YYYYMMDD")
    parser.add_argument("--end-date", default=None, help="截止日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()

    # 初始化表
    from database import init_rsi_tables
    init_rsi_tables()

    codes = args.codes.split(",") if args.codes else None

    if args.source == "sw":
        freqs_to_run = SW_FREQS if args.all_freqs else (args.freq,)
        for freq in freqs_to_run:
            if freq not in SW_FREQS:
                print(f"[ERROR] 申万行业仅支持 {SW_FREQS}，不支持 {freq}")
                sys.exit(1)
            compute_sw_rsi(freq=freq, ts_codes=codes,
                           start_date=args.start_date, end_date=args.end_date)
    else:
        freqs_to_run = FREQS if args.all_freqs else (args.freq,)
        for freq in freqs_to_run:
            compute_index_rsi(freq=freq, ts_codes=codes,
                              start_date=args.start_date, end_date=args.end_date)
