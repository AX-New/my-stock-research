"""个股 RSI 全市场计算入口 (Phase 1)

读取 my_stock 库 K线数据（含复权），计算 4 个周期的 RSI（6/12/14/24），
写入 stock_rsi 库的 12 张 stock_rsi_{freq}_{adj} 表。

用法:
  # 全量（4频率×3复权=12组合，每组合5473只）
  python rsi/research/compute_stock_rsi.py --all-freqs --all-adjs

  # 增量（只写入指定日期之后的数据）
  python rsi/research/compute_stock_rsi.py --all-freqs --all-adjs --start-date 20260315

  # 指定股票
  python rsi/research/compute_stock_rsi.py --codes "300750.SZ,600519.SH" --freq daily --adj qfq
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.logger import get_logger
from kline_loader import load_stock_kline, get_all_stock_codes
from models import MODEL_MAP
from db_utils import batch_upsert
from rsi_calc import calc_all_rsi

log = get_logger("research.compute_stock_rsi")

FREQS = ("daily", "weekly", "monthly", "yearly")
ADJS = ("bfq", "qfq", "hfq")

# 写入字段: K线行情 + 4个RSI
_COLS = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "pct_chg",
         "rsi_6", "rsi_12", "rsi_14", "rsi_24"]


def compute_stock_rsi(freq: str = "daily", adj: str = "bfq",
                      ts_codes: list[str] | None = None,
                      start_date: str | None = None, end_date: str | None = None):
    """
    计算指定 freq×adj 组合的全市场股票 RSI

    流程:
    1. 获取全部股票代码（或指定列表）
    2. 逐只股票: 加载K线 → 计算RSI(6,12,14,24) → batch_upsert写入
    3. 每100只打印进度，结束打印总耗时

    增量模式: 始终加载全量K线计算RSI（保证EWM准确），只写入 [start_date, end_date] 范围内的数据
    """
    model = MODEL_MAP.get(("stock", freq, adj))
    if not model:
        log.error(f"[stock_rsi] 找不到模型: stock/{freq}/{adj}")
        return

    codes = ts_codes or get_all_stock_codes()
    total = len(codes)
    date_info = ""
    if start_date:
        date_info += f" | start_date={start_date}"
    if end_date:
        date_info += f" | end_date={end_date}"
    log.info(f"[stock_rsi] 开始计算 | freq={freq} adj={adj} | 股票数: {total}{date_info}")

    start = time.time()
    processed = 0
    total_rows = 0

    for i, ts_code in enumerate(codes, 1):
        try:
            # load_stock_kline 内部管理连接，含复权计算
            df = load_stock_kline(ts_code, freq, adj)
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
            log.error(f"[stock_rsi] 计算失败 | ts_code={ts_code} | {e}")

        if i % 100 == 0:
            elapsed = round(time.time() - start, 1)
            log.info(f"[stock_rsi] 进度: {i}/{total} | 已处理: {processed} | "
                     f"数据量: {total_rows} | 耗时: {elapsed}s")

    elapsed = round(time.time() - start, 1)
    log.info(f"[stock_rsi] 完成 | freq={freq} adj={adj} | "
             f"处理: {processed}/{total} | 数据量: {total_rows} | 总耗时: {elapsed}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="计算个股 RSI 指标（含复权）")
    parser.add_argument("--freq", default="daily", choices=FREQS, help="K线周期")
    parser.add_argument("--adj", default="bfq", choices=ADJS,
                        help="复权类型: bfq(不复权)/qfq(前复权)/hfq(后复权)")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔 (如: 000001.SZ,600519.SH)")
    parser.add_argument("--all-freqs", action="store_true",
                        help="一次性计算所有周期（忽略 --freq）")
    parser.add_argument("--all-adjs", action="store_true",
                        help="一次性计算所有复权类型（忽略 --adj）")
    parser.add_argument("--start-date", default=None, help="增量起始日期 YYYYMMDD")
    parser.add_argument("--end-date", default=None, help="截止日期 YYYYMMDD（默认今天）")
    args = parser.parse_args()

    # 初始化表
    from database import init_rsi_tables
    init_rsi_tables()

    codes = args.codes.split(",") if args.codes else None
    freqs_to_run = FREQS if args.all_freqs else (args.freq,)
    adjs_to_run = ADJS if args.all_adjs else (args.adj,)

    for freq in freqs_to_run:
        for adj in adjs_to_run:
            compute_stock_rsi(freq=freq, adj=adj, ts_codes=codes,
                              start_date=args.start_date, end_date=args.end_date)
