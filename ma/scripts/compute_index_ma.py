"""指数/申万行业 MA 均线全市场计算入口

读取 my_stock 库 K线数据，计算 7 条 SMA + 4 个乖离率，写入 stock_ma 库。
参照 research/macd/scripts/compute_index_macd.py 模式编写。
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

log = get_logger("research.compute_index_ma")

FREQS = ("daily", "weekly", "monthly", "yearly")
SW_FREQS = ("daily", "weekly", "monthly")

# 写入字段: K线行情 + 7条均线 + 4个乖离率
_COLS = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "pct_chg",
         "ma5", "ma10", "ma20", "ma30", "ma60", "ma90", "ma250",
         "bias5", "bias10", "bias20", "bias60"]


def calc_ma(df):
    """计算 7 条均线 + 4 个乖离率

    均线: SMA 5/10/20/30/60/90/250
    乖离率: BIAS = (close - MA) / MA × 100，选取 5/10/20/60 四条
    """
    close = df["close"]

    # 7条均线
    for period in [5, 10, 20, 30, 60, 90, 250]:
        df[f"ma{period}"] = close.rolling(window=period, min_periods=period).mean().round(2)

    # 4个乖离率
    for period in [5, 10, 20, 60]:
        ma_col = f"ma{period}"
        df[f"bias{period}"] = ((close - df[ma_col]) / df[ma_col] * 100).round(2)

    return df


def compute_index_ma(freq: str = "daily", ts_codes: list[str] | None = None):
    """
    计算指定周期的指数 MA 均线

    流程:
    1. 获取全部指数代码（或指定列表）
    2. 逐只指数: 加载K线 → 计算MA → batch_upsert写入
    3. 每100只打印进度，结束打印总耗时
    """
    model = MODEL_MAP.get(("index", freq))
    if not model:
        log.error(f"[index_ma] 找不到模型: index/{freq}")
        return

    codes = ts_codes or get_all_index_codes()
    total = len(codes)
    log.info(f"[index_ma] 开始计算 | freq={freq} | 指数数: {total}")

    start = time.time()
    processed = 0
    total_rows = 0

    # 全循环复用同一读连接
    with read_engine.connect() as conn:
        for i, ts_code in enumerate(codes, 1):
            try:
                df = load_index_kline(ts_code, freq, conn=conn)
                if df.empty or len(df) < 2:
                    continue

                # 计算均线和乖离率
                df = df.reset_index(drop=True)
                df = calc_ma(df)

                # 写入
                records = df[_COLS].to_dict(orient="records")
                batch_upsert(model, records, unique_keys=["ts_code", "trade_date"])
                processed += 1
                total_rows += len(records)
            except Exception as e:
                log.error(f"[index_ma] 计算失败 | ts_code={ts_code} | {e}")

            if i % 100 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[index_ma] 进度: {i}/{total} | 已处理: {processed} | "
                         f"数据量: {total_rows} | 耗时: {elapsed}s")

    elapsed = round(time.time() - start, 1)
    log.info(f"[index_ma] 完成 | freq={freq} | "
             f"处理: {processed}/{total} | 数据量: {total_rows} | 总耗时: {elapsed}s")


def compute_sw_ma(freq: str = "daily", ts_codes: list[str] | None = None):
    """
    计算申万行业指数 MA 均线

    数据来源: my_stock.sw_daily（日线自动聚合周/月线）
    写入目标: stock_ma.sw_ma_{freq}
    """
    model = MODEL_MAP.get(("sw", freq))
    if not model:
        log.error(f"[sw_ma] 找不到模型: sw/{freq}")
        return

    codes = ts_codes or get_sw_l1_codes()
    total = len(codes)
    log.info(f"[sw_ma] 开始计算 | freq={freq} | 行业数: {total}")

    start = time.time()
    processed = 0
    total_rows = 0

    with read_engine.connect() as conn:
        for i, ts_code in enumerate(codes, 1):
            try:
                df = load_sw_kline(ts_code, freq, conn=conn)
                if df.empty or len(df) < 2:
                    continue

                # 计算均线和乖离率
                df = df.reset_index(drop=True)
                df = calc_ma(df)

                # 写入
                records = df[_COLS].to_dict(orient="records")
                batch_upsert(model, records, unique_keys=["ts_code", "trade_date"])
                processed += 1
                total_rows += len(records)
            except Exception as e:
                log.error(f"[sw_ma] 计算失败 | ts_code={ts_code} | {e}")

            if i % 10 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[sw_ma] 进度: {i}/{total} | 已处理: {processed} | "
                         f"数据量: {total_rows} | 耗时: {elapsed}s")

    elapsed = round(time.time() - start, 1)
    log.info(f"[sw_ma] 完成 | freq={freq} | "
             f"处理: {processed}/{total} | 数据量: {total_rows} | 总耗时: {elapsed}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="计算指数/申万行业 MA 均线指标")
    parser.add_argument("--freq", default="daily", choices=FREQS, help="K线周期")
    parser.add_argument("--codes", default=None, help="代码列表，逗号分隔")
    parser.add_argument("--source", default="index", choices=["index", "sw"],
                        help="数据来源: index(指数) / sw(申万行业)")
    parser.add_argument("--all-freqs", action="store_true",
                        help="一次性计算所有周期（忽略 --freq）")
    args = parser.parse_args()

    # 初始化表
    from database import init_ma_tables
    init_ma_tables()

    codes = args.codes.split(",") if args.codes else None

    if args.source == "sw":
        freqs_to_run = SW_FREQS if args.all_freqs else (args.freq,)
        for freq in freqs_to_run:
            if freq not in SW_FREQS:
                print(f"[ERROR] 申万行业仅支持 {SW_FREQS}，不支持 {freq}")
                sys.exit(1)
            compute_sw_ma(freq=freq, ts_codes=codes)
    else:
        freqs_to_run = FREQS if args.all_freqs else (args.freq,)
        for freq in freqs_to_run:
            compute_index_ma(freq=freq, ts_codes=codes)
