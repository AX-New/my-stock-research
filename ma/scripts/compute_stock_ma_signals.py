"""个股 MA 均线信号事件回填

从 my_stock K线数据现场计算 MA 均线，
检测所有信号(乖离率极值/方向突破/假突破/支撑阻力/排列/粘合/交叉)，
计算 T+5/10/20/60 收益率，逐条写入 stock_ma_signal 表。

注意: stock_ma_* 数据表为空，因此从原始 K线 + 复权因子 现场计算 MA，
不依赖预计算的 MA 数据表。

核心信号（个股级有效）:
- support_resist: ma20_support(买66.1%), ma20_resist(卖67.0%)
- bias_extreme: bias20/60_extreme_low(买55.5%)

用法:
  python research/ma/scripts/compute_stock_ma_signals.py                     # 全市场
  python research/ma/scripts/compute_stock_ma_signals.py --codes "300750.SZ" # 指定股票
  python research/ma/scripts/compute_stock_ma_signals.py --freq weekly       # 指定周期
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
from sqlalchemy import text

from app.logger import get_logger
from database import MABase, read_engine, write_engine
from signal_detector_ma import detect_all_signals
from kline_loader import load_stock_kline, get_all_stock_codes
from models import SIGNAL_MAP
from db_utils import batch_upsert

log = get_logger("research.compute_stock_ma_signals")

StockMaSignal = SIGNAL_MAP["stock"]

FREQS = ("daily", "weekly", "monthly")
SIGNAL_UNIQUE_KEYS = ["ts_code", "trade_date", "freq", "signal_name"]


# ── 数据加载 ──────────────────────────────────────────────────

def _calc_ma(df):
    """计算 7 条均线 + 4 个乖离率（就地修改 df）"""
    close = df["close"]
    for period in [5, 10, 20, 30, 60, 90, 250]:
        df[f"ma{period}"] = close.rolling(window=period, min_periods=period).mean().round(2)
    for period in [5, 10, 20, 60]:
        ma_col = f"ma{period}"
        df[f"bias{period}"] = ((close - df[ma_col]) / df[ma_col] * 100).round(2)


def load_stock_ma(ts_code: str, freq: str) -> "pd.DataFrame":
    """从 my_stock 加载 K线 → 计算 7 条 MA + 4 个乖离率

    使用 qfq（前复权），与其他指标保持一致。
    """
    df = load_stock_kline(ts_code, freq=freq, adj="qfq")
    if df.empty or len(df) < 60:
        return df
    _calc_ma(df)
    return df


# ── 收益率计算 ────────────────────────────────────────────────

def _calc_returns(close_arr, idx: int) -> dict:
    """计算信号后 T+5/10/20/60 收益率(%)"""
    base = float(close_arr[idx])
    result = {}
    for h in (5, 10, 20, 60):
        target = idx + h
        if target < len(close_arr) and base > 0:
            result[f"ret_{h}"] = round((float(close_arr[target]) / base - 1) * 100, 2)
        else:
            result[f"ret_{h}"] = None
    return result


# ── 单只股票处理 ──────────────────────────────────────────────

def process_one(ts_code: str, freq: str) -> list[dict]:
    """检测单只股票单个周期的所有 MA 信号，返回信号记录列表"""
    df = load_stock_ma(ts_code, freq)
    if df.empty or len(df) < 60:
        return []

    close_arr = df["close"].values
    records = []

    # 检测所有 MA 信号（7类36种）
    signals = detect_all_signals(df, freq=freq)

    for sig in signals:
        idx = sig["idx"]
        record = {
            "ts_code": ts_code,
            "trade_date": sig["trade_date"],
            "freq": freq,
            "signal_type": sig["signal_type"],
            "signal_name": sig["signal_name"],
            "direction": sig["direction"],
            "signal_value": sig.get("signal_value"),
            "close": sig.get("close"),
            "ma_values": sig.get("ma_values"),
            **_calc_returns(close_arr, idx),
        }
        records.append(record)

    return records


# ── 批量处理 ──────────────────────────────────────────────────

def process_all(ts_codes: list[str], freqs: tuple = FREQS):
    """批量处理所有股票，写入信号表"""
    total = len(ts_codes)
    log.info(f"[compute] 开始 | 股票: {total} | 周期: {len(freqs)}")

    start = time.time()
    all_records = []
    signal_count = 0

    for i, ts_code in enumerate(ts_codes, 1):
        for freq in freqs:
            try:
                records = process_one(ts_code, freq)
                if records:
                    all_records.extend(records)
                    signal_count += len(records)
            except Exception as e:
                log.error(f"[compute] 失败 | {ts_code}/{freq} | {e}")

        # 每 100 只写入一次
        if i % 100 == 0:
            elapsed = round(time.time() - start, 1)
            log.info(f"[compute] 进度: {i}/{total} | 信号: {signal_count} | 耗时: {elapsed}s")
            if all_records:
                batch_upsert(StockMaSignal, all_records, SIGNAL_UNIQUE_KEYS)
                all_records = []

    # 剩余写入
    if all_records:
        batch_upsert(StockMaSignal, all_records, SIGNAL_UNIQUE_KEYS)

    elapsed = round(time.time() - start, 1)
    log.info(f"[compute] 完成 | 信号总数: {signal_count} | 耗时: {elapsed}s")


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股 MA 信号事件回填")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔")
    parser.add_argument("--freq", default=None, help="周期，逗号分隔 (默认: daily,weekly,monthly)")
    args = parser.parse_args()

    # 建表
    from database import init_ma_tables
    init_ma_tables()

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    else:
        codes = get_all_stock_codes()
        log.info(f"加载全市场股票: {len(codes)} 只")

    freqs = tuple(f.strip() for f in args.freq.split(",")) if args.freq else FREQS
    process_all(codes, freqs=freqs)
