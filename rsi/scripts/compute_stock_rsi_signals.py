"""个股 RSI 信号事件回填

从 stock_rsi_{freq}_qfq 表读取已计算的 RSI 数据，
检测所有信号(极端值/背离/失败摆动/中轴穿越)，
计算 T+5/10/20/60 收益率，逐条写入 stock_rsi_signal 表。

核心信号（个股级有效）:
- divergence: rsi14_bull_divergence(买71.4%), rsi14_bear_divergence(卖73.7%) — 跨层级最稳定
- extreme: rsi14_strong_oversold(买66.7%), rsi14_oversold(买56.8%)

用法:
  python rsi/research/compute_stock_rsi_signals.py                     # 全市场
  python rsi/research/compute_stock_rsi_signals.py --codes "300750.SZ" # 指定股票
  python rsi/research/compute_stock_rsi_signals.py --freq weekly       # 指定周期
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
from database import RSIBase, write_engine
from signal_detector_rsi import detect_all_signals
from models import SIGNAL_MAP
from db_utils import batch_upsert

log = get_logger("research.compute_stock_rsi_signals")

StockRsiSignal = SIGNAL_MAP["stock"]

FREQS = ("daily", "weekly", "monthly")
SIGNAL_UNIQUE_KEYS = ["ts_code", "trade_date", "freq", "signal_name"]


# ── 数据加载 ──────────────────────────────────────────────────

def load_stock_rsi(ts_code: str, freq: str, conn) -> "pd.DataFrame":
    """从 stock_rsi 库读取已计算的个股 RSI 数据（qfq）"""
    import pandas as pd
    table = f"stock_rsi_{freq}_qfq"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, "
        f"rsi_6, rsi_12, rsi_14, rsi_24 "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


def get_all_stock_codes() -> list[str]:
    """从 stock_rsi_daily_qfq 获取所有股票代码"""
    sql = text("SELECT DISTINCT ts_code FROM stock_rsi_daily_qfq ORDER BY ts_code")
    with write_engine.connect() as conn:
        result = conn.execute(sql).fetchall()
    return [r[0] for r in result]


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

def process_one(ts_code: str, freq: str, conn) -> list[dict]:
    """检测单只股票单个周期的所有 RSI 信号，返回信号记录列表"""
    df = load_stock_rsi(ts_code, freq, conn)
    if df.empty or len(df) < 30:
        return []

    close_arr = df["close"].values
    records = []

    # 检测所有 RSI 信号（4类18种，完整版含 trade_date/signal_value/rsi_values）
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
            "rsi_values": sig.get("rsi_values"),
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

    with write_engine.connect() as conn:
        for i, ts_code in enumerate(ts_codes, 1):
            for freq in freqs:
                try:
                    records = process_one(ts_code, freq, conn)
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
                    batch_upsert(StockRsiSignal, all_records, SIGNAL_UNIQUE_KEYS)
                    all_records = []

    # 剩余写入
    if all_records:
        batch_upsert(StockRsiSignal, all_records, SIGNAL_UNIQUE_KEYS)

    elapsed = round(time.time() - start, 1)
    log.info(f"[compute] 完成 | 信号总数: {signal_count} | 耗时: {elapsed}s")


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股 RSI 信号事件回填")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔")
    parser.add_argument("--freq", default=None, help="周期，逗号分隔 (默认: daily,weekly,monthly)")
    args = parser.parse_args()

    # 建表
    from database import init_rsi_tables
    init_rsi_tables()

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    else:
        codes = get_all_stock_codes()
        log.info(f"加载全市场股票: {len(codes)} 只")

    freqs = tuple(f.strip() for f in args.freq.split(",")) if args.freq else FREQS
    process_all(codes, freqs=freqs)
