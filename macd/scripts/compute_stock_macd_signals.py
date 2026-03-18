"""个股 MACD 信号事件回填

从 stock_macd_{freq}_qfq 表读取已计算的 MACD 数据，
检测所有信号(交叉/零轴穿越/背离/DIF极值)，
计算 T+5/10/20/60 收益率，逐条写入 stock_macd_signal 表。

信号类型:
- cross: golden_cross, zero_golden_cross, death_cross, zero_death_cross
- zero_cross: dif_cross_zero_up, dif_cross_zero_down
- divergence: top_divergence, bottom_divergence
- dif_extreme: dif_peak(卖), dif_trough(买) — 最强信号

用法:
  python research/macd/scripts/compute_stock_macd_signals.py                     # 全市场
  python research/macd/scripts/compute_stock_macd_signals.py --codes "300750.SZ" # 指定股票
  python research/macd/scripts/compute_stock_macd_signals.py --freq weekly       # 指定周期
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.logger import get_logger
from database import ResearchBase, write_engine
from signal_detector import (
    detect_all_signals, FREQ_ORDER,
    _find_local_peaks, _find_local_troughs,
)
from models import SIGNAL_MAP
from db_utils import batch_upsert

log = get_logger("research.compute_stock_macd_signals")

StockMacdSignal = SIGNAL_MAP["stock"]

FREQS = ("daily", "weekly", "monthly")
SIGNAL_UNIQUE_KEYS = ["ts_code", "trade_date", "freq", "signal_name"]

# signal_name → (signal_type, direction) 映射
SIGNAL_META = {
    "golden_cross":       ("cross", "buy"),
    "zero_golden_cross":  ("cross", "buy"),
    "death_cross":        ("cross", "sell"),
    "zero_death_cross":   ("cross", "sell"),
    "dif_cross_zero_up":  ("zero_cross", "buy"),
    "dif_cross_zero_down": ("zero_cross", "sell"),
    "top_divergence":     ("divergence", "sell"),
    "bottom_divergence":  ("divergence", "buy"),
}


# ── 数据加载 ──────────────────────────────────────────────────

def load_stock_macd(ts_code: str, freq: str, conn) -> pd.DataFrame:
    """从 stock_research 读取已计算的个股 MACD 数据（qfq）"""
    table = f"stock_macd_{freq}_qfq"
    sql = text(
        f"SELECT trade_date, close, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


def get_all_stock_codes() -> list[str]:
    """从 stock_research 的已有数据中获取股票列表"""
    sql = text("SELECT DISTINCT ts_code FROM stock_macd_daily_qfq ORDER BY ts_code")
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
    """检测单只股票单个周期的所有 MACD 信号，返回信号记录列表"""
    df = load_stock_macd(ts_code, freq, conn)
    if df.empty or len(df) < 30:
        return []

    close_arr = df["close"].values
    dif_arr = df["dif"].values
    dea_arr = df["dea"].values
    dates = df["trade_date"].values
    order = FREQ_ORDER.get(freq, 20)

    records = []

    # 1. 检测标准信号（交叉/零轴穿越/背离）
    signals_df = detect_all_signals(df, freq=freq)
    if not signals_df.empty:
        for _, sig in signals_df.iterrows():
            sig_name = sig["signal"]
            if sig_name not in SIGNAL_META:
                continue
            sig_type, direction = SIGNAL_META[sig_name]
            idx = int(sig["idx"])
            record = {
                "ts_code": ts_code,
                "trade_date": sig["trade_date"],
                "freq": freq,
                "signal_type": sig_type,
                "signal_name": sig_name,
                "direction": direction,
                "signal_value": float(sig["dif"]),
                "close": float(sig["close"]),
                "dif": float(sig["dif"]),
                "dea": float(sig["dea"]),
                **_calc_returns(close_arr, idx),
            }
            records.append(record)

    # 2. 检测 DIF 局部极值（最强信号）
    peaks = _find_local_peaks(dif_arr, order)
    for idx in peaks:
        if np.isnan(dif_arr[idx]):
            continue
        record = {
            "ts_code": ts_code,
            "trade_date": str(dates[idx]),
            "freq": freq,
            "signal_type": "dif_extreme",
            "signal_name": "dif_peak",
            "direction": "sell",
            "signal_value": float(dif_arr[idx]),
            "close": float(close_arr[idx]),
            "dif": float(dif_arr[idx]),
            "dea": float(dea_arr[idx]),
            **_calc_returns(close_arr, idx),
        }
        records.append(record)

    troughs = _find_local_troughs(dif_arr, order)
    for idx in troughs:
        if np.isnan(dif_arr[idx]):
            continue
        record = {
            "ts_code": ts_code,
            "trade_date": str(dates[idx]),
            "freq": freq,
            "signal_type": "dif_extreme",
            "signal_name": "dif_trough",
            "direction": "buy",
            "signal_value": float(dif_arr[idx]),
            "close": float(close_arr[idx]),
            "dif": float(dif_arr[idx]),
            "dea": float(dea_arr[idx]),
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

            # 每 100 只写入一次，控制内存
            if i % 100 == 0:
                elapsed = round(time.time() - start, 1)
                log.info(f"[compute] 进度: {i}/{total} | 信号: {signal_count} | 耗时: {elapsed}s")
                if all_records:
                    batch_upsert(StockMacdSignal, all_records, SIGNAL_UNIQUE_KEYS)
                    all_records = []

    # 剩余写入
    if all_records:
        batch_upsert(StockMacdSignal, all_records, SIGNAL_UNIQUE_KEYS)

    elapsed = round(time.time() - start, 1)
    log.info(f"[compute] 完成 | 信号总数: {signal_count} | 耗时: {elapsed}s")


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股 MACD 信号事件回填")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔")
    parser.add_argument("--freq", default=None, help="周期，逗号分隔 (默认: daily,weekly,monthly)")
    args = parser.parse_args()

    # 建表
    ResearchBase.metadata.create_all(bind=write_engine)

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    else:
        codes = get_all_stock_codes()
        log.info(f"加载全市场股票: {len(codes)} 只")

    freqs = tuple(f.strip() for f in args.freq.split(",")) if args.freq else FREQS
    process_all(codes, freqs=freqs)
