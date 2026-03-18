"""个股换手率信号事件回填

从 my_stock.daily_basic 读取换手率数据，
检测所有信号(极端值/背离/区间/持续性/突变/交叉)，
计算 T+5/10/20/60 收益率，逐条写入 stock_turnover_signal 表。

核心信号（个股级有效）:
- extreme: extreme_high(卖59%) — 四指标中唯一在个股级提供有效卖出信号
- surge: 个股级退化为随机(52.5%)，但与MACD共振时有价值

用法:
  python turnover/research/compute_stock_turnover_signals.py                     # 全市场
  python turnover/research/compute_stock_turnover_signals.py --codes "300750.SZ" # 指定股票
  python turnover/research/compute_stock_turnover_signals.py --freq daily        # 指定周期
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
from database import read_engine, write_engine, TurnoverBase
from signal_detector_turnover import detect_all_signals
from models import StockTurnoverSignal, STOCK_SIGNAL_UNIQUE_KEYS
from db_utils import batch_upsert

log = get_logger("research.compute_stock_turnover_signals")

FREQS = ("daily", "weekly", "monthly")

# 信号检测滚动窗口
SIGNAL_WINDOW = {"daily": 250, "weekly": 52, "monthly": 24}


# ── 数据加载 ──────────────────────────────────────────────────

def load_stock_codes() -> list[str]:
    """从 stock_basic 获取全部上市A股代码"""
    sql = text("SELECT ts_code FROM stock_basic WHERE list_status='L' ORDER BY ts_code")
    with read_engine.connect() as conn:
        result = conn.execute(sql).fetchall()
    codes = [r[0] for r in result]
    log.info(f"加载上市A股: {len(codes)} 只")
    return codes


def load_daily_basic_batch(codes: list, conn) -> pd.DataFrame:
    """批量加载个股日线数据（100只/批, 单SQL）"""
    if not codes:
        return pd.DataFrame()

    placeholders = ",".join([f":c{i}" for i in range(len(codes))])
    sql = text(
        f"SELECT ts_code, trade_date, close, turnover_rate_f, total_mv "
        f"FROM daily_basic "
        f"WHERE ts_code IN ({placeholders}) AND trade_date >= '20131030' "
        f"ORDER BY ts_code, trade_date"
    )
    params = {f"c{i}": code for i, code in enumerate(codes)}
    return pd.read_sql(sql, conn, params=params)


# ── 聚合 ──────────────────────────────────────────────────────

def aggregate_to_freq(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """将日线聚合为周线/月线（换手率用均值法）"""
    if freq == "daily":
        return df.copy()

    df = df.copy()
    df["trade_date_dt"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    if freq == "weekly":
        df["period"] = (df["trade_date_dt"].dt.isocalendar().year.astype(str) + "W" +
                        df["trade_date_dt"].dt.isocalendar().week.astype(str).str.zfill(2))
    elif freq == "monthly":
        df["period"] = df["trade_date"].str[:6]

    agg = df.groupby("period").agg(
        trade_date=("trade_date", "last"),
        close=("close", "last"),
        turnover_rate_f=("turnover_rate_f", "mean"),
    ).reset_index(drop=True)

    return agg.sort_values("trade_date").reset_index(drop=True)


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

def process_one(ts_code: str, df_stock: pd.DataFrame, freq: str) -> list[dict]:
    """检测单只股票单个频率的所有换手率信号，返回信号记录列表"""
    # 聚合为目标频率
    df = aggregate_to_freq(df_stock, freq)

    # 数据不足则跳过
    min_rows = SIGNAL_WINDOW[freq] + 50
    if len(df) < min_rows:
        return []

    close_arr = df["close"].values
    turnover_arr = df["turnover_rate_f"].values
    dates = df["trade_date"].values
    window = SIGNAL_WINDOW[freq]

    # 信号检测（lite 模式: 返回 (idx, signal_type, signal_name, direction) 元组）
    signals = detect_all_signals(
        df, col="turnover_rate_f", price_col="close",
        extreme_window=window, zone_window=window,
        lite=True,
    )

    records = []
    for idx, sig_type, sig_name, direction in signals:
        # 获取换手率值
        tv = turnover_arr[idx]
        sig_value = float(tv) if not np.isnan(tv) else None

        record = {
            "ts_code": ts_code,
            "trade_date": str(dates[idx]),
            "freq": freq,
            "signal_type": sig_type,
            "signal_name": sig_name,
            "direction": direction,
            "signal_value": sig_value,
            "close": float(close_arr[idx]) if not np.isnan(close_arr[idx]) else None,
            **_calc_returns(close_arr, idx),
        }
        records.append(record)

    return records


# ── 批量处理 ──────────────────────────────────────────────────

def process_all(ts_codes: list[str], freqs: tuple = FREQS, batch_size: int = 100):
    """批量处理所有股票，写入信号表"""
    total = len(ts_codes)
    log.info(f"[compute] 开始 | 股票: {total} | 周期: {len(freqs)} | 批次: {batch_size}")

    start = time.time()
    all_records = []
    signal_count = 0
    skipped = 0

    batches = [ts_codes[i:i + batch_size] for i in range(0, total, batch_size)]

    with read_engine.connect() as read_conn:
        for batch_idx, batch_codes in enumerate(batches):
            # 批量加载数据
            df_batch = load_daily_basic_batch(batch_codes, read_conn)
            if df_batch.empty:
                skipped += len(batch_codes)
                continue

            # 按股票分组
            grouped = {code: group.copy() for code, group in df_batch.groupby("ts_code")}

            for ts_code in batch_codes:
                df_stock = grouped.get(ts_code)
                if df_stock is None or df_stock.empty:
                    skipped += 1
                    continue

                # 去除空值
                df_stock = df_stock.dropna(subset=["turnover_rate_f", "close"])
                if len(df_stock) < 300:
                    skipped += 1
                    continue

                for freq in freqs:
                    try:
                        records = process_one(ts_code, df_stock, freq)
                        if records:
                            all_records.extend(records)
                            signal_count += len(records)
                    except Exception as e:
                        log.error(f"[compute] 失败 | {ts_code}/{freq} | {e}")

            # 每批写入
            batch_end = min((batch_idx + 1) * batch_size, total)
            elapsed = round(time.time() - start, 1)
            log.info(f"[compute] 进度: {batch_end}/{total} | 信号: {signal_count} | "
                     f"跳过: {skipped} | 耗时: {elapsed}s")
            if all_records:
                batch_upsert(StockTurnoverSignal, all_records, STOCK_SIGNAL_UNIQUE_KEYS)
                all_records = []

    # 剩余写入
    if all_records:
        batch_upsert(StockTurnoverSignal, all_records, STOCK_SIGNAL_UNIQUE_KEYS)

    elapsed = round(time.time() - start, 1)
    log.info(f"[compute] 完成 | 信号总数: {signal_count} | 跳过: {skipped} | 耗时: {elapsed}s")


# ── 主入口 ────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="个股换手率信号事件回填")
    parser.add_argument("--codes", default=None, help="股票代码，逗号分隔")
    parser.add_argument("--freq", default=None, help="周期，逗号分隔 (默认: daily,weekly,monthly)")
    parser.add_argument("--batch-size", type=int, default=100, help="批量读取大小")
    args = parser.parse_args()

    # 建表
    TurnoverBase.metadata.create_all(bind=write_engine)

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    else:
        codes = load_stock_codes()

    freqs = tuple(f.strip() for f in args.freq.split(",")) if args.freq else FREQS
    process_all(codes, freqs=freqs, batch_size=args.batch_size)
