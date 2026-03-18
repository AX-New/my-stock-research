"""K线数据加载 + 复权（从 my_stock 库读取）"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import pandas as pd
from sqlalchemy import text

from database import read_engine
from app.logger import get_logger

log = get_logger(__name__)

# 股票K线表映射
STOCK_FREQ_TABLE = {
    "daily": "market_daily",
    "weekly": "market_weekly",
    "monthly": "market_monthly",
}
_VALID_STOCK_TABLES = set(STOCK_FREQ_TABLE.values())

# 指数K线表映射
INDEX_FREQ_TABLE = {
    "daily": "index_daily",
    "weekly": "index_weekly",
    "monthly": "index_monthly",
}
_VALID_INDEX_TABLES = set(INDEX_FREQ_TABLE.values())

# 需要复权调整的价格列
PRICE_COLS = ["open", "high", "low", "close"]


# ── 股票K线 ──────────────────────────────────────────────────

def _load_stock_kline_raw(conn, table: str, ts_code: str) -> pd.DataFrame:
    """从 my_stock 读取股票原始行情（全量，按日期升序）"""
    if table not in _VALID_STOCK_TABLES:
        raise ValueError(f"非法的表名: {table}")
    sql = text(
        f"SELECT ts_code, trade_date, open, high, low, close, pct_chg, vol "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


def _load_adj_factor(conn, ts_code: str) -> pd.DataFrame:
    """读取复权因子（全量）"""
    sql = text(
        "SELECT trade_date, adj_factor FROM adj_factor "
        "WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


def _load_latest_adj_factor(conn, ts_code: str) -> float | None:
    """读取最新复权因子（前复权用）"""
    sql = text(
        "SELECT adj_factor FROM adj_factor "
        "WHERE ts_code = :ts_code ORDER BY trade_date DESC LIMIT 1"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    row = result.fetchone()
    return row[0] if row else None


def _apply_adjustment(df: pd.DataFrame, df_adj: pd.DataFrame,
                      adj_type: str, latest_factor: float | None) -> pd.DataFrame:
    """对行情数据做复权计算（复用 kline_adjust_service 逻辑）"""
    if df_adj.empty:
        return df

    df = df.merge(df_adj, on="trade_date", how="left")
    mask = df["adj_factor"].notna()

    if adj_type == "hfq":
        for col in PRICE_COLS:
            if col in df.columns:
                df.loc[mask, col] = df.loc[mask, col] * df.loc[mask, "adj_factor"]
    elif adj_type == "qfq" and latest_factor:
        for col in PRICE_COLS:
            if col in df.columns:
                df.loc[mask, col] = df.loc[mask, col] * (df.loc[mask, "adj_factor"] / latest_factor)

    for col in PRICE_COLS:
        if col in df.columns:
            df[col] = df[col].round(2)

    df.drop(columns=["adj_factor"], inplace=True)
    return df


def _aggregate_yearly(df: pd.DataFrame) -> pd.DataFrame:
    """从日线聚合年线（含年涨跌幅计算）"""
    if df.empty:
        return df

    df["year"] = df["trade_date"].str[:4]

    def agg_year(group):
        sorted_g = group.sort_values("trade_date")
        first_open = sorted_g["open"].iloc[0]
        last_close = sorted_g["close"].iloc[-1]
        # 年涨跌幅 = (最后收盘 - 首日开盘) / 首日开盘 × 100
        pct_chg = round((last_close - first_open) / first_open * 100, 2) if first_open else None
        return pd.Series({
            "ts_code": sorted_g["ts_code"].iloc[0],
            "trade_date": sorted_g["year"].iloc[0],
            "open": first_open,
            "high": sorted_g["high"].max(),
            "low": sorted_g["low"].min(),
            "close": last_close,
            "vol": sorted_g["vol"].sum(),
            "pct_chg": pct_chg,
        })

    result = df.groupby("year").apply(agg_year).reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        if col in result.columns:
            result[col] = result[col].round(2)
    return result


def load_stock_kline(ts_code: str, freq: str, adj: str) -> pd.DataFrame:
    """
    从 my_stock 库读取复权K线

    freq: daily / weekly / monthly / yearly
    adj:  bfq(不复权) / qfq(前复权) / hfq(后复权)

    返回: DataFrame [ts_code, trade_date, open, high, low, close, vol, pct_chg]
    """
    with read_engine.connect() as conn:
        if freq == "yearly":
            # 年线: 从日线读取，先复权再聚合
            df = _load_stock_kline_raw(conn, "market_daily", ts_code)
            if not df.empty and adj != "bfq":
                df_adj = _load_adj_factor(conn, ts_code)
                latest = _load_latest_adj_factor(conn, ts_code) if adj == "qfq" else None
                df = _apply_adjustment(df, df_adj, adj, latest)
            if not df.empty:
                df = _aggregate_yearly(df)
        else:
            table = STOCK_FREQ_TABLE.get(freq)
            if not table:
                return pd.DataFrame()
            df = _load_stock_kline_raw(conn, table, ts_code)
            if not df.empty and adj != "bfq":
                df_adj = _load_adj_factor(conn, ts_code)
                latest = _load_latest_adj_factor(conn, ts_code) if adj == "qfq" else None
                df = _apply_adjustment(df, df_adj, adj, latest)

    return df


# ── 指数K线 ──────────────────────────────────────────────────

def _load_index_kline_raw(conn, table: str, ts_code: str) -> pd.DataFrame:
    """从 my_stock 读取指数原始行情（全量，按日期升序）"""
    if table not in _VALID_INDEX_TABLES:
        raise ValueError(f"非法的表名: {table}")
    sql = text(
        f"SELECT ts_code, trade_date, open, high, low, close, pct_chg, vol "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    result = conn.execute(sql, {"ts_code": ts_code})
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=result.keys())


def load_index_kline(ts_code: str, freq: str) -> pd.DataFrame:
    """
    从 my_stock 库读取指数K线（无复权）

    freq: daily / weekly / monthly / yearly

    返回: DataFrame [ts_code, trade_date, open, high, low, close, vol, pct_chg]
    """
    with read_engine.connect() as conn:
        if freq == "yearly":
            # 年线: 从指数日线聚合
            df = _load_index_kline_raw(conn, "index_daily", ts_code)
            if not df.empty:
                df = _aggregate_yearly(df)
        else:
            table = INDEX_FREQ_TABLE.get(freq)
            if not table:
                return pd.DataFrame()
            df = _load_index_kline_raw(conn, table, ts_code)

    return df


# ── 代码列表 ──────────────────────────────────────────────────

def get_all_stock_codes() -> list[str]:
    """从 stock_basic 获取全市场股票代码（仅上市状态）"""
    with read_engine.connect() as conn:
        result = conn.execute(text(
            "SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code"
        ))
        return [row[0] for row in result.fetchall()]


def get_all_index_codes() -> list[str]:
    """从 index_basic 获取主要指数代码"""
    with read_engine.connect() as conn:
        result = conn.execute(text(
            "SELECT ts_code FROM index_basic ORDER BY ts_code"
        ))
        return [row[0] for row in result.fetchall()]
