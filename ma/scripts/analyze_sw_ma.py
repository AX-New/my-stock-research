"""Layer 3: 申万一级行业 MA 信号分析

30个行业按5大类别分组，批量分析MA信号，验证Layer 1/2指数结论的行业适用性。

数据来源: stock_ma.sw_ma_{daily|weekly|monthly}
牛熊标注: 复用上证指数周期（bull_bear_phases.py）

用法:
  python research/ma/scripts/analyze_sw_ma.py                          # 默认周线全行业
  python research/ma/scripts/analyze_sw_ma.py --freq daily             # 日线
  python research/ma/scripts/analyze_sw_ma.py --freq monthly           # 月线
  python research/ma/scripts/analyze_sw_ma.py --codes 801120.SI,801080.SI   # 指定行业
  python research/ma/scripts/analyze_sw_ma.py --save-signals           # 同时写入数据库
"""
import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import pandas as pd
from sqlalchemy import text

from database import write_engine, read_engine, init_ma_tables
from bull_bear_phases import get_phase, tag_trend
from signal_detector_ma import detect_all_signals
from models import SIGNAL_MAP
from db_utils import batch_upsert
from kline_loader import get_sw_l1_codes
from analyze_index_ma import (
    RETURN_HORIZONS, FREQ_NAMES, SIGNAL_TYPE_NAMES,
)
from app.logger import get_logger

log = get_logger(__name__)

# ── 行业5大类别 ─────────────────────────────────────────────────
INDUSTRY_CATEGORIES = {
    "周期": ["钢铁", "煤炭", "有色金属", "石油石化", "基础化工", "建筑材料"],
    "金融地产": ["银行", "非银金融", "房地产"],
    "大消费": ["食品饮料", "家用电器", "医药生物", "美容护理", "社会服务", "商贸零售", "纺织服饰", "轻工制造", "农林牧渔"],
    "科技成长": ["电子", "计算机", "通信", "传媒", "电力设备"],
    "稳定制造": ["公用事业", "交通运输", "机械设备", "建筑装饰", "国防军工", "汽车", "环保"],
}


# ── 获取行业名称映射 ────────────────────────────────────────────

def get_sw_names() -> dict:
    """获取申万行业名称映射 {ts_code: industry_name}

    从 my_stock 库的 index_classify 表查询
    """
    sql = text(
        "SELECT index_code, industry_name FROM index_classify "
        "WHERE level='L1' AND src='SW2021'"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql)
        return {row[0]: row[1] for row in result.fetchall()}


def _build_name_to_code(sw_names: dict) -> dict:
    """构建 industry_name → ts_code 的反向映射"""
    return {v: k for k, v in sw_names.items()}


def _build_code_to_category(sw_names: dict) -> dict:
    """构建 ts_code → category_name 映射"""
    name_to_code = _build_name_to_code(sw_names)
    code_to_cat = {}
    for cat_name, industry_names in INDUSTRY_CATEGORIES.items():
        for ind_name in industry_names:
            code = name_to_code.get(ind_name)
            if code:
                code_to_cat[code] = cat_name
    return code_to_cat


# ── 数据加载 ──────────────────────────────────────────────────

def load_sw_ma_data(freq: str, ts_code: str) -> pd.DataFrame:
    """从 stock_ma 库加载申万行业 MA 数据"""
    table = f"sw_ma_{freq}"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, "
        f"ma5, ma10, ma20, ma30, ma60, ma90, ma250, "
        f"bias5, bias10, bias20, bias60 "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


# ── 信号分析（单行业）────────────────────────────────────────────

def analyze_industry_signals(df: pd.DataFrame, freq: str, ts_code: str) -> list[dict]:
    """对单个行业检测信号 + 标注牛熊 + 计算后续收益

    返回: enriched 信号列表（每个元素是 dict）
    """
    signals = detect_all_signals(df, freq=freq)
    if not signals:
        return []

    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])
    db_ret_cols = ["ret_5", "ret_10", "ret_20", "ret_60"]

    for sig in signals:
        # 标注牛熊阶段
        sig["trend"] = tag_trend(sig["trade_date"])
        phase = get_phase(sig["trade_date"])
        sig["phase_id"] = phase["id"] if phase else None
        sig["phase_label"] = phase["label"] if phase else None

        # 计算信号后各窗口收益率
        idx = sig["idx"]
        base_close = df.iloc[idx]["close"]
        for i, h in enumerate(horizons):
            target = idx + h
            col_name = db_ret_cols[i] if i < len(db_ret_cols) else f"ret_{h}"
            if target < len(df):
                future_close = df.iloc[target]["close"]
                sig[col_name] = round((future_close - base_close) / base_close * 100, 2)
            else:
                sig[col_name] = None
            # 同时保留原始 horizon 键
            if col_name != f"ret_{h}":
                sig[f"ret_{h}"] = sig[col_name]

        sig["freq"] = freq
        sig["ts_code"] = ts_code

    return signals


# ── 信号写入数据库 ──────────────────────────────────────────────

def save_signals(signals: list[dict]):
    """将信号写入 sw_ma_signal 表"""
    if not signals:
        return

    init_ma_tables()
    model = SIGNAL_MAP["sw"]

    write_cols = [
        "ts_code", "trade_date", "freq", "signal_type", "signal_name",
        "direction", "signal_value", "close", "ma_values",
        "ret_5", "ret_10", "ret_20", "ret_60",
        "trend", "phase_id", "phase_label",
    ]
    records = [{k: sig.get(k) for k in write_cols} for sig in signals]
    batch_upsert(model, records, unique_keys=["ts_code", "trade_date", "freq", "signal_name"])
    log.info("写入信号 %d 条 → %s", len(records), model.__tablename__)



# ── 批量分析 ──────────────────────────────────────────────────

def run_all_analysis(codes: list[str], freq: str, sw_names: dict) -> dict:
    """对所有行业批量运行分析

    Returns: {
        ts_code: {
            "data": DataFrame,
            "signals": list[dict],
        }
    }
    """
    results = {}
    total = len(codes)

    for i, ts_code in enumerate(codes, 1):
        name = sw_names.get(ts_code, ts_code)
        df = load_sw_ma_data(freq, ts_code)
        if df.empty:
            log.warning("  [%d/%d] %s: 无数据", i, total, name)
            continue

        signals = analyze_industry_signals(df, freq, ts_code)
        results[ts_code] = {"data": df, "signals": signals}

        # 各信号类型计数
        type_counts = defaultdict(int)
        for sig in signals:
            type_counts[sig["signal_type"]] += 1
        type_summary = ", ".join(f"{SIGNAL_TYPE_NAMES.get(t, t)}:{c}" for t, c in sorted(type_counts.items()))

        log.info("  [%d/%d] %s: %d条K线, %d信号 (%s)",
                 i, total, name, len(df), len(signals), type_summary)

    return results




# ── 主函数 ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="申万行业 MA 信号分析 (Layer 3)")
    parser.add_argument("--freq", default="weekly",
                        choices=["daily", "weekly", "monthly"],
                        help="分析周期 (默认 weekly)")
    parser.add_argument("--codes", default=None,
                        help="行业代码列表，逗号分隔（默认全部）")
    parser.add_argument("--save-signals", action="store_true",
                        help="将信号写入数据库（默认不写）")
    args = parser.parse_args()

    freq = args.freq
    freq_cn = FREQ_NAMES.get(freq, freq)

    start_time = time.time()
    log.info("=" * 60)
    log.info("Layer 3: 申万行业 MA 信号分析")
    log.info("周期: %s", freq_cn)
    log.info("Start: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # Step 1: 获取行业代码和名称
    sw_names = get_sw_names()
    log.info("从数据库获取行业名称: %d 个", len(sw_names))

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",")]
    else:
        codes = get_sw_l1_codes()

    # 过滤掉不在 INDUSTRY_CATEGORIES 中的行业（如"综合"）
    code_to_cat = _build_code_to_category(sw_names)
    codes = [c for c in codes if c in code_to_cat]
    log.info("分析行业: %d 个（已排除无分类的行业）", len(codes))
    log.info("")

    # Step 2: 批量分析
    results = run_all_analysis(codes, freq, sw_names)
    log.info("")

    # Step 3: 可选写入数据库
    if args.save_signals:
        log.info("--- 写入信号到数据库 ---")
        all_signals = []
        for r in results.values():
            all_signals.extend(r["signals"])
        save_signals(all_signals)
        log.info("")

    elapsed = round(time.time() - start_time, 1)
    log.info("")
    log.info("=" * 60)
    log.info("分析完成 | 行业: %d | 周期: %s | 耗时: %.1fs",
             len(results), freq_cn, elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
