"""Layer 1: 指数 MA 均线信号全维度分析

分析内容:
1. 各周期 MA 信号统计
2. 信号后续收益分析（平均收益 + 胜率）
3. 牛熊阶段 × 信号类型交叉分析（核心）
4. 各频率对比

用法:
  python research/ma/scripts/analyze_index_ma.py                            # 默认上证指数
  python research/ma/scripts/analyze_index_ma.py --ts_code 399001.SZ --name 深证成指
  python research/ma/scripts/analyze_index_ma.py --ts_code 399006.SZ --name 创业板指
  python research/ma/scripts/analyze_index_ma.py --save-signals             # 同时将信号写入数据库
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

from database import write_engine, init_ma_tables
from bull_bear_phases import get_phase, tag_trend
from signal_detector_ma import detect_all_signals
from models import SIGNAL_MAP
from db_utils import batch_upsert
from app.logger import get_logger

log = get_logger(__name__)

# ── 默认值，可通过命令行参数覆盖 ────────────────────────────────
TS_CODE = "000001.SH"
INDEX_NAME = "上证指数"
FREQS = ["daily", "weekly", "monthly"]  # 年线数据点太少，不做信号统计

# ── 各周期的后续收益计算窗口（单位: K线根数）──────────────────────
RETURN_HORIZONS = {
    "daily": [5, 10, 20, 60],       # 1周 / 2周 / 1月 / 3月
    "weekly": [2, 4, 8, 13],        # 2周 / 1月 / 2月 / 1季
    "monthly": [1, 3, 6, 12],       # 1月 / 1季 / 半年 / 1年
}

# ── 各周期收益窗口的可读名称 ─────────────────────────────────────
HORIZON_LABELS = {
    "daily": {5: "T+5(1周)", 10: "T+10(2周)", 20: "T+20(1月)", 60: "T+60(3月)"},
    "weekly": {2: "T+2(2周)", 4: "T+4(1月)", 8: "T+8(2月)", 13: "T+13(1季)"},
    "monthly": {1: "T+1(1月)", 3: "T+3(1季)", 6: "T+6(半年)", 12: "T+12(1年)"},
}

FREQ_NAMES = {"daily": "日线", "weekly": "周线", "monthly": "月线"}

# ── 信号中文名映射 ──────────────────────────────────────────────
SIGNAL_NAMES = {
    # bias_extreme — 乖离率极值
    "bias5_extreme_low": "MA5乖离率超卖", "bias5_extreme_high": "MA5乖离率过热",
    "bias10_extreme_low": "MA10乖离率超卖", "bias10_extreme_high": "MA10乖离率过热",
    "bias20_extreme_low": "MA20乖离率超卖", "bias20_extreme_high": "MA20乖离率过热",
    "bias60_extreme_low": "MA60乖离率超卖", "bias60_extreme_high": "MA60乖离率过热",
    # direction_break — 方向突破
    "ma20_break_up": "MA20方向突破", "ma20_break_down": "MA20方向跌破",
    "ma60_break_up": "MA60方向突破", "ma60_break_down": "MA60方向跌破",
    # fake_break — 假突破
    "ma20_fake_break_up": "MA20假突破(回落)", "ma20_fake_break_down": "MA20假跌破(回升)",
    "ma60_fake_break_up": "MA60假突破(回落)", "ma60_fake_break_down": "MA60假跌破(回升)",
    # support_resist — 支撑阻力
    "ma20_support": "MA20支撑", "ma20_resist": "MA20阻力",
    "ma60_support": "MA60支撑", "ma60_resist": "MA60阻力",
    # alignment — 均线排列
    "alignment_bull": "多头排列形成", "alignment_bear": "空头排列形成",
    # convergence — 粘合发散
    "convergence_bull": "粘合后向上发散", "convergence_bear": "粘合后向下发散",
    # ma_cross — MA交叉 (6对 × 2)
    "ma5_cross_ma10_golden": "MA5/10金叉", "ma5_cross_ma10_death": "MA5/10死叉",
    "ma5_cross_ma20_golden": "MA5/20金叉", "ma5_cross_ma20_death": "MA5/20死叉",
    "ma5_cross_ma30_golden": "MA5/30金叉", "ma5_cross_ma30_death": "MA5/30死叉",
    "ma10_cross_ma20_golden": "MA10/20金叉", "ma10_cross_ma20_death": "MA10/20死叉",
    "ma10_cross_ma30_golden": "MA10/30金叉", "ma10_cross_ma30_death": "MA10/30死叉",
    "ma20_cross_ma30_golden": "MA20/30金叉", "ma20_cross_ma30_death": "MA20/30死叉",
}

# ── 信号大类中文名映射 ──────────────────────────────────────────
SIGNAL_TYPE_NAMES = {
    "bias_extreme": "乖离率极值",
    "direction_break": "方向突破",
    "fake_break": "假突破",
    "support_resist": "支撑阻力",
    "alignment": "均线排列",
    "convergence": "粘合发散",
    "ma_cross": "MA交叉",
}

# ── buy 方向的信号集合（用于判断胜率方向）──────────────────────────
BUY_SIGNALS = {name for name in SIGNAL_NAMES
               if any(kw in name for kw in [
                   "extreme_low", "break_up", "fake_break_down",
                   "support", "alignment_bull", "convergence_bull", "golden",
               ])}


# ── 数据加载 ──────────────────────────────────────────────────

def load_ma_data(freq: str, ts_code: str = None) -> pd.DataFrame:
    """从 stock_ma 库加载指数 MA 数据"""
    ts_code = ts_code or TS_CODE
    table = f"index_ma_{freq}"
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


# ── 信号分析 ──────────────────────────────────────────────────

def analyze_signals(df: pd.DataFrame, freq: str, ts_code: str) -> list[dict]:
    """对指定周期的数据检测信号 + 标注牛熊 + 计算后续收益

    返回 enriched 信号列表（每个元素是 dict），包含:
    - 原始信号字段 (idx, trade_date, signal_type, signal_name, direction, signal_value, close, ma_values)
    - 牛熊标注 (trend, phase_id, phase_label)
    - 后续收益 (ret_5, ret_10, ret_20, ret_60 等)
    - 标识信息 (freq, ts_code)
    """
    signals = detect_all_signals(df, freq=freq)
    if not signals:
        return []

    horizons = RETURN_HORIZONS.get(freq, [5, 10, 20, 60])

    enriched = []
    for sig in signals:
        # 标注牛熊阶段
        sig["trend"] = tag_trend(sig["trade_date"])
        phase = get_phase(sig["trade_date"])
        sig["phase_id"] = phase["id"] if phase else None
        sig["phase_label"] = phase["label"] if phase else None

        # 计算信号后各窗口收益率
        # DB 列固定为 ret_5/ret_10/ret_20/ret_60，不同频率的 horizon 映射到这4列
        # daily: [5,10,20,60] → ret_5/10/20/60
        # weekly: [2,4,8,13] → ret_5/10/20/60 (存储含义不同但列名统一)
        # monthly: [1,3,6,12] → ret_5/10/20/60
        db_ret_cols = ["ret_5", "ret_10", "ret_20", "ret_60"]
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
            # 同时保留原始 horizon 键，供统计函数使用
            if col_name != f"ret_{h}":
                sig[f"ret_{h}"] = sig[col_name]

        # 补充标识信息（用于 DB 写入）
        sig["freq"] = freq
        sig["ts_code"] = ts_code
        enriched.append(sig)

    return enriched


# ── 信号写入数据库 ────────────────────────────────────────────

def save_signals(signals: list[dict], source_type: str = "index"):
    """将信号写入信号表"""
    if not signals:
        return

    init_ma_tables()
    model = SIGNAL_MAP[source_type]

    # 只取信号表对应的列
    write_cols = [
        "ts_code", "trade_date", "freq", "signal_type", "signal_name",
        "direction", "signal_value", "close", "ma_values",
        "ret_5", "ret_10", "ret_20", "ret_60",
        "trend", "phase_id", "phase_label",
    ]
    records = [{k: sig.get(k) for k in write_cols} for sig in signals]
    batch_upsert(model, records, unique_keys=["ts_code", "trade_date", "freq", "signal_name"])
    log.info("写入信号 %d 条 → %s", len(records), model.__tablename__)




# ── 主函数 ──────────────────────────────────────────────────

def main():
    global TS_CODE, INDEX_NAME

    parser = argparse.ArgumentParser(description="指数 MA 信号分析")
    parser.add_argument("--ts_code", default="000001.SH",
                        help="指数代码 (如 000001.SH, 399001.SZ, 399006.SZ)")
    parser.add_argument("--name", default=None,
                        help="指数名称 (如 上证指数, 深证成指, 创业板指)")
    parser.add_argument("--save-signals", action="store_true",
                        help="将信号写入数据库（默认不写，方便快速迭代）")
    args = parser.parse_args()

    TS_CODE = args.ts_code
    INDEX_NAME = args.name or {
        "000001.SH": "上证指数",
        "399001.SZ": "深证成指",
        "399006.SZ": "创业板指",
    }.get(TS_CODE, TS_CODE)

    start_time = time.time()
    log.info("=" * 60)
    log.info("MA Signal Analysis")
    log.info("Index: %s (%s)", TS_CODE, INDEX_NAME)
    log.info("Start: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    all_results = {}

    # Step 1: 加载数据 + 检测信号 + 统计
    for freq in FREQS:
        log.info("--- %s ---", FREQ_NAMES[freq])
        df = load_ma_data(freq, TS_CODE)
        if df.empty:
            log.warning("  无数据，跳过（请先运行 compute_index_ma.py --freq %s）", freq)
            continue

        log.info("  数据量: %d 条", len(df))
        log.info("  时间范围: %s ~ %s", df['trade_date'].iloc[0], df['trade_date'].iloc[-1])

        # 检测信号并标注牛熊+计算收益
        signals = analyze_signals(df, freq, TS_CODE)
        log.info("  信号数量: %d", len(signals))

        if signals:
            # 各信号类型数量统计（控制台输出）
            type_counts = defaultdict(int)
            for sig in signals:
                type_counts[sig["signal_type"]] += 1
            for sig_type, count in sorted(type_counts.items()):
                log.info("    %s: %d", SIGNAL_TYPE_NAMES.get(sig_type, sig_type), count)

        all_results[freq] = {
            "data": df,
            "signals": signals,
        }

    log.info("")

    # Step 2: 可选写入数据库
    if args.save_signals:
        log.info("--- 写入信号到数据库 ---")
        for freq in FREQS:
            if freq in all_results and all_results[freq]["signals"]:
                save_signals(all_results[freq]["signals"], source_type="index")
        log.info("")

    elapsed = round(time.time() - start_time, 1)
    log.info("")
    log.info("=" * 60)
    log.info("分析完成 | 总耗时: %.1fs", elapsed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
