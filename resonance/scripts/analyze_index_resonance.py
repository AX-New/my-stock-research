"""指数级四指标共振分析

以 MACD DIF 极值为锚点，检测 ±2 周窗口内 MA/RSI/换手率信号的共现情况。
评估共振后的胜率和收益率是否优于 MACD 单独信号。

数据源:
  MACD:   stock_research.index_macd_weekly (原始DIF值，脚本内检测极值)
  MA:     stock_ma.index_ma_signal (周线信号事件)
  RSI:    stock_rsi.index_rsi_signal (周线信号事件)
  换手率: stock_turnover.index_turnover_signal (周线信号事件)

分析范围: 上证/深证/创业板 × 周线
共振窗口: ±14 天（约2根周线）

用法:
  python research/resonance/scripts/analyze_index_resonance.py
  python research/resonance/scripts/analyze_index_resonance.py --window 7   # ±1周
  python research/resonance/scripts/analyze_index_resonance.py --window 21  # ±3周
"""
import io
import sys

# Windows 控制台 UTF-8 输出
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import argparse
import sys
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# 复用 MACD 研究的配置
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'macd', 'scripts'))
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD

# ── 数据库连接 ──────────────────────────────────────────────
def _uri(db: str) -> str:
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{db}?charset=utf8mb4"

ENGINE_RESEARCH = create_engine(_uri("stock_research"))
ENGINE_MA = create_engine(_uri("stock_ma"))
ENGINE_RSI = create_engine(_uri("stock_rsi"))
ENGINE_TURNOVER = create_engine(_uri("stock_turnover"))

INDEXES = ["000001.SH", "399001.SZ", "399006.SZ"]
INDEX_NAMES = {"000001.SH": "上证指数", "399001.SZ": "深证成指", "399006.SZ": "创业板指"}

# ── Step 1: MACD DIF 极值检测 ──────────────────────────────
def _find_local_peaks(values: np.ndarray, order: int) -> list[int]:
    """局部极大值点索引（2*order+1 窗口内最大值）"""
    peaks = []
    for i in range(order, len(values) - order):
        window = values[i - order: i + order + 1]
        if not np.isnan(values[i]) and values[i] == np.nanmax(window):
            peaks.append(i)
    return peaks


def _find_local_troughs(values: np.ndarray, order: int) -> list[int]:
    """局部极小值点索引"""
    troughs = []
    for i in range(order, len(values) - order):
        window = values[i - order: i + order + 1]
        if not np.isnan(values[i]) and values[i] == np.nanmin(window):
            troughs.append(i)
    return troughs


def load_macd_signals(ts_code: str) -> pd.DataFrame:
    """从周线 MACD 原始数据检测 DIF 极值，计算后续收益"""
    sql = text("""
        SELECT ts_code, trade_date, close, dif, dea, macd
        FROM index_macd_weekly
        WHERE ts_code = :code
        ORDER BY trade_date
    """)
    df = pd.read_sql(sql, ENGINE_RESEARCH, params={"code": ts_code})
    if df.empty:
        return pd.DataFrame()

    dif = df["dif"].values
    close = df["close"].values
    order = 8  # 周线窗口，与 signal_detector.py 一致

    peaks = _find_local_peaks(dif, order)
    troughs = _find_local_troughs(dif, order)

    signals = []
    # DIF 极大值 = 卖出方向
    for idx in peaks:
        ret = {}
        for w, label in [(2, "ret_2w"), (4, "ret_4w"), (8, "ret_8w"), (13, "ret_13w")]:
            if idx + w < len(close):
                ret[label] = (close[idx + w] - close[idx]) / close[idx] * 100
            else:
                ret[label] = np.nan
        signals.append({
            "ts_code": ts_code,
            "trade_date": df.iloc[idx]["trade_date"],
            "signal_name": "dif_peak",
            "direction": "sell",
            "dif": float(dif[idx]),
            "close": float(close[idx]),
            **ret,
        })
    # DIF 极小值 = 买入方向
    for idx in troughs:
        ret = {}
        for w, label in [(2, "ret_2w"), (4, "ret_4w"), (8, "ret_8w"), (13, "ret_13w")]:
            if idx + w < len(close):
                ret[label] = (close[idx + w] - close[idx]) / close[idx] * 100
            else:
                ret[label] = np.nan
        signals.append({
            "ts_code": ts_code,
            "trade_date": df.iloc[idx]["trade_date"],
            "signal_name": "dif_trough",
            "direction": "buy",
            "dif": float(dif[idx]),
            "close": float(close[idx]),
            **ret,
        })

    result = pd.DataFrame(signals)
    if not result.empty:
        result = result.sort_values("trade_date").reset_index(drop=True)
    return result


# ── Step 2: 加载其他指标信号 ────────────────────────────────
# 每个指标筛选"个股级验证过的有效信号"

# MA: 支撑/阻力（个股级66-67%）+ 乖离率极值（指数级强但个股级弱化到56%，仍纳入对比）
MA_BUY_SIGNALS = ["ma20_support", "ma60_support", "bias20_extreme_low", "bias60_extreme_low"]
MA_SELL_SIGNALS = ["ma20_resist", "ma60_resist", "bias20_extreme_high", "bias60_extreme_high"]

# RSI: 背离（71-74%跨层级稳定）+ 强超卖/超买（66.7%）+ 标准超卖/超买
RSI_BUY_SIGNALS = ["rsi14_bull_divergence", "rsi14_strong_oversold", "rsi14_oversold"]
RSI_SELL_SIGNALS = ["rsi14_bear_divergence", "rsi14_strong_overbought", "rsi14_overbought"]

# 换手率: 极端高（卖出）+ 暴增（指数级买入68%）
TURNOVER_BUY_SIGNALS = ["surge", "extreme_low"]
TURNOVER_SELL_SIGNALS = ["extreme_high", "plunge"]


def load_indicator_signals(engine, table: str, ts_codes: list[str]) -> pd.DataFrame:
    """加载指定指标的周线信号事件"""
    codes_str = ",".join(f"'{c}'" for c in ts_codes)
    sql = text(f"""
        SELECT ts_code, trade_date, signal_type, signal_name, direction,
               signal_value, ret_5, ret_10, ret_20, ret_60
        FROM {table}
        WHERE freq = 'weekly' AND ts_code IN ({codes_str})
        ORDER BY ts_code, trade_date
    """)
    return pd.read_sql(sql, engine)


# ── Step 3: 共振检测 ───────────────────────────────────────
def find_resonance(macd_events: pd.DataFrame,
                   ma_signals: pd.DataFrame,
                   rsi_signals: pd.DataFrame,
                   turnover_signals: pd.DataFrame,
                   window_days: int = 14) -> pd.DataFrame:
    """对每个 MACD DIF 极值事件，在 ±window 天内搜索同向信号"""

    results = []

    for _, event in macd_events.iterrows():
        ts_code = event["ts_code"]
        direction = event["direction"]
        date_str = event["trade_date"]
        # trade_date 格式 YYYYMMDD
        event_date = datetime.strptime(date_str, "%Y%m%d")
        date_lo = (event_date - timedelta(days=window_days)).strftime("%Y%m%d")
        date_hi = (event_date + timedelta(days=window_days)).strftime("%Y%m%d")

        # 根据方向选择对应信号集
        if direction == "buy":
            ma_target = MA_BUY_SIGNALS
            rsi_target = RSI_BUY_SIGNALS
            turnover_target = TURNOVER_BUY_SIGNALS
        else:
            ma_target = MA_SELL_SIGNALS
            rsi_target = RSI_SELL_SIGNALS
            turnover_target = TURNOVER_SELL_SIGNALS

        # 在窗口内搜索同指数、同向信号
        def _search(df: pd.DataFrame, targets: list[str]) -> list[str]:
            if df.empty:
                return []
            mask = (
                (df["ts_code"] == ts_code) &
                (df["trade_date"] >= date_lo) &
                (df["trade_date"] <= date_hi) &
                (df["signal_name"].isin(targets))
            )
            # 换手率 surge/plunge 是 neutral 方向，特殊处理
            if "surge" in targets or "plunge" in targets:
                mask_neutral = (
                    (df["ts_code"] == ts_code) &
                    (df["trade_date"] >= date_lo) &
                    (df["trade_date"] <= date_hi) &
                    (df["signal_name"].isin(["surge", "plunge"]))
                )
                mask_directional = (
                    (df["ts_code"] == ts_code) &
                    (df["trade_date"] >= date_lo) &
                    (df["trade_date"] <= date_hi) &
                    (df["signal_name"].isin(targets)) &
                    (~df["signal_name"].isin(["surge", "plunge"]))
                )
                mask = mask_neutral | mask_directional
            found = df.loc[mask, "signal_name"].unique().tolist()
            return found

        ma_found = _search(ma_signals, ma_target)
        rsi_found = _search(rsi_signals, rsi_target)
        turnover_found = _search(turnover_signals, turnover_target)

        # 分大类标记：是否有该指标的任意信号共振
        has_ma = len(ma_found) > 0
        has_rsi = len(rsi_found) > 0
        has_turnover = len(turnover_found) > 0

        # 细分标记
        has_ma_support = any(s in ma_found for s in ["ma20_support", "ma60_support", "ma20_resist", "ma60_resist"])
        has_ma_bias = any(s.startswith("bias") for s in ma_found)
        has_rsi_divergence = any("divergence" in s for s in rsi_found)
        has_rsi_extreme = any("oversold" in s or "overbought" in s for s in rsi_found)
        has_turnover_extreme = any(s in turnover_found for s in ["extreme_high", "extreme_low"])
        has_turnover_surge = any(s in turnover_found for s in ["surge", "plunge"])

        results.append({
            "ts_code": event["ts_code"],
            "trade_date": event["trade_date"],
            "direction": direction,
            "signal_name": event["signal_name"],
            "dif": event["dif"],
            "close": event["close"],
            "ret_2w": event["ret_2w"],
            "ret_4w": event["ret_4w"],
            "ret_8w": event["ret_8w"],
            "ret_13w": event["ret_13w"],
            # 大类共振标记
            "has_ma": has_ma,
            "has_rsi": has_rsi,
            "has_turnover": has_turnover,
            # 细分共振标记
            "has_ma_support": has_ma_support,
            "has_ma_bias": has_ma_bias,
            "has_rsi_divergence": has_rsi_divergence,
            "has_rsi_extreme": has_rsi_extreme,
            "has_turnover_extreme": has_turnover_extreme,
            "has_turnover_surge": has_turnover_surge,
            # 原始信号列表
            "ma_signals": ",".join(sorted(ma_found)) if ma_found else "",
            "rsi_signals": ",".join(sorted(rsi_found)) if rsi_found else "",
            "turnover_signals": ",".join(sorted(turnover_found)) if turnover_found else "",
        })

    return pd.DataFrame(results)


# ── Step 4: 统计输出 ───────────────────────────────────────
def calc_stats(df: pd.DataFrame, label: str, direction: str, ret_col: str = "ret_4w") -> dict:
    """计算一组信号的胜率和平均收益"""
    if df.empty:
        return {"label": label, "direction": direction, "count": 0}

    valid = df[ret_col].dropna()
    if len(valid) == 0:
        return {"label": label, "direction": direction, "count": 0}

    if direction == "buy":
        wins = (valid > 0).sum()
    else:
        wins = (valid < 0).sum()

    return {
        "label": label,
        "direction": direction,
        "count": len(valid),
        "win_rate": wins / len(valid) * 100,
        "avg_ret": valid.mean(),
        "median_ret": valid.median(),
    }


def print_resonance_stats(resonance_df: pd.DataFrame, window_days: int):
    """按买入/卖出方向分别输出共振统计"""

    print("=" * 80)
    print(f"指数级四指标共振分析（周线，共振窗口 ±{window_days} 天）")
    print(f"指数: {', '.join(INDEX_NAMES.values())}")
    print(f"MACD DIF 极值事件总数: {len(resonance_df)}")
    print("=" * 80)

    for direction, dir_label in [("buy", "买入侧 (DIF极小值)"), ("sell", "卖出侧 (DIF极大值)")]:
        sub = resonance_df[resonance_df["direction"] == direction].copy()
        print(f"\n{'─' * 70}")
        print(f"  {dir_label}  —  共 {len(sub)} 个锚点事件")
        print(f"{'─' * 70}")

        # 共现频率统计
        n = len(sub)
        if n == 0:
            print("  无事件\n")
            continue

        print(f"\n  ▸ 共现频率（{n} 个 MACD DIF 极值中，有多少同时伴随其他信号）:")
        for col, label in [
            ("has_ma", "MA (支撑/阻力/乖离率)"),
            ("has_ma_support", "  ├─ MA 支撑/阻力"),
            ("has_ma_bias", "  └─ MA 乖离率极值"),
            ("has_rsi", "RSI (背离/超卖超买)"),
            ("has_rsi_divergence", "  ├─ RSI 背离"),
            ("has_rsi_extreme", "  └─ RSI 超卖/超买"),
            ("has_turnover", "换手率 (极端/暴增)"),
            ("has_turnover_extreme", "  ├─ 换手率极端"),
            ("has_turnover_surge", "  └─ 换手率暴增/暴跌"),
        ]:
            cnt = sub[col].sum()
            pct = cnt / n * 100
            print(f"    {label:30s}  {cnt:3d}/{n}  ({pct:5.1f}%)")

        # 多指标组合频率
        combos = {
            "MACD+MA": sub["has_ma"],
            "MACD+RSI": sub["has_rsi"],
            "MACD+换手率": sub["has_turnover"],
            "MACD+MA+RSI": sub["has_ma"] & sub["has_rsi"],
            "MACD+MA+换手率": sub["has_ma"] & sub["has_turnover"],
            "MACD+RSI+换手率": sub["has_rsi"] & sub["has_turnover"],
            "四指标全共振": sub["has_ma"] & sub["has_rsi"] & sub["has_turnover"],
        }
        print(f"\n  ▸ 多指标组合共现频率:")
        for combo_label, mask in combos.items():
            cnt = mask.sum()
            pct = cnt / n * 100
            print(f"    {combo_label:20s}  {cnt:3d}/{n}  ({pct:5.1f}%)")

        # 分组胜率对比（核心输出）
        for ret_col, ret_label in [("ret_2w", "T+2周"), ("ret_4w", "T+4周"), ("ret_8w", "T+8周"), ("ret_13w", "T+13周")]:
            print(f"\n  ▸ 胜率与收益对比 — 评估窗口 {ret_label}:")
            print(f"    {'组合':22s} {'样本':>5s} {'胜率':>7s} {'平均收益':>9s} {'中位收益':>9s}")
            print(f"    {'─' * 55}")

            # 基准: MACD 单独
            stats_base = calc_stats(sub, "MACD单独", direction, ret_col)
            _print_stat_row(stats_base)

            # 各组合
            for combo_label, mask in [
                ("MACD+MA支撑阻力", sub["has_ma_support"]),
                ("MACD+MA乖离率", sub["has_ma_bias"]),
                ("MACD+RSI背离", sub["has_rsi_divergence"]),
                ("MACD+RSI超卖超买", sub["has_rsi_extreme"]),
                ("MACD+换手率极端", sub["has_turnover_extreme"]),
                ("MACD+换手率暴增", sub["has_turnover_surge"]),
                ("MACD+MA+RSI", sub["has_ma"] & sub["has_rsi"]),
                ("MACD+MA+换手率", sub["has_ma"] & sub["has_turnover"]),
                ("四指标全共振", sub["has_ma"] & sub["has_rsi"] & sub["has_turnover"]),
            ]:
                s = calc_stats(sub[mask], combo_label, direction, ret_col)
                _print_stat_row(s)

            # 对照组: 无任何共振
            no_resonance = ~sub["has_ma"] & ~sub["has_rsi"] & ~sub["has_turnover"]
            s = calc_stats(sub[no_resonance], "MACD独行(无共振)", direction, ret_col)
            _print_stat_row(s)


def _print_stat_row(s: dict):
    """打印单行统计"""
    if s["count"] == 0:
        print(f"    {s['label']:22s} {'0':>5s}   {'N/A':>6s}   {'N/A':>8s}   {'N/A':>8s}")
    else:
        print(f"    {s['label']:22s} {s['count']:5d}  {s['win_rate']:6.1f}%  {s['avg_ret']:+8.2f}%  {s['median_ret']:+8.2f}%")


def print_detail_examples(resonance_df: pd.DataFrame, n: int = 10):
    """打印共振事件的详细样例，方便人工核验"""
    print(f"\n{'=' * 80}")
    print(f"共振事件样例（最近 {n} 个四指标共振或三指标共振）")
    print("=" * 80)

    # 优先显示多指标共振
    full = resonance_df[
        resonance_df["has_ma"] & resonance_df["has_rsi"] & resonance_df["has_turnover"]
    ].tail(n)

    if len(full) < n:
        # 补充三指标共振
        triple = resonance_df[
            (resonance_df["has_ma"] & resonance_df["has_rsi"]) |
            (resonance_df["has_ma"] & resonance_df["has_turnover"]) |
            (resonance_df["has_rsi"] & resonance_df["has_turnover"])
        ].tail(n - len(full))
        full = pd.concat([triple, full]).drop_duplicates(subset=["ts_code", "trade_date"]).tail(n)

    if full.empty:
        print("  无多指标共振事件")
        return

    for _, row in full.iterrows():
        name = INDEX_NAMES.get(row["ts_code"], row["ts_code"])
        dir_str = "买入" if row["direction"] == "buy" else "卖出"
        print(f"\n  {name} {row['trade_date']}  MACD DIF {'极小值' if row['direction']=='buy' else '极大值'}({dir_str})")
        print(f"    DIF={row['dif']:.2f}  收盘={row['close']:.2f}")
        if row["ma_signals"]:
            print(f"    MA 共振: {row['ma_signals']}")
        if row["rsi_signals"]:
            print(f"    RSI共振: {row['rsi_signals']}")
        if row["turnover_signals"]:
            print(f"    换手率共振: {row['turnover_signals']}")
        r2 = f"{row['ret_2w']:+.2f}%" if not np.isnan(row['ret_2w']) else "N/A"
        r4 = f"{row['ret_4w']:+.2f}%" if not np.isnan(row['ret_4w']) else "N/A"
        r8 = f"{row['ret_8w']:+.2f}%" if not np.isnan(row['ret_8w']) else "N/A"
        r13 = f"{row['ret_13w']:+.2f}%" if not np.isnan(row['ret_13w']) else "N/A"
        print(f"    后续收益: 2w={r2}  4w={r4}  8w={r8}  13w={r13}")


# ── 按指数分别输出 ──────────────────────────────────────────
def print_per_index_stats(resonance_df: pd.DataFrame, window_days: int):
    """按单个指数分别输出统计，检查结论跨指数一致性"""
    print(f"\n\n{'=' * 80}")
    print("分指数统计（检查跨指数一致性）")
    print("=" * 80)

    ret_col = "ret_4w"  # 用 T+4周 作为主评估窗口
    for ts_code in INDEXES:
        name = INDEX_NAMES[ts_code]
        sub = resonance_df[resonance_df["ts_code"] == ts_code]
        if sub.empty:
            continue

        print(f"\n  ◆ {name} ({ts_code}) — {len(sub)} 个 DIF 极值事件")

        for direction, dir_label in [("buy", "买入"), ("sell", "卖出")]:
            d = sub[sub["direction"] == direction]
            if d.empty:
                continue
            n = len(d)

            # 基准
            base = calc_stats(d, "MACD单独", direction, ret_col)
            # 有MA共振
            with_ma = calc_stats(d[d["has_ma"]], "+MA", direction, ret_col)
            # 有RSI共振
            with_rsi = calc_stats(d[d["has_rsi"]], "+RSI", direction, ret_col)
            # 无共振
            alone = calc_stats(d[~d["has_ma"] & ~d["has_rsi"] & ~d["has_turnover"]],
                               "无共振", direction, ret_col)

            print(f"    {dir_label}({n}):  ", end="")
            parts = []
            for s in [base, with_ma, with_rsi, alone]:
                if s["count"] > 0:
                    parts.append(f"{s['label']}={s['win_rate']:.0f}%({s['count']})")
                else:
                    parts.append(f"{s['label']}=N/A(0)")
            print("  |  ".join(parts))


# ── Main ───────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="指数级四指标共振分析")
    parser.add_argument("--window", type=int, default=14, help="共振窗口天数（默认14天=±2周）")
    args = parser.parse_args()

    t0 = datetime.now()
    print(f"开始加载数据... ({t0.strftime('%H:%M:%S')})")

    # 1) 加载 MACD DIF 极值
    macd_all = []
    for code in INDEXES:
        df = load_macd_signals(code)
        print(f"  MACD {INDEX_NAMES[code]}: {len(df)} 个 DIF 极值")
        macd_all.append(df)
    macd_events = pd.concat(macd_all, ignore_index=True)

    # 2) 加载其他指标信号
    ma_signals = load_indicator_signals(ENGINE_MA, "index_ma_signal", INDEXES)
    print(f"  MA 周线信号: {len(ma_signals)} 条")

    rsi_signals = load_indicator_signals(ENGINE_RSI, "index_rsi_signal", INDEXES)
    print(f"  RSI 周线信号: {len(rsi_signals)} 条")

    turnover_signals = load_indicator_signals(ENGINE_TURNOVER, "index_turnover_signal", INDEXES)
    print(f"  换手率 周线信号: {len(turnover_signals)} 条")

    t1 = datetime.now()
    print(f"数据加载完成 ({(t1-t0).total_seconds():.1f}s)")

    # 3) 共振检测
    print(f"\n检测共振（窗口 ±{args.window} 天）...")
    resonance_df = find_resonance(macd_events, ma_signals, rsi_signals, turnover_signals,
                                  window_days=args.window)
    t2 = datetime.now()
    print(f"共振检测完成 ({(t2-t1).total_seconds():.1f}s)")

    # 4) 输出统计
    print_resonance_stats(resonance_df, args.window)
    print_per_index_stats(resonance_df, args.window)
    print_detail_examples(resonance_df, n=15)

    t3 = datetime.now()
    print(f"\n总耗时: {(t3-t0).total_seconds():.1f}s")


if __name__ == "__main__":
    main()
