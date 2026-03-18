#!/usr/bin/env python
"""
个股选股扫描程序 — 跨4指标信号表查询 + 共振检测 + 统计分析

数据源:
  - stock_research.stock_macd_signal  (317万, MACD信号)
  - stock_ma.stock_ma_signal          (3146万, MA信号)
  - stock_rsi.stock_rsi_signal        (759万, RSI信号)
  - stock_turnover.stock_turnover_signal (1791万, 换手率信号)

用法:
  python la/scripts/stock_screener.py scan                        # 最近交易日信号
  python la/scripts/stock_screener.py scan --date 20260312        # 指定日期
  python la/scripts/stock_screener.py scan --days 5               # 近5天
  python la/scripts/stock_screener.py scan --grade S              # 只看S级
  python la/scripts/stock_screener.py stock 300750.SZ             # 单股信号
  python la/scripts/stock_screener.py stock 300750.SZ --days 30   # 单股近30天
  python la/scripts/stock_screener.py resonance                   # 共振分析
  python la/scripts/stock_screener.py resonance --date 20260312   # 指定日期共振
  python la/scripts/stock_screener.py stats                       # 统计分析(报告用)
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from app.logger import get_logger

log = get_logger("screener")

# ─── 数据库 ────────────────────────────────────────────────────────────────────

MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3307))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")

# 单引擎，跨库查询 (MySQL 支持 db.table 语法)
DB_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/my_stock?charset=utf8mb4"
)
engine = create_engine(DB_URI, pool_pre_ping=True, pool_recycle=3600)

# ─── 4张信号表 ─────────────────────────────────────────────────────────────────

SIGNAL_TABLES = [
    {
        "db": "stock_research",
        "table": "stock_macd_signal",
        "indicator": "MACD",
    },
    {
        "db": "stock_ma",
        "table": "stock_ma_signal",
        "indicator": "MA",
    },
    {
        "db": "stock_rsi",
        "table": "stock_rsi_signal",
        "indicator": "RSI",
    },
    {
        "db": "stock_turnover",
        "table": "stock_turnover_signal",
        "indicator": "TURN",
    },
]

# ─── 信号分级 (S/A/B/C) ───────────────────────────────────────────────────────
# 基于四指标 L1-L4 研究结论，个股级胜率验证

SIGNAL_GRADE = {
    # === S级: 最强信号 ===
    # MACD DIF极值 — 个股周线胜率97.5%(买) / 90%+(卖)
    "dif_trough": "S",
    "dif_peak": "S",

    # === A级: 强信号 ===
    # MACD背离 — 指数级99.9%但极稀缺，个股级仍强
    "bottom_divergence": "A",
    "top_divergence": "A",
    # RSI14背离 — 跨层级最稳定: 底背离71.4%, 顶背离73.7%
    "rsi14_bull_divergence": "A",
    "rsi14_bear_divergence": "A",
    # MA支撑/阻力 — 个股级衰减最小: 支撑66.1%, 阻力67.0%
    "ma20_support": "A",
    "ma20_resist": "A",
    "ma60_support": "A",
    "ma60_resist": "A",

    # === B级: 辅助信号 ===
    # RSI极端值 — 抗噪声: 强超卖66.7%, 超卖56.8%
    "rsi14_strong_oversold": "B",
    "rsi14_strong_overbought": "B",
    "rsi14_oversold": "B",
    "rsi14_overbought": "B",
    # 换手率超高 — 个股级唯一有效卖出: 59%
    "extreme_high": "B",
    "extreme_low": "B",
    # MA乖离率极端 — 个股级弱化但有参考价值
    "bias20_extreme_low": "B",
    "bias20_extreme_high": "B",
    "bias60_extreme_low": "B",
    "bias60_extreme_high": "B",
    "bias10_extreme_low": "B",
    "bias10_extreme_high": "B",
    "bias5_extreme_low": "B",
    "bias5_extreme_high": "B",
    # RSI自适应阈值
    "rsi14_adaptive_high": "B",
    "rsi14_adaptive_low": "B",
    # MA突破/假突破
    "ma20_break_up": "B",
    "ma20_break_down": "B",
    "ma60_break_up": "B",
    "ma60_break_down": "B",
    "ma20_fake_break_up": "B",
    "ma20_fake_break_down": "B",
    "ma60_fake_break_up": "B",
    "ma60_fake_break_down": "B",
    # 换手率持续/量价背离
    "sustained_high": "B",
    "sustained_low": "B",
    "price_up_vol_down": "B",
    "price_down_vol_up": "B",

    # === C级: 噪音信号(入库但不主动推荐) ===
    # MACD交叉 — 四指标一致结论:交叉=噪音
    "golden_cross": "C",
    "death_cross": "C",
    "zero_golden_cross": "C",
    "zero_death_cross": "C",
    "dif_cross_zero_up": "C",
    "dif_cross_zero_down": "C",
    # RSI交叉/摆动
    "rsi14_cross_above_50": "C",
    "rsi14_cross_below_50": "C",
    "rsi6_overbought": "C",
    "rsi6_oversold": "C",
    "rsi12_overbought": "C",
    "rsi12_oversold": "C",
    "rsi24_overbought": "C",
    "rsi24_oversold": "C",
    "rsi14_bull_failure_swing": "C",
    "rsi14_bear_failure_swing": "C",
    # MA交叉 — 噪音
    "ma5_cross_ma10_golden": "C",
    "ma5_cross_ma10_death": "C",
    "ma5_cross_ma20_golden": "C",
    "ma5_cross_ma20_death": "C",
    "ma5_cross_ma30_golden": "C",
    "ma5_cross_ma30_death": "C",
    "ma10_cross_ma20_golden": "C",
    "ma10_cross_ma20_death": "C",
    "ma10_cross_ma30_golden": "C",
    "ma10_cross_ma30_death": "C",
    "ma20_cross_ma30_golden": "C",
    "ma20_cross_ma30_death": "C",
    # MA排列/收敛
    "alignment_bull": "C",
    "alignment_bear": "C",
    "convergence_bull": "C",
    "convergence_bear": "C",
    # 换手率交叉/区间/异动
    "ma_cross_up": "C",
    "ma_cross_down": "C",
    "zone_high": "C",
    "zone_low": "C",
    "surge": "C",
    "plunge": "C",
}

# 信号方向映射(优先取数据库direction字段，备用)
SIGNAL_DIRECTION = {
    "dif_trough": "buy", "dif_peak": "sell",
    "bottom_divergence": "buy", "top_divergence": "sell",
    "rsi14_bull_divergence": "buy", "rsi14_bear_divergence": "sell",
    "ma20_support": "buy", "ma20_resist": "sell",
    "ma60_support": "buy", "ma60_resist": "sell",
    "extreme_high": "sell", "extreme_low": "buy",
}

# 分级顺序
GRADE_ORDER = {"S": 0, "A": 1, "B": 2, "C": 3}


# ─── 股票信息缓存 ──────────────────────────────────────────────────────────────

_stock_info_cache = None


def get_stock_info() -> pd.DataFrame:
    """获取全部股票基础信息(带缓存)"""
    global _stock_info_cache
    if _stock_info_cache is None:
        sql = "SELECT ts_code, name, industry, market FROM stock_basic WHERE list_status = 'L'"
        _stock_info_cache = pd.read_sql(text(sql), engine)
    return _stock_info_cache


def enrich_with_stock_info(df: pd.DataFrame) -> pd.DataFrame:
    """为信号数据添加股票名称和行业"""
    if df.empty:
        return df
    info = get_stock_info()
    return df.merge(info[["ts_code", "name", "industry"]], on="ts_code", how="left")


# ─── 信号查询 ──────────────────────────────────────────────────────────────────

def query_signals(
    date: str = None,
    date_start: str = None,
    date_end: str = None,
    ts_code: str = None,
    freq: str = None,
    min_grade: str = None,
    direction: str = None,
    indicators: list = None,
) -> pd.DataFrame:
    """
    跨4张信号表统一查询，返回标准化DataFrame

    参数:
        date: 精确日期 (YYYYMMDD)
        date_start/date_end: 日期范围
        ts_code: 股票代码
        freq: 周期 (daily/weekly/monthly)
        min_grade: 最低信号等级 (S/A/B/C)
        direction: buy/sell
        indicators: 指标列表 ['MACD','RSI','MA','TURN']
    """
    # 标准列(所有表共有)
    common_cols = [
        "ts_code", "trade_date", "freq",
        "signal_type", "signal_name", "direction",
        "signal_value", "close",
        "ret_5", "ret_10", "ret_20", "ret_60",
    ]

    all_dfs = []
    tables = SIGNAL_TABLES
    if indicators:
        tables = [t for t in tables if t["indicator"] in indicators]

    for tbl in tables:
        fqn = f"{tbl['db']}.{tbl['table']}"
        cols_sql = ", ".join(common_cols)
        where = ["1=1"]
        params = {}

        if date:
            where.append("trade_date = :date")
            params["date"] = date
        if date_start:
            where.append("trade_date >= :date_start")
            params["date_start"] = date_start
        if date_end:
            where.append("trade_date <= :date_end")
            params["date_end"] = date_end
        if ts_code:
            where.append("ts_code = :ts_code")
            params["ts_code"] = ts_code
        if freq:
            where.append("freq = :freq")
            params["freq"] = freq
        if direction:
            where.append("direction = :direction")
            params["direction"] = direction

        # 信号等级过滤: 只查grade <= min_grade的signal_name
        if min_grade:
            max_order = GRADE_ORDER.get(min_grade, 3)
            allowed = [k for k, v in SIGNAL_GRADE.items()
                       if GRADE_ORDER.get(v, 99) <= max_order]
            if not allowed:
                continue
            placeholders = ", ".join(f":sig_{i}" for i in range(len(allowed)))
            where.append(f"signal_name IN ({placeholders})")
            for i, s in enumerate(allowed):
                params[f"sig_{i}"] = s

        sql = f"SELECT {cols_sql} FROM {fqn} WHERE {' AND '.join(where)}"
        try:
            df = pd.read_sql(text(sql), engine, params=params)
            df["indicator"] = tbl["indicator"]
            all_dfs.append(df)
        except Exception as e:
            log.warning(f"查询 {fqn} 失败: {e}")

    if not all_dfs:
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True)

    # 添加信号等级
    result["grade"] = result["signal_name"].map(SIGNAL_GRADE).fillna("C")
    result["grade_order"] = result["grade"].map(GRADE_ORDER).fillna(99)

    # 按等级 > 日期 > 股票排序
    result.sort_values(
        ["grade_order", "trade_date", "ts_code"],
        ascending=[True, False, True],
        inplace=True,
    )
    return result


def get_latest_signal_date() -> str:
    """获取信号表中最新的交易日期"""
    dates = []
    with engine.connect() as conn:
        for tbl in SIGNAL_TABLES:
            fqn = f"{tbl['db']}.{tbl['table']}"
            r = conn.execute(text(f"SELECT MAX(trade_date) FROM {fqn}"))
            d = r.fetchone()[0]
            if d:
                dates.append(d)
    return min(dates) if dates else None  # 取最早的(保证4表都有数据)


# ─── 共振检测 ──────────────────────────────────────────────────────────────────

def detect_resonance(
    date: str = None,
    date_start: str = None,
    date_end: str = None,
    window_days: int = 14,
    freq: str = "weekly",
) -> pd.DataFrame:
    """
    共振检测: 以MACD DIF极值为锚点，搜索 ±window_days 内其他指标的共振信号

    返回: 每个DIF极值事件 + 共振信号列表 + 共振评分
    """
    # 1. 获取MACD DIF极值事件
    macd_extremes = query_signals(
        date=date,
        date_start=date_start,
        date_end=date_end,
        freq=freq,
        indicators=["MACD"],
    )
    macd_extremes = macd_extremes[
        macd_extremes["signal_name"].isin(["dif_trough", "dif_peak"])
    ]

    if macd_extremes.empty:
        log.info("[resonance] 未找到DIF极值事件")
        return pd.DataFrame()

    log.info(f"[resonance] 找到 {len(macd_extremes)} 个DIF极值事件，检测共振...")

    results = []
    for _, anchor in macd_extremes.iterrows():
        ts_code = anchor["ts_code"]
        trade_date = anchor["trade_date"]

        # 计算窗口期(交易日约为自然日的0.7倍，用自然日扩大范围)
        dt = datetime.strptime(trade_date, "%Y%m%d")
        win_start = (dt - timedelta(days=window_days * 2)).strftime("%Y%m%d")
        win_end = (dt + timedelta(days=window_days * 2)).strftime("%Y%m%d")

        # 搜索其他指标信号
        other_signals = query_signals(
            date_start=win_start,
            date_end=win_end,
            ts_code=ts_code,
            min_grade="B",  # 只看B级以上信号参与共振
            indicators=["RSI", "MA", "TURN"],
        )

        # 按方向匹配(买入锚点找买入共振, 卖出找卖出)
        anchor_dir = anchor["direction"]
        if not other_signals.empty and anchor_dir:
            matched = other_signals[other_signals["direction"] == anchor_dir]
        else:
            matched = pd.DataFrame()

        # 共振评分: 每个共振指标+1, A级+2, B级+1
        resonance_indicators = set()
        resonance_score = 0
        resonance_signals = []
        if not matched.empty:
            for _, sig in matched.iterrows():
                ind = sig["indicator"]
                resonance_indicators.add(ind)
                grade = sig["grade"]
                score = 2 if grade in ("S", "A") else 1
                resonance_score += score
                resonance_signals.append(
                    f"{ind}:{sig['signal_name']}({sig['trade_date']})"
                )

        results.append({
            "ts_code": ts_code,
            "trade_date": trade_date,
            "freq": freq,
            "anchor_signal": anchor["signal_name"],
            "anchor_direction": anchor_dir,
            "close": anchor["close"],
            "ret_5": anchor["ret_5"],
            "ret_10": anchor["ret_10"],
            "ret_20": anchor["ret_20"],
            "ret_60": anchor["ret_60"],
            "resonance_count": len(resonance_indicators),
            "resonance_score": resonance_score,
            "resonance_indicators": "+".join(sorted(resonance_indicators)) or "无",
            "resonance_signals": "; ".join(resonance_signals[:5]) or "无",
        })

    df = pd.DataFrame(results)
    df.sort_values("resonance_score", ascending=False, inplace=True)
    return df


# ─── 统计分析(报告数据生成) ─────────────────────────────────────────────────────

def generate_stats() -> dict:
    """
    生成统计数据，供策略报告使用

    返回:
        dict: 包含多维度统计结果
    """
    stats = {}
    t0 = time.time()

    with engine.connect() as conn:
        # 1. 各信号在不同频率下的胜率和收益
        log.info("[stats] 计算各信号胜率和收益...")
        signal_stats = []
        for tbl in SIGNAL_TABLES:
            fqn = f"{tbl['db']}.{tbl['table']}"
            sql = text(f"""
                SELECT signal_name, direction, freq,
                    COUNT(*) as cnt,
                    ROUND(AVG(CASE
                        WHEN direction='buy' AND ret_20>0 THEN 1
                        WHEN direction='sell' AND ret_20<0 THEN 1
                        ELSE 0
                    END)*100, 1) as win_rate_20,
                    ROUND(AVG(ret_5), 2) as avg_ret5,
                    ROUND(AVG(ret_10), 2) as avg_ret10,
                    ROUND(AVG(ret_20), 2) as avg_ret20,
                    ROUND(AVG(ret_60), 2) as avg_ret60,
                    ROUND(STDDEV(ret_20), 2) as std_ret20
                FROM {fqn}
                WHERE ret_20 IS NOT NULL
                GROUP BY signal_name, direction, freq
            """)
            df = pd.read_sql(sql, conn)
            df["indicator"] = tbl["indicator"]
            df["grade"] = df["signal_name"].map(SIGNAL_GRADE).fillna("C")
            signal_stats.append(df)
        stats["signal_stats"] = pd.concat(signal_stats, ignore_index=True)

        # 2. 周线S/A级信号的月度胜率稳定性
        log.info("[stats] 计算S/A级信号月度胜率...")
        sa_signals = [k for k, v in SIGNAL_GRADE.items() if v in ("S", "A")]
        monthly_wr = []
        for tbl in SIGNAL_TABLES:
            fqn = f"{tbl['db']}.{tbl['table']}"
            placeholders = ", ".join(f"'{s}'" for s in sa_signals)
            sql = text(f"""
                SELECT signal_name, direction,
                    LEFT(trade_date, 6) as month,
                    COUNT(*) as cnt,
                    ROUND(AVG(CASE
                        WHEN direction='buy' AND ret_20>0 THEN 1
                        WHEN direction='sell' AND ret_20<0 THEN 1
                        ELSE 0
                    END)*100, 1) as win_rate_20
                FROM {fqn}
                WHERE freq = 'weekly'
                  AND ret_20 IS NOT NULL
                  AND signal_name IN ({placeholders})
                GROUP BY signal_name, direction, LEFT(trade_date, 6)
                HAVING cnt >= 5
            """)
            df = pd.read_sql(sql, conn)
            if not df.empty:
                df["indicator"] = tbl["indicator"]
                monthly_wr.append(df)
        if monthly_wr:
            stats["monthly_stability"] = pd.concat(monthly_wr, ignore_index=True)

        # 3. 共振vs单独信号的收益对比 (采样最近5年数据)
        log.info("[stats] 计算共振收益对比...")
        # 取MACD DIF极值(周线), 近5年
        macd_sql = text("""
            SELECT ts_code, trade_date, signal_name, direction,
                   close, ret_5, ret_10, ret_20, ret_60
            FROM stock_research.stock_macd_signal
            WHERE signal_name IN ('dif_trough', 'dif_peak')
              AND freq = 'weekly'
              AND trade_date >= '20210101'
              AND ret_20 IS NOT NULL
        """)
        macd_df = pd.read_sql(macd_sql, conn)
        stats["macd_anchors"] = macd_df
        log.info(f"  DIF极值事件(周线, 近5年): {len(macd_df)}")

        # 4. 行业维度信号分布
        log.info("[stats] 计算行业维度信号分布...")
        # 取S/A级周线信号 + stock_basic行业
        industry_stats = []
        for tbl in SIGNAL_TABLES:
            fqn = f"{tbl['db']}.{tbl['table']}"
            placeholders = ", ".join(f"'{s}'" for s in sa_signals)
            sql = text(f"""
                SELECT s.signal_name, s.direction, b.industry,
                    COUNT(*) as cnt,
                    ROUND(AVG(CASE
                        WHEN s.direction='buy' AND s.ret_20>0 THEN 1
                        WHEN s.direction='sell' AND s.ret_20<0 THEN 1
                        ELSE 0
                    END)*100, 1) as win_rate_20,
                    ROUND(AVG(s.ret_20), 2) as avg_ret20
                FROM {fqn} s
                JOIN my_stock.stock_basic b ON s.ts_code = b.ts_code COLLATE utf8mb4_general_ci
                WHERE s.freq = 'weekly'
                  AND s.ret_20 IS NOT NULL
                  AND s.signal_name IN ({placeholders})
                GROUP BY s.signal_name, s.direction, b.industry
                HAVING cnt >= 20
            """)
            df = pd.read_sql(sql, conn)
            if not df.empty:
                df["indicator"] = tbl["indicator"]
                industry_stats.append(df)
        if industry_stats:
            stats["industry_stats"] = pd.concat(industry_stats, ignore_index=True)

    elapsed = round(time.time() - t0, 1)
    log.info(f"[stats] 统计完成 | 耗时: {elapsed}s")
    return stats


# ─── 控制台输出 ─────────────────────────────────────────────────────────────────

def print_scan_report(df: pd.DataFrame, title: str = "选股扫描结果"):
    """打印扫描结果表格"""
    if df.empty:
        print(f"\n{title}: 无信号")
        return

    df = enrich_with_stock_info(df)
    print(f"\n{'='*80}")
    print(f" {title}")
    print(f"{'='*80}")

    # 按等级分组显示
    for grade in ["S", "A", "B"]:
        sub = df[df["grade"] == grade]
        if sub.empty:
            continue

        grade_label = {"S": "S级(最强)", "A": "A级(强)", "B": "B级(辅助)"}
        print(f"\n--- {grade_label.get(grade, grade)} [{len(sub)}条] ---")
        print(f"{'股票':12s} {'名称':8s} {'日期':10s} {'周期':6s} {'指标':6s} "
              f"{'信号':30s} {'方向':4s} {'收盘':>8s} {'T+20收益':>8s}")
        print("-" * 100)

        for _, r in sub.head(50).iterrows():
            name = str(r.get("name", ""))[:8]
            ret20 = f"{r['ret_20']:+.1f}%" if pd.notna(r.get("ret_20")) else "N/A"
            print(f"{r['ts_code']:12s} {name:8s} {r['trade_date']:10s} "
                  f"{r['freq']:6s} {r['indicator']:6s} "
                  f"{r['signal_name']:30s} {r['direction'] or '':4s} "
                  f"{r['close']:>8.2f} {ret20:>8s}")

    # 汇总
    print(f"\n总计: {len(df)}条信号 | "
          f"S:{len(df[df['grade']=='S'])} A:{len(df[df['grade']=='A'])} "
          f"B:{len(df[df['grade']=='B'])} C:{len(df[df['grade']=='C'])}")


def print_resonance_report(df: pd.DataFrame):
    """打印共振分析结果"""
    if df.empty:
        print("\n共振分析: 无DIF极值事件")
        return

    df = enrich_with_stock_info(df)
    print(f"\n{'='*80}")
    print(f" 共振分析报告 (DIF极值 ± 窗口期)")
    print(f"{'='*80}")

    for direction in ["buy", "sell"]:
        sub = df[df["anchor_direction"] == direction]
        if sub.empty:
            continue
        label = "买入" if direction == "buy" else "卖出"
        print(f"\n=== {label}信号 ({len(sub)}个DIF极值) ===")
        print(f"{'股票':12s} {'名称':8s} {'日期':10s} {'共振数':>6s} "
              f"{'评分':>4s} {'共振指标':15s} {'T+20':>7s} {'T+60':>7s}")
        print("-" * 90)

        for _, r in sub.head(30).iterrows():
            name = str(r.get("name", ""))[:8]
            ret20 = f"{r['ret_20']:+.1f}%" if pd.notna(r.get("ret_20")) else "N/A"
            ret60 = f"{r['ret_60']:+.1f}%" if pd.notna(r.get("ret_60")) else "N/A"
            print(f"{r['ts_code']:12s} {name:8s} {r['trade_date']:10s} "
                  f"{r['resonance_count']:>6d} {r['resonance_score']:>4d} "
                  f"{r['resonance_indicators']:15s} {ret20:>7s} {ret60:>7s}")

    # 共振vs无共振统计
    buy = df[df["anchor_direction"] == "buy"]
    sell = df[df["anchor_direction"] == "sell"]
    if not buy.empty:
        with_res = buy[buy["resonance_count"] > 0]
        no_res = buy[buy["resonance_count"] == 0]
        print(f"\n买入侧统计:")
        if not with_res.empty:
            print(f"  有共振: n={len(with_res)}, "
                  f"avg_ret20={with_res['ret_20'].mean():+.2f}%, "
                  f"avg_ret60={with_res['ret_60'].mean():+.2f}%")
        if not no_res.empty:
            print(f"  无共振: n={len(no_res)}, "
                  f"avg_ret20={no_res['ret_20'].mean():+.2f}%, "
                  f"avg_ret60={no_res['ret_60'].mean():+.2f}%")
    if not sell.empty:
        with_res = sell[sell["resonance_count"] > 0]
        no_res = sell[sell["resonance_count"] == 0]
        print(f"\n卖出侧统计:")
        if not with_res.empty:
            print(f"  有共振: n={len(with_res)}, "
                  f"avg_ret20={with_res['ret_20'].mean():+.2f}%, "
                  f"avg_ret60={with_res['ret_60'].mean():+.2f}%")
        if not no_res.empty:
            print(f"  无共振: n={len(no_res)}, "
                  f"avg_ret20={no_res['ret_20'].mean():+.2f}%, "
                  f"avg_ret60={no_res['ret_60'].mean():+.2f}%")


# ─── CLI ───────────────────────────────────────────────────────────────────────

def cmd_scan(args):
    """扫描信号"""
    if args.date:
        date = args.date
        title = f"选股扫描 [{date}]"
        df = query_signals(date=date, freq=args.freq, min_grade=args.grade,
                           direction=args.direction)
    elif args.days:
        latest = get_latest_signal_date()
        if not latest:
            print("信号表无数据")
            return
        dt = datetime.strptime(latest, "%Y%m%d")
        start = (dt - timedelta(days=args.days * 2)).strftime("%Y%m%d")
        title = f"选股扫描 [近{args.days}天, {start}~{latest}]"
        df = query_signals(date_start=start, date_end=latest,
                           freq=args.freq, min_grade=args.grade,
                           direction=args.direction)
    else:
        # 默认: 最新交易日
        date = get_latest_signal_date()
        if not date:
            print("信号表无数据")
            return
        title = f"选股扫描 [{date}]"
        df = query_signals(date=date, freq=args.freq, min_grade=args.grade,
                           direction=args.direction)

    # 默认不显示C级
    if not args.grade:
        df = df[df["grade"] != "C"]

    print_scan_report(df, title)


def cmd_stock(args):
    """单股信号查询"""
    ts_code = args.code
    if not ts_code.endswith((".SZ", ".SH", ".BJ")):
        # 尝试补全
        ts_code = ts_code + ".SZ"

    if args.days:
        latest = get_latest_signal_date()
        dt = datetime.strptime(latest, "%Y%m%d")
        start = (dt - timedelta(days=args.days * 2)).strftime("%Y%m%d")
        df = query_signals(date_start=start, ts_code=ts_code,
                           freq=args.freq, min_grade=args.grade)
    else:
        df = query_signals(ts_code=ts_code, freq=args.freq,
                           min_grade=args.grade)

    # 默认不显示C级
    if not args.grade:
        df = df[df["grade"] != "C"]

    print_scan_report(df, f"个股信号 [{ts_code}]")


def cmd_resonance(args):
    """共振分析"""
    if args.date:
        df = detect_resonance(date=args.date, freq=args.freq,
                              window_days=args.window)
    elif args.days:
        latest = get_latest_signal_date()
        dt = datetime.strptime(latest, "%Y%m%d")
        start = (dt - timedelta(days=args.days * 2)).strftime("%Y%m%d")
        df = detect_resonance(date_start=start, date_end=latest,
                              freq=args.freq, window_days=args.window)
    else:
        # 默认最新交易日
        date = get_latest_signal_date()
        df = detect_resonance(date=date, freq=args.freq,
                              window_days=args.window)

    print_resonance_report(df)


def cmd_stats(args):
    """统计分析"""
    stats = generate_stats()

    # 输出关键统计
    ss = stats["signal_stats"]
    weekly = ss[ss["freq"] == "weekly"].copy()
    weekly["grade"] = weekly["signal_name"].map(SIGNAL_GRADE).fillna("C")
    weekly.sort_values(["grade", "win_rate_20"], ascending=[True, False], inplace=True)

    print(f"\n{'='*100}")
    print(f" 信号统计报告 (周线)")
    print(f"{'='*100}")
    print(f"{'指标':6s} {'等级':4s} {'信号':35s} {'方向':5s} {'样本':>8s} "
          f"{'胜率20':>7s} {'ret5':>7s} {'ret20':>7s} {'ret60':>8s} {'std20':>7s}")
    print("-" * 100)

    for _, r in weekly.iterrows():
        grade = SIGNAL_GRADE.get(r["signal_name"], "C")
        if grade == "C" and not args.all:
            continue
        print(f"{r['indicator']:6s} {grade:4s} {r['signal_name']:35s} "
              f"{r['direction'] or '':5s} {r['cnt']:>8,} "
              f"{r['win_rate_20']:>6.1f}% {r['avg_ret5']:>+6.2f} "
              f"{r['avg_ret20']:>+6.2f} {r['avg_ret60']:>+7.2f} {r['std_ret20']:>7.2f}")

    # MACD锚点共振统计
    macd_df = stats.get("macd_anchors")
    if macd_df is not None and not macd_df.empty:
        print(f"\n--- MACD DIF极值 (周线, 近5年) ---")
        for sig in ["dif_trough", "dif_peak"]:
            sub = macd_df[macd_df["signal_name"] == sig]
            if sub.empty:
                continue
            direction = "buy" if sig == "dif_trough" else "sell"
            if direction == "buy":
                wr = (sub["ret_20"] > 0).mean() * 100
            else:
                wr = (sub["ret_20"] < 0).mean() * 100
            print(f"  {sig:20s} n={len(sub):>6,}  WR20={wr:.1f}%  "
                  f"ret20={sub['ret_20'].mean():+.2f}%  "
                  f"ret60={sub['ret_60'].mean():+.2f}%")

    # 行业差异
    ind_stats = stats.get("industry_stats")
    if ind_stats is not None and not ind_stats.empty:
        print(f"\n--- S/A级信号行业胜率差异 (周线, top/bottom 5) ---")
        for sig_name in ind_stats["signal_name"].unique():
            sub = ind_stats[ind_stats["signal_name"] == sig_name]
            if len(sub) < 5:
                continue
            top5 = sub.nlargest(5, "win_rate_20")
            bot5 = sub.nsmallest(5, "win_rate_20")
            print(f"\n  {sig_name}:")
            print(f"    最强行业: {', '.join(f'{r.industry}({r.win_rate_20}%)' for _, r in top5.iterrows())}")
            print(f"    最弱行业: {', '.join(f'{r.industry}({r.win_rate_20}%)' for _, r in bot5.iterrows())}")

    return stats


# ─── 主入口 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="个股选股扫描程序 — 跨4指标信号查询 + 共振检测"
    )
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # scan
    p_scan = subparsers.add_parser("scan", help="扫描信号")
    p_scan.add_argument("--date", help="日期 (YYYYMMDD)")
    p_scan.add_argument("--days", type=int, help="近N天")
    p_scan.add_argument("--freq", help="周期 (daily/weekly/monthly)")
    p_scan.add_argument("--grade", help="最低等级 (S/A/B/C)")
    p_scan.add_argument("--direction", help="方向 (buy/sell)")

    # stock
    p_stock = subparsers.add_parser("stock", help="单股信号")
    p_stock.add_argument("code", help="股票代码 (如 300750.SZ)")
    p_stock.add_argument("--days", type=int, default=30, help="近N天 (默认30)")
    p_stock.add_argument("--freq", help="周期")
    p_stock.add_argument("--grade", help="最低等级")

    # resonance
    p_res = subparsers.add_parser("resonance", help="共振分析")
    p_res.add_argument("--date", help="日期")
    p_res.add_argument("--days", type=int, help="近N天")
    p_res.add_argument("--freq", default="weekly", help="周期 (默认weekly)")
    p_res.add_argument("--window", type=int, default=14, help="共振窗口天数 (默认14)")

    # stats
    p_stats = subparsers.add_parser("stats", help="统计分析(报告用)")
    p_stats.add_argument("--all", action="store_true", help="显示所有等级")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    cmd_map = {
        "scan": cmd_scan,
        "stock": cmd_stock,
        "resonance": cmd_resonance,
        "stats": cmd_stats,
    }
    cmd_map[args.command](args)


if __name__ == "__main__":
    main()
