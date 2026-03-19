"""DIF极值实时检测方法对比研究

核心问题:
  当前DIF极值检测使用前后窗滑动窗口(order=20)，需要20根未来K线确认。
  日线需等1个月、周线需等4个月才能确认极值——完全无法用于实时交易。
  前期研究的高胜率结论因此缺乏实操价值。

研究目标:
  对比多种仅使用历史数据的实时检测方法，评估其:
  1. 召回率(Recall) — 能捕捉到多少真实极值
  2. 精确率(Precision) — 检测信号中真正是极值的比例
  3. 检测延迟(Lag) — 比真实极值晚几根K线确认
  4. 可执行收益 — 从确认点出发的实际收益（vs Oracle的理论收益）

候选方法:
  M1: DIF-DEA交叉确认法 — 死叉确认峰值，金叉确认谷值（最经典）
  M2: MACD柱缩减2根 — 柱状图连续2根缩短（较早检测）
  M3: MACD柱缩减3根 — 柱状图连续3根缩短（M2保守版）
  M4: DIF反转2根 — DIF方向连续2根反转（最早检测）
  M5: DIF反转3根 — DIF方向连续3根反转（M4保守版）

数据模式:
  --mode docker   使用本地Docker MySQL（stock_research库预计算MACD）
  --mode remote   使用腾讯云MySQL（my-stock库原始K线，实时计算MACD）

三层级分析:
  --level index     大盘指数（仅docker模式）
  --level industry  行业指数（仅docker模式）
  --level stock     个股（两种模式均可）

用法:
  python analyze_dif_realtime_detection.py --level stock --mode remote
  python analyze_dif_realtime_detection.py --level index --mode docker
  python analyze_dif_realtime_detection.py --level stock --mode remote --limit 100  # 限制股票数
"""
import argparse
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# 根据模式选择导入（延迟导入，在main中根据mode决定）
write_engine = None
read_engine = None
remote_engine = None


def init_docker_engines():
    """初始化Docker模式数据库引擎（stock_research + my_stock）"""
    global write_engine, read_engine
    from database import write_engine as we, read_engine as re
    write_engine = we
    read_engine = re


def init_remote_engine():
    """初始化远程模式数据库引擎（腾讯云 my-stock）"""
    global remote_engine
    port = int(os.getenv('REMOTE_MYSQL_PORT', '3310'))
    host = os.getenv('REMOTE_MYSQL_HOST', '127.0.0.1')
    user = os.getenv('REMOTE_MYSQL_USER', 'root')
    password = os.getenv('REMOTE_MYSQL_PASSWORD', 'root')
    uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/my-stock?charset=utf8mb4"
    remote_engine = create_engine(uri, pool_pre_ping=True, pool_recycle=3600,
                                  pool_size=10, max_overflow=20, echo=False)


# MACD计算（内联，避免对database模块的硬依赖）
def _calc_macd(close_series, short=12, long=26, signal=9):
    """计算MACD指标"""
    ema_short = close_series.ewm(span=short, adjust=False).mean()
    ema_long = close_series.ewm(span=long, adjust=False).mean()
    dif = ema_short - ema_long
    dea = dif.ewm(span=signal, adjust=False).mean()
    macd_val = (dif - dea) * 2
    return pd.DataFrame({"dif": dif.round(4), "dea": dea.round(4), "macd": macd_val.round(4)})


# 极值检测函数（内联，避免对signal_detector模块的硬依赖）
def _find_local_peaks(values, order):
    """找局部极大值点索引"""
    peaks = []
    for i in range(order, len(values) - order):
        window = values[i - order: i + order + 1]
        if not np.isnan(values[i]) and values[i] == np.nanmax(window):
            peaks.append(i)
    return peaks


def _find_local_troughs(values, order):
    """找局部极小值点索引"""
    troughs = []
    for i in range(order, len(values) - order):
        window = values[i - order: i + order + 1]
        if not np.isnan(values[i]) and values[i] == np.nanmin(window):
            troughs.append(i)
    return troughs


FREQ_ORDER = {"daily": 20, "weekly": 8, "monthly": 3, "yearly": 1}


def tag_trend(trade_date):
    """返回日期对应的趋势标签（简化版，数据不足时标unknown）"""
    try:
        from bull_bear_phases import tag_trend as _tag
        return _tag(trade_date)
    except ImportError:
        return "unknown"


# ── 常量定义 ──────────────────────────────────────────────────

FREQS = ["daily", "weekly"]  # 月线样本太少，暂不分析
FREQ_NAMES = {"daily": "日线", "weekly": "周线"}

# 前瞻收益窗口
RETURN_HORIZONS = {
    "daily":   [1, 3, 5, 10, 20, 60],
    "weekly":  [1, 2, 4, 8, 13, 26],
}

HORIZON_LABELS = {
    "daily":   {1: "T+1", 3: "T+3", 5: "T+5", 10: "T+10", 20: "T+20", 60: "T+60"},
    "weekly":  {1: "T+1w", 2: "T+2w", 4: "T+4w", 8: "T+8w", 13: "T+13w", 26: "T+26w"},
}

# 匹配容忍窗口（单侧K线数，检测点允许在oracle极值之后多少根K线内匹配）
MATCH_TOLERANCE = {
    "daily": 40,   # 约2个月
    "weekly": 16,  # 约4个月
}

# 7大指数
INDEX_CODES = [
    ("000001.SH", "上证指数"),
    ("399001.SZ", "深证成指"),
    ("399006.SZ", "创业板指"),
    ("000016.SH", "上证50"),
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("000852.SH", "中证1000"),
]

# 申万一级行业5大类别
SW_CATEGORIES = {
    "上游资源": ["801010.SI", "801050.SI", "801950.SI", "801960.SI"],
    "中游制造": ["801040.SI", "801030.SI", "801730.SI", "801890.SI", "801740.SI",
                  "801880.SI", "801710.SI", "801720.SI", "801140.SI"],
    "下游消费": ["801120.SI", "801150.SI", "801110.SI", "801130.SI", "801170.SI",
                  "801200.SI", "801210.SI", "801980.SI"],
    "TMT": ["801750.SI", "801080.SI", "801760.SI", "801770.SI"],
    "金融地产+公用": ["801780.SI", "801790.SI", "801180.SI", "801160.SI",
                       "801970.SI", "801230.SI"],
}

START_DATE = "20160101"

# 方法名称映射
METHOD_NAMES = {
    "M1_cross": "DIF-DEA交叉确认",
    "M2_hist2": "MACD柱缩减2根",
    "M3_hist3": "MACD柱缩减3根",
    "M4_rev2": "DIF反转2根",
    "M5_rev3": "DIF反转3根",
}


# ══════════════════════════════════════════════════════════════
# Part 1: 数据加载
# ══════════════════════════════════════════════════════════════

# --- Docker模式数据加载 ---

def load_index_macd(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 加载指数 MACD 数据（docker模式）"""
    table = f"index_macd_{freq}"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def load_sw_daily(ts_code: str) -> pd.DataFrame:
    """从 my_stock.sw_daily 加载申万行业日线（docker模式），并计算 MACD"""
    sql = text(
        "SELECT trade_date, open, high, low, close, vol, pct_change as pct_chg "
        "FROM sw_daily WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=result.keys())

    macd_df = _calc_macd(df["close"])
    for col in ["dif", "dea", "macd"]:
        df[col] = macd_df[col].values
    return df


def load_stock_macd_docker(ts_code: str, freq: str) -> pd.DataFrame:
    """从 stock_research 加载个股前复权 MACD 数据（docker模式）"""
    table = f"stock_macd_{freq}_qfq"
    sql = text(
        f"SELECT trade_date, open, high, low, close, vol, pct_chg, dif, dea, macd "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )
    with write_engine.connect() as conn:
        result = conn.execute(sql, {"ts_code": ts_code})
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=result.keys())


def get_all_stock_codes_docker() -> list[str]:
    """获取全市场在市股票代码（docker模式）"""
    sql = text("SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code")
    with read_engine.connect() as conn:
        result = conn.execute(sql)
        return [r[0] for r in result]


def get_sw_industry_name(ts_code: str) -> str:
    """获取申万行业名称"""
    sql = text(
        "SELECT industry_name FROM index_classify "
        "WHERE index_code = :code AND level = 'L1' AND src = 'SW2021'"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql, {"code": ts_code})
        row = result.fetchone()
        return row[0] if row else ts_code


# --- Remote模式数据加载（腾讯云 my-stock） ---

def load_stock_macd_remote(ts_code: str, freq: str = "daily") -> pd.DataFrame:
    """从腾讯云 my-stock 加载原始K线并计算MACD（remote模式）

    使用前复权(close_qfq)计算MACD
    trade_date转为YYYYMMDD字符串以兼容分析函数
    含重试逻辑，处理SSH隧道连接中断
    """
    global remote_engine
    table_map = {"daily": "stock_daily", "weekly": "stock_weekly", "monthly": "stock_monthly"}
    table = table_map.get(freq)
    if not table:
        return pd.DataFrame()

    sql = text(
        f"SELECT trade_date, open_qfq as `open`, high_qfq as high, low_qfq as low, "
        f"close_qfq as close, vol, pct_chg "
        f"FROM `{table}` WHERE ts_code = :ts_code ORDER BY trade_date"
    )

    for attempt in range(3):
        try:
            with remote_engine.connect() as conn:
                result = conn.execute(sql, {"ts_code": ts_code})
                rows = result.fetchall()
                if not rows:
                    return pd.DataFrame()
                df = pd.DataFrame(rows, columns=result.keys())
            break
        except Exception as e:
            if attempt < 2:
                import time as _time
                _time.sleep(2)
                # 重建引擎
                remote_engine.dispose()
                continue
            else:
                print(f"\n    警告: {ts_code} 连接失败3次，跳过: {e}")
                return pd.DataFrame()

    # trade_date: date对象 → YYYYMMDD字符串
    df["trade_date"] = df["trade_date"].apply(lambda d: d.strftime("%Y%m%d"))

    # 转为float
    for col in ["open", "high", "low", "close", "vol", "pct_chg"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 过滤无效数据
    df = df.dropna(subset=["close"])
    if len(df) < 60:
        return pd.DataFrame()

    # 计算MACD
    macd_df = _calc_macd(df["close"])
    for col in ["dif", "dea", "macd"]:
        df[col] = macd_df[col].values

    return df.reset_index(drop=True)


def get_all_stock_codes_remote() -> list[str]:
    """获取全市场在市股票代码（remote模式）"""
    sql = text("SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code")
    with remote_engine.connect() as conn:
        result = conn.execute(sql)
        return [r[0] for r in result]


# --- 统一接口 ---

DATA_MODE = "docker"  # 全局变量，在main中设置


def load_stock_data(ts_code: str, freq: str = "daily") -> pd.DataFrame:
    """统一的股票数据加载接口"""
    if DATA_MODE == "remote":
        return load_stock_macd_remote(ts_code, freq)
    else:
        return load_stock_macd_docker(ts_code, freq)


def get_all_stock_codes() -> list[str]:
    """统一的股票代码获取接口"""
    if DATA_MODE == "remote":
        return get_all_stock_codes_remote()
    else:
        return get_all_stock_codes_docker()


# ══════════════════════════════════════════════════════════════
# Part 2: Oracle极值检测（需要未来数据的基准方法）
# ══════════════════════════════════════════════════════════════

def detect_oracle_extremes(df: pd.DataFrame, freq: str) -> list[dict]:
    """用前后窗滑动窗口检测DIF极值（基准/上帝视角）

    返回列表，每项包含:
      type: 'peak' 或 'trough'
      idx: 极值在df中的索引
      trade_date: 日期
      dif: DIF值
      close: 收盘价
      trend: 牛/熊标签
    """
    if df.empty or len(df) < 10:
        return []

    order = FREQ_ORDER.get(freq, 20)
    dif_values = df["dif"].values

    peak_indices = _find_local_peaks(dif_values, order)
    trough_indices = _find_local_troughs(dif_values, order)

    results = []
    for idx in peak_indices:
        row = df.iloc[idx]
        if row["trade_date"] < START_DATE:
            continue
        results.append({
            "type": "peak",
            "idx": idx,
            "trade_date": row["trade_date"],
            "dif": float(row["dif"]),
            "close": float(row["close"]),
            "trend": tag_trend(row["trade_date"]),
        })

    for idx in trough_indices:
        row = df.iloc[idx]
        if row["trade_date"] < START_DATE:
            continue
        results.append({
            "type": "trough",
            "idx": idx,
            "trade_date": row["trade_date"],
            "dif": float(row["dif"]),
            "close": float(row["close"]),
            "trend": tag_trend(row["trade_date"]),
        })

    results.sort(key=lambda x: x["idx"])
    return results


# ══════════════════════════════════════════════════════════════
# Part 3: 实时检测方法（仅使用历史数据）
# ══════════════════════════════════════════════════════════════

def detect_m1_cross(df: pd.DataFrame) -> list[dict]:
    """M1: DIF-DEA交叉确认法

    原理:
      - 死叉发生时，确认此前DIF>DEA区间内的DIF最大值为峰值
      - 金叉发生时，确认此前DIF<DEA区间内的DIF最小值为谷值

    优势: 最经典，逻辑简单，信号明确
    劣势: 延迟较大（需等到DIF与DEA交叉）
    """
    dif = df["dif"].values
    dea = df["dea"].values
    close = df["close"].values
    n = len(df)
    results = []

    # 追踪当前所在的DIF-DEA关系区间
    seg_start = 0  # 区间起始索引
    was_above = dif[0] > dea[0]  # DIF是否在DEA上方

    for i in range(1, n):
        is_above = dif[i] > dea[i]

        if was_above and not is_above:
            # 死叉: DIF从上方穿越到下方 → 确认峰值
            if seg_start < i:
                seg_dif = dif[seg_start:i]
                peak_local = np.argmax(seg_dif)
                peak_idx = seg_start + peak_local
                td = df.iloc[peak_idx]["trade_date"]
                if td >= START_DATE:
                    results.append({
                        "type": "peak",
                        "extreme_idx": peak_idx,
                        "detect_idx": i,
                        "trade_date": td,
                        "detect_date": df.iloc[i]["trade_date"],
                        "dif": float(dif[peak_idx]),
                        "close_at_extreme": float(close[peak_idx]),
                        "close_at_detect": float(close[i]),
                        "trend": tag_trend(td),
                    })
            seg_start = i

        elif not was_above and is_above:
            # 金叉: DIF从下方穿越到上方 → 确认谷值
            if seg_start < i:
                seg_dif = dif[seg_start:i]
                trough_local = np.argmin(seg_dif)
                trough_idx = seg_start + trough_local
                td = df.iloc[trough_idx]["trade_date"]
                if td >= START_DATE:
                    results.append({
                        "type": "trough",
                        "extreme_idx": trough_idx,
                        "detect_idx": i,
                        "trade_date": td,
                        "detect_date": df.iloc[i]["trade_date"],
                        "dif": float(dif[trough_idx]),
                        "close_at_extreme": float(close[trough_idx]),
                        "close_at_detect": float(close[i]),
                        "trend": tag_trend(td),
                    })
            seg_start = i

        was_above = is_above

    return results


def detect_m2m3_histogram(df: pd.DataFrame, n_shrink: int = 2) -> list[dict]:
    """M2/M3: MACD柱连续缩减确认法

    原理:
      - MACD柱(>0)连续n_shrink根缩短 → DIF峰值确认
      - MACD柱(<0)连续n_shrink根增长(绝对值缩小) → DIF谷值确认

    参数:
      n_shrink=2 → M2(较敏感), n_shrink=3 → M3(较保守)

    极值位置: 检测点回溯，在当前正/负区间内的DIF最大/最小值
    """
    dif = df["dif"].values
    dea = df["dea"].values
    macd = df["macd"].values
    close = df["close"].values
    n = len(df)
    results = []

    # 追踪连续缩减计数
    shrink_count = 0  # 正向柱缩减（峰值候选）
    grow_count = 0    # 负向柱缩减（谷值候选）

    # 追踪当前正/负区间起始
    pos_seg_start = None  # 正MACD区间起始
    neg_seg_start = None  # 负MACD区间起始

    # 记录已确认的极值，避免同一区间重复确认
    last_peak_seg_start = -1
    last_trough_seg_start = -1

    for i in range(1, n):
        # 追踪正/负区间
        if macd[i] > 0 and (pos_seg_start is None or macd[i - 1] <= 0):
            pos_seg_start = i
            shrink_count = 0
        if macd[i] < 0 and (neg_seg_start is None or macd[i - 1] >= 0):
            neg_seg_start = i
            grow_count = 0

        # 正向柱缩减检测（峰值）
        if macd[i] > 0 and macd[i - 1] > 0:
            if macd[i] < macd[i - 1]:
                shrink_count += 1
            else:
                shrink_count = 0

            if shrink_count >= n_shrink and pos_seg_start is not None and pos_seg_start != last_peak_seg_start:
                # 在当前正区间内找DIF最大值
                seg_dif = dif[pos_seg_start:i + 1]
                peak_local = np.argmax(seg_dif)
                peak_idx = pos_seg_start + peak_local
                td = df.iloc[peak_idx]["trade_date"]
                if td >= START_DATE:
                    results.append({
                        "type": "peak",
                        "extreme_idx": peak_idx,
                        "detect_idx": i,
                        "trade_date": td,
                        "detect_date": df.iloc[i]["trade_date"],
                        "dif": float(dif[peak_idx]),
                        "close_at_extreme": float(close[peak_idx]),
                        "close_at_detect": float(close[i]),
                        "trend": tag_trend(td),
                    })
                last_peak_seg_start = pos_seg_start
        else:
            shrink_count = 0

        # 负向柱缩减检测（谷值）
        if macd[i] < 0 and macd[i - 1] < 0:
            # 绝对值缩小 = 值在增大（向零靠近）
            if macd[i] > macd[i - 1]:
                grow_count += 1
            else:
                grow_count = 0

            if grow_count >= n_shrink and neg_seg_start is not None and neg_seg_start != last_trough_seg_start:
                # 在当前负区间内找DIF最小值
                seg_dif = dif[neg_seg_start:i + 1]
                trough_local = np.argmin(seg_dif)
                trough_idx = neg_seg_start + trough_local
                td = df.iloc[trough_idx]["trade_date"]
                if td >= START_DATE:
                    results.append({
                        "type": "trough",
                        "extreme_idx": trough_idx,
                        "detect_idx": i,
                        "trade_date": td,
                        "detect_date": df.iloc[i]["trade_date"],
                        "dif": float(dif[trough_idx]),
                        "close_at_extreme": float(close[trough_idx]),
                        "close_at_detect": float(close[i]),
                        "trend": tag_trend(td),
                    })
                last_trough_seg_start = neg_seg_start
        else:
            grow_count = 0

    return results


def detect_m4m5_reversal(df: pd.DataFrame, n_bars: int = 2) -> list[dict]:
    """M4/M5: DIF连续反转确认法

    原理:
      - DIF连续n_bars根下降（且之前至少有1根上升）→ 确认峰值
      - DIF连续n_bars根上升（且之前至少有1根下降）→ 确认谷值

    参数:
      n_bars=2 → M4(最灵敏), n_bars=3 → M5(较保守)

    极值位置: 第一根反转之前那根K线
    """
    dif = df["dif"].values
    close = df["close"].values
    n = len(df)
    results = []

    # 计算DIF逐根变化方向
    # direction[i] = 1: 上升, -1: 下降, 0: 持平
    direction = np.zeros(n, dtype=int)
    for i in range(1, n):
        if dif[i] > dif[i - 1]:
            direction[i] = 1
        elif dif[i] < dif[i - 1]:
            direction[i] = -1

    # 扫描连续反转
    for i in range(n_bars + 1, n):
        # 检查连续n_bars根下降
        all_down = all(direction[i - j] == -1 for j in range(n_bars))
        if all_down:
            # 检查之前至少有1根上升（确认是从上升转为下降）
            has_prior_up = False
            for k in range(i - n_bars, max(0, i - n_bars - 5), -1):
                if direction[k] == 1:
                    has_prior_up = True
                    break
            if has_prior_up:
                # 峰值在第一根下降之前: i - n_bars
                peak_idx = i - n_bars
                td = df.iloc[peak_idx]["trade_date"]
                if td >= START_DATE:
                    # 避免同一个峰重复报告（如果前一根也触发了）
                    if not results or results[-1].get("type") != "peak" or results[-1]["extreme_idx"] != peak_idx:
                        results.append({
                            "type": "peak",
                            "extreme_idx": peak_idx,
                            "detect_idx": i,
                            "trade_date": td,
                            "detect_date": df.iloc[i]["trade_date"],
                            "dif": float(dif[peak_idx]),
                            "close_at_extreme": float(close[peak_idx]),
                            "close_at_detect": float(close[i]),
                            "trend": tag_trend(td),
                        })

        # 检查连续n_bars根上升
        all_up = all(direction[i - j] == 1 for j in range(n_bars))
        if all_up:
            has_prior_down = False
            for k in range(i - n_bars, max(0, i - n_bars - 5), -1):
                if direction[k] == -1:
                    has_prior_down = True
                    break
            if has_prior_down:
                # 谷值在第一根上升之前: i - n_bars
                trough_idx = i - n_bars
                td = df.iloc[trough_idx]["trade_date"]
                if td >= START_DATE:
                    if not results or results[-1].get("type") != "trough" or results[-1]["extreme_idx"] != trough_idx:
                        results.append({
                            "type": "trough",
                            "extreme_idx": trough_idx,
                            "detect_idx": i,
                            "trade_date": td,
                            "detect_date": df.iloc[i]["trade_date"],
                            "dif": float(dif[trough_idx]),
                            "close_at_extreme": float(close[trough_idx]),
                            "close_at_detect": float(close[i]),
                            "trend": tag_trend(td),
                        })

    return results


# 方法分发表
METHODS = {
    "M1_cross": lambda df: detect_m1_cross(df),
    "M2_hist2": lambda df: detect_m2m3_histogram(df, n_shrink=2),
    "M3_hist3": lambda df: detect_m2m3_histogram(df, n_shrink=3),
    "M4_rev2": lambda df: detect_m4m5_reversal(df, n_bars=2),
    "M5_rev3": lambda df: detect_m4m5_reversal(df, n_bars=3),
}


# ══════════════════════════════════════════════════════════════
# Part 4: 匹配与评估
# ══════════════════════════════════════════════════════════════

def match_and_evaluate(oracle: list[dict], detected: list[dict],
                       df: pd.DataFrame, freq: str) -> dict:
    """将实时检测结果与Oracle匹配，计算评估指标

    匹配规则:
      对每个Oracle极值，找时间上最近的同类型检测结果
      检测点(detect_idx)必须 >= Oracle极值点(idx) 且 <= idx + tolerance
      每个Oracle极值最多匹配一个检测结果

    返回指标:
      recall: 被检测到的Oracle极值比例
      precision: 检测结果中真正匹配Oracle的比例
      lag_mean/median/p25/p75: 检测延迟统计
      oracle_returns: Oracle点的前瞻收益
      detect_returns: 检测点的前瞻收益
    """
    tolerance = MATCH_TOLERANCE[freq]
    horizons = RETURN_HORIZONS[freq]
    labels = HORIZON_LABELS[freq]
    close = df["close"].values
    n = len(close)

    # 按类型分组
    result = {}
    for ext_type in ["peak", "trough"]:
        oracle_of_type = [o for o in oracle if o["type"] == ext_type]
        detected_of_type = [d for d in detected if d["type"] == ext_type]

        if not oracle_of_type:
            continue

        # 贪心匹配: 按Oracle索引顺序，为每个Oracle找最近的未匹配检测
        matched_oracle = set()
        matched_detect = set()
        matches = []

        for oi, orc in enumerate(oracle_of_type):
            best_di = None
            best_lag = float("inf")
            for di, det in enumerate(detected_of_type):
                if di in matched_detect:
                    continue
                lag = det["detect_idx"] - orc["idx"]
                # 检测点必须在Oracle之后(或同时)，且在容忍窗口内
                if 0 <= lag <= tolerance:
                    if lag < best_lag:
                        best_lag = lag
                        best_di = di
            if best_di is not None:
                matched_oracle.add(oi)
                matched_detect.add(best_di)
                matches.append({
                    "oracle_idx": orc["idx"],
                    "detect_idx": detected_of_type[best_di]["detect_idx"],
                    "extreme_idx": detected_of_type[best_di]["extreme_idx"],
                    "lag": best_lag,
                    "trend": orc["trend"],
                })

        n_oracle = len(oracle_of_type)
        n_detect = len(detected_of_type)
        n_match = len(matches)

        recall = n_match / n_oracle if n_oracle > 0 else 0
        precision = n_match / n_detect if n_detect > 0 else 0

        # 延迟统计
        lags = [m["lag"] for m in matches]
        lag_stats = {}
        if lags:
            lag_stats = {
                "mean": np.mean(lags),
                "median": np.median(lags),
                "p25": np.percentile(lags, 25),
                "p75": np.percentile(lags, 75),
            }

        # 前瞻收益: Oracle点 vs 检测点
        oracle_returns = {h: [] for h in horizons}
        detect_returns = {h: [] for h in horizons}
        oracle_bull_returns = {h: [] for h in horizons}
        oracle_bear_returns = {h: [] for h in horizons}
        detect_bull_returns = {h: [] for h in horizons}
        detect_bear_returns = {h: [] for h in horizons}

        for m in matches:
            o_idx = m["oracle_idx"]
            d_idx = m["detect_idx"]
            trend = m["trend"]

            for h in horizons:
                # Oracle收益
                if o_idx + h < n:
                    ret = (close[o_idx + h] - close[o_idx]) / close[o_idx] * 100
                    oracle_returns[h].append(ret)
                    if trend == "bull":
                        oracle_bull_returns[h].append(ret)
                    elif trend == "bear":
                        oracle_bear_returns[h].append(ret)

                # 检测点收益
                if d_idx + h < n:
                    ret = (close[d_idx + h] - close[d_idx]) / close[d_idx] * 100
                    detect_returns[h].append(ret)
                    if trend == "bull":
                        detect_bull_returns[h].append(ret)
                    elif trend == "bear":
                        detect_bear_returns[h].append(ret)

        # 计算胜率和均值
        def _winrate_and_mean(returns_dict, ext_type):
            """计算各窗口胜率和均值
            峰值: 价格下跌为胜 (ret < 0)
            谷值: 价格上涨为胜 (ret > 0)
            """
            stats = {}
            for h, rets in returns_dict.items():
                if not rets:
                    stats[h] = {"winrate": None, "mean": None, "n": 0}
                    continue
                arr = np.array(rets)
                if ext_type == "peak":
                    wins = np.sum(arr < 0)
                else:
                    wins = np.sum(arr > 0)
                stats[h] = {
                    "winrate": wins / len(arr) * 100,
                    "mean": np.mean(arr),
                    "n": len(arr),
                }
            return stats

        result[ext_type] = {
            "n_oracle": n_oracle,
            "n_detect": n_detect,
            "n_match": n_match,
            "recall": recall * 100,
            "precision": precision * 100,
            "lag": lag_stats,
            "matches_by_trend": {
                "bull": len([m for m in matches if m["trend"] == "bull"]),
                "bear": len([m for m in matches if m["trend"] == "bear"]),
            },
            "oracle_returns": _winrate_and_mean(oracle_returns, ext_type),
            "detect_returns": _winrate_and_mean(detect_returns, ext_type),
            "oracle_bull_returns": _winrate_and_mean(oracle_bull_returns, ext_type),
            "oracle_bear_returns": _winrate_and_mean(oracle_bear_returns, ext_type),
            "detect_bull_returns": _winrate_and_mean(detect_bull_returns, ext_type),
            "detect_bear_returns": _winrate_and_mean(detect_bear_returns, ext_type),
        }

    return result


# ══════════════════════════════════════════════════════════════
# Part 5: 单标的分析
# ══════════════════════════════════════════════════════════════

def analyze_single(df: pd.DataFrame, freq: str) -> dict:
    """分析单个标的的所有方法

    返回: {method_name: match_and_evaluate结果}
    """
    # 检测Oracle极值
    oracle = detect_oracle_extremes(df, freq)
    if not oracle:
        return {}

    results = {}
    for method_name, method_fn in METHODS.items():
        detected = method_fn(df)
        eval_result = match_and_evaluate(oracle, detected, df, freq)
        results[method_name] = eval_result

    return results


# ══════════════════════════════════════════════════════════════
# Part 6: 汇总统计
# ══════════════════════════════════════════════════════════════

def aggregate_results(all_results: list[dict]) -> dict:
    """汇总多个标的的分析结果

    输入: analyze_single 返回值的列表
    输出: 各方法的汇总统计
    """
    agg = {}

    for method_name in METHODS:
        method_agg = {}
        for ext_type in ["peak", "trough"]:
            total_oracle = 0
            total_detect = 0
            total_match = 0
            all_lags = []
            match_bull = 0
            match_bear = 0

            # 收益汇总
            freq_key = None
            all_oracle_rets = {}
            all_detect_rets = {}
            all_oracle_bull = {}
            all_oracle_bear = {}
            all_detect_bull = {}
            all_detect_bear = {}

            for single in all_results:
                if method_name not in single or ext_type not in single[method_name]:
                    continue
                r = single[method_name][ext_type]
                total_oracle += r["n_oracle"]
                total_detect += r["n_detect"]
                total_match += r["n_match"]
                match_bull += r["matches_by_trend"]["bull"]
                match_bear += r["matches_by_trend"]["bear"]

                # 收集延迟
                if r["lag"]:
                    # 用匹配数近似，延迟中位数等需要原始数据
                    pass

                # 收集收益数据
                for h, stats in r["oracle_returns"].items():
                    if h not in all_oracle_rets:
                        all_oracle_rets[h] = {"wins": 0, "total": 0, "sum": 0}
                    if stats["n"] > 0:
                        # 还原胜负次数
                        n_h = stats["n"]
                        win_n = int(round(stats["winrate"] / 100 * n_h)) if stats["winrate"] is not None else 0
                        all_oracle_rets[h]["wins"] += win_n
                        all_oracle_rets[h]["total"] += n_h
                        all_oracle_rets[h]["sum"] += stats["mean"] * n_h

                for h, stats in r["detect_returns"].items():
                    if h not in all_detect_rets:
                        all_detect_rets[h] = {"wins": 0, "total": 0, "sum": 0}
                    if stats["n"] > 0:
                        n_h = stats["n"]
                        win_n = int(round(stats["winrate"] / 100 * n_h)) if stats["winrate"] is not None else 0
                        all_detect_rets[h]["wins"] += win_n
                        all_detect_rets[h]["total"] += n_h
                        all_detect_rets[h]["sum"] += stats["mean"] * n_h

                # 牛熊分别汇总
                for h, stats in r["oracle_bull_returns"].items():
                    if h not in all_oracle_bull:
                        all_oracle_bull[h] = {"wins": 0, "total": 0, "sum": 0}
                    if stats["n"] > 0:
                        n_h = stats["n"]
                        win_n = int(round(stats["winrate"] / 100 * n_h)) if stats["winrate"] is not None else 0
                        all_oracle_bull[h]["wins"] += win_n
                        all_oracle_bull[h]["total"] += n_h
                        all_oracle_bull[h]["sum"] += stats["mean"] * n_h

                for h, stats in r["oracle_bear_returns"].items():
                    if h not in all_oracle_bear:
                        all_oracle_bear[h] = {"wins": 0, "total": 0, "sum": 0}
                    if stats["n"] > 0:
                        n_h = stats["n"]
                        win_n = int(round(stats["winrate"] / 100 * n_h)) if stats["winrate"] is not None else 0
                        all_oracle_bear[h]["wins"] += win_n
                        all_oracle_bear[h]["total"] += n_h
                        all_oracle_bear[h]["sum"] += stats["mean"] * n_h

                for h, stats in r["detect_bull_returns"].items():
                    if h not in all_detect_bull:
                        all_detect_bull[h] = {"wins": 0, "total": 0, "sum": 0}
                    if stats["n"] > 0:
                        n_h = stats["n"]
                        win_n = int(round(stats["winrate"] / 100 * n_h)) if stats["winrate"] is not None else 0
                        all_detect_bull[h]["wins"] += win_n
                        all_detect_bull[h]["total"] += n_h
                        all_detect_bull[h]["sum"] += stats["mean"] * n_h

                for h, stats in r["detect_bear_returns"].items():
                    if h not in all_detect_bear:
                        all_detect_bear[h] = {"wins": 0, "total": 0, "sum": 0}
                    if stats["n"] > 0:
                        n_h = stats["n"]
                        win_n = int(round(stats["winrate"] / 100 * n_h)) if stats["winrate"] is not None else 0
                        all_detect_bear[h]["wins"] += win_n
                        all_detect_bear[h]["total"] += n_h
                        all_detect_bear[h]["sum"] += stats["mean"] * n_h

            # 汇总
            def _agg_rets(d):
                out = {}
                for h, v in d.items():
                    if v["total"] > 0:
                        out[h] = {
                            "winrate": v["wins"] / v["total"] * 100,
                            "mean": v["sum"] / v["total"],
                            "n": v["total"],
                        }
                    else:
                        out[h] = {"winrate": None, "mean": None, "n": 0}
                return out

            method_agg[ext_type] = {
                "n_oracle": total_oracle,
                "n_detect": total_detect,
                "n_match": total_match,
                "recall": total_match / total_oracle * 100 if total_oracle > 0 else 0,
                "precision": total_match / total_detect * 100 if total_detect > 0 else 0,
                "match_bull": match_bull,
                "match_bear": match_bear,
                "oracle_returns": _agg_rets(all_oracle_rets),
                "detect_returns": _agg_rets(all_detect_rets),
                "oracle_bull_returns": _agg_rets(all_oracle_bull),
                "oracle_bear_returns": _agg_rets(all_oracle_bear),
                "detect_bull_returns": _agg_rets(all_detect_bull),
                "detect_bear_returns": _agg_rets(all_detect_bear),
            }

        agg[method_name] = method_agg

    return agg


# ══════════════════════════════════════════════════════════════
# Part 7: 延迟精细统计（需要原始匹配数据）
# ══════════════════════════════════════════════════════════════

def collect_lag_data(all_results_raw: list[tuple]) -> dict:
    """从原始数据中收集所有方法的延迟分布

    all_results_raw: [(df, freq, oracle, {method: detected}), ...]
    返回: {method: {ext_type: [lags]}}
    """
    lag_data = {m: {"peak": [], "trough": []} for m in METHODS}

    for df, freq, oracle, method_detections in all_results_raw:
        tolerance = MATCH_TOLERANCE[freq]
        for method_name, detected in method_detections.items():
            for ext_type in ["peak", "trough"]:
                oracle_of_type = [o for o in oracle if o["type"] == ext_type]
                detected_of_type = [d for d in detected if d["type"] == ext_type]

                matched_detect = set()
                for orc in oracle_of_type:
                    best_di = None
                    best_lag = float("inf")
                    for di, det in enumerate(detected_of_type):
                        if di in matched_detect:
                            continue
                        lag = det["detect_idx"] - orc["idx"]
                        if 0 <= lag <= tolerance:
                            if lag < best_lag:
                                best_lag = lag
                                best_di = di
                    if best_di is not None:
                        matched_detect.add(best_di)
                        lag_data[method_name][ext_type].append(best_lag)

    return lag_data


# ══════════════════════════════════════════════════════════════
# Part 8: 输出格式化
# ══════════════════════════════════════════════════════════════

def format_comparison_table(agg: dict, freq: str, lag_data: dict = None) -> str:
    """格式化方法对比表"""
    labels = HORIZON_LABELS[freq]
    horizons = RETURN_HORIZONS[freq]
    lines = []

    for ext_type, type_name in [("peak", "DIF峰值(看跌信号)"), ("trough", "DIF谷值(看涨信号)")]:
        lines.append(f"\n### {type_name}\n")

        # 核心指标对比表
        lines.append("| 方法 | Oracle数 | 检测数 | 匹配数 | 召回率 | 精确率 |")
        lines.append("|------|---------|--------|--------|--------|--------|")

        for method, name in METHOD_NAMES.items():
            if ext_type not in agg.get(method, {}):
                continue
            r = agg[method][ext_type]
            lines.append(
                f"| {name} | {r['n_oracle']} | {r['n_detect']} | {r['n_match']} "
                f"| {r['recall']:.1f}% | {r['precision']:.1f}% |"
            )

        # 延迟统计
        if lag_data:
            lines.append(f"\n**检测延迟（K线根数）:**\n")
            lines.append("| 方法 | 均值 | P25 | 中位数 | P75 |")
            lines.append("|------|------|-----|--------|-----|")
            for method, name in METHOD_NAMES.items():
                lags = lag_data.get(method, {}).get(ext_type, [])
                if lags:
                    lines.append(
                        f"| {name} | {np.mean(lags):.1f} | {np.percentile(lags, 25):.0f} "
                        f"| {np.median(lags):.0f} | {np.percentile(lags, 75):.0f} |"
                    )
                else:
                    lines.append(f"| {name} | - | - | - | - |")

        # 胜率对比: Oracle vs 检测点
        lines.append(f"\n**全量胜率对比（Oracle点 vs 检测点）:**\n")
        header = "| 方法 | 视角 |"
        for h in horizons:
            header += f" {labels[h]} |"
        lines.append(header)
        lines.append("|------|------|" + "--------|" * len(horizons))

        for method, name in METHOD_NAMES.items():
            if ext_type not in agg.get(method, {}):
                continue
            r = agg[method][ext_type]

            # Oracle行
            row_o = f"| {name} | Oracle |"
            for h in horizons:
                s = r["oracle_returns"].get(h, {})
                if s and s.get("winrate") is not None:
                    row_o += f" {s['winrate']:.1f}% |"
                else:
                    row_o += " - |"
            lines.append(row_o)

            # 检测点行
            row_d = f"| | 检测点 |"
            for h in horizons:
                s = r["detect_returns"].get(h, {})
                if s and s.get("winrate") is not None:
                    row_d += f" {s['winrate']:.1f}% |"
                else:
                    row_d += " - |"
            lines.append(row_d)

        # 牛熊分别的胜率
        for trend, trend_name in [("bull", "牛市"), ("bear", "熊市")]:
            okey = f"oracle_{trend}_returns"
            dkey = f"detect_{trend}_returns"

            # 检查是否有数据
            has_data = False
            for method in METHOD_NAMES:
                if ext_type in agg.get(method, {}):
                    r = agg[method][ext_type]
                    for h in horizons:
                        if r[okey].get(h, {}).get("n", 0) > 0:
                            has_data = True
                            break
                if has_data:
                    break

            if not has_data:
                continue

            lines.append(f"\n**{trend_name}胜率对比:**\n")
            header = "| 方法 | 视角 |"
            for h in horizons:
                header += f" {labels[h]} |"
            lines.append(header)
            lines.append("|------|------|" + "--------|" * len(horizons))

            for method, name in METHOD_NAMES.items():
                if ext_type not in agg.get(method, {}):
                    continue
                r = agg[method][ext_type]

                row_o = f"| {name} | Oracle |"
                for h in horizons:
                    s = r[okey].get(h, {})
                    if s and s.get("winrate") is not None and s.get("n", 0) > 0:
                        row_o += f" {s['winrate']:.1f}%({s['n']}) |"
                    else:
                        row_o += " - |"
                lines.append(row_o)

                row_d = f"| | 检测点 |"
                for h in horizons:
                    s = r[dkey].get(h, {})
                    if s and s.get("winrate") is not None and s.get("n", 0) > 0:
                        row_d += f" {s['winrate']:.1f}%({s['n']}) |"
                    else:
                        row_d += " - |"
                lines.append(row_d)

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
# Part 9: 层级分析入口
# ══════════════════════════════════════════════════════════════

def analyze_index_level(codes=None):
    """大盘指数层面分析"""
    if codes is None:
        codes = INDEX_CODES

    output_lines = []
    output_lines.append("## 一、大盘指数层面\n")

    for freq in FREQS:
        freq_name = FREQ_NAMES[freq]
        print(f"\n{'='*60}")
        print(f"指数层面 - {freq_name}")
        print(f"{'='*60}")

        all_results = []
        all_raw = []

        for ts_code, name in codes:
            print(f"  处理 {name} ({ts_code})...", end=" ")
            df = load_index_macd(ts_code, freq)
            if df.empty:
                print("无数据，跳过")
                continue

            oracle = detect_oracle_extremes(df, freq)
            method_detections = {}
            for method_name, method_fn in METHODS.items():
                method_detections[method_name] = method_fn(df)

            single_result = {}
            for method_name in METHODS:
                single_result[method_name] = match_and_evaluate(
                    oracle, method_detections[method_name], df, freq
                )
            all_results.append(single_result)
            all_raw.append((df, freq, oracle, method_detections))

            n_peaks = len([o for o in oracle if o["type"] == "peak"])
            n_troughs = len([o for o in oracle if o["type"] == "trough"])
            print(f"Oracle: {n_peaks}峰+{n_troughs}谷")

        if not all_results:
            continue

        # 汇总
        agg = aggregate_results(all_results)
        lag_data = collect_lag_data(all_raw)

        output_lines.append(f"### {freq_name}（{len(codes)}大指数汇总）\n")
        output_lines.append(format_comparison_table(agg, freq, lag_data))
        output_lines.append("")

        # 打印简要
        for ext_type in ["peak", "trough"]:
            type_name = "峰值" if ext_type == "peak" else "谷值"
            print(f"\n  [{freq_name}-{type_name}] 方法对比:")
            for method, name in METHOD_NAMES.items():
                if ext_type in agg.get(method, {}):
                    r = agg[method][ext_type]
                    lag_median = np.median(lag_data[method][ext_type]) if lag_data[method][ext_type] else 0
                    print(f"    {name}: 召回{r['recall']:.1f}% 精确{r['precision']:.1f}% 延迟中位{lag_median:.0f}根")

    return "\n".join(output_lines)


def analyze_industry_level():
    """行业指数层面分析"""
    output_lines = []
    output_lines.append("## 二、行业指数层面\n")

    freq = "daily"
    freq_name = "日线"
    print(f"\n{'='*60}")
    print(f"行业层面 - {freq_name}")
    print(f"{'='*60}")

    all_results = []
    all_raw = []
    processed = 0

    for category, code_list in SW_CATEGORIES.items():
        print(f"\n  {category}:")
        for ts_code in code_list:
            name = get_sw_industry_name(ts_code)
            print(f"    {name} ({ts_code})...", end=" ")

            df = load_sw_daily(ts_code)
            if df.empty or len(df) < 100:
                print("数据不足，跳过")
                continue

            oracle = detect_oracle_extremes(df, freq)
            method_detections = {}
            for method_name, method_fn in METHODS.items():
                method_detections[method_name] = method_fn(df)

            single_result = {}
            for method_name in METHODS:
                single_result[method_name] = match_and_evaluate(
                    oracle, method_detections[method_name], df, freq
                )
            all_results.append(single_result)
            all_raw.append((df, freq, oracle, method_detections))
            processed += 1

            n_peaks = len([o for o in oracle if o["type"] == "peak"])
            n_troughs = len([o for o in oracle if o["type"] == "trough"])
            print(f"Oracle: {n_peaks}峰+{n_troughs}谷")

    if not all_results:
        return ""

    # 汇总
    agg = aggregate_results(all_results)
    lag_data = collect_lag_data(all_raw)

    output_lines.append(f"### {freq_name}（{processed}个行业汇总）\n")
    output_lines.append(format_comparison_table(agg, freq, lag_data))
    output_lines.append("")

    # 打印简要
    for ext_type in ["peak", "trough"]:
        type_name = "峰值" if ext_type == "peak" else "谷值"
        print(f"\n  [{freq_name}-{type_name}] 方法对比:")
        for method, name in METHOD_NAMES.items():
            if ext_type in agg.get(method, {}):
                r = agg[method][ext_type]
                lag_median = np.median(lag_data[method][ext_type]) if lag_data[method][ext_type] else 0
                print(f"    {name}: 召回{r['recall']:.1f}% 精确{r['precision']:.1f}% 延迟中位{lag_median:.0f}根")

    return "\n".join(output_lines)


def analyze_stock_level(limit=0):
    """个股层面分析

    参数:
      limit: 限制处理股票数，0=不限制
    """
    output_lines = []
    output_lines.append("## 三、个股层面\n")

    codes = get_all_stock_codes()
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    print(f"\n{'='*60}")
    print(f"个股层面 — 共{total}只")
    print(f"{'='*60}")

    # remote模式只有约1年日线数据，周线样本太少
    freqs = ["daily"] if DATA_MODE == "remote" else FREQS

    for freq in freqs:
        freq_name = FREQ_NAMES[freq]
        print(f"\n--- {freq_name} ---")

        all_results = []
        all_raw = []
        processed = 0
        skipped = 0
        t0 = time.time()

        for i, ts_code in enumerate(codes):
            if (i + 1) % 500 == 0:
                elapsed = time.time() - t0
                speed = (i + 1) / elapsed
                eta = (total - i - 1) / speed / 60
                print(f"  进度: {i+1}/{total} ({processed}有效, {skipped}跳过) "
                      f"速度{speed:.0f}只/秒 剩余{eta:.1f}分钟")

            df = load_stock_data(ts_code, freq)
            if df.empty or len(df) < 100:
                skipped += 1
                continue

            oracle = detect_oracle_extremes(df, freq)
            if not oracle:
                skipped += 1
                continue

            method_detections = {}
            for method_name, method_fn in METHODS.items():
                method_detections[method_name] = method_fn(df)

            single_result = {}
            for method_name in METHODS:
                single_result[method_name] = match_and_evaluate(
                    oracle, method_detections[method_name], df, freq
                )
            all_results.append(single_result)
            all_raw.append((df, freq, oracle, method_detections))
            processed += 1

        elapsed = time.time() - t0
        print(f"\n  {freq_name}完成: {processed}只有效, {skipped}只跳过, 耗时{elapsed:.0f}秒")

        if not all_results:
            continue

        # 汇总
        agg = aggregate_results(all_results)
        lag_data = collect_lag_data(all_raw)

        output_lines.append(f"### {freq_name}（{processed}只个股汇总）\n")
        output_lines.append(format_comparison_table(agg, freq, lag_data))
        output_lines.append("")

        # 打印简要
        for ext_type in ["peak", "trough"]:
            type_name = "峰值" if ext_type == "peak" else "谷值"
            print(f"\n  [{freq_name}-{type_name}] 方法对比:")
            for method, name in METHOD_NAMES.items():
                if ext_type in agg.get(method, {}):
                    r = agg[method][ext_type]
                    lag_median = np.median(lag_data[method][ext_type]) if lag_data[method][ext_type] else 0
                    print(f"    {name}: 召回{r['recall']:.1f}% 精确{r['precision']:.1f}% 延迟中位{lag_median:.0f}根")

        # 释放内存
        del all_raw

    return "\n".join(output_lines)


# ══════════════════════════════════════════════════════════════
# Part 10: 主程序
# ══════════════════════════════════════════════════════════════

def main():
    global DATA_MODE, START_DATE
    parser = argparse.ArgumentParser(description="DIF极值实时检测方法对比研究")
    parser.add_argument("--level", required=True, choices=["index", "industry", "stock"],
                        help="分析层级")
    parser.add_argument("--mode", default="remote", choices=["docker", "remote"],
                        help="数据模式: docker(本地Docker MySQL) / remote(腾讯云MySQL)")
    parser.add_argument("--codes", nargs="*", default=None,
                        help="指定代码（格式: 000001.SH:上证指数）")
    parser.add_argument("--limit", type=int, default=0,
                        help="限制处理股票数量（调试用，0=不限制）")
    args = parser.parse_args()

    DATA_MODE = args.mode

    # 初始化数据库引擎
    if args.mode == "docker":
        init_docker_engines()
    else:
        init_remote_engine()
        # remote模式数据时间较短，不设START_DATE过滤
        START_DATE = "20000101"

    # remote模式仅支持stock层级
    if args.mode == "remote" and args.level in ("index", "industry"):
        print(f"错误: remote模式不支持{args.level}层级（无指数/行业数据），请使用docker模式")
        return

    print(f"DIF极值实时检测方法对比研究")
    print(f"层级: {args.level} | 模式: {args.mode}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"候选方法: {', '.join(METHOD_NAMES.values())}")

    if args.level == "index":
        codes = None
        if args.codes:
            codes = []
            for c in args.codes:
                parts = c.split(":")
                code = parts[0]
                name = parts[1] if len(parts) > 1 else code
                codes.append((code, name))
        output = analyze_index_level(codes)

    elif args.level == "industry":
        output = analyze_industry_level()

    elif args.level == "stock":
        output = analyze_stock_level(limit=args.limit)
    else:
        print(f"未知层级: {args.level}")
        return

    # 保存输出
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"dif_realtime_{args.level}_output.txt")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(output)
    print(f"\n结果已保存到: {output_file}")


if __name__ == "__main__":
    main()
