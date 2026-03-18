"""换手率信号检测器（全向量化）

信号类型（按优先级）:
1. extreme  — 极端值（滚动分位数）
2. divergence — 量价背离（价格方向 vs 换手率方向矛盾）
3. zone — 位置（历史分位区间）
4. persistent — 持续性（连续高/低换手）
5. surge — 突变（相对MA的突然变化）
6. cross — 交叉（MA5/MA20金叉死叉，大概率噪音）

设计说明:
- 指数级换手率非常平滑，日/MA5比值99分位才1.44，所以surge阈值不能用2.0x
- 使用250日滚动窗口（探索分析中变异系数最低）
- 所有检测函数返回信号列表，支持 lite 模式（用于L4批量统计）
"""
import numpy as np
import pandas as pd


# ── lite 信号构造 ──────────────────────────────────────────

def _make_signal(df, idx, signal_type, signal_name, direction, signal_value):
    """构造完整信号 dict（用于 L1-L3 落库）"""
    return {
        "trade_date": df.iloc[idx]["trade_date"],
        "signal_type": signal_type,
        "signal_name": signal_name,
        "direction": direction,
        "signal_value": float(signal_value) if not np.isnan(signal_value) else None,
    }


def _make_signal_lite(idx, signal_type, signal_name, direction):
    """轻量信号（用于 L4 批量统计，跳过 dict 构造开销）"""
    return (idx, signal_type, signal_name, direction)


# ── 1. 极端值信号 ──────────────────────────────────────────

def detect_extreme(df, col="turnover_rate_f", window=250, high_q=0.95, low_q=0.05, lite=False):
    """滚动窗口分位数极端值检测

    Args:
        df: 必须含 col 和 trade_date 列
        col: 换手率列名
        window: 滚动窗口大小（默认250日=1年）
        high_q: 超高换手分位数阈值（默认0.95）
        low_q: 超低换手分位数阈值（默认0.05）
        lite: True 返回轻量元组，False 返回完整 dict
    """
    signals = []
    s = df[col].values
    n = len(s)
    if n < window:
        return signals

    # 向量化：滚动分位数
    sr = pd.Series(s)
    q_high = sr.rolling(window, min_periods=window).quantile(high_q).values
    q_low = sr.rolling(window, min_periods=window).quantile(low_q).values

    # 超高换手（卖出信号）
    mask_high = (s > q_high) & ~np.isnan(q_high)
    for idx in np.where(mask_high)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "extreme", "extreme_high", "sell"))
        else:
            signals.append(_make_signal(df, idx, "extreme", "extreme_high", "sell", s[idx]))

    # 超低换手（买入信号）
    mask_low = (s < q_low) & ~np.isnan(q_low)
    for idx in np.where(mask_low)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "extreme", "extreme_low", "buy"))
        else:
            signals.append(_make_signal(df, idx, "extreme", "extreme_low", "buy", s[idx]))

    return signals


# ── 2. 量价背离信号 ──────────────────────────────────────────

def detect_divergence(df, col="turnover_rate_f", price_col="close", n_days=5, lite=False):
    """量价背离检测：价格连续上涨但换手率趋势下降，或反之

    方法: 用 n_days 日滚动斜率方向来判断趋势
    - 价涨量缩: close 的 n日变化 > 0 且 turnover 的 n日变化 < 0，持续 n_days 天
    - 价跌量增: close 的 n日变化 < 0 且 turnover 的 n日变化 > 0，持续 n_days 天
    """
    signals = []
    n = len(df)
    if n < n_days + 1:
        return signals

    close = df[price_col].values if price_col in df.columns else None
    if close is None:
        return signals
    turnover = df[col].values

    # 价格和换手率的 n_days 日变化
    price_change = np.full(n, np.nan)
    vol_change = np.full(n, np.nan)
    price_change[n_days:] = close[n_days:] - close[:-n_days]
    vol_change[n_days:] = turnover[n_days:] - turnover[:-n_days]

    # 价涨量缩（卖出信号）：价格上涨 + 换手率下降
    mask_up_down = (price_change > 0) & (vol_change < 0) & ~np.isnan(price_change)
    for idx in np.where(mask_up_down)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "divergence", "price_up_vol_down", "sell"))
        else:
            signals.append(_make_signal(df, idx, "divergence", "price_up_vol_down", "sell", turnover[idx]))

    # 价跌量增（买入信号）：价格下跌 + 换手率上升
    mask_down_up = (price_change < 0) & (vol_change > 0) & ~np.isnan(price_change)
    for idx in np.where(mask_down_up)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "divergence", "price_down_vol_up", "buy"))
        else:
            signals.append(_make_signal(df, idx, "divergence", "price_down_vol_up", "buy", turnover[idx]))

    return signals


# ── 3. 位置信号 ──────────────────────────────────────────────

def detect_zone(df, col="turnover_rate_f", window=250, high_q=0.80, low_q=0.20, lite=False):
    """历史分位数位置判断

    与 extreme 的区别: extreme 用更严格的阈值(95/5)标记极端，zone 用宽松阈值(80/20)标记区间
    """
    signals = []
    s = df[col].values
    n = len(s)
    if n < window:
        return signals

    sr = pd.Series(s)
    q_high = sr.rolling(window, min_periods=window).quantile(high_q).values
    q_low = sr.rolling(window, min_periods=window).quantile(low_q).values

    # 高换手区（卖出倾向）
    mask_high = (s > q_high) & ~np.isnan(q_high)
    for idx in np.where(mask_high)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "zone", "zone_high", "sell"))
        else:
            signals.append(_make_signal(df, idx, "zone", "zone_high", "sell", s[idx]))

    # 低换手区（买入倾向）
    mask_low = (s < q_low) & ~np.isnan(q_low)
    for idx in np.where(mask_low)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "zone", "zone_low", "buy"))
        else:
            signals.append(_make_signal(df, idx, "zone", "zone_low", "buy", s[idx]))

    return signals


# ── 4. 持续性信号 ──────────────────────────────────────────

def detect_persistent(df, col="turnover_rate_f", ma_window=20, high_mult=1.5,
                      low_mult=0.7, min_consecutive=3, lite=False):
    """连续放量/缩量检测

    连续放量: 连续 min_consecutive 天 > MA20 * high_mult
    连续缩量: 连续 min_consecutive 天 < MA20 * low_mult
    只在连续期的最后一天产生信号（避免重复）
    """
    signals = []
    s = df[col].values
    n = len(s)
    if n < ma_window + min_consecutive:
        return signals

    ma = pd.Series(s).rolling(ma_window, min_periods=ma_window).mean().values

    # 连续放量
    is_high = (s > ma * high_mult) & ~np.isnan(ma)
    consec_high = _count_consecutive(is_high)
    # 在连续期结束时触发（当前为True但下一个为False，或是最后一个数据点）
    for idx in range(ma_window, n):
        if consec_high[idx] >= min_consecutive:
            # 只在连续期的最后一天触发
            if idx == n - 1 or not is_high[idx + 1]:
                if lite:
                    signals.append(_make_signal_lite(idx, "persistent", "sustained_high", "sell"))
                else:
                    signals.append(_make_signal(df, idx, "persistent", "sustained_high", "sell", s[idx]))

    # 连续缩量
    is_low = (s < ma * low_mult) & ~np.isnan(ma)
    consec_low = _count_consecutive(is_low)
    for idx in range(ma_window, n):
        if consec_low[idx] >= min_consecutive:
            if idx == n - 1 or not is_low[idx + 1]:
                if lite:
                    signals.append(_make_signal_lite(idx, "persistent", "sustained_low", "buy"))
                else:
                    signals.append(_make_signal(df, idx, "persistent", "sustained_low", "buy", s[idx]))

    return signals


def _count_consecutive(mask: np.ndarray) -> np.ndarray:
    """计算布尔数组中每个 True 位置的连续 True 计数（向量化）"""
    n = len(mask)
    result = np.zeros(n, dtype=int)
    for i in range(n):
        if mask[i]:
            result[i] = result[i - 1] + 1 if i > 0 else 1
    return result


# ── 5. 突变信号 ──────────────────────────────────────────

def detect_surge(df, col="turnover_rate_f", ma_window=20, surge_mult=1.5,
                 plunge_mult=0.5, lite=False):
    """换手率突变检测（相对MA20的倍数）

    注意: 指数级换手率很平滑，日/MA5比值99分位才1.44。
    所以用MA20作为基准（比MA5有更大的偏离空间），阈值用1.5x/0.5x。
    """
    signals = []
    s = df[col].values
    n = len(s)
    if n < ma_window:
        return signals

    ma = pd.Series(s).rolling(ma_window, min_periods=ma_window).mean().values
    ratio = np.where(ma > 0, s / ma, np.nan)

    # 暴增（方向待定：可能是恐慌抛售也可能是资金涌入）
    mask_surge = (ratio > surge_mult) & ~np.isnan(ratio)
    for idx in np.where(mask_surge)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "surge", "surge", "neutral"))
        else:
            signals.append(_make_signal(df, idx, "surge", "surge", "neutral", s[idx]))

    # 骤降
    mask_plunge = (ratio < plunge_mult) & ~np.isnan(ratio)
    for idx in np.where(mask_plunge)[0]:
        if lite:
            signals.append(_make_signal_lite(idx, "surge", "plunge", "neutral"))
        else:
            signals.append(_make_signal(df, idx, "surge", "plunge", "neutral", s[idx]))

    return signals


# ── 6. 交叉信号 ──────────────────────────────────────────

def detect_cross(df, col="turnover_rate_f", fast=5, slow=20, lite=False):
    """换手率MA金叉/死叉（大概率噪音，验证用）"""
    signals = []
    s = df[col].values
    n = len(s)
    if n < slow + 1:
        return signals

    sr = pd.Series(s)
    ma_fast = sr.rolling(fast, min_periods=fast).mean().values
    ma_slow = sr.rolling(slow, min_periods=slow).mean().values

    # 金叉: MA5 从下穿上 MA20
    for idx in range(slow, n):
        if (np.isnan(ma_fast[idx]) or np.isnan(ma_slow[idx]) or
            np.isnan(ma_fast[idx-1]) or np.isnan(ma_slow[idx-1])):
            continue

        if ma_fast[idx] > ma_slow[idx] and ma_fast[idx-1] <= ma_slow[idx-1]:
            if lite:
                signals.append(_make_signal_lite(idx, "cross", "ma_cross_up", "buy"))
            else:
                signals.append(_make_signal(df, idx, "cross", "ma_cross_up", "buy", s[idx]))

        elif ma_fast[idx] < ma_slow[idx] and ma_fast[idx-1] >= ma_slow[idx-1]:
            if lite:
                signals.append(_make_signal_lite(idx, "cross", "ma_cross_down", "sell"))
            else:
                signals.append(_make_signal(df, idx, "cross", "ma_cross_down", "sell", s[idx]))

    return signals


# ── 汇总检测 ──────────────────────────────────────────────

def detect_all_signals(df, col="turnover_rate_f", price_col="close", lite=False, **kwargs):
    """运行所有信号检测器，返回合并的信号列表

    kwargs 可传入各检测器的参数覆盖默认值:
        extreme_window, extreme_high_q, extreme_low_q,
        divergence_n_days,
        zone_window, zone_high_q, zone_low_q,
        persistent_ma_window, persistent_high_mult, persistent_low_mult, persistent_min_consecutive,
        surge_ma_window, surge_mult, plunge_mult,
        cross_fast, cross_slow
    """
    all_signals = []

    # 1. 极端值
    all_signals.extend(detect_extreme(
        df, col=col,
        window=kwargs.get("extreme_window", 250),
        high_q=kwargs.get("extreme_high_q", 0.95),
        low_q=kwargs.get("extreme_low_q", 0.05),
        lite=lite,
    ))

    # 2. 量价背离
    all_signals.extend(detect_divergence(
        df, col=col, price_col=price_col,
        n_days=kwargs.get("divergence_n_days", 5),
        lite=lite,
    ))

    # 3. 位置
    all_signals.extend(detect_zone(
        df, col=col,
        window=kwargs.get("zone_window", 250),
        high_q=kwargs.get("zone_high_q", 0.80),
        low_q=kwargs.get("zone_low_q", 0.20),
        lite=lite,
    ))

    # 4. 持续性
    all_signals.extend(detect_persistent(
        df, col=col,
        ma_window=kwargs.get("persistent_ma_window", 20),
        high_mult=kwargs.get("persistent_high_mult", 1.5),
        low_mult=kwargs.get("persistent_low_mult", 0.7),
        min_consecutive=kwargs.get("persistent_min_consecutive", 3),
        lite=lite,
    ))

    # 5. 突变
    all_signals.extend(detect_surge(
        df, col=col,
        ma_window=kwargs.get("surge_ma_window", 20),
        surge_mult=kwargs.get("surge_mult", 1.5),
        plunge_mult=kwargs.get("plunge_mult", 0.5),
        lite=lite,
    ))

    # 6. 交叉
    all_signals.extend(detect_cross(
        df, col=col,
        fast=kwargs.get("cross_fast", 5),
        slow=kwargs.get("cross_slow", 20),
        lite=lite,
    ))

    return all_signals


# 信号名 → 中文映射
SIGNAL_NAMES_CN = {
    "extreme_high": "超高换手(>P95)",
    "extreme_low": "超低换手(<P5)",
    "price_up_vol_down": "价涨量缩",
    "price_down_vol_up": "价跌量增",
    "zone_high": "高换手区(>P80)",
    "zone_low": "低换手区(<P20)",
    "sustained_high": "连续放量",
    "sustained_low": "连续缩量",
    "surge": "换手率暴增",
    "plunge": "换手率骤降",
    "ma_cross_up": "MA5上穿MA20",
    "ma_cross_down": "MA5下穿MA20",
}

SIGNAL_TYPE_NAMES_CN = {
    "extreme": "极端值",
    "divergence": "量价背离",
    "zone": "位置",
    "persistent": "持续性",
    "surge": "突变",
    "cross": "交叉",
}

# buy 方向信号集合（用于胜率计算）
BUY_SIGNALS = {"extreme_low", "price_down_vol_up", "zone_low", "sustained_low", "ma_cross_up"}
SELL_SIGNALS = {"extreme_high", "price_up_vol_down", "zone_high", "sustained_high", "ma_cross_down"}
NEUTRAL_SIGNALS = {"surge", "plunge"}  # 方向待数据确定
