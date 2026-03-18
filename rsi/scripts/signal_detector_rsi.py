"""RSI 信号检测引擎

输入: 含 K线 + RSI 的 DataFrame
    必须包含: trade_date, open, high, low, close, vol, pct_chg,
             rsi_6, rsi_12, rsi_14, rsi_24
    数据按 trade_date 升序排列

输出: 信号列表 (list of dict)，每条信号包含:
    idx, trade_date, signal_type, signal_name, direction, signal_value, close, rsi_values

支持的 4 类信号:
1. extreme        - RSI 极端值（超买/超卖，固定阈值+自适应）
2. divergence     - RSI 与价格背离（顶背离/底背离）
3. failure_swing  - RSI 失败摆动（M头/W底形态）
4. centerline     - RSI 中轴穿越（上穿/下穿50）

可复用: 输入任意标的的 RSI DataFrame 即可，不绑定具体指数/股票
"""
import json
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=RuntimeWarning, module="numpy")


# ── 各周期参数 ────────────────────────────────────────────────
FREQ_PARAMS = {
    "daily":   {"adaptive_lookback": 250, "divergence_order": 5},
    "weekly":  {"adaptive_lookback": 52,  "divergence_order": 3},
    "monthly": {"adaptive_lookback": 24,  "divergence_order": 2},
    "yearly":  {"adaptive_lookback": 10,  "divergence_order": 1},
}


# ── 辅助函数 ──────────────────────────────────────────────────

def make_signal(df, idx, signal_type, signal_name, direction, signal_value, rsi_periods=None):
    """构造信号记录（完整版，用于写入信号表）

    rsi_periods: 需要记录的 RSI 周期列表（如 [6, 14]），为 None 则记录全部
    """
    row = df.iloc[idx]
    rsi_vals = {}
    periods = rsi_periods or [6, 12, 14, 24]
    for p in periods:
        col = f"rsi_{p}"
        v = row.get(col)
        if v is not None and not np.isnan(v):
            rsi_vals[col] = round(float(v), 2)
    return {
        "idx": idx,
        "trade_date": row["trade_date"],
        "signal_type": signal_type,
        "signal_name": signal_name,
        "direction": direction,
        "signal_value": round(float(signal_value), 2) if signal_value is not None else None,
        "close": float(row["close"]),
        "rsi_values": json.dumps(rsi_vals) if rsi_vals else None,
    }


def _make_signal_lite(idx, signal_type, signal_name, direction):
    """构造轻量信号记录（含 signal_name，跳过 iloc/JSON 开销，用于 stats 分析）"""
    return {"idx": idx, "signal_type": signal_type, "signal_name": signal_name, "direction": direction}


def _find_peaks(arr, order=5):
    """找局部极大值索引（值大于 order 个邻居）"""
    n = len(arr)
    peaks = []
    for i in range(order, n - order):
        if np.isnan(arr[i]):
            continue
        window = arr[i - order:i + order + 1]
        if np.any(np.isnan(window)):
            continue
        if arr[i] >= np.max(window):
            peaks.append(i)
    return peaks


def _find_troughs(arr, order=5):
    """找局部极小值索引"""
    n = len(arr)
    troughs = []
    for i in range(order, n - order):
        if np.isnan(arr[i]):
            continue
        window = arr[i - order:i + order + 1]
        if np.any(np.isnan(window)):
            continue
        if arr[i] <= np.min(window):
            troughs.append(i)
    return troughs


# ── Signal 1: extreme（RSI极端值）────────────────────────────

def detect_extreme(df, lookback=250, lite=False):
    """检测 RSI 极端值信号

    固定阈值:
    - RSI > 70 → overbought (sell)
    - RSI < 30 → oversold (buy)
    - RSI > 80 → strong_overbought (sell)
    - RSI < 20 → strong_oversold (buy)

    自适应阈值（仅 RSI14）:
    - RSI > 滚动Q90 → adaptive_high (sell)
    - RSI < 滚动Q10 → adaptive_low (buy)

    只在 RSI 进入极端区间的第一天发出信号（避免钝化期间重复发信号）。
    """
    signals = []
    mk = _make_signal_lite if lite else None

    # 1. 固定阈值: 对所有4个RSI周期检测 70/30
    for period in [6, 12, 14, 24]:
        rsi_col = f"rsi_{period}"
        rsi = df[rsi_col].values

        # 检测进入极端区域的第一天（entry only）
        prev_rsi = np.roll(rsi, 1)
        prev_rsi[0] = np.nan

        for i in range(1, len(df)):
            if np.isnan(rsi[i]) or np.isnan(prev_rsi[i]):
                continue

            # 进入超买区（前一天 <=70，今天 >70）
            if rsi[i] > 70 and prev_rsi[i] <= 70:
                if lite:
                    signals.append(mk(i, "extreme", f"rsi{period}_overbought", "sell"))
                else:
                    signals.append(make_signal(
                        df, i, "extreme", f"rsi{period}_overbought",
                        "sell", rsi[i]))

            # 进入超卖区
            elif rsi[i] < 30 and prev_rsi[i] >= 30:
                if lite:
                    signals.append(mk(i, "extreme", f"rsi{period}_oversold", "buy"))
                else:
                    signals.append(make_signal(
                        df, i, "extreme", f"rsi{period}_oversold",
                        "buy", rsi[i]))

    # 2. 强极端阈值: 仅 RSI14 检测 80/20
    rsi14 = df["rsi_14"].values
    prev_rsi14 = np.roll(rsi14, 1)
    prev_rsi14[0] = np.nan

    for i in range(1, len(df)):
        if np.isnan(rsi14[i]) or np.isnan(prev_rsi14[i]):
            continue

        if rsi14[i] > 80 and prev_rsi14[i] <= 80:
            if lite:
                signals.append(mk(i, "extreme", "rsi14_strong_overbought", "sell"))
            else:
                signals.append(make_signal(
                    df, i, "extreme", "rsi14_strong_overbought",
                    "sell", rsi14[i]))

        elif rsi14[i] < 20 and prev_rsi14[i] >= 20:
            if lite:
                signals.append(mk(i, "extreme", "rsi14_strong_oversold", "buy"))
            else:
                signals.append(make_signal(
                    df, i, "extreme", "rsi14_strong_oversold",
                    "buy", rsi14[i]))

    # 3. 自适应阈值: RSI14 的滚动 Q90/Q10（仿 MA bias_extreme）
    series14 = pd.Series(rsi14)
    shifted = series14.shift(1)
    q10_arr = shifted.rolling(window=lookback, min_periods=lookback // 2).quantile(0.1).values
    q90_arr = shifted.rolling(window=lookback, min_periods=lookback // 2).quantile(0.9).values

    for i in range(lookback, len(df)):
        current = rsi14[i]
        if np.isnan(current) or np.isnan(q10_arr[i]):
            continue

        # 进入自适应极端区域的第一天
        prev = rsi14[i - 1] if i > 0 else np.nan
        if np.isnan(prev):
            continue

        if current >= q90_arr[i] and prev < q90_arr[i - 1] if i > 0 and not np.isnan(q90_arr[i - 1]) else True:
            if lite:
                signals.append(mk(i, "extreme", "rsi14_adaptive_high", "sell"))
            else:
                signals.append(make_signal(
                    df, i, "extreme", "rsi14_adaptive_high",
                    "sell", current))

        elif current <= q10_arr[i] and prev > q10_arr[i - 1] if i > 0 and not np.isnan(q10_arr[i - 1]) else True:
            if lite:
                signals.append(mk(i, "extreme", "rsi14_adaptive_low", "buy"))
            else:
                signals.append(make_signal(
                    df, i, "extreme", "rsi14_adaptive_low",
                    "buy", current))

    return signals


# ── Signal 2: divergence（背离）──────────────────────────────

def detect_divergence(df, order=5, lite=False):
    """检测 RSI(14) 与价格的背离信号

    顶背离 (sell): 价格创新高但 RSI 未创新高 → 上涨动能衰竭
    底背离 (buy):  价格创新低但 RSI 未创新低 → 下跌动能衰竭

    使用局部极值匹配:
    1. 找价格和 RSI 的局部极值
    2. 对每对连续极值检查是否背离
    3. 信号在第二个极值处发出（此时背离可确认）

    order: 局部极值检测窗口（需 order 根K线确认）
    """
    signals = []
    close = df["close"].values
    rsi14 = df["rsi_14"].values

    # 找极值
    price_peaks = _find_peaks(close, order)
    price_troughs = _find_troughs(close, order)
    rsi_peaks = _find_peaks(rsi14, order)
    rsi_troughs = _find_troughs(rsi14, order)

    # 顶背离: 价格高点递增 + RSI高点递减
    # 对每对连续价格高点，找时间上最近的RSI高点
    for i in range(1, len(price_peaks)):
        pp1, pp2 = price_peaks[i - 1], price_peaks[i]

        # 找距离 pp1 和 pp2 最近的 RSI 高点
        rp1 = _find_nearest(rsi_peaks, pp1, max_dist=order * 3)
        rp2 = _find_nearest(rsi_peaks, pp2, max_dist=order * 3)
        if rp1 is None or rp2 is None or rp1 >= rp2:
            continue

        # 价格更高 + RSI更低 = 顶背离
        if close[pp2] > close[pp1] and rsi14[rp2] < rsi14[rp1]:
            if lite:
                signals.append(_make_signal_lite(pp2, "divergence", "rsi14_bear_divergence", "sell"))
            else:
                signals.append(make_signal(
                    df, pp2, "divergence", "rsi14_bear_divergence",
                    "sell", rsi14[rp2]))

    # 底背离: 价格低点递减 + RSI低点递增
    for i in range(1, len(price_troughs)):
        pt1, pt2 = price_troughs[i - 1], price_troughs[i]

        rt1 = _find_nearest(rsi_troughs, pt1, max_dist=order * 3)
        rt2 = _find_nearest(rsi_troughs, pt2, max_dist=order * 3)
        if rt1 is None or rt2 is None or rt1 >= rt2:
            continue

        # 价格更低 + RSI更高 = 底背离
        if close[pt2] < close[pt1] and rsi14[rt2] > rsi14[rt1]:
            if lite:
                signals.append(_make_signal_lite(pt2, "divergence", "rsi14_bull_divergence", "buy"))
            else:
                signals.append(make_signal(
                    df, pt2, "divergence", "rsi14_bull_divergence",
                    "buy", rsi14[rt2]))

    return signals


def _find_nearest(indices, target, max_dist=15):
    """在 indices 列表中找距离 target 最近的值，超出 max_dist 返回 None"""
    if not indices:
        return None
    best = None
    best_dist = max_dist + 1
    for idx in indices:
        d = abs(idx - target)
        if d < best_dist:
            best_dist = d
            best = idx
    return best if best_dist <= max_dist else None


# ── Signal 3: failure_swing（失败摆动）────────────────────────

def detect_failure_swing(df, lite=False):
    """检测 RSI(14) 失败摆动信号

    空头失败摆动 (sell):
    1. RSI 上穿 70 进入超买区
    2. RSI 从超买区回落（形成高点 P1）
    3. RSI 再次上升但未超过 P1（形成更低的高点 P2）
    4. RSI 跌破 P1-P2 之间的低点 → 卖出信号

    多头失败摆动 (buy):
    1. RSI 下穿 30 进入超卖区
    2. RSI 从超卖区反弹（形成低点 T1）
    3. RSI 再次下降但未低于 T1（形成更高的低点 T2）
    4. RSI 突破 T1-T2 之间的高点 → 买入信号
    """
    signals = []
    rsi14 = df["rsi_14"].values
    n = len(df)

    # 多头失败摆动: 在超卖区(RSI<30)找W底形态
    # 状态机追踪
    i = 0
    while i < n - 2:
        if np.isnan(rsi14[i]):
            i += 1
            continue

        # Step 1: RSI 进入超卖区 (<30)
        if rsi14[i] >= 30:
            i += 1
            continue

        # Step 2: 找超卖区内的第一个低点 T1
        t1_idx = i
        t1_val = rsi14[i]
        j = i + 1
        while j < n and not np.isnan(rsi14[j]) and rsi14[j] <= t1_val:
            t1_val = rsi14[j]
            t1_idx = j
            j += 1

        # Step 3: RSI 反弹，找反弹高点 (T1和T2之间的峰)
        if j >= n:
            break
        peak_idx = j
        peak_val = rsi14[j] if not np.isnan(rsi14[j]) else 0
        j += 1
        while j < n and not np.isnan(rsi14[j]) and rsi14[j] >= peak_val:
            peak_val = rsi14[j]
            peak_idx = j
            j += 1

        # Step 4: RSI 再次下降，找第二个低点 T2（必须高于 T1）
        if j >= n:
            break
        t2_idx = j
        t2_val = rsi14[j] if not np.isnan(rsi14[j]) else 100
        j += 1
        while j < n and not np.isnan(rsi14[j]) and rsi14[j] <= t2_val:
            t2_val = rsi14[j]
            t2_idx = j
            j += 1

        # Step 5: 验证 W 底条件 (T2 > T1) 且 RSI 突破 peak
        if t2_val > t1_val:
            # 找 RSI 突破 peak_val 的时刻
            for k in range(t2_idx + 1, min(t2_idx + 20, n)):
                if np.isnan(rsi14[k]):
                    continue
                if rsi14[k] > peak_val:
                    if lite:
                        signals.append(_make_signal_lite(k, "failure_swing", "rsi14_bull_failure_swing", "buy"))
                    else:
                        signals.append(make_signal(
                            df, k, "failure_swing", "rsi14_bull_failure_swing",
                            "buy", rsi14[k]))
                    break

        i = max(j, i + 1)

    # 空头失败摆动: 在超买区(RSI>70)找M头形态
    i = 0
    while i < n - 2:
        if np.isnan(rsi14[i]):
            i += 1
            continue

        # Step 1: RSI 进入超买区 (>70)
        if rsi14[i] <= 70:
            i += 1
            continue

        # Step 2: 找超买区内的第一个高点 P1
        p1_idx = i
        p1_val = rsi14[i]
        j = i + 1
        while j < n and not np.isnan(rsi14[j]) and rsi14[j] >= p1_val:
            p1_val = rsi14[j]
            p1_idx = j
            j += 1

        # Step 3: RSI 回落，找回调低点
        if j >= n:
            break
        trough_idx = j
        trough_val = rsi14[j] if not np.isnan(rsi14[j]) else 100
        j += 1
        while j < n and not np.isnan(rsi14[j]) and rsi14[j] <= trough_val:
            trough_val = rsi14[j]
            trough_idx = j
            j += 1

        # Step 4: RSI 再次上升，找第二个高点 P2（必须低于 P1）
        if j >= n:
            break
        p2_idx = j
        p2_val = rsi14[j] if not np.isnan(rsi14[j]) else 0
        j += 1
        while j < n and not np.isnan(rsi14[j]) and rsi14[j] >= p2_val:
            p2_val = rsi14[j]
            p2_idx = j
            j += 1

        # Step 5: 验证 M 头条件 (P2 < P1) 且 RSI 跌破 trough
        if p2_val < p1_val:
            for k in range(p2_idx + 1, min(p2_idx + 20, n)):
                if np.isnan(rsi14[k]):
                    continue
                if rsi14[k] < trough_val:
                    if lite:
                        signals.append(_make_signal_lite(k, "failure_swing", "rsi14_bear_failure_swing", "sell"))
                    else:
                        signals.append(make_signal(
                            df, k, "failure_swing", "rsi14_bear_failure_swing",
                            "sell", rsi14[k]))
                    break

        i = max(j, i + 1)

    return signals


# ── Signal 4: centerline（中轴穿越）─────────────────────────

def detect_centerline(df, lite=False):
    """检测 RSI(14) 穿越 50 中轴信号

    RSI 上穿 50 → buy（多方力量开始占优）
    RSI 下穿 50 → sell（空方力量开始占优）

    类似 MACD 零轴穿越概念。
    """
    signals = []
    rsi14 = df["rsi_14"].values

    prev = np.roll(rsi14, 1)
    prev[0] = np.nan

    for i in range(1, len(df)):
        if np.isnan(rsi14[i]) or np.isnan(prev[i]):
            continue

        # 上穿 50
        if prev[i] < 50 and rsi14[i] >= 50:
            if lite:
                signals.append(_make_signal_lite(i, "centerline", "rsi14_cross_above_50", "buy"))
            else:
                signals.append(make_signal(
                    df, i, "centerline", "rsi14_cross_above_50",
                    "buy", rsi14[i]))

        # 下穿 50
        elif prev[i] > 50 and rsi14[i] <= 50:
            if lite:
                signals.append(_make_signal_lite(i, "centerline", "rsi14_cross_below_50", "sell"))
            else:
                signals.append(make_signal(
                    df, i, "centerline", "rsi14_cross_below_50",
                    "sell", rsi14[i]))

    return signals


# ── 主入口 ────────────────────────────────────────────────────

def detect_all_signals(df, freq="daily"):
    """检测所有 RSI 信号，汇总返回（完整版，用于写入信号表）

    df: 含 K线 + 4个RSI的 DataFrame，按 trade_date 升序
    freq: K线周期，影响自适应回望窗口和背离检测参数

    Returns: list of dict，按 trade_date 升序排列
    """
    params = FREQ_PARAMS.get(freq, FREQ_PARAMS["daily"])

    all_signals = []
    all_signals.extend(detect_extreme(df, lookback=params["adaptive_lookback"]))
    all_signals.extend(detect_divergence(df, order=params["divergence_order"]))
    all_signals.extend(detect_failure_swing(df))
    all_signals.extend(detect_centerline(df))

    # 按 trade_date 排序
    all_signals.sort(key=lambda x: x["trade_date"])
    return all_signals


def detect_all_signals_fast(df, freq="daily"):
    """检测所有 RSI 信号（轻量版，仅返回 idx/signal_type/direction）

    跳过 df.iloc 行访问和 JSON 序列化，用于 stats 统计分析。
    """
    params = FREQ_PARAMS.get(freq, FREQ_PARAMS["daily"])

    all_signals = []
    all_signals.extend(detect_extreme(df, lookback=params["adaptive_lookback"], lite=True))
    all_signals.extend(detect_divergence(df, order=params["divergence_order"], lite=True))
    all_signals.extend(detect_failure_swing(df, lite=True))
    all_signals.extend(detect_centerline(df, lite=True))

    return all_signals
