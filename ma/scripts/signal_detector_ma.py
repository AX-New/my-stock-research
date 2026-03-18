"""MA 均线信号检测引擎

输入: 含 K线 + 均线 + 乖离率的 DataFrame
    必须包含: trade_date, open, high, low, close, vol, pct_chg,
             ma5, ma10, ma20, ma30, ma60, ma90, ma250,
             bias5, bias10, bias20, bias60
    数据按 trade_date 升序排列

输出: 信号列表 (list of dict)，每条信号包含:
    idx, trade_date, signal_type, signal_name, direction, signal_value, close, ma_values

支持的 7 类信号:
1. bias_extreme   - 乖离率极值（超卖/过热）
2. direction_break - 方向突破/跌破（均线拐头+价格穿越）
3. fake_break     - 假突破（突破后N日内回撤）
4. support_resist - 均线支撑/阻力（接近均线后反弹/受阻）
5. alignment      - 均线排列（多头/空头排列转换）
6. convergence    - 均线粘合与发散（密集后展开）
7. ma_cross       - MA交叉 金叉死叉（短均线穿越长均线）

可复用: 输入任意标的的 MA DataFrame 即可，不绑定具体指数/股票
"""
import json

import numpy as np


# ── 各周期参数 ────────────────────────────────────────────────
FREQ_PARAMS = {
    "daily":   {"bias_lookback": 250, "fake_break_n": 5},
    "weekly":  {"bias_lookback": 52,  "fake_break_n": 3},
    "monthly": {"bias_lookback": 24,  "fake_break_n": 2},
    "yearly":  {"bias_lookback": 10,  "fake_break_n": 1},
}


# ── 辅助函数 ──────────────────────────────────────────────────

def make_signal(df, idx, signal_type, signal_name, direction, signal_value, ma_periods=None):
    """构造信号记录

    df: 数据源 DataFrame
    idx: 信号所在行索引
    signal_type: 信号大类（7种之一）
    signal_name: 具体信号名（如 bias20_extreme_low）
    direction: 方向 buy/sell
    signal_value: 信号关联数值（如乖离率）
    ma_periods: 需要记录的均线周期列表（如 [20, 60]），为 None 则不记录
    """
    row = df.iloc[idx]
    ma_vals = {}
    if ma_periods:
        for p in ma_periods:
            v = row.get(f"ma{p}")
            if v is not None and not np.isnan(v):
                ma_vals[f"ma{p}"] = round(float(v), 2)
    return {
        "idx": idx,
        "trade_date": row["trade_date"],
        "signal_type": signal_type,
        "signal_name": signal_name,
        "direction": direction,
        "signal_value": round(float(signal_value), 4) if signal_value is not None else None,
        "close": float(row["close"]),
        "ma_values": json.dumps(ma_vals) if ma_vals else None,
    }


# ── Signal 1: bias_extreme（乖离率极值）──────────────────────

def detect_bias_extreme(df, lookback=250):
    """检测乖离率极值信号

    对 bias5/bias10/bias20/bias60 四列，用滚动窗口计算历史 Q10/Q90 分位数。
    当前值超过 Q90 → sell（过热）; 低于 Q10 → buy（超卖）。
    仅使用历史数据计算分位，无未来信息泄露。

    lookback: 滚动分位数计算窗口（数据点数）
    """
    signals = []
    for period in [5, 10, 20, 60]:
        bias_col = f"bias{period}"
        bias = df[bias_col].values

        for i in range(lookback, len(df)):
            # 用历史窗口（不含当前点）计算分位
            window = bias[max(0, i - lookback):i]
            valid = window[~np.isnan(window)]
            if len(valid) < lookback // 2:
                continue

            q10 = np.percentile(valid, 10)
            q90 = np.percentile(valid, 90)
            current = bias[i]
            if np.isnan(current):
                continue

            if current <= q10:
                signals.append(make_signal(
                    df, i, "bias_extreme", f"bias{period}_extreme_low",
                    "buy", current, ma_periods=[period]))
            elif current >= q90:
                signals.append(make_signal(
                    df, i, "bias_extreme", f"bias{period}_extreme_high",
                    "sell", current, ma_periods=[period]))
    return signals


# ── Signal 2: direction_break（方向突破/跌破）─────────────────

def detect_direction_break(df):
    """检测均线方向突破/跌破信号（Granville B1/S1 模式）

    对 MA20 和 MA60:
    - Buy:  均线拐头向上（前一日 MA 走平或下行，今日 MA 上行）且收盘价站上均线
    - Sell: 均线拐头向下（前一日 MA 走平或上行，今日 MA 下行）且收盘价跌破均线
    仅关注 MA20/MA60，短周期均线噪音过大。
    """
    signals = []
    close = df["close"].values

    for period in [20, 60]:
        ma_col = f"ma{period}"
        ma = df[ma_col].values

        # 需要至少3个点来判断方向变化
        for i in range(2, len(df)):
            if np.isnan(ma[i]) or np.isnan(ma[i - 1]) or np.isnan(ma[i - 2]):
                continue
            if np.isnan(close[i]):
                continue

            prev_slope = ma[i - 1] - ma[i - 2]  # 前一日斜率
            curr_slope = ma[i] - ma[i - 1]       # 当日斜率

            # 均线拐头向上 + 价格在均线之上
            if prev_slope <= 0 and curr_slope > 0 and close[i] > ma[i]:
                signals.append(make_signal(
                    df, i, "direction_break", f"ma{period}_break_up",
                    "buy", curr_slope, ma_periods=[period]))

            # 均线拐头向下 + 价格在均线之下
            elif prev_slope >= 0 and curr_slope < 0 and close[i] < ma[i]:
                signals.append(make_signal(
                    df, i, "direction_break", f"ma{period}_break_down",
                    "sell", curr_slope, ma_periods=[period]))

    return signals


# ── Signal 3: fake_break（假突破）─────────────────────────────

def detect_fake_break(df, n=5):
    """检测假突破信号

    价格穿越均线后，在 N 根 K 线内又回到原侧，判定为假突破。
    对 MA20 和 MA60:
    - fake_break_up (sell): 价格向上穿越均线后 N 日内跌回均线下方
    - fake_break_down (buy): 价格向下穿越均线后 N 日内涨回均线上方

    n: 回撤判定窗口（日线默认5，周线3，月线2，年线1）
    """
    signals = []
    close = df["close"].values

    for period in [20, 60]:
        ma_col = f"ma{period}"
        ma = df[ma_col].values

        for i in range(1, len(df)):
            if np.isnan(ma[i]) or np.isnan(ma[i - 1]):
                continue
            if np.isnan(close[i]) or np.isnan(close[i - 1]):
                continue

            # 向上穿越: 昨日收盘在均线下方，今日收盘在均线上方
            if close[i - 1] < ma[i - 1] and close[i] >= ma[i]:
                # 在之后 N 根 K 线内检查是否跌回
                for j in range(1, n + 1):
                    check_idx = i + j
                    if check_idx >= len(df):
                        break
                    if np.isnan(close[check_idx]) or np.isnan(ma[check_idx]):
                        continue
                    if close[check_idx] < ma[check_idx]:
                        # 跌回了 → 假突破（向上穿越失败，偏空信号）
                        signals.append(make_signal(
                            df, check_idx, "fake_break", f"ma{period}_fake_break_up",
                            "sell", close[check_idx] - ma[check_idx],
                            ma_periods=[period]))
                        break

            # 向下穿越: 昨日收盘在均线上方，今日收盘在均线下方
            elif close[i - 1] > ma[i - 1] and close[i] <= ma[i]:
                for j in range(1, n + 1):
                    check_idx = i + j
                    if check_idx >= len(df):
                        break
                    if np.isnan(close[check_idx]) or np.isnan(ma[check_idx]):
                        continue
                    if close[check_idx] > ma[check_idx]:
                        # 涨回了 → 假突破（向下穿越失败，偏多信号）
                        signals.append(make_signal(
                            df, check_idx, "fake_break", f"ma{period}_fake_break_down",
                            "buy", close[check_idx] - ma[check_idx],
                            ma_periods=[period]))
                        break

    return signals


# ── Signal 4: support_resist（均线支撑/阻力）─────────────────

def detect_support_resist(df):
    """检测均线支撑/阻力信号

    价格接近均线（距离 < 1%）后反弹或受阻，需次日确认方向。
    对 MA20 和 MA60:
    - support (buy):  价格在均线上方且距离 < 1%，次日继续上涨 → 均线支撑有效
    - resist (sell): 价格在均线下方且距离 < 1%，次日继续下跌 → 均线阻力有效
    """
    signals = []
    close = df["close"].values

    for period in [20, 60]:
        ma_col = f"ma{period}"
        ma = df[ma_col].values

        # 需要次日确认，所以到 len-1
        for i in range(0, len(df) - 1):
            if np.isnan(ma[i]) or np.isnan(close[i]) or np.isnan(close[i + 1]):
                continue
            if ma[i] == 0:
                continue

            distance_pct = abs(close[i] - ma[i]) / ma[i] * 100

            if distance_pct >= 1.0:
                continue

            # 价格在均线上方，次日上涨 → 支撑
            if close[i] > ma[i] and close[i + 1] > close[i]:
                signals.append(make_signal(
                    df, i, "support_resist", f"ma{period}_support",
                    "buy", distance_pct, ma_periods=[period]))

            # 价格在均线下方，次日下跌 → 阻力
            elif close[i] < ma[i] and close[i + 1] < close[i]:
                signals.append(make_signal(
                    df, i, "support_resist", f"ma{period}_resist",
                    "sell", distance_pct, ma_periods=[period]))

    return signals


# ── Signal 5: alignment（均线排列）───────────────────────────

def _get_alignment_state(ma5, ma10, ma20, ma30, ma60):
    """判断均线排列状态

    返回:
    - 'bull_full': 完全多头排列 MA5 > MA10 > MA20 > MA30 > MA60
    - 'bear_full': 完全空头排列 MA5 < MA10 < MA20 < MA30 < MA60
    - 'mixed': 其他（混合排列）
    """
    mas = [ma5, ma10, ma20, ma30, ma60]
    if any(v is None or np.isnan(v) for v in mas):
        return "mixed"
    if all(mas[j] > mas[j + 1] for j in range(len(mas) - 1)):
        return "bull_full"
    if all(mas[j] < mas[j + 1] for j in range(len(mas) - 1)):
        return "bear_full"
    return "mixed"


def detect_alignment(df):
    """检测均线排列转换信号

    仅在排列状态发生转换时产生信号（不是每根K线都产生）:
    - alignment_bull (buy):  从非多头排列变为完全多头排列
    - alignment_bear (sell): 从非空头排列变为完全空头排列
    """
    signals = []
    ma5 = df["ma5"].values
    ma10 = df["ma10"].values
    ma20 = df["ma20"].values
    ma30 = df["ma30"].values
    ma60 = df["ma60"].values

    prev_state = "mixed"

    for i in range(len(df)):
        state = _get_alignment_state(ma5[i], ma10[i], ma20[i], ma30[i], ma60[i])

        if state == "bull_full" and prev_state != "bull_full":
            # 进入多头排列 → 买入信号
            # signal_value 存储 MA5 和 MA60 的价差比例
            spread = (ma5[i] - ma60[i]) / ma60[i] * 100 if ma60[i] != 0 else 0
            signals.append(make_signal(
                df, i, "alignment", "alignment_bull",
                "buy", spread, ma_periods=[5, 10, 20, 30, 60]))

        elif state == "bear_full" and prev_state != "bear_full":
            # 进入空头排列 → 卖出信号
            spread = (ma5[i] - ma60[i]) / ma60[i] * 100 if ma60[i] != 0 else 0
            signals.append(make_signal(
                df, i, "alignment", "alignment_bear",
                "sell", spread, ma_periods=[5, 10, 20, 30, 60]))

        prev_state = state

    return signals


# ── Signal 6: convergence（均线粘合与发散）────────────────────

def detect_convergence(df):
    """检测均线粘合后发散信号

    计算 MA5~MA60 五条均线的密度 = std / mean:
    - 密度低于阈值(1%) → 均线粘合状态
    - 粘合后密度突破阈值 → 发散信号
    - 发散方向由 MA5 与 MA60 的相对位置决定

    convergence_bull (buy):  粘合后发散，MA5 > MA60（向上展开）
    convergence_bear (sell): 粘合后发散，MA5 < MA60（向下展开）
    """
    signals = []
    ma5 = df["ma5"].values
    ma10 = df["ma10"].values
    ma20 = df["ma20"].values
    ma30 = df["ma30"].values
    ma60 = df["ma60"].values

    threshold = 0.01  # 1% 密度阈值
    was_converged = False  # 之前是否处于粘合状态

    for i in range(len(df)):
        mas = [ma5[i], ma10[i], ma20[i], ma30[i], ma60[i]]
        if any(np.isnan(v) for v in mas):
            was_converged = False
            continue

        mean_val = np.mean(mas)
        if mean_val == 0:
            was_converged = False
            continue

        density = np.std(mas) / mean_val

        if density < threshold:
            # 进入粘合状态
            was_converged = True
        elif was_converged and density >= threshold:
            # 从粘合状态发散
            was_converged = False
            if ma5[i] > ma60[i]:
                signals.append(make_signal(
                    df, i, "convergence", "convergence_bull",
                    "buy", density, ma_periods=[5, 10, 20, 30, 60]))
            else:
                signals.append(make_signal(
                    df, i, "convergence", "convergence_bear",
                    "sell", density, ma_periods=[5, 10, 20, 30, 60]))

    return signals


# ── Signal 7: ma_cross（MA交叉 金叉死叉）─────────────────────

def detect_ma_cross(df):
    """检测 MA 交叉信号（金叉/死叉）

    6 对均线组合: (5,10), (5,20), (5,30), (10,20), (10,30), (20,30)
    - 金叉 (buy):  短均线从下方穿越长均线（前一日短<长，当日短>=长）
    - 死叉 (sell): 短均线从上方穿越长均线（前一日短>长，当日短<=长）
    """
    signals = []
    cross_pairs = [(5, 10), (5, 20), (5, 30), (10, 20), (10, 30), (20, 30)]

    for short_p, long_p in cross_pairs:
        short_ma = df[f"ma{short_p}"].values
        long_ma = df[f"ma{long_p}"].values

        for i in range(1, len(df)):
            if (np.isnan(short_ma[i]) or np.isnan(short_ma[i - 1]) or
                    np.isnan(long_ma[i]) or np.isnan(long_ma[i - 1])):
                continue

            prev_diff = short_ma[i - 1] - long_ma[i - 1]
            curr_diff = short_ma[i] - long_ma[i]

            # 金叉: 短均线上穿长均线
            if prev_diff < 0 and curr_diff >= 0:
                signals.append(make_signal(
                    df, i, "ma_cross",
                    f"ma{short_p}_cross_ma{long_p}_golden",
                    "buy", curr_diff, ma_periods=[short_p, long_p]))

            # 死叉: 短均线下穿长均线
            elif prev_diff > 0 and curr_diff <= 0:
                signals.append(make_signal(
                    df, i, "ma_cross",
                    f"ma{short_p}_cross_ma{long_p}_death",
                    "sell", curr_diff, ma_periods=[short_p, long_p]))

    return signals


# ── 主入口 ────────────────────────────────────────────────────

def detect_all_signals(df, freq="daily"):
    """检测所有 MA 信号，汇总返回

    df: 含 K线 + 7条均线 + 4个乖离率的 DataFrame，按 trade_date 升序
    freq: K线周期，影响乖离率回望窗口和假突破判定窗口

    Returns: list of dict，按 trade_date 升序排列
    """
    params = FREQ_PARAMS.get(freq, FREQ_PARAMS["daily"])

    all_signals = []
    all_signals.extend(detect_bias_extreme(df, lookback=params["bias_lookback"]))
    all_signals.extend(detect_direction_break(df))
    all_signals.extend(detect_fake_break(df, n=params["fake_break_n"]))
    all_signals.extend(detect_support_resist(df))
    all_signals.extend(detect_alignment(df))
    all_signals.extend(detect_convergence(df))
    all_signals.extend(detect_ma_cross(df))

    # 按 trade_date 排序
    all_signals.sort(key=lambda x: x["trade_date"])
    return all_signals
