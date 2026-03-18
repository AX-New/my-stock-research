"""MACD 信号检测引擎

输入: 含 MACD 数据的 DataFrame (必须包含 trade_date, close, dif, dea, macd 列)
输出: 信号列表

支持的信号类型:
- golden_cross / zero_golden_cross: 金叉（零轴下/零轴上）
- death_cross / zero_death_cross: 死叉（零轴上/零轴下）
- top_divergence: 顶背离（价格新高但 DIF 未新高）
- bottom_divergence: 底背离（价格新低但 DIF 未新低）
- dif_cross_zero_up / dif_cross_zero_down: DIF 零轴穿越

可复用: 输入任意标的的 MACD DataFrame 即可，不绑定具体指数/股票
"""
import pandas as pd
import numpy as np


# 各周期建议的局部极值查找窗口（单侧K线根数）
FREQ_ORDER = {
    "daily": 20,    # 约1个月
    "weekly": 8,    # 约2个月
    "monthly": 3,   # 约1个季度
    "yearly": 1,    # 相邻年份
}


def detect_crosses(df: pd.DataFrame) -> list[dict]:
    """检测金叉和死叉

    金叉: 前一根 DIF < DEA, 当前 DIF >= DEA
    死叉: 前一根 DIF > DEA, 当前 DIF <= DEA

    零轴上金叉(zero_golden_cross): 金叉时 DIF > 0，表示强势区域金叉
    零轴下死叉(zero_death_cross): 死叉时 DIF < 0，表示弱势区域死叉
    """
    signals = []
    dif = df["dif"].values
    dea = df["dea"].values

    for i in range(1, len(df)):
        prev_diff = dif[i - 1] - dea[i - 1]
        curr_diff = dif[i] - dea[i]

        if prev_diff < 0 and curr_diff >= 0:
            above_zero = dif[i] > 0
            signals.append({
                "idx": i,
                "trade_date": df.iloc[i]["trade_date"],
                "signal": "zero_golden_cross" if above_zero else "golden_cross",
                "close": float(df.iloc[i]["close"]),
                "dif": float(dif[i]),
                "dea": float(dea[i]),
                "macd": float(df.iloc[i]["macd"]),
            })
        elif prev_diff > 0 and curr_diff <= 0:
            below_zero = dif[i] < 0
            signals.append({
                "idx": i,
                "trade_date": df.iloc[i]["trade_date"],
                "signal": "zero_death_cross" if below_zero else "death_cross",
                "close": float(df.iloc[i]["close"]),
                "dif": float(dif[i]),
                "dea": float(dea[i]),
                "macd": float(df.iloc[i]["macd"]),
            })

    return signals


def detect_zero_crosses(df: pd.DataFrame) -> list[dict]:
    """检测 DIF 零轴穿越

    dif_cross_zero_up: DIF 从负转正，趋势转强
    dif_cross_zero_down: DIF 从正转负，趋势转弱
    """
    signals = []
    dif = df["dif"].values

    for i in range(1, len(df)):
        if dif[i - 1] < 0 and dif[i] >= 0:
            signals.append({
                "idx": i,
                "trade_date": df.iloc[i]["trade_date"],
                "signal": "dif_cross_zero_up",
                "close": float(df.iloc[i]["close"]),
                "dif": float(dif[i]),
                "dea": float(df.iloc[i]["dea"]),
                "macd": float(df.iloc[i]["macd"]),
            })
        elif dif[i - 1] > 0 and dif[i] <= 0:
            signals.append({
                "idx": i,
                "trade_date": df.iloc[i]["trade_date"],
                "signal": "dif_cross_zero_down",
                "close": float(df.iloc[i]["close"]),
                "dif": float(dif[i]),
                "dea": float(df.iloc[i]["dea"]),
                "macd": float(df.iloc[i]["macd"]),
            })

    return signals


def _find_local_peaks(values: np.ndarray, order: int) -> list[int]:
    """找局部极大值点索引（值需为 2*order+1 窗口内最大值）"""
    peaks = []
    for i in range(order, len(values) - order):
        window = values[i - order: i + order + 1]
        if not np.isnan(values[i]) and values[i] == np.nanmax(window):
            peaks.append(i)
    return peaks


def _find_local_troughs(values: np.ndarray, order: int) -> list[int]:
    """找局部极小值点索引"""
    troughs = []
    for i in range(order, len(values) - order):
        window = values[i - order: i + order + 1]
        if not np.isnan(values[i]) and values[i] == np.nanmin(window):
            troughs.append(i)
    return troughs


def detect_divergences(df: pd.DataFrame, order: int = 20) -> list[dict]:
    """检测顶背离和底背离

    顶背离: 连续两个价格局部高点中，价格创新高但 DIF（在价格高点处的值）未创新高
    底背离: 连续两个价格局部低点中，价格创新低但 DIF（在价格低点处的值）未创新低

    order: 局部极值判定的单侧窗口大小
    """
    signals = []
    close = df["close"].values
    dif = df["dif"].values

    # 找价格的局部极值
    peaks = _find_local_peaks(close, order)
    troughs = _find_local_troughs(close, order)

    # 顶背离: 价格新高 + DIF 未新高
    for j in range(1, len(peaks)):
        p1, p2 = peaks[j - 1], peaks[j]
        if close[p2] > close[p1] and dif[p2] < dif[p1]:
            signals.append({
                "idx": p2,
                "trade_date": df.iloc[p2]["trade_date"],
                "signal": "top_divergence",
                "close": float(close[p2]),
                "dif": float(dif[p2]),
                "dea": float(df.iloc[p2]["dea"]),
                "macd": float(df.iloc[p2]["macd"]),
            })

    # 底背离: 价格新低 + DIF 未新低
    for j in range(1, len(troughs)):
        t1, t2 = troughs[j - 1], troughs[j]
        if close[t2] < close[t1] and dif[t2] > dif[t1]:
            signals.append({
                "idx": t2,
                "trade_date": df.iloc[t2]["trade_date"],
                "signal": "bottom_divergence",
                "close": float(close[t2]),
                "dif": float(dif[t2]),
                "dea": float(df.iloc[t2]["dea"]),
                "macd": float(df.iloc[t2]["macd"]),
            })

    return signals


def detect_all_signals(df: pd.DataFrame, freq: str = "daily") -> pd.DataFrame:
    """检测所有 MACD 信号，汇总返回

    df: 必须包含 trade_date, close, dif, dea, macd 列，按日期升序
    freq: 周期，影响背离检测窗口大小

    返回: 信号 DataFrame，含 idx, trade_date, signal, close, dif, dea, macd 列
    """
    order = FREQ_ORDER.get(freq, 20)

    all_signals = []
    all_signals.extend(detect_crosses(df))
    all_signals.extend(detect_zero_crosses(df))
    all_signals.extend(detect_divergences(df, order=order))

    if not all_signals:
        return pd.DataFrame()

    result = pd.DataFrame(all_signals)
    result = result.sort_values("trade_date").reset_index(drop=True)
    return result
