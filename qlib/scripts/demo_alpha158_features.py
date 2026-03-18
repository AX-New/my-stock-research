"""Qlib Alpha158 因子体系复现（纯 Python 实现）

复现 Microsoft Qlib 的 Alpha158 因子工程核心逻辑，不依赖 Qlib 包。
Alpha158 包含 158 个因子，涵盖价量、动量、波动、相关性等维度。

本示例实现其中最重要的 ~50 个因子，展示因子工程的思路和方法。

因子分类：
  1. 价格类 (KBAR): 开高低收的比例关系
  2. 动量类 (MOM): 不同周期收益率
  3. 量价类 (VOLUME): 成交量相关因子
  4. 波动类 (VOLATILITY): 滚动波动率
  5. 技术指标 (TECH): RSI/MACD/均线等
  6. 相关性 (CORR): 价量相关性

用法:
  python research/qlib/demo_alpha158_features.py --code 000001.SZ
  python research/qlib/demo_alpha158_features.py --code 600519.SH --top 20
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.database import engine
from app.logger import get_logger

log = get_logger("research.qlib.alpha158")


def load_data(ts_code: str) -> pd.DataFrame:
    """加载前复权日线 + 每日指标"""
    # 日线数据
    sql = text("""
        SELECT d.trade_date, d.open, d.high, d.low, d.close, d.vol, d.amount,
               d.pre_close, d.pct_chg,
               b.turnover_rate, b.volume_ratio
        FROM market_daily d
        LEFT JOIN daily_basic b ON d.ts_code = b.ts_code AND d.trade_date = b.trade_date
        WHERE d.ts_code = :code
        ORDER BY d.trade_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    log.info(f"加载 {ts_code}: {len(df)} 行")
    return df


def compute_alpha158(df: pd.DataFrame) -> pd.DataFrame:
    """计算 Alpha158 因子（核心子集约50个）

    返回因子 DataFrame，每行一个交易日，每列一个因子。
    """
    o, h, l, c, v = df['open'], df['high'], df['low'], df['close'], df['vol']
    amount = df['amount']
    vwap = amount / (v + 1e-8)  # 成交均价

    factors = pd.DataFrame(index=df.index)
    factors['trade_date'] = df['trade_date']

    # ════════════════════ 1. KBAR 价格类因子 ════════════════════
    factors['KBAR_open'] = o / (c + 1e-8)        # 开盘价/收盘价
    factors['KBAR_high'] = h / (c + 1e-8)        # 最高价/收盘价
    factors['KBAR_low'] = l / (c + 1e-8)         # 最低价/收盘价
    factors['KBAR_close'] = c / (o + 1e-8)       # 收盘价/开盘价
    factors['KBAR_mid'] = (o + c) / 2 / (h + 1e-8)  # 中间价/最高价
    factors['KBAR_upper'] = h - np.maximum(o, c)  # 上影线
    factors['KBAR_lower'] = np.minimum(o, c) - l  # 下影线
    factors['KBAR_body'] = abs(c - o)            # 实体

    # ════════════════════ 2. 动量类因子 ════════════════════
    for d in [1, 2, 3, 5, 10, 20, 30, 60]:
        factors[f'MOM_ret_{d}'] = c.pct_change(d)
    # 最高/最低价动量
    for d in [5, 10, 20]:
        factors[f'MOM_high_{d}'] = h.rolling(d).max() / c - 1
        factors[f'MOM_low_{d}'] = c / (l.rolling(d).min() + 1e-8) - 1

    # ════════════════════ 3. 量价类因子 ════════════════════
    for d in [5, 10, 20, 60]:
        factors[f'VOL_ma_{d}'] = v.rolling(d).mean() / (v + 1e-8)
    factors['VOL_ratio'] = v / (v.rolling(5).mean() + 1e-8)
    # 量价相关性
    for d in [5, 10, 20]:
        factors[f'CORR_cv_{d}'] = c.rolling(d).corr(v)

    # ════════════════════ 4. 波动类因子 ════════════════════
    ret = c.pct_change()
    for d in [5, 10, 20, 60]:
        factors[f'VOLATILITY_{d}'] = ret.rolling(d).std()

    # 真实波动幅度 (ATR)
    tr = pd.concat([
        h - l,
        abs(h - c.shift(1)),
        abs(l - c.shift(1)),
    ], axis=1).max(axis=1)
    for d in [5, 14, 20]:
        factors[f'ATR_{d}'] = tr.rolling(d).mean()

    # ════════════════════ 5. 技术指标因子 ════════════════════
    # RSI
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    for d in [6, 12, 24]:
        avg_g = gain.rolling(d).mean()
        avg_l = loss.rolling(d).mean()
        factors[f'RSI_{d}'] = 100 - 100 / (1 + avg_g / (avg_l + 1e-8))

    # MACD
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    factors['MACD_dif'] = dif
    factors['MACD_dea'] = dea
    factors['MACD_hist'] = 2 * (dif - dea)

    # 均线偏离
    for d in [5, 10, 20, 60]:
        ma = c.rolling(d).mean()
        factors[f'MA_bias_{d}'] = (c - ma) / (ma + 1e-8)

    # VWAP 偏离
    factors['VWAP_bias'] = (c - vwap) / (vwap + 1e-8)

    # 换手率（如果有）
    if 'turnover_rate' in df.columns:
        tr_rate = df['turnover_rate']
        factors['TURN'] = tr_rate
        for d in [5, 10, 20]:
            factors[f'TURN_ma_{d}'] = tr_rate.rolling(d).mean()

    return factors


def analyze_factor_importance(factors: pd.DataFrame, df: pd.DataFrame, top_n: int = 20):
    """分析因子与次日收益的相关性（IC值）"""
    # 次日收益
    next_ret = df['pct_chg'].shift(-1) / 100
    factor_cols = [c for c in factors.columns if c != 'trade_date']

    ic_results = []
    for col in factor_cols:
        valid = factors[col].notna() & next_ret.notna()
        if valid.sum() < 100:
            continue
        ic = factors.loc[valid, col].corr(next_ret[valid])
        # 计算 IC 的均值和 IR（IC 的 t 值）
        rolling_ic = factors[col].rolling(20).corr(next_ret)
        ic_mean = rolling_ic.mean()
        ic_std = rolling_ic.std()
        icir = ic_mean / (ic_std + 1e-8)

        ic_results.append({
            'factor': col,
            'IC': ic,
            'IC_mean': ic_mean,
            'ICIR': icir,
            'abs_IC': abs(ic),
        })

    ic_df = pd.DataFrame(ic_results).sort_values('abs_IC', ascending=False)

    print(f"\n{'='*70}")
    print(f"  Alpha158 因子 IC 排名 (Top {top_n})")
    print(f"{'='*70}")
    print(f"  {'因子':<20s} {'IC':>8s} {'IC均值':>8s} {'ICIR':>8s}")
    print(f"  {'─'*20} {'─'*8} {'─'*8} {'─'*8}")

    for _, row in ic_df.head(top_n).iterrows():
        print(f"  {row['factor']:<20s} {row['IC']:>8.4f} {row['IC_mean']:>8.4f} {row['ICIR']:>8.4f}")

    print(f"\n  共计算 {len(ic_df)} 个因子")
    print(f"  |IC| > 0.02 的因子: {(ic_df['abs_IC'] > 0.02).sum()} 个")
    print(f"  |IC| > 0.05 的因子: {(ic_df['abs_IC'] > 0.05).sum()} 个")

    return ic_df


def main():
    parser = argparse.ArgumentParser(description="Alpha158 因子计算")
    parser.add_argument('--code', default='000001.SZ', help='股票代码')
    parser.add_argument('--top', type=int, default=20, help='显示前N个因子')
    args = parser.parse_args()

    t0 = time.time()

    df = load_data(args.code)
    if len(df) < 200:
        log.error("数据不足")
        return

    factors = compute_alpha158(df)
    log.info(f"计算完成: {factors.shape[1]-1} 个因子, {len(factors)} 行")

    ic_df = analyze_factor_importance(factors, df, top_n=args.top)

    # 打印因子统计摘要
    factor_cols = [c for c in factors.columns if c != 'trade_date']
    desc = factors[factor_cols].describe()
    print(f"\n因子统计摘要 (非NaN行: {factors[factor_cols].dropna().shape[0]}):")
    print(f"  因子总数: {len(factor_cols)}")

    print(f"\n耗时: {time.time()-t0:.1f}s")


if __name__ == '__main__':
    main()
