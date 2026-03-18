"""涨跌分布分析与可视化

分析个股和市场的涨跌幅统计分布特征：
  - 日收益率分布（直方图 + 正态拟合）
  - 涨跌天数统计
  - 连涨/连跌天数分布
  - 不同市场状态下的分布差异
  - 尾部风险分析（极端涨跌概率）

此脚本不依赖 PyTorch，纯统计分析，为后续深度学习建模提供数据洞察。

用法:
  python research/pytorch/demo_price_distribution.py --code 000001.SZ
  python research/pytorch/demo_price_distribution.py --code 600519.SH --save
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

from app.database import engine
from app.logger import get_logger

log = get_logger("research.pytorch.price_distribution")


def load_data(ts_code: str) -> pd.DataFrame:
    """加载前复权日线"""
    sql = text("""
        SELECT trade_date, open, high, low, close, vol, pct_chg
        FROM market_daily
        WHERE ts_code = :code
        ORDER BY trade_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    log.info(f"加载 {ts_code}: {len(df)} 行")
    return df


def analyze_return_distribution(df: pd.DataFrame, code: str):
    """分析日收益率分布"""
    returns = df['pct_chg'].dropna() / 100  # 转为小数

    print(f"\n{'='*60}")
    print(f"  {code} 日收益率分布分析")
    print(f"{'='*60}")

    # 基本统计
    print(f"\n【基本统计量】")
    print(f"  样本数:    {len(returns)}")
    print(f"  均值:      {returns.mean():.6f} ({returns.mean()*100:.4f}%)")
    print(f"  中位数:    {returns.median():.6f}")
    print(f"  标准差:    {returns.std():.6f}")
    print(f"  偏度:      {returns.skew():.4f}")
    print(f"  峰度:      {returns.kurtosis():.4f}")
    print(f"  最大涨幅:  {returns.max()*100:.2f}%")
    print(f"  最大跌幅:  {returns.min()*100:.2f}%")

    # 涨跌天数
    n_up = (returns > 0).sum()
    n_down = (returns < 0).sum()
    n_flat = (returns == 0).sum()
    print(f"\n【涨跌天数】")
    print(f"  上涨: {n_up} ({n_up/len(returns)*100:.1f}%)")
    print(f"  下跌: {n_down} ({n_down/len(returns)*100:.1f}%)")
    print(f"  平盘: {n_flat} ({n_flat/len(returns)*100:.1f}%)")

    # 分区间统计
    bins = [(-999, -0.05), (-0.05, -0.03), (-0.03, -0.01), (-0.01, 0),
            (0, 0.01), (0.01, 0.03), (0.03, 0.05), (0.05, 999)]
    labels = ['<-5%', '-5~-3%', '-3~-1%', '-1~0%',
              '0~1%', '1~3%', '3~5%', '>5%']

    print(f"\n【涨跌幅分布】")
    for (lo, hi), label in zip(bins, labels):
        count = ((returns >= lo) & (returns < hi)).sum()
        pct = count / len(returns) * 100
        bar = '█' * int(pct)
        print(f"  {label:>8s}: {count:5d} ({pct:5.1f}%) {bar}")

    # 正态性检验
    _, p_value = stats.normaltest(returns)
    print(f"\n【正态性检验 (D'Agostino-Pearson)】")
    print(f"  p-value: {p_value:.2e}")
    print(f"  结论: {'符合正态分布' if p_value > 0.05 else '不符合正态分布（厚尾特征）'}")

    # 尾部风险
    print(f"\n【尾部风险分析】")
    for threshold in [3, 5, 7, 10]:
        up_pct = (returns > threshold/100).mean() * 100
        down_pct = (returns < -threshold/100).mean() * 100
        print(f"  涨幅>{threshold}%: {up_pct:.2f}%  跌幅>{threshold}%: {down_pct:.2f}%")

    return returns


def analyze_streaks(df: pd.DataFrame, code: str):
    """分析连涨/连跌天数"""
    returns = df['pct_chg'].dropna()
    signs = np.sign(returns)

    # 计算连续涨跌天数
    streaks = []
    current_sign = 0
    current_len = 0

    for s in signs:
        if s == current_sign and s != 0:
            current_len += 1
        else:
            if current_len > 0:
                streaks.append((current_sign, current_len))
            current_sign = s
            current_len = 1
    if current_len > 0:
        streaks.append((current_sign, current_len))

    up_streaks = [l for s, l in streaks if s > 0]
    down_streaks = [l for s, l in streaks if s < 0]

    print(f"\n【连涨/连跌分析】")
    print(f"  最长连涨: {max(up_streaks) if up_streaks else 0} 天")
    print(f"  最长连跌: {max(down_streaks) if down_streaks else 0} 天")
    print(f"  平均连涨: {np.mean(up_streaks):.1f} 天")
    print(f"  平均连跌: {np.mean(down_streaks):.1f} 天")

    # 连涨/连跌天数分布
    for name, data in [('连涨', up_streaks), ('连跌', down_streaks)]:
        print(f"\n  {name}天数分布:")
        for d in range(1, 8):
            cnt = sum(1 for x in data if x == d)
            pct = cnt / len(data) * 100 if data else 0
            print(f"    {d}天: {cnt:4d} ({pct:5.1f}%)")
        cnt = sum(1 for x in data if x >= 8)
        pct = cnt / len(data) * 100 if data else 0
        print(f"    8+天: {cnt:4d} ({pct:5.1f}%)")


def analyze_regime_returns(df: pd.DataFrame, code: str):
    """不同市场状态下的收益分布差异"""
    c = df['close']
    ret = df['pct_chg'] / 100

    # 用 MA60 判断牛熊
    ma60 = c.rolling(60).mean()
    bull = ret[c > ma60].dropna()
    bear = ret[c <= ma60].dropna()

    print(f"\n【牛熊状态下收益差异 (以MA60为分界)】")
    print(f"  {'指标':>10s}  {'牛市(价>MA60)':>14s}  {'熊市(价≤MA60)':>14s}")
    print(f"  {'─'*10}  {'─'*14}  {'─'*14}")
    print(f"  {'日均收益':>10s}  {bull.mean()*100:>13.4f}%  {bear.mean()*100:>13.4f}%")
    print(f"  {'波动率':>10s}  {bull.std()*100:>13.4f}%  {bear.std()*100:>13.4f}%")
    print(f"  {'上涨概率':>10s}  {(bull>0).mean()*100:>12.1f}%  {(bear>0).mean()*100:>12.1f}%")
    print(f"  {'夏普比':>10s}  {bull.mean()/bull.std()*np.sqrt(252):>14.4f}  "
          f"{bear.mean()/bear.std()*np.sqrt(252):>14.4f}")

    # 检验均值是否有显著差异
    t_stat, p_val = stats.ttest_ind(bull, bear)
    print(f"\n  t检验: t={t_stat:.4f}, p={p_val:.4e}")
    print(f"  结论: {'牛熊收益有显著差异' if p_val < 0.05 else '差异不显著'}")


def main():
    parser = argparse.ArgumentParser(description="涨跌分布分析")
    parser.add_argument('--code', default='000001.SZ', help='股票代码')
    args = parser.parse_args()

    t0 = time.time()
    df = load_data(args.code)

    if len(df) < 100:
        log.error("数据不足")
        return

    analyze_return_distribution(df, args.code)
    analyze_streaks(df, args.code)
    analyze_regime_returns(df, args.code)

    print(f"\n耗时: {time.time()-t0:.1f}s")


if __name__ == '__main__':
    main()
