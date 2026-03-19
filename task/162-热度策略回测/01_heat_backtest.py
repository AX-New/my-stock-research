"""
热度策略回测脚本（向量化版本）
策略逻辑：股票热度跌到低点时买入，涨到高点时卖出
数据来源：
  - 热度排名: my_trend.popularity_rank (东财人气排名, rank越小越热门)
  - 行情数据: my_stock.market_daily + adj_factor (前复权收盘价)
回测周期：近一年 (2025-03-15 ~ 2026-03-19)
"""

import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text

# ============================================================
# 配置
# ============================================================

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3307
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'

# 默认策略参数
LOOKBACK = 20          # 回看窗口天数
BUY_PERCENTILE = 0.80  # rank分位数 >= 此值 → 热度低谷 → 买入
SELL_PERCENTILE = 0.20  # rank分位数 <= 此值 → 热度高峰 → 卖出
MAX_HOLD_DAYS = 30     # 最大持仓交易日数
MIN_DATA_DAYS = 30     # 至少需要天数

START_DATE = '2025-03-15'
END_DATE = '2026-03-19'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ============================================================
# 数据加载
# ============================================================

def get_engine(db_name):
    return create_engine(
        f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4',
        pool_pre_ping=True,
    )


def load_all_data():
    """一次性加载所有需要的数据"""
    logger.info("加载数据...")

    trend_engine = get_engine('my_trend')
    stock_engine = get_engine('my_stock')

    # 热度数据
    heat_df = pd.read_sql(f"""
        SELECT stock_code, date, `rank`
        FROM popularity_rank
        WHERE date >= '{START_DATE}' AND date <= '{END_DATE}'
    """, trend_engine)
    heat_df['date'] = pd.to_datetime(heat_df['date']).dt.date
    logger.info(f"  热度: {len(heat_df)} 条, {heat_df['stock_code'].nunique()} 只")

    # 交易日
    td_df = pd.read_sql(f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1
          AND cal_date >= '{START_DATE.replace('-', '')}'
          AND cal_date <= '{END_DATE.replace('-', '')}'
    """, stock_engine)
    trading_days = sorted(pd.to_datetime(td_df['cal_date'], format='%Y%m%d').dt.date.tolist())
    td_set = set(trading_days)
    logger.info(f"  交易日: {len(trading_days)} 天")

    # 行情 + 复权
    price_df = pd.read_sql(f"""
        SELECT m.ts_code, m.trade_date, m.open, m.close, a.adj_factor
        FROM market_daily m
        JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        WHERE m.trade_date >= '{START_DATE.replace('-', '')}'
          AND m.trade_date <= '{END_DATE.replace('-', '')}'
    """, stock_engine)
    price_df['date'] = pd.to_datetime(price_df['trade_date'], format='%Y%m%d').dt.date
    price_df['stock_code'] = price_df['ts_code'].str[:6]
    # 前复权
    latest_adj = price_df.groupby('stock_code')['adj_factor'].transform('last')
    price_df['qfq_close'] = price_df['close'] * price_df['adj_factor'] / latest_adj
    logger.info(f"  行情: {len(price_df)} 条, {price_df['stock_code'].nunique()} 只")

    # 沪深300基准
    index_df = pd.read_sql(f"""
        SELECT trade_date, close as index_close FROM index_daily
        WHERE ts_code = '000300.SH'
          AND trade_date >= '{START_DATE.replace('-', '')}'
          AND trade_date <= '{END_DATE.replace('-', '')}'
        ORDER BY trade_date
    """, stock_engine)
    index_df['date'] = pd.to_datetime(index_df['trade_date'], format='%Y%m%d').dt.date

    return heat_df, trading_days, td_set, price_df, index_df


# ============================================================
# 向量化信号生成
# ============================================================

def generate_signals_vectorized(heat_df, td_set, lookback, buy_pct, sell_pct):
    """
    向量化生成买卖信号
    rank越大=越冷门（热度低），rank分位数高=热度低谷=买入
    """
    logger.info(f"生成信号 (lookback={lookback}, buy={buy_pct}, sell={sell_pct})...")

    # 只保留交易日的热度数据
    heat_td = heat_df[heat_df['date'].isin(td_set)].copy()

    # 按股票+日期排序
    heat_td = heat_td.sort_values(['stock_code', 'date'])

    # 使用 groupby + rolling 计算分位数
    # 分位数 = 窗口内 <= 当前值的比例
    def rolling_percentile(group):
        ranks = group['rank']
        # 使用 rolling apply 计算百分位
        result = ranks.rolling(window=lookback + 1, min_periods=max(lookback // 2, 5)).apply(
            lambda w: np.sum(w[:-1] <= w[-1]) / (len(w) - 1) if len(w) > 1 else 0.5,
            raw=True
        )
        return result

    heat_td['percentile'] = heat_td.groupby('stock_code', group_keys=False).apply(
        lambda g: rolling_percentile(g)
    ).values

    # 过滤有效信号
    heat_td = heat_td.dropna(subset=['percentile'])

    buy_signals = heat_td[heat_td['percentile'] >= buy_pct].copy()
    buy_signals['signal'] = 'BUY'
    sell_signals = heat_td[heat_td['percentile'] <= sell_pct].copy()
    sell_signals['signal'] = 'SELL'

    signals = pd.concat([buy_signals, sell_signals], ignore_index=True)
    signals = signals.sort_values(['stock_code', 'date'])

    buy_cnt = len(buy_signals)
    sell_cnt = len(sell_signals)
    logger.info(f"  信号: {len(signals)} (买={buy_cnt}, 卖={sell_cnt})")
    return signals[['stock_code', 'date', 'signal', 'rank', 'percentile']]


# ============================================================
# 向量化回测
# ============================================================

def backtest_vectorized(signal_df, price_df, trading_days, max_hold):
    """向量化回测引擎"""
    logger.info("执行回测...")

    # 构建交易日序号映射
    td_list = sorted(trading_days)
    td_map = {d: i for i, d in enumerate(td_list)}

    # 构建价格查询表
    price_lookup = price_df.set_index(['stock_code', 'date'])[['qfq_close', 'close']].to_dict('index')

    def get_next_td(d):
        idx = td_map.get(d)
        if idx is not None and idx + 1 < len(td_list):
            return td_list[idx + 1]
        return None

    def get_nth_td(d, n):
        idx = td_map.get(d)
        if idx is not None and idx + n < len(td_list):
            return td_list[idx + n]
        return None

    # 按股票分组回测
    trades = []
    grouped = signal_df.groupby('stock_code')
    total_groups = len(grouped)
    processed = 0

    for code, group in grouped:
        processed += 1
        if processed % 1000 == 0:
            logger.info(f"  回测: {processed}/{total_groups}")

        sigs = group.sort_values('date')
        holding = False
        entry_date = entry_qfq = entry_close = hold_start = None

        for _, sig in sigs.iterrows():
            sig_date = sig['date']

            # 持仓超时检查
            if holding and td_map.get(sig_date, 0) - hold_start >= max_hold:
                force_date = get_nth_td(entry_date, max_hold)
                if force_date:
                    pinfo = price_lookup.get((code, force_date))
                    if pinfo:
                        ret = (pinfo['qfq_close'] - entry_qfq) / entry_qfq
                        trades.append({
                            'stock_code': code,
                            'entry_date': entry_date,
                            'exit_date': force_date,
                            'entry_price': entry_close,
                            'exit_price': pinfo['close'],
                            'return': ret,
                            'hold_days': td_map[force_date] - hold_start,
                            'exit_reason': 'timeout',
                        })
                holding = False

            if not holding and sig['signal'] == 'BUY':
                exec_date = get_next_td(sig_date)
                if exec_date is None:
                    continue
                pinfo = price_lookup.get((code, exec_date))
                if pinfo is None:
                    continue
                holding = True
                entry_date = exec_date
                entry_qfq = pinfo['qfq_close']
                entry_close = pinfo['close']
                hold_start = td_map[exec_date]

            elif holding and sig['signal'] == 'SELL':
                exec_date = get_next_td(sig_date)
                if exec_date is None:
                    continue
                pinfo = price_lookup.get((code, exec_date))
                if pinfo is None:
                    continue
                ret = (pinfo['qfq_close'] - entry_qfq) / entry_qfq
                trades.append({
                    'stock_code': code,
                    'entry_date': entry_date,
                    'exit_date': exec_date,
                    'entry_price': entry_close,
                    'exit_price': pinfo['close'],
                    'return': ret,
                    'hold_days': td_map[exec_date] - hold_start,
                    'exit_reason': 'signal',
                })
                holding = False

        # 期末未平仓
        if holding:
            last_td = td_list[-1]
            pinfo = price_lookup.get((code, last_td))
            if pinfo:
                ret = (pinfo['qfq_close'] - entry_qfq) / entry_qfq
                trades.append({
                    'stock_code': code,
                    'entry_date': entry_date,
                    'exit_date': last_td,
                    'entry_price': entry_close,
                    'exit_price': pinfo['close'],
                    'return': ret,
                    'hold_days': td_map[last_td] - hold_start,
                    'exit_reason': 'end_of_period',
                })

    trade_df = pd.DataFrame(trades)
    logger.info(f"  交易笔数: {len(trade_df)}")
    return trade_df


# ============================================================
# 分析报告
# ============================================================

def build_report(trade_df, index_df, params):
    """生成 Markdown 报告"""
    if len(trade_df) == 0:
        return "# 回测无交易\n\n未产生任何交易记录。\n"

    lookback, buy_pct, sell_pct, max_hold = params
    lines = []

    lines.append("# 热度低买高卖策略回测报告")
    lines.append(f"**创建时间**: {datetime.now().strftime('%Y%m%d %H:%M')}")
    lines.append("")

    # --- 策略说明 ---
    lines.append("## 策略说明")
    lines.append("")
    lines.append("**核心逻辑**: 当个股东财人气排名跌至近期低谷（热度低点，rank处于近期高位）时买入，"
                  "排名回升至近期高峰（热度高点，rank处于近期低位）时卖出。")
    lines.append("")
    lines.append("**数据基础**: 东财人气排名（`popularity_rank`），rank=1为全市场最热门，"
                  "rank越大越冷门。每日覆盖约5,490只个股。")
    lines.append("")
    lines.append("**信号定义**:")
    lines.append(f"- 买入: 个股rank在过去{lookback}天滚动窗口内 >= {int(buy_pct*100)}%分位"
                  f"（热度处于近期低谷）")
    lines.append(f"- 卖出: 个股rank在过去{lookback}天滚动窗口内 <= {int(sell_pct*100)}%分位"
                  f"（热度处于近期高峰）")
    lines.append(f"- 最大持仓: {max_hold}个交易日，超时强制平仓")
    lines.append(f"- 执行价格: 信号次日收盘价")
    lines.append(f"- 回测区间: {START_DATE} ~ {END_DATE}")
    lines.append("")

    # --- 总体统计 ---
    lines.append("## 总体统计")
    lines.append("")

    total = len(trade_df)
    wins = len(trade_df[trade_df['return'] > 0])
    losses = len(trade_df[trade_df['return'] < 0])
    win_rate = wins / total * 100
    avg_ret = trade_df['return'].mean() * 100
    med_ret = trade_df['return'].median() * 100
    std_ret = trade_df['return'].std() * 100
    avg_hold = trade_df['hold_days'].mean()
    max_ret = trade_df['return'].max() * 100
    min_ret = trade_df['return'].min() * 100

    # 盈亏比
    avg_win = trade_df[trade_df['return'] > 0]['return'].mean() * 100 if wins > 0 else 0
    avg_loss = trade_df[trade_df['return'] < 0]['return'].mean() * 100 if losses > 0 else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')

    lines.append("| 指标 | 值 |")
    lines.append("|------|-----|")
    lines.append(f"| 总交易笔数 | {total:,} |")
    lines.append(f"| 涉及股票数 | {trade_df['stock_code'].nunique():,} |")
    lines.append(f"| 盈利 / 亏损 / 持平 | {wins:,} / {losses:,} / {total - wins - losses:,} |")
    lines.append(f"| **胜率** | **{win_rate:.1f}%** |")
    lines.append(f"| **平均收益率** | **{avg_ret:.2f}%** |")
    lines.append(f"| 中位数收益率 | {med_ret:.2f}% |")
    lines.append(f"| 收益标准差 | {std_ret:.2f}% |")
    lines.append(f"| 平均盈利 / 平均亏损 | {avg_win:.2f}% / {avg_loss:.2f}% |")
    lines.append(f"| **盈亏比** | **{profit_factor:.2f}** |")
    lines.append(f"| 最大单笔盈利 | {max_ret:.2f}% |")
    lines.append(f"| 最大单笔亏损 | {min_ret:.2f}% |")
    lines.append(f"| 平均持仓天数 | {avg_hold:.1f} |")
    lines.append("")

    # --- 按退出原因 ---
    lines.append("## 按退出原因分析")
    lines.append("")
    lines.append("| 退出原因 | 笔数 | 占比 | 胜率 | 平均收益 | 平均持仓 |")
    lines.append("|---------|------|------|------|---------|---------|")
    for reason, label in [('signal', '信号平仓'), ('timeout', '超时平仓'), ('end_of_period', '期末平仓')]:
        sub = trade_df[trade_df['exit_reason'] == reason]
        if len(sub) > 0:
            wr = len(sub[sub['return'] > 0]) / len(sub) * 100
            ar = sub['return'].mean() * 100
            ah = sub['hold_days'].mean()
            pct = len(sub) / total * 100
            lines.append(f"| {label} | {len(sub):,} | {pct:.1f}% | {wr:.1f}% | {ar:.2f}% | {ah:.1f} |")
    lines.append("")

    # --- 收益分布 ---
    lines.append("## 收益分布")
    lines.append("")
    bins = [(-np.inf, -0.20), (-0.20, -0.10), (-0.10, -0.05), (-0.05, 0),
            (0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, np.inf)]
    labels = ['< -20%', '-20%~-10%', '-10%~-5%', '-5%~0%',
              '0%~5%', '5%~10%', '10%~20%', '> 20%']

    lines.append("| 收益区间 | 笔数 | 占比 |")
    lines.append("|---------|------|------|")
    for (lo, hi), label in zip(bins, labels):
        cnt = len(trade_df[(trade_df['return'] > lo) & (trade_df['return'] <= hi)])
        pct = cnt / total * 100
        lines.append(f"| {label} | {cnt:,} | {pct:.1f}% |")
    lines.append("")

    # --- 按月统计 ---
    lines.append("## 按月统计")
    lines.append("")
    tc = trade_df.copy()
    tc['month'] = pd.to_datetime(tc['entry_date']).dt.to_period('M')
    monthly = tc.groupby('month').agg(
        trades=('return', 'count'),
        win_rate=('return', lambda x: (x > 0).sum() / len(x) * 100),
        avg_return=('return', lambda x: x.mean() * 100),
        total_return=('return', 'sum'),
    ).reset_index()

    lines.append("| 月份 | 笔数 | 胜率 | 平均收益 | 月度累计收益 |")
    lines.append("|------|------|------|---------|-----------|")
    for _, row in monthly.iterrows():
        lines.append(f"| {row['month']} | {row['trades']} | {row['win_rate']:.1f}% | "
                      f"{row['avg_return']:.2f}% | {row['total_return']*100:.1f}% |")
    lines.append("")

    # --- 基准对比 ---
    if index_df is not None and len(index_df) > 1:
        lines.append("## 与沪深300对比")
        lines.append("")
        idx_start = index_df.iloc[0]['index_close']
        idx_end = index_df.iloc[-1]['index_close']
        idx_return = (idx_end - idx_start) / idx_start * 100

        lines.append(f"| 指标 | 策略 | 沪深300 |")
        lines.append(f"|------|------|---------|")
        lines.append(f"| 每笔平均收益 | {avg_ret:.2f}% | - |")
        lines.append(f"| 区间涨跌幅 | - | {idx_return:.2f}% |")
        lines.append(f"| 胜率 | {win_rate:.1f}% | - |")
        lines.append("")

    # --- 持仓天数 vs 收益 ---
    lines.append("## 持仓天数与收益")
    lines.append("")
    hold_bins = [(1, 3), (3, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 100)]
    hold_labels = ['1-2天', '3-4天', '5-9天', '10-14天', '15-19天', '20-29天', '30+天']

    lines.append("| 持仓区间 | 笔数 | 胜率 | 平均收益 |")
    lines.append("|---------|------|------|---------|")
    for (lo, hi), label in zip(hold_bins, hold_labels):
        sub = trade_df[(trade_df['hold_days'] >= lo) & (trade_df['hold_days'] < hi)]
        if len(sub) > 0:
            wr = len(sub[sub['return'] > 0]) / len(sub) * 100
            ar = sub['return'].mean() * 100
            lines.append(f"| {label} | {len(sub):,} | {wr:.1f}% | {ar:.2f}% |")
    lines.append("")

    # --- Top 10 ---
    lines.append("## Top 10 盈利交易")
    lines.append("")
    lines.append("| 股票 | 买入日期 | 卖出日期 | 持仓 | 收益率 | 退出 |")
    lines.append("|------|---------|---------|------|-------|------|")
    for _, row in trade_df.nlargest(10, 'return').iterrows():
        lines.append(f"| {row['stock_code']} | {row['entry_date']} | {row['exit_date']} | "
                      f"{row['hold_days']}天 | {row['return']*100:.2f}% | {row['exit_reason']} |")
    lines.append("")

    lines.append("## Top 10 亏损交易")
    lines.append("")
    lines.append("| 股票 | 买入日期 | 卖出日期 | 持仓 | 收益率 | 退出 |")
    lines.append("|------|---------|---------|------|-------|------|")
    for _, row in trade_df.nsmallest(10, 'return').iterrows():
        lines.append(f"| {row['stock_code']} | {row['entry_date']} | {row['exit_date']} | "
                      f"{row['hold_days']}天 | {row['return']*100:.2f}% | {row['exit_reason']} |")
    lines.append("")

    # --- 参数 ---
    lines.append("## 参数设置")
    lines.append("")
    lines.append(f"| 参数 | 值 | 说明 |")
    lines.append(f"|------|-----|------|")
    lines.append(f"| LOOKBACK | {lookback} | 热度分位数回看窗口（天） |")
    lines.append(f"| BUY_PERCENTILE | {buy_pct} | 买入阈值 |")
    lines.append(f"| SELL_PERCENTILE | {sell_pct} | 卖出阈值 |")
    lines.append(f"| MAX_HOLD_DAYS | {max_hold} | 最大持仓交易日 |")
    lines.append("")

    return "\n".join(lines)


# ============================================================
# 参数敏感性测试
# ============================================================

def sensitivity_test(heat_df, price_df, trading_days, td_set):
    """快速参数扫描"""
    logger.info("参数敏感性测试...")

    combos = [
        (10, 0.80, 0.20, 30),
        (10, 0.90, 0.10, 30),
        (20, 0.80, 0.20, 30),
        (20, 0.90, 0.10, 30),
        (30, 0.80, 0.20, 30),
        (30, 0.90, 0.10, 30),
        (20, 0.70, 0.30, 30),
        (20, 0.85, 0.15, 30),
        (20, 0.80, 0.20, 15),
        (20, 0.80, 0.20, 60),
    ]

    results = []
    for lb, bp, sp, mh in combos:
        logger.info(f"  测试: lb={lb} buy={bp} sell={sp} hold={mh}")
        sigs = generate_signals_vectorized(heat_df, td_set, lb, bp, sp)
        if len(sigs) == 0:
            continue
        trades = backtest_vectorized(sigs, price_df, trading_days, mh)
        if len(trades) == 0:
            continue

        wins = len(trades[trades['return'] > 0])
        results.append({
            'lookback': lb, 'buy_pct': bp, 'sell_pct': sp, 'max_hold': mh,
            'trades': len(trades),
            'win_rate': wins / len(trades) * 100,
            'avg_return': trades['return'].mean() * 100,
            'median_return': trades['return'].median() * 100,
            'avg_hold': trades['hold_days'].mean(),
        })

    return pd.DataFrame(results)


# ============================================================
# 主函数
# ============================================================

def main():
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("热度低买高卖策略回测")
    logger.info("=" * 60)

    # 1. 加载数据
    heat_df, trading_days, td_set, price_df, index_df = load_all_data()

    # 2. 默认参数回测
    signals = generate_signals_vectorized(heat_df, td_set, LOOKBACK, BUY_PERCENTILE, SELL_PERCENTILE)
    if len(signals) == 0:
        logger.error("无信号，退出")
        return
    trades = backtest_vectorized(signals, price_df, trading_days, MAX_HOLD_DAYS)
    if len(trades) == 0:
        logger.error("无交易，退出")
        return

    # 3. 生成报告
    report = build_report(trades, index_df, (LOOKBACK, BUY_PERCENTILE, SELL_PERCENTILE, MAX_HOLD_DAYS))

    # 4. 参数敏感性
    sens_df = sensitivity_test(heat_df, price_df, trading_days, td_set)
    if len(sens_df) > 0:
        report += "\n## 参数敏感性测试\n\n"
        report += "| 回看 | 买入阈值 | 卖出阈值 | 最大持仓 | 笔数 | 胜率 | 平均收益 | 中位数收益 | 平均持仓 |\n"
        report += "|------|---------|---------|---------|------|------|---------|----------|--------|\n"
        for _, r in sens_df.iterrows():
            report += (f"| {r['lookback']} | {r['buy_pct']} | {r['sell_pct']} | {r['max_hold']} | "
                       f"{r['trades']:,} | {r['win_rate']:.1f}% | {r['avg_return']:.2f}% | "
                       f"{r['median_return']:.2f}% | {r['avg_hold']:.1f} |\n")
        report += "\n"

    # 5. 保存
    output_dir = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(output_dir, '热度低买高卖策略回测报告.md')
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"报告: {report_path}")

    trade_path = os.path.join(output_dir, 'trades.csv')
    trades.to_csv(trade_path, index=False, encoding='utf-8-sig')
    logger.info(f"明细: {trade_path}")

    elapsed = time.time() - t0
    logger.info(f"完成，耗时 {elapsed:.1f}秒")


if __name__ == '__main__':
    main()
