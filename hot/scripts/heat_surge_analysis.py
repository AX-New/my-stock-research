"""
热度飙升选股有效性分析

核心逻辑：
1. 用 my_trend.popularity_rank 日快照，计算每日每只股票的 rank_change = 昨日rank - 今日rank
2. 选出 rank_change 最大的 Top20（热度飙升最快的股票）
3. 用 my_stock.market_daily 验证这些股票后续 T+1/T+3/T+5/T+10/T+20 的收益表现
4. 全量统计胜率、平均收益、中位收益等

这对应 my-trend 项目中 heat_live/analyze.py 的逻辑：
  盘中实时收集 popularity_rank_live → 与前日 popularity_rank 对比 → 选出 heat_change_top

本脚本用历史数据回溯验证该选股信号的有效性。
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ========== 数据库连接 ==========
# my_trend: SSH隧道 3310 端口
trend_engine = create_engine(
    'mysql+pymysql://root:root@127.0.0.1:3310/my_trend',
    pool_pre_ping=True
)
# my_stock: 本地 3307 端口
stock_engine = create_engine(
    'mysql+pymysql://root:root@127.0.0.1:3307/my_stock',
    pool_pre_ping=True
)


def load_popularity_rank():
    """加载全量热度排名数据"""
    print("加载 popularity_rank 数据...")
    sql = """
    SELECT stock_code, stock_name, date, `rank`, new_price, change_rate,
           volume_ratio, turnover_rate, deal_amount
    FROM popularity_rank
    WHERE date >= '2025-03-15'
    ORDER BY date, `rank`
    """
    df = pd.read_sql(sql, trend_engine)
    print(f"  共 {len(df):,} 条记录，{df['date'].nunique()} 个交易日")
    print(f"  日期范围: {df['date'].min()} ~ {df['date'].max()}")
    return df


def load_market_daily():
    """加载行情数据（前复权收盘价）"""
    print("加载 market_daily 数据...")
    sql = """
    SELECT m.ts_code, m.trade_date, m.close, a.adj_factor
    FROM market_daily m
    JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
    WHERE m.trade_date >= '20250301'
    """
    df = pd.read_sql(sql, stock_engine)
    # ts_code -> stock_code: 000001.SZ -> 000001
    df['stock_code'] = df['ts_code'].str[:6]
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    # 前复权价格
    latest_adj = df.groupby('stock_code')['adj_factor'].transform('last')
    df['qfq_close'] = df['close'] * df['adj_factor'] / latest_adj
    print(f"  共 {len(df):,} 条记录")
    return df


def calc_daily_rank_change(pop_df):
    """计算每日排名变化量"""
    print("计算每日 rank_change...")
    dates = sorted(pop_df['date'].unique())

    results = []
    for i in range(1, len(dates)):
        today = dates[i]
        yesterday = dates[i - 1]

        today_df = pop_df[pop_df['date'] == today][['stock_code', 'stock_name', 'rank', 'new_price',
                                                      'change_rate', 'volume_ratio', 'turnover_rate',
                                                      'deal_amount']].copy()
        yesterday_df = pop_df[pop_df['date'] == yesterday][['stock_code', 'rank']].copy()

        # 合并计算 rank_change
        merged = today_df.merge(yesterday_df, on='stock_code', suffixes=('_today', '_yesterday'))
        merged['rank_change'] = merged['rank_yesterday'] - merged['rank_today']
        merged['date'] = today

        # 取 Top20（热度飙升最快）
        top20 = merged.nlargest(20, 'rank_change')
        results.append(top20)

    result_df = pd.concat(results, ignore_index=True)
    print(f"  共 {len(result_df):,} 条信号，覆盖 {result_df['date'].nunique()} 个交易日")
    return result_df


def calc_forward_returns(signals_df, market_df):
    """计算信号股票的未来收益"""
    print("计算未来收益...")

    # 构建 stock_code + date 的价格查找表
    market_df = market_df.sort_values(['stock_code', 'trade_date'])

    # 为每只股票建立日期索引（用字符串日期避免类型问题）
    price_dict = {}
    for code, grp in market_df.groupby('stock_code'):
        grp = grp.sort_values('trade_date')
        # 用日期字符串作为索引
        date_strs = grp['trade_date'].dt.strftime('%Y-%m-%d').values
        prices = grp['qfq_close'].values
        price_dict[code] = (date_strs, prices)

    periods = [1, 3, 5, 10, 20]
    for p in periods:
        signals_df[f'ret_T{p}'] = np.nan

    found = 0
    not_found = 0

    for idx, row in signals_df.iterrows():
        code = row['stock_code']
        sig_date_str = str(row['date'])  # datetime.date -> '2025-03-15'

        if code not in price_dict:
            not_found += 1
            continue

        date_strs, prices = price_dict[code]

        # 找到信号日在行情中的位置
        pos_arr = np.where(date_strs == sig_date_str)[0]
        if len(pos_arr) == 0:
            not_found += 1
            continue

        pos = pos_arr[0]
        buy_price = prices[pos]
        found += 1

        for p in periods:
            sell_pos = pos + p
            if sell_pos < len(prices):
                sell_price = prices[sell_pos]
                signals_df.at[idx, f'ret_T{p}'] = (sell_price / buy_price - 1) * 100

    print(f"  匹配成功: {found}, 未匹配: {not_found}")
    return signals_df


def analyze_results(signals_df):
    """分析信号有效性"""
    periods = [1, 3, 5, 10, 20]

    print("\n" + "=" * 80)
    print("热度飙升选股（每日Top20）信号有效性分析")
    print("=" * 80)

    # 基本统计
    print(f"\n信号总数: {len(signals_df)}")
    print(f"覆盖交易日: {signals_df['date'].nunique()}")
    print(f"日期范围: {signals_df['date'].min()} ~ {signals_df['date'].max()}")

    # 排名变化分布
    print(f"\nrank_change 分布:")
    print(f"  均值: {signals_df['rank_change'].mean():.0f}")
    print(f"  中位数: {signals_df['rank_change'].median():.0f}")
    print(f"  最小值: {signals_df['rank_change'].min():.0f}")
    print(f"  最大值: {signals_df['rank_change'].max():.0f}")

    # 各周期收益统计
    print(f"\n{'周期':<8} {'样本数':<8} {'胜率%':<8} {'均值%':<10} {'中位数%':<10} {'标准差%':<10} {'最大涨%':<10} {'最大跌%':<10}")
    print("-" * 74)

    results_summary = {}
    for p in periods:
        col = f'ret_T{p}'
        valid = signals_df[col].dropna()
        n = len(valid)
        if n == 0:
            continue
        win_rate = (valid > 0).mean() * 100
        mean_ret = valid.mean()
        median_ret = valid.median()
        std_ret = valid.std()
        max_ret = valid.max()
        min_ret = valid.min()
        print(f"T+{p:<5} {n:<8} {win_rate:<8.1f} {mean_ret:<10.2f} {median_ret:<10.2f} "
              f"{std_ret:<10.2f} {max_ret:<10.2f} {min_ret:<10.2f}")
        results_summary[f'T+{p}'] = {
            'n': n, 'win_rate': win_rate, 'mean_ret': mean_ret,
            'median_ret': median_ret, 'std': std_ret
        }

    # 与大盘基准对比
    print("\n\n与大盘（沪深300）基准对比:")
    benchmark_returns = calc_benchmark_returns(signals_df)
    if benchmark_returns:
        print(f"{'周期':<8} {'信号均值%':<12} {'基准均值%':<12} {'超额收益%':<12} {'信号胜率%':<12} {'基准胜率%':<12}")
        print("-" * 56)
        for p in periods:
            col = f'ret_T{p}'
            valid = signals_df[col].dropna()
            if len(valid) == 0:
                continue
            bm = benchmark_returns.get(f'T+{p}', {})
            sig_mean = valid.mean()
            bm_mean = bm.get('mean', 0)
            excess = sig_mean - bm_mean
            sig_wr = (valid > 0).mean() * 100
            bm_wr = bm.get('win_rate', 0)
            print(f"T+{p:<5} {sig_mean:<12.2f} {bm_mean:<12.2f} {excess:<12.2f} {sig_wr:<12.1f} {bm_wr:<12.1f}")

    # 按 rank_change 分档分析
    print("\n\n按 rank_change 分档分析（T+5收益）:")
    signals_df['rc_bin'] = pd.qcut(signals_df['rank_change'], q=4, labels=['Q1(小)', 'Q2', 'Q3', 'Q4(大)'])
    for label in ['Q1(小)', 'Q2', 'Q3', 'Q4(大)']:
        sub = signals_df[signals_df['rc_bin'] == label]['ret_T5'].dropna()
        if len(sub) > 0:
            print(f"  {label}: 样本={len(sub)}, 胜率={((sub > 0).mean() * 100):.1f}%, "
                  f"均值={sub.mean():.2f}%, 中位数={sub.median():.2f}%")

    # 按当日涨跌幅分档
    print("\n按当日涨跌幅分档分析（T+5收益）:")
    signals_df['cr_bin'] = pd.cut(signals_df['change_rate'],
                                   bins=[-30, -5, 0, 5, 10, 30],
                                   labels=['<-5%', '-5~0%', '0~5%', '5~10%', '>10%'])
    for label in ['<-5%', '-5~0%', '0~5%', '5~10%', '>10%']:
        sub = signals_df[signals_df['cr_bin'] == label]['ret_T5'].dropna()
        if len(sub) > 0:
            print(f"  {label}: 样本={len(sub)}, 胜率={((sub > 0).mean() * 100):.1f}%, "
                  f"均值={sub.mean():.2f}%, 中位数={sub.median():.2f}%")

    # 按月度分析信号表现
    print("\n\n月度信号表现（T+5）:")
    signals_df['month'] = signals_df['date'].apply(lambda x: str(x)[:7])
    monthly = signals_df.groupby('month').apply(
        lambda g: pd.Series({
            '信号数': len(g),
            '胜率%': (g['ret_T5'].dropna() > 0).mean() * 100 if len(g['ret_T5'].dropna()) > 0 else 0,
            '均值%': g['ret_T5'].dropna().mean() if len(g['ret_T5'].dropna()) > 0 else 0
        })
    )
    for month, row in monthly.iterrows():
        print(f"  {month}: 信号{row['信号数']:.0f}个, 胜率{row['胜率%']:.1f}%, 均值{row['均值%']:.2f}%")

    # 选股重复度分析
    print("\n\n选股重复度分析:")
    stock_counts = signals_df.groupby('stock_code').size().reset_index(name='count')
    print(f"  总共选出不同股票: {len(stock_counts)}")
    print(f"  只出现1次: {(stock_counts['count'] == 1).sum()} ({(stock_counts['count'] == 1).mean() * 100:.1f}%)")
    print(f"  出现>=5次: {(stock_counts['count'] >= 5).sum()} ({(stock_counts['count'] >= 5).mean() * 100:.1f}%)")
    print(f"  出现>=10次: {(stock_counts['count'] >= 10).sum()} ({(stock_counts['count'] >= 10).mean() * 100:.1f}%)")

    # 高频出现股票的表现
    frequent = stock_counts[stock_counts['count'] >= 10]['stock_code']
    if len(frequent) > 0:
        freq_signals = signals_df[signals_df['stock_code'].isin(frequent)]
        print(f"\n  高频股（>=10次）的T+5表现:")
        freq_ret = freq_signals['ret_T5'].dropna()
        if len(freq_ret) > 0:
            print(f"    样本: {len(freq_ret)}, 胜率: {(freq_ret > 0).mean() * 100:.1f}%, "
                  f"均值: {freq_ret.mean():.2f}%, 中位数: {freq_ret.median():.2f}%")

    return results_summary


def calc_benchmark_returns(signals_df):
    """计算同期沪深300基准收益"""
    try:
        sql = """
        SELECT trade_date, close
        FROM market_daily
        WHERE ts_code = '000300.SH' AND trade_date >= '20250301'
        ORDER BY trade_date
        """
        bm = pd.read_sql(sql, stock_engine)
        if len(bm) == 0:
            # 尝试用上证指数
            sql = """
            SELECT trade_date, close
            FROM market_daily
            WHERE ts_code = '000001.SH' AND trade_date >= '20250301'
            ORDER BY trade_date
            """
            bm = pd.read_sql(sql, stock_engine)

        if len(bm) == 0:
            print("  (基准数据不可用)")
            return {}

        bm['trade_date'] = pd.to_datetime(bm['trade_date'], format='%Y%m%d')
        bm = bm.sort_values('trade_date')
        dates = bm['trade_date'].values
        prices = bm['close'].values

        results = {}
        for p in [1, 3, 5, 10, 20]:
            rets = []
            for sig_date in signals_df['date'].unique():
                sig_ts = pd.Timestamp(sig_date)
                pos = np.searchsorted(dates, sig_ts)
                if pos >= len(dates) or dates[pos] != sig_ts:
                    continue
                sell_pos = pos + p
                if sell_pos < len(dates):
                    ret = (prices[sell_pos] / prices[pos] - 1) * 100
                    rets.append(ret)
            if rets:
                rets = np.array(rets)
                results[f'T+{p}'] = {
                    'mean': rets.mean(),
                    'win_rate': (rets > 0).mean() * 100
                }
        return results
    except Exception as e:
        print(f"  基准计算异常: {e}")
        return {}


def analyze_intraday_timing(signals_df):
    """分析不同 rank_change 阈值对表现的影响"""
    print("\n\n" + "=" * 80)
    print("rank_change 阈值敏感性分析（T+5收益）")
    print("=" * 80)

    # rank_change 最小值约 700，用更高的阈值分层
    thresholds = [1000, 2000, 3000, 3500, 4000, 4500, 5000]
    print(f"{'阈值':<8} {'样本数':<10} {'胜率%':<10} {'均值%':<10} {'中位数%':<10}")
    print("-" * 48)

    for thresh in thresholds:
        sub = signals_df[signals_df['rank_change'] >= thresh]['ret_T5'].dropna()
        if len(sub) > 0:
            print(f">={thresh:<6} {len(sub):<10} {(sub > 0).mean() * 100:<10.1f} "
                  f"{sub.mean():<10.2f} {sub.median():<10.2f}")


def main():
    start_time = datetime.now()
    print(f"开始分析: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # 1. 加载数据
    pop_df = load_popularity_rank()
    market_df = load_market_daily()

    # 2. 计算每日排名变化，选出Top20
    signals_df = calc_daily_rank_change(pop_df)

    # 3. 计算未来收益
    signals_df = calc_forward_returns(signals_df, market_df)

    # 4. 分析信号有效性
    results = analyze_results(signals_df)

    # 5. 阈值敏感性分析
    analyze_intraday_timing(signals_df)

    # 6. 保存详细数据
    output_path = 'F:/projects/my-stock-research-wt-claude02/hot/data/heat_surge_signals.csv'
    signals_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n详细信号数据已保存: {output_path}")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n分析完成，耗时 {elapsed:.1f} 秒")


if __name__ == '__main__':
    main()
