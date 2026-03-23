"""
历史回测：热度排名变化信号 x 次日收益
使用 popularity_rank 日快照计算排名跃升，回测策略
"""
import pymysql
import numpy as np
import warnings
from collections import defaultdict
warnings.filterwarnings('ignore')


def run(days=15, rc_threshold=1500):
    """
    回测最近N个交易日的热度排名变化信号
    Args:
        days: 回测天数
        rc_threshold: 排名变化阈值
    """
    conn_trend = pymysql.connect(host='127.0.0.1', port=3310, user='root', password='root', database='my_trend')
    conn_stock = pymysql.connect(host='127.0.0.1', port=3307, user='root', password='root', database='my_stock')
    cur_t = conn_trend.cursor()
    cur_s = conn_stock.cursor()

    # 获取日期列表
    cur_t.execute("SELECT DISTINCT date FROM popularity_rank ORDER BY date")
    all_dates = [r[0] for r in cur_t.fetchall()]
    print(f"popularity_rank 总日期: {len(all_dates)}, 范围: {all_dates[0]} ~ {all_dates[-1]}")

    recent = all_dates[-(days + 1):]

    # 逐日取排名变化信号
    signals = []
    for i in range(1, len(recent)):
        today = recent[i]
        prev_idx = all_dates.index(today) - 1
        if prev_idx < 0:
            continue
        prev = all_dates[prev_idx]

        cur_t.execute(f"""
        SELECT p1.stock_code, p1.stock_name, (p2.`rank`-p1.`rank`) as rc, p1.change_rate
        FROM popularity_rank p1
        JOIN popularity_rank p2 ON p1.stock_code=p2.stock_code AND p2.date='{prev.strftime('%Y-%m-%d')}'
        WHERE p1.date='{today.strftime('%Y-%m-%d')}' AND (p2.`rank`-p1.`rank`)>{rc_threshold}
        ORDER BY rc DESC LIMIT 30
        """)
        for r in cur_t.fetchall():
            signals.append((today, r[0], r[1], r[2], r[3]))

    print(f"信号数(rc>{rc_threshold}): {len(signals)}")

    # 逐个查日K线回测
    results_a, results_b = [], []
    for date, code, name, rc, chg in signals:
        ds = date.strftime('%Y%m%d')
        if code.startswith('6'):
            ts = f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            ts = f"{code}.SZ"
        else:
            ts = f"{code}.BJ"

        cur_s.execute(f"""
        SELECT trade_date, open, close FROM market_daily
        WHERE ts_code='{ts}' AND trade_date>='{ds}' ORDER BY trade_date LIMIT 3
        """)
        rows = cur_s.fetchall()
        if len(rows) < 2:
            continue

        sig_day, next_day = rows[0], rows[1]

        # 策略A: 信号日开盘买 -> 次日收盘卖
        buy_a = sig_day[1]
        if buy_a and buy_a > 0:
            ret_a = (next_day[2] - buy_a) / buy_a * 100
            results_a.append((date, code, name, rc, chg, ret_a))

        # 策略B: 次日开盘买 -> 次日收盘卖
        buy_b = next_day[1]
        if buy_b and buy_b > 0:
            ret_b = (next_day[2] - buy_b) / buy_b * 100
            results_b.append((date, code, name, rc, chg, ret_b))

    # 统计输出
    print(f"\n{'='*80}")
    print("策略A: 信号日开盘买 -> 次日收盘卖")
    print(f"{'='*80}")
    _print_stats(results_a, 5)

    print(f"\n{'='*80}")
    print("策略B: 次日开盘买 -> 次日收盘卖")
    print(f"{'='*80}")
    _print_stats(results_b, 5)

    # 按涨跌幅分组
    print(f"\n{'='*80}")
    print("按信号日涨跌幅分组（策略A）")
    print(f"{'='*80}")
    for lo, hi, label in [(None, -3, '跌>3%'), (-3, 0, '跌0-3%'),
                           (0, 5, '涨0-5%'), (5, 10, '涨5-10%'), (10, None, '涨>10%')]:
        sub = [r[5] for r in results_a if r[4] is not None
               and (lo is None or r[4] >= lo) and (hi is None or r[4] < hi)]
        if len(sub) < 3:
            continue
        w = len([x for x in sub if x > 0])
        print(f"  {label}: {len(sub)}条, 胜率{w/len(sub)*100:.1f}%, 均{np.mean(sub):+.2f}%")

    # 按日期
    print(f"\n{'='*80}")
    print("按日期分组（策略A）")
    print(f"{'='*80}")
    date_rets = defaultdict(list)
    for r in results_a:
        date_rets[r[0]].append(r[5])
    for dt in sorted(date_rets.keys()):
        rets = date_rets[dt]
        w = len([x for x in rets if x > 0])
        print(f"  {dt}: {len(rets)}条, 胜率{w}/{len(rets)}={w/len(rets)*100:.1f}%, 均{np.mean(rets):+.2f}%")

    conn_trend.close()
    conn_stock.close()


def _print_stats(results, idx):
    rets = [r[idx] for r in results]
    if not rets:
        print("  无有效数据")
        return
    w = len([x for x in rets if x > 0])
    print(f"  总: {len(rets)}条, 胜率{w}/{len(rets)}={w/len(rets)*100:.1f}%")
    print(f"  均收益: {np.mean(rets):+.2f}%, 中位数: {np.median(rets):+.2f}%")

    for rc_min in [1500, 2000, 3000]:
        sub = [r[idx] for r in results if r[3] >= rc_min]
        if not sub:
            continue
        sw = len([x for x in sub if x > 0])
        print(f"  rc>={rc_min}: {len(sub)}条, 胜率{sw/len(sub)*100:.1f}%, 均{np.mean(sub):+.2f}%, 中位{np.median(sub):+.2f}%")


if __name__ == '__main__':
    run()
