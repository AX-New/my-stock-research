"""
热度排名 x 分钟K线 交叉分析脚本
分析 heat_change_top 飙升信号与分钟曲线的关联，回测早盘买入次日卖出策略
同时用 popularity_rank 日快照做更长周期的回测
"""
import pymysql
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


def get_connections():
    conn_trend = pymysql.connect(host='127.0.0.1', port=3310, user='root', password='root', database='my_trend')
    conn_stock = pymysql.connect(host='127.0.0.1', port=3307, user='root', password='root', database='my_stock')
    return conn_trend, conn_stock


def analyze_part1_minute_curves():
    """Part1: 3/23 heat_change_top 信号与分钟曲线交叉分析"""
    conn_trend, _ = get_connections()
    cur = conn_trend.cursor()

    print("=" * 80)
    print("Part1: 3/23 热度飙升信号 × 分钟曲线交叉分析")
    print("=" * 80)

    # 获取3/23有分钟线的热度飙升股
    cur.execute("""
    SELECT h.stock_code, h.stock_name, h.time_point, h.rank_change, h.change_rate
    FROM heat_change_top h
    WHERE h.date = '2026-03-23'
      AND h.stock_code IN (SELECT DISTINCT stock_code FROM heat_stock_minute WHERE trade_date='2026-03-23')
    ORDER BY h.stock_code, h.time_point
    """)
    df_heat = pd.DataFrame(cur.fetchall(),
                           columns=['stock_code', 'stock_name', 'time_point', 'rank_change', 'change_rate'])

    # 获取分钟线
    cur.execute("""
    SELECT stock_code, stock_name, time, open, high, low, close, volume
    FROM heat_stock_minute WHERE trade_date = '2026-03-23'
    ORDER BY stock_code, time
    """)
    df_min = pd.DataFrame(cur.fetchall(),
                          columns=['stock_code', 'stock_name', 'time', 'open', 'high', 'low', 'close', 'volume'])

    codes = df_heat['stock_code'].unique()
    print(f"有交叉数据的股票: {len(codes)} 只\n")

    # 分析每只股的日内走势
    analysis = []
    for code in codes:
        h = df_heat[df_heat['stock_code'] == code]
        m = df_min[df_min['stock_code'] == code].copy()
        if len(m) < 10:
            continue

        name = h.iloc[0]['stock_name']
        first_signal = h.iloc[0]['time_point']
        max_rank_change = h['rank_change'].max()

        open_price = m.iloc[0]['open']
        close_price = m.iloc[-1]['close']
        high_price = m['high'].max()
        low_price = m['low'].min()
        if open_price <= 0:
            continue

        day_return = (close_price - open_price) / open_price * 100
        max_gain = (high_price - open_price) / open_price * 100
        max_loss = (low_price - open_price) / open_price * 100

        m.loc[:, 'hm'] = m['time'].apply(lambda x: x.strftime('%H:%M') if hasattr(x, 'strftime') else str(x)[11:16])

        high_time = m.loc[m['high'].idxmax(), 'hm']
        low_time = m.loc[m['low'].idxmin(), 'hm']

        # 各时段
        segs = {}
        for seg_name, lo_t, hi_t in [('早盘30分', '09:30', '10:00'), ('上午', '10:00', '11:31'),
                                      ('午后', '13:00', '14:00'), ('尾盘', '14:00', '15:01')]:
            seg = m[(m['hm'] >= lo_t) & (m['hm'] < hi_t)]
            if len(seg) >= 2:
                segs[seg_name] = (seg.iloc[-1]['close'] - seg.iloc[0]['open']) / seg.iloc[0]['open'] * 100
            else:
                segs[seg_name] = None

        total_vol = m['volume'].sum()
        am_vol = m[m['hm'] <= '11:30']['volume'].sum()
        pm_vol = m[m['hm'] >= '13:00']['volume'].sum()
        vol_ratio = am_vol / pm_vol if pm_vol > 0 else 999

        analysis.append({
            'code': code, 'name': name, 'first_signal': first_signal,
            'max_rank_change': max_rank_change,
            'day_return': day_return, 'max_gain': max_gain, 'max_loss': max_loss,
            'high_time': high_time, 'low_time': low_time,
            'seg_early': segs.get('早盘30分'), 'seg_am': segs.get('上午'),
            'seg_pm_early': segs.get('午后'), 'seg_pm_late': segs.get('尾盘'),
            'vol_ratio': vol_ratio
        })

    df_a = pd.DataFrame(analysis)

    # 输出关键发现
    print("--- 最高/最低价出现时间 ---")
    for col, label in [('high_time', '最高价'), ('low_time', '最低价')]:
        period = pd.cut(
            df_a[col].apply(lambda x: int(x.replace(':', ''))),
            bins=[0, 1000, 1130, 1300, 1400, 1600],
            labels=['9:30-10:00', '10:00-11:30', '11:30-13:00', '13:00-14:00', '14:00-15:00']
        )
        print(f"\n  {label}:")
        for p, cnt in period.value_counts().sort_index().items():
            if cnt > 0:
                print(f"    {p}: {cnt} 只 ({cnt/len(df_a)*100:.0f}%)")

    print(f"\n--- 日内走势形态 ---")
    print(f"  收阳: {len(df_a[df_a['day_return'] > 0])} 只, 收阴: {len(df_a[df_a['day_return'] <= 0])} 只")
    print(f"  平均日内收益: {df_a['day_return'].mean():.2f}%")
    print(f"  平均最大浮盈: {df_a['max_gain'].mean():.2f}%")
    print(f"  平均最大浮亏: {df_a['max_loss'].mean():.2f}%")

    valid = df_a.dropna(subset=['seg_early'])
    am_up = valid[valid['seg_early'] > 0]
    am_down = valid[valid['seg_early'] <= 0]
    print(f"\n  早盘30分上涨: {len(am_up)} 只 → 全天均{am_up['day_return'].mean():.2f}%")
    print(f"  早盘30分下跌: {len(am_down)} 只 → 全天均{am_down['day_return'].mean():.2f}%")

    print(f"\n--- 各时段涨跌概率 ---")
    for seg_col, seg_name in [('seg_early', '早盘30分'), ('seg_am', '上午10-11:30'),
                               ('seg_pm_early', '午后13-14'), ('seg_pm_late', '尾盘14-15')]:
        v = df_a.dropna(subset=[seg_col])
        up = len(v[v[seg_col] > 0])
        avg = v[seg_col].mean()
        print(f"  {seg_name}: 上涨率{up}/{len(v)}={up/len(v)*100:.1f}%, 均{avg:+.2f}%")

    print(f"\n--- 首次信号时间 vs 表现 ---")
    for tp in ['09:30', '10:00', '11:00', '12:10']:
        grp = df_a[df_a['first_signal'] == tp]
        if len(grp) < 3:
            continue
        win = len(grp[grp['day_return'] > 0])
        print(f"  {tp}: {len(grp)}只, 收阳率{win/len(grp)*100:.1f}%, 均收益{grp['day_return'].mean():+.2f}%, 均最大浮盈{grp['max_gain'].mean():.2f}%")

    print(f"\n--- 成交量分布 ---")
    print(f"  上午/下午量比均值: {df_a['vol_ratio'].mean():.2f}")
    print(f"  量集中上午(>1.5): {len(df_a[df_a['vol_ratio'] > 1.5])} 只")

    conn_trend.close()
    return df_a


def analyze_part2_history_backtest():
    """Part2: 用 popularity_rank 日变化做历史回测"""
    conn_trend, conn_stock = get_connections()
    cur = conn_trend.cursor()

    print("\n" + "=" * 80)
    print("Part2: 历史回测 - popularity_rank 排名变化 x 次日收益")
    print("=" * 80)

    # 获取所有日期对（相邻交易日）
    cur.execute("SELECT DISTINCT date FROM popularity_rank ORDER BY date")
    all_dates = [r[0] for r in cur.fetchall()]
    print(f"总日期数: {len(all_dates)}")

    # 只取最近一个月（约20个交易日）做回测，避免查询太慢
    recent_dates = all_dates[-25:]
    date_pairs = [(recent_dates[i], recent_dates[i + 1]) for i in range(len(recent_dates) - 1)]
    print(f"回测日期对: {len(date_pairs)}")

    all_signals = []
    for date_today, date_yesterday_candidate in date_pairs:
        # 找前一个日期
        idx = all_dates.index(date_today)
        if idx == 0:
            continue
        date_prev = all_dates[idx - 1]

        dt_str = date_today.strftime('%Y-%m-%d')
        dp_str = date_prev.strftime('%Y-%m-%d')

        # 直接取两天的排名做差
        cur.execute(f"""
        SELECT p1.stock_code, p1.stock_name, p1.`rank` as rank_today,
               p2.`rank` as rank_prev, (p2.`rank` - p1.`rank`) as rank_change,
               p1.change_rate
        FROM popularity_rank p1
        JOIN popularity_rank p2 ON p1.stock_code = p2.stock_code AND p2.date = '{dp_str}'
        WHERE p1.date = '{dt_str}'
        AND (p2.`rank` - p1.`rank`) > 1000
        ORDER BY rank_change DESC
        LIMIT 50
        """)
        rows = cur.fetchall()
        for r in rows:
            all_signals.append({
                'date': date_today,
                'code': r[0], 'name': r[1],
                'rank_today': r[2], 'rank_prev': r[3],
                'rank_change': r[4], 'change_rate': r[5]
            })

    df_signals = pd.DataFrame(all_signals)
    print(f"排名跃升>1000信号: {len(df_signals)} 条, 日期: {df_signals['date'].nunique()} 天")

    # 获取日K线
    ts_codes = set()
    for c in df_signals['code'].unique():
        if c.startswith('6'):
            ts_codes.add(f"{c}.SH")
        elif c.startswith('0') or c.startswith('3'):
            ts_codes.add(f"{c}.SZ")
        else:
            ts_codes.add(f"{c}.BJ")

    ts_list = list(ts_codes)
    daily_frames = []
    for i in range(0, len(ts_list), 200):
        batch = ts_list[i:i + 200]
        codes_str = ','.join([f"'{c}'" for c in batch])
        q = f"""
        SELECT ts_code, trade_date, open, close, pct_chg
        FROM market_daily
        WHERE ts_code IN ({codes_str}) AND trade_date >= '20260220'
        """
        daily_frames.append(pd.read_sql(q, conn_stock))

    df_daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    df_daily['stock_code'] = df_daily['ts_code'].str[:6]
    print(f"日K线: {len(df_daily)} 条")

    # 回测
    results = []
    for _, sig in df_signals.iterrows():
        code = sig['code']
        sig_date_str = sig['date'].strftime('%Y%m%d')

        stock_d = df_daily[df_daily['stock_code'] == code].sort_values('trade_date')
        sig_day = stock_d[stock_d['trade_date'] == sig_date_str]
        if len(sig_day) == 0:
            continue

        next_days = stock_d[stock_d['trade_date'] > sig_date_str]
        if len(next_days) == 0:
            continue

        sig_day = sig_day.iloc[0]
        next_day = next_days.iloc[0]

        # 策略A: 信号日开盘买 → 次日收盘卖
        buy = sig_day['open']
        sell = next_day['close']
        ret_a = (sell - buy) / buy * 100 if buy and buy > 0 else None

        # 策略B: 次日开盘买 → 次日收盘卖
        buy_b = next_day['open']
        sell_b = next_day['close']
        ret_b = (sell_b - buy_b) / buy_b * 100 if buy_b and buy_b > 0 else None

        results.append({
            **sig.to_dict(),
            'signal_day_pct': sig_day['pct_chg'],
            'strat_a_ret': ret_a,
            'strat_b_ret': ret_b,
            'next_day_pct': next_day['pct_chg']
        })

    df_res = pd.DataFrame(results)
    print(f"有效回测: {len(df_res)} 条\n")

    # 策略A统计
    print("--- 策略A: 信号日开盘买 → 次日收盘卖 ---")
    valid = df_res.dropna(subset=['strat_a_ret'])
    for rc_min in [1000, 1500, 2000, 3000]:
        grp = valid[valid['rank_change'] >= rc_min]
        if len(grp) == 0:
            continue
        win = len(grp[grp['strat_a_ret'] > 0])
        avg = grp['strat_a_ret'].mean()
        med = grp['strat_a_ret'].median()
        print(f"  rank_change>={rc_min}: {len(grp)}条, 胜率{win}/{len(grp)}={win/len(grp)*100:.1f}%, 均{avg:+.2f}%, 中位{med:+.2f}%")

    # 策略B统计
    print("\n--- 策略B: 次日开盘买 → 次日收盘卖 ---")
    valid = df_res.dropna(subset=['strat_b_ret'])
    for rc_min in [1000, 1500, 2000, 3000]:
        grp = valid[valid['rank_change'] >= rc_min]
        if len(grp) == 0:
            continue
        win = len(grp[grp['strat_b_ret'] > 0])
        avg = grp['strat_b_ret'].mean()
        med = grp['strat_b_ret'].median()
        print(f"  rank_change>={rc_min}: {len(grp)}条, 胜率{win}/{len(grp)}={win/len(grp)*100:.1f}%, 均{avg:+.2f}%, 中位{med:+.2f}%")

    # 按信号日涨跌幅分组
    print("\n--- 按信号日涨跌幅分组（策略A）---")
    valid = df_res.dropna(subset=['strat_a_ret'])
    ranges = [
        (None, -3, '跌>3%'), (-3, 0, '跌0-3%'),
        (0, 5, '涨0-5%'), (5, 10, '涨5-10%'), (10, None, '涨>10%')
    ]
    for lo, hi, label in ranges:
        cond = pd.Series(True, index=valid.index)
        if lo is not None:
            cond = cond & (valid['change_rate'] >= lo)
        if hi is not None:
            cond = cond & (valid['change_rate'] < hi)
        grp = valid[cond]
        if len(grp) < 3:
            continue
        win = len(grp[grp['strat_a_ret'] > 0])
        avg = grp['strat_a_ret'].mean()
        print(f"  {label}: {len(grp)}条, 胜率{win/len(grp)*100:.1f}%, 均{avg:+.2f}%")

    # 按日期分组
    print("\n--- 按日期分组（策略A, rank_change>=1500）---")
    valid = df_res[(df_res['rank_change'] >= 1500) & df_res['strat_a_ret'].notna()]
    for dt, grp in valid.groupby('date'):
        win = len(grp[grp['strat_a_ret'] > 0])
        avg = grp['strat_a_ret'].mean()
        print(f"  {dt}: {len(grp)}条, 胜率{win}/{len(grp)}={win/len(grp)*100:.1f}%, 均{avg:+.2f}%")

    conn_trend.close()
    conn_stock.close()
    return df_res


if __name__ == '__main__':
    df_part1 = analyze_part1_minute_curves()
    df_part2 = analyze_part2_history_backtest()
