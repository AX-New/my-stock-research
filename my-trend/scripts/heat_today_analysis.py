"""
当日热度飙升信号 x 分钟K线 交叉分析
重点分析：热度排名变化与分钟价格曲线的交叉特征
"""
import pymysql
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')


def analyze_today(date='2026-03-23'):
    """分析指定日期的热度飙升信号与分钟曲线交叉"""
    conn = pymysql.connect(host='127.0.0.1', port=3310, user='root', password='root', database='my_trend')
    cur = conn.cursor()

    # 选取出现次数>=4次的热度飙升股
    cur.execute(f"""
    SELECT stock_code, stock_name, COUNT(*) as cnt, MAX(rank_change) as max_rc
    FROM heat_change_top WHERE date = '{date}'
    GROUP BY stock_code, stock_name
    HAVING COUNT(*) >= 4
    ORDER BY max_rc DESC LIMIT 15
    """)
    top_stocks = cur.fetchall()

    print("=" * 100)
    print(f"{date} 热度飙升Top15 x 分钟曲线交叉分析")
    print("=" * 100)

    for code, name, cnt, max_rc in top_stocks:
        # 热度信号时间序列
        cur.execute("""
        SELECT time_point, rank_today, rank_change, change_rate
        FROM heat_change_top WHERE date=%s AND stock_code=%s ORDER BY time_point
        """, (date, code))
        heat_series = cur.fetchall()

        # 分钟K线
        cur.execute("""
        SELECT time, open, high, low, close, volume
        FROM heat_stock_minute WHERE trade_date=%s AND stock_code=%s ORDER BY time
        """, (date, code))
        min_rows = cur.fetchall()
        if not min_rows:
            continue

        df_min = pd.DataFrame(min_rows, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        open_price = df_min.iloc[0]['open']
        if not open_price or open_price <= 0:
            continue

        close_price = df_min.iloc[-1]['close']
        high_price = df_min['high'].max()
        low_price = df_min['low'].min()
        day_ret = (close_price - open_price) / open_price * 100
        max_gain = (high_price - open_price) / open_price * 100
        max_loss = (low_price - open_price) / open_price * 100

        high_time = str(df_min.loc[df_min['high'].idxmax(), 'time'])[11:16]
        low_time = str(df_min.loc[df_min['low'].idxmin(), 'time'])[11:16]

        print(f"\n{'='*80}")
        print(f"  {code} {name} | 排名变化最大+{max_rc} | 日收益{day_ret:+.2f}%")
        print(f"  开盘{open_price:.2f} -> 收盘{close_price:.2f} | 最高{high_price:.2f}({high_time}) 最低{low_price:.2f}({low_time})")
        print(f"  最大浮盈{max_gain:+.2f}% | 最大浮亏{max_loss:+.2f}%")
        print(f"  {'时间点':<8} {'热度排名':<10} {'排名变化':<10} {'信号涨幅':<10} {'分钟线价位':<12} {'相对开盘':<10}")

        for tp, rank, rc, chg in heat_series:
            tp_h, tp_m = int(tp.split(':')[0]), int(tp.split(':')[1])
            closest = None
            for _, mrow in df_min.iterrows():
                mt = mrow['time']
                if hasattr(mt, 'hour') and mt.hour == tp_h and abs(mt.minute - tp_m) <= 5:
                    closest = mrow['close']
                    break

            if closest and open_price > 0:
                pchg = (closest - open_price) / open_price * 100
                print(f"  {tp:<8} {rank:<10} +{rc:<9} {chg if chg else 'N/A':<10} {closest:<12.2f} {pchg:+.2f}%")
            else:
                print(f"  {tp:<8} {rank:<10} +{rc:<9} {chg if chg else 'N/A':<10} {'N/A':<12} {'N/A'}")

    conn.close()


if __name__ == '__main__':
    analyze_today()
