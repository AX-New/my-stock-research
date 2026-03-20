"""
热度轮转策略 - 实时选股器

基于 03_heat_rotation_no_timeout.py 的买入逻辑，扫描全市场，
筛选当前满足买入条件的股票候选。

【筛选逻辑】
1. heat_position = (当前rank - 20日最低rank) / (20日最高rank - 20日最低rank)
   - 过滤: heat_position >= 0.80
2. rank_surge = 当前rank / 20日均值rank
   - 用于候选排序（越高越好，表示相对自身常态冷了越多）
3. deal_amount >= 5000万（最新交易日）
4. 必须有最新交易日的价格数据

【输出】
按 rank_surge 降序排列的候选股列表，含排名、代码、名称、各项指标。
"""

import sys
import io
import time
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text

# 强制 UTF-8 输出（Windows 兼容）
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ============================================================
# 配置
# ============================================================

MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3307
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'

# 策略参数
LOOKBACK = 20           # 热度位置回看窗口（天）
BUY_THRESHOLD = 0.80    # 买入阈值：heat_position >= 此值
MIN_DEAL_AMOUNT = 5e7   # 最低日成交额 5000万


# ============================================================
# 数据加载
# ============================================================

def get_engine(db_name):
    return create_engine(
        f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4',
        pool_pre_ping=True,
    )


def load_data():
    """加载选股所需的全部数据"""
    print("=" * 70)
    print("加载数据...")
    print("=" * 70)

    trend_engine = get_engine('my_trend')
    stock_engine = get_engine('my_stock')

    # 1. 获取热度排名的最新日期
    latest_date_row = pd.read_sql(
        "SELECT MAX(date) as max_date FROM popularity_rank", trend_engine
    )
    latest_heat_date = latest_date_row['max_date'].iloc[0]
    print(f"  热度数据最新日期: {latest_heat_date}")

    # 2. 获取最近的交易日（用于价格数据匹配）
    #    取热度最新日期对应的或之前最近的交易日
    latest_heat_str = pd.Timestamp(latest_heat_date).strftime('%Y%m%d')
    trade_cal_df = pd.read_sql(f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1 AND cal_date <= '{latest_heat_str}'
        ORDER BY cal_date DESC LIMIT 1
    """, stock_engine)
    latest_trade_date = trade_cal_df['cal_date'].iloc[0]
    print(f"  最新交易日: {latest_trade_date}")

    # 3. 获取最近30个交易日列表（用于回看窗口）
    trade_days_df = pd.read_sql(f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1 AND cal_date <= '{latest_heat_str}'
        ORDER BY cal_date DESC LIMIT 30
    """, stock_engine)
    trade_days_list = sorted(trade_days_df['cal_date'].tolist())
    # 转换为 date 对象
    trade_days_dates = sorted([
        pd.to_datetime(d, format='%Y%m%d').date() if isinstance(d, str) else pd.Timestamp(d).date()
        for d in trade_days_list
    ])
    print(f"  回看交易日范围: {trade_days_dates[0]} ~ {trade_days_dates[-1]} ({len(trade_days_dates)}天)")

    # 4. 加载热度排名数据（最近30个交易日，足够计算20日窗口）
    start_date = trade_days_dates[0]
    end_date = trade_days_dates[-1]
    heat_df = pd.read_sql(f"""
        SELECT stock_code, date, `rank`, deal_amount
        FROM popularity_rank
        WHERE date >= '{start_date}' AND date <= '{end_date}'
    """, trend_engine)
    heat_df['date'] = pd.to_datetime(heat_df['date']).dt.date
    print(f"  热度数据: {len(heat_df):,} 条, {heat_df['stock_code'].nunique()} 只股票")

    # 5. 加载行情数据（不复权收盘价）
    start_str = trade_days_dates[0].strftime('%Y%m%d')
    end_str = trade_days_dates[-1].strftime('%Y%m%d')
    price_df = pd.read_sql(f"""
        SELECT ts_code, trade_date, close
        FROM market_daily
        WHERE trade_date >= '{start_str}' AND trade_date <= '{end_str}'
    """, stock_engine)
    price_df['date'] = pd.to_datetime(price_df['trade_date'], format='%Y%m%d').dt.date
    price_df['stock_code'] = price_df['ts_code'].str[:6]
    print(f"  行情数据: {len(price_df):,} 条, {price_df['stock_code'].nunique()} 只股票")

    # 6. 加载股票基本信息
    basic_df = pd.read_sql("""
        SELECT ts_code, name, industry FROM stock_basic
        WHERE list_status = 'L'
    """, stock_engine)
    basic_df['stock_code'] = basic_df['ts_code'].str[:6]
    print(f"  上市股票: {len(basic_df)} 只")

    return heat_df, price_df, basic_df, trade_days_dates, latest_heat_date, latest_trade_date


# ============================================================
# 核心选股逻辑
# ============================================================

def screen_stocks(heat_df, price_df, basic_df, trade_days_dates):
    """
    执行选股筛选

    返回: (candidates_df, stats_dict)
    """
    print("\n" + "=" * 70)
    print("计算热度指标...")
    print("=" * 70)

    td_set = set(trade_days_dates)
    heat_td = heat_df[heat_df['date'].isin(td_set)].copy()

    # 透视表：行=日期, 列=股票代码, 值=rank
    rank_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='rank')
    rank_pivot = rank_pivot.sort_index()

    # 成交额透视表
    deal_pivot = heat_td.pivot_table(index='date', columns='stock_code', values='deal_amount')
    deal_pivot = deal_pivot.sort_index()

    total_stocks_scanned = rank_pivot.shape[1]
    print(f"  矩阵大小: {rank_pivot.shape[0]} 天 x {rank_pivot.shape[1]} 只股票")

    # 滚动计算（20日窗口）
    rolling_max = rank_pivot.rolling(window=LOOKBACK, min_periods=LOOKBACK // 2).max()
    rolling_min = rank_pivot.rolling(window=LOOKBACK, min_periods=LOOKBACK // 2).min()
    rolling_mean = rank_pivot.rolling(window=LOOKBACK, min_periods=LOOKBACK // 2).mean()

    # heat_position = (当前rank - 最低rank) / (最高rank - 最低rank)
    range_val = rolling_max - rolling_min
    range_val = range_val.replace(0, np.nan)
    heat_position = (rank_pivot - rolling_min) / range_val

    # rank_surge = 当前rank / 滚动均值rank
    rolling_mean = rolling_mean.replace(0, np.nan)
    rank_surge = rank_pivot / rolling_mean

    # 取最新一天的数据
    latest_date = rank_pivot.index[-1]
    print(f"  选股日期: {latest_date}")

    hp_today = heat_position.loc[latest_date].dropna()
    surge_today = rank_surge.loc[latest_date].dropna()
    rank_today = rank_pivot.loc[latest_date].dropna()
    rolling_mean_today = rolling_mean.loc[latest_date].dropna()

    print(f"  有 heat_position 数据的股票: {len(hp_today)}")

    # ---- 筛选步骤 ----

    # 步骤1: heat_position >= 阈值
    hp_pass = hp_today[hp_today >= BUY_THRESHOLD]
    n_hp_pass = len(hp_pass)
    print(f"\n  筛选步骤:")
    print(f"    1. heat_position >= {BUY_THRESHOLD}: {n_hp_pass} 只")

    # 步骤2: 成交额 >= 5000万
    deal_today = deal_pivot.loc[latest_date].reindex(hp_pass.index)
    deal_pass = hp_pass[deal_today.fillna(0) >= MIN_DEAL_AMOUNT]
    n_deal_pass = len(deal_pass)
    print(f"    2. 成交额 >= {MIN_DEAL_AMOUNT/1e4:.0f}万: {n_deal_pass} 只")

    # 步骤3: 有最新交易日价格数据
    latest_trade_str = trade_days_dates[-1].strftime('%Y%m%d')
    price_latest = price_df[price_df['date'] == trade_days_dates[-1]]
    codes_with_price = set(price_latest['stock_code'].unique())
    price_pass_codes = [c for c in deal_pass.index if c in codes_with_price]
    candidates = deal_pass.loc[price_pass_codes]
    n_price_pass = len(candidates)
    print(f"    3. 有最新价格数据: {n_price_pass} 只")

    if n_price_pass == 0:
        print("\n  没有满足所有条件的候选股票！")
        stats = {
            'total_scanned': total_stocks_scanned,
            'hp_pass': n_hp_pass,
            'deal_pass': n_deal_pass,
            'final': 0,
            'screen_date': latest_date,
        }
        return pd.DataFrame(), stats

    # ---- 组装结果表 ----
    results = []
    # 价格查找表: stock_code -> {ts_code, close_latest, close_20d_ago}
    price_latest_lookup = price_latest.set_index('stock_code')[['ts_code', 'close']].to_dict('index')

    # 20日前的价格（用于计算涨跌幅）
    # 取倒数第20个交易日（如果有的话）
    if len(trade_days_dates) >= LOOKBACK:
        date_20d_ago = trade_days_dates[-LOOKBACK]
    else:
        date_20d_ago = trade_days_dates[0]
    price_20d_ago = price_df[price_df['date'] == date_20d_ago].set_index('stock_code')['close'].to_dict()

    # 股票基本信息查找表
    basic_lookup = basic_df.set_index('stock_code')[['ts_code', 'name', 'industry']].to_dict('index')

    for code in candidates.index:
        hp_val = hp_today.get(code, np.nan)
        surge_val = surge_today.get(code, np.nan)
        rank_val = rank_today.get(code, np.nan)
        mean_rank_val = rolling_mean_today.get(code, np.nan)
        deal_val = deal_today.get(code, 0)

        # 价格信息
        price_info = price_latest_lookup.get(code, {})
        ts_code = price_info.get('ts_code', f'{code}.??')
        close_now = price_info.get('close', np.nan)

        # 20日涨跌幅
        close_20d = price_20d_ago.get(code, np.nan)
        if not np.isnan(close_now) and not np.isnan(close_20d) and close_20d > 0:
            pct_20d = (close_now / close_20d - 1) * 100
        else:
            pct_20d = np.nan

        # 基本信息
        basic_info = basic_lookup.get(code, {})
        name = basic_info.get('name', '-')
        industry = basic_info.get('industry', '-')
        # 如果 basic_lookup 里有 ts_code，优先用它（更准确）
        if 'ts_code' in basic_info:
            ts_code = basic_info['ts_code']

        results.append({
            '股票代码': code,
            'ts_code': ts_code,
            '股票名称': name,
            '当前rank': int(rank_val) if not np.isnan(rank_val) else '-',
            '20日均rank': f"{mean_rank_val:.1f}" if not np.isnan(mean_rank_val) else '-',
            'rank_surge': round(surge_val, 4) if not np.isnan(surge_val) else 0,
            'heat_position': round(hp_val, 4) if not np.isnan(hp_val) else 0,
            '当前价格': round(close_now, 2) if not np.isnan(close_now) else '-',
            '近20日涨跌幅': round(pct_20d, 2) if not np.isnan(pct_20d) else '-',
            '最新成交额(亿)': round(deal_val / 1e8, 2) if deal_val else 0,
            '所属行业': industry,
        })

    result_df = pd.DataFrame(results)
    # 按 rank_surge 降序排列
    result_df = result_df.sort_values('rank_surge', ascending=False).reset_index(drop=True)
    result_df.insert(0, '排名', range(1, len(result_df) + 1))

    stats = {
        'total_scanned': total_stocks_scanned,
        'hp_pass': n_hp_pass,
        'deal_pass': n_deal_pass,
        'final': n_price_pass,
        'screen_date': latest_date,
    }

    return result_df, stats


# ============================================================
# 输出
# ============================================================

def print_results(result_df, stats):
    """格式化打印选股结果"""
    print("\n" + "=" * 70)
    print("选股结果统计")
    print("=" * 70)
    print(f"  选股日期:          {stats['screen_date']}")
    print(f"  扫描股票总数:      {stats['total_scanned']}")
    print(f"  heat_position>=0.80: {stats['hp_pass']}")
    print(f"  成交额>=5000万:    {stats['deal_pass']}")
    print(f"  最终候选数:        {stats['final']}")

    if len(result_df) == 0:
        print("\n没有满足条件的候选股票。")
        return

    # ---- 完整候选列表 ----
    print("\n" + "=" * 70)
    print(f"全部候选股票（共 {len(result_df)} 只，按 rank_surge 降序）")
    print("=" * 70)

    # 设置 pandas 显示选项
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)

    # 打印表格
    display_df = result_df.copy()
    # 格式化 rank_surge 和 heat_position 为更易读的格式
    display_df['rank_surge'] = display_df['rank_surge'].apply(lambda x: f"{x:.4f}")
    display_df['heat_position'] = display_df['heat_position'].apply(lambda x: f"{x:.4f}")
    display_df['近20日涨跌幅'] = display_df['近20日涨跌幅'].apply(
        lambda x: f"{x:+.2f}%" if isinstance(x, (int, float)) else x
    )
    display_df['最新成交额(亿)'] = display_df['最新成交额(亿)'].apply(lambda x: f"{x:.2f}")

    print(display_df.to_string(index=False))

    # ---- TOP 10 高亮 ----
    print("\n" + "=" * 70)
    top_n = min(10, len(result_df))
    print(f"TOP {top_n} 候选股票（rank_surge 最高）")
    print("=" * 70)

    top_df = result_df.head(top_n).copy()
    for _, row in top_df.iterrows():
        pct_str = f"{row['近20日涨跌幅']:+.2f}%" if isinstance(row['近20日涨跌幅'], (int, float)) else row['近20日涨跌幅']
        print(f"  #{row['排名']:>2d}  {row['股票代码']}  {row['ts_code']:<10s}  "
              f"{row['股票名称']:<6s}  "
              f"rank={str(row['当前rank']):<5s}  "
              f"surge={row['rank_surge']:.4f}  "
              f"hp={row['heat_position']:.4f}  "
              f"价格={row['当前价格']}  "
              f"20日涨跌={pct_str}  "
              f"成交额={row['最新成交额(亿)']:.2f}亿  "
              f"{row['所属行业']}")

    print("\n" + "=" * 70)
    print("选股完成")
    print("=" * 70)


# ============================================================
# 主函数
# ============================================================

def main():
    t0 = time.time()
    print("=" * 70)
    print("热度轮转策略 - 实时选股器")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print(f"参数: LOOKBACK={LOOKBACK}, BUY_THRESHOLD={BUY_THRESHOLD}, "
          f"MIN_DEAL_AMOUNT={MIN_DEAL_AMOUNT/1e4:.0f}万")

    # 1. 加载数据
    heat_df, price_df, basic_df, trade_days_dates, latest_heat_date, latest_trade_date = load_data()

    # 2. 执行选股
    result_df, stats = screen_stocks(heat_df, price_df, basic_df, trade_days_dates)

    # 3. 打印结果
    print_results(result_df, stats)

    elapsed = time.time() - t0
    print(f"\n总耗时: {elapsed:.1f} 秒")


if __name__ == '__main__':
    main()
