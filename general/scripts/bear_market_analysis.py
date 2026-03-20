"""
大盘熊市概率分析 - 数据提取脚本
分析近一年A股市场情况，评估进入熊市的概率
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from lib.database import read_engine

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:.4f}'.format)

# ========== 时间范围 ==========
end_date = '20260319'
start_date = '20250319'  # 近一年
start_date_2y = '20240319'  # 近两年（看更长趋势）

print("=" * 80)
print("一、主要指数近一年走势")
print("=" * 80)

# 上证综指、深证成指、创业板指、沪深300、中证500
indices = {
    '000001.SH': '上证综指',
    '399001.SZ': '深证成指',
    '399006.SZ': '创业板指',
    '000300.SH': '沪深300',
    '000905.SH': '中证500',
}

for ts_code, name in indices.items():
    sql = f"""
    SELECT trade_date, close, open, high, low, vol, amount
    FROM index_daily
    WHERE ts_code = '{ts_code}'
      AND trade_date >= '{start_date}'
      AND trade_date <= '{end_date}'
    ORDER BY trade_date
    """
    df = pd.read_sql(sql, read_engine)
    if df.empty:
        print(f"\n{name} ({ts_code}): 无数据")
        continue

    # 计算关键指标
    current = df.iloc[-1]['close']
    year_start = df.iloc[0]['close']
    year_high = df['high'].max()
    year_low = df['low'].min()
    year_return = (current / year_start - 1) * 100
    from_high = (current / year_high - 1) * 100
    from_low = (current / year_low - 1) * 100

    # 计算均线
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    df['ma120'] = df['close'].rolling(120).mean()
    df['ma250'] = df['close'].rolling(250).mean()

    latest = df.iloc[-1]

    print(f"\n--- {name} ({ts_code}) ---")
    print(f"  最新收盘: {current:.2f}  一年前: {year_start:.2f}")
    print(f"  年内涨跌: {year_return:+.2f}%")
    print(f"  年内最高: {year_high:.2f}  距最高: {from_high:.2f}%")
    print(f"  年内最低: {year_low:.2f}  距最低: {from_low:+.2f}%")
    print(f"  均线位置: MA5={latest['ma5']:.2f} MA10={latest['ma10']:.2f} MA20={latest['ma20']:.2f} MA60={latest['ma60']:.2f}")
    if not np.isnan(latest['ma120']):
        print(f"             MA120={latest['ma120']:.2f}", end="")
    if not np.isnan(latest.get('ma250', np.nan)):
        print(f"  MA250={latest['ma250']:.2f}", end="")
    print()

    # 均线排列判断
    ma_values = {}
    for ma_name in ['ma5', 'ma10', 'ma20', 'ma60']:
        if not np.isnan(latest[ma_name]):
            ma_values[ma_name] = latest[ma_name]

    if len(ma_values) >= 4:
        if ma_values['ma5'] > ma_values['ma10'] > ma_values['ma20'] > ma_values['ma60']:
            print(f"  均线排列: 多头排列 (看多)")
        elif ma_values['ma5'] < ma_values['ma10'] < ma_values['ma20'] < ma_values['ma60']:
            print(f"  均线排列: 空头排列 (看空)")
        else:
            print(f"  均线排列: 交叉/纠缠 (震荡)")

    # 近期趋势（近1月、近3月）
    if len(df) >= 20:
        m1_return = (current / df.iloc[-20]['close'] - 1) * 100
        print(f"  近1月涨跌: {m1_return:+.2f}%")
    if len(df) >= 60:
        m3_return = (current / df.iloc[-60]['close'] - 1) * 100
        print(f"  近3月涨跌: {m3_return:+.2f}%")

    # 成交量趋势
    if len(df) >= 20:
        recent_vol = df['amount'].tail(20).mean()
        prev_vol = df['amount'].tail(60).head(40).mean() if len(df) >= 60 else df['amount'].head(len(df)-20).mean()
        vol_change = (recent_vol / prev_vol - 1) * 100 if prev_vol > 0 else 0
        print(f"  成交额变化(近20日vs前40日): {vol_change:+.2f}%")

print("\n" + "=" * 80)
print("二、MACD 指标分析（上证综指）")
print("=" * 80)

sql = f"""
SELECT trade_date, close
FROM index_daily
WHERE ts_code = '000001.SH'
  AND trade_date >= '{start_date_2y}'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_sh = pd.read_sql(sql, read_engine)

# 计算 MACD
ema12 = df_sh['close'].ewm(span=12, adjust=False).mean()
ema26 = df_sh['close'].ewm(span=26, adjust=False).mean()
df_sh['dif'] = ema12 - ema26
df_sh['dea'] = df_sh['dif'].ewm(span=9, adjust=False).mean()
df_sh['macd'] = (df_sh['dif'] - df_sh['dea']) * 2

# 只看近一年
df_sh_1y = df_sh[df_sh['trade_date'] >= start_date].copy()
latest_sh = df_sh_1y.iloc[-1]
print(f"\n最新 DIF: {latest_sh['dif']:.4f}  DEA: {latest_sh['dea']:.4f}  MACD柱: {latest_sh['macd']:.4f}")

# MACD 趋势
if latest_sh['dif'] > latest_sh['dea']:
    print("MACD状态: DIF > DEA (多头)")
else:
    print("MACD状态: DIF < DEA (空头)")

if latest_sh['dif'] > 0:
    print("DIF位置: 零轴之上 (中期看多)")
else:
    print("DIF位置: 零轴之下 (中期看空)")

# 找最近的金叉/死叉
df_sh_1y['cross'] = np.where(df_sh_1y['dif'] > df_sh_1y['dea'], 1, -1)
df_sh_1y['cross_change'] = df_sh_1y['cross'].diff()
crosses = df_sh_1y[df_sh_1y['cross_change'] != 0].tail(5)
print("\n最近5次金叉/死叉:")
for _, row in crosses.iterrows():
    cross_type = "金叉" if row['cross_change'] > 0 else "死叉"
    print(f"  {row['trade_date']} {cross_type} DIF={row['dif']:.4f}")

print("\n" + "=" * 80)
print("三、市场估值水平")
print("=" * 80)

# 上证综指、沪深300的PE/PB
for ts_code, name in [('000001.SH', '上证综指'), ('000300.SH', '沪深300')]:
    sql = f"""
    SELECT trade_date, pe, pe_ttm, pb, total_mv, float_mv, turnover_rate
    FROM index_dailybasic
    WHERE ts_code = '{ts_code}'
      AND trade_date >= '{start_date}'
      AND trade_date <= '{end_date}'
    ORDER BY trade_date
    """
    df_val = pd.read_sql(sql, read_engine)
    if df_val.empty:
        print(f"\n{name}: 无估值数据")
        continue

    latest_val = df_val.iloc[-1]
    print(f"\n--- {name} 估值 ---")
    print(f"  最新PE_TTM: {latest_val['pe_ttm']:.2f}  PB: {latest_val['pb']:.2f}")
    print(f"  PE_TTM范围: {df_val['pe_ttm'].min():.2f} ~ {df_val['pe_ttm'].max():.2f}")
    print(f"  PB范围:     {df_val['pb'].min():.2f} ~ {df_val['pb'].max():.2f}")

    # 历史分位数（用更长数据）
    sql_long = f"""
    SELECT pe_ttm, pb
    FROM index_dailybasic
    WHERE ts_code = '{ts_code}'
      AND trade_date >= '20150101'
      AND trade_date <= '{end_date}'
    ORDER BY trade_date
    """
    df_long = pd.read_sql(sql_long, read_engine)
    if not df_long.empty:
        pe_pct = (df_long['pe_ttm'] < latest_val['pe_ttm']).mean() * 100
        pb_pct = (df_long['pb'] < latest_val['pb']).mean() * 100
        print(f"  PE_TTM历史分位(2015至今): {pe_pct:.1f}%")
        print(f"  PB历史分位(2015至今):     {pb_pct:.1f}%")

print("\n" + "=" * 80)
print("四、北向资金流向")
print("=" * 80)

sql = f"""
SELECT trade_date, north_money, south_money
FROM moneyflow_hsgt
WHERE trade_date >= '{start_date}'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_hsgt = pd.read_sql(sql, read_engine)
if not df_hsgt.empty:
    # north_money 单位: 百万元
    df_hsgt['north_money_yi'] = df_hsgt['north_money'] / 100  # 转换为亿
    total_north = df_hsgt['north_money_yi'].sum()
    recent_20d = df_hsgt.tail(20)['north_money_yi'].sum()
    recent_60d = df_hsgt.tail(60)['north_money_yi'].sum()

    print(f"近一年北向资金合计: {total_north:.2f} 亿元")
    print(f"近20个交易日: {recent_20d:.2f} 亿元")
    print(f"近60个交易日: {recent_60d:.2f} 亿元")

    # 按月统计
    df_hsgt['month'] = df_hsgt['trade_date'].str[:6]
    monthly = df_hsgt.groupby('month')['north_money_yi'].sum()
    print("\n北向资金月度流入:")
    for month, val in monthly.items():
        print(f"  {month}: {val:+.2f} 亿")
else:
    print("无北向资金数据")

print("\n" + "=" * 80)
print("五、融资融券余额")
print("=" * 80)

sql = f"""
SELECT trade_date, rzye, rqye, rzmre, rzche
FROM margin
WHERE trade_date >= '{start_date}'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_margin = pd.read_sql(sql, read_engine)
if not df_margin.empty:
    latest_m = df_margin.iloc[-1]
    print(f"最新融资余额: {latest_m['rzye']/1e8:.2f} 亿元")
    print(f"最新融券余额: {latest_m['rqye']/1e8:.2f} 亿元")
    print(f"一年前融资余额: {df_margin.iloc[0]['rzye']/1e8:.2f} 亿元")
    print(f"融资余额变化: {(latest_m['rzye'] - df_margin.iloc[0]['rzye'])/1e8:+.2f} 亿元")
    print(f"年内融资余额最高: {df_margin['rzye'].max()/1e8:.2f} 亿元")
    print(f"年内融资余额最低: {df_margin['rzye'].min()/1e8:.2f} 亿元")
else:
    print("无融资融券数据")

print("\n" + "=" * 80)
print("六、大盘资金流向（东财）")
print("=" * 80)

sql = f"""
SELECT trade_date, net_amount, buy_elg_amount, buy_lg_amount, buy_md_amount, buy_sm_amount
FROM moneyflow_mkt_dc
WHERE trade_date >= '{start_date}'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_mf = pd.read_sql(sql, read_engine)
if not df_mf.empty:
    # net_amount 是主力净流入（亿元）
    df_mf['month'] = df_mf['trade_date'].str[:6]
    monthly_mf = df_mf.groupby('month')['net_amount'].sum()
    print("大盘主力净流入月度统计（亿元）:")
    for month, val in monthly_mf.items():
        print(f"  {month}: {val:+.2f}")

    # 近期趋势
    recent_20 = df_mf.tail(20)['net_amount'].sum()
    recent_5 = df_mf.tail(5)['net_amount'].sum()
    print(f"\n近5个交易日主力净流入: {recent_5:+.2f} 亿")
    print(f"近20个交易日主力净流入: {recent_20:+.2f} 亿")
else:
    print("无大盘资金流向数据")

print("\n" + "=" * 80)
print("七、市场情绪指标")
print("=" * 80)

# 涨跌家数统计
sql = f"""
SELECT trade_date, up_count, down_count, limit_up_count, limit_down_count
FROM daily_info
WHERE trade_date >= '{start_date}'
  AND trade_date <= '{end_date}'
  AND exchange = 'SH'
ORDER BY trade_date
"""
df_info = pd.read_sql(sql, read_engine)
if not df_info.empty:
    # 最近涨跌家数
    latest_info = df_info.iloc[-1]
    print(f"最新交易日({latest_info['trade_date']}):")
    print(f"  上涨: {latest_info['up_count']}  下跌: {latest_info['down_count']}  涨停: {latest_info['limit_up_count']}  跌停: {latest_info['limit_down_count']}")

    # 近20日平均
    recent = df_info.tail(20)
    print(f"\n近20日平均:")
    print(f"  上涨: {recent['up_count'].mean():.0f}  下跌: {recent['down_count'].mean():.0f}")
    print(f"  涨停: {recent['limit_up_count'].mean():.1f}  跌停: {recent['limit_down_count'].mean():.1f}")
else:
    print("无市场情绪数据")

# 换手率变化
sql = f"""
SELECT trade_date, turnover_rate
FROM index_dailybasic
WHERE ts_code = '000001.SH'
  AND trade_date >= '{start_date}'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_turn = pd.read_sql(sql, read_engine)
if not df_turn.empty:
    print(f"\n上证综指换手率:")
    print(f"  最新: {df_turn.iloc[-1]['turnover_rate']:.2f}%")
    print(f"  年内均值: {df_turn['turnover_rate'].mean():.2f}%")
    print(f"  近20日均值: {df_turn.tail(20)['turnover_rate'].mean():.2f}%")
    print(f"  年内最高: {df_turn['turnover_rate'].max():.2f}%  最低: {df_turn['turnover_rate'].min():.2f}%")

print("\n" + "=" * 80)
print("八、宏观经济指标")
print("=" * 80)

# PMI
sql = f"""
SELECT month, pmi, pmi_mp
FROM macro_cn_pmi
WHERE month >= '202503'
ORDER BY month DESC
LIMIT 12
"""
df_pmi = pd.read_sql(sql, read_engine)
if df_pmi.empty:
    # 尝试不同日期范围
    sql = f"""
    SELECT month, pmi, pmi_mp
    FROM macro_cn_pmi
    ORDER BY month DESC
    LIMIT 12
    """
    df_pmi = pd.read_sql(sql, read_engine)

if not df_pmi.empty:
    print("\nPMI (近12月):")
    for _, row in df_pmi.iterrows():
        pmi_val = row.get('pmi', None)
        pmi_mp_val = row.get('pmi_mp', None)
        line = f"  {row['month']}: "
        if pmi_val is not None and not pd.isna(pmi_val):
            line += f"制造业PMI={pmi_val:.1f}"
        if pmi_mp_val is not None and not pd.isna(pmi_mp_val):
            line += f"  非制造业={pmi_mp_val:.1f}"
        print(line)

# 社融
sql = """
SELECT month, total
FROM macro_sf_month
ORDER BY month DESC
LIMIT 12
"""
df_sf = pd.read_sql(sql, read_engine)
if not df_sf.empty:
    print("\n社会融资规模增量 (近12月, 亿元):")
    for _, row in df_sf.iterrows():
        print(f"  {row['month']}: {row['total']:.0f}")

# M2
sql = """
SELECT month, m2_yoy, m1_yoy, m0_yoy
FROM macro_cn_m
ORDER BY month DESC
LIMIT 12
"""
df_m = pd.read_sql(sql, read_engine)
if not df_m.empty:
    print("\n货币供应量同比增速 (近12月):")
    for _, row in df_m.iterrows():
        print(f"  {row['month']}: M2={row['m2_yoy']:.1f}%  M1={row['m1_yoy']:.1f}%  M0={row['m0_yoy']:.1f}%")

# LPR
sql = """
SELECT date, 1y, 5y
FROM macro_shibor_lpr
ORDER BY date DESC
LIMIT 12
"""
df_lpr = pd.read_sql(sql, read_engine)
if not df_lpr.empty:
    print("\nLPR利率 (近12次报价):")
    for _, row in df_lpr.iterrows():
        print(f"  {row['date']}: 1Y={row['1y']:.2f}%  5Y={row['5y']:.2f}%")

print("\n" + "=" * 80)
print("九、关键阶段区间统计")
print("=" * 80)

# 上证综指 - 按季度统计
sql = f"""
SELECT trade_date, close, vol, amount
FROM index_daily
WHERE ts_code = '000001.SH'
  AND trade_date >= '{start_date}'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_q = pd.read_sql(sql, read_engine)
df_q['quarter'] = pd.to_datetime(df_q['trade_date'], format='%Y%m%d').dt.to_period('Q').astype(str)

for q, grp in df_q.groupby('quarter'):
    q_return = (grp.iloc[-1]['close'] / grp.iloc[0]['close'] - 1) * 100
    avg_amount = grp['amount'].mean()
    print(f"  {q}: 涨跌{q_return:+.2f}%  日均成交额{avg_amount/1e6:.0f}亿  收盘{grp.iloc[0]['close']:.0f}→{grp.iloc[-1]['close']:.0f}")

print("\n" + "=" * 80)
print("十、历史牛熊对比参考")
print("=" * 80)

# 取上证近10年数据看当前位置
sql = f"""
SELECT trade_date, close
FROM index_daily
WHERE ts_code = '000001.SH'
  AND trade_date >= '20150101'
  AND trade_date <= '{end_date}'
ORDER BY trade_date
"""
df_hist = pd.read_sql(sql, read_engine)
if not df_hist.empty:
    current_close = df_hist.iloc[-1]['close']
    hist_high = df_hist['close'].max()
    hist_low = df_hist['close'].min()

    print(f"2015年至今 上证综指:")
    print(f"  最高: {hist_high:.2f}  最低: {hist_low:.2f}  当前: {current_close:.2f}")
    print(f"  当前位于历史区间的 {(current_close - hist_low) / (hist_high - hist_low) * 100:.1f}%")

    # 历史关键点位
    key_levels = [2600, 2800, 3000, 3200, 3400, 3600]
    print(f"\n  距关键点位:")
    for level in key_levels:
        pct = (current_close / level - 1) * 100
        print(f"    {level}: {pct:+.2f}%")

print("\n" + "=" * 80)
print("分析完成")
print("=" * 80)
