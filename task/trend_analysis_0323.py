# -*- coding: utf-8 -*-
"""2026-03-23 my_trend 数据分析脚本"""
import pymysql
import sys

# 设置控制台编码
sys.stdout.reconfigure(encoding='utf-8')

conn = pymysql.connect(
    host='127.0.0.1', port=3310, user='root', password='root',
    database='my_trend', charset='utf8mb4'
)
cur = conn.cursor()
DATE = '2026-03-23'

def safe(val, width=8):
    """安全格式化，处理None"""
    if val is None:
        return '-'.rjust(width)
    if isinstance(val, float):
        return f'{val:.2f}'.rjust(width)
    return str(val).rjust(width)

# =============================================
# 1. 热度排名 TOP 30
# =============================================
print('='*80)
print('一、今日热度排名 TOP 30')
print('='*80)
cur.execute(
    'SELECT stock_code, stock_name, `rank`, new_price, change_rate, '
    'volume_ratio, turnover_rate, deal_amount '
    'FROM popularity_rank WHERE date=%s ORDER BY `rank` LIMIT 30', (DATE,)
)
print(f'{"排名":>4} | {"代码":<8} {"名称":<10} | {"现价":>8} | {"涨跌%":>7} | {"量比":>6} | {"换手%":>6} | {"成交额(亿)":>10}')
print('-'*80)
for row in cur.fetchall():
    amt = row[7]/1e8 if row[7] else 0
    print(f'{row[2]:>4} | {row[0]:<8} {row[1]:<10} | {safe(row[3])} | {safe(row[4],7)} | {safe(row[5],6)} | {safe(row[6],6)} | {amt:>10.2f}')

# =============================================
# 2. 热度飙升（去重，每只股票取最大飙升值）
# =============================================
print()
print('='*80)
print('二、今日热度飙升 TOP（去重后）')
print('='*80)
cur.execute(
    'SELECT stock_code, stock_name, MIN(rank_today) as best_rank, MAX(rank_change) as max_change, '
    'MAX(new_price) as price, MAX(change_rate) as chg '
    'FROM heat_change_top WHERE date=%s '
    'GROUP BY stock_code, stock_name ORDER BY max_change DESC LIMIT 20', (DATE,)
)
print(f'{"代码":<8} {"名称":<10} | {"最佳排名":>6} | {"最大飙升":>6} | {"价格":>8} | {"涨跌%":>7}')
print('-'*70)
for row in cur.fetchall():
    print(f'{row[0]:<8} {row[1]:<10} | {safe(row[2],6)} | {safe(row[3],6)} | {safe(row[4])} | {safe(row[5],7)}')

# =============================================
# 3. 行业分析
# =============================================
print()
print('='*80)
print('三、行业分析（申万一级 31行业）')
print('='*80)
cur.execute(
    'SELECT industry_name, sentiment, score, summary '
    'FROM industry_analysis WHERE date=%s ORDER BY score DESC', (DATE,)
)
rows = cur.fetchall()
pos = sum(1 for r in rows if r[1] == 'positive')
neg = sum(1 for r in rows if r[1] == 'negative')
neu = sum(1 for r in rows if r[1] == 'neutral')
print(f'情绪分布: 正面={pos} 中性={neu} 负面={neg}')
print()
print(f'{"行业":<8} | {"情绪":<8} | {"评分":>5} | {"摘要"}')
print('-'*80)
for row in rows:
    s = (row[3] or '')[:70]
    print(f'{row[0]:<8} | {row[1]:<8} | {safe(row[2],5)} | {s}')

# =============================================
# 4. 板块分析
# =============================================
print()
print('='*80)
print('四、板块分析概况')
print('='*80)
cur.execute(
    'SELECT sentiment, COUNT(*) as cnt, ROUND(AVG(score),1) as avg_score '
    'FROM sector_analysis WHERE date=%s GROUP BY sentiment ORDER BY avg_score DESC', (DATE,)
)
for row in cur.fetchall():
    print(f'{row[0]:<8} | 数量:{row[1]:>4} | 均分:{row[2]}')

print()
print('--- 板块 TOP 15（最看好）---')
cur.execute(
    'SELECT sector_name, sentiment, score, summary '
    'FROM sector_analysis WHERE date=%s ORDER BY score DESC LIMIT 15', (DATE,)
)
for row in cur.fetchall():
    s = (row[3] or '')[:60]
    print(f'{row[0]:<14} | {row[1]:<8} | {safe(row[2],5)} | {s}')

print()
print('--- 板块 BOTTOM 10（最看空）---')
cur.execute(
    'SELECT sector_name, sentiment, score, summary '
    'FROM sector_analysis WHERE date=%s ORDER BY score ASC LIMIT 10', (DATE,)
)
for row in cur.fetchall():
    s = (row[3] or '')[:60]
    print(f'{row[0]:<14} | {row[1]:<8} | {safe(row[2],5)} | {s}')

# =============================================
# 5. 新闻分析
# =============================================
print()
print('='*80)
print('五、新闻分析概况')
print('='*80)
cur.execute(
    'SELECT analysis_type, COUNT(*) as cnt, '
    'SUM(CASE WHEN sentiment="positive" THEN 1 ELSE 0 END) as pos, '
    'SUM(CASE WHEN sentiment="negative" THEN 1 ELSE 0 END) as neg, '
    'SUM(CASE WHEN sentiment="neutral" THEN 1 ELSE 0 END) as neu, '
    'ROUND(AVG(score),1) as avg_score '
    'FROM news_analysis WHERE date=%s GROUP BY analysis_type', (DATE,)
)
for row in cur.fetchall():
    print(f'{row[0]:<10} | 总数:{row[1]:>3} | 正面:{row[2]:>3} | 负面:{row[3]:>3} | 中性:{row[4]:>3} | 均分:{row[5]}')

print()
print('--- 宏观新闻分析 ---')
cur.execute(
    'SELECT analysis_type, sentiment, score, summary '
    'FROM news_analysis WHERE date=%s AND analysis_type IN ("global", "domestic") '
    'ORDER BY analysis_type, score DESC', (DATE,)
)
for row in cur.fetchall():
    s = (row[3] or '')[:100]
    print(f'{row[0]:<10} | {row[1]:<8} | {safe(row[2],5)} | {s}')

print()
print('--- 个股新闻 TOP 15（最看好）---')
cur.execute(
    'SELECT stock_code, stock_name, sentiment, score, summary '
    'FROM news_analysis WHERE date=%s AND analysis_type="stock" '
    'ORDER BY score DESC LIMIT 15', (DATE,)
)
for row in cur.fetchall():
    s = (row[4] or '')[:70]
    print(f'{row[0]} {row[1]:<8} | {row[2]:<8} | {safe(row[3],5)} | {s}')

print()
print('--- 个股新闻 BOTTOM 10（最看空）---')
cur.execute(
    'SELECT stock_code, stock_name, sentiment, score, summary '
    'FROM news_analysis WHERE date=%s AND analysis_type="stock" '
    'ORDER BY score ASC LIMIT 10', (DATE,)
)
for row in cur.fetchall():
    s = (row[4] or '')[:70]
    print(f'{row[0]} {row[1]:<8} | {row[2]:<8} | {safe(row[3],5)} | {s}')

# =============================================
# 6. 热度与涨跌关联
# =============================================
print()
print('='*80)
print('六、热度 TOP 30 涨跌分布')
print('='*80)
cur.execute(
    'SELECT stock_code, stock_name, `rank`, change_rate '
    'FROM popularity_rank WHERE date=%s ORDER BY `rank` LIMIT 30', (DATE,)
)
rows = cur.fetchall()
up = sum(1 for r in rows if r[3] and r[3] > 0)
down = sum(1 for r in rows if r[3] and r[3] < 0)
flat = sum(1 for r in rows if r[3] is None or r[3] == 0)
limit_up = sum(1 for r in rows if r[3] and r[3] >= 9.9)
limit_down = sum(1 for r in rows if r[3] and r[3] <= -9.9)
avg_chg = sum(r[3] for r in rows if r[3]) / len([r for r in rows if r[3]]) if rows else 0
print(f'上涨: {up}  下跌: {down}  平盘: {flat}')
print(f'涨停(>=9.9%): {limit_up}  跌停(<=-9.9%): {limit_down}')
print(f'平均涨跌幅: {avg_chg:.2f}%')

# =============================================
# 7. 热度排名 vs 前一日对比
# =============================================
print()
print('='*80)
print('七、热度排名变化（今日 vs 前一日 TOP 30）')
print('='*80)
cur.execute(
    'SELECT a.stock_code, a.stock_name, a.`rank` as today_rank, '
    'b.`rank` as yesterday_rank, (b.`rank` - a.`rank`) as rank_up, '
    'a.change_rate '
    'FROM popularity_rank a '
    'LEFT JOIN popularity_rank b ON a.stock_code = b.stock_code AND b.date = '
    '(SELECT MAX(date) FROM popularity_rank WHERE date < %s) '
    'WHERE a.date = %s ORDER BY a.`rank` LIMIT 30', (DATE, DATE)
)
print(f'{"代码":<8} {"名称":<10} | {"今日":>4} | {"昨日":>4} | {"排名升":>5} | {"涨跌%":>7}')
print('-'*60)
for row in cur.fetchall():
    yd = str(row[3]) if row[3] else '新进'
    ru = str(row[4]) if row[4] else '-'
    print(f'{row[0]:<8} {row[1]:<10} | {row[2]:>4} | {yd:>4} | {ru:>5} | {safe(row[5],7)}')

conn.close()
print()
print('分析完成。')
