# my_trend 数据表目录
**最后更新**: 20260320（基于腾讯云 MySQL 实际抓取）

my-trend 项目维护，运行在腾讯云（`ssh root@154.8.136.130`），爬虫定时采集东方财富/新闻/股吧等舆情数据。**只读**。

> 共 14 张表，按模块分组如下。

## 热度排名（heat 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `popularity_rank` | ~199万 | 每日17:00 | 东财人气排名日快照（rank/最新价/涨跌幅/量比/换手率/成交量/成交额） |
| `em_hot_rank_detail` | ~198万 | 首次回溯 | 个股历史趋势（排名/新增粉丝/铁杆粉丝），366天 |
| `em_hot_keyword` | ~4600 | 按需 | 个股热门关键词（概念名称/概念代码/热度值） |
| `heat_stock_minute` | ~476 | 每日17:20 | 热度Top股票1分钟K线（OHLCV），信号股收盘后采集 |

## 盘中实时热度（heat_live 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `popularity_rank_live` | ~5320 | 盘中×8次 | 全市场实时排名（TRUNCATE覆盖，datetime.now()取时间） |
| `heat_change_top` | ~20 | 盘中×8次 | 热度飙升Top20（rank变化/入选时间/价格/涨跌幅/量比/换手率） |

## 新闻资讯（news 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `articles` | ~78 | 按需采集 | 个股新闻文章（来源/分类/标题/URL/内容/去重计数），7天滚动 |

## LLM 分析（analysis 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `news_analysis` | ~3900 | 按需采集 | LLM新闻分析（类型: global/domestic/stock，情感/摘要/评分/详情JSON） |
| `analysis_failure` | ~95 | 伴随analysis | 分析失败记录（阶段: search/llm/guba，错误/重试/解决状态） |
| `analysis_run` | ~12 | 伴随analysis | 分析任务运行记录（run_id/状态/计数/游标） |

## 股吧情绪（guba 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `guba_sentiment` | ~263 | 按需采集 | 股吧情感评分（score -1~+1/看多看空中性数/阅读量/活跃度/人气排名） |
| `guba_post_detail` | ~3 | 伴随guba | 股吧帖子明细（标题/点击/评论/转发/LLM标签1=多/-1=空/权重1-5） |

## 行业分析（industry 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `industry_analysis` | ~32 | 每日0:00 | 申万L1 31个行业LLM分析（情感/摘要/百分制评分/详情JSON） |

## 板块分析（sector 模块）

| 表名 | 行数 | 更新频率 | 说明 |
|------|------|---------|------|
| `sector_analysis` | ~69 | 每日1:00 | 东财~1010个板块LLM分析（情感/摘要/百分制评分/详情JSON） |

## 采集调度（腾讯云 crontab）

```
盘中（交易日 周一至周五）:
  09:20,09:50,10:50,12:00,12:50,13:50,14:20,15:00  heat_live.main     → popularity_rank_live
  09:30,10:00,11:00,12:10,13:00,14:00,14:30,15:10  heat_live.analyze  → heat_change_top

收盘后（交易日 周一至周五）:
  17:00         heat.main                → popularity_rank
  17:20         heat.minute              → heat_stock_minute

凌晨（每日）:
  00:00         industry.main            → industry_analysis
  01:00         sector.main              → sector_analysis
```

## 研究常用查询

```sql
-- 某只股票的热度排名历史
SELECT * FROM popularity_rank WHERE stock_code='000001' ORDER BY date DESC LIMIT 30;

-- 盘中热度飙升信号（最近一天）
SELECT * FROM heat_change_top WHERE date = (SELECT MAX(date) FROM heat_change_top) ORDER BY rank_change DESC;

-- 个股新闻分析（最新）
SELECT * FROM news_analysis WHERE stock_code='000001' AND analysis_type='stock' ORDER BY date DESC LIMIT 5;

-- 行业情感分布
SELECT industry_name, sentiment, score FROM industry_analysis WHERE date = (SELECT MAX(date) FROM industry_analysis) ORDER BY score DESC;
```
