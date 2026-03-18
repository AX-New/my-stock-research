# 数据库设计文档
**创建时间**: 20260319

## 双库架构

| 库 | 用途 | 引擎 | 说明 |
|----|------|------|------|
| `my_stock` | 读生产数据 | `read_engine` | my-stock 项目维护，Tushare 同步入库 |
| `stock_research` | 写研究结果 | `write_engine` | 本项目维护，各主题计算结果 |

连接配置在 `lib/config.py` 或各主题 `scripts/config.py`。

---

## my_stock 库 — 常用读取表

> 完整接口文档见 `tushare_docs/`，按 Tushare 分类组织。

### 基础数据

| 表名 | 说明 | 关键字段 | 唯一键 |
|------|------|---------|--------|
| `stock_basic` | 股票列表 | ts_code, name, industry, list_status, cnspell | ts_code |
| `trade_cal` | 交易日历 | exchange, cal_date, is_open | exchange, cal_date |
| `index_basic` | 指数基本信息 | ts_code, name, market, publisher | ts_code |
| `index_classify` | 指数分类 | index_code, index_name, level, industry_name | — |
| `index_member_all` | 指数成分股 | index_code, con_code, in_date, out_date | — |

### 行情数据

| 表名 | 说明 | 关键字段 | 唯一键 |
|------|------|---------|--------|
| `market_daily` | 个股日线(不复权) | ts_code, trade_date, open/high/low/close, vol, amount | ts_code, trade_date |
| `market_weekly` | 个股周线 | 同上 | ts_code, trade_date |
| `market_monthly` | 个股月线 | 同上 | ts_code, trade_date |
| `adj_factor` | 复权因子 | ts_code, trade_date, adj_factor | ts_code, trade_date |
| `daily_basic` | 每日指标 | ts_code, trade_date, pe/pb/ps, total_mv, circ_mv, turnover_rate | ts_code, trade_date |
| `index_daily` | 指数日线 | ts_code, trade_date, open/high/low/close, vol, amount | ts_code, trade_date |
| `index_weekly` | 指数周线 | 同上 | ts_code, trade_date |
| `index_monthly` | 指数月线 | 同上 | ts_code, trade_date |
| `sw_daily` | 申万行业日线 | ts_code, trade_date, open/high/low/close, vol, amount | ts_code, trade_date |

### 资金流向

| 表名 | 说明 | 关键字段 | 唯一键 |
|------|------|---------|--------|
| `moneyflow` | 个股资金流向 | ts_code, trade_date, buy_sm/md/lg/elg_amount | ts_code, trade_date |
| `moneyflow_ind_dc` | 行业资金流向(东财) | name, trade_date, net_amount | — |
| `moneyflow_mkt_dc` | 大盘资金流向(东财) | trade_date, net_amount | — |

### 财务数据

| 表名 | 说明 | 关键字段 | 唯一键 |
|------|------|---------|--------|
| `finance_income` | 利润表 | ts_code, end_date, revenue, n_income | ts_code, end_date, report_type |
| `finance_balancesheet` | 资产负债表 | ts_code, end_date, total_assets, total_liab | ts_code, end_date, report_type |
| `finance_cashflow` | 现金流量表 | ts_code, end_date, n_cashflow_act | ts_code, end_date, report_type |
| `finance_fina_indicator` | 财务指标 | ts_code, end_date, roe, roa, debt_to_assets | ts_code, end_date |
| `finance_dividend` | 分红送股 | ts_code, end_date, cash_div_tax | ts_code, end_date |

### LA 选股相关

| 表名 | 说明 | 关键字段 | 唯一键 |
|------|------|---------|--------|
| `la_pick` | LA 选股结果 | eval_date, ts_code, model, version, score, reason | — |
| `la_indicator` | LA 技术指标 | ts_code, trade_date, 各技术面/资金面字段 | ts_code, trade_date |

### 复权计算方式

研究脚本读取 `market_daily`（不复权）+ `adj_factor`，自行计算复权价：
- **前复权(qfq)**: `close * (adj_factor / 最新adj_factor)`
- **后复权(hfq)**: `close * adj_factor`
- **禁止使用 bfq（不复权）做指标计算**，详见 `docs/constraints/020-复权选型规则.md`

---

## stock_research 库 — 研究产出表

### MACD 主题

| 表名 | 说明 |
|------|------|
| `index_macd_daily/weekly/monthly/yearly` | 指数 MACD 指标值 |
| `stock_macd_daily_qfq/hfq/bfq` | 个股 MACD（按复权类型） |
| `stock_macd_weekly/monthly/yearly_*` | 个股周/月/年线 MACD |
| `sw_macd_daily/weekly/monthly` | 申万行业 MACD |
| `stock_macd_signal` | 个股 MACD 信号（金叉/死叉） |
| `stock_macd_signal_stats` | 信号统计汇总 |

### 其他主题（按需创建）

各主题的 `scripts/models.py` 定义写入表结构，运行 `init_research_tables()` 自动建表。

---

## 查找 Tushare 接口

如果研究需要的数据在 `my_stock` 库中没有，可以查阅 Tushare 文档自行接入：

1. **查接口目录**: `tushare_docs/interface_catalog.csv` — 所有接口的 api_name、分类、doc_id
2. **查接口详情**: `tushare_docs/document/2/doc_id-{id}.md` — 输入参数、输出字段、调用示例
3. **按分类浏览**: `tushare_docs/股票数据/`、`tushare_docs/指数专题/` 等

研究脚本可以直接调用 Tushare API 获取数据（不需要通过 my-stock 的同步机制）：

```python
import tushare as ts
pro = ts.pro_api()  # 需要 TUSHARE_TOKEN 环境变量
df = pro.daily(ts_code='000001.SZ', start_date='20230101')
```
