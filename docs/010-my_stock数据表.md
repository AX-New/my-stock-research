# my_stock 数据表目录
**最后更新**: 20260320

my-stock 项目维护，Tushare/AkShare 同步入库。**只读**。

> 完整接口文档见 `tushare_docs/`，查 `tushare_docs/interface_catalog.csv` 可定位 doc_id。

## 基础数据

| 表名 | 说明 |
|------|------|
| `stock_basic` | A股上市公司基本信息（代码/名称/行业/上市日期/市场类型） |
| `stock_company` | 上市公司工商信息（注册资本/法人/员工数/经营范围） |
| `trade_cal` | 交易日历（每日是否为交易日） |
| `stock_st` | ST/退市风险股票标识 |
| `new_share` | IPO新股信息（发行价/中签率/募集资金） |

## 行情数据

| 表名 | 说明 |
|------|------|
| `market_daily` | 个股日线行情（OHLCV，不复权） |
| `market_weekly` | 个股周线行情 |
| `market_monthly` | 个股月线行情 |
| `adj_factor` | 复权因子（每日前复权/后复权系数） |
| `daily_basic` | 每日基础指标（PE/PB/PS/市值/换手率） |
| `suspend_d` | 每日停复牌信息 |
| `realtime_kline` | 个股实时日线（AkShare，TTL=15s） |
| `realtime_min` | 个股实时分钟线（AkShare） |

## 指数数据

| 表名 | 说明 |
|------|------|
| `index_basic` | 指数基本信息（代码/名称/市场/发布方） |
| `index_daily` | 指数日线行情（OHLCV） |
| `index_weekly` | 指数周线行情 |
| `index_monthly` | 指数月线行情 |
| `index_dailybasic` | 指数每日指标（PE_TTM/PB/股息率/总市值） |
| `index_weight` | 指数成分股权重 |
| `index_classify` | 申万行业分类（层级/行业名） |
| `index_member_all` | 申万行业成分股（含进出日期） |
| `index_realtime_kline` | 指数实时日线（AkShare） |
| `sw_daily` | 申万行业指数日线行情 |
| `index_global` | 全球主要指数行情（道琼斯/纳斯达克/日经等） |

## 概念板块

| 表名 | 说明 |
|------|------|
| `dc_index` | 东财概念板块列表 |
| `dc_member` | 东财概念板块成分股 |
| `dc_daily` | 东财概念板块日线行情 |
| `dc_hot` | 东财概念热度排名 |
| `ths_index` | 同花顺概念指数列表 |
| `ths_member` | 同花顺概念成分股 |
| `ths_daily` | 同花顺概念指数日线行情 |
| `ths_hot` | 同花顺热股榜 |
| `tdx_index` | 通达信板块列表 |
| `tdx_member` | 通达信板块成分股 |
| `tdx_daily` | 通达信板块日线行情 |
| `kpl_list` | 开盘啦涨停榜 |
| `kpl_concept_cons` | 开盘啦概念成分股 |
| `ci_daily` | 中信行业指数日线行情 |
| `ci_index_member` | 中信行业成分股 |

## 资金流向

| 表名 | 说明 |
|------|------|
| `moneyflow` | 个股资金流向（超大/大/中/小单统计） |
| `moneyflow_dc` | 个股资金流向（东财DC数据源） |
| `moneyflow_ind_dc` | 行业资金流向（东财DC） |
| `moneyflow_mkt_dc` | 大盘资金流向（东财DC） |
| `moneyflow_hsgt` | 沪深港通资金流向 |

## 财务数据

| 表名 | 说明 |
|------|------|
| `finance_income` | 利润表（营收/净利润/毛利润） |
| `finance_balancesheet` | 资产负债表（总资产/总负债/股东权益） |
| `finance_cashflow` | 现金流量表（经营/投资/筹资活动） |
| `finance_fina_indicator` | 财务指标汇总（ROE/ROA/毛利率/EPS等60+指标） |
| `finance_forecast` | 业绩预告（预计净利润变动） |
| `finance_express` | 业绩快报（正式披露前简要数据） |
| `finance_dividend` | 分红送股（每股分红/除权日） |
| `finance_fina_audit` | 财务审计意见 |
| `finance_fina_mainbz` | 主营业务构成（按产品/地区） |
| `finance_disclosure_date` | 财报披露计划日期 |

## 两融及转融通

| 表名 | 说明 |
|------|------|
| `margin` | 融资融券交易汇总（每日余额） |
| `margin_detail` | 融资融券明细（个股级别） |
| `margin_secs` | 融资融券标的清单 |
| `slb_len` | 转融通余量汇总 |
| `slb_len_mm` | 转融通余量明细 |

## 涨跌停/龙虎榜

| 表名 | 说明 |
|------|------|
| `limit_list_d` | 每日涨跌停统计 |
| `limit_list_ths` | 同花顺涨停榜 |
| `limit_cpt_list` | 涨跌停概念统计 |
| `limit_step` | 连板统计 |
| `top_list` | 龙虎榜每日明细 |
| `top_inst` | 龙虎榜机构席位明细 |
| `block_trade` | 大宗交易 |

## 股东/机构

| 表名 | 说明 |
|------|------|
| `top10_holders` | 前十大股东 |
| `top10_floatholders` | 前十大流通股东 |
| `stk_holdernumber` | 股东户数变化 |
| `stk_holdertrade` | 股东增减持 |
| `share_float` | 限售股解禁 |
| `repurchase` | 股票回购 |

## 特色/参考数据

| 表名 | 说明 |
|------|------|
| `stk_surv` | 机构调研记录 |
| `stk_nineturn` | 九转序列（TD序列信号） |
| `broker_recommend` | 券商月度推荐 |
| `report_rc` | 盈利预测（分析师EPS/营收预测） |
| `hm_list` | 游资营业部信息 |
| `daily_info` | 每日市场统计（上涨家数/成交额等） |
| `sz_daily_info` | 深市每日统计 |

## 宏观经济

| 表名 | 说明 |
|------|------|
| `macro_cn_cpi` | CPI 居民消费价格指数 |
| `macro_cn_ppi` | PPI 工业生产者出厂价格指数 |
| `macro_cn_gdp` | GDP 国内生产总值 |
| `macro_cn_pmi` | PMI 采购经理指数 |
| `macro_cn_m` | M0/M1/M2 货币供应量 |
| `macro_sf_month` | 社会融资规模 |
| `macro_shibor` | Shibor 利率 |
| `macro_shibor_quote` | Shibor 报价明细 |
| `macro_shibor_lpr` | LPR 贷款基础利率 |
| `macro_hibor` | Hibor 利率 |
| `macro_libor` | Libor 利率 |
| `macro_gz_index` | 广州民间借贷利率 |
| `macro_wz_index` | 温州民间借贷利率 |
| `macro_us_tycr` | 美国国债收益率曲线 |
| `macro_us_trycr` | 美国国债实际收益率 |
| `macro_us_tltr` | 美国长期国债利率 |
| `macro_us_trltr` | 美国实际长期利率 |
| `macro_us_tbr` | 美国短期国债利率 |

## LA 选股

| 表名 | 说明 |
|------|------|
| `la_pick` | LA 选股结果（日期/股票/模型/评分/理由） |
| `la_indicator` | LA 技术指标（技术面+资金面综合指标） |
| `la_factor` | LA 因子值 |
| `la_task` | LA 选股任务记录 |
| `la_version` | LA 版本管理 |
