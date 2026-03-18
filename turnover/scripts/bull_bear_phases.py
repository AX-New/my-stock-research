"""A股牛熊周期数据 + 标注工具

数据来源: 上证指数月线级别牛熊周期划分
文档: report/01-a-share-bull-bear-cycles.md

用途:
1. 给任意交易日期标注所属牛熊阶段
2. 提供已知的顶部/底部时间点列表
3. 后续 Layer 2/3/4 可替换为其他指数的周期数据
"""
import pandas as pd


# 上证指数 21 个牛熊阶段
# start_ym: 阶段开始月份(含), 即上一阶段顶/底所在月
# 查找逻辑: 找最后一个 start_ym <= 目标月份 的阶段
SH_PHASES = [
    {"id": 1,  "trend": "bull", "start_ym": "199012", "start_point": 100,  "end_point": 1429, "pct_chg": 1329.0, "duration_months": 17, "label": "沪市开市"},
    {"id": 2,  "trend": "bear", "start_ym": "199205", "start_point": 1429, "end_point": 387,  "pct_chg": -73.0,  "duration_months": 6,  "label": "新股扩容"},
    {"id": 3,  "trend": "bull", "start_ym": "199211", "start_point": 387,  "end_point": 1559, "pct_chg": 303.0,  "duration_months": 3,  "label": "南巡讲话"},
    {"id": 4,  "trend": "bear", "start_ym": "199302", "start_point": 1559, "end_point": 326,  "pct_chg": -79.0,  "duration_months": 17, "label": "紧缩调控"},
    {"id": 5,  "trend": "bull", "start_ym": "199407", "start_point": 326,  "end_point": 1053, "pct_chg": 223.0,  "duration_months": 2,  "label": "三大救市政策"},
    {"id": 6,  "trend": "bear", "start_ym": "199409", "start_point": 1053, "end_point": 513,  "pct_chg": -51.0,  "duration_months": 16, "label": "政策利好消退"},
    {"id": 7,  "trend": "bull", "start_ym": "199601", "start_point": 513,  "end_point": 1510, "pct_chg": 194.0,  "duration_months": 16, "label": "降息周期"},
    {"id": 8,  "trend": "bear", "start_ym": "199705", "start_point": 1510, "end_point": 1048, "pct_chg": -31.0,  "duration_months": 24, "label": "亚洲金融危机"},
    {"id": 9,  "trend": "bull", "start_ym": "199905", "start_point": 1048, "end_point": 2245, "pct_chg": 114.0,  "duration_months": 25, "label": "519行情"},
    {"id": 10, "trend": "bear", "start_ym": "200106", "start_point": 2245, "end_point": 998,  "pct_chg": -56.0,  "duration_months": 48, "label": "国有股减持"},
    {"id": 11, "trend": "bull", "start_ym": "200506", "start_point": 998,  "end_point": 6124, "pct_chg": 514.0,  "duration_months": 28, "label": "股权分置改革"},
    {"id": 12, "trend": "bear", "start_ym": "200710", "start_point": 6124, "end_point": 1665, "pct_chg": -73.0,  "duration_months": 12, "label": "全球金融危机"},
    {"id": 13, "trend": "bull", "start_ym": "200810", "start_point": 1665, "end_point": 3478, "pct_chg": 109.0,  "duration_months": 10, "label": "四万亿刺激"},
    {"id": 14, "trend": "bear", "start_ym": "200908", "start_point": 3478, "end_point": 1850, "pct_chg": -47.0,  "duration_months": 46, "label": "刺激退出"},
    {"id": 15, "trend": "bull", "start_ym": "201306", "start_point": 1850, "end_point": 5178, "pct_chg": 180.0,  "duration_months": 24, "label": "杠杆牛"},
    {"id": 16, "trend": "bear", "start_ym": "201506", "start_point": 5178, "end_point": 2638, "pct_chg": -49.0,  "duration_months": 7,  "label": "去杠杆/熔断"},
    {"id": 17, "trend": "bull", "start_ym": "201601", "start_point": 2638, "end_point": 3587, "pct_chg": 36.0,   "duration_months": 24, "label": "供给侧改革"},
    {"id": 18, "trend": "bear", "start_ym": "201801", "start_point": 3587, "end_point": 2441, "pct_chg": -32.0,  "duration_months": 12, "label": "贸易战"},
    {"id": 19, "trend": "bull", "start_ym": "201901", "start_point": 2441, "end_point": 3732, "pct_chg": 53.0,   "duration_months": 25, "label": "核心资产"},
    {"id": 20, "trend": "bear", "start_ym": "202102", "start_point": 3732, "end_point": 2635, "pct_chg": -29.0,  "duration_months": 36, "label": "地产暴雷"},
    {"id": 21, "trend": "bull", "start_ym": "202402", "start_point": 2635, "end_point": 4163, "pct_chg": 58.0,   "duration_months": 24, "label": "924政策底"},
]

# 已确认的顶部（牛转熊的转折点）
SH_TOPS = [
    {"ym": "199205", "point": 1429, "label": "沪市开市顶"},
    {"ym": "199302", "point": 1559, "label": "南巡讲话顶"},
    {"ym": "199409", "point": 1053, "label": "三大救市顶"},
    {"ym": "199705", "point": 1510, "label": "降息周期顶"},
    {"ym": "200106", "point": 2245, "label": "519行情顶"},
    {"ym": "200710", "point": 6124, "label": "6124历史顶"},
    {"ym": "200908", "point": 3478, "label": "四万亿顶"},
    {"ym": "201506", "point": 5178, "label": "杠杆牛顶"},
    {"ym": "201801", "point": 3587, "label": "供给侧顶"},
    {"ym": "202102", "point": 3732, "label": "核心资产顶"},
]

# 已确认的底部（熊转牛的转折点）
SH_BOTTOMS = [
    {"ym": "199211", "point": 387,  "label": "新股扩容底"},
    {"ym": "199407", "point": 326,  "label": "紧缩调控底"},
    {"ym": "199601", "point": 513,  "label": "政策消退底"},
    {"ym": "199905", "point": 1048, "label": "金融危机底"},
    {"ym": "200506", "point": 998,  "label": "998历史底"},
    {"ym": "200810", "point": 1665, "label": "金融海啸底"},
    {"ym": "201306", "point": 1850, "label": "刺激退出底"},
    {"ym": "201601", "point": 2638, "label": "熔断底"},
    {"ym": "201901", "point": 2441, "label": "贸易战底"},
    {"ym": "202402", "point": 2635, "label": "地产暴雷底"},
]


def get_phase(trade_date: str) -> dict | None:
    """给定 YYYYMMDD 格式日期，返回所属牛熊阶段

    查找逻辑: 从后往前找第一个 start_ym <= 日期月份 的阶段
    边界处理: 转折月份归属新阶段（如199205归属熊市，因为顶部确立后开始下跌）
    """
    ym = trade_date[:6]
    for i in range(len(SH_PHASES) - 1, -1, -1):
        if ym >= SH_PHASES[i]["start_ym"]:
            return SH_PHASES[i]
    return None


def tag_trend(trade_date: str) -> str:
    """返回日期对应的趋势标签: 'bull' / 'bear' / 'unknown'"""
    phase = get_phase(trade_date)
    return phase["trend"] if phase else "unknown"


def find_nearest_date(df: pd.DataFrame, target_ym: str) -> str | None:
    """在 DataFrame 中找到目标月份对应的交易日

    target_ym: YYYYMM 格式
    df: 必须含 trade_date 列（YYYYMMDD 格式），按日期升序

    对于月线/周线: 返回该月的数据行日期
    对于日线: 返回该月最后一个交易日
    如该月无数据，返回该月之前最近的交易日
    """
    mask = df["trade_date"].str[:6] == target_ym
    if mask.any():
        return df.loc[mask, "trade_date"].iloc[-1]

    # 该月无数据，找之前最近的
    mask_before = df["trade_date"].str[:6] < target_ym
    if mask_before.any():
        return df.loc[mask_before, "trade_date"].iloc[-1]

    return None
