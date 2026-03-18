"""
市场统计数据计算脚本
从数据库计算估值面/技术面/资金面/情绪面四个维度的市场指标
输出格式化文本，可直接插入大模型选股提示词
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from app.database import engine


def get_latest_trade_date():
    """获取最近的交易日"""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT MAX(trade_date) FROM market_daily"
        )).fetchone()
        return row[0] if row else None


def get_trade_date_n_days_ago(trade_date: str, n: int) -> str:
    """获取N个交易日前的日期"""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT trade_date FROM market_daily "
            "WHERE trade_date <= :td "
            "GROUP BY trade_date ORDER BY trade_date DESC LIMIT 1 OFFSET :n"
        ), {"td": trade_date, "n": n}).fetchone()
        return row[0] if row else trade_date


def calc_valuation(conn, trade_date: str) -> dict:
    """估值面指标"""
    # 指数估值: 沪深300(399300.SZ) 和 中证500(000905.SH)
    index_vals = {}
    for code, name in [("399300.SZ", "沪深300"), ("000905.SH", "中证500")]:
        row = conn.execute(text(
            "SELECT pe, pe_ttm, pb FROM index_dailybasic "
            "WHERE ts_code = :code AND trade_date = :td"
        ), {"code": code, "td": trade_date}).fetchone()
        if row:
            index_vals[name] = {"pe": row[0], "pe_ttm": row[1], "pb": row[2]}

    # 全市场估值分布(排除ST、停牌、异常值)
    market_row = conn.execute(text("""
        SELECT
            ROUND(AVG(pe_ttm), 2) as avg_pe,
            ROUND(AVG(pb), 2) as avg_pb,
            ROUND(AVG(dv_ttm), 2) as avg_dv,
            SUM(CASE WHEN pb < 1 AND pb > 0 THEN 1 ELSE 0 END) as broken_net_count,
            COUNT(*) as total_count
        FROM daily_basic
        WHERE trade_date = :td AND pe_ttm > 0 AND pe_ttm < 500
    """), {"td": trade_date}).fetchone()

    # PE中位数 (用变量方式绕过MySQL LIMIT不支持子查询的限制)
    pe_count_row = conn.execute(text(
        "SELECT COUNT(*) FROM daily_basic WHERE trade_date = :td AND pe_ttm > 0 AND pe_ttm < 500"
    ), {"td": trade_date}).fetchone()
    pe_offset = (pe_count_row[0] // 2) if pe_count_row else 0
    pe_median_row = conn.execute(text(
        f"SELECT ROUND(pe_ttm, 2) FROM daily_basic "
        f"WHERE trade_date = :td AND pe_ttm > 0 AND pe_ttm < 500 "
        f"ORDER BY pe_ttm LIMIT 1 OFFSET {pe_offset}"
    ), {"td": trade_date}).fetchone()

    # PB中位数
    pb_count_row = conn.execute(text(
        "SELECT COUNT(*) FROM daily_basic WHERE trade_date = :td AND pb > 0 AND pb < 50"
    ), {"td": trade_date}).fetchone()
    pb_offset = (pb_count_row[0] // 2) if pb_count_row else 0
    pb_median_row = conn.execute(text(
        f"SELECT ROUND(pb, 2) FROM daily_basic "
        f"WHERE trade_date = :td AND pb > 0 AND pb < 50 "
        f"ORDER BY pb LIMIT 1 OFFSET {pb_offset}"
    ), {"td": trade_date}).fetchone()

    return {
        "index_vals": index_vals,
        "avg_pe": market_row[0] if market_row else None,
        "median_pe": pe_median_row[0] if pe_median_row else None,
        "avg_pb": market_row[1] if market_row else None,
        "median_pb": pb_median_row[0] if pb_median_row else None,
        "avg_dv": market_row[2] if market_row else None,
        "broken_net_count": int(market_row[3]) if market_row else 0,
        "total_count": int(market_row[4]) if market_row else 0,
    }


def calc_technical(conn, trade_date: str) -> dict:
    """技术面指标"""
    # 指数涨跌幅
    index_chg = {}
    for code, name in [("000001.SH", "上证指数"), ("399001.SZ", "深证成指"), ("399006.SZ", "创业板指")]:
        row = conn.execute(text(
            "SELECT close, pct_chg FROM index_daily "
            "WHERE ts_code = :code AND trade_date = :td"
        ), {"code": code, "td": trade_date}).fetchone()
        if row:
            # 5日和20日涨跌幅
            chg_5d = conn.execute(text("""
                SELECT ROUND((MAX(c) - MIN(c_start)) / MIN(c_start) * 100, 2)
                FROM (
                    SELECT close as c,
                           FIRST_VALUE(close) OVER (ORDER BY trade_date) as c_start
                    FROM index_daily
                    WHERE ts_code = :code AND trade_date <= :td
                    ORDER BY trade_date DESC LIMIT 5
                ) t
            """), {"code": code, "td": trade_date}).fetchone()

            chg_20d = conn.execute(text("""
                SELECT ROUND((last_close - first_close) / first_close * 100, 2)
                FROM (
                    SELECT
                        FIRST_VALUE(close) OVER (ORDER BY trade_date ASC) as first_close,
                        FIRST_VALUE(close) OVER (ORDER BY trade_date DESC) as last_close
                    FROM index_daily
                    WHERE ts_code = :code AND trade_date <= :td
                    ORDER BY trade_date DESC LIMIT 20
                ) t LIMIT 1
            """), {"code": code, "td": trade_date}).fetchone()

            index_chg[name] = {
                "close": row[0],
                "pct_chg_1d": row[1],
                "pct_chg_5d": chg_5d[0] if chg_5d else None,
                "pct_chg_20d": chg_20d[0] if chg_20d else None,
            }

    # 上涨/下跌/涨停/跌停家数
    breadth_row = conn.execute(text("""
        SELECT
            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as up_count,
            SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) as down_count,
            SUM(CASE WHEN pct_chg = 0 THEN 1 ELSE 0 END) as flat_count,
            SUM(CASE WHEN pct_chg >= 9.9 THEN 1 ELSE 0 END) as limit_up,
            SUM(CASE WHEN pct_chg <= -9.9 THEN 1 ELSE 0 END) as limit_down,
            COUNT(*) as total
        FROM market_daily WHERE trade_date = :td
    """), {"td": trade_date}).fetchone()

    # 全市场平均换手率
    turnover_row = conn.execute(text(
        "SELECT ROUND(AVG(turnover_rate), 2) FROM daily_basic WHERE trade_date = :td"
    ), {"td": trade_date}).fetchone()

    return {
        "index_chg": index_chg,
        "up_count": int(breadth_row[0] or 0),
        "down_count": int(breadth_row[1] or 0),
        "flat_count": int(breadth_row[2] or 0),
        "limit_up": int(breadth_row[3] or 0),
        "limit_down": int(breadth_row[4] or 0),
        "total": int(breadth_row[5] or 0),
        "avg_turnover": turnover_row[0] if turnover_row else None,
    }


def calc_capital(conn, trade_date: str) -> dict:
    """资金面指标"""
    # 市场资金流(moneyflow_mkt_dc)
    mkt_row = conn.execute(text(
        "SELECT net_amount, net_amount_rate, buy_elg_amount, buy_elg_amount_rate, "
        "buy_lg_amount, buy_lg_amount_rate, buy_sm_amount, buy_sm_amount_rate "
        "FROM moneyflow_mkt_dc WHERE trade_date = :td"
    ), {"td": trade_date}).fetchone()

    # 近5日主力累计净流入
    mf_5d_row = conn.execute(text(
        "SELECT ROUND(SUM(net_amount), 2) FROM ("
        "  SELECT net_amount FROM moneyflow_mkt_dc "
        "  WHERE trade_date <= :td ORDER BY trade_date DESC LIMIT 5"
        ") t"
    ), {"td": trade_date}).fetchone()

    # 全市场成交额(amount单位千元, 转亿)
    today_amount_row = conn.execute(text(
        "SELECT ROUND(SUM(amount) / 100000, 2) FROM market_daily WHERE trade_date = :td"
    ), {"td": trade_date}).fetchone()

    avg_5d_amount_row = conn.execute(text(
        "SELECT ROUND(AVG(day_amount), 2) FROM ("
        "  SELECT SUM(amount)/100000 as day_amount FROM market_daily "
        "  WHERE trade_date <= :td GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5"
        ") t"
    ), {"td": trade_date}).fetchone()

    amount_row = (
        today_amount_row[0] if today_amount_row else None,
        avg_5d_amount_row[0] if avg_5d_amount_row else None,
    )

    today_amount = amount_row[0]
    avg_5d_amount = amount_row[1]

    return {
        "net_amount": round(mkt_row[0] / 100000000, 2) if mkt_row and mkt_row[0] else None,  # 转亿
        "net_amount_rate": mkt_row[1] if mkt_row else None,
        "elg_amount": round(mkt_row[2] / 100000000, 2) if mkt_row and mkt_row[2] else None,
        "elg_rate": mkt_row[3] if mkt_row else None,
        "lg_amount": round(mkt_row[4] / 100000000, 2) if mkt_row and mkt_row[4] else None,
        "lg_rate": mkt_row[5] if mkt_row else None,
        "sm_amount": round(mkt_row[6] / 100000000, 2) if mkt_row and mkt_row[6] else None,
        "sm_rate": mkt_row[7] if mkt_row else None,
        "net_5d": round(mf_5d_row[0] / 100000000, 2) if mf_5d_row and mf_5d_row[0] else None,
        "today_amount": today_amount,
        "avg_5d_amount": avg_5d_amount,
        "amount_ratio": round(today_amount / avg_5d_amount, 2) if today_amount and avg_5d_amount else None,
    }


def calc_sentiment(conn, trade_date: str) -> dict:
    """情绪面指标"""
    # 全市场平均涨跌幅
    avg_chg_row = conn.execute(text(
        "SELECT ROUND(AVG(pct_chg), 2) FROM market_daily WHERE trade_date = :td"
    ), {"td": trade_date}).fetchone()

    # 量能变化: 当日总成交量 / 20日均量
    vol_ratio_row = conn.execute(text("""
        SELECT ROUND(today_vol / avg_20d_vol, 2) FROM (
            SELECT
                (SELECT SUM(vol) FROM market_daily WHERE trade_date = :td) as today_vol,
                (SELECT AVG(day_vol) FROM (
                    SELECT SUM(vol) as day_vol FROM market_daily
                    WHERE trade_date <= :td
                    GROUP BY trade_date ORDER BY trade_date DESC LIMIT 20
                ) t) as avg_20d_vol
        ) v
    """), {"td": trade_date}).fetchone()

    # 行业板块涨幅排名(近5日, 用申万一级行业分类)
    industry_row = conn.execute(text("""
        SELECT b.industry,
               ROUND(AVG(m.pct_chg), 2) as avg_chg,
               COUNT(*) as cnt
        FROM market_daily m
        JOIN stock_basic b ON m.ts_code = b.ts_code
        WHERE m.trade_date = :td AND b.industry IS NOT NULL
        GROUP BY b.industry
        ORDER BY avg_chg DESC
        LIMIT 10
    """), {"td": trade_date}).fetchall()

    top_industries = [{"name": r[0], "avg_chg": r[1], "count": r[2]} for r in industry_row] if industry_row else []

    # 行业垫底
    bottom_industry_row = conn.execute(text("""
        SELECT b.industry,
               ROUND(AVG(m.pct_chg), 2) as avg_chg,
               COUNT(*) as cnt
        FROM market_daily m
        JOIN stock_basic b ON m.ts_code = b.ts_code
        WHERE m.trade_date = :td AND b.industry IS NOT NULL
        GROUP BY b.industry
        ORDER BY avg_chg ASC
        LIMIT 5
    """), {"td": trade_date}).fetchall()

    bottom_industries = [{"name": r[0], "avg_chg": r[1], "count": r[2]} for r in bottom_industry_row] if bottom_industry_row else []

    return {
        "avg_pct_chg": avg_chg_row[0] if avg_chg_row else None,
        "vol_ratio_20d": vol_ratio_row[0] if vol_ratio_row else None,
        "top_industries": top_industries,
        "bottom_industries": bottom_industries,
    }


def format_market_stats(trade_date: str = None) -> str:
    """计算并格式化市场统计数据，返回可插入提示词的文本"""
    if not trade_date:
        trade_date = get_latest_trade_date()
    if not trade_date:
        return "无法获取交易日数据"

    with engine.connect() as conn:
        val = calc_valuation(conn, trade_date)
        tech = calc_technical(conn, trade_date)
        cap = calc_capital(conn, trade_date)
        sent = calc_sentiment(conn, trade_date)

    lines = [f"## 市场统计数据（{trade_date}）\n"]

    # 估值面
    lines.append("### 一、估值面")
    for name, v in val.get("index_vals", {}).items():
        lines.append(f"- {name}: PE={v['pe']}, PE_TTM={v['pe_ttm']}, PB={v['pb']}")
    lines.append(f"- 全市场PE中位数: {val.get('median_pe')}")
    lines.append(f"- 全市场PB中位数: {val.get('median_pb')}")
    lines.append(f"- 全市场平均股息率: {val.get('avg_dv')}%")
    lines.append(f"- 破净股数量: {val.get('broken_net_count')}只 (共{val.get('total_count')}只)")
    lines.append("")

    # 技术面
    lines.append("### 二、技术面")
    for name, v in tech.get("index_chg", {}).items():
        lines.append(f"- {name}: 收盘{v['close']}, 今日{v['pct_chg_1d']}%, 5日{v['pct_chg_5d']}%, 20日{v['pct_chg_20d']}%")
    lines.append(f"- 上涨: {tech['up_count']}家, 下跌: {tech['down_count']}家, 平盘: {tech['flat_count']}家")
    lines.append(f"- 涨停: {tech['limit_up']}家, 跌停: {tech['limit_down']}家")
    lines.append(f"- 全市场平均换手率: {tech.get('avg_turnover')}%")
    lines.append("")

    # 资金面
    lines.append("### 三、资金面")
    lines.append(f"- 主力净流入: {cap.get('net_amount')}亿 ({cap.get('net_amount_rate')}%)")
    lines.append(f"- 超大单净流入: {cap.get('elg_amount')}亿 ({cap.get('elg_rate')}%)")
    lines.append(f"- 大单净流入: {cap.get('lg_amount')}亿 ({cap.get('lg_rate')}%)")
    lines.append(f"- 散户(小单)净流入: {cap.get('sm_amount')}亿 ({cap.get('sm_rate')}%)")
    lines.append(f"- 近5日主力累计净流入: {cap.get('net_5d')}亿")
    lines.append(f"- 全市场成交额: {cap.get('today_amount')}亿, 5日均值: {cap.get('avg_5d_amount')}亿, 量比: {cap.get('amount_ratio')}")
    lines.append("")

    # 情绪面
    lines.append("### 四、情绪面")
    up = tech['up_count'] or 1
    down = tech['down_count'] or 1
    lu = tech['limit_up'] or 0
    ld = tech['limit_down'] or 1
    lines.append(f"- 涨跌比: {round(up/down, 2)}")
    lines.append(f"- 涨停跌停比: {round(lu/ld, 2) if ld > 0 else lu}")
    lines.append(f"- 全市场平均涨跌幅: {sent.get('avg_pct_chg')}%")
    lines.append(f"- 量能变化(今日/20日均): {sent.get('vol_ratio_20d')}")
    lines.append("")

    # 行业热度
    lines.append("### 五、行业板块")
    lines.append("**涨幅前10行业:**")
    for ind in sent.get("top_industries", []):
        lines.append(f"- {ind['name']}: 平均涨幅{ind['avg_chg']}% ({ind['count']}只)")
    lines.append("**跌幅前5行业:**")
    for ind in sent.get("bottom_industries", []):
        lines.append(f"- {ind['name']}: 平均跌幅{ind['avg_chg']}% ({ind['count']}只)")

    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="计算市场统计数据")
    parser.add_argument("--date", help="交易日期 YYYYMMDD，默认最新")
    args = parser.parse_args()
    print(format_market_stats(args.date))
