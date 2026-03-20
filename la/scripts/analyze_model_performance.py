"""
LA 选股模型综合分析脚本

分析维度：
1. 市场行情概览（近期涨跌情况）
2. 各模型整体预测能力（胜率、平均收益）
3. 做多 vs 做空准确性与收益
4. 各策略方法论表现
5. 时间维度有效性（T+1 / T+2 / T+5）
6. 数据质量说明

用法:
    python la/scripts/analyze_model_performance.py
    python la/scripts/analyze_model_performance.py --output report.md
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import numpy as np
from sqlalchemy import text
from app.database import engine

# 有效模型（排除数据质量有问题的 doubao-online）
VALID_MODELS = ['claude', 'doubao', 'tare', 'trae']

# 策略方法论中文映射
METHOD_CN = {
    'capital_flow': '资金流向',
    'comprehensive': '综合选股',
    'dividend_defense': '股息防御',
    'growth': '成长动量',
    'hotspot': '热点题材',
    'macd': 'MACD技术',
    'trend': '趋势跟踪',
    'turnaround': '困境反转',
    'value': '价值低估',
}


def load_market_data(conn) -> pd.DataFrame:
    """加载近期大盘指数数据"""
    sql = """
        SELECT ts_code, trade_date, close, pct_chg
        FROM index_daily
        WHERE ts_code IN ('000001.SH', '399001.SZ', '399006.SZ')
        AND trade_date >= '20260301'
        ORDER BY ts_code, trade_date
    """
    return pd.read_sql(sql, conn)


def load_pick_data(conn) -> pd.DataFrame:
    """加载有效模型的选股数据"""
    placeholders = ','.join(f"'{m}'" for m in VALID_MODELS)
    sql = f"""
        SELECT
            model_name, methodology, direction, eval_date, ts_code, score,
            buy_price, target_price,
            return_t1, return_t2, return_t5, return_t10, return_t20
        FROM la_pick
        WHERE model_name IN ({placeholders})
    """
    df = pd.read_sql(sql, conn)

    # 计算方向调整后收益率：做多=原始收益，做空=-原始收益（负值为盈）
    for t in [1, 2, 5, 10, 20]:
        col = f'return_t{t}'
        if col in df.columns:
            df[f'adj_t{t}'] = df.apply(
                lambda row: row[col] if row['direction'] == 'long'
                else (-row[col] if pd.notna(row[col]) else np.nan),
                axis=1
            )

    return df


def fmt_return(val):
    """格式化收益率显示"""
    if pd.isna(val):
        return 'N/A'
    sign = '+' if val >= 0 else ''
    return f'{sign}{val:.2f}%'


def fmt_winrate(val):
    """格式化胜率显示"""
    if pd.isna(val):
        return 'N/A'
    return f'{val:.1f}%'


def analyze_market(df_market: pd.DataFrame) -> str:
    """分析市场行情"""
    lines = ['## 一、市场行情概览\n']

    index_names = {'000001.SH': '上证指数', '399001.SZ': '深证成指', '399006.SZ': '创业板指'}

    for code, name in index_names.items():
        idx_df = df_market[df_market['ts_code'] == code].sort_values('trade_date')
        if idx_df.empty:
            continue

        start_row = idx_df.iloc[0]
        end_row = idx_df.iloc[-1]
        period_ret = (end_row['close'] - start_row['close']) / start_row['close'] * 100

        # 计算最近5日、10日涨跌
        recent5 = idx_df.tail(5)
        ret5 = recent5['pct_chg'].sum()

        lines.append(f'### {name}（{code}）')
        lines.append(f'- 区间: {start_row["trade_date"]} → {end_row["trade_date"]}')
        lines.append(f'- 最新收盘: {end_row["close"]:.2f}')
        lines.append(f'- 区间涨跌: {fmt_return(period_ret)}')
        lines.append(f'- 最近5日涨跌: {fmt_return(ret5)}')

        # 显示每日涨跌
        dates_str = ' | '.join(
            f'{r["trade_date"][-4:]} {fmt_return(r["pct_chg"])}'
            for _, r in idx_df.iterrows()
        )
        lines.append(f'- 日涨跌: {dates_str}')
        lines.append('')

    # 市场趋势判断
    sh = df_market[df_market['ts_code'] == '000001.SH'].sort_values('trade_date')
    if not sh.empty:
        recent = sh.tail(5)
        up_days = (recent['pct_chg'] > 0).sum()
        total_ret = (sh.iloc[-1]['close'] - sh.iloc[0]['close']) / sh.iloc[0]['close'] * 100

        if total_ret < -3:
            trend = '**下跌趋势**（区间跌幅超过3%，空头市场）'
        elif total_ret > 3:
            trend = '**上涨趋势**（区间涨幅超过3%，多头市场）'
        else:
            trend = '**震荡整理**（区间涨跌幅在3%以内）'

        lines.append(f'**市场趋势判断**: {trend}')
        lines.append(f'- 选股数据区间内上证指数从 {sh.iloc[0]["close"]:.2f} 跌至 {sh.iloc[-1]["close"]:.2f}，累计跌幅 {fmt_return(total_ret)}')
        lines.append(f'- 最近5个交易日：{up_days}涨{5-up_days}跌')
        lines.append('')

    return '\n'.join(lines)


def analyze_model_overall(df: pd.DataFrame) -> str:
    """分析各模型整体表现"""
    lines = ['## 二、各模型整体表现\n']

    # 按模型汇总
    rows = []
    for model in VALID_MODELS:
        m_df = df[df['model_name'] == model]
        if m_df.empty:
            continue

        row = {'模型': model, '总选股数': len(m_df)}

        for t, label in [(1, 'T+1'), (2, 'T+2'), (5, 'T+5')]:
            col = f'adj_t{t}'
            valid = m_df[col].dropna()
            if len(valid) == 0:
                row[f'{label}样本数'] = 0
                row[f'{label}平均收益'] = 'N/A'
                row[f'{label}胜率'] = 'N/A'
            else:
                row[f'{label}样本数'] = len(valid)
                row[f'{label}平均收益'] = fmt_return(valid.mean())
                row[f'{label}胜率'] = fmt_winrate((valid > 0).mean() * 100)

        rows.append(row)

    result_df = pd.DataFrame(rows)
    lines.append(result_df.to_markdown(index=False))
    lines.append('')
    lines.append('> **胜率定义**: 方向调整后收益率 > 0 的比例（做多股票上涨 / 做空股票下跌均计为"胜"）')
    lines.append('')

    return '\n'.join(lines)


def analyze_direction(df: pd.DataFrame) -> str:
    """分析做多/做空的准确性与收益"""
    lines = ['## 三、做多 vs 做空分析\n']

    rows = []
    for model in VALID_MODELS:
        m_df = df[df['model_name'] == model]
        if m_df.empty:
            continue

        for direction, dir_label in [('long', '做多'), ('short', '做空')]:
            d_df = m_df[m_df['direction'] == direction]
            if d_df.empty:
                continue

            row = {'模型': model, '方向': dir_label, '总数': len(d_df)}

            for t, label in [(1, 'T+1'), (2, 'T+2'), (5, 'T+5')]:
                col = f'adj_t{t}'
                valid = d_df[col].dropna()
                if len(valid) == 0:
                    row[f'{label}均收益'] = 'N/A'
                    row[f'{label}胜率'] = 'N/A'
                else:
                    row[f'{label}均收益'] = fmt_return(valid.mean())
                    row[f'{label}胜率'] = fmt_winrate((valid > 0).mean() * 100)
                    row[f'{label}样本'] = len(valid)

            rows.append(row)

    result_df = pd.DataFrame(rows)
    lines.append(result_df.to_markdown(index=False))
    lines.append('')

    # 上涨/下跌幅度分析
    lines.append('### 上涨/下跌幅度分析（T+1，有效样本）\n')
    lines.append('| 模型 | 方向 | 正确预测均涨幅 | 错误预测均亏损 | 最大单笔收益 | 最大单笔亏损 |')
    lines.append('|------|------|---------------|---------------|-------------|-------------|')

    for model in VALID_MODELS:
        m_df = df[df['model_name'] == model]

        for direction, dir_label in [('long', '做多'), ('short', '做空')]:
            d_df = m_df[m_df['direction'] == direction]
            valid = d_df['adj_t1'].dropna()
            if len(valid) == 0:
                continue

            winners = valid[valid > 0]
            losers = valid[valid <= 0]

            avg_win = fmt_return(winners.mean()) if len(winners) > 0 else 'N/A'
            avg_loss = fmt_return(losers.mean()) if len(losers) > 0 else 'N/A'
            max_win = fmt_return(valid.max())
            max_loss = fmt_return(valid.min())

            lines.append(f'| {model} | {dir_label} | {avg_win} | {avg_loss} | {max_win} | {max_loss} |')

    lines.append('')
    return '\n'.join(lines)


def analyze_methodology(df: pd.DataFrame) -> str:
    """分析各策略方法论表现"""
    lines = ['## 四、各策略方法论表现（T+1 调整后收益）\n']

    rows = []
    methods = df['methodology'].dropna().unique()

    for method in sorted(methods):
        method_label = METHOD_CN.get(method, method)
        m_df = df[df['methodology'] == method]

        valid_t1 = m_df['adj_t1'].dropna()
        valid_t2 = m_df['adj_t2'].dropna() if 'adj_t2' in m_df.columns else pd.Series()

        if len(valid_t1) == 0:
            continue

        row = {
            '策略': f'{method_label}({method})',
            '样本数': len(valid_t1),
            'T+1均收益': fmt_return(valid_t1.mean()),
            'T+1胜率': fmt_winrate((valid_t1 > 0).mean() * 100),
            'T+2均收益': fmt_return(valid_t2.mean()) if len(valid_t2) > 0 else 'N/A',
        }
        rows.append(row)

    result_df = pd.DataFrame(rows).sort_values('T+1均收益', ascending=False)
    lines.append(result_df.to_markdown(index=False))
    lines.append('')

    return '\n'.join(lines)


def analyze_time_horizon(df: pd.DataFrame) -> str:
    """分析预测时间有效性"""
    lines = ['## 五、预测有效时间范围分析\n']

    lines.append('### 各时间窗口胜率与均收益（全模型合并）\n')
    lines.append('| 时间窗口 | 样本数 | 平均调整收益 | 胜率 | 做多胜率 | 做空胜率 |')
    lines.append('|----------|--------|-------------|------|---------|---------|')

    for t in [1, 2, 5, 10, 20]:
        col = f'adj_t{t}'
        if col not in df.columns:
            continue

        valid = df[col].dropna()
        if len(valid) == 0:
            continue

        long_valid = df[df['direction'] == 'long'][col].dropna()
        short_valid = df[df['direction'] == 'short'][col].dropna()

        avg = fmt_return(valid.mean())
        wr = fmt_winrate((valid > 0).mean() * 100)
        long_wr = fmt_winrate((long_valid > 0).mean() * 100) if len(long_valid) > 0 else 'N/A'
        short_wr = fmt_winrate((short_valid > 0).mean() * 100) if len(short_valid) > 0 else 'N/A'

        lines.append(f'| T+{t} | {len(valid)} | {avg} | {wr} | {long_wr} | {short_wr} |')

    lines.append('')

    # 按模型的时间维度衰减
    lines.append('### 各模型收益随时间衰减（平均调整收益）\n')
    lines.append('| 模型 | T+1 | T+2 | T+5 |')
    lines.append('|------|-----|-----|-----|')

    for model in VALID_MODELS:
        m_df = df[df['model_name'] == model]
        row_vals = [model]
        for t in [1, 2, 5]:
            col = f'adj_t{t}'
            valid = m_df[col].dropna() if col in m_df.columns else pd.Series()
            row_vals.append(fmt_return(valid.mean()) if len(valid) > 0 else 'N/A')
        lines.append(f'| {" | ".join(row_vals)} |')

    lines.append('')
    return '\n'.join(lines)


def analyze_per_date(df: pd.DataFrame, df_market: pd.DataFrame) -> str:
    """按日期分析选股表现与市场的关系"""
    lines = ['## 六、逐日表现 vs 市场行情\n']

    sh = df_market[df_market['ts_code'] == '000001.SH'].set_index('trade_date')

    lines.append('| 选股日期 | 市场(上证) | 样本数 | T+1均调整收益 | T+1胜率 |')
    lines.append('|----------|-----------|--------|-------------|--------|')

    for date in sorted(df['eval_date'].unique()):
        day_df = df[df['eval_date'] == date]
        valid = day_df['adj_t1'].dropna()

        # 找该日期对应的市场涨跌（用下一交易日的涨跌来对比）
        # 实际上选股日的T+1就是下一日，所以用T+1行情
        mkt_str = 'N/A'

        avg_ret = fmt_return(valid.mean()) if len(valid) > 0 else 'N/A'
        win_rate = fmt_winrate((valid > 0).mean() * 100) if len(valid) > 0 else 'N/A'

        lines.append(f'| {date} | {mkt_str} | {len(valid)} | {avg_ret} | {win_rate} |')

    lines.append('')
    return '\n'.join(lines)


def data_quality_note(df_all: pd.DataFrame) -> str:
    """数据质量说明"""
    lines = ['## 七、数据质量说明\n']

    lines.append('### 数据覆盖情况\n')
    lines.append('| 模型 | 数据范围 | 状态 |')
    lines.append('|------|---------|------|')

    for model in ['claude', 'doubao', 'tare', 'trae', 'doubao-online']:
        m_df = df_all[df_all['model_name'] == model] if model in df_all['model_name'].values else pd.DataFrame()
        if m_df.empty:
            continue

        min_d = m_df['eval_date'].min()
        max_d = m_df['eval_date'].max()
        n = len(m_df)

        if model == 'doubao-online':
            status = '⚠️ **数据异常**（buy_price与市场实际价格严重偏差，排除分析）'
        else:
            status = '✅ 正常'

        lines.append(f'| {model} | {min_d} ~ {max_d}（{n}条）| {status} |')

    lines.append('')
    lines.append('### doubao-online 数据异常说明')
    lines.append('- doubao-online 的 buy_price 与选股日实际市场价格存在严重偏差（偏差率50%~3000%+）')
    lines.append('- 例：601899.SH 的 buy_price=10.25，而实际收盘价=32.32（偏差215%）')
    lines.append('- 因此该模型的 return_t1 计算结果（均值25-37%）为无效数据')
    lines.append('- 已从所有模型对比分析中排除，建议重新生成该模型的选股数据\n')

    lines.append('### 数据局限性')
    lines.append('- 选股数据仅覆盖 9 个交易日（20260312 ~ 20260320），样本量偏少')
    lines.append('- T+5 数据仅 2026-03-12 日期有效，其余日期尚无法评估')
    lines.append('- T+10 及更长周期数据暂不可用')
    lines.append('- 评估期间市场整体下跌（上证跌约5%），可能导致做多胜率系统性偏低\n')

    return '\n'.join(lines)


def generate_report(output_path: str | None = None) -> str:
    """生成完整分析报告"""
    print("[分析] 连接数据库...")

    with engine.connect() as conn:
        df_market = load_market_data(conn)
        df = load_pick_data(conn)

        # 加载所有数据（含 doubao-online）用于数据质量说明
        df_all = pd.read_sql(
            "SELECT model_name, eval_date FROM la_pick", conn
        )

    print(f"[分析] 加载选股数据: {len(df)} 条（有效模型: {VALID_MODELS}）")
    print(f"[分析] 加载市场数据: {len(df_market)} 条")

    # 生成各分析章节
    today = datetime.now().strftime('%Y%m%d')

    sections = [
        f'# LA 选股模型预测能力分析报告',
        f'**创建时间**: {today}  ',
        f'**分析范围**: 选股日期 {df["eval_date"].min()} ~ {df["eval_date"].max()}  ',
        f'**有效模型**: {", ".join(VALID_MODELS)}  ',
        f'**有效选股总数**: {len(df)} 条  ',
        '',
        analyze_market(df_market),
        analyze_model_overall(df),
        analyze_direction(df),
        analyze_methodology(df),
        analyze_time_horizon(df),
        analyze_per_date(df, df_market),
        data_quality_note(df_all),
        '---',
        '*本报告基于 my_stock.la_pick 表数据生成，数据来源为各 LLM 模型实时选股记录*',
    ]

    report = '\n'.join(sections)

    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)
        print(f"[完成] 报告已保存: {output_path}")
    else:
        print(report)

    return report


def main():
    parser = argparse.ArgumentParser(description='LA 选股模型综合分析')
    parser.add_argument('--output', '-o', help='输出 Markdown 文件路径')
    args = parser.parse_args()

    generate_report(args.output)


if __name__ == '__main__':
    main()
