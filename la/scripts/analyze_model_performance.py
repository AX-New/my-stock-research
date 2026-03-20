"""
LA 选股模型预测能力深度分析脚本

核心原则：做多和做空是两个完全不同的预测任务，所有分析维度都按方向分开。

分析维度：
1. 市场行情概览
2. 做多预测能力（各模型选涨股的准确性）
3. 做空预测能力（各模型选跌股的准确性）
4. 策略方法论（按方向分开的策略表现）
5. 评分与收益相关性（高分选股是否更准）
6. 时间衰减分析（预测在哪个时间窗口最有效）
7. 逐日表现与市场关系
8. 数据质量说明

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
    """加载有效模型的选股数据，包含评分和评级"""
    placeholders = ','.join(f"'{m}'" for m in VALID_MODELS)
    sql = f"""
        SELECT
            model_name, methodology, direction, eval_date, ts_code,
            stock_name, score, rating, buy_price, target_price,
            return_t1, return_t2, return_t3, return_t5, return_t10, return_t20
        FROM la_pick
        WHERE model_name IN ({placeholders})
    """
    return pd.read_sql(sql, conn)


def fmt_ret(val):
    """格式化收益率"""
    if pd.isna(val):
        return 'N/A'
    return f'{"+{:.2f}" if val >= 0 else "{:.2f}"}'.format(val) + '%'


def fmt_wr(val):
    """格式化胜率"""
    if pd.isna(val):
        return 'N/A'
    return f'{val:.1f}%'


def calc_stats(series):
    """计算一组收益率的核心统计指标"""
    valid = series.dropna()
    if len(valid) == 0:
        return {'n': 0, 'avg': np.nan, 'wr': np.nan, 'median': np.nan}
    return {
        'n': len(valid),
        'avg': valid.mean(),
        'wr': (valid > 0).mean() * 100,
        'median': valid.median(),
    }


def calc_profit_factor(series):
    """计算盈亏比：总盈利 / 总亏损的绝对值"""
    valid = series.dropna()
    if len(valid) == 0:
        return np.nan
    gains = valid[valid > 0].sum()
    losses = abs(valid[valid < 0].sum())
    if losses == 0:
        return np.inf if gains > 0 else np.nan
    return gains / losses


# ============================================================
# 第一章：市场行情概览（保留原有逻辑）
# ============================================================

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

        recent5 = idx_df.tail(5)
        ret5 = recent5['pct_chg'].sum()

        lines.append(f'### {name}（{code}）')
        lines.append(f'- 区间: {start_row["trade_date"]} → {end_row["trade_date"]}')
        lines.append(f'- 最新收盘: {end_row["close"]:.2f}')
        lines.append(f'- 区间涨跌: {fmt_ret(period_ret)}')
        lines.append(f'- 最近5日涨跌: {fmt_ret(ret5)}')
        dates_str = ' | '.join(
            f'{r["trade_date"][-4:]} {fmt_ret(r["pct_chg"])}'
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
        start_close = sh.iloc[0]["close"]
        end_close = sh.iloc[-1]["close"]
        direction_word = "跌至" if end_close < start_close else "涨至"
        lines.append(f'- 选股数据区间内上证指数从 {start_close:.2f} {direction_word} {end_close:.2f}，累计涨跌 {fmt_ret(total_ret)}')
        lines.append(f'- 最近5个交易日：{up_days}涨{5-up_days}跌')
        lines.append('')

    return '\n'.join(lines)


# ============================================================
# 第二章：做多预测能力分析
# ============================================================

def analyze_long(df: pd.DataFrame) -> str:
    """分析做多方向：哪个模型选涨股最准"""
    long_df = df[df['direction'] == 'long'].copy()
    lines = ['## 二、做多预测能力分析（选涨股）\n']
    lines.append('> 做多含义：模型预测该股票会上涨，推荐买入。收益率 = (实际价格 - 买入价) / 买入价\n')

    # 2.1 各模型做多总览
    lines.append('### 2.1 各模型做多表现\n')
    rows = []
    for model in VALID_MODELS:
        m_df = long_df[long_df['model_name'] == model]
        if m_df.empty:
            continue
        row = {'模型': model, '选股数': len(m_df)}
        for t in [1, 2, 5]:
            col = f'return_t{t}'
            s = calc_stats(m_df[col])
            row[f'T+{t}样本'] = s['n']
            row[f'T+{t}均收益'] = fmt_ret(s['avg'])
            row[f'T+{t}胜率'] = fmt_wr(s['wr'])
            row[f'T+{t}中位数'] = fmt_ret(s['median'])
        # 盈亏比（T+1）
        pf = calc_profit_factor(m_df['return_t1'].dropna())
        row['T+1盈亏比'] = f'{pf:.2f}' if np.isfinite(pf) else 'N/A'
        rows.append(row)

    lines.append(pd.DataFrame(rows).to_markdown(index=False))
    lines.append('')

    # 2.2 做多涨跌幅度分析
    lines.append('### 2.2 做多盈亏幅度分析（T+1）\n')
    lines.append('| 模型 | 上涨笔数 | 平均涨幅 | 下跌笔数 | 平均跌幅 | 最大涨幅 | 最大跌幅 |')
    lines.append('|------|---------|---------|---------|---------|---------|---------|')

    for model in VALID_MODELS:
        m_df = long_df[long_df['model_name'] == model]
        valid = m_df['return_t1'].dropna()
        if len(valid) == 0:
            continue
        up = valid[valid > 0]
        down = valid[valid <= 0]
        lines.append(
            f'| {model} | {len(up)} | {fmt_ret(up.mean()) if len(up) > 0 else "N/A"} '
            f'| {len(down)} | {fmt_ret(down.mean()) if len(down) > 0 else "N/A"} '
            f'| {fmt_ret(valid.max())} | {fmt_ret(valid.min())} |'
        )
    lines.append('')

    # 2.3 做多策略方法论
    lines.append('### 2.3 做多各策略表现（全模型合并）\n')
    rows = []
    for method in sorted(long_df['methodology'].dropna().unique()):
        m_df = long_df[long_df['methodology'] == method]
        label = f'{METHOD_CN.get(method, method)}({method})'
        for t in [1, 2]:
            col = f'return_t{t}'
            s = calc_stats(m_df[col])
            if t == 1:
                row = {'策略': label, '样本': s['n'],
                       'T+1均收益': fmt_ret(s['avg']),
                       'T+1胜率': fmt_wr(s['wr'])}
            else:
                row['T+2均收益'] = fmt_ret(s['avg'])
                row['T+2胜率'] = fmt_wr(s['wr'])
        pf = calc_profit_factor(m_df['return_t1'].dropna())
        row['盈亏比'] = f'{pf:.2f}' if np.isfinite(pf) else 'N/A'
        rows.append(row)

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        result_df = result_df.sort_values('T+1均收益', ascending=False,
                                          key=lambda x: x.str.replace('%', '').str.replace('+', '').astype(float, errors='ignore'))
        lines.append(result_df.to_markdown(index=False))
    lines.append('')

    # 2.4 模型×策略交叉（做多T+1胜率矩阵）
    lines.append('### 2.4 模型×策略交叉矩阵（做多T+1胜率）\n')
    methods = sorted(long_df['methodology'].dropna().unique())
    header = '| 模型 | ' + ' | '.join(METHOD_CN.get(m, m) for m in methods) + ' |'
    sep = '|------|' + '|'.join(['------'] * len(methods)) + '|'
    lines.append(header)
    lines.append(sep)

    for model in VALID_MODELS:
        cells = [model]
        for method in methods:
            sub = long_df[(long_df['model_name'] == model) & (long_df['methodology'] == method)]
            valid = sub['return_t1'].dropna()
            if len(valid) >= 5:  # 至少5个样本才有参考价值
                wr = (valid > 0).mean() * 100
                cells.append(f'{wr:.0f}%({len(valid)})')
            elif len(valid) > 0:
                wr = (valid > 0).mean() * 100
                cells.append(f'{wr:.0f}%({len(valid)})*')
            else:
                cells.append('-')
        lines.append('| ' + ' | '.join(cells) + ' |')
    lines.append('')
    lines.append('> 括号内为样本数，带*表示样本不足5个，仅供参考\n')

    return '\n'.join(lines)


# ============================================================
# 第三章：做空预测能力分析
# ============================================================

def analyze_short(df: pd.DataFrame) -> str:
    """分析做空方向：哪个模型选跌股最准"""
    short_df = df[df['direction'] == 'short'].copy()
    lines = ['## 三、做空预测能力分析（选跌股）\n']
    lines.append('> 做空含义：模型预测该股票会下跌，推荐卖出/回避。判定胜利条件：实际收益率 < 0（股价确实下跌了）\n')

    # 3.1 各模型做空总览
    lines.append('### 3.1 各模型做空表现\n')
    rows = []
    for model in VALID_MODELS:
        m_df = short_df[short_df['model_name'] == model]
        if m_df.empty:
            continue
        row = {'模型': model, '选股数': len(m_df)}
        for t in [1, 2, 5]:
            col = f'return_t{t}'
            valid = m_df[col].dropna()
            s_n = len(valid)
            if s_n == 0:
                row[f'T+{t}样本'] = 0
                row[f'T+{t}均收益'] = 'N/A'
                row[f'T+{t}命中率'] = 'N/A'
            else:
                row[f'T+{t}样本'] = s_n
                # 做空的原始收益率：正=股票涨了(做空亏)，负=股票跌了(做空赚)
                row[f'T+{t}均收益'] = fmt_ret(valid.mean())
                # 命中率：预测下跌且确实下跌（return < 0）
                row[f'T+{t}命中率'] = fmt_wr((valid < 0).mean() * 100)
        rows.append(row)

    lines.append(pd.DataFrame(rows).to_markdown(index=False))
    lines.append('')
    lines.append('> **命中率定义**: 做空推荐的股票实际收益率 < 0（即股价确实下跌了）的比例\n')

    # 3.2 做空涨跌幅度分析
    lines.append('### 3.2 做空盈亏幅度分析（T+1）\n')
    lines.append('> 做空视角：股票下跌=盈利，股票上涨=亏损\n')
    lines.append('| 模型 | 命中(跌)笔数 | 平均跌幅 | 失误(涨)笔数 | 平均涨幅 | 最大跌幅 | 最大涨幅 |')
    lines.append('|------|------------|---------|------------|---------|---------|---------|')

    for model in VALID_MODELS:
        m_df = short_df[short_df['model_name'] == model]
        valid = m_df['return_t1'].dropna()
        if len(valid) == 0:
            continue
        down = valid[valid < 0]   # 股票跌了，做空命中
        up = valid[valid >= 0]    # 股票涨了，做空失误
        lines.append(
            f'| {model} | {len(down)} | {fmt_ret(down.mean()) if len(down) > 0 else "N/A"} '
            f'| {len(up)} | {fmt_ret(up.mean()) if len(up) > 0 else "N/A"} '
            f'| {fmt_ret(valid.min())} | {fmt_ret(valid.max())} |'
        )
    lines.append('')

    # 3.3 做空策略方法论
    lines.append('### 3.3 做空各策略表现（全模型合并）\n')
    rows = []
    for method in sorted(short_df['methodology'].dropna().unique()):
        m_df = short_df[short_df['methodology'] == method]
        label = f'{METHOD_CN.get(method, method)}({method})'
        valid_t1 = m_df['return_t1'].dropna()
        valid_t2 = m_df['return_t2'].dropna()
        if len(valid_t1) == 0:
            continue
        row = {
            '策略': label,
            '样本': len(valid_t1),
            'T+1均收益': fmt_ret(valid_t1.mean()),
            'T+1命中率': fmt_wr((valid_t1 < 0).mean() * 100),
            'T+2均收益': fmt_ret(valid_t2.mean()) if len(valid_t2) > 0 else 'N/A',
            'T+2命中率': fmt_wr((valid_t2 < 0).mean() * 100) if len(valid_t2) > 0 else 'N/A',
        }
        rows.append(row)

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        # 按 T+1 命中率降序
        result_df = result_df.sort_values('T+1命中率', ascending=False,
                                          key=lambda x: x.str.replace('%', '').astype(float, errors='ignore'))
        lines.append(result_df.to_markdown(index=False))
    lines.append('')

    # 3.4 模型×策略交叉（做空T+1命中率矩阵）
    lines.append('### 3.4 模型×策略交叉矩阵（做空T+1命中率）\n')
    methods = sorted(short_df['methodology'].dropna().unique())
    header = '| 模型 | ' + ' | '.join(METHOD_CN.get(m, m) for m in methods) + ' |'
    sep = '|------|' + '|'.join(['------'] * len(methods)) + '|'
    lines.append(header)
    lines.append(sep)

    for model in VALID_MODELS:
        cells = [model]
        for method in methods:
            sub = short_df[(short_df['model_name'] == model) & (short_df['methodology'] == method)]
            valid = sub['return_t1'].dropna()
            if len(valid) >= 5:
                wr = (valid < 0).mean() * 100
                cells.append(f'{wr:.0f}%({len(valid)})')
            elif len(valid) > 0:
                wr = (valid < 0).mean() * 100
                cells.append(f'{wr:.0f}%({len(valid)})*')
            else:
                cells.append('-')
        lines.append('| ' + ' | '.join(cells) + ' |')
    lines.append('')
    lines.append('> 括号内为样本数，带*表示样本不足5个，仅供参考\n')

    return '\n'.join(lines)


# ============================================================
# 第四章：评分与收益相关性
# ============================================================

def analyze_score_effectiveness(df: pd.DataFrame) -> str:
    """分析评分对预测准确性的影响（分方向）"""
    lines = ['## 四、评分与预测准确性\n']
    lines.append('> 分析模型打分（score）是否与实际收益相关：高分选股是否更准？\n')

    for direction, dir_label, win_col_fn in [
        ('long', '做多', lambda s: s > 0),
        ('short', '做空', lambda s: s < 0),
    ]:
        d_df = df[df['direction'] == direction].copy()
        valid = d_df.dropna(subset=['score', 'return_t1'])
        if len(valid) < 20:
            continue

        lines.append(f'### 4.{1 if direction == "long" else 2} {dir_label}：评分分档表现（T+1）\n')

        # 按分数段分组
        # 做多的 score 范围通常是 70-98，做空的是 8-95（差异较大）
        # 用分位数分成高中低三档
        q33 = valid['score'].quantile(0.33)
        q66 = valid['score'].quantile(0.66)

        bins_data = [
            ('低分', valid[valid['score'] <= q33]),
            ('中分', valid[(valid['score'] > q33) & (valid['score'] <= q66)]),
            ('高分', valid[valid['score'] > q66]),
        ]

        lines.append(f'| 分档 | 分数范围 | 样本数 | T+1均收益 | 命中率 | 中位数收益 | 盈亏比 |')
        lines.append(f'|------|---------|--------|----------|--------|----------|--------|')

        for label, bin_df in bins_data:
            if len(bin_df) == 0:
                continue
            r = bin_df['return_t1']
            score_range = f'{bin_df["score"].min():.0f}~{bin_df["score"].max():.0f}'
            avg = fmt_ret(r.mean())
            if direction == 'long':
                wr = fmt_wr((r > 0).mean() * 100)
            else:
                wr = fmt_wr((r < 0).mean() * 100)
            med = fmt_ret(r.median())
            pf = calc_profit_factor(r if direction == 'long' else -r)
            pf_str = f'{pf:.2f}' if np.isfinite(pf) else 'N/A'
            lines.append(f'| {label} | {score_range} | {len(r)} | {avg} | {wr} | {med} | {pf_str} |')

        lines.append('')

        # 评分相关系数
        corr = valid['score'].corr(valid['return_t1'])
        lines.append(f'**评分与T+1收益相关系数**: {corr:.3f}')
        if abs(corr) < 0.1:
            lines.append(f'- 相关性极弱，{dir_label}方向的评分基本无法区分好坏选股')
        elif abs(corr) < 0.3:
            direction_word = "正" if corr > 0 else "负"
            lines.append(f'- 弱{direction_word}相关，评分有微弱参考价值')
        else:
            direction_word = "正" if corr > 0 else "负"
            lines.append(f'- 中等{direction_word}相关，评分有参考价值')
        lines.append('')

    return '\n'.join(lines)


# ============================================================
# 第五章：时间衰减分析
# ============================================================

def analyze_time_decay(df: pd.DataFrame) -> str:
    """分析预测在不同时间窗口的有效性（分方向）"""
    lines = ['## 五、预测时间衰减分析\n']
    lines.append('> 分析模型预测在T+1、T+2、T+5哪个窗口最有效\n')

    for direction, dir_label, win_fn in [
        ('long', '做多', lambda s: (s > 0).mean() * 100),
        ('short', '做空', lambda s: (s < 0).mean() * 100),
    ]:
        d_df = df[df['direction'] == direction]
        lines.append(f'### 5.{1 if direction == "long" else 2} {dir_label}预测时间衰减\n')

        # 汇总表
        lines.append('| 时间窗口 | 样本数 | 平均收益 | 命中率 | 中位数收益 |')
        lines.append('|----------|--------|---------|--------|----------|')

        for t in [1, 2, 3, 5, 10, 20]:
            col = f'return_t{t}'
            if col not in d_df.columns:
                continue
            valid = d_df[col].dropna()
            if len(valid) == 0:
                continue
            lines.append(
                f'| T+{t} | {len(valid)} | {fmt_ret(valid.mean())} '
                f'| {fmt_wr(win_fn(valid))} | {fmt_ret(valid.median())} |'
            )

        lines.append('')

        # 各模型分别的时间衰减
        lines.append(f'**各模型{dir_label}收益随时间变化**\n')
        header_cols = ['模型']
        for t in [1, 2, 5]:
            header_cols.extend([f'T+{t}均收益', f'T+{t}命中率'])
        lines.append('| ' + ' | '.join(header_cols) + ' |')
        lines.append('|' + '|'.join(['------'] * len(header_cols)) + '|')

        for model in VALID_MODELS:
            m_df = d_df[d_df['model_name'] == model]
            cells = [model]
            for t in [1, 2, 5]:
                col = f'return_t{t}'
                valid = m_df[col].dropna()
                if len(valid) > 0:
                    cells.append(fmt_ret(valid.mean()))
                    cells.append(fmt_wr(win_fn(valid)))
                else:
                    cells.extend(['N/A', 'N/A'])
            lines.append('| ' + ' | '.join(cells) + ' |')

        lines.append('')

    return '\n'.join(lines)


# ============================================================
# 第六章：逐日表现
# ============================================================

def analyze_per_date(df: pd.DataFrame, df_market: pd.DataFrame) -> str:
    """按日期和方向分析选股表现"""
    lines = ['## 六、逐日表现分析\n']

    sh = df_market[df_market['ts_code'] == '000001.SH'].sort_values('trade_date')
    sh_dates = sh.set_index('trade_date')['pct_chg'].to_dict()

    for direction, dir_label, win_fn in [
        ('long', '做多', lambda s: (s > 0).mean() * 100),
        ('short', '做空', lambda s: (s < 0).mean() * 100),
    ]:
        d_df = df[df['direction'] == direction]
        lines.append(f'### 6.{1 if direction == "long" else 2} {dir_label}逐日表现\n')
        lines.append('| 选股日期 | 次日大盘 | 样本数 | T+1均收益 | T+1命中率 |')
        lines.append('|----------|---------|--------|----------|----------|')

        dates = sorted(d_df['eval_date'].unique())
        # 构建交易日列表用于找"次日"
        trade_dates = sorted(sh_dates.keys())

        for date in dates:
            day_df = d_df[d_df['eval_date'] == date]
            valid = day_df['return_t1'].dropna()

            # 找下一个交易日的大盘涨跌
            mkt_str = 'N/A'
            try:
                idx = trade_dates.index(date)
                if idx + 1 < len(trade_dates):
                    next_date = trade_dates[idx + 1]
                    mkt_str = fmt_ret(sh_dates[next_date])
            except ValueError:
                pass

            avg_ret = fmt_ret(valid.mean()) if len(valid) > 0 else 'N/A'
            wr = fmt_wr(win_fn(valid)) if len(valid) > 0 else 'N/A'

            lines.append(f'| {date} | {mkt_str} | {len(valid)} | {avg_ret} | {wr} |')

        lines.append('')

    return '\n'.join(lines)


# ============================================================
# 第七章：数据质量说明
# ============================================================

def data_quality_note(df: pd.DataFrame, df_all: pd.DataFrame) -> str:
    """数据质量和局限性说明"""
    lines = ['## 七、数据质量说明\n']

    lines.append('### 数据覆盖情况\n')
    lines.append('| 模型 | 做多数 | 做空数 | 日期范围 | 状态 |')
    lines.append('|------|--------|--------|---------|------|')

    for model in VALID_MODELS:
        m_df = df[df['model_name'] == model]
        if m_df.empty:
            continue
        long_n = len(m_df[m_df['direction'] == 'long'])
        short_n = len(m_df[m_df['direction'] == 'short'])
        min_d = m_df['eval_date'].min()
        max_d = m_df['eval_date'].max()
        lines.append(f'| {model} | {long_n} | {short_n} | {min_d}~{max_d} | 正常 |')

    # doubao-online 异常
    if 'doubao-online' in df_all['model_name'].values:
        do_df = df_all[df_all['model_name'] == 'doubao-online']
        lines.append(f'| doubao-online | - | - | {do_df["eval_date"].min()}~{do_df["eval_date"].max()} | **数据异常，已排除** |')

    lines.append('')

    # T+N 覆盖率
    lines.append('### 各时间窗口数据覆盖率\n')
    lines.append('| 时间窗口 | 做多有效样本 | 做空有效样本 | 总覆盖率 |')
    lines.append('|----------|-----------|-----------|---------|')
    total = len(df)
    for t in [1, 2, 3, 5, 10, 20]:
        col = f'return_t{t}'
        if col not in df.columns:
            continue
        long_valid = df[(df['direction'] == 'long') & df[col].notna()]
        short_valid = df[(df['direction'] == 'short') & df[col].notna()]
        total_valid = len(long_valid) + len(short_valid)
        coverage = total_valid / total * 100 if total > 0 else 0
        lines.append(f'| T+{t} | {len(long_valid)} | {len(short_valid)} | {coverage:.1f}% |')
    lines.append('')

    lines.append('### 数据局限性')
    dates = sorted(df['eval_date'].unique())
    lines.append(f'- 选股数据覆盖 {len(dates)} 个选股日（{dates[0]} ~ {dates[-1]}）')
    lines.append('- 评估期间市场整体下跌（上证跌约5%），做多胜率受系统性拖累')
    lines.append('- 样本量有限，部分模型×策略组合样本不足，结论需后续数据验证')
    lines.append('')

    return '\n'.join(lines)


# ============================================================
# 主入口
# ============================================================

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
    print(f"[分析] 做多: {len(df[df['direction'] == 'long'])} 条，做空: {len(df[df['direction'] == 'short'])} 条")
    print(f"[分析] 加载市场数据: {len(df_market)} 条")

    today = datetime.now().strftime('%Y%m%d')
    long_n = len(df[df['direction'] == 'long'])
    short_n = len(df[df['direction'] == 'short'])

    sections = [
        f'# LA 选股模型预测能力深度分析',
        f'**创建时间**: {today}  ',
        f'**分析范围**: 选股日期 {df["eval_date"].min()} ~ {df["eval_date"].max()}  ',
        f'**有效模型**: {", ".join(VALID_MODELS)}  ',
        f'**数据总量**: {len(df)} 条（做多 {long_n} + 做空 {short_n}）  ',
        '',
        analyze_market(df_market),
        analyze_long(df),
        analyze_short(df),
        analyze_score_effectiveness(df),
        analyze_time_decay(df),
        analyze_per_date(df, df_market),
        data_quality_note(df, df_all),
        '---',
        '*本报告基于 my_stock.la_pick 表数据生成，做多与做空完全分开分析*',
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
    parser = argparse.ArgumentParser(description='LA 选股模型深度分析')
    parser.add_argument('--output', '-o', help='输出 Markdown 文件路径')
    args = parser.parse_args()

    generate_report(args.output)


if __name__ == '__main__':
    main()
