#!/usr/bin/env python3
"""
热度轮转策略实盘信号生成

每日运行时机：收盘后 17:10（my_trend.popularity_rank 已更新）
运行方式：
    cd F:/projects/my-stock-research
    python heat/scripts/live_signal.py

输出：
    heat/scripts/signals/YYYYMMDD_signal.json  ← 当日信号文件
    heat/scripts/state.json                    ← 持仓状态（跨日维护）
    控制台信号摘要

前提条件：
    - SSH 隧道已启动（Desktop/Tssh-tunnel.bat）
    - best_params.json 已生成（否则使用默认参数）
"""
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

# 将项目根目录加入 Python 路径
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from heat.scripts.database import get_trend_engine, get_stock_engine
from heat.scripts.strategy import HeatRotationStrategy

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
logger = logging.getLogger(__name__)

# 文件路径
SCRIPTS_DIR = Path(__file__).parent
BEST_PARAMS_FILE = SCRIPTS_DIR / 'best_params.json'
STATE_FILE = SCRIPTS_DIR / 'state.json'
SIGNALS_DIR = SCRIPTS_DIR / 'signals'

# 默认参数（best_params.json 不存在时使用）
DEFAULT_PARAMS = {
    'lookback': 20,
    'buy_threshold': 0.80,
    'sell_threshold': 0.20,
    'max_hold_days': 9999,
    'min_deal_amount': 5e7,
    'n_positions': 1,
    'sort_by': 'rank_surge',
}


def load_params() -> dict:
    """
    加载策略参数

    优先读取 best_params.json，不存在则使用 DEFAULT_PARAMS。

    Returns:
        策略参数 dict
    """
    if BEST_PARAMS_FILE.exists():
        with open(BEST_PARAMS_FILE, 'r', encoding='utf-8') as f:
            params = json.load(f)
        logger.info(f"加载最优参数: {BEST_PARAMS_FILE}")
        return params
    else:
        logger.info("best_params.json 不存在，使用默认参数")
        return dict(DEFAULT_PARAMS)


def load_state() -> dict:
    """
    读取持仓状态文件

    格式示例：
        {
          "positions": [
            {"code": "000001", "entry_date": "2026-03-18",
             "entry_qfq": 10.5, "n_days": 3}
          ],
          "last_updated": "2026-03-19"
        }

    Returns:
        状态 dict，初始为空持仓
    """
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'positions': [], 'last_updated': None}


def save_state(state: dict):
    """
    保存持仓状态到 state.json

    Args:
        state: 状态 dict
    """
    state['last_updated'] = date.today().isoformat()
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    logger.info(f"持仓状态已保存: {STATE_FILE}")


def load_trading_days_recent(lookback: int) -> list:
    """
    从 my_stock.trade_cal 加载最近 lookback + 10 个交易日

    多取 10 天保证滚动窗口预热有足够数据。

    Args:
        lookback: 策略回看窗口天数

    Returns:
        list of datetime.date，已排序
    """
    engine = get_stock_engine()
    need_days = lookback + 10
    # 用 Python 生成今日日期字符串，避免 DATE_FORMAT 中 %Y 被 PyMySQL 转义
    today_int = date.today().strftime('%Y%m%d')
    sql = f"""
        SELECT cal_date FROM trade_cal
        WHERE is_open = 1
          AND cal_date <= '{today_int}'
        ORDER BY cal_date DESC
        LIMIT {need_days}
    """
    df = pd.read_sql(sql, engine)
    trading_days = sorted(pd.to_datetime(df['cal_date'], format='%Y%m%d').dt.date.tolist())
    logger.info(f"加载最近交易日: {len(trading_days)} 天 ({trading_days[0]} ~ {trading_days[-1]})")
    return trading_days


def load_recent_heat_data(lookback: int, trading_days: list = None) -> pd.DataFrame:
    """
    从 my_trend.popularity_rank 加载近期热度排名数据

    重要：只加载交易日的数据，与回测保持一致。
    popularity_rank 可能包含周末/节假日数据（来自 --init 回填），
    必须过滤到交易日以确保 lookback 窗口含义与回测一致。

    Args:
        lookback: 策略回看窗口天数
        trading_days: 交易日列表（如不传则自动加载）

    Returns:
        热度排名 DataFrame（stock_code, date, rank, deal_amount）
    """
    # 获取交易日列表
    if trading_days is None:
        trading_days = load_trading_days_recent(lookback)

    engine = get_trend_engine()
    # 用交易日范围查询，避免加载非交易日数据
    start_date = trading_days[0].strftime('%Y-%m-%d')
    end_date = trading_days[-1].strftime('%Y-%m-%d')

    sql = f"""
        SELECT stock_code, date, `rank`, deal_amount
        FROM popularity_rank
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        ORDER BY date ASC
    """
    df = pd.read_sql(sql, engine)
    df['date'] = pd.to_datetime(df['date']).dt.date

    # 过滤到交易日（关键：与回测 strategy.py _compute_heat_position 逻辑一致）
    td_set = set(trading_days)
    before_filter = len(df)
    df = df[df['date'].isin(td_set)].copy()
    filtered_out = before_filter - len(df)
    if filtered_out > 0:
        logger.info(f"  过滤非交易日数据: {filtered_out:,} 条（周末/节假日）")

    logger.info(f"加载热度数据: {len(df):,} 条，{df['date'].nunique()} 个交易日")
    return df


def load_recent_price_data(stock_codes: list) -> pd.DataFrame:
    """
    从 my_stock.market_daily 加载指定股票最新前复权价格

    正确计算前复权价格：qfq_close = close * adj_factor / latest_adj_factor
    与 data_loader.py 保持一致。

    Args:
        stock_codes: 6 位股票代码列表

    Returns:
        DataFrame（stock_code, date, close, qfq_close）
    """
    if not stock_codes:
        return pd.DataFrame()

    engine = get_stock_engine()
    # 转换为 ts_code 格式
    ts_codes = []
    for code in stock_codes:
        if code.startswith(('600', '601', '603', '605', '688', '689')):
            ts_codes.append(f"{code}.SH")
        else:
            ts_codes.append(f"{code}.SZ")

    codes_str = "','".join(ts_codes)
    sql = f"""
        SELECT m.ts_code, m.trade_date, m.close, a.adj_factor
        FROM market_daily m
        JOIN adj_factor a ON m.ts_code = a.ts_code AND m.trade_date = a.trade_date
        WHERE m.ts_code IN ('{codes_str}')
          AND m.trade_date >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 10 DAY), '%Y%m%d')
        ORDER BY m.trade_date DESC
    """
    df = pd.read_sql(sql, engine)
    df['stock_code'] = df['ts_code'].str[:6]
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date

    # 正确计算前复权价格（与 data_loader.py 一致）
    # qfq_close = close * adj_factor / latest_adj_factor
    latest_adj = df.groupby('stock_code')['adj_factor'].transform('first')  # DESC排序，first=最新
    df['qfq_close'] = df['close'] * df['adj_factor'] / latest_adj
    return df


def compute_heat_signal(heat_df: pd.DataFrame, params: dict) -> tuple:
    """
    计算当日热度信号

    前提：heat_df 已经过滤到交易日（由 load_recent_heat_data 完成），
    rolling(window=lookback) 滚过的是交易日数量，与回测 strategy.py 一致。

    Args:
        heat_df: 仅含交易日的热度排名 DataFrame
        params:  策略参数

    Returns:
        (today_heat_position, today_rank_surge, today_deal, today_date) 元组
    """
    lookback = params['lookback']

    # heat_df 已过滤为交易日，dates 即交易日序列
    dates = sorted(heat_df['date'].unique())
    if len(dates) < lookback // 2:
        logger.warning(f"热度数据天数不足（{len(dates)} < {lookback // 2}），信号可能不准确")

    # 透视表
    rank_pivot = heat_df.pivot_table(index='date', columns='stock_code', values='rank')
    rank_pivot = rank_pivot.sort_index()

    deal_pivot = heat_df.pivot_table(index='date', columns='stock_code', values='deal_amount')
    deal_pivot = deal_pivot.sort_index()

    # 滚动计算
    rolling_max = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).max()
    rolling_min = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).min()
    rolling_mean = rank_pivot.rolling(window=lookback, min_periods=lookback // 2).mean()

    range_val = (rolling_max - rolling_min).replace(0, np.nan)
    heat_position = (rank_pivot - rolling_min) / range_val
    rank_surge = rank_pivot / rolling_mean.replace(0, np.nan)

    # 取最新一天的数据
    today = dates[-1]
    today_hp = heat_position.loc[today].dropna() if today in heat_position.index else pd.Series()
    today_rs = rank_surge.loc[today].dropna() if today in rank_surge.index else pd.Series()
    today_deal = deal_pivot.loc[today].dropna() if today in deal_pivot.index else pd.Series()

    return today_hp, today_rs, today_deal, today


def generate_signal(params: dict) -> dict:
    """
    生成实盘信号主函数

    流程：
    1. 加载近期热度数据
    2. 计算 heat_position 和 rank_surge
    3. 读取持仓状态（state.json）
    4. 检查持仓卖出条件
    5. 扫描买入候选
    6. 生成信号文件

    Args:
        params: 策略参数

    Returns:
        信号 dict（含 date/positions/signals/params_used 等字段）
    """
    today_str = date.today().isoformat()
    logger.info(f"生成信号日期: {today_str}")
    logger.info(f"使用参数: {params}")

    # ---- 1. 加载交易日和热度数据（仅交易日，与回测一致） ----
    lookback = params.get('lookback', 20)
    trading_days = load_trading_days_recent(lookback)
    heat_df = load_recent_heat_data(lookback, trading_days)

    if len(heat_df) == 0:
        logger.error("热度数据为空，无法生成信号")
        return {}

    # ---- 2. 计算信号 ----
    today_hp, today_rs, today_deal, data_date = compute_heat_signal(heat_df, params)
    logger.info(f"热度数据最新日期: {data_date}")

    # 数据新鲜度检查：最新数据应为最近交易日
    latest_td = trading_days[-1]
    if data_date != latest_td:
        logger.warning(f"⚠️ 数据可能不新鲜！热度最新={data_date}，最近交易日={latest_td}")
        if (latest_td - data_date).days > 3:
            logger.error(f"❌ 数据严重滞后（{(latest_td - data_date).days}天），放弃生成信号")
            return {}

    # ---- 3. 读取持仓状态 ----
    state = load_state()
    positions = state.get('positions', [])
    n_positions = params.get('n_positions', 1)

    logger.info(f"当前持仓: {len(positions)}/{n_positions} 个槽位")

    # ---- 4. 检查卖出条件 ----
    signals = []
    positions_to_keep = []

    for pos in positions:
        code = pos['code']
        hp = today_hp.get(code, np.nan)
        sell_threshold = params.get('sell_threshold', 0.20)

        if not np.isnan(hp) and hp <= sell_threshold:
            # 触发卖出
            signals.append({
                'action': 'sell',
                'stock_code': code,
                'heat_position': round(float(hp), 4),
                'reason': f'heat_position({hp:.3f}) <= sell_threshold({sell_threshold})',
                'entry_date': pos.get('entry_date', ''),
            })
            logger.info(f"  SELL: {code}，热度位置 {hp:.3f} <= {sell_threshold}")
        else:
            positions_to_keep.append(pos)
            if not np.isnan(hp):
                logger.info(f"  HOLD: {code}，热度位置 {hp:.3f}")
            else:
                logger.info(f"  HOLD: {code}，热度数据缺失（可能停牌）")

    # ---- 5. 扫描买入候选 ----
    empty_slots = n_positions - len(positions_to_keep)
    held_codes = {p['code'] for p in positions_to_keep}

    if empty_slots > 0:
        buy_threshold = params.get('buy_threshold', 0.80)
        min_deal_amount = params.get('min_deal_amount', 5e7)

        # 候选过滤
        candidates = today_hp[today_hp >= buy_threshold]
        candidates = candidates[~candidates.index.isin(held_codes)]

        # 流动性过滤
        if min_deal_amount > 0 and len(today_deal) > 0:
            deal_filtered = today_deal.reindex(candidates.index).fillna(0)
            candidates = candidates[deal_filtered >= min_deal_amount]

        if len(candidates) > 0:
            # 按 rank_surge 排序
            sort_by = params.get('sort_by', 'rank_surge')
            if sort_by == 'rank_surge' and len(today_rs) > 0:
                surge_vals = today_rs.reindex(candidates.index).dropna()
                sorted_candidates = surge_vals.sort_values(ascending=False)
            else:
                sorted_candidates = candidates.sort_values(ascending=False)

            # 生成 BUY 信号（最多填满空仓）
            buy_count = 0
            for code in sorted_candidates.index[:empty_slots]:
                hp_val = float(today_hp.get(code, np.nan))
                rs_val = float(today_rs.get(code, np.nan)) if code in today_rs else np.nan
                deal_val = float(today_deal.get(code, 0))

                signals.append({
                    'action': 'buy',
                    'stock_code': code,
                    'heat_position': round(hp_val, 4) if not np.isnan(hp_val) else None,
                    'rank_surge': round(rs_val, 4) if not np.isnan(rs_val) else None,
                    'deal_amount_yi': round(deal_val / 1e8, 2),
                    'reason': f'heat_position({hp_val:.3f}) >= buy_threshold({buy_threshold})',
                })
                logger.info(f"  BUY:  {code}，热度位置 {hp_val:.3f}，rank_surge {rs_val:.3f}")
                buy_count += 1

            if buy_count == 0:
                logger.info(f"  无合适买入候选（候选数: {len(candidates)}）")
        else:
            logger.info(f"  无买入候选（buy_threshold={buy_threshold}, "
                        f"min_deal={min_deal_amount/1e8:.1f}亿）")
    else:
        logger.info("  持仓已满，不扫描买入")

    # ---- 6. 更新持仓状态 ----
    # 对于 BUY 信号，加入待买入状态（次日实际执行）
    new_positions = list(positions_to_keep)
    for sig in signals:
        if sig['action'] == 'buy':
            new_positions.append({
                'code': sig['stock_code'],
                'entry_date': today_str,
                'signal_date': today_str,
                'heat_position': sig.get('heat_position'),
                'rank_surge': sig.get('rank_surge'),
                'n_days': 0,
            })

    save_state({'positions': new_positions})

    # ---- 7. 构建信号文档 ----
    signal_doc = {
        'date': today_str,
        'data_date': str(data_date),
        'positions': positions,
        'signals': signals,
        'params_used': {k: v for k, v in params.items()},
        'generated_at': datetime.now().isoformat(),
    }

    # 保存信号文件
    SIGNALS_DIR.mkdir(exist_ok=True)
    signal_file = SIGNALS_DIR / f"{today_str.replace('-', '')}_signal.json"
    with open(signal_file, 'w', encoding='utf-8') as f:
        json.dump(signal_doc, f, indent=2, ensure_ascii=False)

    logger.info(f"信号文件 → {signal_file}")

    # ---- 打印信号摘要 ----
    print("\n" + "=" * 50)
    print(f"【热度策略实盘信号】{today_str}")
    print("=" * 50)
    if signals:
        for sig in signals:
            action = sig['action'].upper()
            code = sig['stock_code']
            if action == 'BUY':
                print(f"  {action}: {code}  热度位置={sig.get('heat_position', 'N/A')}  "
                      f"rank_surge={sig.get('rank_surge', 'N/A')}")
            else:
                print(f"  {action}: {code}  原因：{sig.get('reason', '')}")
    else:
        print("  今日无信号（HOLD）")
    print("=" * 50)

    return signal_doc


def main():
    """实盘信号生成入口"""
    params = load_params()
    signal = generate_signal(params)

    if not signal:
        logger.error("信号生成失败")
        sys.exit(1)

    logger.info("信号生成完成")


if __name__ == '__main__':
    main()
