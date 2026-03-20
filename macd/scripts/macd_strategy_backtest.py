"""
MACD 经典策略回测核心模块

策略规则 (经典金叉/死叉):
    买入 (金叉): DIF 从下方穿越 DEA，即:
        前一天 DIF <= DEA，当天 DIF > DEA
    卖出 (死叉): DIF 从上方穿越 DEA，即:
        前一天 DIF >= DEA，当天 DIF < DEA

信号执行:
    - T 日收盘确认信号
    - T+1 日开盘价执行 (含滑点)

交易成本:
    - 佣金: 0.03% 双边，最低 5 元
    - 印花税: 0.1% (仅卖出)
    - 滑点: 0.2% 双边

数据来源:
    从 my_stock 库读取前复权 (qfq) 日线数据
"""
import os
import sys
import logging
import numpy as np
import pandas as pd
from sqlalchemy import text

# 将 macd/scripts 目录加入路径
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SCRIPT_DIR)

# 导入 macd 研究库的数据库引擎
from database import read_engine

logger = logging.getLogger(__name__)


# ================================================================
# 数据加载
# ================================================================

def load_stock_qfq(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    加载前复权日线数据

    返回 DataFrame 包含: trade_date, open, high, low, close, vol, pct_chg
    按 trade_date 升序排列
    """
    sql = text("""
        SELECT m.trade_date, m.open, m.high, m.low, m.close, m.vol, m.pct_chg
        FROM market_daily m
        WHERE m.ts_code = :ts_code
          AND m.trade_date >= :start_date
          AND m.trade_date <= :end_date
        ORDER BY m.trade_date
    """)
    sql_adj = text("""
        SELECT trade_date, adj_factor
        FROM adj_factor
        WHERE ts_code = :ts_code
        ORDER BY trade_date
    """)
    sql_latest_adj = text("""
        SELECT adj_factor FROM adj_factor
        WHERE ts_code = :ts_code
        ORDER BY trade_date DESC LIMIT 1
    """)

    with read_engine.connect() as conn:
        rows = conn.execute(sql, {
            'ts_code': ts_code,
            'start_date': start_date.replace('-', ''),
            'end_date': end_date.replace('-', ''),
        }).fetchall()
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=['trade_date', 'open', 'high', 'low', 'close', 'vol', 'pct_chg'])
        df = df.astype({'open': float, 'high': float, 'low': float, 'close': float,
                        'vol': float, 'pct_chg': float})

        # 读取复权因子 (不限日期范围, 取全量做前复权)
        rows_adj = conn.execute(sql_adj, {'ts_code': ts_code}).fetchall()
        latest = conn.execute(sql_latest_adj, {'ts_code': ts_code}).scalar()

        if rows_adj and latest and latest > 0:
            df_adj = pd.DataFrame(rows_adj, columns=['trade_date', 'adj_factor'])
            df_adj['adj_factor'] = df_adj['adj_factor'].astype(float)
            # 只取 start_date 到 end_date 内的复权因子
            df = df.merge(df_adj, on='trade_date', how='left')
            mask = df['adj_factor'].notna()
            for col in ['open', 'high', 'low', 'close']:
                df.loc[mask, col] = df.loc[mask, col] * (df.loc[mask, 'adj_factor'] / latest)
                df[col] = df[col].round(2)
            df.drop(columns=['adj_factor'], inplace=True)

    return df.reset_index(drop=True)


# ================================================================
# MACD 计算
# ================================================================

def calc_macd(close: pd.Series, fast: int, slow: int, signal: int):
    """
    计算 MACD 三线

    返回: (dif, dea, hist) 三个 Series
    - DIF  = EMA(close, fast) - EMA(close, slow)
    - DEA  = EMA(DIF, signal)
    - HIST = (DIF - DEA) * 2  (MACD 柱)
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = (dif - dea) * 2
    return dif, dea, hist


# ================================================================
# 回测核心
# ================================================================

def run_single_stock_backtest(
    df: pd.DataFrame,
    fast_period: int,
    slow_period: int,
    signal_period: int,
    initial_capital: float = 1_000_000.0,
    commission_rate: float = 0.0003,    # 佣金率 0.03%
    min_commission: float = 5.0,         # 最低佣金 5 元
    stamp_duty_rate: float = 0.001,      # 印花税率 0.1%
    slippage_rate: float = 0.002,        # 滑点 0.2%
) -> dict | None:
    """
    对单只股票执行 MACD 金叉/死叉策略回测

    参数:
        df:             前复权日线 DataFrame (trade_date, open, high, low, close)
        fast_period:    EMA 快线周期
        slow_period:    EMA 慢线周期
        signal_period:  DEA 信号线周期

    返回:
        metrics dict, 包含 sharpe/annual_return/max_drawdown/win_rate/total_trades
        若数据不足则返回 None
    """
    min_bars = slow_period + signal_period + 10
    if len(df) < min_bars:
        return None

    df = df.copy().reset_index(drop=True)

    # 1. 计算 MACD
    dif, dea, hist = calc_macd(df['close'], fast_period, slow_period, signal_period)
    df['dif'] = dif
    df['dea'] = dea
    df['hist'] = hist

    # 2. 生成信号 (T 日收盘确认)
    #    金叉: 今天 DIF > DEA, 昨天 DIF <= DEA → BUY (+1)
    #    死叉: 今天 DIF < DEA, 昨天 DIF >= DEA → SELL (-1)
    prev_dif = dif.shift(1)
    prev_dea = dea.shift(1)
    golden_cross = (dif > dea) & (prev_dif <= prev_dea)
    dead_cross = (dif < dea) & (prev_dif >= prev_dea)

    df['signal'] = 0
    df.loc[golden_cross, 'signal'] = 1
    df.loc[dead_cross, 'signal'] = -1

    # 3. 逐日回测 (在 T+1 日开盘执行 T 日的信号)
    cash = float(initial_capital)
    position = 0       # 持仓股数
    avg_cost = 0.0     # 持仓均价 (含佣金)
    trades = []
    equity_curve = []

    for i in range(len(df)):
        row = df.iloc[i]

        # 执行前一日信号 (T+1 开盘)
        if i > 0:
            prev_signal = int(df.iloc[i - 1]['signal'])
            open_p = float(row['open'])

            if prev_signal == 1 and position == 0:
                # ---- 买入 ----
                buy_price = open_p * (1 + slippage_rate)
                # 最大可买手数 (每手100股), 预留 0.5% 余量给佣金
                max_qty = int((cash * 0.995) / buy_price / 100) * 100
                if max_qty >= 100:
                    commission = max(buy_price * max_qty * commission_rate, min_commission)
                    total_cost = buy_price * max_qty + commission
                    if total_cost <= cash:
                        cash -= total_cost
                        position = max_qty
                        avg_cost = total_cost / max_qty
                        trades.append({
                            'trade_date': row['trade_date'],
                            'direction': 'buy',
                            'price': round(buy_price, 2),
                            'quantity': max_qty,
                            'amount': round(total_cost, 2),
                            'commission': round(commission, 2),
                            'stamp_duty': 0,
                            'pnl': 0,
                        })

            elif prev_signal == -1 and position > 0:
                # ---- 卖出 ----
                sell_price = open_p * (1 - slippage_rate)
                commission = max(sell_price * position * commission_rate, min_commission)
                stamp_duty = sell_price * position * stamp_duty_rate
                proceeds = sell_price * position - commission - stamp_duty
                pnl = proceeds - avg_cost * position

                cash += proceeds
                trades.append({
                    'trade_date': row['trade_date'],
                    'direction': 'sell',
                    'price': round(sell_price, 2),
                    'quantity': position,
                    'amount': round(proceeds, 2),
                    'commission': round(commission, 2),
                    'stamp_duty': round(stamp_duty, 2),
                    'pnl': round(pnl, 2),
                })
                position = 0
                avg_cost = 0.0

        # 记录当日权益
        close_p = float(row['close'])
        market_val = position * close_p
        total_val = cash + market_val
        equity_curve.append({
            'trade_date': row['trade_date'],
            'total_value': total_val,
        })

    # 4. 期末强制清仓 (按最后一日收盘价)
    if position > 0:
        last = df.iloc[-1]
        sell_price = float(last['close']) * (1 - slippage_rate)
        commission = max(sell_price * position * commission_rate, min_commission)
        stamp_duty = sell_price * position * stamp_duty_rate
        proceeds = sell_price * position - commission - stamp_duty
        pnl = proceeds - avg_cost * position
        cash += proceeds
        trades.append({
            'trade_date': last['trade_date'],
            'direction': 'sell',
            'price': round(sell_price, 2),
            'quantity': position,
            'amount': round(proceeds, 2),
            'commission': round(commission, 2),
            'stamp_duty': round(stamp_duty, 2),
            'pnl': round(pnl, 2),
        })
        # 更新最后一日的权益
        if equity_curve:
            equity_curve[-1]['total_value'] = cash

    if not equity_curve:
        return None

    # 5. 计算绩效指标
    final_value = equity_curve[-1]['total_value']
    n_days = len(equity_curve)
    years = n_days / 252

    total_return = (final_value - initial_capital) / initial_capital
    if total_return > -1:
        annual_return = (1 + total_return) ** (1 / max(years, 0.01)) - 1
    else:
        annual_return = -1.0

    # 最大回撤
    peak = initial_capital
    max_dd = 0.0
    for pt in equity_curve:
        v = pt['total_value']
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # 夏普比率 (无风险利率 3%/年)
    vals = [pt['total_value'] for pt in equity_curve]
    if len(vals) > 1:
        rets = [(vals[i] - vals[i - 1]) / vals[i - 1]
                for i in range(1, len(vals)) if vals[i - 1] > 0]
        if len(rets) > 1:
            avg_r = sum(rets) / len(rets)
            std_r = (sum((r - avg_r) ** 2 for r in rets) / len(rets)) ** 0.5
            sharpe = (avg_r - 0.03 / 252) / std_r * (252 ** 0.5) if std_r > 0 else 0.0
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    # 胜率和盈亏比
    sell_trades = [t for t in trades if t['direction'] == 'sell']
    wins = [t for t in sell_trades if t['pnl'] > 0]
    losses = [t for t in sell_trades if t['pnl'] < 0]
    win_rate = len(wins) / len(sell_trades) if sell_trades else 0.0
    avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(t['pnl'] for t in losses) / len(losses)) if losses else 0.0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0.0

    return {
        'sharpe': round(sharpe, 4),
        'annual_return': round(annual_return * 100, 2),
        'max_drawdown': round(max_dd * 100, 2),
        'total_return': round(total_return * 100, 2),
        'win_rate': round(win_rate * 100, 2),
        'profit_loss_ratio': round(profit_loss_ratio, 2),
        'total_trades': len(sell_trades),
        'equity_curve': equity_curve,
        'trades': trades,
    }


def run_portfolio_backtest(
    stock_data: dict,
    fast_period: int,
    slow_period: int,
    signal_period: int,
    start_date: str,
    end_date: str,
    initial_capital: float = 1_000_000.0,
    commission_rate: float = 0.0003,
    min_commission: float = 5.0,
    stamp_duty_rate: float = 0.001,
    slippage_rate: float = 0.002,
) -> dict | None:
    """
    对多只股票分别回测, 返回平均绩效指标

    stock_data: {ts_code: df} 预加载的数据字典
    """
    all_metrics = []

    for ts_code, df in stock_data.items():
        # 过滤日期范围
        mask = (df['trade_date'] >= start_date.replace('-', '')) & \
               (df['trade_date'] <= end_date.replace('-', ''))
        df_filtered = df[mask].copy().reset_index(drop=True)

        m = run_single_stock_backtest(
            df_filtered,
            fast_period=fast_period,
            slow_period=slow_period,
            signal_period=signal_period,
            initial_capital=initial_capital,
            commission_rate=commission_rate,
            min_commission=min_commission,
            stamp_duty_rate=stamp_duty_rate,
            slippage_rate=slippage_rate,
        )
        if m is not None and m['total_trades'] > 0:
            all_metrics.append(m)

    if not all_metrics:
        return None

    # 取平均值
    avg = {
        'sharpe': round(sum(m['sharpe'] for m in all_metrics) / len(all_metrics), 4),
        'annual_return': round(sum(m['annual_return'] for m in all_metrics) / len(all_metrics), 2),
        'max_drawdown': round(sum(m['max_drawdown'] for m in all_metrics) / len(all_metrics), 2),
        'total_return': round(sum(m['total_return'] for m in all_metrics) / len(all_metrics), 2),
        'win_rate': round(sum(m['win_rate'] for m in all_metrics) / len(all_metrics), 2),
        'profit_loss_ratio': round(sum(m['profit_loss_ratio'] for m in all_metrics) / len(all_metrics), 2),
        'total_trades': int(sum(m['total_trades'] for m in all_metrics) / len(all_metrics)),
        'n_stocks': len(all_metrics),
    }
    return avg
