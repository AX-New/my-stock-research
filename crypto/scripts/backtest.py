"""
加密货币量化交易 - 回测引擎

功能：
  - 基于历史K线数据回测策略
  - 支持做多/做空（现货仅做多）
  - 支持止损止盈
  - 输出回测报告（收益率、夏普比率、最大回撤等）

用法：
  python backtest.py --symbol BTC/USDT --timeframe 1h --strategy dual_ma --days 180
"""
import sys
import os
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import STOP_LOSS_PCT, TAKE_PROFIT_PCT
from data_fetcher import load_klines
from strategy import get_strategy

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from logger import get_logger

logger = get_logger('crypto.backtest')


class BacktestEngine:
    """回测引擎"""

    def __init__(self, initial_capital: float = 100000,
                 commission: float = 0.001,
                 stop_loss: float = None,
                 take_profit: float = None):
        """
        Args:
            initial_capital: 初始资金（USDT）
            commission: 手续费率（默认0.1%）
            stop_loss: 止损比例（如 0.05 表示5%）
            take_profit: 止盈比例（如 0.10 表示10%）
        """
        self.initial_capital = initial_capital
        self.commission = commission
        self.stop_loss = stop_loss or STOP_LOSS_PCT
        self.take_profit = take_profit or TAKE_PROFIT_PCT

    def run(self, df: pd.DataFrame, strategy_name: str,
            strategy_kwargs: dict = None) -> dict:
        """
        执行回测

        Args:
            df: K线数据 DataFrame（index=open_time, 含 OHLCV）
            strategy_name: 策略名称
            strategy_kwargs: 策略参数

        Returns:
            回测结果字典
        """
        strategy = get_strategy(strategy_name, **(strategy_kwargs or {}))
        logger.info(f"开始回测: 策略={strategy.name}, 数据={len(df)}条, "
                     f"资金={self.initial_capital}")

        capital = self.initial_capital
        position = 0.0  # 持仓数量
        entry_price = 0.0  # 入场价
        trades = []  # 交易记录
        equity_curve = []  # 权益曲线

        for i in range(50, len(df)):
            # 用截止到当前的数据计算信号
            current_data = df.iloc[:i+1].copy()
            current_price = current_data['close'].iloc[-1]
            current_time = current_data.index[-1]

            # 计算当前权益
            equity = capital + position * current_price
            equity_curve.append({
                'time': current_time,
                'equity': equity,
                'price': current_price,
                'position': position,
            })

            # 检查止损止盈（有持仓时）
            if position > 0:
                pnl_pct = (current_price - entry_price) / entry_price

                # 止损
                if pnl_pct <= -self.stop_loss:
                    sell_value = position * current_price
                    fee = sell_value * self.commission
                    capital += sell_value - fee
                    trades.append({
                        'time': current_time,
                        'side': 'sell',
                        'price': current_price,
                        'amount': position,
                        'cost': sell_value,
                        'fee': fee,
                        'reason': f'止损({pnl_pct*100:.1f}%)',
                        'pnl': (current_price - entry_price) * position - fee,
                    })
                    position = 0.0
                    entry_price = 0.0
                    continue

                # 止盈
                if pnl_pct >= self.take_profit:
                    sell_value = position * current_price
                    fee = sell_value * self.commission
                    capital += sell_value - fee
                    trades.append({
                        'time': current_time,
                        'side': 'sell',
                        'price': current_price,
                        'amount': position,
                        'cost': sell_value,
                        'fee': fee,
                        'reason': f'止盈({pnl_pct*100:.1f}%)',
                        'pnl': (current_price - entry_price) * position - fee,
                    })
                    position = 0.0
                    entry_price = 0.0
                    continue

            # 计算策略信号
            signal, reason = strategy.compute_signal(current_data)

            # 执行交易
            if signal == 'BUY' and position == 0:
                # 用全部可用资金买入
                buy_cost = capital * 0.95  # 留5%缓冲
                fee = buy_cost * self.commission
                buy_amount = (buy_cost - fee) / current_price
                position = buy_amount
                entry_price = current_price
                capital -= buy_cost
                trades.append({
                    'time': current_time,
                    'side': 'buy',
                    'price': current_price,
                    'amount': buy_amount,
                    'cost': buy_cost,
                    'fee': fee,
                    'reason': reason,
                    'pnl': 0,
                })

            elif signal == 'SELL' and position > 0:
                sell_value = position * current_price
                fee = sell_value * self.commission
                pnl = (current_price - entry_price) * position - fee
                capital += sell_value - fee
                trades.append({
                    'time': current_time,
                    'side': 'sell',
                    'price': current_price,
                    'amount': position,
                    'cost': sell_value,
                    'fee': fee,
                    'reason': reason,
                    'pnl': pnl,
                })
                position = 0.0
                entry_price = 0.0

        # 如果还有持仓，按最后价格平仓
        if position > 0:
            last_price = df['close'].iloc[-1]
            sell_value = position * last_price
            fee = sell_value * self.commission
            pnl = (last_price - entry_price) * position - fee
            capital += sell_value - fee
            trades.append({
                'time': df.index[-1],
                'side': 'sell',
                'price': last_price,
                'amount': position,
                'cost': sell_value,
                'fee': fee,
                'reason': '回测结束平仓',
                'pnl': pnl,
            })
            position = 0.0

        # 计算统计指标
        result = self._compute_stats(trades, equity_curve, df)
        result['strategy'] = strategy.name
        result['trades'] = trades

        return result

    def _compute_stats(self, trades: list, equity_curve: list,
                       df: pd.DataFrame) -> dict:
        """计算回测统计指标"""
        if not equity_curve:
            return {'error': '无交易数据'}

        eq_df = pd.DataFrame(equity_curve)

        # 最终权益
        final_equity = eq_df['equity'].iloc[-1]

        # 总收益率
        total_return = (final_equity - self.initial_capital) / self.initial_capital

        # 买入持有收益率（基准）
        buy_hold_return = (df['close'].iloc[-1] - df['close'].iloc[0]) / df['close'].iloc[0]

        # 年化收益率
        days = (eq_df['time'].iloc[-1] - eq_df['time'].iloc[0]).total_seconds() / 86400
        annual_return = (1 + total_return) ** (365 / max(days, 1)) - 1 if days > 0 else 0

        # 最大回撤
        eq_df['peak'] = eq_df['equity'].cummax()
        eq_df['drawdown'] = (eq_df['equity'] - eq_df['peak']) / eq_df['peak']
        max_drawdown = eq_df['drawdown'].min()

        # 夏普比率（假设无风险利率3%）
        eq_df['daily_return'] = eq_df['equity'].pct_change()
        sharpe = 0
        if eq_df['daily_return'].std() > 0:
            sharpe = (eq_df['daily_return'].mean() * 252 - 0.03) / (eq_df['daily_return'].std() * np.sqrt(252))

        # 交易统计
        sell_trades = [t for t in trades if t['side'] == 'sell']
        win_trades = [t for t in sell_trades if t['pnl'] > 0]
        lose_trades = [t for t in sell_trades if t['pnl'] < 0]

        total_trades = len(sell_trades)
        win_rate = len(win_trades) / total_trades if total_trades > 0 else 0

        avg_win = np.mean([t['pnl'] for t in win_trades]) if win_trades else 0
        avg_loss = np.mean([t['pnl'] for t in lose_trades]) if lose_trades else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')

        total_fee = sum(t['fee'] for t in trades)

        return {
            'initial_capital': self.initial_capital,
            'final_equity': round(final_equity, 2),
            'total_return': round(total_return * 100, 2),
            'annual_return': round(annual_return * 100, 2),
            'buy_hold_return': round(buy_hold_return * 100, 2),
            'max_drawdown': round(max_drawdown * 100, 2),
            'sharpe_ratio': round(sharpe, 3),
            'total_trades': total_trades,
            'win_rate': round(win_rate * 100, 1),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_factor': round(profit_factor, 3),
            'total_fee': round(total_fee, 2),
            'days': round(days, 1),
        }


def print_report(result: dict):
    """打印回测报告"""
    print("\n" + "=" * 60)
    print(f"  回测报告 - 策略: {result['strategy']}")
    print("=" * 60)
    print(f"  初始资金:     {result['initial_capital']:,.2f} USDT")
    print(f"  最终权益:     {result['final_equity']:,.2f} USDT")
    print(f"  总收益率:     {result['total_return']:+.2f}%")
    print(f"  年化收益率:   {result['annual_return']:+.2f}%")
    print(f"  买入持有:     {result['buy_hold_return']:+.2f}%")
    print(f"  最大回撤:     {result['max_drawdown']:.2f}%")
    print(f"  夏普比率:     {result['sharpe_ratio']:.3f}")
    print("-" * 60)
    print(f"  交易次数:     {result['total_trades']}")
    print(f"  胜率:         {result['win_rate']:.1f}%")
    print(f"  平均盈利:     {result['avg_win']:,.2f} USDT")
    print(f"  平均亏损:     {result['avg_loss']:,.2f} USDT")
    print(f"  盈亏比:       {result['profit_factor']:.3f}")
    print(f"  总手续费:     {result['total_fee']:,.2f} USDT")
    print(f"  回测天数:     {result['days']:.0f} 天")
    print("=" * 60)

    # 打印最近10笔交易
    trades = result.get('trades', [])
    if trades:
        print(f"\n最近交易（共 {len(trades)} 笔）:")
        print(f"{'时间':<22} {'方向':<6} {'价格':>12} {'数量':>12} {'盈亏':>12} {'原因'}")
        print("-" * 100)
        for t in trades[-20:]:
            time_str = t['time'].strftime('%Y-%m-%d %H:%M') if hasattr(t['time'], 'strftime') else str(t['time'])[:16]
            pnl_str = f"{t['pnl']:+,.2f}" if t['side'] == 'sell' else '-'
            print(f"{time_str:<22} {t['side']:<6} {t['price']:>12,.2f} "
                  f"{t['amount']:>12.6f} {pnl_str:>12} {t['reason'][:40]}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='加密货币策略回测')
    parser.add_argument('--symbol', default='BTC/USDT', help='交易对')
    parser.add_argument('--timeframe', default='1h', help='K线周期')
    parser.add_argument('--strategy', default='dual_ma', help='策略名称')
    parser.add_argument('--days', type=int, default=180, help='回测天数')
    parser.add_argument('--capital', type=float, default=100000, help='初始资金')
    parser.add_argument('--stop-loss', type=float, default=0.05, help='止损比例')
    parser.add_argument('--take-profit', type=float, default=0.10, help='止盈比例')
    parser.add_argument('--exchange', default=None, help='交易所')
    args = parser.parse_args()

    # 加载数据
    logger.info(f"加载K线数据: {args.symbol} {args.timeframe}")
    df = load_klines(args.symbol, args.timeframe, exchange_name=args.exchange)

    if df.empty:
        print(f"错误: 没有K线数据，请先运行 data_fetcher.py 获取数据")
        sys.exit(1)

    # 执行回测
    engine = BacktestEngine(
        initial_capital=args.capital,
        stop_loss=args.stop_loss,
        take_profit=args.take_profit,
    )
    result = engine.run(df, args.strategy)

    # 打印报告
    print_report(result)
