"""
CLI: 策略回测

用法：
  python scripts/run_backtest.py --symbol BTC/USDT --timeframe 1h --strategy dual_ma
  python scripts/run_backtest.py --symbol ETH/USDT --strategy macd --capital 50000
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse

from app.services.backtest_engine import BacktestEngine
from app.services.data_sync import load_klines


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

    trades = result.get("trades", [])
    if trades:
        print(f"\n最近交易（共 {len(trades)} 笔）:")
        print(f"{'时间':<22} {'方向':<6} {'价格':>12} {'数量':>12} {'盈亏':>12} {'原因'}")
        print("-" * 100)
        for t in trades[-20:]:
            time_str = (t["time"].strftime("%Y-%m-%d %H:%M")
                        if hasattr(t["time"], "strftime") else str(t["time"])[:16])
            pnl_str = f"{t['pnl']:+,.2f}" if t["side"] == "sell" else "-"
            print(f"{time_str:<22} {t['side']:<6} {t['price']:>12,.2f} "
                  f"{t['amount']:>12.6f} {pnl_str:>12} {t['reason'][:40]}")


def main():
    parser = argparse.ArgumentParser(description="加密货币策略回测")
    parser.add_argument("--symbol", default="BTC/USDT", help="交易对")
    parser.add_argument("--timeframe", default="1h", help="K线周期")
    parser.add_argument("--strategy", default="dual_ma",
                        help="策略(dual_ma/rsi/macd/bollinger/composite)")
    parser.add_argument("--capital", type=float, default=100000, help="初始资金")
    parser.add_argument("--stop-loss", type=float, default=0.05, help="止损比例")
    parser.add_argument("--take-profit", type=float, default=0.10, help="止盈比例")
    parser.add_argument("--exchange", default=None, help="交易所")
    args = parser.parse_args()

    df = load_klines(args.symbol, args.timeframe, exchange_name=args.exchange)
    if df.empty:
        print(f"错误: 没有K线数据，请先运行 sync_data.py 获取数据")
        sys.exit(1)

    engine = BacktestEngine(
        initial_capital=args.capital,
        stop_loss=args.stop_loss,
        take_profit=args.take_profit,
    )
    result = engine.run(df, args.strategy)
    print_report(result)


if __name__ == "__main__":
    main()
