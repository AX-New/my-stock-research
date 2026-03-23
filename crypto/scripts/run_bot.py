"""
CLI: 自动交易机器人

用法：
  # 模拟交易（默认）
  python scripts/run_bot.py --symbol BTC/USDT --strategy dual_ma

  # 多交易对
  python scripts/run_bot.py --symbol BTC/USDT,ETH/USDT --strategy macd

  # 实盘交易（需配置 API Key）
  python scripts/run_bot.py --symbol BTC/USDT --strategy composite --live
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse
import signal as sig
import time
from datetime import datetime

from app.config import Config
from app.services.data_sync import sync_klines, load_klines
from app.services.trade_engine import TradeEngine
from app.services.exchange_client import fetch_ticker
from app.strategies import get_strategy
from app.logger import get_logger

logger = get_logger("crypto.bot")

_running = True


def _signal_handler(signum, frame):
    global _running
    logger.info("收到停止信号，准备退出...")
    _running = False


def run_once(trader, symbol, timeframe, strategy_name, strategy_kwargs=None):
    """执行一轮交易逻辑"""
    logger.info(f"--- 执行: {symbol} {timeframe} {strategy_name} ---")

    # 1. 获取行情
    try:
        ticker = fetch_ticker(trader.exchange, symbol)
        current_price = ticker["last"]
        logger.info(f"当前价格: {symbol} = {current_price:,.2f} USDT")
    except Exception as e:
        logger.error(f"获取行情失败: {e}")
        return {"error": str(e)}

    # 2. 检查止损止盈
    sl_tp = trader.check_stop_loss_take_profit(symbol, strategy_name, current_price)
    if sl_tp["action"] == "SELL":
        return sl_tp

    # 3. 增量更新K线
    try:
        sync_klines(symbol, timeframe, exchange=trader.exchange)
    except Exception as e:
        logger.error(f"更新K线失败: {e}")

    # 4. 加载K线
    df = load_klines(symbol, timeframe, exchange_name=trader.exchange_name)
    if df.empty or len(df) < 100:
        return {"action": "SKIP", "reason": f"数据不足({len(df)}条)"}

    # 5. 计算信号
    strategy = get_strategy(strategy_name, **(strategy_kwargs or {}))
    signal, reason = strategy.compute_signal(df.copy())
    indicators = strategy.get_indicators_snapshot(df)
    logger.info(f"信号: {signal} - {reason}")

    # 6. 执行
    result = trader.execute_signal(
        symbol=symbol, signal=signal, reason=reason,
        strategy=strategy_name, price=current_price, indicators=indicators,
    )
    logger.info(f"结果: {result}")
    return result


def main():
    global _running

    parser = argparse.ArgumentParser(description="加密货币自动交易机器人")
    parser.add_argument("--symbol", default="BTC/USDT",
                        help="交易对，多个用逗号分隔")
    parser.add_argument("--timeframe", default="1h", help="K线周期")
    parser.add_argument("--strategy", default="dual_ma",
                        help="策略(dual_ma/rsi/macd/bollinger/composite)")
    parser.add_argument("--interval", type=int, default=300,
                        help="检查间隔（秒，默认300）")
    parser.add_argument("--live", action="store_true", help="实盘模式")
    parser.add_argument("--sandbox", action="store_true", help="沙盒模式")
    parser.add_argument("--exchange", default=None, help="交易所")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbol.split(",")]

    if args.live and not args.sandbox:
        print("\n" + "!" * 60)
        print("  警告: 即将启动实盘交易！将使用真实资金！")
        print("!" * 60)
        confirm = input("\n输入 'YES' 确认: ")
        if confirm != "YES":
            print("已取消")
            sys.exit(0)

    sig.signal(sig.SIGINT, _signal_handler)
    sig.signal(sig.SIGTERM, _signal_handler)

    trader = TradeEngine(
        exchange_name=args.exchange,
        is_paper=not args.live,
        sandbox=args.sandbox,
    )

    mode = "模拟交易" if not args.live else "实盘交易"
    logger.info("=" * 60)
    logger.info(f"交易机器人启动 | {mode} | {trader.exchange_name}")
    logger.info(f"交易对: {', '.join(symbols)} | 策略: {args.strategy}")
    logger.info("=" * 60)

    round_num = 0
    while _running:
        round_num += 1
        logger.info(f"\n{'=' * 40} 第 {round_num} 轮 {'=' * 40}")

        for symbol in symbols:
            try:
                run_once(trader, symbol, args.timeframe, args.strategy)
                balance = trader.get_balance()
                usdt = balance.get("USDT", {}).get("total", 0)
                logger.info(f"余额: {usdt:,.2f} USDT")
            except Exception as e:
                logger.error(f"执行出错 {symbol}: {e}", exc_info=True)

        for _ in range(args.interval):
            if not _running:
                break
            time.sleep(1)

    logger.info("交易机器人已停止")


if __name__ == "__main__":
    main()
