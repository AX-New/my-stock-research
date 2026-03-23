"""
加密货币量化交易 - 自动交易机器人

全自动运行：定时获取K线 → 计算信号 → 执行交易 → 检查止损止盈

用法：
  # 模拟交易（默认）
  python run_bot.py --symbol BTC/USDT --strategy dual_ma --interval 60

  # 实盘交易（需配置 API Key）
  python run_bot.py --symbol BTC/USDT --strategy composite --live

  # 多交易对
  python run_bot.py --symbols BTC/USDT,ETH/USDT --strategy macd
"""
import sys
import os
import time
import signal as sig
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from config import DEFAULT_SYMBOL, DEFAULT_TIMEFRAME
from data_fetcher import update_klines, load_klines
from strategy import get_strategy
from trader import Trader
from exchange_client import fetch_ticker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from logger import get_logger

logger = get_logger('crypto.bot')

# 全局停止标志
_running = True


def _signal_handler(signum, frame):
    """优雅停止"""
    global _running
    logger.info("收到停止信号，准备退出...")
    _running = False


def run_once(trader: Trader, symbol: str, timeframe: str,
             strategy_name: str, strategy_kwargs: dict = None) -> dict:
    """
    执行一轮交易逻辑

    1. 增量更新K线
    2. 加载最新K线数据
    3. 计算策略信号
    4. 检查止损止盈
    5. 执行交易信号

    Returns:
        本轮执行结果
    """
    logger.info(f"--- 开始执行: {symbol} {timeframe} {strategy_name} ---")

    # 1. 获取最新行情
    try:
        ticker = fetch_ticker(trader.exchange, symbol)
        current_price = ticker['last']
        logger.info(f"当前价格: {symbol} = {current_price:,.2f} USDT")
    except Exception as e:
        logger.error(f"获取行情失败: {e}")
        return {'error': str(e)}

    # 2. 检查止损止盈（优先于策略信号）
    sl_tp_result = trader.check_stop_loss_take_profit(
        symbol, strategy_name, current_price)
    if sl_tp_result['action'] in ('SELL',):
        logger.info(f"止损/止盈触发: {sl_tp_result}")
        return sl_tp_result

    # 3. 增量更新K线数据
    try:
        update_klines(symbol, timeframe, exchange_name=trader.exchange_name)
    except Exception as e:
        logger.error(f"更新K线失败: {e}")
        # 不中断，尝试用已有数据计算

    # 4. 加载K线数据
    df = load_klines(symbol, timeframe, exchange_name=trader.exchange_name)
    if df.empty or len(df) < 100:
        logger.warning(f"K线数据不足: {len(df)} 条，跳过本轮")
        return {'action': 'SKIP', 'reason': f'数据不足({len(df)}条)'}

    # 5. 计算策略信号
    strategy = get_strategy(strategy_name, **(strategy_kwargs or {}))
    signal, reason = strategy.compute_signal(df.copy())
    indicators = strategy.get_indicators_snapshot(df)

    logger.info(f"策略信号: {signal} - {reason}")

    # 6. 执行信号
    result = trader.execute_signal(
        symbol=symbol,
        signal=signal,
        reason=reason,
        strategy=strategy_name,
        price=current_price,
        indicators=indicators,
    )

    logger.info(f"执行结果: {result}")
    return result


def run_bot(symbols: list, timeframe: str, strategy_name: str,
            strategy_kwargs: dict = None, interval: int = 300,
            is_paper: bool = True, exchange_name: str = None,
            sandbox: bool = False):
    """
    启动自动交易机器人

    Args:
        symbols: 交易对列表
        timeframe: K线周期
        strategy_name: 策略名称
        strategy_kwargs: 策略参数
        interval: 检查间隔（秒）
        is_paper: 是否模拟交易
        exchange_name: 交易所
        sandbox: 是否使用沙盒
    """
    global _running

    # 注册信号处理器
    sig.signal(sig.SIGINT, _signal_handler)
    sig.signal(sig.SIGTERM, _signal_handler)

    trader = Trader(exchange_name=exchange_name, is_paper=is_paper,
                    sandbox=sandbox)

    mode_str = "模拟交易" if is_paper else "实盘交易"
    logger.info("=" * 60)
    logger.info(f"加密货币量化交易机器人启动")
    logger.info(f"  模式:     {mode_str}")
    logger.info(f"  交易所:   {trader.exchange_name}")
    logger.info(f"  交易对:   {', '.join(symbols)}")
    logger.info(f"  K线周期:  {timeframe}")
    logger.info(f"  策略:     {strategy_name}")
    logger.info(f"  检查间隔: {interval}秒")
    logger.info("=" * 60)

    # 首次运行前，确保有历史数据
    for symbol in symbols:
        try:
            df = load_klines(symbol, timeframe, exchange_name=trader.exchange_name)
            if len(df) < 100:
                logger.info(f"历史数据不足，开始获取: {symbol}")
                from data_fetcher import fetch_and_store_klines
                fetch_and_store_klines(symbol, timeframe,
                                       exchange_name=trader.exchange_name,
                                       days=30, exchange=trader.exchange)
        except Exception as e:
            logger.error(f"初始化数据失败 {symbol}: {e}")

    # 主循环
    round_num = 0
    while _running:
        round_num += 1
        logger.info(f"\n{'='*40} 第 {round_num} 轮 {'='*40}")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        for symbol in symbols:
            try:
                result = run_once(trader, symbol, timeframe,
                                  strategy_name, strategy_kwargs)

                # 打印余额
                balance = trader.get_balance()
                usdt_total = balance.get('USDT', {}).get('total', 0)
                logger.info(f"账户余额: {usdt_total:,.2f} USDT")

            except Exception as e:
                logger.error(f"执行出错 {symbol}: {e}", exc_info=True)

        # 等待下一轮
        logger.info(f"下次执行: {interval}秒后")
        for _ in range(interval):
            if not _running:
                break
            time.sleep(1)

    logger.info("交易机器人已停止")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='加密货币自动交易机器人')
    parser.add_argument('--symbol', default=DEFAULT_SYMBOL,
                        help='交易对，多个用逗号分隔（如 BTC/USDT,ETH/USDT）')
    parser.add_argument('--timeframe', default=DEFAULT_TIMEFRAME, help='K线周期')
    parser.add_argument('--strategy', default='dual_ma',
                        help='策略名称(dual_ma/rsi/macd/bollinger/composite)')
    parser.add_argument('--interval', type=int, default=300,
                        help='检查间隔（秒，默认300=5分钟）')
    parser.add_argument('--live', action='store_true',
                        help='实盘交易模式（默认模拟交易）')
    parser.add_argument('--sandbox', action='store_true',
                        help='使用交易所沙盒/测试网')
    parser.add_argument('--exchange', default=None,
                        help='交易所(binance/okx)')

    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbol.split(',')]

    if args.live and not args.sandbox:
        print("\n" + "!" * 60)
        print("  警告: 即将启动实盘交易模式！")
        print("  这将使用真实资金进行交易！")
        print("!" * 60)
        confirm = input("\n输入 'YES' 确认启动实盘交易: ")
        if confirm != 'YES':
            print("已取消")
            sys.exit(0)

    run_bot(
        symbols=symbols,
        timeframe=args.timeframe,
        strategy_name=args.strategy,
        interval=args.interval,
        is_paper=not args.live,
        exchange_name=args.exchange,
        sandbox=args.sandbox,
    )
