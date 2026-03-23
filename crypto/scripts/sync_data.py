"""
CLI: 数据同步

用法：
  # 同步单个交易对
  python scripts/sync_data.py --symbol BTC/USDT --timeframe 1h

  # 全量同步（所有配置的交易对和周期）
  python scripts/sync_data.py --all

  # 同步交易对元数据
  python scripts/sync_data.py --symbols-only
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse

from app.services.data_sync import sync_klines, sync_symbols, sync_all


def main():
    parser = argparse.ArgumentParser(description="加密货币数据同步")
    parser.add_argument("--symbol", default="BTC/USDT", help="交易对")
    parser.add_argument("--timeframe", default="1h", help="K线周期")
    parser.add_argument("--exchange", default=None, help="交易所(binance/okx)")
    parser.add_argument("--days", type=int, default=None, help="同步天数")
    parser.add_argument("--all", action="store_true", help="全量同步所有交易对和周期")
    parser.add_argument("--symbols-only", action="store_true", help="仅同步交易对元数据")
    args = parser.parse_args()

    if args.symbols_only:
        count = sync_symbols(args.exchange)
        print(f"同步交易对完成: {count} 个")
    elif args.all:
        result = sync_all(args.exchange)
        print(f"全量同步完成: {result}")
    else:
        count = sync_klines(args.symbol, args.timeframe, args.exchange, args.days)
        print(f"同步完成: {args.symbol} {args.timeframe}, {count} 条K线")


if __name__ == "__main__":
    main()
