"""
加密货币量化交易 - 交易执行模块

功能：
  - 模拟交易（paper trading）：不连接交易所，记录虚拟交易
  - 实盘交易（live trading）：通过 ccxt 下单到交易所

模式切换通过 is_paper 参数控制，默认模拟交易。
"""
import sys
import os
import json
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import MAX_POSITION_RATIO, STOP_LOSS_PCT, TAKE_PROFIT_PCT
from database import Session, init_tables
from models import CryptoTrade, CryptoPosition, CryptoSignal
from exchange_client import (
    create_exchange, fetch_ticker, fetch_balance,
    place_order, get_exchange_name,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from logger import get_logger

logger = get_logger('crypto.trader')


class Trader:
    """交易执行器"""

    def __init__(self, exchange_name: str = None, is_paper: bool = True,
                 sandbox: bool = False):
        """
        Args:
            exchange_name: 交易所名称
            is_paper: 是否模拟交易（默认True）
            sandbox: 是否使用沙盒模式（仅实盘时有效）
        """
        self.is_paper = is_paper
        self.exchange = create_exchange(exchange_name, sandbox=sandbox)
        self.exchange_name = get_exchange_name(self.exchange)

        # 模拟账户余额（模拟交易时使用）
        self.paper_balance = {
            'USDT': {'total': 100000, 'free': 100000, 'used': 0},
        }

        init_tables()
        logger.info(f"交易执行器已创建: exchange={self.exchange_name}, "
                     f"模式={'模拟' if is_paper else '实盘'}")

    def get_balance(self) -> dict:
        """获取账户余额"""
        if self.is_paper:
            return self.paper_balance
        return fetch_balance(self.exchange)

    def get_position(self, symbol: str, strategy: str) -> CryptoPosition:
        """从数据库获取持仓"""
        session = Session()
        try:
            pos = session.query(CryptoPosition).filter_by(
                exchange=self.exchange_name,
                symbol=symbol,
                strategy=strategy,
                status='open',
            ).first()
            if pos:
                # 确保返回的对象不绑定到 session
                session.expunge(pos)
            return pos
        finally:
            session.close()

    def execute_signal(self, symbol: str, signal: str, reason: str,
                       strategy: str, price: float, indicators: str = '{}') -> dict:
        """
        执行交易信号

        Args:
            symbol: 交易对
            signal: 信号类型（BUY/SELL/HOLD）
            reason: 信号原因
            strategy: 策略名称
            price: 当前价格
            indicators: 指标快照JSON

        Returns:
            执行结果字典
        """
        if signal == 'HOLD':
            return {'action': 'HOLD', 'reason': reason}

        # 记录信号
        signal_id = self._save_signal(symbol, signal, reason, strategy,
                                       price, indicators)

        position = self.get_position(symbol, strategy)

        if signal == 'BUY':
            if position and position.amount > 0:
                logger.info(f"已有持仓，跳过买入信号: {symbol} {strategy}")
                return {'action': 'SKIP', 'reason': '已有持仓'}
            return self._execute_buy(symbol, strategy, price, signal_id, reason)

        elif signal == 'SELL':
            if not position or position.amount <= 0:
                logger.info(f"无持仓，跳过卖出信号: {symbol} {strategy}")
                return {'action': 'SKIP', 'reason': '无持仓'}
            return self._execute_sell(symbol, strategy, price, position,
                                      signal_id, reason)

        return {'action': 'UNKNOWN', 'signal': signal}

    def _execute_buy(self, symbol: str, strategy: str, price: float,
                     signal_id: int, reason: str) -> dict:
        """执行买入"""
        # 计算买入金额
        balance = self.get_balance()
        usdt_free = balance.get('USDT', {}).get('free', 0)
        buy_amount_usdt = usdt_free * MAX_POSITION_RATIO

        if buy_amount_usdt < 10:
            logger.warning(f"可用余额不足: {usdt_free:.2f} USDT")
            return {'action': 'SKIP', 'reason': f'余额不足({usdt_free:.2f} USDT)'}

        # 计算买入数量
        amount = buy_amount_usdt / price

        order_id = None
        actual_price = price
        fee = buy_amount_usdt * 0.001  # 默认0.1%手续费

        if self.is_paper:
            # 模拟交易：更新虚拟余额
            self.paper_balance['USDT']['free'] -= buy_amount_usdt
            self.paper_balance['USDT']['used'] += buy_amount_usdt
            base_currency = symbol.split('/')[0]
            if base_currency not in self.paper_balance:
                self.paper_balance[base_currency] = {'total': 0, 'free': 0, 'used': 0}
            self.paper_balance[base_currency]['total'] += amount
            self.paper_balance[base_currency]['free'] += amount
            order_id = f"PAPER-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            logger.info(f"[模拟] 买入 {amount:.6f} {symbol} @ {price:.2f}")
        else:
            # 实盘交易
            try:
                order = place_order(self.exchange, symbol, 'buy', amount)
                order_id = order.get('id', '')
                actual_price = order.get('average', price)
                fee = order.get('fee', {}).get('cost', fee)
                amount = order.get('filled', amount)
                logger.info(f"[实盘] 买入 {amount:.6f} {symbol} @ {actual_price:.2f}")
            except Exception as e:
                logger.error(f"买入失败: {e}")
                return {'action': 'FAILED', 'reason': str(e)}

        # 止损止盈价
        stop_loss = actual_price * (1 - STOP_LOSS_PCT)
        take_profit = actual_price * (1 + TAKE_PROFIT_PCT)

        # 保存交易记录和持仓
        self._save_trade(symbol, 'buy', 'market', amount, actual_price,
                         buy_amount_usdt, fee, strategy, signal_id, order_id, reason)
        self._save_position(symbol, strategy, 'long', amount, actual_price,
                            stop_loss, take_profit)

        return {
            'action': 'BUY',
            'symbol': symbol,
            'amount': amount,
            'price': actual_price,
            'cost': buy_amount_usdt,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'order_id': order_id,
        }

    def _execute_sell(self, symbol: str, strategy: str, price: float,
                      position: CryptoPosition, signal_id: int,
                      reason: str) -> dict:
        """执行卖出"""
        amount = position.amount
        sell_value = amount * price
        fee = sell_value * 0.001

        order_id = None
        actual_price = price

        if self.is_paper:
            # 模拟交易
            self.paper_balance['USDT']['free'] += sell_value - fee
            self.paper_balance['USDT']['used'] -= position.avg_price * amount
            base_currency = symbol.split('/')[0]
            if base_currency in self.paper_balance:
                self.paper_balance[base_currency]['total'] -= amount
                self.paper_balance[base_currency]['free'] -= amount
            order_id = f"PAPER-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            logger.info(f"[模拟] 卖出 {amount:.6f} {symbol} @ {price:.2f}")
        else:
            # 实盘交易
            try:
                order = place_order(self.exchange, symbol, 'sell', amount)
                order_id = order.get('id', '')
                actual_price = order.get('average', price)
                fee = order.get('fee', {}).get('cost', fee)
                logger.info(f"[实盘] 卖出 {amount:.6f} {symbol} @ {actual_price:.2f}")
            except Exception as e:
                logger.error(f"卖出失败: {e}")
                return {'action': 'FAILED', 'reason': str(e)}

        pnl = (actual_price - position.avg_price) * amount - fee
        pnl_pct = (actual_price - position.avg_price) / position.avg_price * 100

        # 保存交易记录
        self._save_trade(symbol, 'sell', 'market', amount, actual_price,
                         sell_value, fee, strategy, signal_id, order_id,
                         f"{reason} | PnL: {pnl:+.2f} ({pnl_pct:+.1f}%)")

        # 关闭持仓
        self._close_position(symbol, strategy, pnl)

        return {
            'action': 'SELL',
            'symbol': symbol,
            'amount': amount,
            'price': actual_price,
            'value': sell_value,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'order_id': order_id,
        }

    def check_stop_loss_take_profit(self, symbol: str, strategy: str,
                                     current_price: float) -> dict:
        """检查止损止盈"""
        position = self.get_position(symbol, strategy)
        if not position or position.amount <= 0:
            return {'action': 'NONE'}

        # 止损
        if position.stop_loss and current_price <= position.stop_loss:
            logger.warning(f"触发止损: {symbol} 当前价={current_price:.2f}, "
                          f"止损价={position.stop_loss:.2f}")
            return self._execute_sell(
                symbol, strategy, current_price, position, None,
                f'止损触发(价格{current_price:.2f} <= {position.stop_loss:.2f})')

        # 止盈
        if position.take_profit and current_price >= position.take_profit:
            logger.info(f"触发止盈: {symbol} 当前价={current_price:.2f}, "
                       f"止盈价={position.take_profit:.2f}")
            return self._execute_sell(
                symbol, strategy, current_price, position, None,
                f'止盈触发(价格{current_price:.2f} >= {position.take_profit:.2f})')

        return {'action': 'NONE'}

    def _save_signal(self, symbol, signal, reason, strategy, price,
                     indicators) -> int:
        """保存信号到数据库"""
        session = Session()
        try:
            sig = CryptoSignal(
                exchange=self.exchange_name,
                symbol=symbol,
                timeframe='',
                strategy=strategy,
                signal=signal,
                signal_time=datetime.now(),
                price=price,
                reason=reason,
                indicators=indicators,
            )
            session.add(sig)
            session.commit()
            signal_id = sig.id
            return signal_id
        except Exception as e:
            session.rollback()
            logger.error(f"保存信号失败: {e}")
            return 0
        finally:
            session.close()

    def _save_trade(self, symbol, side, order_type, amount, price, cost,
                    fee, strategy, signal_id, order_id, note):
        """保存交易记录到数据库"""
        session = Session()
        try:
            trade = CryptoTrade(
                exchange=self.exchange_name,
                symbol=symbol,
                side=side,
                order_type=order_type,
                amount=amount,
                price=price,
                cost=cost,
                fee=fee,
                strategy=strategy,
                signal_id=signal_id,
                order_id=order_id or '',
                status='filled',
                trade_time=datetime.now(),
                is_paper=1 if self.is_paper else 0,
                note=note,
            )
            session.add(trade)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"保存交易记录失败: {e}")
        finally:
            session.close()

    def _save_position(self, symbol, strategy, side, amount, price,
                       stop_loss, take_profit):
        """保存/更新持仓"""
        session = Session()
        try:
            pos = session.query(CryptoPosition).filter_by(
                exchange=self.exchange_name,
                symbol=symbol,
                strategy=strategy,
            ).first()

            if pos:
                pos.side = side
                pos.amount = amount
                pos.avg_price = price
                pos.current_price = price
                pos.stop_loss = stop_loss
                pos.take_profit = take_profit
                pos.status = 'open'
            else:
                pos = CryptoPosition(
                    exchange=self.exchange_name,
                    symbol=symbol,
                    strategy=strategy,
                    side=side,
                    amount=amount,
                    avg_price=price,
                    current_price=price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    status='open',
                )
                session.add(pos)

            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"保存持仓失败: {e}")
        finally:
            session.close()

    def _close_position(self, symbol, strategy, realized_pnl):
        """关闭持仓"""
        session = Session()
        try:
            pos = session.query(CryptoPosition).filter_by(
                exchange=self.exchange_name,
                symbol=symbol,
                strategy=strategy,
                status='open',
            ).first()

            if pos:
                pos.status = 'closed'
                pos.amount = 0
                pos.realized_pnl += realized_pnl
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"关闭持仓失败: {e}")
        finally:
            session.close()
