"""
交易执行引擎

支持模拟交易（paper trading）和实盘交易（live trading）。
管理持仓、止损止盈、信号执行。
"""
from datetime import datetime

from app.config import Config
from app.database import SessionLocal, init_db
from app.models.signal import CryptoSignal
from app.models.trade import CryptoTrade
from app.models.position import CryptoPosition
from app.services.exchange_client import (
    create_exchange, fetch_ticker, fetch_balance,
    place_order, get_exchange_name,
)
from app.logger import get_logger

logger = get_logger("crypto.trader")


class TradeEngine:
    """交易执行引擎"""

    def __init__(self, exchange_name: str = None, is_paper: bool = True,
                 sandbox: bool = False):
        self.is_paper = is_paper
        self.exchange = create_exchange(exchange_name, sandbox=sandbox)
        self.exchange_name = get_exchange_name(self.exchange)

        # 模拟账户余额
        self.paper_balance = {
            "USDT": {"total": 100000, "free": 100000, "used": 0},
        }

        init_db()
        logger.info(f"交易引擎已创建: exchange={self.exchange_name}, "
                    f"模式={'模拟' if is_paper else '实盘'}")

    def get_balance(self) -> dict:
        """获取账户余额"""
        if self.is_paper:
            return self.paper_balance
        return fetch_balance(self.exchange)

    def get_position(self, symbol: str, strategy: str) -> CryptoPosition | None:
        """获取持仓"""
        session = SessionLocal()
        try:
            pos = session.query(CryptoPosition).filter_by(
                exchange=self.exchange_name,
                symbol=symbol,
                strategy=strategy,
                status="open",
            ).first()
            if pos:
                session.expunge(pos)
            return pos
        finally:
            session.close()

    def execute_signal(self, symbol: str, signal: str, reason: str,
                       strategy: str, price: float,
                       indicators: str = "{}") -> dict:
        """执行交易信号"""
        if signal == "HOLD":
            return {"action": "HOLD", "reason": reason}

        signal_id = self._save_signal(symbol, signal, reason, strategy,
                                      price, indicators)

        position = self.get_position(symbol, strategy)

        if signal == "BUY":
            if position and position.amount > 0:
                logger.info(f"已有持仓，跳过买入: {symbol} {strategy}")
                return {"action": "SKIP", "reason": "已有持仓"}
            return self._execute_buy(symbol, strategy, price, signal_id, reason)

        elif signal == "SELL":
            if not position or position.amount <= 0:
                logger.info(f"无持仓，跳过卖出: {symbol} {strategy}")
                return {"action": "SKIP", "reason": "无持仓"}
            return self._execute_sell(symbol, strategy, price, position,
                                     signal_id, reason)

        return {"action": "UNKNOWN", "signal": signal}

    def check_stop_loss_take_profit(self, symbol: str, strategy: str,
                                    current_price: float) -> dict:
        """检查止损止盈"""
        position = self.get_position(symbol, strategy)
        if not position or position.amount <= 0:
            return {"action": "NONE"}

        if position.stop_loss and current_price <= position.stop_loss:
            logger.warning(f"触发止损: {symbol} price={current_price:.2f}, "
                          f"stop={position.stop_loss:.2f}")
            return self._execute_sell(
                symbol, strategy, current_price, position, None,
                f"止损触发(价格{current_price:.2f} <= {position.stop_loss:.2f})")

        if position.take_profit and current_price >= position.take_profit:
            logger.info(f"触发止盈: {symbol} price={current_price:.2f}, "
                       f"tp={position.take_profit:.2f}")
            return self._execute_sell(
                symbol, strategy, current_price, position, None,
                f"止盈触发(价格{current_price:.2f} >= {position.take_profit:.2f})")

        return {"action": "NONE"}

    def _execute_buy(self, symbol, strategy, price, signal_id, reason):
        """执行买入"""
        balance = self.get_balance()
        usdt_free = balance.get("USDT", {}).get("free", 0)
        buy_amount_usdt = usdt_free * Config.MAX_POSITION_RATIO

        if buy_amount_usdt < 10:
            return {"action": "SKIP", "reason": f"余额不足({usdt_free:.2f} USDT)"}

        amount = buy_amount_usdt / price
        order_id = None
        actual_price = price
        fee = buy_amount_usdt * 0.001

        if self.is_paper:
            self.paper_balance["USDT"]["free"] -= buy_amount_usdt
            self.paper_balance["USDT"]["used"] += buy_amount_usdt
            base = symbol.split("/")[0]
            if base not in self.paper_balance:
                self.paper_balance[base] = {"total": 0, "free": 0, "used": 0}
            self.paper_balance[base]["total"] += amount
            self.paper_balance[base]["free"] += amount
            order_id = f"PAPER-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            logger.info(f"[模拟] 买入 {amount:.6f} {symbol} @ {price:.2f}")
        else:
            try:
                order = place_order(self.exchange, symbol, "buy", amount)
                order_id = order.get("id", "")
                actual_price = order.get("average", price)
                fee = order.get("fee", {}).get("cost", fee)
                amount = order.get("filled", amount)
            except Exception as e:
                logger.error(f"买入失败: {e}")
                return {"action": "FAILED", "reason": str(e)}

        stop_loss = actual_price * (1 - Config.STOP_LOSS_PCT)
        take_profit = actual_price * (1 + Config.TAKE_PROFIT_PCT)

        self._save_trade(symbol, "buy", "market", amount, actual_price,
                         buy_amount_usdt, fee, strategy, signal_id, order_id, reason)
        self._save_position(symbol, strategy, "long", amount, actual_price,
                            stop_loss, take_profit)

        return {
            "action": "BUY", "symbol": symbol, "amount": amount,
            "price": actual_price, "cost": buy_amount_usdt,
            "stop_loss": stop_loss, "take_profit": take_profit,
            "order_id": order_id,
        }

    def _execute_sell(self, symbol, strategy, price, position, signal_id, reason):
        """执行卖出"""
        amount = position.amount
        sell_value = amount * price
        fee = sell_value * 0.001

        order_id = None
        actual_price = price

        if self.is_paper:
            self.paper_balance["USDT"]["free"] += sell_value - fee
            self.paper_balance["USDT"]["used"] -= position.avg_price * amount
            base = symbol.split("/")[0]
            if base in self.paper_balance:
                self.paper_balance[base]["total"] -= amount
                self.paper_balance[base]["free"] -= amount
            order_id = f"PAPER-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            logger.info(f"[模拟] 卖出 {amount:.6f} {symbol} @ {price:.2f}")
        else:
            try:
                order = place_order(self.exchange, symbol, "sell", amount)
                order_id = order.get("id", "")
                actual_price = order.get("average", price)
                fee = order.get("fee", {}).get("cost", fee)
            except Exception as e:
                logger.error(f"卖出失败: {e}")
                return {"action": "FAILED", "reason": str(e)}

        pnl = (actual_price - position.avg_price) * amount - fee
        pnl_pct = (actual_price - position.avg_price) / position.avg_price * 100

        self._save_trade(symbol, "sell", "market", amount, actual_price,
                         sell_value, fee, strategy, signal_id, order_id,
                         f"{reason} | PnL: {pnl:+.2f} ({pnl_pct:+.1f}%)")
        self._close_position(symbol, strategy, pnl)

        return {
            "action": "SELL", "symbol": symbol, "amount": amount,
            "price": actual_price, "value": sell_value,
            "pnl": pnl, "pnl_pct": pnl_pct, "order_id": order_id,
        }

    # ---- 数据库操作 ----

    def _save_signal(self, symbol, signal, reason, strategy, price, indicators):
        session = SessionLocal()
        try:
            sig = CryptoSignal(
                exchange=self.exchange_name, symbol=symbol, timeframe="",
                strategy=strategy, signal=signal, signal_time=datetime.now(),
                price=price, reason=reason, indicators=indicators,
            )
            session.add(sig)
            session.commit()
            return sig.id
        except Exception as e:
            session.rollback()
            logger.error(f"保存信号失败: {e}")
            return 0
        finally:
            session.close()

    def _save_trade(self, symbol, side, order_type, amount, price,
                    cost, fee, strategy, signal_id, order_id, note):
        session = SessionLocal()
        try:
            trade = CryptoTrade(
                exchange=self.exchange_name, symbol=symbol, side=side,
                order_type=order_type, amount=amount, price=price,
                cost=cost, fee=fee, strategy=strategy,
                signal_id=signal_id, order_id=order_id or "",
                status="filled", trade_time=datetime.now(),
                is_paper=1 if self.is_paper else 0, note=note,
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
        session = SessionLocal()
        try:
            pos = session.query(CryptoPosition).filter_by(
                exchange=self.exchange_name, symbol=symbol, strategy=strategy,
            ).first()

            if pos:
                pos.side = side
                pos.amount = amount
                pos.avg_price = price
                pos.current_price = price
                pos.stop_loss = stop_loss
                pos.take_profit = take_profit
                pos.status = "open"
            else:
                pos = CryptoPosition(
                    exchange=self.exchange_name, symbol=symbol,
                    strategy=strategy, side=side, amount=amount,
                    avg_price=price, current_price=price,
                    stop_loss=stop_loss, take_profit=take_profit, status="open",
                )
                session.add(pos)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"保存持仓失败: {e}")
        finally:
            session.close()

    def _close_position(self, symbol, strategy, realized_pnl):
        session = SessionLocal()
        try:
            pos = session.query(CryptoPosition).filter_by(
                exchange=self.exchange_name, symbol=symbol,
                strategy=strategy, status="open",
            ).first()
            if pos:
                pos.status = "closed"
                pos.amount = 0
                pos.realized_pnl += realized_pnl
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"关闭持仓失败: {e}")
        finally:
            session.close()
