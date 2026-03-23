"""
回测引擎

基于历史K线数据回测策略表现。
支持止损止盈、手续费建模、多维度统计指标。
"""
import numpy as np
import pandas as pd

from app.config import Config
from app.strategies import get_strategy
from app.logger import get_logger

logger = get_logger("crypto.backtest")


class BacktestEngine:
    """回测引擎"""

    def __init__(self, initial_capital: float = 100000,
                 commission: float = 0.001,
                 stop_loss: float = None,
                 take_profit: float = None):
        """
        Args:
            initial_capital: 初始资金（USDT）
            commission: 手续费率（默认 0.1%）
            stop_loss: 止损比例
            take_profit: 止盈比例
        """
        self.initial_capital = initial_capital
        self.commission = commission
        self.stop_loss = stop_loss or Config.STOP_LOSS_PCT
        self.take_profit = take_profit or Config.TAKE_PROFIT_PCT

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
        position = 0.0
        entry_price = 0.0
        trades = []
        equity_curve = []

        for i in range(50, len(df)):
            current_data = df.iloc[:i + 1].copy()
            current_price = current_data["close"].iloc[-1]
            current_time = current_data.index[-1]

            # 当前权益
            equity = capital + position * current_price
            equity_curve.append({
                "time": current_time,
                "equity": equity,
                "price": current_price,
                "position": position,
            })

            # 检查止损止盈
            if position > 0:
                pnl_pct = (current_price - entry_price) / entry_price

                # 止损
                if pnl_pct <= -self.stop_loss:
                    sell_value = position * current_price
                    fee = sell_value * self.commission
                    capital += sell_value - fee
                    trades.append({
                        "time": current_time, "side": "sell",
                        "price": current_price, "amount": position,
                        "cost": sell_value, "fee": fee,
                        "reason": f"止损({pnl_pct * 100:.1f}%)",
                        "pnl": (current_price - entry_price) * position - fee,
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
                        "time": current_time, "side": "sell",
                        "price": current_price, "amount": position,
                        "cost": sell_value, "fee": fee,
                        "reason": f"止盈({pnl_pct * 100:.1f}%)",
                        "pnl": (current_price - entry_price) * position - fee,
                    })
                    position = 0.0
                    entry_price = 0.0
                    continue

            # 策略信号
            signal, reason = strategy.compute_signal(current_data)

            if signal == "BUY" and position == 0:
                buy_cost = capital * 0.95
                fee = buy_cost * self.commission
                buy_amount = (buy_cost - fee) / current_price
                position = buy_amount
                entry_price = current_price
                capital -= buy_cost
                trades.append({
                    "time": current_time, "side": "buy",
                    "price": current_price, "amount": buy_amount,
                    "cost": buy_cost, "fee": fee,
                    "reason": reason, "pnl": 0,
                })

            elif signal == "SELL" and position > 0:
                sell_value = position * current_price
                fee = sell_value * self.commission
                pnl = (current_price - entry_price) * position - fee
                capital += sell_value - fee
                trades.append({
                    "time": current_time, "side": "sell",
                    "price": current_price, "amount": position,
                    "cost": sell_value, "fee": fee,
                    "reason": reason, "pnl": pnl,
                })
                position = 0.0
                entry_price = 0.0

        # 回测结束：强制平仓
        if position > 0:
            last_price = df["close"].iloc[-1]
            sell_value = position * last_price
            fee = sell_value * self.commission
            pnl = (last_price - entry_price) * position - fee
            capital += sell_value - fee
            trades.append({
                "time": df.index[-1], "side": "sell",
                "price": last_price, "amount": position,
                "cost": sell_value, "fee": fee,
                "reason": "回测结束平仓", "pnl": pnl,
            })

        result = self._compute_stats(trades, equity_curve, df)
        result["strategy"] = strategy.name
        result["trades"] = trades
        return result

    def _compute_stats(self, trades: list, equity_curve: list,
                       df: pd.DataFrame) -> dict:
        """计算回测统计指标"""
        if not equity_curve:
            return {"error": "无交易数据"}

        eq_df = pd.DataFrame(equity_curve)

        final_equity = eq_df["equity"].iloc[-1]
        total_return = (final_equity - self.initial_capital) / self.initial_capital
        buy_hold_return = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0]

        days = (eq_df["time"].iloc[-1] - eq_df["time"].iloc[0]).total_seconds() / 86400
        annual_return = (1 + total_return) ** (365 / max(days, 1)) - 1 if days > 0 else 0

        # 最大回撤
        eq_df["peak"] = eq_df["equity"].cummax()
        eq_df["drawdown"] = (eq_df["equity"] - eq_df["peak"]) / eq_df["peak"]
        max_drawdown = eq_df["drawdown"].min()

        # 夏普比率（无风险利率 3%）
        eq_df["daily_return"] = eq_df["equity"].pct_change()
        sharpe = 0
        if eq_df["daily_return"].std() > 0:
            sharpe = ((eq_df["daily_return"].mean() * 252 - 0.03)
                      / (eq_df["daily_return"].std() * np.sqrt(252)))

        # 交易统计
        sell_trades = [t for t in trades if t["side"] == "sell"]
        win_trades = [t for t in sell_trades if t["pnl"] > 0]
        lose_trades = [t for t in sell_trades if t["pnl"] < 0]

        total_trades = len(sell_trades)
        win_rate = len(win_trades) / total_trades if total_trades > 0 else 0

        avg_win = np.mean([t["pnl"] for t in win_trades]) if win_trades else 0
        avg_loss = np.mean([t["pnl"] for t in lose_trades]) if lose_trades else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

        total_fee = sum(t["fee"] for t in trades)

        return {
            "initial_capital": self.initial_capital,
            "final_equity": round(final_equity, 2),
            "total_return": round(total_return * 100, 2),
            "annual_return": round(annual_return * 100, 2),
            "buy_hold_return": round(buy_hold_return * 100, 2),
            "max_drawdown": round(max_drawdown * 100, 2),
            "sharpe_ratio": round(sharpe, 3),
            "total_trades": total_trades,
            "win_rate": round(win_rate * 100, 1),
            "avg_win": round(float(avg_win), 2),
            "avg_loss": round(float(avg_loss), 2),
            "profit_factor": round(profit_factor, 3),
            "total_fee": round(total_fee, 2),
            "days": round(days, 1),
        }
