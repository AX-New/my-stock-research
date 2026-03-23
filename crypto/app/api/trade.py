"""交易相关 API"""
from fastapi import APIRouter

from app.api.response import ok, fail
from app.database import SessionLocal
from app.models.trade import CryptoTrade
from app.models.position import CryptoPosition
from sqlalchemy import select

router = APIRouter(prefix="/trade")


@router.get("/positions")
def api_get_positions(exchange: str = None, status: str = "open"):
    """查询持仓列表"""
    session = SessionLocal()
    try:
        query = select(CryptoPosition).where(CryptoPosition.status == status)
        if exchange:
            query = query.where(CryptoPosition.exchange == exchange)
        query = query.order_by(CryptoPosition.updated_at.desc())

        rows = session.execute(query).scalars().all()
        result = []
        for r in rows:
            result.append({
                "id": r.id, "exchange": r.exchange, "symbol": r.symbol,
                "strategy": r.strategy, "side": r.side, "amount": r.amount,
                "avg_price": r.avg_price, "current_price": r.current_price,
                "unrealized_pnl": r.unrealized_pnl,
                "realized_pnl": r.realized_pnl,
                "stop_loss": r.stop_loss, "take_profit": r.take_profit,
                "status": r.status,
                "updated_at": str(r.updated_at),
            })
        return ok(result)
    finally:
        session.close()


@router.get("/history")
def api_get_trades(exchange: str = None, symbol: str = None,
                   limit: int = 50):
    """查询交易历史"""
    session = SessionLocal()
    try:
        query = select(CryptoTrade).order_by(CryptoTrade.trade_time.desc())
        if exchange:
            query = query.where(CryptoTrade.exchange == exchange)
        if symbol:
            query = query.where(CryptoTrade.symbol == symbol)
        query = query.limit(limit)

        rows = session.execute(query).scalars().all()
        result = []
        for r in rows:
            result.append({
                "id": r.id, "exchange": r.exchange, "symbol": r.symbol,
                "side": r.side, "order_type": r.order_type,
                "amount": r.amount, "price": r.price, "cost": r.cost,
                "fee": r.fee, "strategy": r.strategy,
                "status": r.status, "is_paper": r.is_paper,
                "trade_time": str(r.trade_time), "note": r.note,
            })
        return ok(result)
    finally:
        session.close()
