"""API 路由注册"""
from fastapi import APIRouter

from app.api.data import router as data_router
from app.api.strategy import router as strategy_router
from app.api.backtest import router as backtest_router
from app.api.trade import router as trade_router

api_router = APIRouter(prefix="/api")
api_router.include_router(data_router, tags=["Data"])
api_router.include_router(strategy_router, tags=["Strategy"])
api_router.include_router(backtest_router, tags=["Backtest"])
api_router.include_router(trade_router, tags=["Trade"])
