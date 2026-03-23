"""
加密货币量化交易系统 - FastAPI 应用

独立部署的 Web 服务，提供：
  - 数据同步 API（全量存储，无需复权）
  - 策略计算 API
  - 回测 API
  - 交易管理 API
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import init_db
from app.api import api_router
from app.logger import get_logger

logger = get_logger("crypto.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动：初始化数据库
    logger.info("加密货币量化交易系统启动中...")
    init_db()
    logger.info("数据库初始化完成")
    yield
    # 关闭
    logger.info("系统已关闭")


app = FastAPI(
    title="Crypto Quant",
    description="加密货币量化交易系统 - 独立部署版",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(api_router)


@app.get("/")
def root():
    return {
        "name": "Crypto Quant",
        "version": "1.0.0",
        "description": "加密货币量化交易系统",
    }


@app.get("/health")
def health():
    return {"status": "ok"}
