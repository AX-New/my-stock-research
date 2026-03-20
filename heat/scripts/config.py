"""
热度策略脚本配置

数据库连接：
  - my_stock: 本地（A股行情/基本面），port 3307
  - my_trend: 腾讯云 SSH 隧道（port 3310 → 腾讯云 3306）

前提条件：
  - SSH 隧道已启动（Desktop/Tssh-tunnel.bat）
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# my_stock 数据库配置（本地，A股行情/基本面）
# ============================================================
MY_STOCK_HOST = os.getenv('MYSQL_HOST', '127.0.0.1')
MY_STOCK_PORT = int(os.getenv('MYSQL_PORT', 3307))
MY_STOCK_USER = os.getenv('MYSQL_USER', 'root')
MY_STOCK_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root')
MY_STOCK_DB_URI = (
    f"mysql+pymysql://{MY_STOCK_USER}:{MY_STOCK_PASSWORD}"
    f"@{MY_STOCK_HOST}:{MY_STOCK_PORT}/my_stock?charset=utf8mb4"
)

# ============================================================
# my_trend 数据库配置（腾讯云 SSH 隧道，port 3310 → 腾讯云 3306）
# 前提：本地 SSH 隧道已启动（Desktop/Tssh-tunnel.bat）
# ============================================================
MY_TREND_HOST = os.getenv('MY_TREND_HOST', '127.0.0.1')
MY_TREND_PORT = int(os.getenv('MY_TREND_PORT', 3310))
MY_TREND_USER = os.getenv('MY_TREND_USER', 'root')
MY_TREND_PASSWORD = os.getenv('MY_TREND_PASSWORD', 'root')
MY_TREND_DB_URI = (
    f"mysql+pymysql://{MY_TREND_USER}:{MY_TREND_PASSWORD}"
    f"@{MY_TREND_HOST}:{MY_TREND_PORT}/my_trend?charset=utf8mb4"
)

# ============================================================
# 回测默认参数
# ============================================================
INITIAL_CAPITAL = 1_000_000      # 初始资金 100 万
START_DATE = '2025-03-15'        # 回测起始日期
END_DATE = '2026-03-19'          # 回测结束日期

# ============================================================
# 报告输出目录（OrderBasedEngine 报告存放位置）
# ============================================================
REPORT_OUTPUT_DIR = os.getenv(
    'REPORT_OUTPUT_DIR',
    'F:/projects/my-stock-research/backtest/output'
)

# ============================================================
# my-stock 项目根目录（用于调用 run_backtest.py CLI）
# ============================================================
MY_STOCK_PROJECT = os.getenv('MY_STOCK_PROJECT', 'F:/projects/my-stock')
