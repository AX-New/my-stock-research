"""
ML 热度分析配置

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
# ML 训练参数
# ============================================================
# 数据时间范围
START_DATE = '2025-03-15'
END_DATE = '2026-03-20'

# 训练/测试切分日期（约 80% 训练，20% 测试）
SPLIT_DATE = '2026-01-01'

# 预测目标：未来 N 天收益率
FORWARD_DAYS = [3, 5, 10]

# 最小数据量要求（低于此数的股票跳过）
MIN_STOCK_DAYS = 30

# 输出目录
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'report')
