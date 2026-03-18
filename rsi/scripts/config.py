"""stock_rsi 库连接配置（双库：读 my_stock / 写 stock_rsi）"""
import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3307))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root')

# 读: my_stock (生产库)
READ_DB_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/my_stock?charset=utf8mb4"
)

# 写: stock_rsi (RSI研究库)
WRITE_DB_NAME = "stock_rsi"
WRITE_DB_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{WRITE_DB_NAME}?charset=utf8mb4"
)
