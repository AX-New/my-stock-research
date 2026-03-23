"""
加密货币量化交易系统 - 日志模块

自包含日志，不依赖外部项目。
控制台 + 文件双输出，文件自动轮转。
"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from app.config import Config

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"

os.makedirs(Config.LOG_DIR, exist_ok=True)

# Windows 控制台强制 UTF-8
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# 控制台输出
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.basicConfig(level=getattr(logging, Config.LOG_LEVEL), handlers=[_console_handler])

# 文件输出（10MB 轮转，保留 5 个备份）
_file_handler = RotatingFileHandler(
    os.path.join(Config.LOG_DIR, "crypto.log"),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(_file_handler)


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的 logger"""
    return logging.getLogger(name)
