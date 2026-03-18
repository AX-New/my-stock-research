"""简化版日志：控制台 + 文件（scripts.log）"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Windows 控制台强制 UTF-8 输出
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.basicConfig(level=logging.INFO, handlers=[_console_handler])

_file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "research.log"),
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(_file_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
