"""
加密货币量化交易系统 - 启动入口

用法：
  python run.py                    # 启动 API 服务
  python run.py --reload           # 开发模式（自动重载）
"""
import os
import socket
import sys

import uvicorn

from app.config import Config


def check_port_available(port: int) -> bool:
    """检查端口是否可用"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


if __name__ == "__main__":
    port = Config.API_PORT

    if not check_port_available(port):
        print(f"\n[ERROR] 端口 {port} 已被占用")
        sys.exit(1)

    reload = "--reload" in sys.argv or os.getenv("UVICORN_RELOAD", "false").lower() in ("true", "1")

    print(f"\n  Crypto Quant v1.0.0")
    print(f"  API: http://localhost:{port}")
    print(f"  Docs: http://localhost:{port}/docs")
    print(f"  Mode: {'development' if reload else 'production'}\n")

    uvicorn.run(
        "app.main:app",
        host=Config.API_HOST,
        port=port,
        reload=reload,
    )
