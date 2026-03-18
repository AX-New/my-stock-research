"""K线数据加载 — 复用 MACD 研究模块的加载函数

数据来源: my_stock 库
复用: research/macd/scripts/kline_loader.py（load_index_kline / load_stock_kline）
本地实现: get_sw_l1_codes（MACD 模块中不存在）

注意: 因为本文件与 MACD 模块同名，用 importlib 显式按路径加载，避免循环导入。
"""
import importlib.util
import os
import sys

from sqlalchemy import text

# MACD kline_loader 需要项目根目录在 sys.path 中（它 import app.logger 等）
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# 用 importlib 显式加载 MACD 的 kline_loader，避免同名循环导入
_macd_kline_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'macd', 'scripts', 'kline_loader.py'
))
_spec = importlib.util.spec_from_file_location("_macd_kline_loader", _macd_kline_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

load_index_kline = _mod.load_index_kline
load_stock_kline = _mod.load_stock_kline
get_all_index_codes = _mod.get_all_index_codes
get_all_stock_codes = _mod.get_all_stock_codes


def get_sw_l1_codes() -> list[str]:
    """获取申万一级行业代码列表（从 my_stock 库查询）"""
    from database import read_engine
    sql = text(
        "SELECT index_code FROM index_classify "
        "WHERE level='L1' AND src='SW2021' ORDER BY index_code"
    )
    with read_engine.connect() as conn:
        result = conn.execute(sql)
        return [row[0] for row in result.fetchall()]
