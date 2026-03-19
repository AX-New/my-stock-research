"""K线数据加载 — 复用 MACD 研究模块的加载函数

数据来源: my_stock 库
复用: macd/scripts/kline_loader.py
"""
import importlib.util
import os
import sys

# MACD kline_loader 需要项目根目录在 sys.path 中
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

# SW 函数可能不存在
load_sw_kline = getattr(_mod, "load_sw_kline", None)
get_sw_l1_codes = getattr(_mod, "get_sw_l1_codes", None)
