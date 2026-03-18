"""牛熊周期数据 — 从 MACD 研究模块复用

所有技术指标分析都必须结合牛熊周期做判断，这是 MACD 研究得出的最重要结论：
- 同一信号在牛市和熊市下表现完全相反
- 不判断牛熊就做交易 = 赌博

数据来源: research/macd/scripts/bull_bear_phases.py
文档: report/01-a-share-bull-bear-cycles.md

注意: 因为本文件与 MACD 模块同名，用 importlib 显式按路径加载，避免循环导入。
"""
import importlib.util
import os

# 用 importlib 显式加载 MACD 的 bull_bear_phases，避免同名循环导入
_macd_bb_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'macd', 'scripts', 'bull_bear_phases.py'
))
_spec = importlib.util.spec_from_file_location("_macd_bull_bear_phases", _macd_bb_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

SH_PHASES = _mod.SH_PHASES
SH_TOPS = _mod.SH_TOPS
SH_BOTTOMS = _mod.SH_BOTTOMS
get_phase = _mod.get_phase
tag_trend = _mod.tag_trend
find_nearest_date = _mod.find_nearest_date
