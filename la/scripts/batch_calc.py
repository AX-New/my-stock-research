"""
批量计算 LA 系统因子和市场指标

用法:
    # 计算全部（la_factor + la_indicator），自动跳过已有日期
    python la/scripts/batch_calc.py

    # 只计算 la_factor
    python la/scripts/batch_calc.py --target factor

    # 只计算 la_indicator
    python la/scripts/batch_calc.py --target indicator

    # 指定日期范围
    python la/scripts/batch_calc.py --start 20240101 --end 20241231

    # 强制重新计算（不跳过已有日期）
    python la/scripts/batch_calc.py --force

    # 出错时停止（默认跳过出错日期继续执行）
    python la/scripts/batch_calc.py --stop-on-error

数据依赖:
    la_factor:    stk_factor_pro(交叉信号) + finance_fina_indicator(质量/成长)
                  + moneyflow(资金) + market_daily(波动率)
                  最早可算日期: stk_factor_pro 第2个交易日（交叉信号需前一天）
    la_indicator: index_dailybasic(估值) + index_daily(技术) + market_daily(涨跌)
                  + moneyflow_mkt_dc(资金) + stock_basic(行业)
                  最早可算日期: stk_factor_pro 第1个交易日
"""

import sys
import os
import argparse
import time
from datetime import datetime

# 项目根添加到 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from app.database import engine
from app.logger import get_logger

log = get_logger("batch_calc_la")


def get_all_trade_dates(start: str = None, end: str = None) -> list[str]:
    """从 stk_factor_pro 获取所有交易日期（升序）"""
    sql = "SELECT DISTINCT trade_date FROM stk_factor_pro"
    conditions = []
    if start:
        conditions.append(f"trade_date >= '{start}'")
    if end:
        conditions.append(f"trade_date <= '{end}'")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY trade_date ASC"

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return [r[0] for r in rows]


def get_existing_dates(table: str) -> set[str]:
    """查询表中已有的 trade_date 集合"""
    sql = f"SELECT DISTINCT trade_date FROM {table}"
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {r[0] for r in rows}


def format_eta(elapsed_sec: float, done: int, total: int) -> str:
    """根据已完成进度估算剩余时间"""
    if done == 0:
        return "计算中..."
    avg = elapsed_sec / done
    remain = avg * (total - done)
    if remain < 60:
        return f"{remain:.0f}s"
    if remain < 3600:
        return f"{remain/60:.1f}min"
    return f"{remain/3600:.1f}h"


def batch_calc_factor(dates: list[str], force: bool, stop_on_error: bool) -> dict:
    """批量计算 la_factor"""
    from app.services.la.factor_service import calc_factor

    # la_factor 需要前一天数据做交叉信号，第一个日期无法计算
    all_dates = get_all_trade_dates()
    if dates and dates[0] == all_dates[0]:
        log.warning(f"[batch_factor] 跳过首个交易日 {dates[0]}（无前一天数据，无法算交叉信号）")
        dates = dates[1:]

    if not force:
        existing = get_existing_dates("la_factor")
        before = len(dates)
        dates = [d for d in dates if d not in existing]
        skipped = before - len(dates)
        if skipped > 0:
            log.info(f"[batch_factor] 跳过已计算日期: {skipped} 天")

    total = len(dates)
    if total == 0:
        log.info("[batch_factor] 无需计算（所有日期已完成）")
        return {"total_dates": 0, "total_records": 0, "errors": []}

    log.info(f"[batch_factor] ========== 开始批量计算 ==========")
    log.info(f"[batch_factor] 日期范围: {dates[0]} ~ {dates[-1]} | 共 {total} 天")

    total_records = 0
    errors = []
    start_time = time.time()

    for i, trade_date in enumerate(dates):
        t0 = time.time()
        try:
            result = calc_factor(trade_date)
            count = result.get("total", 0)
            total_records += count
            elapsed_this = time.time() - t0
            elapsed_total = time.time() - start_time
            eta = format_eta(elapsed_total, i + 1, total)

            log.info(
                f"[batch_factor] 进度: {i+1}/{total} ({(i+1)*100/total:.1f}%) | "
                f"{trade_date} | 写入: {count} 条 | 耗时: {elapsed_this:.1f}s | "
                f"累计: {total_records} 条 | ETA: {eta}"
            )
        except Exception as e:
            elapsed_this = time.time() - t0
            log.error(f"[batch_factor] 出错: {trade_date} | {type(e).__name__}: {e} | 耗时: {elapsed_this:.1f}s")
            errors.append({"date": trade_date, "error": str(e)})
            if stop_on_error:
                log.error(f"[batch_factor] --stop-on-error 已设置，终止执行")
                break

    total_elapsed = time.time() - start_time
    log.info(f"[batch_factor] ========== 计算完成 ==========")
    log.info(
        f"[batch_factor] 总计: {i+1}/{total} 天 | 记录: {total_records} 条 | "
        f"失败: {len(errors)} 天 | 总耗时: {total_elapsed:.1f}s ({total_elapsed/60:.1f}min)"
    )
    if errors:
        log.warning(f"[batch_factor] 失败日期: {[e['date'] for e in errors]}")

    return {"total_dates": i + 1, "total_records": total_records, "errors": errors}


def batch_calc_indicator(dates: list[str], force: bool, stop_on_error: bool) -> dict:
    """批量计算 la_indicator"""
    from app.services.la.indicator_service import calc_indicator

    if not force:
        existing = get_existing_dates("la_indicator")
        before = len(dates)
        dates = [d for d in dates if d not in existing]
        skipped = before - len(dates)
        if skipped > 0:
            log.info(f"[batch_indicator] 跳过已计算日期: {skipped} 天")

    total = len(dates)
    if total == 0:
        log.info("[batch_indicator] 无需计算（所有日期已完成）")
        return {"total_dates": 0, "errors": []}

    log.info(f"[batch_indicator] ========== 开始批量计算 ==========")
    log.info(f"[batch_indicator] 日期范围: {dates[0]} ~ {dates[-1]} | 共 {total} 天")

    errors = []
    start_time = time.time()

    for i, trade_date in enumerate(dates):
        t0 = time.time()
        try:
            result = calc_indicator(trade_date)
            indicator_count = result.get("indicator_count", 0)
            elapsed_this = time.time() - t0
            elapsed_total = time.time() - start_time
            eta = format_eta(elapsed_total, i + 1, total)

            log.info(
                f"[batch_indicator] 进度: {i+1}/{total} ({(i+1)*100/total:.1f}%) | "
                f"{trade_date} | 指标: {indicator_count} 项 | 耗时: {elapsed_this:.1f}s | ETA: {eta}"
            )
        except Exception as e:
            elapsed_this = time.time() - t0
            log.error(f"[batch_indicator] 出错: {trade_date} | {type(e).__name__}: {e} | 耗时: {elapsed_this:.1f}s")
            errors.append({"date": trade_date, "error": str(e)})
            if stop_on_error:
                log.error(f"[batch_indicator] --stop-on-error 已设置，终止执行")
                break

    total_elapsed = time.time() - start_time
    log.info(f"[batch_indicator] ========== 计算完成 ==========")
    log.info(
        f"[batch_indicator] 总计: {i+1}/{total} 天 | "
        f"失败: {len(errors)} 天 | 总耗时: {total_elapsed:.1f}s ({total_elapsed/60:.1f}min)"
    )
    if errors:
        log.warning(f"[batch_indicator] 失败日期: {[e['date'] for e in errors]}")

    return {"total_dates": i + 1, "errors": errors}


def main():
    parser = argparse.ArgumentParser(description="批量计算 LA 系统因子和市场指标")
    parser.add_argument("--target", choices=["factor", "indicator", "both"], default="both",
                        help="计算目标: factor/indicator/both (默认 both)")
    parser.add_argument("--start", type=str, default=None,
                        help="起始日期 YYYYMMDD (默认从 stk_factor_pro 最早日期开始)")
    parser.add_argument("--end", type=str, default=None,
                        help="结束日期 YYYYMMDD (默认到 stk_factor_pro 最新日期)")
    parser.add_argument("--force", action="store_true",
                        help="强制重新计算（不跳过已有日期）")
    parser.add_argument("--stop-on-error", action="store_true",
                        help="遇到错误时停止（默认跳过出错日期继续执行）")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info(f"[batch_calc_la] 启动 | target={args.target} | "
             f"start={args.start or '自动'} | end={args.end or '自动'} | "
             f"force={args.force} | stop_on_error={args.stop_on_error}")
    log.info("=" * 70)

    # 获取所有交易日期
    dates = get_all_trade_dates(args.start, args.end)
    if not dates:
        log.error("[batch_calc_la] 未找到交易日期，请检查 stk_factor_pro 表是否有数据")
        return

    log.info(f"[batch_calc_la] stk_factor_pro 交易日范围: {dates[0]} ~ {dates[-1]} | 共 {len(dates)} 天")

    overall_start = time.time()
    results = {}

    # 计算 la_factor
    if args.target in ("factor", "both"):
        results["factor"] = batch_calc_factor(dates[:], args.force, args.stop_on_error)

    # 计算 la_indicator
    if args.target in ("indicator", "both"):
        results["indicator"] = batch_calc_indicator(dates[:], args.force, args.stop_on_error)

    # 总结
    overall_elapsed = time.time() - overall_start
    log.info("=" * 70)
    log.info(f"[batch_calc_la] 全部完成 | 总耗时: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f}min)")
    for key, val in results.items():
        error_count = len(val.get("errors", []))
        if key == "factor":
            log.info(f"  {key}: {val['total_dates']} 天, {val['total_records']} 条记录, {error_count} 失败")
        else:
            log.info(f"  {key}: {val['total_dates']} 天, {error_count} 失败")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
