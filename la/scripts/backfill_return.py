"""
LA系统 - 收益率回填脚本

从 market_daily 读取行情数据，计算选股后 T+N 收益率，回填到 la_pick。

收益率计算: return_t{N} = (close_at_T+N - buy_price) / buy_price * 100
T+N 为评估日后第 N 个交易日（非自然日）。

脚本可反复运行，只填充尚未回填的周期（字段为 NULL 且已有行情数据）。
同时更新 latest_price / latest_date 为最新可用收盘价。

用法:
    python la/scripts/backfill_return.py                       # 回填所有未完成记录
    python la/scripts/backfill_return.py --eval-date 20260312  # 只回填指定日期
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text
from app.database import engine
from app.logger import get_logger

log = get_logger("la_backfill_return")

# T+N 周期定义，与 la_pick 表字段对应
RETURN_PERIODS = [1, 2, 3, 5, 10, 20, 30, 60, 90, 120]


def get_eval_dates(conn, eval_date: str | None) -> list[str]:
    """获取需要回填的 eval_date 列表（至少有一个 return 字段为 NULL）"""
    conditions = []
    params = {}

    # 任意一个 return 字段为 NULL 就需要回填
    null_checks = " OR ".join(f"return_t{n} IS NULL" for n in RETURN_PERIODS)
    conditions.append(f"({null_checks})")

    if eval_date:
        conditions.append("eval_date = :eval_date")
        params["eval_date"] = eval_date

    sql = f"SELECT DISTINCT eval_date FROM la_pick WHERE {' AND '.join(conditions)} ORDER BY eval_date"
    rows = conn.execute(text(sql), params).fetchall()
    return [row[0] for row in rows]


def get_trading_dates_after(conn, eval_date: str, count: int = 120) -> list[str]:
    """获取 eval_date 之后的交易日列表（从 market_daily 取 distinct trade_date）"""
    sql = """
        SELECT DISTINCT trade_date FROM market_daily
        WHERE trade_date > :eval_date
        ORDER BY trade_date
        LIMIT :cnt
    """
    rows = conn.execute(text(sql), {"eval_date": eval_date, "cnt": count}).fetchall()
    return [row[0] for row in rows]


def backfill_one_eval_date(conn, eval_date: str) -> int:
    """
    回填一个 eval_date 的所有 la_pick 记录。

    Returns: 本次更新的记录数
    """
    # 1. 获取评估日后的交易日历
    trading_dates = get_trading_dates_after(conn, eval_date, count=max(RETURN_PERIODS))
    if not trading_dates:
        log.info(f"[backfill] eval_date={eval_date} | 暂无后续交易日数据，跳过")
        return 0

    # 建立 T+N → trade_date 映射（T+1 对应 trading_dates[0]）
    period_date_map = {}
    for n in RETURN_PERIODS:
        idx = n - 1  # T+1 → index 0
        if idx < len(trading_dates):
            period_date_map[n] = trading_dates[idx]

    if not period_date_map:
        log.info(f"[backfill] eval_date={eval_date} | 交易日不足，跳过")
        return 0

    log.info(f"[backfill] eval_date={eval_date} | 可回填周期: "
             f"{[f'T+{n}={d}' for n, d in period_date_map.items()]}")

    # 2. 获取需要回填的 la_pick 记录
    null_checks = " OR ".join(f"return_t{n} IS NULL" for n in RETURN_PERIODS)
    picks = conn.execute(text(
        f"SELECT id, ts_code, buy_price FROM la_pick "
        f"WHERE eval_date = :eval_date AND ({null_checks})"
    ), {"eval_date": eval_date}).fetchall()

    if not picks:
        return 0

    ts_codes = list(set(row[1] for row in picks))
    target_dates = list(period_date_map.values())

    log.info(f"[backfill] eval_date={eval_date} | 待回填: {len(picks)} 条 | 股票数: {len(ts_codes)}")

    # 3. 批量查询行情数据: {(ts_code, trade_date): close}
    price_map = {}
    # 分批查询避免 IN 子句过大
    batch_size = 200
    for i in range(0, len(ts_codes), batch_size):
        batch_codes = ts_codes[i:i + batch_size]
        placeholders_codes = ",".join(f":c{j}" for j in range(len(batch_codes)))
        placeholders_dates = ",".join(f":d{j}" for j in range(len(target_dates)))

        params = {}
        for j, c in enumerate(batch_codes):
            params[f"c{j}"] = c
        for j, d in enumerate(target_dates):
            params[f"d{j}"] = d

        sql = (f"SELECT ts_code, trade_date, close FROM market_daily "
               f"WHERE ts_code IN ({placeholders_codes}) AND trade_date IN ({placeholders_dates})")
        rows = conn.execute(text(sql), params).fetchall()
        for row in rows:
            price_map[(row[0], row[1])] = row[2]

    log.info(f"[backfill] eval_date={eval_date} | 行情数据点: {len(price_map)}")

    # 4. 逐条计算收益率并更新
    updated = 0
    for pick_id, ts_code, buy_price in picks:
        if not buy_price or buy_price <= 0:
            continue

        updates = {}
        latest_p = None
        latest_d = None

        for n in RETURN_PERIODS:
            if n not in period_date_map:
                continue
            td = period_date_map[n]
            close = price_map.get((ts_code, td))
            if close is not None:
                ret = round((close - buy_price) / buy_price * 100, 2)
                updates[f"return_t{n}"] = ret
                # 记录最远日期的价格作为 latest
                if latest_d is None or td > latest_d:
                    latest_p = close
                    latest_d = td

        if not updates:
            continue

        if latest_p is not None:
            updates["latest_price"] = latest_p
            updates["latest_date"] = latest_d

        # 只更新 NULL 字段，避免覆盖已有值
        set_clauses = []
        params = {"pick_id": pick_id}
        for col, val in updates.items():
            # return_t{N} 只在原值为 NULL 时更新
            if col.startswith("return_t"):
                set_clauses.append(f"{col} = IFNULL({col}, :{col})")
            else:
                set_clauses.append(f"{col} = :{col}")
            params[col] = val

        sql = f"UPDATE la_pick SET {', '.join(set_clauses)} WHERE id = :pick_id"
        conn.execute(text(sql), params)
        updated += 1

    conn.commit()
    log.info(f"[backfill] eval_date={eval_date} | 更新完成: {updated} 条")
    return updated


def main():
    parser = argparse.ArgumentParser(description="LA系统 - 收益率回填")
    parser.add_argument("--eval-date", help="指定评估日期，不传则回填所有")
    args = parser.parse_args()

    with engine.connect() as conn:
        eval_dates = get_eval_dates(conn, args.eval_date)
        if not eval_dates:
            log.info("[backfill] 无需回填的记录")
            print("无需回填的记录")
            return

        log.info(f"[backfill] 待回填日期: {eval_dates}")
        total = 0
        for ed in eval_dates:
            total += backfill_one_eval_date(conn, ed)

        log.info(f"[backfill] 全部完成 | 日期数: {len(eval_dates)} | 总更新: {total} 条")
        print(f"回填完成: {len(eval_dates)} 个日期, {total} 条记录更新")


if __name__ == "__main__":
    main()
