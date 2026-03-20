#!/usr/bin/env python3
"""
MACD 经典策略参数优化

使用 my-stock 优化引擎 (run_callable_optimization) 对 MACD 参数进行网格搜索

参数空间:
    fast_period:   EMA 快线周期 [6, 8, 10, 12, 14, 16]
    slow_period:   EMA 慢线周期 [20, 22, 24, 26, 28, 30, 35, 40]
    signal_period: DEA 信号线周期 [7, 8, 9, 10, 11]
    总组合: 6 × 8 × 5 = 240 组 (过滤 fast >= slow 后更少)

优化目标:
    Sharpe Ratio（考虑佣金0.03%、印花税0.1%、滑点0.2%）

测试股票:
    8 只蓝筹股，涵盖消费/金融/能源/医药，取各股平均指标

测试区间:
    2015-01-01 至 2025-12-31 (覆盖完整牛熊周期)

用法:
    cd F:/projects/my-stock-research
    python macd/scripts/macd_optimizer.py

输出:
    macd/scripts/best_params.json
    macd/scripts/optimization_results.csv
    macd/scripts/top20_results.csv
"""
import os
import sys
import json
import time
import logging
import itertools
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- 路径设置 ----
_SCRIPT_DIR = Path(__file__).resolve().parent
_RESEARCH_ROOT = _SCRIPT_DIR.parents[1]   # my-stock-research
_MY_STOCK_ROOT = Path('F:/projects/my-stock')

# 研究项目根目录
sys.path.insert(0, str(_RESEARCH_ROOT))
# my-stock 项目 (用于导入优化引擎)
sys.path.insert(0, str(_MY_STOCK_ROOT))

# ---- 本地模块 ----
sys.path.insert(0, str(_SCRIPT_DIR))
from macd_strategy_backtest import load_stock_qfq, run_portfolio_backtest

# ---- 日志配置 ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# ================================================================
# 配置
# ================================================================

# 测试股票池 (8只蓝筹, 覆盖主要行业)
TEST_STOCKS = [
    '600519.SH',   # 贵州茅台 - 消费白酒
    '000858.SZ',   # 五粮液   - 消费白酒
    '600036.SH',   # 招商银行 - 银行
    '601318.SH',   # 中国平安 - 保险/金融
    '000001.SZ',   # 平安银行 - 银行
    '600000.SH',   # 浦发银行 - 银行
    '600028.SH',   # 中国石化 - 能源
    '600276.SH',   # 恒瑞医药 - 医药
]

# 回测配置
START_DATE = '2015-01-01'
END_DATE   = '2025-12-31'
INITIAL_CAPITAL = 1_000_000

# 交易成本 (真实 A 股成本)
COMMISSION_RATE = 0.0003    # 佣金 0.03%, 双边
MIN_COMMISSION  = 5.0       # 最低佣金 5 元
STAMP_DUTY_RATE = 0.001     # 印花税 0.1%, 仅卖出
SLIPPAGE_RATE   = 0.002     # 滑点 0.2%, 双边

# 参数搜索空间
PARAM_GRID = {
    'fast_period':   [6, 8, 10, 12, 14, 16],
    'slow_period':   [20, 22, 24, 26, 28, 30, 35, 40],
    'signal_period': [7, 8, 9, 10, 11],
}

# 输出目录
OUTPUT_DIR = _SCRIPT_DIR


# ================================================================
# 数据预加载
# ================================================================

def preload_stock_data(stocks: list, start_date: str, end_date: str) -> dict:
    """
    一次性加载所有股票数据 (前复权日线), 避免优化时重复 IO

    返回: {ts_code: DataFrame}
    """
    logger.info(f"预加载 {len(stocks)} 只股票数据 ({start_date} ~ {end_date})...")
    stock_data = {}
    for ts in stocks:
        try:
            df = load_stock_qfq(ts, start_date, end_date)
            if df.empty:
                logger.warning(f"  {ts}: 无数据，跳过")
                continue
            stock_data[ts] = df
            logger.info(f"  {ts}: {len(df)} 条记录")
        except Exception as e:
            logger.warning(f"  {ts}: 加载失败 ({e})，跳过")
    logger.info(f"数据加载完成: {len(stock_data)}/{len(stocks)} 只股票有效")
    return stock_data


# ================================================================
# 优化引擎接入
# ================================================================

def import_optimizer():
    """
    从 my-stock 导入 run_callable_optimization 优化引擎

    如导入失败 (环境问题), 自动降级为本地实现
    """
    try:
        from app.services.optimizer_service import run_callable_optimization
        logger.info("使用 my-stock optimizer_service.run_callable_optimization")
        return run_callable_optimization
    except ImportError as e:
        logger.warning(f"无法导入 my-stock 优化引擎: {e}")
        logger.warning("降级为本地网格搜索实现")
        return _local_grid_search


def _local_grid_search(strategy_fn, param_grid: dict, metric: str = 'sharpe', n_top: int = 20):
    """
    本地网格搜索 (my-stock optimizer 的等价实现, 用于降级)

    与 run_callable_optimization 接口完全兼容
    """
    keys = list(param_grid.keys())
    combinations = [dict(zip(keys, v)) for v in itertools.product(*param_grid.values())]

    logger.info(f"[网格搜索] 总组合数: {len(combinations)}, 优化指标: {metric}")
    results = []

    for idx, params in enumerate(combinations):
        try:
            metrics = strategy_fn(params)
            if isinstance(metrics, dict) and metric in metrics:
                results.append({'parameters': params, 'metrics': metrics})
        except Exception as e:
            logger.error(f"[网格搜索] 参数 {params} 执行失败: {e}")

        if (idx + 1) % 20 == 0:
            logger.info(f"[网格搜索] 进度: {idx + 1}/{len(combinations)}")

    results.sort(key=lambda x: x['metrics'].get(metric, float('-inf')), reverse=True)
    logger.info(f"[网格搜索] 完成，有效结果: {len(results)}")

    if results:
        best = results[0]
        logger.info(f"[网格搜索] 最优参数: {best['parameters']}, "
                    f"{metric}={best['metrics'].get(metric):.4f}")

    return results[:n_top]


# ================================================================
# 主优化流程
# ================================================================

def build_strategy_fn(stock_data: dict) -> callable:
    """
    构建策略函数 (闭包), 复用预加载数据

    strategy_fn 签名: fn(params: dict) -> dict
    返回的 dict 至少包含: sharpe, annual_return, max_drawdown, win_rate
    """

    def strategy_fn(params: dict) -> dict:
        fast   = int(params['fast_period'])
        slow   = int(params['slow_period'])
        signal = int(params['signal_period'])

        # 过滤无效参数: 快线必须小于慢线, 且保留一定间距
        if fast >= slow - 4:
            return {'sharpe': -99, 'annual_return': -99, 'max_drawdown': 100, 'win_rate': 0}

        metrics = run_portfolio_backtest(
            stock_data=stock_data,
            fast_period=fast,
            slow_period=slow,
            signal_period=signal,
            start_date=START_DATE,
            end_date=END_DATE,
            initial_capital=INITIAL_CAPITAL,
            commission_rate=COMMISSION_RATE,
            min_commission=MIN_COMMISSION,
            stamp_duty_rate=STAMP_DUTY_RATE,
            slippage_rate=SLIPPAGE_RATE,
        )

        if metrics is None:
            return {'sharpe': -99, 'annual_return': -99, 'max_drawdown': 100, 'win_rate': 0}

        return metrics

    return strategy_fn


def save_results(results: list, output_dir: Path) -> tuple[dict, dict]:
    """
    保存优化结果

    保存:
        best_params.json          最优参数
        optimization_results.csv  全部结果
        top20_results.csv         Top 20

    返回: (best_params, best_metrics)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if not results:
        logger.warning("无优化结果可保存")
        return {}, {}

    # 整理为 DataFrame
    rows = []
    for r in results:
        row = dict(r['parameters'])
        row.update(r['metrics'])
        rows.append(row)

    df = pd.DataFrame(rows)
    metric_col = 'sharpe'
    df = df.sort_values(metric_col, ascending=False).reset_index(drop=True)

    # 最优参数
    best_row = df.iloc[0]
    best_params = {
        'fast_period':   int(best_row['fast_period']),
        'slow_period':   int(best_row['slow_period']),
        'signal_period': int(best_row['signal_period']),
    }
    best_metrics = {
        'sharpe':         float(best_row.get('sharpe', 0)),
        'annual_return':  float(best_row.get('annual_return', 0)),
        'max_drawdown':   float(best_row.get('max_drawdown', 0)),
        'total_return':   float(best_row.get('total_return', 0)),
        'win_rate':       float(best_row.get('win_rate', 0)),
        'profit_loss_ratio': float(best_row.get('profit_loss_ratio', 0)),
        'total_trades':   int(best_row.get('total_trades', 0)),
    }

    # 保存文件
    bp_path = output_dir / 'best_params.json'
    with open(bp_path, 'w', encoding='utf-8') as f:
        json.dump({'best_params': best_params, 'best_metrics': best_metrics,
                   'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                   'config': {
                       'stocks': TEST_STOCKS,
                       'start_date': START_DATE,
                       'end_date': END_DATE,
                       'commission_rate': COMMISSION_RATE,
                       'stamp_duty_rate': STAMP_DUTY_RATE,
                       'slippage_rate': SLIPPAGE_RATE,
                   }}, f, indent=2, ensure_ascii=False)
    logger.info(f"最优参数 → {bp_path}")

    all_path = output_dir / 'optimization_results.csv'
    df.to_csv(all_path, index=False, encoding='utf-8-sig')
    logger.info(f"全部结果 → {all_path} ({len(df)} 行)")

    top20_path = output_dir / 'top20_results.csv'
    df.head(20).to_csv(top20_path, index=False, encoding='utf-8-sig')
    logger.info(f"Top 20 → {top20_path}")

    return best_params, best_metrics


def print_summary(best_params: dict, best_metrics: dict, df_top20: pd.DataFrame):
    """打印优化结果摘要"""
    logger.info("=" * 65)
    logger.info("MACD 策略参数优化结果摘要")
    logger.info("=" * 65)
    logger.info(f"最优参数: EMA({best_params['fast_period']},{best_params['slow_period']},"
                f"{best_params['signal_period']})")
    logger.info(f"  Sharpe Ratio:  {best_metrics.get('sharpe', 0):.4f}")
    logger.info(f"  年化收益率:    {best_metrics.get('annual_return', 0):.2f}%")
    logger.info(f"  最大回撤:      {best_metrics.get('max_drawdown', 0):.2f}%")
    logger.info(f"  总收益率:      {best_metrics.get('total_return', 0):.2f}%")
    logger.info(f"  胜率:          {best_metrics.get('win_rate', 0):.1f}%")
    logger.info(f"  盈亏比:        {best_metrics.get('profit_loss_ratio', 0):.2f}")
    logger.info(f"  平均交易次数:  {best_metrics.get('total_trades', 0)}")
    logger.info("=" * 65)

    if df_top20 is not None and len(df_top20) > 0:
        display_cols = [c for c in
                        ['fast_period', 'slow_period', 'signal_period',
                         'sharpe', 'annual_return', 'max_drawdown', 'win_rate', 'total_trades']
                        if c in df_top20.columns]
        print("\nTop 20 参数组合:")
        print(df_top20[display_cols].head(20).to_string(index=True))


# ================================================================
# 入口
# ================================================================

def main():
    t0 = time.time()
    logger.info("=" * 65)
    logger.info("MACD 经典策略参数优化系统")
    logger.info(f"回测区间: {START_DATE} ~ {END_DATE}")
    logger.info(f"测试股票: {len(TEST_STOCKS)} 只")
    logger.info(f"交易成本: 佣金{COMMISSION_RATE*100:.2f}%  印花税{STAMP_DUTY_RATE*100:.1f}%  "
                f"滑点{SLIPPAGE_RATE*100:.1f}%")
    logger.info(f"参数搜索空间: fast{PARAM_GRID['fast_period']} × "
                f"slow{PARAM_GRID['slow_period']} × signal{PARAM_GRID['signal_period']}")
    logger.info("=" * 65)

    # Step 1: 预加载数据
    stock_data = preload_stock_data(TEST_STOCKS, START_DATE, END_DATE)
    if len(stock_data) < 3:
        logger.error("有效股票数量不足 (< 3)，退出")
        sys.exit(1)

    # Step 2: 构建策略函数
    strategy_fn = build_strategy_fn(stock_data)

    # Step 3: 导入 my-stock 优化引擎
    run_callable_optimization = import_optimizer()

    # Step 4: 运行网格搜索
    logger.info("开始参数优化...")
    results = run_callable_optimization(
        strategy_fn=strategy_fn,
        param_grid=PARAM_GRID,
        metric='sharpe',
        n_top=20,
    )
    logger.info(f"优化完成，有效结果: {len(results)} 组")

    # Step 5: 保存结果
    best_params, best_metrics = save_results(results, OUTPUT_DIR)

    # Step 6: 打印摘要
    top20_path = OUTPUT_DIR / 'top20_results.csv'
    df_top20 = pd.read_csv(top20_path, encoding='utf-8-sig') if top20_path.exists() else None
    print_summary(best_params, best_metrics, df_top20)

    elapsed = time.time() - t0
    logger.info(f"总耗时: {elapsed:.1f} 秒")
    logger.info(f"结果目录: {OUTPUT_DIR}")


if __name__ == '__main__':
    main()
