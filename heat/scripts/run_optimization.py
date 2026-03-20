#!/usr/bin/env python3
"""
热度策略参数优化 CLI

完整优化流程：
  1. 加载数据（my_trend + my_stock，一次性）
  2. Phase 1：网格搜索（1500 组合，lookback 缓存加速）
  3. Phase 2：MLP 代理模型精细优化（可选，--skip-ai 跳过）
  4. 保存 best_params.json 和 optimization_results.csv
  5. 用最优参数生成 orders.csv（OrderBasedEngine 格式）
  6. 调用 my-stock run_backtest.py CLI 生成可视化报告
  7. 打印报告路径和关键指标

用法：
    cd F:/projects/my-stock-research
    python heat/scripts/run_optimization.py \\
        --start 2025-03-15 --end 2026-03-19 \\
        --output F:/projects/my-stock-research/backtest/output \\
        [--metric sharpe] \\
        [--skip-ai]

前提条件：
    - SSH 隧道已启动（Desktop/Tssh-tunnel.bat）
    - my_stock（port 3307）和 my_trend（port 3310）可访问
"""
import argparse
import logging
import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

# 将项目根目录加入 Python 路径（确保 heat.scripts 可以被导入）
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from heat.scripts.config import (
    INITIAL_CAPITAL, START_DATE, END_DATE,
    REPORT_OUTPUT_DIR, MY_STOCK_PROJECT
)
from heat.scripts.data_loader import load_data_bundle
from heat.scripts.strategy import HeatRotationStrategy
from heat.scripts.optimizer import HeatOptimizer

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='热度策略参数优化',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python heat/scripts/run_optimization.py --start 2025-03-15 --end 2026-03-19
  python heat/scripts/run_optimization.py --metric annual_return --skip-ai
        """
    )
    parser.add_argument('--start', default=START_DATE, help='回测起始日期（YYYY-MM-DD）')
    parser.add_argument('--end', default=END_DATE, help='回测结束日期（YYYY-MM-DD）')
    parser.add_argument(
        '--output', default=REPORT_OUTPUT_DIR,
        help='报告输出目录（OrderBasedEngine 报告）'
    )
    parser.add_argument(
        '--metric', default='sharpe',
        choices=['sharpe', 'annual_return', 'excess_annual'],
        help='优化目标指标（默认 sharpe）'
    )
    parser.add_argument(
        '--max-drawdown-limit', type=float, default=0.40,
        help='最大回撤约束（超出此值的结果丢弃），默认 0.40 = 40%%'
    )
    parser.add_argument(
        '--skip-ai', action='store_true',
        help='跳过 AI 优化阶段（仅网格搜索）'
    )
    parser.add_argument(
        '--skip-backtest', action='store_true',
        help='跳过 OrderBasedEngine 回测（不调用 my-stock CLI）'
    )
    return parser.parse_args()


def run_order_based_backtest(orders_csv_path: str, output_dir: str,
                              strategy_name: str = '热度轮转优化最优参数') -> bool:
    """
    调用 my-stock OrderBasedEngine CLI 生成可视化报告

    在 F:/projects/my-stock 目录下执行：
        python -m app.services.run_backtest \\
            --orders <path>/orders.csv \\
            --output <output_dir> \\
            --strategy-name <name> \\
            --price-mode qfq

    Args:
        orders_csv_path: orders.csv 的绝对路径
        output_dir:      报告输出目录
        strategy_name:   策略名称（显示在报告中）

    Returns:
        True 表示成功，False 表示失败
    """
    my_stock_dir = MY_STOCK_PROJECT
    if not Path(my_stock_dir).exists():
        logger.warning(f"my-stock 目录不存在: {my_stock_dir}，跳过 OrderBasedEngine 回测")
        return False

    cmd = [
        sys.executable, '-m', 'app.services.run_backtest',
        '--orders', orders_csv_path,
        '--output', output_dir,
        '--strategy-name', strategy_name,
        '--price-mode', 'qfq',
    ]

    logger.info(f"调用 OrderBasedEngine CLI: {' '.join(cmd)}")
    logger.info(f"工作目录: {my_stock_dir}")

    try:
        result = subprocess.run(
            cmd,
            cwd=my_stock_dir,
            capture_output=True,
            text=True,
            timeout=300,  # 5 分钟超时
        )
        if result.returncode == 0:
            logger.info("OrderBasedEngine 报告生成成功")
            logger.info(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
            return True
        else:
            logger.warning(f"OrderBasedEngine 返回错误码 {result.returncode}")
            logger.warning(result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)
            return False
    except subprocess.TimeoutExpired:
        logger.warning("OrderBasedEngine 超时（5分钟）")
        return False
    except Exception as e:
        logger.warning(f"调用 OrderBasedEngine 失败: {e}")
        return False


def main():
    t_start = time.time()
    args = parse_args()

    logger.info("=" * 60)
    logger.info("热度策略参数优化系统")
    logger.info(f"回测区间: {args.start} ~ {args.end}")
    logger.info(f"优化目标: {args.metric}")
    logger.info(f"最大回撤约束: {args.max_drawdown_limit * 100:.0f}%")
    logger.info(f"AI优化: {'跳过' if args.skip_ai else '启用'}")
    logger.info("=" * 60)

    # ====================================================
    # Step 1：加载数据
    # ====================================================
    logger.info("Step 1: 加载数据...")
    data_bundle = load_data_bundle(args.start, args.end)

    # ====================================================
    # Step 2：运行优化
    # ====================================================
    logger.info("Step 2: 初始化策略和优化器...")
    strategy = HeatRotationStrategy()
    optimizer = HeatOptimizer(
        strategy=strategy,
        data_bundle=data_bundle,
        metric=args.metric,
        max_drawdown_limit=args.max_drawdown_limit,
    )

    logger.info("Step 3: 运行参数优化...")
    opt_results = optimizer.run(skip_ai=args.skip_ai)

    if not opt_results:
        logger.error("优化未产生结果，退出")
        sys.exit(1)

    # ====================================================
    # Step 3：保存优化结果
    # ====================================================
    # 保存到 heat/scripts/ 目录（供 live_signal.py 读取）
    scripts_dir = str(Path(__file__).parent)
    optimizer.save_results(opt_results, scripts_dir)

    best_params = opt_results['best_params']
    best_metrics = opt_results['best_metrics']

    logger.info("=" * 60)
    logger.info("优化结果摘要")
    logger.info("=" * 60)
    logger.info(f"最优参数: {best_params}")
    logger.info(f"Sharpe:   {best_metrics.get('sharpe', 'N/A'):.4f}")
    logger.info(f"年化收益: {best_metrics.get('annual_return', 'N/A'):.2f}%")
    logger.info(f"最大回撤: {best_metrics.get('max_drawdown', 'N/A'):.2f}%")
    logger.info(f"胜率:     {best_metrics.get('win_rate', 'N/A'):.1f}%")
    logger.info(f"交易笔数: {best_metrics.get('total_trades', 'N/A')}")
    logger.info("=" * 60)

    # 打印 Top 10
    if opt_results.get('top20') is not None and len(opt_results['top20']) > 0:
        top10 = opt_results['top20'].head(10)
        logger.info("\nTop 10 参数组合:")
        display_cols = ['lookback', 'buy_threshold', 'sell_threshold', 'n_positions',
                        'sharpe', 'annual_return', 'max_drawdown', 'win_rate']
        display_cols = [c for c in display_cols if c in top10.columns]
        print(top10[display_cols].to_string(index=True))

    # ====================================================
    # Step 4：用最优参数生成 orders.csv
    # ====================================================
    if not args.skip_backtest:
        logger.info("Step 4: 用最优参数生成 orders.csv...")

        # 复用优化器已预构建的 lookups
        lookups = optimizer.lookups
        hp_data = strategy._compute_heat_position(
            data_bundle['heat_df'], data_bundle['trading_days'],
            best_params.get('lookback', 20)
        )
        result = strategy.run_with_precomputed(hp_data, best_params, lookups)
        orders_df = result['orders_df']

        if len(orders_df) == 0:
            logger.warning("最优参数未产生任何订单，跳过 OrderBasedEngine 回测")
        else:
            # 保存 orders.csv
            output_path = Path(args.output)
            output_path.mkdir(parents=True, exist_ok=True)
            orders_csv = output_path / 'best_params_orders.csv'
            orders_df.to_csv(orders_csv, index=False, encoding='utf-8-sig')
            logger.info(f"orders.csv → {orders_csv}（{len(orders_df)} 条订单）")

            # ====================================================
            # Step 5：调用 OrderBasedEngine CLI
            # ====================================================
            logger.info("Step 5: 调用 OrderBasedEngine CLI 生成可视化报告...")
            success = run_order_based_backtest(
                orders_csv_path=str(orders_csv),
                output_dir=str(output_path),
                strategy_name=f'热度轮转优化_{args.metric}最优',
            )

            if success:
                logger.info(f"报告目录: {output_path}")
                logger.info("访问方式：启动 my-stock 后端后打开 /report 页面查看")
            else:
                logger.info("OrderBasedEngine 报告生成失败（可能是 my-stock 未配置），"
                            "可手动运行：")
                logger.info(f"  cd F:/projects/my-stock")
                logger.info(f"  python -m app.services.run_backtest "
                            f"--orders {orders_csv} --output {output_path}")

    elapsed = time.time() - t_start
    logger.info(f"\n全部完成，总耗时 {elapsed:.1f} 秒")
    logger.info(f"最优参数文件: {scripts_dir}/best_params.json")
    logger.info(f"全部结果文件: {scripts_dir}/optimization_results.csv")


if __name__ == '__main__':
    main()
