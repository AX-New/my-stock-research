"""
热度策略参数优化引擎

两阶段优化：
  Phase 1：网格搜索（全量参数空间扫描，约 1500 组合）
  Phase 2：MLP 代理模型精细优化（在网格搜索最优区间内继续探索）

关键优化：
  相同 lookback 的参数组合复用同一个 heat_position 矩阵，
  避免重复计算（实际只需计算 5 次矩阵，而非 1500 次）。
"""
import json
import logging
import itertools
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

from heat.scripts.strategy import HeatRotationStrategy

logger = logging.getLogger(__name__)


class HeatOptimizer:
    """
    热度策略参数优化器

    使用方式：
        optimizer = HeatOptimizer(strategy, data_bundle)
        results = optimizer.run()
        optimizer.save_results(results, 'heat/scripts')
    """

    # 默认参数搜索空间
    DEFAULT_PARAM_SPACE = {
        'lookback':        [10, 15, 20, 30, 40],
        'buy_threshold':   [0.70, 0.75, 0.80, 0.85, 0.90],
        'sell_threshold':  [0.10, 0.15, 0.20, 0.25, 0.30],
        'min_deal_amount': [1e7, 3e7, 5e7, 1e8],
        'n_positions':     [1, 2, 3],
    }

    def __init__(self, strategy: HeatRotationStrategy, data_bundle: dict,
                 metric: str = 'sharpe', max_drawdown_limit: float = 0.40):
        """
        初始化优化器

        Args:
            strategy:            HeatRotationStrategy 实例
            data_bundle:         load_data_bundle() 返回的数据包
            metric:              优化目标指标（'sharpe'/'annual_return'/'excess_annual'）
            max_drawdown_limit:  最大回撤约束（超出此值的结果丢弃），如 0.40 = 40%
        """
        self.strategy = strategy
        self.data_bundle = data_bundle
        self.metric = metric
        self.max_drawdown_limit = max_drawdown_limit
        self.price_df = data_bundle['price_df']
        self.trading_days = data_bundle['trading_days']
        self.index_df = data_bundle['index_df']
        self.heat_df = data_bundle['heat_df']

        # 预构建 lookups（复用 data_bundle 中的预构建表，全部优化共享一份）
        logger.info("优化器初始化：准备价格查询表...")
        self.lookups = strategy.prepare_lookups(data_bundle)
        logger.info(f"  价格查询表: {len(self.lookups['price_lookup']):,} 条")

    def run_grid_search(self, param_space: dict = None) -> pd.DataFrame:
        """
        Phase 1：网格搜索

        遍历所有参数组合，按 lookback 分组复用 heat_position 矩阵，
        大幅减少计算量（lookback 5档 × 其他参数 300 组 = 实际只算 5 次矩阵）。

        Args:
            param_space: 参数搜索空间，不传则使用 DEFAULT_PARAM_SPACE

        Returns:
            包含所有有效结果的 DataFrame，按优化指标降序排序
        """
        if param_space is None:
            param_space = self.DEFAULT_PARAM_SPACE

        # 生成所有参数组合
        keys = list(param_space.keys())
        all_combos = [
            dict(zip(keys, vals))
            for vals in itertools.product(*[param_space[k] for k in keys])
        ]

        # 确保 buy_threshold > sell_threshold（无效组合直接跳过）
        all_combos = [
            c for c in all_combos
            if c.get('buy_threshold', 1) > c.get('sell_threshold', 0)
        ]

        total = len(all_combos)
        logger.info(f"网格搜索：{total} 组合（过滤无效组合后）")

        # 按 lookback 分组，复用 heat_position 矩阵
        groups = {}
        for combo in all_combos:
            lb = combo['lookback']
            groups.setdefault(lb, []).append(combo)

        # lookback 缓存
        hp_cache = {}
        results = []
        done = 0

        for lb, group_params in sorted(groups.items()):
            # 计算或复用 heat_position 矩阵
            if lb not in hp_cache:
                logger.info(f"  计算 lookback={lb} 矩阵...")
                hp_cache[lb] = self.strategy._compute_heat_position(
                    self.heat_df, self.trading_days, lb
                )

            hp_data = hp_cache[lb]

            for params in group_params:
                done += 1
                if done % 50 == 0:
                    logger.info(f"  进度: {done}/{total}")

                try:
                    result = self.strategy.run_with_precomputed(
                        hp_data, params, self.lookups
                    )
                    metrics = result['metrics']

                    if not metrics:
                        continue

                    # 最大回撤约束过滤（max_drawdown 是负值，如 -30%）
                    if abs(metrics.get('max_drawdown', 0)) > self.max_drawdown_limit * 100:
                        continue

                    row = dict(params)
                    row.update(metrics)
                    results.append(row)

                except Exception as e:
                    logger.warning(f"  参数 {params} 运行失败: {e}")
                    continue

        if not results:
            logger.warning("网格搜索无有效结果")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df = df.sort_values(self.metric, ascending=False).reset_index(drop=True)

        logger.info(f"网格搜索完成：{len(df)} 个有效结果")
        logger.info(f"最优 {self.metric}: {df.iloc[0][self.metric]:.4f}")
        logger.info(f"最优参数: {df.iloc[0][['lookback','buy_threshold','sell_threshold','n_positions','min_deal_amount']].to_dict()}")

        return df

    def run_ai_optimization(self, initial_results_df: pd.DataFrame,
                             n_iter: int = 5, n_samples: int = 10) -> pd.DataFrame:
        """
        Phase 2：基于 MLP 代理模型的精细优化

        在网格搜索最优区间内用代理模型预测下一批候选参数，
        逐步收敛到局部最优。

        Args:
            initial_results_df: 网格搜索结果（Phase 1 输出）
            n_iter:             迭代轮次
            n_samples:          每轮随机采样候选数

        Returns:
            合并后的结果 DataFrame（包含网格搜索 + AI 优化结果）
        """
        try:
            from sklearn.neural_network import MLPRegressor
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            logger.warning("scikit-learn 未安装，跳过 AI 优化阶段")
            return initial_results_df

        if len(initial_results_df) < 5:
            logger.warning("初始结果不足，跳过 AI 优化")
            return initial_results_df

        logger.info(f"AI 优化阶段：{n_iter} 轮，每轮 {n_samples} 个候选...")

        # 参数列（数值型，用于代理模型）
        param_cols = ['lookback', 'buy_threshold', 'sell_threshold',
                      'min_deal_amount', 'n_positions']
        # 过滤掉缺少这些列的行
        df = initial_results_df[
            [c for c in param_cols if c in initial_results_df.columns] + [self.metric]
        ].dropna()

        if len(df) < 5:
            logger.warning("有效数据不足，跳过 AI 优化")
            return initial_results_df

        all_results = list(initial_results_df.to_dict('records'))

        for iteration in range(n_iter):
            logger.info(f"  AI 轮次 {iteration + 1}/{n_iter}...")

            # 训练 MLP 代理模型
            X = df[param_cols].values.astype(float)
            y = df[self.metric].values

            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)

            model = MLPRegressor(
                hidden_layer_sizes=(64, 32),
                max_iter=500,
                random_state=42 + iteration,
            )
            model.fit(X_scaled, y)

            # 在最优参数区间内随机采样候选
            best_row = df.nlargest(5, self.metric).iloc[0]
            candidates = self._sample_around_best(best_row, param_cols, n_samples)

            # 用代理模型预测，选 top 3 实际运行
            if len(candidates) > 0:
                X_cand = np.array([[c[p] for p in param_cols] for c in candidates])
                X_cand_scaled = scaler.transform(X_cand)
                preds = model.predict(X_cand_scaled)
                top_indices = np.argsort(preds)[-3:][::-1]

                for idx in top_indices:
                    params = candidates[idx]
                    # buy_threshold 必须大于 sell_threshold
                    if params.get('buy_threshold', 1) <= params.get('sell_threshold', 0):
                        continue

                    lb = int(params['lookback'])
                    hp_data = self.strategy._compute_heat_position(
                        self.heat_df, self.trading_days, lb
                    )

                    try:
                        result = self.strategy.run_with_precomputed(
                            hp_data, params, self.lookups
                        )
                        metrics = result['metrics']
                        if not metrics:
                            continue
                        if abs(metrics.get('max_drawdown', 0)) > self.max_drawdown_limit * 100:
                            continue
                        row = dict(params)
                        row.update(metrics)
                        all_results.append(row)

                        # 加入训练数据
                        new_row = {p: params[p] for p in param_cols}
                        new_row[self.metric] = metrics[self.metric]
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

                    except Exception as e:
                        logger.warning(f"  AI候选 {params} 运行失败: {e}")

        combined_df = pd.DataFrame(all_results)
        combined_df = combined_df.sort_values(self.metric, ascending=False).reset_index(drop=True)
        logger.info(f"AI 优化完成，共 {len(combined_df)} 个结果")
        return combined_df

    def _sample_around_best(self, best_row: pd.Series,
                             param_cols: list, n_samples: int) -> list:
        """
        在最优参数周围随机采样候选

        Args:
            best_row:   最优参数行
            param_cols: 参数列名列表
            n_samples:  采样数量

        Returns:
            候选参数列表
        """
        import random

        # 每个参数的扰动范围
        perturbations = {
            'lookback':        [5, 10, 15, 20, 25, 30, 35, 40],
            'buy_threshold':   [round(x * 0.05, 2) for x in range(13, 19)],  # 0.65 ~ 0.90
            'sell_threshold':  [round(x * 0.05, 2) for x in range(1, 8)],    # 0.05 ~ 0.35
            'min_deal_amount': [1e7, 2e7, 3e7, 5e7, 7e7, 1e8],
            'n_positions':     [1, 2, 3],
        }

        candidates = []
        for _ in range(n_samples):
            candidate = {}
            for col in param_cols:
                if col in perturbations:
                    candidate[col] = random.choice(perturbations[col])
                elif col in best_row:
                    candidate[col] = best_row[col]
            candidates.append(candidate)

        return candidates

    def run(self, param_space: dict = None, skip_ai: bool = False) -> dict:
        """
        完整优化流程：Phase 1 网格搜索 + Phase 2 AI 优化

        Args:
            param_space: 参数空间，不传则使用默认空间
            skip_ai:     是否跳过 AI 优化阶段

        Returns:
            dict，包含:
                'best_params':  最优参数 dict
                'best_metrics': 最优参数对应绩效指标 dict
                'all_results':  所有结果 DataFrame
                'top20':        Top 20 结果 DataFrame
        """
        import time
        t0 = time.time()

        logger.info("=" * 60)
        logger.info("参数优化开始（Phase 1：网格搜索）")
        logger.info("=" * 60)

        # Phase 1：网格搜索
        grid_results = self.run_grid_search(param_space)

        if len(grid_results) == 0:
            logger.error("网格搜索无结果，优化终止")
            return {}

        all_results = grid_results

        # Phase 2：AI 优化
        if not skip_ai:
            logger.info("=" * 60)
            logger.info("参数优化（Phase 2：AI 代理模型优化）")
            logger.info("=" * 60)
            all_results = self.run_ai_optimization(grid_results)

        # 提取最优参数
        best_row = all_results.iloc[0]
        param_cols = ['lookback', 'buy_threshold', 'sell_threshold',
                      'min_deal_amount', 'n_positions']
        best_params = {
            col: (int(best_row[col]) if col in ('lookback', 'n_positions')
                  else float(best_row[col]))
            for col in param_cols if col in best_row
        }
        # sort_by 默认 rank_surge
        best_params['sort_by'] = 'rank_surge'
        best_params['max_hold_days'] = 9999

        # 提取最优绩效指标
        metric_cols = ['total_return', 'annual_return', 'max_drawdown', 'sharpe',
                       'excess_total', 'excess_annual', 'win_rate', 'total_trades',
                       'avg_return', 'avg_hold']
        best_metrics = {
            col: float(best_row[col])
            for col in metric_cols if col in best_row
        }

        top20 = all_results.head(20)
        elapsed = time.time() - t0

        logger.info("=" * 60)
        logger.info(f"优化完成，耗时 {elapsed:.1f} 秒")
        logger.info(f"最优参数: {best_params}")
        logger.info(f"最优 Sharpe: {best_metrics.get('sharpe', 'N/A'):.4f}")
        logger.info(f"最优年化: {best_metrics.get('annual_return', 'N/A'):.2f}%")
        logger.info(f"最大回撤: {best_metrics.get('max_drawdown', 'N/A'):.2f}%")
        logger.info("=" * 60)

        return {
            'best_params': best_params,
            'best_metrics': best_metrics,
            'all_results': all_results,
            'top20': top20,
        }

    def save_results(self, results: dict, output_dir: str):
        """
        保存优化结果到指定目录

        保存文件：
            best_params.json          ← 最优参数（供 live_signal.py 读取）
            optimization_results.csv  ← 所有结果（按优化指标降序）
            top20_results.csv         ← Top 20 结果

        Args:
            results:    run() 的返回值
            output_dir: 输出目录路径
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 保存最优参数
        best_params_path = output_path / 'best_params.json'
        with open(best_params_path, 'w', encoding='utf-8') as f:
            json.dump(results['best_params'], f, indent=2, ensure_ascii=False)
        logger.info(f"最优参数 → {best_params_path}")

        # 保存所有结果
        if len(results.get('all_results', [])) > 0:
            all_path = output_path / 'optimization_results.csv'
            results['all_results'].to_csv(all_path, index=False, encoding='utf-8-sig')
            logger.info(f"全部结果 → {all_path}")

        # 保存 Top 20
        if len(results.get('top20', [])) > 0:
            top20_path = output_path / 'top20_results.csv'
            results['top20'].to_csv(top20_path, index=False, encoding='utf-8-sig')
            logger.info(f"Top 20 → {top20_path}")
