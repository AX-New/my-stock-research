"""
热度数据 ML 分析主入口

完整流水线：数据加载 → 特征工程 → 模型训练 → 评估 → 特征重要性分析

使用方式：
  # 默认运行（LightGBM + XGBoost，回归任务，预测5日收益）
  python -m ml.scripts.run_heat_ml

  # 指定模型和目标
  python -m ml.scripts.run_heat_ml --models lgb xgb --target 5 --task regression

  # 包含 LSTM（需要 PyTorch + GPU）
  python -m ml.scripts.run_heat_ml --models lgb xgb lstm --target 5

  # 分类任务（涨/跌方向）
  python -m ml.scripts.run_heat_ml --task classification --target 5
"""
import argparse
import logging
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd

# 项目根目录加入 path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from ml.scripts.config import (
    START_DATE, END_DATE, SPLIT_DATE, FORWARD_DAYS,
    MIN_STOCK_DAYS, OUTPUT_DIR, REPORT_DIR,
)
from ml.scripts.data_loader import load_ml_dataset
from ml.scripts.feature_engine import build_features, get_feature_columns
from ml.scripts.models import LGBModel, XGBModel, LSTMModel

warnings.filterwarnings('ignore')

# ════════════════════════════════════════════════════════════════
# 日志配置
# ════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('ml.heat')


# ════════════════════════════════════════════════════════════════
# 评估指标
# ════════════════════════════════════════════════════════════════

def evaluate_regression(y_true, y_pred, label=''):
    """回归评估：IC、ICIR、MSE、方向准确率"""
    from scipy.stats import spearmanr, pearsonr

    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    y_t = y_true[mask]
    y_p = y_pred[mask]

    if len(y_t) < 10:
        return {}

    mse = np.mean((y_t - y_p) ** 2)
    ic_pearson, _ = pearsonr(y_t, y_p)
    ic_spearman, _ = spearmanr(y_t, y_p)

    # 方向准确率
    direction_acc = np.mean((y_t > 0) == (y_p > 0))

    # 分组收益（5组）
    group_returns = _compute_group_returns(y_t, y_p, n_groups=5)

    metrics = {
        'MSE': mse,
        'IC(Pearson)': ic_pearson,
        'IC(Spearman)': ic_spearman,
        '方向准确率': direction_acc,
        '样本数': len(y_t),
    }
    metrics.update(group_returns)

    logger.info(f"  {label} 回归评估:")
    logger.info(f"    IC(Pearson)={ic_pearson:.4f}, IC(Spearman)={ic_spearman:.4f}")
    logger.info(f"    MSE={mse:.6f}, 方向准确率={direction_acc:.4f}")
    logger.info(f"    分组收益: {group_returns}")
    return metrics


def evaluate_classification(y_true, y_pred_proba, label=''):
    """分类评估：AUC、准确率、精确率、召回率"""
    from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score

    mask = ~(np.isnan(y_true) | np.isnan(y_pred_proba))
    y_t = y_true[mask]
    y_p = y_pred_proba[mask]

    if len(y_t) < 10:
        return {}

    y_pred_label = (y_p > 0.5).astype(int)
    auc = roc_auc_score(y_t, y_p)
    acc = accuracy_score(y_t, y_pred_label)
    precision = precision_score(y_t, y_pred_label, zero_division=0)
    recall = recall_score(y_t, y_pred_label, zero_division=0)

    metrics = {
        'AUC': auc,
        '准确率': acc,
        '精确率': precision,
        '召回率': recall,
        '样本数': len(y_t),
        '正样本比例': y_t.mean(),
    }

    logger.info(f"  {label} 分类评估:")
    logger.info(f"    AUC={auc:.4f}, 准确率={acc:.4f}")
    logger.info(f"    精确率={precision:.4f}, 召回率={recall:.4f}")
    logger.info(f"    正样本比例={y_t.mean():.4f}")
    return metrics


def _compute_group_returns(y_true, y_pred, n_groups=5):
    """分组收益：按预测值分 N 组，看各组实际平均收益"""
    df = pd.DataFrame({'true': y_true, 'pred': y_pred})
    df['group'] = pd.qcut(df['pred'], n_groups, labels=False, duplicates='drop')
    group_means = df.groupby('group')['true'].mean()
    result = {}
    for g in range(len(group_means)):
        result[f'G{g+1}均收益'] = round(group_means.iloc[g], 6) if g < len(group_means) else None
    # 多空收益 = Top组 - Bottom组
    if len(group_means) >= 2:
        result['多空收益'] = round(group_means.iloc[-1] - group_means.iloc[0], 6)
    return result


# ════════════════════════════════════════════════════════════════
# 按日计算 IC（时间序列 IC 分析）
# ════════════════════════════════════════════════════════════════

def compute_daily_ic(df_test, y_pred, target_col, feature_cols):
    """
    按日计算 IC，分析预测能力的时间稳定性

    Returns:
        DataFrame: 每日 IC 值
    """
    from scipy.stats import spearmanr

    df_eval = df_test[['date']].copy()
    df_eval['y_true'] = df_test[target_col].values
    df_eval['y_pred'] = y_pred

    daily_ic = []
    for date, group in df_eval.groupby('date'):
        valid = group.dropna(subset=['y_true', 'y_pred'])
        if len(valid) < 10:
            continue
        ic, _ = spearmanr(valid['y_true'], valid['y_pred'])
        daily_ic.append({'date': date, 'IC': ic, 'n_stocks': len(valid)})

    if not daily_ic:
        return pd.DataFrame()

    ic_df = pd.DataFrame(daily_ic)
    mean_ic = ic_df['IC'].mean()
    std_ic = ic_df['IC'].std()
    icir = mean_ic / std_ic if std_ic > 0 else 0
    ic_positive_rate = (ic_df['IC'] > 0).mean()

    logger.info(f"  每日IC分析:")
    logger.info(f"    均值IC={mean_ic:.4f}, IC标准差={std_ic:.4f}, ICIR={icir:.4f}")
    logger.info(f"    IC>0比例={ic_positive_rate:.2%}, 交易日数={len(ic_df)}")
    return ic_df


# ════════════════════════════════════════════════════════════════
# SHAP 特征重要性分析
# ════════════════════════════════════════════════════════════════

def analyze_shap(model, X_test, feature_cols, output_dir, model_name):
    """
    使用 SHAP 分析特征重要性

    生成：
      1. SHAP 重要性排名表（CSV）
      2. 全局特征重要性摘要
    """
    try:
        import shap
    except ImportError:
        logger.warning("SHAP 未安装，跳过 SHAP 分析")
        return None

    logger.info(f"  计算 {model_name} SHAP 值...")
    start = time.time()

    # 对树模型使用 TreeExplainer（快），其他用 KernelExplainer（慢，采样）
    if hasattr(model, 'model') and hasattr(model.model, 'predict'):
        inner_model = model.model
        try:
            explainer = shap.TreeExplainer(inner_model)
            # 采样避免过慢
            sample_size = min(2000, len(X_test))
            X_sample = X_test[:sample_size] if isinstance(X_test, np.ndarray) else X_test.iloc[:sample_size]
            shap_values = explainer.shap_values(X_sample)
        except Exception as e:
            logger.warning(f"  SHAP TreeExplainer 失败: {e}，跳过")
            return None
    else:
        logger.info("  非树模型，跳过 SHAP 分析")
        return None

    # 处理分类任务返回的列表格式
    if isinstance(shap_values, list):
        shap_values = shap_values[1]  # 取正类的 SHAP 值

    # 计算平均绝对 SHAP 值
    mean_abs_shap = np.abs(shap_values).mean(axis=0)
    shap_df = pd.DataFrame({
        'feature': feature_cols,
        'mean_abs_shap': mean_abs_shap,
    }).sort_values('mean_abs_shap', ascending=False)

    # 保存结果
    shap_path = os.path.join(output_dir, f'shap_{model_name}.csv')
    shap_df.to_csv(shap_path, index=False)
    elapsed = time.time() - start
    logger.info(f"  SHAP 分析完成: {elapsed:.1f}s, Top10 特征:")
    for _, row in shap_df.head(10).iterrows():
        logger.info(f"    {row['feature']:30s} SHAP={row['mean_abs_shap']:.6f}")

    return shap_df


# ════════════════════════════════════════════════════════════════
# 主流水线
# ════════════════════════════════════════════════════════════════

def run_pipeline(model_names=None, target_days=5, task='regression'):
    """
    执行完整 ML 分析流水线

    Args:
        model_names: 模型列表，如 ['lgb', 'xgb', 'lstm']
        target_days: 预测目标天数
        task: 'regression' 或 'classification'
    """
    if model_names is None:
        model_names = ['lgb', 'xgb']

    total_start = time.time()
    logger.info("=" * 70)
    logger.info("热度数据 ML 分析流水线")
    logger.info(f"  模型: {model_names}")
    logger.info(f"  任务: {task}")
    logger.info(f"  目标: 未来 {target_days} 天{'收益率' if task == 'regression' else '涨跌方向'}")
    logger.info(f"  数据: {START_DATE} ~ {END_DATE}, 切分: {SPLIT_DATE}")
    logger.info("=" * 70)

    # ──── 1. 加载数据 ────
    data_bundle = load_ml_dataset(START_DATE, END_DATE)

    # ──── 2. 特征工程 ────
    df = build_features(data_bundle, forward_days=FORWARD_DAYS)

    # 确定目标列
    if task == 'regression':
        target_col = f'target_{target_days}d_return'
    else:
        target_col = f'target_{target_days}d_dir'

    if target_col not in df.columns:
        logger.error(f"目标列 {target_col} 不存在，可用: {[c for c in df.columns if 'target' in c]}")
        return

    # ──── 3. 训练/测试切分（时间切分，避免未来信息泄露） ────
    from datetime import date as dt_date
    split = dt_date.fromisoformat(SPLIT_DATE)
    df_train = df[df['date'] < split].copy()
    df_test = df[df['date'] >= split].copy()

    # 获取特征列
    feature_cols = get_feature_columns(df)
    logger.info(f"特征数: {len(feature_cols)}, 训练集: {len(df_train):,}, 测试集: {len(df_test):,}")

    # 去掉目标为空的样本
    df_train = df_train.dropna(subset=[target_col])
    df_test = df_test.dropna(subset=[target_col])
    logger.info(f"去除空目标后 — 训练: {len(df_train):,}, 测试: {len(df_test):,}")

    # 准备 X/y（保留 DataFrame 以传递列名给树模型）
    X_train = df_train[feature_cols]
    y_train = df_train[target_col].values
    X_test = df_test[feature_cols]
    y_test = df_test[target_col].values

    # ──── 4. 训练和评估每个模型 ────
    results = {}
    all_shap = {}
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for name in model_names:
        logger.info("-" * 50)
        logger.info(f"训练模型: {name.upper()}")
        logger.info("-" * 50)

        if name == 'lgb':
            model = LGBModel(task=task)
        elif name == 'xgb':
            model = XGBModel(task=task)
        elif name == 'lstm':
            model = LSTMModel(task=task, sequence_length=20, epochs=50)
        else:
            logger.warning(f"未知模型: {name}")
            continue

        # 训练（用训练集最后20%做验证集）
        val_split = int(len(X_train) * 0.8)
        X_tr, X_val = X_train[:val_split], X_train[val_split:]
        y_tr, y_val = y_train[:val_split], y_train[val_split:]

        # NaN 处理：LSTM 需要填充，树模型原生支持 NaN
        if name == 'lstm':
            X_tr_clean = np.nan_to_num(X_tr.values, nan=0.0)
            X_val_clean = np.nan_to_num(X_val.values, nan=0.0)
            X_test_clean = np.nan_to_num(X_test.values, nan=0.0)
            model.fit(X_tr_clean, y_tr, X_val_clean, y_val)
            y_pred = model.predict(X_test_clean)
        else:
            model.fit(X_tr, y_tr, X_val, y_val)
            y_pred = model.predict(X_test)

        # 评估
        if task == 'regression':
            metrics = evaluate_regression(y_test, y_pred, label=name.upper())
        else:
            metrics = evaluate_classification(y_test, y_pred, label=name.upper())
        metrics['训练时间'] = model.train_time
        results[name] = metrics

        # 每日 IC 分析（仅回归任务）
        if task == 'regression' and name != 'lstm':
            ic_df = compute_daily_ic(df_test, y_pred, target_col, feature_cols)
            if len(ic_df) > 0:
                ic_df.to_csv(os.path.join(OUTPUT_DIR, f'daily_ic_{name}.csv'), index=False)

        # 特征重要性（树模型）
        if name in ('lgb', 'xgb'):
            imp_df = model.get_feature_importance()
            if len(imp_df) > 0:
                imp_df.to_csv(os.path.join(OUTPUT_DIR, f'feature_importance_{name}.csv'), index=False)
                logger.info(f"  Top15 特征重要性:")
                for _, row in imp_df.head(15).iterrows():
                    logger.info(f"    {str(row['feature']):30s} {float(row['importance']):.0f}")

        # SHAP 分析（树模型）
        if name in ('lgb', 'xgb'):
            shap_df = analyze_shap(model, X_test, feature_cols, OUTPUT_DIR, name)
            if shap_df is not None:
                all_shap[name] = shap_df

    # ──── 5. 汇总对比 ────
    logger.info("=" * 70)
    logger.info("模型对比汇总")
    logger.info("=" * 70)
    summary_df = pd.DataFrame(results).T
    logger.info(f"\n{summary_df.to_string()}")
    summary_df.to_csv(os.path.join(OUTPUT_DIR, 'model_comparison.csv'))

    # ──── 6. 保存完整特征矩阵（供后续分析） ────
    feature_path = os.path.join(OUTPUT_DIR, 'feature_matrix.csv')
    # 只保存测试集的特征+预测结果
    df_test_out = df_test[['stock_code', 'date'] + feature_cols + [target_col]].copy()
    df_test_out.to_csv(feature_path, index=False)
    logger.info(f"特征矩阵已保存: {feature_path}")

    total_time = time.time() - total_start
    logger.info(f"\n总耗时: {total_time:.1f}s")
    logger.info("=" * 70)

    return {
        'results': results,
        'feature_cols': feature_cols,
        'summary': summary_df,
        'shap': all_shap,
    }


# ════════════════════════════════════════════════════════════════
# CLI 入口
# ════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='热度数据 ML 分析')
    parser.add_argument('--models', nargs='+', default=['lgb', 'xgb'],
                        choices=['lgb', 'xgb', 'lstm'],
                        help='使用的模型（默认: lgb xgb）')
    parser.add_argument('--target', type=int, default=5,
                        help='预测目标天数（默认: 5）')
    parser.add_argument('--task', default='regression',
                        choices=['regression', 'classification'],
                        help='任务类型（默认: regression）')
    args = parser.parse_args()
    run_pipeline(model_names=args.models, target_days=args.target, task=args.task)


if __name__ == '__main__':
    main()
