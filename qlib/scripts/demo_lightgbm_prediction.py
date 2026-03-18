"""LightGBM 多因子选股预测示例（复现 Qlib 核心流程）

用 Qlib 的 Alpha158 因子 + LightGBM 预测次日收益排名。
这是 Qlib 最经典的 benchmark，复现其核心逻辑。

流程:
  1. 加载多只股票数据
  2. 计算 Alpha158 因子
  3. 训练 LightGBM 预测次日收益
  4. 评估 IC/ICIR/分组收益

用法:
  python research/qlib/demo_lightgbm_prediction.py
  python research/qlib/demo_lightgbm_prediction.py --stocks 10 --top 50
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sqlalchemy import text

from app.database import engine
from app.logger import get_logger

log = get_logger("research.qlib.lgb_prediction")

# 尝试导入 LightGBM，不可用则回退到 sklearn
try:
    import lightgbm as lgb
    HAS_LGB = True
    log.info("使用 LightGBM")
except ImportError:
    HAS_LGB = False
    log.info("LightGBM 不可用，使用 sklearn GradientBoosting 替代")


def get_stock_list(n: int = 20) -> list:
    """获取流动性较好的 N 只股票"""
    sql = text("""
        SELECT ts_code
        FROM stock_basic
        WHERE market IN ('主板', '中小板') AND list_status = 'L'
        ORDER BY ts_code
        LIMIT :n
    """)
    with engine.connect() as conn:
        result = conn.execute(sql, {"n": n})
        codes = [row[0] for row in result]
    log.info(f"获取 {len(codes)} 只股票")
    return codes


def load_multi_stock_data(codes: list) -> pd.DataFrame:
    """批量加载多只股票数据"""
    all_data = []
    for code in codes:
        sql = text("""
            SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close,
                   d.vol, d.amount, d.pre_close, d.pct_chg,
                   b.turnover_rate
            FROM market_daily d
            LEFT JOIN daily_basic b ON d.ts_code = b.ts_code AND d.trade_date = b.trade_date
            WHERE d.ts_code = :code
            ORDER BY d.trade_date
        """)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"code": code})
        if len(df) > 200:
            all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    combined['trade_date'] = pd.to_datetime(combined['trade_date'])
    log.info(f"加载 {len(all_data)} 只股票, 共 {len(combined)} 行")
    return combined


def compute_features_per_stock(group: pd.DataFrame) -> pd.DataFrame:
    """对单只股票计算因子（与 Alpha158 一致）"""
    c = group['close']
    o, h, l, v = group['open'], group['high'], group['low'], group['vol']

    f = pd.DataFrame(index=group.index)

    # 动量
    for d in [1, 5, 10, 20]:
        f[f'ret_{d}'] = c.pct_change(d)

    # 均线偏离
    for d in [5, 10, 20]:
        ma = c.rolling(d).mean()
        f[f'ma_bias_{d}'] = (c - ma) / (ma + 1e-8)

    # 波动率
    ret = c.pct_change()
    for d in [5, 20]:
        f[f'volatility_{d}'] = ret.rolling(d).std()

    # KBAR
    f['kbar_body'] = (c - o) / (o + 1e-8)
    f['kbar_upper'] = (h - np.maximum(o, c)) / (c + 1e-8)
    f['kbar_lower'] = (np.minimum(o, c) - l) / (c + 1e-8)

    # RSI
    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta).clip(lower=0).rolling(14).mean()
    f['rsi_14'] = 100 - 100 / (1 + gain / (loss + 1e-8))

    # MACD
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    f['macd_dif'] = ema12 - ema26

    # 量能
    f['vol_ratio'] = v / (v.rolling(5).mean() + 1e-8)
    f['vol_change'] = v.pct_change()

    # 换手率
    if 'turnover_rate' in group.columns:
        f['turnover'] = group['turnover_rate']

    # 标签: 次日收益率
    f['target'] = group['pct_chg'].shift(-1) / 100

    return f


FEATURE_COLS = [
    'ret_1', 'ret_5', 'ret_10', 'ret_20',
    'ma_bias_5', 'ma_bias_10', 'ma_bias_20',
    'volatility_5', 'volatility_20',
    'kbar_body', 'kbar_upper', 'kbar_lower',
    'rsi_14', 'macd_dif',
    'vol_ratio', 'vol_change', 'turnover',
]


def train_model(X_train, y_train, X_val, y_val):
    """训练预测模型"""
    if HAS_LGB:
        params = {
            'objective': 'regression',
            'metric': 'mse',
            'num_leaves': 64,
            'learning_rate': 0.05,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
        }
        train_data = lgb.Dataset(X_train, label=y_train)
        val_data = lgb.Dataset(X_val, label=y_val)
        model = lgb.train(
            params, train_data,
            num_boost_round=300,
            valid_sets=[val_data],
            callbacks=[lgb.log_evaluation(50), lgb.early_stopping(30)],
        )
    else:
        # sklearn 替代方案
        model = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
        )
        model.fit(X_train, y_train)
        val_pred = model.predict(X_val)
        val_ic = np.corrcoef(val_pred, y_val)[0, 1]
        log.info(f"验证集 IC: {val_ic:.4f}")

    return model


def evaluate_model(model, X_test, y_test, test_dates):
    """评估模型：IC / ICIR / 分组收益"""
    if HAS_LGB:
        pred = model.predict(X_test)
    else:
        pred = model.predict(X_test)

    # 整体 IC
    overall_ic = np.corrcoef(pred, y_test)[0, 1]

    # 按日期计算 daily IC
    test_df = pd.DataFrame({
        'date': test_dates,
        'pred': pred,
        'actual': y_test,
    })

    daily_ic = test_df.groupby('date').apply(
        lambda g: g['pred'].corr(g['actual']) if len(g) > 5 else np.nan
    ).dropna()

    ic_mean = daily_ic.mean()
    ic_std = daily_ic.std()
    icir = ic_mean / (ic_std + 1e-8)

    # 分 5 组看收益
    test_df['group'] = test_df.groupby('date')['pred'].transform(
        lambda x: pd.qcut(x, 5, labels=[1,2,3,4,5], duplicates='drop')
        if len(x) >= 5 else np.nan
    )
    group_ret = test_df.groupby('group')['actual'].mean() * 100

    print(f"\n{'='*60}")
    print(f"  LightGBM 多因子预测评估")
    print(f"{'='*60}")
    print(f"  整体 IC:  {overall_ic:.4f}")
    print(f"  日均 IC:  {ic_mean:.4f}")
    print(f"  IC 标准差: {ic_std:.4f}")
    print(f"  ICIR:     {icir:.4f}")
    print(f"  IC>0 占比: {(daily_ic > 0).mean()*100:.1f}%")
    print(f"\n  分组日均收益 (%):")
    for g in range(1, 6):
        if g in group_ret.index:
            bar = '█' * int(abs(group_ret[g]) * 50)
            sign = '+' if group_ret[g] > 0 else ''
            print(f"    第{g}组(预测{'最弱' if g==1 else '最强' if g==5 else '中等'}): "
                  f"{sign}{group_ret[g]:.4f}% {bar}")

    # 多空收益
    if 5 in group_ret.index and 1 in group_ret.index:
        long_short = group_ret[5] - group_ret[1]
        print(f"\n  多空收益 (Top-Bottom): {long_short:.4f}%/天")
        print(f"  年化多空: {long_short * 252:.2f}%")

    # 特征重要性
    if HAS_LGB:
        importance = model.feature_importance(importance_type='gain')
        feat_names = model.feature_name()
    else:
        importance = model.feature_importances_
        feat_names = FEATURE_COLS

    imp_df = pd.DataFrame({'feature': feat_names, 'importance': importance})
    imp_df = imp_df.sort_values('importance', ascending=False)

    print(f"\n  特征重要性 Top 10:")
    for _, row in imp_df.head(10).iterrows():
        bar = '█' * int(row['importance'] / imp_df['importance'].max() * 20)
        print(f"    {row['feature']:<16s} {bar}")

    return {'IC': overall_ic, 'IC_mean': ic_mean, 'ICIR': icir}


def main():
    parser = argparse.ArgumentParser(description="LightGBM 多因子选股预测")
    parser.add_argument('--stocks', type=int, default=20, help='股票数量')
    args = parser.parse_args()

    t0 = time.time()

    # 1. 获取股票列表
    codes = get_stock_list(args.stocks)
    if not codes:
        log.error("没有获取到股票")
        return

    # 2. 加载数据
    data = load_multi_stock_data(codes)
    if data.empty:
        log.error("数据为空")
        return

    # 3. 计算因子
    all_features = []
    for code, group in data.groupby('ts_code'):
        group = group.sort_values('trade_date').reset_index(drop=True)
        features = compute_features_per_stock(group)
        features['ts_code'] = code
        features['trade_date'] = group['trade_date']
        all_features.append(features)

    feat_df = pd.concat(all_features, ignore_index=True)

    # 去掉 NaN
    available_cols = [c for c in FEATURE_COLS if c in feat_df.columns]
    feat_clean = feat_df[available_cols + ['target', 'trade_date']].dropna()
    log.info(f"有效数据: {len(feat_clean)} 行, {len(available_cols)} 个因子")

    # 4. 按时间划分
    dates = sorted(feat_clean['trade_date'].unique())
    train_end = dates[int(len(dates) * 0.6)]
    val_end = dates[int(len(dates) * 0.8)]

    train = feat_clean[feat_clean['trade_date'] <= train_end]
    val = feat_clean[(feat_clean['trade_date'] > train_end) & (feat_clean['trade_date'] <= val_end)]
    test = feat_clean[feat_clean['trade_date'] > val_end]

    log.info(f"训练: {len(train)}, 验证: {len(val)}, 测试: {len(test)}")

    X_train, y_train = train[available_cols].values, train['target'].values
    X_val, y_val = val[available_cols].values, val['target'].values
    X_test, y_test = test[available_cols].values, test['target'].values

    # 5. 训练
    model = train_model(X_train, y_train, X_val, y_val)

    # 6. 评估
    results = evaluate_model(model, X_test, y_test, test['trade_date'].values)

    print(f"\n耗时: {time.time()-t0:.1f}s")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
