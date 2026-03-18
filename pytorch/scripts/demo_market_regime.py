"""PyTorch 牛熊市场特征识别示例

用深度学习自动识别市场所处的牛/熊/震荡状态。
思路：基于指数日线数据，提取多维度特征，训练分类器识别市场状态。

标签定义：
  - 牛市: 未来60天收益率 > 10%
  - 熊市: 未来60天收益率 < -10%
  - 震荡: 其余

特征维度：
  - 价格动量 (5/10/20/60日收益)
  - 波动率 (5/20/60日滚动标准差)
  - 均线排列 (MA5>MA10>MA20>MA60 的得分)
  - 成交量趋势 (量能变化)
  - RSI / MACD 状态

用法:
  python research/pytorch/demo_market_regime.py
  python research/pytorch/demo_market_regime.py --index 000001.SH --epochs 80
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.database import engine
from app.logger import get_logger

log = get_logger("research.pytorch.market_regime")


def load_index_data(ts_code: str = '000001.SH') -> pd.DataFrame:
    """加载指数日线数据"""
    sql = text("""
        SELECT trade_date, open, high, low, close, vol, amount, pct_chg
        FROM index_daily
        WHERE ts_code = :code
        ORDER BY trade_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    log.info(f"加载指数 {ts_code}: {len(df)} 行")
    return df


def compute_regime_features(df: pd.DataFrame) -> pd.DataFrame:
    """计算市场状态识别特征 + 标签"""
    c = df['close']

    # ── 价格动量 ──
    for d in [5, 10, 20, 60]:
        df[f'mom_{d}'] = c.pct_change(d)

    # ── 波动率 ──
    ret = c.pct_change()
    for d in [5, 20, 60]:
        df[f'vol_{d}'] = ret.rolling(d).std()

    # ── 均线排列得分 ──
    ma5 = c.rolling(5).mean()
    ma10 = c.rolling(10).mean()
    ma20 = c.rolling(20).mean()
    ma60 = c.rolling(60).mean()
    # 得分: MA5>MA10 + MA10>MA20 + MA20>MA60, 范围 0~3
    df['ma_alignment'] = ((ma5 > ma10).astype(int) +
                          (ma10 > ma20).astype(int) +
                          (ma20 > ma60).astype(int))

    # ── 均线偏离 ──
    df['ma20_bias'] = (c - ma20) / (ma20 + 1e-8)
    df['ma60_bias'] = (c - ma60) / (ma60 + 1e-8)

    # ── RSI ──
    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta).clip(lower=0).rolling(14).mean()
    df['rsi'] = 100 - 100 / (1 + gain / (loss + 1e-8))

    # ── MACD ──
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df['dif'] = ema12 - ema26
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()

    # ── 量能 ──
    df['vol_ma5_ratio'] = df['vol'] / (df['vol'].rolling(5).mean() + 1e-8)
    df['vol_trend'] = df['vol'].rolling(5).mean() / (df['vol'].rolling(20).mean() + 1e-8)

    # ── 标签: 未来60天收益 ──
    future_return = c.shift(-60) / c - 1
    df['regime'] = 1  # 默认震荡
    df.loc[future_return > 0.10, 'regime'] = 2   # 牛市
    df.loc[future_return < -0.10, 'regime'] = 0  # 熊市

    return df


REGIME_FEATURES = [
    'mom_5', 'mom_10', 'mom_20', 'mom_60',
    'vol_5', 'vol_20', 'vol_60',
    'ma_alignment', 'ma20_bias', 'ma60_bias',
    'rsi', 'dif', 'dea',
    'vol_ma5_ratio', 'vol_trend',
]


def build_and_train_regime(X_train, y_train, X_val, y_val, epochs=60):
    """训练市场状态分类器

    模型: GRU(32, 2层) → FC → 3分类 (熊市/震荡/牛市)
    选用 GRU 而非 LSTM: 参数更少，对中等规模数据更稳定
    """
    import torch
    import torch.nn as nn
    from torch.utils.data import TensorDataset, DataLoader

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    X_tr = torch.FloatTensor(X_train).to(device)
    y_tr = torch.LongTensor(y_train).to(device)
    X_va = torch.FloatTensor(X_val).to(device)
    y_va = torch.LongTensor(y_val).to(device)

    # 类别权重（处理不平衡）
    class_counts = np.bincount(y_train, minlength=3).astype(float)
    class_weights = 1.0 / (class_counts + 1)
    class_weights = class_weights / class_weights.sum() * 3
    weights = torch.FloatTensor(class_weights).to(device)

    train_dl = DataLoader(TensorDataset(X_tr, y_tr), batch_size=32, shuffle=True)

    class MarketRegimeGRU(nn.Module):
        def __init__(self, n_feat, hidden=32, n_layers=2, n_classes=3):
            super().__init__()
            self.gru = nn.GRU(n_feat, hidden, n_layers, batch_first=True, dropout=0.2)
            self.fc = nn.Sequential(
                nn.Linear(hidden, 16),
                nn.ReLU(),
                nn.Linear(16, n_classes),
            )

        def forward(self, x):
            _, h_n = self.gru(x)
            return self.fc(h_n[-1])

    model = MarketRegimeGRU(X_train.shape[2]).to(device)
    criterion = nn.CrossEntropyLoss(weight=weights)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.002)

    best_acc = 0
    best_state = None

    for epoch in range(epochs):
        model.train()
        for xb, yb in train_dl:
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            optimizer.step()

        model.eval()
        with torch.no_grad():
            val_acc = (model(X_va).argmax(1) == y_va).float().mean().item()
        if val_acc > best_acc:
            best_acc = val_acc
            best_state = model.state_dict().copy()

        if (epoch + 1) % 20 == 0:
            log.info(f"  Epoch {epoch+1}: val_acc={val_acc:.4f}")

    model.load_state_dict(best_state)
    log.info(f"最佳验证准确率: {best_acc:.4f}")
    return model, device


def main():
    parser = argparse.ArgumentParser(description="市场牛熊状态识别")
    parser.add_argument('--index', default='000001.SH', help='指数代码')
    parser.add_argument('--lookback', type=int, default=20, help='回看天数')
    parser.add_argument('--epochs', type=int, default=60, help='训练轮数')
    args = parser.parse_args()

    t0 = time.time()

    # 加载数据
    df = load_index_data(args.index)
    df = compute_regime_features(df)

    # 构建序列
    feat_df = df[REGIME_FEATURES + ['regime', 'trade_date']].dropna()
    features = feat_df[REGIME_FEATURES].values.astype(np.float32)
    labels = feat_df['regime'].values.astype(np.int64)

    # 标准化
    mean = features.mean(axis=0)
    std = features.std(axis=0) + 1e-8
    features = (features - mean) / std

    # 构建序列
    X, y = [], []
    for i in range(args.lookback, len(features)):
        X.append(features[i - args.lookback:i])
        y.append(labels[i])
    X, y = np.array(X), np.array(y)

    # 划分
    n = len(X)
    tr_end = int(n * 0.7)
    va_end = int(n * 0.85)

    log.info(f"数据集: 总={n}, 熊={sum(y==0)}, 震荡={sum(y==1)}, 牛={sum(y==2)}")

    # 训练
    model, device = build_and_train_regime(
        X[:tr_end], y[:tr_end], X[tr_end:va_end], y[tr_end:va_end],
        epochs=args.epochs
    )

    # 测试
    import torch
    from sklearn.metrics import classification_report

    model.eval()
    X_test = torch.FloatTensor(X[va_end:]).to(device)
    with torch.no_grad():
        pred = model(X_test).argmax(1).cpu().numpy()
    y_test = y[va_end:]

    report = classification_report(y_test, pred,
                                   target_names=['熊市', '震荡', '牛市'], digits=4)
    acc = (pred == y_test).mean()

    print(f"\n{'='*60}")
    print(f"  {args.index} 市场状态识别结果")
    print(f"{'='*60}")
    print(f"  测试集准确率: {acc:.4f}")
    print(f"\n{report}")
    print(f"  耗时: {time.time()-t0:.1f}s")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
