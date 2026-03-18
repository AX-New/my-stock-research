"""PyTorch LSTM 多因子次日涨跌预测示例

基于多个技术指标（MACD、RSI、MA、换手率、成交量）预测次日涨跌方向。
演示完整的深度学习量化投资流水线：数据加载 → 特征工程 → 模型训练 → 评估。

核心思路：
  - 用过去 N 天的多因子特征作为输入序列
  - LSTM 编码时序依赖关系
  - 输出次日涨跌方向（二分类：涨/跌）

用法:
  python research/pytorch/demo_lstm_prediction.py --code 000001.SZ --days 60
  python research/pytorch/demo_lstm_prediction.py --code 600519.SH --lookback 30 --epochs 100
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

log = get_logger("research.pytorch.lstm_prediction")

# ────────────────────────────── 数据加载 ──────────────────────────────

def load_stock_data(ts_code: str, min_rows: int = 500) -> pd.DataFrame:
    """从数据库加载前复权日线数据

    Args:
        ts_code: 股票代码，如 '000001.SZ'
        min_rows: 最少需要的数据行数

    Returns:
        DataFrame 包含 OHLCV + 前复权价格
    """
    sql = text("""
        SELECT trade_date, open, high, low, close, vol, amount,
               pre_close, pct_chg
        FROM market_daily
        WHERE ts_code = :code
        ORDER BY trade_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"code": ts_code})

    if len(df) < min_rows:
        log.warning(f"{ts_code} 数据不足 {min_rows} 行，实际 {len(df)} 行")

    df['trade_date'] = pd.to_datetime(df['trade_date'])
    log.info(f"加载 {ts_code} 数据: {len(df)} 行, {df['trade_date'].min()} ~ {df['trade_date'].max()}")
    return df


# ────────────────────────────── 特征工程 ──────────────────────────────

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标特征

    特征列表 (共 15 个):
      - 价格类: pct_chg, 振幅, 实体比例
      - 均线类: MA5/10/20/60 偏离度
      - MACD: DIF, DEA, MACD柱
      - RSI: RSI_14
      - 量价类: 量比, 换手率变化
    """
    c = df['close'].copy()

    # 1. 价格类特征
    df['return_1d'] = c.pct_change()  # 日收益率
    df['amplitude'] = (df['high'] - df['low']) / df['pre_close']  # 振幅
    df['body_ratio'] = abs(c - df['open']) / (df['high'] - df['low'] + 1e-8)  # 实体/影线比

    # 2. 均线偏离度（价格相对 MA 的百分比偏差）
    for period in [5, 10, 20, 60]:
        ma = c.rolling(period).mean()
        df[f'ma{period}_bias'] = (c - ma) / (ma + 1e-8)

    # 3. MACD (12, 26, 9)
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df['dif'] = ema12 - ema26
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = 2 * (df['dif'] - df['dea'])

    # 4. RSI_14
    delta = c.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / (avg_loss + 1e-8)
    df['rsi_14'] = 100 - 100 / (1 + rs)

    # 5. 量价特征
    df['vol_ratio'] = df['vol'] / (df['vol'].rolling(5).mean() + 1e-8)  # 量比
    df['vol_change'] = df['vol'].pct_change()  # 成交量变化率

    # 标签：次日涨跌方向（1=涨, 0=跌）
    df['target'] = (df['return_1d'].shift(-1) > 0).astype(int)

    return df


FEATURE_COLS = [
    'return_1d', 'amplitude', 'body_ratio',
    'ma5_bias', 'ma10_bias', 'ma20_bias', 'ma60_bias',
    'dif', 'dea', 'macd_hist',
    'rsi_14',
    'vol_ratio', 'vol_change',
]


# ────────────────────────────── 数据集构建 ──────────────────────────────

def build_sequences(df: pd.DataFrame, lookback: int = 20):
    """构建 LSTM 输入序列

    Args:
        df: 含特征和标签的 DataFrame
        lookback: 回看窗口天数

    Returns:
        X: shape (N, lookback, n_features) 的 numpy 数组
        y: shape (N,) 的标签
        dates: 对应的日期列表
    """
    # 只保留特征列，去掉 NaN 行
    feature_df = df[FEATURE_COLS + ['target', 'trade_date']].dropna()

    features = feature_df[FEATURE_COLS].values.astype(np.float32)
    labels = feature_df['target'].values.astype(np.int64)
    dates = feature_df['trade_date'].values

    # Z-Score 标准化（用滚动窗口避免未来数据泄漏）
    # 简化版：用全局统计量（示例用途）
    mean = features.mean(axis=0)
    std = features.std(axis=0) + 1e-8
    features = (features - mean) / std

    X, y, d = [], [], []
    for i in range(lookback, len(features) - 1):
        X.append(features[i - lookback:i])
        y.append(labels[i])
        d.append(dates[i])

    return np.array(X), np.array(y), d, mean, std


# ────────────────────────────── PyTorch 模型 ──────────────────────────────

def build_and_train(X_train, y_train, X_val, y_val, epochs=50, lr=0.001):
    """构建 LSTM 模型并训练

    模型结构:
      Input (lookback, 13) → LSTM(64, 2层) → Dropout(0.3) → FC(64→32) → FC(32→2)
    """
    import torch
    import torch.nn as nn
    from torch.utils.data import TensorDataset, DataLoader

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    log.info(f"使用设备: {device}")

    # 转为 Tensor
    X_tr = torch.FloatTensor(X_train).to(device)
    y_tr = torch.LongTensor(y_train).to(device)
    X_va = torch.FloatTensor(X_val).to(device)
    y_va = torch.LongTensor(y_val).to(device)

    train_ds = TensorDataset(X_tr, y_tr)
    train_dl = DataLoader(train_ds, batch_size=64, shuffle=True)

    # LSTM 分类模型
    class StockLSTM(nn.Module):
        def __init__(self, input_dim, hidden_dim=64, num_layers=2, num_classes=2):
            super().__init__()
            self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers,
                                batch_first=True, dropout=0.3)
            self.fc = nn.Sequential(
                nn.Linear(hidden_dim, 32),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(32, num_classes),
            )

        def forward(self, x):
            # x: (batch, seq_len, features)
            lstm_out, (h_n, _) = self.lstm(x)
            # 取最后一个时间步的输出
            last_hidden = h_n[-1]  # (batch, hidden_dim)
            return self.fc(last_hidden)

    n_features = X_train.shape[2]
    model = StockLSTM(n_features).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)

    best_val_acc = 0
    best_state = None

    log.info(f"开始训练: {epochs} 轮, 训练集 {len(X_train)}, 验证集 {len(X_val)}")

    for epoch in range(epochs):
        model.train()
        total_loss = 0
        for xb, yb in train_dl:
            optimizer.zero_grad()
            pred = model(xb)
            loss = criterion(pred, yb)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += loss.item()

        # 验证
        model.eval()
        with torch.no_grad():
            val_pred = model(X_va)
            val_loss = criterion(val_pred, y_va).item()
            val_acc = (val_pred.argmax(1) == y_va).float().mean().item()

        scheduler.step(val_loss)

        if val_acc > best_val_acc:
            best_val_acc = val_acc
            best_state = model.state_dict().copy()

        if (epoch + 1) % 10 == 0:
            log.info(f"  Epoch {epoch+1}/{epochs}: loss={total_loss/len(train_dl):.4f}, "
                     f"val_loss={val_loss:.4f}, val_acc={val_acc:.4f}")

    # 恢复最佳模型
    model.load_state_dict(best_state)
    log.info(f"训练完成, 最佳验证准确率: {best_val_acc:.4f}")

    return model, device


# ────────────────────────────── 评估 ──────────────────────────────

def evaluate(model, X_test, y_test, device):
    """评估模型性能"""
    import torch
    from sklearn.metrics import classification_report, confusion_matrix

    model.eval()
    X_te = torch.FloatTensor(X_test).to(device)

    with torch.no_grad():
        pred = model(X_te).argmax(1).cpu().numpy()

    # 分类报告
    report = classification_report(y_test, pred, target_names=['跌', '涨'], digits=4)
    cm = confusion_matrix(y_test, pred)
    acc = (pred == y_test).mean()

    log.info(f"\n测试集准确率: {acc:.4f}")
    log.info(f"\n分类报告:\n{report}")
    log.info(f"\n混淆矩阵:\n{cm}")

    return {
        'accuracy': acc,
        'predictions': pred,
        'report': report,
        'confusion_matrix': cm,
    }


# ────────────────────────────── 主流程 ──────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="LSTM 多因子次日涨跌预测")
    parser.add_argument('--code', default='000001.SZ', help='股票代码')
    parser.add_argument('--lookback', type=int, default=20, help='回看天数')
    parser.add_argument('--epochs', type=int, default=50, help='训练轮数')
    parser.add_argument('--lr', type=float, default=0.001, help='学习率')
    args = parser.parse_args()

    t0 = time.time()

    # 1. 加载数据
    df = load_stock_data(args.code)
    if len(df) < 200:
        log.error("数据不足，退出")
        return

    # 2. 特征工程
    df = compute_features(df)

    # 3. 构建序列
    X, y, dates, mean, std = build_sequences(df, args.lookback)
    log.info(f"序列数据: X={X.shape}, y={y.shape}, 涨比例={y.mean():.4f}")

    # 4. 划分数据集（按时间顺序，不随机打乱）
    n = len(X)
    train_end = int(n * 0.7)
    val_end = int(n * 0.85)

    X_train, y_train = X[:train_end], y[:train_end]
    X_val, y_val = X[train_end:val_end], y[train_end:val_end]
    X_test, y_test = X[val_end:], y[val_end:]

    log.info(f"数据集划分: 训练={len(X_train)}, 验证={len(X_val)}, 测试={len(X_test)}")

    # 5. 训练
    model, device = build_and_train(X_train, y_train, X_val, y_val,
                                     epochs=args.epochs, lr=args.lr)

    # 6. 评估
    results = evaluate(model, X_test, y_test, device)

    elapsed = time.time() - t0
    log.info(f"总耗时: {elapsed:.1f}s")

    # 7. 打印关键结论
    print(f"\n{'='*60}")
    print(f"  {args.code} LSTM 次日涨跌预测结果")
    print(f"{'='*60}")
    print(f"  回看窗口: {args.lookback}天")
    print(f"  特征数: {len(FEATURE_COLS)}")
    print(f"  测试集准确率: {results['accuracy']:.4f}")
    print(f"  基准(随机): {y_test.mean():.4f} (涨的比例)")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
