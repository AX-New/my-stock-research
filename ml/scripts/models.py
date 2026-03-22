"""
ML 模型封装模块

提供统一接口的模型封装：
  - LightGBM: 梯度提升树，速度快、可解释
  - XGBoost: 梯度提升树备选，正则化更强
  - LSTM: PyTorch 深度序列模型，利用 GPU 加速

所有模型统一 fit/predict/evaluate 接口。
"""
import logging
import os
import time
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseModel(ABC):
    """模型基类，统一接口"""

    def __init__(self, name: str, task: str = 'regression'):
        """
        Args:
            name: 模型名称
            task: 'regression' 或 'classification'
        """
        self.name = name
        self.task = task
        self.model = None
        self.train_time = 0

    @abstractmethod
    def fit(self, X_train, y_train, X_val=None, y_val=None):
        pass

    @abstractmethod
    def predict(self, X):
        pass

    def get_feature_importance(self) -> pd.DataFrame:
        """返回特征重要性 DataFrame，列: [feature, importance]"""
        return pd.DataFrame()


class LGBModel(BaseModel):
    """LightGBM 模型"""

    def __init__(self, task='regression', **params):
        super().__init__('LightGBM', task)
        self.params = {
            'n_estimators': 500,
            'max_depth': 6,
            'learning_rate': 0.05,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_samples': 50,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'verbose': -1,
            'n_jobs': -1,
        }
        self.params.update(params)

    def fit(self, X_train, y_train, X_val=None, y_val=None):
        import lightgbm as lgb
        start = time.time()

        if self.task == 'regression':
            self.model = lgb.LGBMRegressor(**self.params)
        else:
            self.model = lgb.LGBMClassifier(**self.params)

        fit_params = {}
        if X_val is not None and y_val is not None:
            fit_params['eval_set'] = [(X_val, y_val)]
            # LightGBM 4.x 使用 callbacks 代替 early_stopping_rounds
            fit_params['callbacks'] = [
                lgb.early_stopping(stopping_rounds=50, verbose=False),
                lgb.log_evaluation(period=0),
            ]

        self.model.fit(X_train, y_train, **fit_params)
        self.train_time = time.time() - start
        logger.info(f"  LightGBM 训练完成: {self.train_time:.1f}s, "
                    f"best_iteration={getattr(self.model, 'best_iteration_', 'N/A')}")

    def predict(self, X):
        if self.task == 'classification':
            return self.model.predict_proba(X)[:, 1]
        return self.model.predict(X)

    def get_feature_importance(self) -> pd.DataFrame:
        if self.model is None:
            return pd.DataFrame()
        imp = self.model.feature_importances_
        names = self.model.feature_name_
        return pd.DataFrame({
            'feature': names,
            'importance': imp
        }).sort_values('importance', ascending=False)


class XGBModel(BaseModel):
    """XGBoost 模型"""

    def __init__(self, task='regression', **params):
        super().__init__('XGBoost', task)
        self.params = {
            'n_estimators': 500,
            'max_depth': 6,
            'learning_rate': 0.05,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 50,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'random_state': 42,
            'verbosity': 0,
            'n_jobs': -1,
            'tree_method': 'hist',  # GPU 可改为 'gpu_hist'
        }
        self.params.update(params)

    def fit(self, X_train, y_train, X_val=None, y_val=None):
        import xgboost as xgb
        start = time.time()

        if self.task == 'regression':
            self.model = xgb.XGBRegressor(**self.params)
        else:
            self.params['eval_metric'] = 'logloss'
            self.model = xgb.XGBClassifier(**self.params)

        fit_params = {}
        if X_val is not None and y_val is not None:
            fit_params['eval_set'] = [(X_val, y_val)]
            fit_params['verbose'] = False

        self.model.fit(X_train, y_train, **fit_params)
        self.train_time = time.time() - start
        logger.info(f"  XGBoost 训练完成: {self.train_time:.1f}s, "
                    f"best_iteration={getattr(self.model, 'best_iteration', 'N/A')}")

    def predict(self, X):
        if self.task == 'classification':
            return self.model.predict_proba(X)[:, 1]
        return self.model.predict(X)

    def get_feature_importance(self) -> pd.DataFrame:
        if self.model is None:
            return pd.DataFrame()
        imp = self.model.feature_importances_
        names = self.model.get_booster().feature_names
        if names is None:
            names = [f'f{i}' for i in range(len(imp))]
        return pd.DataFrame({
            'feature': names,
            'importance': imp
        }).sort_values('importance', ascending=False)


class LSTMModel(BaseModel):
    """PyTorch LSTM 序列模型，利用 GPU 加速"""

    def __init__(self, task='regression', sequence_length=20,
                 hidden_size=64, num_layers=2, dropout=0.2,
                 epochs=50, batch_size=512, lr=0.001):
        super().__init__('LSTM', task)
        self.sequence_length = sequence_length
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.dropout = dropout
        self.epochs = epochs
        self.batch_size = batch_size
        self.lr = lr
        self.scaler = None
        self.device = None

    def fit(self, X_train, y_train, X_val=None, y_val=None):
        """
        训练 LSTM 模型

        X_train: 2D array (n_samples, n_features)
            注意：LSTM 需要时间序列输入，这里自动从 2D 数据构造滑动窗口序列。
            调用方应确保数据已按 (stock_code, date) 排序。
        """
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        start = time.time()

        # 设备选择
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        logger.info(f"  LSTM 设备: {self.device}")

        # 标准化
        from sklearn.preprocessing import StandardScaler
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X_train)

        # 构建序列（如果输入已经是 3D 则直接使用，否则从 2D 构造滑动窗口）
        if X_scaled.ndim == 2:
            X_seq, y_seq = self._make_sequences(X_scaled, y_train)
        else:
            X_seq, y_seq = X_scaled, y_train

        if len(X_seq) == 0:
            logger.warning("  LSTM: 序列数据不足，跳过训练")
            return

        n_features = X_seq.shape[2]
        X_tensor = torch.FloatTensor(X_seq).to(self.device)
        y_tensor = torch.FloatTensor(y_seq).to(self.device)

        dataset = TensorDataset(X_tensor, y_tensor)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)

        # 构建模型
        self.model = _LSTMNet(
            input_size=n_features,
            hidden_size=self.hidden_size,
            num_layers=self.num_layers,
            dropout=self.dropout,
            task=self.task,
        ).to(self.device)

        optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr)
        if self.task == 'regression':
            criterion = nn.MSELoss()
        else:
            criterion = nn.BCEWithLogitsLoss()

        # 训练循环
        self.model.train()
        best_loss = float('inf')
        patience_counter = 0
        for epoch in range(self.epochs):
            epoch_loss = 0
            for X_batch, y_batch in loader:
                optimizer.zero_grad()
                output = self.model(X_batch).squeeze()
                loss = criterion(output, y_batch)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item() * len(X_batch)

            avg_loss = epoch_loss / len(X_seq)
            if avg_loss < best_loss:
                best_loss = avg_loss
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= 10:
                    logger.info(f"  LSTM 早停 epoch {epoch+1}, best_loss={best_loss:.6f}")
                    break

            if (epoch + 1) % 10 == 0:
                logger.info(f"  LSTM epoch {epoch+1}/{self.epochs}, loss={avg_loss:.6f}")

        self.train_time = time.time() - start
        logger.info(f"  LSTM 训练完成: {self.train_time:.1f}s, final_loss={best_loss:.6f}")

    def predict(self, X):
        import torch
        if self.model is None or self.scaler is None:
            return np.zeros(len(X))

        X_scaled = self.scaler.transform(X)
        X_seq, _ = self._make_sequences(X_scaled, np.zeros(len(X_scaled)))

        if len(X_seq) == 0:
            return np.zeros(len(X))

        self.model.eval()
        with torch.no_grad():
            X_tensor = torch.FloatTensor(X_seq).to(self.device)
            output = self.model(X_tensor).squeeze().cpu().numpy()

        if self.task == 'classification':
            output = 1 / (1 + np.exp(-output))  # sigmoid

        # 补齐前 sequence_length-1 个没有序列的样本
        padding = np.full(self.sequence_length - 1, np.nan)
        return np.concatenate([padding, output])

    def _make_sequences(self, X, y):
        """从 2D 数据构造 3D 滑动窗口序列"""
        seq_len = self.sequence_length
        if len(X) < seq_len:
            return np.array([]), np.array([])

        sequences = []
        targets = []
        for i in range(seq_len - 1, len(X)):
            sequences.append(X[i - seq_len + 1: i + 1])
            targets.append(y[i])

        return np.array(sequences), np.array(targets)


class _LSTMNet:
    """PyTorch LSTM 网络（延迟导入 torch.nn.Module）"""
    pass


# 实际的 LSTM 网络定义（只在 torch 可用时生效）
try:
    import torch
    import torch.nn as nn

    class _LSTMNet(nn.Module):
        def __init__(self, input_size, hidden_size=64, num_layers=2,
                     dropout=0.2, task='regression'):
            super().__init__()
            self.lstm = nn.LSTM(
                input_size=input_size,
                hidden_size=hidden_size,
                num_layers=num_layers,
                dropout=dropout if num_layers > 1 else 0,
                batch_first=True,
            )
            self.fc = nn.Sequential(
                nn.Linear(hidden_size, 32),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(32, 1),
            )

        def forward(self, x):
            lstm_out, _ = self.lstm(x)
            # 取最后一个时间步的输出
            last_out = lstm_out[:, -1, :]
            return self.fc(last_out)

except ImportError:
    logger.info("PyTorch 未安装，LSTM 模型不可用")
