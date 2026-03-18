# PyTorch 股票量化投资调研

## 目录结构

| 文件 | 说明 |
|------|------|
| `demo_lstm_prediction.py` | LSTM 多因子次日涨跌预测示例 |
| `demo_market_regime.py` | 牛熊市场特征识别示例 |
| `demo_price_distribution.py` | 涨跌分布分析与可视化 |

## 运行方式

```bash
# 确保在 my-stock conda 环境
conda activate my-stock

# 安装 PyTorch（如未安装）
pip install torch>=2.6

# 运行示例
python research/pytorch/demo_lstm_prediction.py
python research/pytorch/demo_market_regime.py
python research/pytorch/demo_price_distribution.py
```

## 数据来源

示例代码直接从项目数据库读取 Tushare 已同步的日线数据，无需额外下载。
