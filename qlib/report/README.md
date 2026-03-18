# Microsoft Qlib 量化框架调研

## 目录结构

| 文件 | 说明 |
|------|------|
| `demo_alpha158_features.py` | Alpha158 因子工程示例（纯 Python 实现，不依赖 Qlib） |
| `demo_lightgbm_prediction.py` | LightGBM 多因子选股预测示例 |

## 运行方式

```bash
conda activate my-stock

# 运行示例（不需要安装 Qlib，使用纯 Python 复现核心逻辑）
python research/qlib/demo_alpha158_features.py
python research/qlib/demo_lightgbm_prediction.py
```

## 为什么不直接用 Qlib？

Qlib (pyqlib) 目前只支持 Python 3.8-3.12，我们的环境是 Python 3.13，暂不兼容。
但 Qlib 的核心思想（Alpha 因子体系、模型流水线）可以用 Python + scikit-learn + PyTorch 复现。
示例代码展示了如何在现有 my-stock 架构中实现 Qlib 的核心功能。
