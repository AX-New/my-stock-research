# PyTorch + Qlib 量化投资框架深度调研报告

> 调研目标：评估 PyTorch 和 Microsoft Qlib 在股票特征挖掘、牛熊识别、涨跌预测中的应用方案
> 日期：2026-03-17

---

## 一、背景与目标

我们的 my-stock 平台已积累了完整的 A 股日线数据（Tushare）和多维技术指标（MACD、RSI、MA、换手率、资金流向），目前的研究主要基于统计分析。引入深度学习框架的目标是：

1. **特征挖掘** — 自动发现有效的价量因子组合
2. **牛熊识别** — 用多维特征识别市场状态（牛/熊/震荡）
3. **涨跌预测** — 基于历史数据预测次日/多日涨跌方向和幅度
4. **涨跌分布建模** — 理解收益率的统计特征，为风控提供依据

---

## 二、PyTorch 量化投资调研

### 2.1 为什么选 PyTorch？

| 维度 | PyTorch | TensorFlow | 结论 |
|------|---------|------------|------|
| 生态 | 学术界主流，最新论文 90%+ 用 PyTorch | 工业部署更成熟 | 研究用 PyTorch 更合适 |
| 灵活性 | 动态图，调试方便 | 静态图为主 | PyTorch 开发效率高 |
| 量化社区 | Qlib/FinRL/pytorch-forecasting 等 | 较少 | PyTorch 生态更丰富 |
| Python 3.13 | ✅ v2.10.0 支持 | ❓ 待确认 | PyTorch 兼容性好 |

### 2.2 常用模型架构

#### LSTM（长短期记忆网络）
```
Input (T×F) → LSTM(hidden=64, layers=2) → FC → Output
```
- **优势**：捕捉时序依赖，处理变长序列
- **适用**：单股票次日涨跌预测、趋势跟踪信号
- **局限**：难以建模股票间关系，训练较慢

#### GRU（门控循环单元）
- LSTM 的简化版，参数更少
- 在中等规模数据上通常与 LSTM 表现接近
- 适合作为快速原型验证

#### Transformer
```
Input (T×F) → Positional Encoding → Multi-Head Attention × N → FC → Output
```
- **优势**：并行计算，长距离依赖建模更强
- **适用**：全市场多股票联合建模
- **注意**：需要更多数据，小样本容易过拟合
- **代表工作**：Stockformer、MASTER（Market-Guided Stock Transformer）

#### 时序卷积网络（TCN）
- 因果卷积 + 空洞卷积
- 计算效率高于 RNN，适合实时推理
- 在某些 benchmark 上优于 LSTM

### 2.3 特征工程方案

深度学习量化的关键不仅是模型，更是特征设计：

```
原始数据 (OHLCV)
    ├── 价格特征: 开高低收比例关系、K线形态
    ├── 动量特征: 多周期收益率 (1/5/10/20/60日)
    ├── 均线特征: MA偏离度、均线排列得分
    ├── 波动特征: 多周期波动率、ATR
    ├── 技术指标: RSI、MACD、KDJ
    ├── 量价特征: 量比、量价相关性
    └── 市场特征: 行业涨跌、大盘状态
           ↓
    标准化 (Z-Score / MinMax)
           ↓
    序列构建 (lookback=20天)
           ↓
    模型输入 (batch, seq_len, n_features)
```

### 2.4 牛熊市场特征识别方案

**方法一：监督学习分类**
- 标签定义：未来N天收益率 > 阈值 = 牛市，< -阈值 = 熊市
- 输入：多维技术指标的时间序列
- 模型：GRU/LSTM 三分类器
- 难点：标签定义主观、边界模糊

**方法二：隐马尔可夫模型 (HMM)**
- 无监督学习，自动发现隐状态
- 可结合 PyTorch 的变分推断
- 更符合金融学理论（市场有不同"状态"切换）

**方法三：自编码器 + 聚类**
- VAE 对市场特征降维
- K-Means / DBSCAN 聚类
- 无需人工标注

### 2.5 次日涨跌预测方案

**核心挑战**：股票价格是弱可预测的，信噪比极低。

**推荐策略**：
1. **不预测绝对价格**，预测排名/相对强弱
2. **多因子 + 集成**：先用传统因子选股，再用 DL 优化信号
3. **多任务学习**：同时预测方向 + 幅度，共享底层特征
4. **关注 IC 而非准确率**：IC > 0.03 就有实用价值

```python
# 多任务学习伪代码
class MultiTaskModel(nn.Module):
    def __init__(self):
        self.shared = LSTM(input_dim, hidden_dim)
        self.direction_head = Linear(hidden_dim, 2)   # 涨/跌
        self.magnitude_head = Linear(hidden_dim, 1)    # 涨跌幅

    def forward(self, x):
        shared_feat = self.shared(x)
        direction = self.direction_head(shared_feat)
        magnitude = self.magnitude_head(shared_feat)
        return direction, magnitude
```

### 2.6 环境兼容性

| 组件 | 版本 | 兼容性 |
|------|------|--------|
| Python | 3.13.12 | ✅ |
| PyTorch | 2.10.0 | ✅ 支持 Python 3.13 |
| NumPy | 2.4.2 | ✅ |
| scikit-learn | 1.8.0 | ✅ |
| CUDA | 可选 | CPU 也可运行 |

安装命令：
```bash
conda activate my-stock
pip install torch>=2.6
```

---

## 三、Microsoft Qlib 量化框架调研

### 3.1 Qlib 概述

Qlib 是微软开源的 AI 量化投资平台，目标是用 AI 赋能量化研究全流程。

**架构层次**：
```
┌─────────────────────────────────────────┐
│  Workflow Layer (qrun/backtest/report)   │
├─────────────────────────────────────────┤
│  Model Layer (LightGBM/LSTM/Transformer)│
├─────────────────────────────────────────┤
│  Feature Layer (Alpha158/Alpha360)      │
├─────────────────────────────────────────┤
│  Data Layer (bin存储/高速读取)            │
└─────────────────────────────────────────┘
```

### 3.2 核心组件

#### Alpha158 因子体系
158 个精心设计的因子，分为：
- **KBAR (8)**：K线形态因子（开高低收比例）
- **MOM (动量)**：多周期收益率、高低点突破
- **VOLUME (量价)**：量比、量价相关性
- **VOLATILITY (波动)**：多周期波动率、ATR
- **TECH (技术)**：RSI、MACD、均线
- **CORR (相关)**：价量滚动相关性

#### Alpha360
- 360 维原始特征（60天×6个OHLCV字段）
- 不做人工特征工程，让模型自行学习
- 适合深度学习模型

#### 内置模型

| 模型 | IC (Alpha158/CSI300) | 年化收益 | 类型 |
|------|---------------------|---------|------|
| LightGBM | 0.0399 | 12.84% | 树模型 |
| MLP | ~0.038 | ~11% | 神经网络 |
| LSTM | ~0.035 | ~10% | 循环网络 |
| Transformer | ~0.040 | ~13% | 注意力 |
| ALSTM (Attention) | ~0.042 | ~14% | 注意力+LSTM |
| TRA | ~0.045 | ~15% | 时变路由 |

> 注：以上为 Qlib 官方 benchmark，基于 CSI300 成分股，20次不同随机种子的平均值

### 3.3 兼容性问题

**⚠️ 关键：Qlib (pyqlib) 目前不支持 Python 3.13**

| 项目 | 支持版本 | 我们的版本 | 状态 |
|------|---------|-----------|------|
| pyqlib | Python 3.8-3.12 | Python 3.13 | ❌ 不兼容 |

**原因**：pyqlib 依赖 Cython 编译的 C 扩展（用于高速数据读取），尚未发布 Python 3.13 的 wheel。

### 3.4 替代方案：手动复现 Qlib 核心逻辑

由于 Qlib 包不可直接安装，我们采用**"学其思想，用自己的代码"**策略：

```
Qlib 的核心价值             我们的复现方案
─────────────────────    ─────────────────────
Alpha158 因子体系    →    demo_alpha158_features.py (已实现 ~50 个核心因子)
LightGBM 预测流水线  →    demo_lightgbm_prediction.py (用 sklearn/LightGBM)
bin 数据存储         →    直接用 MySQL (已有完整数据)
分组评估 (IC/ICIR)   →    自行实现评估逻辑
```

### 3.5 Tushare 数据接入 Qlib（未来方案）

如果后续需要使用完整的 Qlib，可以通过以下方式接入 Tushare 数据：

```bash
# 1. 用 Tushare 下载数据为 CSV
python scripts/dump_tushare_csv.py --start 20150101 --end 20260317

# 2. 用 Qlib 的 dump_bin.py 转换格式
python scripts/dump_bin.py dump_all \
  --csv_path ~/.qlib/csv_data/cn_data \
  --qlib_dir ~/.qlib/qlib_data/cn_data \
  --include_fields open,close,high,low,volume,factor

# 3. 初始化 Qlib
import qlib
qlib.init(provider_uri="~/.qlib/qlib_data/cn_data", region="cn")
```

---

## 四、涨跌分布分析洞察

### 4.1 A股日收益率的典型特征

基于统计学研究，A 股日收益率具有以下特征：

1. **非正态分布**：呈现"尖峰厚尾"特征，极端涨跌概率远高于正态分布预测
2. **右偏（微弱）**：均值略大于 0，反映长期向上趋势
3. **波动聚集**：高波动日后通常跟着高波动（GARCH 效应）
4. **涨跌不对称**：下跌速度快于上涨（恐慌效应）
5. **连涨/连跌**：存在动量效应，连续涨跌的概率高于随机

### 4.2 对建模的启示

| 特征 | 对建模的影响 | 建议 |
|------|------------|------|
| 厚尾分布 | MSE 损失会被极端值主导 | 用 Huber Loss 或分位数回归 |
| 波动聚集 | 单一模型难以适应不同波动状态 | 加入波动率特征，或用 regime-switching 模型 |
| 涨跌不对称 | 上涨/下跌的预测难度不同 | 分别建模或用不对称损失函数 |
| 动量效应 | 短期内存在可利用的趋势 | 动量因子是最有效的因子之一 |

---

## 五、实操建议：引入路径

### 5.1 第一阶段：基础验证（当前）

```
目标：验证深度学习在我们数据上是否有效
工具：PyTorch + scikit-learn
任务：
  ✅ 涨跌分布统计分析（demo_price_distribution.py）
  ✅ LSTM 次日涨跌预测（demo_lstm_prediction.py）
  ✅ GRU 牛熊状态识别（demo_market_regime.py）
  ✅ Alpha158 因子 IC 排名（demo_alpha158_features.py）
  ✅ LightGBM 多因子预测（demo_lightgbm_prediction.py）
```

### 5.2 第二阶段：全市场验证

```
目标：从单股票扩展到全市场
新增：
  □ 全市场 Alpha158 因子计算（写入数据库）
  □ 日级 IC 跟踪（每天记录因子有效性）
  □ 分行业/分市值建模
  □ 回测框架集成（与现有 backtest_service 对接）
```

### 5.3 第三阶段：高级模型

```
目标：探索更强的模型
新增：
  □ Transformer + Cross-Attention（捕捉板块联动）
  □ Graph Neural Network（股票关联图谱）
  □ 强化学习（组合优化）
  □ 等 Qlib 支持 Python 3.13 后整体迁移
```

### 5.4 安装依赖

```bash
# 核心依赖（PyTorch）
pip install torch>=2.6

# 可选依赖
pip install lightgbm    # 更快的树模型
pip install shap        # 因子可解释性
```

---

## 六、关键结论

### 6.1 PyTorch

| 结论 | 说明 |
|------|------|
| ✅ 推荐使用 | 生态成熟，Python 3.13 兼容，灵活性强 |
| 适合场景 | 时序预测（LSTM/GRU）、特征挖掘、牛熊识别 |
| 预期效果 | 单模型 IC ~0.03-0.05，方向准确率 ~52-55% |
| 核心价值 | 自动特征组合 > 手工规则，尤其在多因子融合时 |

### 6.2 Qlib

| 结论 | 说明 |
|------|------|
| ⚠️ 暂不可直接用 | Python 3.13 不兼容 |
| ✅ 思想值得借鉴 | Alpha158 因子体系、IC评估框架 |
| 替代方案 | 用 Python + scikit-learn 复现核心逻辑 |
| 未来计划 | 等 pyqlib 更新后再整体迁移 |

### 6.3 建议优先级

1. **安装 PyTorch** → 跑通示例代码
2. **全市场 Alpha158 因子 IC 筛选** → 找到最有效的因子
3. **LightGBM 多因子选股** → 快速出可用信号（比深度学习更稳定）
4. **LSTM 辅助** → 在 LightGBM 基础上尝试提升

---

## 七、示例代码说明

| 文件 | 位置 | 功能 | 依赖 |
|------|------|------|------|
| demo_lstm_prediction.py | research/pytorch/ | LSTM 多因子次日涨跌预测 | torch |
| demo_market_regime.py | research/pytorch/ | GRU 牛熊市场状态识别 | torch |
| demo_price_distribution.py | research/pytorch/ | 涨跌分布统计分析 | scipy |
| demo_alpha158_features.py | research/qlib/ | Alpha158 因子计算+IC评估 | 无额外依赖 |
| demo_lightgbm_prediction.py | research/qlib/ | LightGBM 多因子选股预测 | lightgbm(可选) |

所有示例都直接从项目 MySQL 数据库读取已同步的 Tushare 数据，无需额外下载。

---

## 参考资料

- [Microsoft Qlib GitHub](https://github.com/microsoft/qlib) — 官方仓库
- [Qlib Alpha158 Benchmark](https://github.com/microsoft/qlib/blob/main/examples/benchmarks/README.md) — 模型对比结果
- [Qlib 文档 - 安装](https://qlib.readthedocs.io/en/latest/start/installation.html)
- [MASTER: Market-Guided Stock Transformer](https://arxiv.org/html/2312.15235v1) — Transformer 选股论文
- [Stockformer: Transformer for Stock Prediction](https://arxiv.org/html/2502.09625v1) — 股票预测 Transformer
- [Tushare 数据接入 Qlib](https://blog.csdn.net/weixin_38175458/article/details/126507102)
- [Qlib PyTorch MLP on Alpha360](https://www.vadim.blog/qlib-ai-quant-workflow-pytorch-mlp)
