# DIF极值ATR标准化重分析 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将DIF极值幅度分层从原始 |DIF| 改为 DIF/ATR(20) 标准化，消除股价和波动率偏差，重新运行三层级分析并生成报告。

**Architecture:** 在现有 `analyze_dif_extreme_effectiveness.py` 基础上新建 `analyze_dif_extreme_atr.py`，复用原脚本的数据加载、收益计算、统计输出等模块，仅替换幅度度量方式（abs_dif → dif_atr_ratio）。ATR计算作为独立函数内嵌在新脚本中。

**Tech Stack:** Python / pandas / numpy / SQLAlchemy / MySQL

---

## File Structure

| 文件 | 操作 | 职责 |
|------|------|------|
| `macd/scripts/analyze_dif_extreme_atr.py` | 新建 | ATR标准化版DIF极值有效性分析（主脚本） |
| `macd/scripts/00-脚本使用说明.md` | 修改 | 新增脚本说明 |

**设计决策：**
- **不修改原脚本** — 保留原始分析结果作为对照
- **不抽取公共模块** — 原脚本和新脚本都是一次性研究脚本，过度抽象无意义
- **ATR函数内嵌** — 仅此脚本使用，不必放到 lib/

---

## ATR标准化核心逻辑

```python
def calc_atr(high, low, close, period=20):
    """计算ATR（Average True Range）

    TR = max(high-low, |high-prev_close|, |low-prev_close|)
    ATR = TR的period日简单移动平均
    """
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low,
                    np.maximum(np.abs(high - prev_close),
                               np.abs(low - prev_close)))
    atr = pd.Series(tr).rolling(period, min_periods=1).mean().values
    return atr
```

极值检测时：
```python
# 原来: abs_dif = |DIF|
# 现在: dif_atr = |DIF| / ATR(20)
atr = calc_atr(df["high"].values, df["low"].values, df["close"].values, period=20)
# 防止ATR=0（停牌/首行等极端情况）导致除零
dif_atr_ratio = abs(dif) / atr[idx] if atr[idx] > 0 else np.nan
```

分位数分档逻辑不变，只是度量从 `abs_dif` 换成 `dif_atr`。pandas quantile 自动忽略 NaN，但 `_band()` 函数需增加 NaN 检查（`if np.isnan(x): return "unknown"`），避免 NaN 被错误分入 >Q75。

---

## Tasks

### Task 0: 创建工作分支

- [ ] **Step 1: 拉取最新代码并创建分支**

```bash
cd F:/projects/my-stock-research
git pull origin master
git checkout -b task-claude01-dif-atr-normalize
```

### Task 1: 创建ATR标准化分析脚本

**Files:**
- Create: `macd/scripts/analyze_dif_extreme_atr.py`

- [ ] **Step 1: 创建脚本框架**

从 `analyze_dif_extreme_effectiveness.py` 复制完整内容到新文件，修改文件头注释说明这是ATR标准化版本。

- [ ] **Step 2: 添加ATR计算函数**

在常量定义区后添加 `calc_atr()` 函数。

- [ ] **Step 3: 修改 `detect_extremes()` — 增加ATR字段**

关键改动：在检测极值时，利用 df 已有的 high/low/close 列计算ATR，无需修改函数签名。

每个极值点记录中新增：
- `atr`: 该点的ATR(20)值
- `dif_atr`: `|DIF| / ATR` — 核心标准化度量（ATR=0时设为NaN）

保留原有的 `abs_dif` 字段用于对比。

- [ ] **Step 4: 修改 `assign_magnitude_band()` — 改用 dif_atr 分档**

将分位数计算从 `extremes["abs_dif"]` 改为 `extremes["dif_atr"]`。分档逻辑（Q25/Q50/Q75四档）不变。`_band()` 函数增加 `if pd.isna(x): return "unknown"` 守卫，防止 NaN 被错误归档。

- [ ] **Step 5: 修改 `print_magnitude_quantiles()` — 输出ATR标准化分位数**

输出改为显示 `dif_atr` 的分位数信息，同时保留 `abs_dif` 作为参考。

- [ ] **Step 6: 行业指数数据适配**

`load_sw_daily()` 返回的 DataFrame 已包含 open/high/low/close，无需修改。`calc_macd_from_kline()` 也保留了这些列。确认数据流正确。

- [ ] **Step 7: 单指数快速验证**

运行单个指数测试，确认ATR计算和标准化分档输出正确：

```bash
cd F:/projects/my-stock-research/macd/scripts
python analyze_dif_extreme_atr.py --level index --codes 000001.SH:上证指数
```

确认输出中：
1. 有 `dif_atr` 分位数信息
2. 分档标签正确（<Q25 / Q25-Q50 / Q50-Q75 / >Q75）
3. 胜率/均值数据合理

- [ ] **Step 8: Commit**

```bash
git add macd/scripts/analyze_dif_extreme_atr.py
git commit -m "[ADD] - claude01 - DIF极值ATR标准化分析脚本"
```

### Task 2: 三层级完整运行 + 输出保存

**Files:**
- Read: `macd/scripts/analyze_dif_extreme_atr.py`
- Create: `macd/data/dif_atr_index_output.txt`
- Create: `macd/data/dif_atr_industry_output.txt`
- Create: `macd/data/dif_atr_stock_output.txt`

- [ ] **Step 1: 运行 Level 1 — 大盘指数**

```bash
cd F:/projects/my-stock-research/macd/scripts
python analyze_dif_extreme_atr.py --level index > ../data/dif_atr_index_output.txt 2>&1
```

检查输出文件，确认7大指数×3周期的数据完整。

- [ ] **Step 2: 运行 Level 2 — 行业指数**

```bash
python analyze_dif_extreme_atr.py --level industry > ../data/dif_atr_industry_output.txt 2>&1
```

- [ ] **Step 3: 运行 Level 3 — 个股**

```bash
python analyze_dif_extreme_atr.py --level stock > ../data/dif_atr_stock_output.txt 2>&1
```

注意：个股层级耗时较长（预计10-30分钟），确认进度输出正常。

- [ ] **Step 4: Commit 数据文件**

```bash
git add macd/data/dif_atr_*.txt
git commit -m "[ADD] - claude01 - DIF极值ATR标准化三层级分析数据"
```

### Task 3: 撰写分析报告

**Files:**
- Create: `macd/report/11-dif-extreme-atr-normalized.md`
- Read: `macd/data/dif_atr_index_output.txt`
- Read: `macd/data/dif_atr_industry_output.txt`
- Read: `macd/data/dif_atr_stock_output.txt`
- Read: `macd/report/10-dif-extreme-effectiveness.md` (原报告，用于对比)

- [ ] **Step 1: 阅读三层级输出数据**

完整阅读三个输出文件，提取关键统计数据。

- [ ] **Step 2: 与原报告对比分析**

核心对比维度：
1. ATR标准化后，幅度分层的胜率梯度是否更清晰？
2. 原报告中"小幅度更优"的结论是否仍然成立？还是因为消除了股价偏差后结论反转？
3. 跨行业/跨个股的可比性是否改善？（离散度是否降低）

- [ ] **Step 3: 撰写报告**

报告结构：
1. 研究问题与方法（ATR标准化的动机和计算方式）
2. 与原报告的关键差异对比
3. Q1重新回答：ATR标准化后，幅度与信号质量的关系
4. Q2验证：衰减曲线是否受标准化影响（预期不变）
5. 新发现（如有）
6. 结论：是否应将ATR标准化纳入信号系统

命名: `macd/report/11-dif-extreme-atr-normalized.md`

- [ ] **Step 4: Commit**

```bash
git add macd/report/11-dif-extreme-atr-normalized.md
git commit -m "[ADD] - claude01 - DIF极值ATR标准化分析报告"
```

### Task 4: 更新脚本说明

**Files:**
- Modify: `macd/scripts/00-脚本使用说明.md`

- [ ] **Step 1: 在分析脚本表格中新增一行**

在 `analyze_dif_extreme_effectiveness.py` 下方添加：

```
| `analyze_dif_extreme_atr.py` | DIF极值有效性分析（ATR标准化版），用 DIF/ATR(20) 替代原始|DIF|做幅度分层 |
```

- [ ] **Step 2: Commit**

```bash
git add macd/scripts/00-脚本使用说明.md
git commit -m "[MOD] - claude01 - 脚本说明新增ATR标准化分析脚本"
```

### Task 5: 合并分支

- [ ] **Step 1: Rebase并合并到master**

```bash
git fetch origin && git rebase origin/master
git checkout master && git merge task-claude01-dif-atr-normalize && git push origin master
```

- [ ] **Step 2: 删除task分支**

```bash
git branch -d task-claude01-dif-atr-normalize
```
