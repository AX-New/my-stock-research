# 热度策略参数优化系统实施计划

**创建时间**: 20260320

## 概述

并行执行两个 Agent：
- **Agent A**（my-stock）：增强 optimizer_service.py，task + sync docs
- **Agent B**（my-stock-research）：建热度策略模块 + 运行优化 + 报告 + 语雀

---

## Agent A：my-stock 优化引擎增强

### 任务文件
`task/project/173-热度策略优化引擎接口.md`

### 实施步骤

**Step 1**：创建 task 文件

**Step 2**：增强 `app/services/optimizer_service.py`
- 在文件末尾追加 `run_callable_optimization()` 函数
- 接受 `strategy_fn: callable`，参数空间，优化目标指标
- 内部使用网格搜索 + 可选 AI 代理模型
- 返回按指标排序的 top N 结果列表

**Step 3**：验证（不需要运行测试，代码审查即可）

**Step 4**：git 工作流
```
git checkout -b task-claude01-optimizer-callable
commit [ADD] - claude01 - optimizer_service 新增 run_callable_optimization 通用接口
rebase + merge master + push + 删除分支
```

**Step 5**：/sync-docs

---

## Agent B：my-stock-research 热度策略优化

### 任务文件
`task/172-热度策略参数优化.md`

### 实施步骤

**Step 1**：创建 task 文件 `task/172-热度策略参数优化.md`

**Step 2**：更新数据库配置文档
- 更新 `CLAUDE.md`：添加 my_trend 通过 SSH 隧道 port 3310 访问的说明
- 更新 `docs/020-my_trend数据表.md`：添加连接信息

**Step 3**：创建目录结构
```
mkdir heat/
mkdir heat/scripts/
mkdir heat/base/
mkdir heat/report/
mkdir heat/data/
```

**Step 4**：实现 `heat/scripts/config.py`
- MY_STOCK_DB_URI: port 3307
- MY_TREND_DB_URI: port 3310

**Step 5**：实现 `heat/scripts/database.py`
- 创建两个引擎

**Step 6**：实现 `heat/scripts/data_loader.py`
- `load_heat_data(start_date, end_date)` - 从 my_trend 加载
- `load_price_data(start_date, end_date)` - 从 my_stock 加载前复权
- `load_trading_days(start_date, end_date)` - 从 my_stock 加载
- `load_index_data(start_date, end_date)` - 沪深300

**Step 7**：实现 `heat/scripts/strategy.py`
- `HeatRotationStrategy` 类
- 提取自 `task/162-热度策略回测/03_heat_rotation_no_timeout.py`
- 支持 n_positions 多仓位
- `run(data_bundle, params)` → metrics + orders_df

**Step 8**：实现 `heat/scripts/optimizer.py`
- `HeatOptimizer` 类
- Phase 1: 网格搜索（lookback 缓存）
- Phase 2: MLP 代理模型优化
- 输出 optimization_results.csv + best_params.json

**Step 9**：实现 `heat/scripts/run_optimization.py`
- CLI 入口
- 加载数据 → 运行优化 → 生成 orders.csv → 调用 OrderBasedEngine CLI → 打印报告路径

**Step 10**：实现 `heat/scripts/live_signal.py`
- 读取 my_trend 最新数据
- 计算信号
- 维护 state.json 持仓状态
- 输出 signals/ 目录

**Step 11**：实现 `heat/scripts/00-脚本使用说明.md`

**Step 12**：运行优化
```bash
cd F:/projects/my-stock-research
python heat/scripts/run_optimization.py \
    --start 2025-03-15 --end 2026-03-19 \
    --output backtest/output
```
记录运行日志和结果。

**Step 13**：启动 my-stock 前后端测试报告
```bash
# 后端（my-stock）
cd F:/projects/my-stock
uvicorn app.main:app --port 8000
# 前端已 build，访问 http://localhost:8000/report 查看
```

**Step 14**：基于优化结果撰写研究报告 `heat/report/01-热度策略参数优化报告.md`
内容包括：
- 优化方法说明
- 参数敏感性分析（热力图描述）
- 最优参数配置及绩效
- 与 baseline（原始参数）对比
- 多仓位测试结论
- 实盘使用说明

**Step 15**：上传报告到语雀
```bash
python scripts/yuque_upload.py "030 - 指标分析" heat/report/01-热度策略参数优化报告.md
```

**Step 16**：更新 `heat/base/` 基础文档
- `01-策略定义.md` - 热度轮转策略定义
- `02-数据源说明.md` - my_trend 数据源

**Step 17**：git 工作流
```
git checkout -b task-claude01-heat-optimizer
多次 commit（代码/配置/报告分开提交）
rebase + merge master + push + 删除分支
```

**Step 18**：/sync-docs

---

## 依赖关系

- Agent A 和 Agent B 可完全并行（不同项目，不共享状态）
- Agent B Step 12（运行优化）需要 my_trend port 3310 可访问
- Agent B Step 13（前端测试）需要 my-stock 后端正常运行
