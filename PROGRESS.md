# 研究经验与踩坑

## K线复权方案

- **后复权(hfq)**: `调整价 = 原始价 × adj_factor`
- **前复权(qfq)**: `调整价 = 原始价 × (adj_factor / 最新adj_factor)`，必须实时计算不能预存
- **个股指标计算禁止 bfq**，bfq 在拆股/送股日产生虚假跳变

## 批量回测 price_lookup 性能瓶颈（2026-03-20）

回测优化器对 1500 个参数组合逐一运行时，每次在 `_simulate_single()` 内用 `iterrows()` 从 130 万行 price_df 重建 price_lookup 字典，耗时 ~25s/组合，全量估算 10+ 小时。

**解决**：在 `data_loader.load_data_bundle()` 中用 numpy 数组索引预构建 price_lookup 一次，optimizer 通过 `prepare_lookups()` 复用，每次回测只需查询不需重建。全量 1500 组合实际耗时 11 分钟。

**规律**：批量回测时任何在"每次评估函数内部"的 O(N) 构建操作都应提到外层只做一次。

## la_factor 首日无法计算（2026-03-13）

la_factor 第一个交易日（20230103）无法计算 → 无前一天数据做交叉信号。脚本默认跳过，断点续算安全。

