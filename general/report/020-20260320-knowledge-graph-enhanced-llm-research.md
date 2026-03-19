# 知识图谱增强 LLM 上下文感知能力：技术调研与项目适配分析
**创建时间**: 20260320 15:00

## 摘要

本报告系统调研知识图谱（Knowledge Graph, KG）增强大语言模型（LLM）上下文感知能力的技术现状，涵盖 GraphRAG 技术原理与实现、学术研究前沿、本地工具链方案，以及 my-stock-research 项目的知识图谱适配性分析。核心结论：**知识图谱技术已从学术走向工程成熟阶段，GraphRAG 在多跳推理场景准确率超传统 RAG 1.5-3.4 倍；对本项目而言，轻量级知识图谱方案（FalkorDB + Graphiti MCP）可解决信号版本追踪和跨主题关联发现的核心痛点。**

---

## 一、背景与动机

### 1.1 观察到的现象

本项目 `my-stock-research` 积累了 **54 篇研究报告、108 个分析脚本、覆盖 6 大技术指标主题**的量化研究知识库。在使用大模型进行分析时，发现当上下文中包含大量已有分析报告时，模型的分析能力有明显增强——能够引用历史结论、识别跨主题模式、避免重复研究。

### 1.2 核心问题

当前的知识组织方式（目录结构 + Markdown 文件）存在以下瓶颈：

| 瓶颈 | 表现 |
|------|------|
| **引用关系隐式化** | 54 个报告之间几乎没有显式超链接，关系只存在于文本内容中 |
| **结论散布** | 核心结论（如"MACD 金叉在牛市胜率 89%"）散布在各报告正文，无法按信号直接查询 |
| **版本不可追踪** | DIF 极值信号在报告 07/10/11/12 中有 4 种定义版本，无机制标注最新定论 |
| **废弃结论未标记** | 综合报告列出 10 条"应废弃"结论，但原始报告未做任何标记 |
| **上下文窗口有限** | 即使 1M token 上下文，也无法同时加载全部报告 |

**问题**：能否通过知识图谱技术，让 LLM 在有限上下文窗口内获得更强的知识关联和推理能力？

---

## 二、GraphRAG 技术现状

### 2.1 核心原理：与传统 RAG 的本质区别

传统 RAG（Retrieval-Augmented Generation）将文档切块后做向量相似度检索，存在两个根本缺陷：
- **缺乏全局理解**：每个文本块独立存在，无法感知跨文档关联
- **无法多跳推理**：对"A 的上级是 B，B 审批了 C"这类多步问题准确率接近 0%

GraphRAG 在索引阶段引入**知识图谱构建**，将文本转化为实体-关系网络，通过图遍历算法检索：

| 维度 | 传统向量 RAG | GraphRAG |
|------|-------------|---------|
| 数据结构 | 文本片段向量 | 实体节点 + 关系边 + 社区摘要 |
| 检索方式 | 向量相似度 | 图遍历 + 向量混合 |
| 跨文档推理 | 弱 | 强（通过关系边直连） |
| 全局理解 | 无 | 社区聚类后的层级摘要 |
| 可解释性 | 低（黑盒相似度） | 高（可追溯关系路径） |
| 抗幻觉 | 中 | 强（事实锚定在图结构） |

### 2.2 效果数据：GraphRAG 的优势场景

**FalkorDB 基准测试（2025）**：
- GraphRAG 准确率 **80%** vs 传统 RAG **50.83%**
- 复杂查询场景：GraphRAG 高出传统 RAG **2 倍**
- Schema 约束查询：传统 RAG 得分 **0%**，GraphRAG 恢复至 **90%+**
- 多实体跨查询（>5 实体）：传统 RAG **0%**，GraphRAG 保持稳定

**HippoRAG v2（俄亥俄州立大学，NeurIPS'24）**：
- MuSiQue 多跳 QA：F1 **51.9** vs 纯向量 RAG **44.8**
- 2WikiMultiHop Recall@5：**90.4%** vs **76.5%**
- 索引 Token 消耗仅为传统方案的 **8%**

**GraphRAG 明显占优的场景**：
1. 多跳推理查询（跨越 2 步以上实体关联）
2. 全局摘要性问题（"主要主题是什么？""关键角色和关系？"）
3. 关系密集型数据（法律、金融、医学文献）
4. 跨文档知识整合（交叉引用和矛盾检测）

### 2.3 主要实现方案对比

| 方案 | 机构 | 索引成本 | 查询延迟 | 多跳推理 | 适用规模 |
|------|------|---------|---------|---------|---------|
| **微软 GraphRAG v2** | Microsoft | 高（v2 降低 77%） | 中 | 极强 | 大 |
| **LazyGraphRAG** | Microsoft | 极低（降低 99%） | 高（3-5s） | 强 | 超大 |
| **LightRAG** | HKUDS | 低 | 低（~80ms） | 强 | 中大 |
| **HippoRAG v2** | OSU | 低（节省 92%） | 中 | 极强 | 中 |
| **nano-graphrag** | 社区 | 低 | 中 | 中 | 小中 |

**微软 GraphRAG** 是开山之作，但索引成本极高（大语料库约 $33,000）。**LazyGraphRAG**（2025.6）将其降至 $33（-99%），**LightRAG** 同等精度下 Token 消耗减少 90%，已发表于 EMNLP 2025。

---

## 三、学术研究前沿

### 3.1 三大研究范式

由 IEEE TKDE 综述论文 *"Unifying Large Language Models and Knowledge Graphs: A Roadmap"*（Pan et al., 2024）定义：

| 范式 | 方向 | 目标 |
|------|------|------|
| **KG-enhanced LLM** | 将 KG 知识注入 LLM | 减少幻觉、提升事实准确性 |
| **LLM-augmented KG** | 用 LLM 辅助 KG 构建 | 自动化知识图谱构建 |
| **Synergized LLM+KG** | 两者双向协作 | 复杂多跳推理、知识可追溯 |

### 3.2 关键论文与成果

**Think-on-Graph (ToG)** — ICLR 2024
- LLM 作为智能体在 KG 上执行波束搜索，迭代探索实体关系路径
- GrailQA 提升 **51.8%**，Zero-Shot RE 提升 **42.9%**
- 小参数 LLM + ToG 可在特定任务超越 GPT-4

**Reasoning on Graphs (RoG)** — ICLR 2024
- 规划-检索-推理三阶段框架
- WebQSP Hits@1 提升 **4.4%**，CWQ Hits@1 提升 **22.3%**

**Graph-constrained Reasoning (GCR)** — ICML 2025
- 构建 KG-Trie 约束解码，每步 token 生成只允许沿合法 KG 路径延伸
- **推理路径幻觉率降至零**

**Think-on-Graph 2.0** — ICLR 2025
- 混合 RAG：同时从非结构化文本和结构化 KG 迭代检索
- GPT-3.5 在 7 个知识密集型数据集中 6 个达到 SOTA

### 3.3 知识图谱减少幻觉的三阶段机制

```
预训练阶段 → 将 KG 实体关系嵌入预训练目标（ERNIE 等）
    ↓
推理阶段（最活跃）→ 实时从 KG 检索事实路径，约束 LLM 生成
    ↓
后生成校验 → 将输出分解为三元组 → 与 KG 比对 → 修正不实信息
```

实验数据：
- 临床问答（本体 KG）：准确率达 **98%**
- GCR（KGQA）：推理路径幻觉率 **0%**
- 知识图谱引导的泛癌 QA：超越 SOTA **33%**

---

## 四、本地工具链与开源方案

### 4.1 图数据库选型

| 数据库 | 存储模型 | 查询语言 | 许可证 | 适合场景 |
|--------|----------|----------|--------|----------|
| **Neo4j Community** | 磁盘图 | Cypher | GPL-3 | 最成熟生态 |
| **FalkorDB** | 内存图（Redis） | Cypher | SSPL | KG+LLM 最佳，AI-native |
| **Memgraph** | 内存图（C++） | Cypher | BSL-1.1 | 实时低延迟 |
| **Apache AGE** | PostgreSQL 扩展 | openCypher+SQL | Apache-2 | 已有 PG 基础设施 |

**推荐 FalkorDB**：Docker 一键部署，与 Graphiti MCP Server 深度集成，专为 GraphRAG 设计。

### 4.2 KG 构建工具

| 工具 | 功能 | 集成 |
|------|------|------|
| **LangChain LLMGraphTransformer** | LLM 提取结构化图数据 | Neo4j/Memgraph 原生支持 |
| **LlamaIndex KnowledgeGraphIndex** | 文档自动提取三元组 | Neo4j/FalkorDB |
| **Graphiti** | 时序知识图谱，跟踪事实随时间变化 | Neo4j/FalkorDB/Neptune |
| **Neo4j LLM Graph Builder** | Web UI 无代码建图 | Neo4j |

### 4.3 MCP + 知识图谱（最值得关注的方向）

**MCP（Model Context Protocol）已成为 LLM 集成知识图谱的主流通道**：

| MCP 服务器 | 功能 | 推荐度 |
|-----------|------|--------|
| **Graphiti MCP Server** | 时序 KG + 语义搜索，专为 AI Agent 设计 | ★★★★★ |
| **Neo4j MCP Server** | Schema 暴露 + Cypher 查询 + 跨会话记忆 | ★★★★ |
| **mcp-knowledge-graph** | 轻量本地 KG，无需云服务 | ★★★ |

Graphiti MCP Server 配合 FalkorDB，可以让 Claude Code **直接通过 MCP 操作知识图谱**，实现跨会话的知识积累和语义搜索。

### 4.4 推荐方案栈

**方案 A：最轻量（快速验证）**
```
LightRAG（本地文件存储）+ 本地 LLM
- 无独立数据库，适合文档 < 500 篇
- 部署难度 ★☆☆，最低 4GB RAM
```

**方案 B：生产级单机（推荐）**
```
FalkorDB（Docker）+ Graphiti MCP Server + Claude Code
- Docker 一键启动，MCP 直接集成
- 部署难度 ★★☆，最低 8GB RAM
```

**方案 C：全功能（大规模）**
```
Neo4j + Microsoft GraphRAG + LangChain
- 生态最完整，适合工程化部署
- 部署难度 ★★★，最低 16GB RAM
```

---

## 五、本项目知识结构分析与适配评估

### 5.1 知识规模统计

| 分类 | 数量 |
|------|------|
| 研究报告（report/） | 54 个 |
| 基础文档（base/） | 8 个 |
| Python 脚本 | 108 个 |
| 覆盖主题 | MACD、RSI、换手率、均线、热度、波动率等 |
| 命名信号 | ~30-40 个 |
| 核心结论 | ~50-80 条 |

### 5.2 跨主题关联密度（高）

已发现的跨主题关联：

1. **全局依赖**：所有主题均依赖 `macd/01` 的牛熊划分标准，但无显式链接
2. **统一结论**：6 个主题 × 所有信号类型均指向"交叉类信号=纯噪音，极值/突变=唯一有效信号"
3. **跨主题验证**：信号从指数到个股衰减 20-30pp 规律在 MACD/MA/RSI 三主题独立验证
4. **牛熊差异**：同一信号在牛熊中效果差异 19-56 个百分点，跨 4 个主题一致
5. **信号共振**：MACD DIF 谷值与换手率暴增存在共振关系

### 5.3 适合构建的知识图谱结构

**节点类型**：

| 节点类型 | 示例 | 估计数量 |
|---------|------|---------|
| 研究报告 | `macd/report/10-dif-extreme-effectiveness.md` | ~45 |
| 信号定义 | DIF 波谷、MA bias 超卖、RSI14 底背离 | ~35 |
| 核心结论 | "交叉类信号胜率 46-54%=噪音" | ~60 |
| 技术指标 | MACD、MA、RSI、换手率、资金流向、热度、ATR | 7 |
| 市场状态 | 大牛市、反弹牛、慢牛/结构牛、急跌熊、阴跌熊 | 6 |

**关系类型**：

| 关系 | 含义 | 示例 |
|------|------|------|
| VALIDATES | 验证 | `macd/07` 验证 DIF 波谷信号 |
| DEPENDS_ON | 依赖 | 所有 L4 报告依赖 `macd/01` 牛熊划分 |
| REFINES | 精化 | `macd/12` 精化 `macd/10` DIF 极值定义 |
| SUPERSEDES | 废弃 | 综合报告废弃各主题"交叉信号有效"假设 |
| RESONATES_WITH | 共振 | MACD DIF 谷值与换手率暴增 |
| APPLIES_TO | 适用于 | MA bias 超卖信号适用于牛市（非熊市） |
| DECAYS_AT | 衰减于 | 换手率暴增信号在 L4 个股方向反转 |

### 5.4 知识图谱的具体收益

**高价值场景**：

| 场景 | 当前做法 | 知识图谱后 |
|------|---------|-----------|
| 查询"当前环境有效信号" | 读 3+ 文档，人工综合 | 节点过滤 `市场状态=牛市 AND 信号等级=S/A`，直接返回 |
| 信号冲突检测 | 依赖人工记忆 | 自动发现 SUPPORTS 与 SUPERSEDES 的矛盾边 |
| 新主题避免重复 | 不知 MACD 研究已用过 ATR | ATR 节点关联多个报告，自动提示 |
| 信号版本追踪 | DIF 极值 4 个版本无标注 | REFINES 关系链追踪到最新定论 |

### 5.5 适合度综合评分

| 维度 | 评估 |
|------|------|
| 知识量 | ★★★☆☆ 中等（45+ 报告，值得建图但非必须） |
| 关联密度 | ★★★★★ 高（跨主题共享结论、信号依赖牛熊判断、层级间衰减） |
| 结论演化 | ★★★★☆ 有（DIF 极值 4 版本，换手率信号 2022 后衰减） |
| 当前短板 | ★★★★☆ 明显（无显式引用、命名不统一、废弃未标记） |
| 建图 ROI | ★★★★☆ 较高（核心价值在信号版本追踪和冲突检测） |

---

## 六、实施建议

### 6.1 短期（立即可做，零基础设施成本）

1. **报告头部加元数据**：每个报告添加 `前置阅读`、`被引用于`、`废弃标记` 字段
2. **信号卡片文档**：为每个信号建独立文件（信号名、定义来源、胜率、适用条件、最新版本）
3. **结论索引文件**：维护一个按信号/主题组织的结论索引 JSON/YAML

### 6.2 中期（推荐路径）

```
FalkorDB（Docker）+ Graphiti MCP Server + Claude Code
```

实施步骤：
1. Docker 部署 FalkorDB：`docker run -p 6379:6379 -p 3000:3000 falkordb/falkordb:latest`
2. 安装 Graphiti：`pip install graphiti-core`
3. 编写 Python 脚本，从现有报告 Markdown 自动提取节点和关系
4. 配置 Graphiti MCP Server 接入 Claude Code
5. 每次新增报告时增量更新图谱

### 6.3 长期（如知识量持续增长）

当报告超过 200 篇、信号超过 100 个时，考虑：
- 升级至 Neo4j + 微软 GraphRAG 的社区摘要能力
- 引入 LightRAG 做混合检索（向量 + 图）
- 构建自动化的报告→图谱 pipeline

---

## 七、结论

1. **知识图谱增强 LLM 已从学术走向工程成熟**：GraphRAG 准确率超传统 RAG 1.5-3.4 倍，2025 年成本问题已基本解决（LazyGraphRAG -99%，LightRAG -90%）

2. **MCP 是最佳集成通道**：Graphiti MCP Server + FalkorDB 可让 Claude Code 直接操作知识图谱，无需修改现有工作流

3. **本项目高度适合知识图谱化**：关联密度高、结论有版本演化、当前组织方式存在明显短板。核心价值在于解决"信号从哪个报告得出、当前是否有效、依赖什么前提"三个问题

4. **建议渐进式实施**：先做报告元数据规范化（零成本），再部署 FalkorDB + Graphiti MCP（中等成本），最后按需扩展

---

## 参考资料

### 学术论文
- Pan et al., "Unifying Large Language Models and Knowledge Graphs: A Roadmap", IEEE TKDE, 2024
- Sun et al., "Think-on-Graph", ICLR 2024
- Luo et al., "Reasoning on Graphs", ICLR 2024
- "Graph-constrained Reasoning", ICML 2025
- Edge et al., "From Local to Global: A Graph RAG Approach", Microsoft Research, 2024
- "LightRAG", EMNLP 2025
- "HippoRAG v2", NeurIPS 2024

### 开源项目
- Microsoft GraphRAG: https://github.com/microsoft/graphrag
- LightRAG: https://github.com/HKUDS/LightRAG
- FalkorDB: https://github.com/FalkorDB/FalkorDB
- Graphiti: https://github.com/getzep/graphiti
- Neo4j MCP Server: https://github.com/neo4j-contrib/mcp-neo4j
- HippoRAG: https://github.com/OSU-NLP-Group/HippoRAG

### 学术资源库
- KG-LLM-Papers (浙大): https://github.com/zjukg/KG-LLM-Papers
- Awesome-GraphRAG: https://github.com/DEEP-PolyU/Awesome-GraphRAG
