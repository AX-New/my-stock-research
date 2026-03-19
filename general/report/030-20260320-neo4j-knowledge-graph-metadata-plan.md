# Neo4j 磁盘型知识图谱：文档元数据标注与数据积累实施方案
**创建时间**: 20260320 22:00

## 一、方案背景与决策

### 1.1 为什么选 Neo4j 磁盘型

前期调研（报告 020）推荐 FalkorDB（内存型图数据库），但实际评估后决定切换到 **Neo4j Community Edition**：

| 维度 | FalkorDB | Neo4j CE | 决策 |
|------|---------|---------|------|
| 存储模型 | 内存图（Redis 内核） | **磁盘图** | 本机内存不足，磁盘型更合适 |
| 数据持久化 | RDB/AOF（Redis 机制） | 原生磁盘存储 | Neo4j 更可靠 |
| 内存占用 | 全量数据 + 索引常驻内存 | 仅热点数据 + 索引缓存 | 项目数据量小，Neo4j 内存可控 |
| 生态成熟度 | 新兴 | **最成熟**（20年历史） | 工具链、文档、社区远超其他 |
| MCP 集成 | Graphiti MCP | Neo4j MCP Server | 两者均可，Neo4j 更稳定 |
| Python 驱动 | falkordb-py | **neo4j (官方)** | Neo4j 官方驱动质量更高 |
| 查询语言 | Cypher（子集） | **完整 Cypher** | Neo4j 是 Cypher 发明者 |

**结论**：对于本项目 ~55 篇报告、~100 个脚本的规模，Neo4j CE 的磁盘存储完全足够，内存占用可控（默认堆 512MB），且生态工具链远超 FalkorDB。

### 1.2 分阶段策略

不急于一步到位建完整知识图谱。先从**文档元数据标注**开始积累结构化数据，为后续自动化建图打基础：

```
阶段 0（当前）→ 文档元数据 YAML 前置块标注
阶段 1 → Neo4j 部署 + 元数据自动导入
阶段 2 → 关系抽取 + 信号图谱构建
阶段 3 → MCP 集成 + LLM 实时查询
```

本文档聚焦**阶段 0 和阶段 1**的详细实施。

---

## 二、阶段 0：文档元数据标注（零基础设施成本）

### 2.1 设计原则

1. **YAML frontmatter 格式** — 标准化、可解析、兼容 Hugo/Jekyll/Obsidian 等工具
2. **最小必要字段** — 不强求填满，但核心字段必须有
3. **渐进标注** — 先标注最重要的报告（综合报告、方法论），再扩展到全部
4. **机器可读** — 所有字段都可通过 Python `yaml` 库解析，直接导入 Neo4j

### 2.2 元数据 Schema 定义

每个 Markdown 文档头部添加 YAML frontmatter 块：

```yaml
---
# === 基础信息 ===
id: macd-report-07                    # 唯一标识符，格式: {topic}-{type}-{序号}
title: "个股 MACD 信号分析"             # 文档标题
type: report                          # 文档类型: report | base | constraint | methodology
topic: macd                           # 所属主题: macd | ma | rsi | turnover | hot | moneyflow | volatility | general
created: "20260315"                   # 创建日期
updated: "20260319"                   # 最后更新日期

# === 研究层级 ===
layer: L4                             # 研究层级: L1(大盘指数) | L2(多指数) | L2.5(宽基) | L3(行业) | L3.5(时间稳定性) | L4(个股) | L5(实时)
scope: stock                          # 数据范围: index | multi-index | broad-market | industry | stock | all

# === 信号与结论 ===
signals:                              # 本文涉及的信号（列表）
  - name: "MACD金叉"
    verdict: noise                    # 结论: effective | noise | conditional | deprecated
    winrate: "42-54%"                 # 胜率（如有）
    condition: null                   # 生效条件
  - name: "DIF极值(波谷)"
    verdict: effective
    winrate: "71-76%"
    condition: "牛市环境"

# === 关系 ===
depends_on:                           # 本文依赖哪些文档
  - macd-report-01                    # 牛熊周期划分
refs:                                 # 被哪些文档引用（反向关系，可选，由工具自动维护）
  - macd-report-10
  - macd-report-11
superseded_by: null                   # 被哪个文档取代（如果已过时）
refines: null                         # 精化了哪个文档的内容
validates:                            # 验证了哪些信号/文档
  - macd-report-04

# === 数据源 ===
data_sources:                         # 用了哪些数据库表
  - my_stock.index_daily_macd
  - my_stock.stock_daily_macd
sample_size: "5346只个股, 2013-2026"   # 样本规模概述

# === 状态 ===
status: active                        # 状态: active | deprecated | superseded | draft
deprecated_reason: null               # 废弃原因（如果 status=deprecated）
confidence: high                      # 结论置信度: high | medium | low | uncertain

# === 标签 ===
tags:                                 # 自由标签
  - 个股验证
  - 信号衰减
  - 牛熊差异
---
```

### 2.3 字段说明与填写规则

#### 必填字段（每个文档必须有）

| 字段 | 说明 | 示例 |
|------|------|------|
| `id` | 全局唯一，格式 `{topic}-{type}-{序号}` | `macd-report-07` |
| `title` | 文档标题 | `"个股 MACD 信号分析"` |
| `type` | 文档类型 | `report` / `base` / `constraint` / `methodology` |
| `topic` | 所属主题 | `macd` / `ma` / `rsi` / `turnover` / `hot` / `general` |
| `created` | 创建日期 | `"20260315"` |
| `status` | 文档状态 | `active` / `deprecated` / `superseded` / `draft` |

#### 推荐字段（研究报告应填）

| 字段 | 说明 | 何时填 |
|------|------|--------|
| `layer` | 研究层级 | 有层级概念的报告 |
| `signals` | 涉及的信号及结论 | 所有信号分析报告 |
| `depends_on` | 依赖文档 | 引用了其他报告结论时 |
| `data_sources` | 数据库表 | 有数据分析的报告 |
| `confidence` | 结论置信度 | 所有有结论的报告 |
| `tags` | 分类标签 | 所有文档 |

#### 可选字段（按需填写）

| 字段 | 说明 | 何时填 |
|------|------|--------|
| `updated` | 更新日期 | 文档有修改时 |
| `superseded_by` | 被取代 | 文档已过时 |
| `refines` | 精化来源 | 对前文做了更细致分析 |
| `validates` | 验证对象 | 独立验证了其他报告的结论 |
| `refs` | 被引文档 | 可由工具自动生成 |
| `deprecated_reason` | 废弃原因 | status=deprecated 时 |

### 2.4 信号 verdict 枚举定义

| 值 | 含义 | 判定标准 |
|----|------|---------|
| `effective` | 有效信号 | 个股周线胜率 > 60% 且跨层级验证一致 |
| `noise` | 纯噪音 | 胜率在 45%-55% 范围内 |
| `conditional` | 条件有效 | 特定市场环境（牛/熊）或特定层级才有效 |
| `deprecated` | 已废弃 | 被后续研究推翻或被更好的信号替代 |

### 2.5 标注优先级

分三批标注，按重要性排序：

**第一批（核心文档，约 15 篇）**：
1. `base/` 下所有方法论文档（7 篇）
2. `base/constraints/` 约束文档（1 篇）
3. 各主题综合报告：
   - `general/report/20260319-six-indicator-research-synthesis.md`
   - `macd/report/04-index-macd-methodology.md`
   - `ma/report/06-ma-methodology.md`
   - `turnover/report/07-methodology-synthesis.md`
4. 牛熊基准：`macd/report/01-a-share-bull-bear-cycles.md`

**第二批（信号定义文档，约 20 篇）**：
- 各主题 L1-L4 核心分析报告
- 信号有效性验证报告（如 `macd/report/10-dif-extreme-effectiveness.md`）

**第三批（其余文档，约 20 篇）**：
- 辅助分析、对比报告、架构调研等

### 2.6 标注示例

以 `macd/report/01-a-share-bull-bear-cycles.md` 为例：

```yaml
---
id: macd-report-01
title: "A股牛熊周期分析"
type: report
topic: macd
created: "20260313"
layer: L1
scope: index
signals: []
depends_on: []
data_sources:
  - my_stock.index_monthly
sample_size: "上证/深证/创业板 3大指数月线全历史"
status: active
confidence: high
tags:
  - 牛熊划分
  - 基准数据
  - 月线分析
---
```

以 `general/report/20260319-six-indicator-research-synthesis.md` 为例：

```yaml
---
id: general-report-synthesis
title: "六大研究主题综合评估报告"
type: report
topic: general
created: "20260319"
layer: null
scope: all
signals:
  - name: "MACD金叉/死叉"
    verdict: noise
    winrate: "42-54%"
    condition: null
  - name: "MA金叉/死叉"
    verdict: noise
    winrate: "45-55%"
    condition: null
  - name: "DIF极值(波谷)"
    verdict: effective
    winrate: "71-76%"
    condition: "牛市环境"
  - name: "MA bias超卖"
    verdict: conditional
    winrate: "96.4%"
    condition: "仅牛市有效"
  - name: "RSI14背离"
    verdict: effective
    winrate: "71.4%"
    condition: null
  - name: "换手率超高(>P95)"
    verdict: effective
    winrate: "58.9-64%"
    condition: "唯一有效卖出信号"
depends_on:
  - macd-report-01
  - macd-report-04
  - ma-report-06
  - turnover-report-07
superseded_by: null
status: active
confidence: high
tags:
  - 综合评估
  - 跨主题
  - 信号分级
  - 废弃清单
---
```

---

## 三、阶段 1：Neo4j 部署与元数据导入

### 3.1 Neo4j Community Edition 部署

**Docker 方式（推荐）**：

```bash
docker run -d \
  --name neo4j-kg \
  -p 7474:7474 -p 7687:7687 \
  -v neo4j_data:/data \
  -v neo4j_logs:/logs \
  -e NEO4J_AUTH=neo4j/your_password \
  -e NEO4J_server_memory_heap_initial__size=256m \
  -e NEO4J_server_memory_heap_max__size=512m \
  -e NEO4J_server_memory_pagecache_size=256m \
  neo4j:5-community
```

**内存配置说明**（磁盘型的核心优势）：

| 参数 | 值 | 说明 |
|------|-----|------|
| `heap.initial_size` | 256MB | JVM 初始堆 |
| `heap.max_size` | 512MB | JVM 最大堆 |
| `pagecache.size` | 256MB | 磁盘页面缓存 |
| **总计** | **~768MB** | 远低于 FalkorDB 的全内存需求 |

对比 FalkorDB：相同数据量下 FalkorDB 需要将所有数据加载到内存（本项目约需 2-4GB），Neo4j 只需 ~768MB 固定开销。

**验证部署**：
- 浏览器访问 `http://localhost:7474` 打开 Neo4j Browser
- 运行 `RETURN 1` 确认连接正常

### 3.2 Python 环境配置

```bash
pip install neo4j pyyaml python-frontmatter
```

### 3.3 图数据模型设计

#### 节点类型

```cypher
// 文档节点
(:Document {
    id: "macd-report-07",
    title: "个股 MACD 信号分析",
    type: "report",          // report | base | constraint | methodology
    topic: "macd",
    layer: "L4",
    scope: "stock",
    created: date("2026-03-15"),
    updated: date("2026-03-19"),
    status: "active",
    confidence: "high",
    file_path: "macd/report/07-stock-macd-signal-analysis.md",
    sample_size: "5346只个股, 2013-2026"
})

// 信号节点
(:Signal {
    name: "DIF极值(波谷)",
    indicator: "macd",       // 所属技术指标
    signal_type: "extreme",  // extreme | crossover | divergence | threshold | surge
    verdict: "effective",
    winrate: "71-76%",
    condition: "牛市环境",
    grade: "S"               // S | A | B | C（信号分级）
})

// 技术指标节点
(:Indicator {
    name: "MACD",
    category: "趋势动能",
    data_source: "价格"
})

// 市场状态节点
(:MarketState {
    name: "大牛市",
    type: "bull",            // bull | bear
    description: "快速上涨行情"
})

// 数据表节点
(:DataTable {
    name: "index_daily_macd",
    database: "my_stock",
    description: "指数日线MACD数据"
})
```

#### 关系类型

```cypher
// 文档间关系
(:Document)-[:DEPENDS_ON]->(:Document)         // 依赖
(:Document)-[:REFINES]->(:Document)            // 精化
(:Document)-[:SUPERSEDES]->(:Document)         // 取代
(:Document)-[:VALIDATES]->(:Document)          // 验证

// 文档-信号关系
(:Document)-[:ANALYZES {verdict: "effective", winrate: "71%"}]->(:Signal)
(:Document)-[:DEFINES]->(:Signal)              // 首次定义该信号
(:Document)-[:DEPRECATES]->(:Signal)           // 废弃该信号

// 信号-市场状态关系
(:Signal)-[:EFFECTIVE_IN]->(:MarketState)      // 在该市场状态有效
(:Signal)-[:INEFFECTIVE_IN]->(:MarketState)    // 在该市场状态无效

// 信号-指标关系
(:Signal)-[:BELONGS_TO]->(:Indicator)          // 属于哪个技术指标

// 信号间关系
(:Signal)-[:RESONATES_WITH]->(:Signal)         // 共振
(:Signal)-[:CONFLICTS_WITH]->(:Signal)         // 冲突

// 文档-数据表关系
(:Document)-[:USES_DATA]->(:DataTable)         // 使用了哪些数据表
```

### 3.4 导入脚本设计

创建 `scripts/kg_import.py`，核心逻辑：

```python
"""
知识图谱元数据导入工具
功能：扫描所有带 YAML frontmatter 的 Markdown 文档，解析元数据，导入 Neo4j
"""

import os
import yaml
import frontmatter
from neo4j import GraphDatabase
from pathlib import Path

class KGImporter:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="your_password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def scan_documents(self, root_dir):
        """扫描所有带 frontmatter 的 Markdown 文档"""
        docs = []
        for md_file in Path(root_dir).rglob("*.md"):
            post = frontmatter.load(str(md_file))
            if post.metadata and 'id' in post.metadata:
                meta = post.metadata
                meta['file_path'] = str(md_file.relative_to(root_dir))
                docs.append(meta)
        return docs

    def import_documents(self, docs):
        """导入文档节点"""
        with self.driver.session() as session:
            for doc in docs:
                session.run("""
                    MERGE (d:Document {id: $id})
                    SET d.title = $title,
                        d.type = $type,
                        d.topic = $topic,
                        d.layer = $layer,
                        d.scope = $scope,
                        d.created = $created,
                        d.status = $status,
                        d.confidence = $confidence,
                        d.file_path = $file_path,
                        d.sample_size = $sample_size
                """, **doc)

    def import_signals(self, docs):
        """导入信号节点及文档-信号关系"""
        with self.driver.session() as session:
            for doc in docs:
                for signal in doc.get('signals', []):
                    session.run("""
                        MERGE (s:Signal {name: $name})
                        SET s.verdict = $verdict,
                            s.winrate = $winrate,
                            s.condition = $condition
                        WITH s
                        MATCH (d:Document {id: $doc_id})
                        MERGE (d)-[:ANALYZES {verdict: $verdict, winrate: $winrate}]->(s)
                    """, doc_id=doc['id'], **signal)

    def import_relationships(self, docs):
        """导入文档间关系"""
        with self.driver.session() as session:
            for doc in docs:
                # depends_on 关系
                for dep in doc.get('depends_on', []) or []:
                    session.run("""
                        MATCH (a:Document {id: $from_id})
                        MATCH (b:Document {id: $to_id})
                        MERGE (a)-[:DEPENDS_ON]->(b)
                    """, from_id=doc['id'], to_id=dep)

                # validates 关系
                for val in doc.get('validates', []) or []:
                    session.run("""
                        MATCH (a:Document {id: $from_id})
                        MATCH (b:Document {id: $to_id})
                        MERGE (a)-[:VALIDATES]->(b)
                    """, from_id=doc['id'], to_id=val)

                # superseded_by 关系
                if doc.get('superseded_by'):
                    session.run("""
                        MATCH (a:Document {id: $from_id})
                        MATCH (b:Document {id: $to_id})
                        MERGE (b)-[:SUPERSEDES]->(a)
                    """, from_id=doc['id'], to_id=doc['superseded_by'])

                # refines 关系
                if doc.get('refines'):
                    session.run("""
                        MATCH (a:Document {id: $from_id})
                        MATCH (b:Document {id: $to_id})
                        MERGE (a)-[:REFINES]->(b)
                    """, from_id=doc['id'], to_id=doc['refines'])

    def import_data_sources(self, docs):
        """导入数据表节点及使用关系"""
        with self.driver.session() as session:
            for doc in docs:
                for table_full in doc.get('data_sources', []) or []:
                    parts = table_full.split('.')
                    db_name = parts[0] if len(parts) > 1 else 'unknown'
                    table_name = parts[-1]
                    session.run("""
                        MERGE (t:DataTable {name: $table_name})
                        SET t.database = $db_name
                        WITH t
                        MATCH (d:Document {id: $doc_id})
                        MERGE (d)-[:USES_DATA]->(t)
                    """, doc_id=doc['id'], table_name=table_name, db_name=db_name)

    def run_full_import(self, root_dir):
        """完整导入流程"""
        docs = self.scan_documents(root_dir)
        print(f"扫描到 {len(docs)} 篇带元数据的文档")

        self.import_documents(docs)
        print("✓ 文档节点导入完成")

        self.import_signals(docs)
        print("✓ 信号节点导入完成")

        self.import_relationships(docs)
        print("✓ 关系导入完成")

        self.import_data_sources(docs)
        print("✓ 数据表节点导入完成")

        # 统计
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count
                UNION ALL
                MATCH ()-[r]->() RETURN type(r) AS label, count(r) AS count
            """)
            for record in result:
                print(f"  {record['label']}: {record['count']}")
```

### 3.5 验证查询示例

导入完成后，可在 Neo4j Browser 或 Python 中执行以下查询验证数据正确性：

```cypher
-- 1. 查看所有有效信号及其来源报告
MATCH (d:Document)-[r:ANALYZES]->(s:Signal)
WHERE s.verdict = 'effective'
RETURN s.name, r.winrate, d.title, d.topic
ORDER BY s.name

-- 2. 追踪 DIF 极值信号的版本演化链
MATCH path = (d:Document)-[:REFINES*]->(origin:Document)
WHERE d.title CONTAINS 'DIF'
RETURN path

-- 3. 查找所有依赖牛熊划分的报告
MATCH (d:Document)-[:DEPENDS_ON*]->(base:Document {id: 'macd-report-01'})
RETURN d.title, d.topic, d.layer

-- 4. 发现跨主题共振信号
MATCH (s1:Signal)-[:RESONATES_WITH]-(s2:Signal)
WHERE s1.name <> s2.name
RETURN s1.name, s2.name

-- 5. 查找已废弃但未标记的文档
MATCH (d:Document)
WHERE d.status = 'active'
AND EXISTS { (d)-[:ANALYZES]->(s:Signal {verdict: 'deprecated'}) }
RETURN d.title, d.file_path

-- 6. 统计各主题的信号数量和有效率
MATCH (d:Document)-[:ANALYZES]->(s:Signal)
RETURN d.topic,
       count(DISTINCT s) AS total_signals,
       count(DISTINCT CASE WHEN s.verdict = 'effective' THEN s END) AS effective,
       count(DISTINCT CASE WHEN s.verdict = 'noise' THEN s END) AS noise
ORDER BY d.topic
```

---

## 四、阶段 2-3 前瞻（后续扩展路径）

### 4.1 阶段 2：关系抽取与信号图谱

在元数据标注完成后，进一步：

1. **自动关系抽取** — 用 LLM 分析报告正文，自动提取文档间引用和信号关联
2. **信号卡片节点** — 为每个信号建立独立的详细节点（定义来源、版本历史、适用条件）
3. **市场状态关联** — 将牛熊分析标准结构化为图节点，与信号有效性关联
4. **自动一致性检查** — 通过图查询发现信号冲突（同一信号在不同报告中结论矛盾）

### 4.2 阶段 3：MCP 集成

1. **Neo4j MCP Server** — 让 Claude Code 通过 MCP 直接查询知识图谱
2. **查询模板** — 预定义常用查询（"当前有效信号"、"信号版本追踪"、"依赖关系树"）
3. **增量更新** — 每次新增报告时自动解析 frontmatter 并更新图谱

### 4.3 与传统向量 RAG 的差异化价值

| 查询类型 | 向量 RAG | Neo4j 知识图谱 |
|---------|---------|---------------|
| "DIF极值的最新定义是什么" | 可能返回旧版本文档片段 | 沿 REFINES 链直达最新版 |
| "哪些结论已被废弃" | 无法回答 | `MATCH (d)-[:SUPERSEDES]->() RETURN d` |
| "当前牛市有效的 S 级信号" | 模糊匹配 | 精确过滤 `verdict=effective AND grade=S AND EFFECTIVE_IN 牛市` |
| "换手率暴增和 DIF 谷值有什么关系" | 两个概念可能不在同一文档块 | `RESONATES_WITH` 关系直接回答 |

---

## 五、实施时间线

| 阶段 | 内容 | 预计工作量 | 前置依赖 |
|------|------|-----------|---------|
| **0a** | 设计并确认元数据 Schema | 已完成（本文档） | 无 |
| **0b** | 标注第一批核心文档（~15 篇） | 一次标注 | Schema 确认 |
| **0c** | 标注第二批信号报告（~20 篇） | 一次标注 | 0b 完成 |
| **0d** | 标注第三批其余文档（~20 篇） | 一次标注 | 0c 完成 |
| **1a** | Docker 部署 Neo4j CE | 一次性 | Docker 环境 |
| **1b** | 开发 `kg_import.py` 导入脚本 | 一次开发 | Neo4j 部署 |
| **1c** | 元数据导入 + 验证查询 | 一次执行 | 脚本 + 标注完成 |
| **2** | 关系抽取 + 信号图谱 | 按需 | 阶段 1 完成 |
| **3** | MCP 集成 | 按需 | 阶段 2 完成 |

**建议**：阶段 0b 和 0c 可以在日常研究过程中渐进完成（每次新写报告就加 frontmatter），不需要一次性全部标注。

---

## 六、风险与注意事项

### 6.1 元数据一致性

| 风险 | 应对 |
|------|------|
| 信号名称不统一（如"DIF极值" vs "DIF波谷" vs "DIF谷值"） | Schema 中定义标准信号名列表（见附录），标注时严格使用 |
| 忘记更新 frontmatter | 阶段 1 脚本检查 `updated` 字段与 git 提交时间是否一致 |
| 关系标注不完整 | `depends_on` 至少标注直接依赖，二级依赖由图查询自动传递 |

### 6.2 Neo4j 运维

| 风险 | 应对 |
|------|------|
| Docker 容器意外停止 | 数据存储在 volume `neo4j_data`，容器重启数据不丢失 |
| 磁盘空间 | 本项目数据量极小（<10MB 图数据），无需担心 |
| 版本升级 | Neo4j 5.x Community 是 LTS，无需频繁升级 |

---

## 附录 A：标准信号名列表

为保证元数据一致性，以下为标准化的信号名（标注时必须使用这些名称）：

### MACD 信号
| 标准名 | 别名 | verdict |
|--------|------|---------|
| `MACD金叉` | 金叉、DIF上穿DEA | noise |
| `MACD死叉` | 死叉、DIF下穿DEA | noise |
| `DIF极值(波谷)` | DIF谷值、DIF波谷 | effective |
| `DIF极值(波峰)` | DIF峰值、DIF波峰 | effective |
| `MACD背离` | 底背离、顶背离 | effective |
| `MACD零轴位置` | 零轴上/下 | conditional |

### MA 信号
| 标准名 | 别名 | verdict |
|--------|------|---------|
| `MA金叉` | 均线金叉 | noise |
| `MA死叉` | 均线死叉 | noise |
| `MA bias超卖` | 乖离率超卖 | conditional |
| `MA bias超买` | 乖离率超买 | conditional |
| `MA支撑` | 均线支撑 | conditional |

### RSI 信号
| 标准名 | verdict |
|--------|---------|
| `RSI14背离` | effective |
| `RSI14强超卖(<20)` | effective |
| `RSI14超买(>80)` | conditional |
| `RSI穿越50中轴` | noise |

### 换手率信号
| 标准名 | verdict |
|--------|---------|
| `换手率超高(>P95)` | effective |
| `换手率暴增(surge)` | conditional |
| `换手率暴跌` | conditional |
| `换手率均线交叉` | noise |

### 热度信号
| 标准名 | verdict |
|--------|---------|
| `热度排名>3500` | effective |
| `热度排名<500` | conditional |
| `热度涨速` | conditional |

---

## 附录 B：完整文档 ID 映射表

以下为所有需标注文档的建议 ID（按目录排序）：

### base/
| ID | 文件路径 |
|----|---------|
| `base-methodology-01` | `base/01-研究方法论.md` |
| `base-methodology-02` | `base/02-分层研究框架.md` |
| `base-methodology-03` | `base/03-跨主题通用经验.md` |
| `base-methodology-04` | `base/04-全市场批量计算模式.md` |
| `base-methodology-05` | `base/05-技术指标研究方法论-旧.md` |
| `base-methodology-06` | `base/06-量化分析适用边界研究.md` |
| `base-methodology-07` | `base/07-量化分析最优周期与策略定位修正.md` |
| `base-constraint-bullbear` | `base/constraints/牛熊分析标准.md` |

### general/report/
| ID | 文件路径 |
|----|---------|
| `general-report-selection` | `general/report/010-20260316-four-indicator-stock-selection-strategy.md` |
| `general-report-kgresearch` | `general/report/020-20260320-knowledge-graph-enhanced-llm-research.md` |
| `general-report-vnpy` | `general/report/2026-03-17-vnpy-framework-analysis.md` |
| `general-report-synthesis` | `general/report/20260319-six-indicator-research-synthesis.md` |

### macd/report/
| ID | 文件路径 |
|----|---------|
| `macd-report-01` | `macd/report/01-a-share-bull-bear-cycles.md` |
| `macd-report-02-sh` | `macd/report/02-sh-index-macd-analysis.md` |
| `macd-report-02-000016` | `macd/report/02-000016-macd-analysis.md` |
| `macd-report-02-000300` | `macd/report/02-000300-macd-analysis.md` |
| `macd-report-02-000852` | `macd/report/02-000852-macd-analysis.md` |
| `macd-report-02-000905` | `macd/report/02-000905-macd-analysis.md` |
| `macd-report-02-399001` | `macd/report/02-399001-macd-analysis.md` |
| `macd-report-02-399006` | `macd/report/02-399006-macd-analysis.md` |
| `macd-report-03` | `macd/report/03-three-points-analysis.md` |
| `macd-report-03b` | `macd/report/03-broad-base-three-points.md` |
| `macd-report-04` | `macd/report/04-index-macd-methodology.md` |
| `macd-report-05` | `macd/report/05-sw-industry-macd-analysis.md` |
| `macd-report-06` | `macd/report/06-current-market-macd-diagnosis.md` |
| `macd-report-07` | `macd/report/07-stock-macd-signal-analysis.md` |
| `macd-report-08` | `macd/report/08-macd-literature-comparison.md` |
| `macd-report-09` | `macd/report/09-index-resonance-analysis.md` |
| `macd-report-10` | `macd/report/10-dif-extreme-effectiveness.md` |
| `macd-report-11` | `macd/report/11-dif-extreme-atr-normalized.md` |
| `macd-report-12` | `macd/report/12-dif-realtime-detection.md` |

### 其余主题
| ID | 文件路径 |
|----|---------|
| `ma-report-01` ~ `06` | `ma/report/01~06-*.md` |
| `rsi-report-01` ~ `06` | `rsi/report/01~06-*.md` |
| `turnover-report-01` ~ `07` | `turnover/report/01~07-*.md` |
| `hot-report-01` ~ `03` | `hot/report/01~03-*.md` |
| `volatility-report-01` ~ `02` | `volatility/report/01~02-*.md` |
