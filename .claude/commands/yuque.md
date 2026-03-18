---
description: 语雀知识库操作（文档创建/读取/目录管理）。需要操作语雀时调用此技能。
---

# 语雀操作指南

## 用户信息
- login: `dalanhou`, user_id: 25948065
- Token 环境变量: `YUQUE_PERSONAL_TOKEN`
- 个人知识库用 `mcp__plugin_yuque-personal_yuque__` 前缀

## 目录约束

上传文档前必须先读取 `docs/constraints/040-语雀目录约束.md`，按照其中定义的目录结构上传。

## 常用知识库

| 名称 | namespace | repo_id |
|------|-----------|---------|
| AI大模型 | dalanhou/ubapkm | 59606793 |
| 技术精进 | dalanhou/rllhr2 | 44751249 |
| 个人总结 | dalanhou/lqdbfq | 28446951 |
| 工具方法论 | dalanhou/hvttf2 | — |

### 项目默认知识库

| 项目 | 默认知识库 |
|------|-----------|
| my-stock | 股票分析 |

## 脚本操作（优先使用，省 token）

使用 `scripts/yuque_note.py` 一次 Bash 调用完成，不要逐步调 MCP 工具。

```bash
# 列出知识库目录
python scripts/yuque_note.py --repo "知识库名" --list

# 读取单个文档
python scripts/yuque_note.py --repo "知识库名" --read "文档slug"

# 批量读取类目（摘要模式，省token）
python scripts/yuque_note.py --repo "知识库名" --read-category "类目标题"

# 批量读取类目（完整内容）
python scripts/yuque_note.py --repo "知识库名" --read-category "类目标题" --full

# 新建文档到类目（类目不存在自动创建）
python scripts/yuque_note.py --repo "知识库名" --category "类目名" --title "标题" --slug "url-slug" --body "markdown内容"

# 从文件读取内容创建文档
python scripts/yuque_note.py --repo "知识库名" --category "类目名" --title "标题" --slug "url-slug" --body-file /tmp/content.md

# 知识库不存在时自动创建（加 --create-repo）
python scripts/yuque_note.py --repo "新知识库名" --create-repo --category "类目名" --title "标题" --slug "url-slug" --body "内容"

# 追加内容到已有文档
python scripts/yuque_note.py --repo "知识库名" --doc-slug "文档slug" --append "追加内容"
```

**注意**:
- `--slug` 必须是英文/数字/横线，2~190字符
- 长内容用 `--body-file` 从临时文件读取，避免命令行转义问题

## MCP 工具操作（脚本不支持时使用）

### 上传文档到知识库（带目录）

**步骤：**
1. **创建文档** — `yuque_create_doc(repo_id, title, slug, body, format="markdown")`
2. **加入目录** — `yuque_update_toc(repo_id, toc_data=JSON)`

**TOC 操作格式（toc_data 为 JSON 字符串）：**

添加同级节点：
```json
{"action":"appendNode","action_mode":"sibling","target_uuid":"<目标节点uuid>","doc_id":<文档id>}
```

添加子节点：
```json
{"action":"appendNode","action_mode":"child","target_uuid":"<父节点uuid>","doc_id":<文档id>}
```

删除节点：
```json
{"action":"removeNode","action_mode":"sibling","node_uuid":"<节点uuid>","target_uuid":"<节点uuid>"}
```

### 目录结构规则

- **类目必须是顶级 TITLE 节点**，不能嵌套在其他文档或类目下
- **文档挂在类目下面作为子节点**，用 `appendNode` + `action_mode: child` + `target_uuid: 类目uuid`
- 创建类目时：先创建 TITLE 类型空文档，再用 `appendNode sibling` 挂到顶级
- **禁止**：类目嵌套在文档下、重复创建同名类目、文档直接挂根级别

### 完整操作流程（批量上传+建目录）

1. 读取本地 MD 文件
2. `yuque_create_doc` 批量创建文档（可并行）
3. `yuque_update_toc` appendNode sibling 创建顶级节点
4. `yuque_update_toc` appendNode child 挂子目录
5. `yuque_update_toc` appendNode child 逐个挂文档到子目录

> 注意：`prependNode` 不可用；全量替换 TOC 格式会报 `action invalid`；只能用 action 指令逐个操作。
