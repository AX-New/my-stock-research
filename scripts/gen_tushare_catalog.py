#!/usr/bin/env python3
"""
生成 Tushare 接口目录 CSV

读取 tushare_docs/_nav_map.json，提取接口层级、doc_id、api_name，
输出 tushare_docs/interface_catalog.csv。

用法：
    python scripts/gen_tushare_catalog.py

输出 CSV 字段：
    doc_id     - Tushare 文档 ID（如 25）
    level1     - 一级分类（如 股票数据）
    level2     - 二级分类（如 基础数据）
    level3     - 三级分类/接口中文名（如 股票列表）
    level4     - 四级分类（部分接口有，通常为空）
    api_name   - Tushare 接口名（如 stock_basic）
    url        - Tushare 文档 URL
    doc_path   - 本地文档文件路径
"""

import json
import csv
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = PROJECT_ROOT / "tushare_docs"
NAV_MAP_FILE = DOCS_ROOT / "_nav_map.json"
URL_MAP_FILE = DOCS_ROOT / "_url_map.json"
DOC_DIR = DOCS_ROOT / "document" / "2"
OUTPUT_CSV = DOCS_ROOT / "interface_catalog.csv"

# _url_map.json 里的旧路径前缀 → 实际路径前缀
_OLD_PREFIX = r"F:\projects\tools\tushare_docs"


def extract_doc_id(url: str) -> str | None:
    m = re.search(r"doc_id=(\d+)", url)
    return m.group(1) if m else None


def extract_api_name(doc_id: str) -> str:
    """从 doc_id-{id}.md 文件中提取接口名（api_name）"""
    doc_file = DOC_DIR / f"doc_id-{doc_id}.md"
    if not doc_file.exists():
        return ""
    try:
        content = doc_file.read_text(encoding="utf-8")
    except Exception:
        return ""
    # 匹配多种格式：
    #   "接口：stock_basic，..."
    #   "接口名称 ：pro_bar ..."
    #   "接口: stock_basic"
    m = re.search(r"接口(?:名称)?\s*[：:]\s*([A-Za-z_][A-Za-z0-9_]*)", content)
    return m.group(1) if m else ""


def resolve_hierarchy_path(url: str, url_map: dict) -> str:
    """从 _url_map.json 获取层级目录路径，并修正前缀为实际路径"""
    raw = url_map.get(url, "")
    if not raw:
        return ""
    # 替换旧前缀为当前实际路径
    actual = raw.replace(_OLD_PREFIX, str(DOCS_ROOT))
    return actual


def main():
    if not NAV_MAP_FILE.exists():
        print(f"[ERROR] 找不到 {NAV_MAP_FILE}")
        return

    with open(NAV_MAP_FILE, encoding="utf-8") as f:
        nav_map: dict[str, list[str]] = json.load(f)

    url_map: dict[str, str] = {}
    if URL_MAP_FILE.exists():
        with open(URL_MAP_FILE, encoding="utf-8") as f:
            url_map = json.load(f)

    # 构建子节点集合：凡是某个 hierarchy 是另一个 hierarchy 的前缀，说明它是分类页
    # 叶子节点 = 没有任何其他节点以它为前缀
    all_hierarchies = [tuple(h) for h in nav_map.values() if h]
    parent_set = set()
    for h in all_hierarchies:
        for length in range(1, len(h)):
            parent_set.add(h[:length])  # 所有前缀都是分类页

    rows = []
    skipped = 0

    for url, hierarchy in nav_map.items():
        doc_id = extract_doc_id(url)
        if not doc_id:
            skipped += 1
            continue

        depth = len(hierarchy)
        if depth < 2:
            # depth=1 只是顶级分类页，跳过
            skipped += 1
            continue

        h_tuple = tuple(hierarchy)

        # 如果自身是某个节点的前缀（即有子节点），则是分类页，跳过
        if h_tuple in parent_set:
            skipped += 1
            continue

        level1 = hierarchy[0] if depth > 0 else ""
        level2 = hierarchy[1] if depth > 1 else ""
        level3 = hierarchy[2] if depth > 2 else ""
        level4 = hierarchy[3] if depth > 3 else ""

        api_name = extract_api_name(doc_id)
        doc_path = str(DOC_DIR / f"doc_id-{doc_id}.md")
        hierarchy_path = resolve_hierarchy_path(url, url_map)

        rows.append({
            "doc_id": doc_id,
            "level1": level1,
            "level2": level2,
            "level3": level3,
            "level4": level4,
            "api_name": api_name,
            "url": url,
            "doc_path": doc_path,
            "hierarchy_path": hierarchy_path,
        })

    # 按层级排序
    rows.sort(key=lambda r: (r["level1"], r["level2"], r["level3"], r["level4"]))

    fieldnames = ["doc_id", "level1", "level2", "level3", "level4", "api_name", "url", "doc_path", "hierarchy_path"]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"生成完成：{OUTPUT_CSV}")
    print(f"  接口记录：{len(rows)} 条")
    print(f"  跳过（分类页/无doc_id）：{skipped} 条")

    # 统计 api_name 提取情况
    missing = [r for r in rows if not r["api_name"]]
    if missing:
        print(f"  [警告] {len(missing)} 条未能提取 api_name：")
        for r in missing[:10]:
            print(f"    doc_id={r['doc_id']} level3={r['level3']}")


if __name__ == "__main__":
    main()
