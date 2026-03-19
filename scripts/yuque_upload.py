"""语雀文档上传脚本

将本地 Markdown 文件上传到语雀「股票分析」知识库的指定目录下。
自动检测目录下最大编号，设置新文档编号，上传并挂入目录树。

用法:
  python scripts/yuque_upload.py <目录路径> <本地md文件>

参数:
  目录路径    逗号分隔的多级目录（如 "030 - 指标分析" 或 "070-工程文档,030-经验总结"）
  本地md文件  要上传的 Markdown 文件路径

示例:
  python scripts/yuque_upload.py "030 - 指标分析" macd/report/10-dif-extreme.md
  python scripts/yuque_upload.py "070-工程文档,030-经验总结" docs/some-report.md
  python scripts/yuque_upload.py "060-量化策略,010-策略方法" strategy/report.md

退出码:
  0 = 成功
  1 = 参数/文件错误（本地问题）
  2 = API调用失败（需要大模型介入）
"""
import os
import re
import sys
import json
import urllib.request
import urllib.error

# ══════════════════════════════════════════════════════════════
# 配置（写死）
# ══════════════════════════════════════════════════════════════

BASE_URL = "https://www.yuque.com/api/v2"
USER_LOGIN = "dalanhou"
REPO_ID = 76530393       # 股票分析知识库
REPO_SLUG = "stock-analysis"


def get_token():
    """获取语雀 Token"""
    token = os.environ.get("YUQUE_PERSONAL_TOKEN")
    if not token:
        print("ERROR: 环境变量 YUQUE_PERSONAL_TOKEN 未设置")
        sys.exit(1)
    return token


def api_request(method, path, data=None):
    """发送语雀 API 请求

    返回 (success: bool, result: dict|str)
    """
    token = get_token()
    url = f"{BASE_URL}{path}"
    headers = {
        "X-Auth-Token": token,
        "Content-Type": "application/json",
        "User-Agent": "yuque-upload-script/1.0",
    }

    body = json.dumps(data).encode("utf-8") if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            return True, result.get("data", result)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        return False, f"HTTP {e.code}: {body_text[:500]}"
    except urllib.error.URLError as e:
        return False, f"网络错误: {e.reason}"
    except Exception as e:
        return False, f"未知错误: {e}"


# ══════════════════════════════════════════════════════════════
# Step 1: 解析参数，读取本地文件
# ══════════════════════════════════════════════════════════════

def step1_parse_args():
    """解析命令行参数，读取本地 MD 文件

    返回: (file_path, content, title, category_path_list)
    """
    if len(sys.argv) < 3:
        print("用法: python scripts/yuque_upload.py <目录路径> <本地md文件>")
        print("\n目录路径用逗号分隔多级，如: \"070-工程文档,030-经验总结\"")
        print("知识库: 股票分析 (写死)")
        sys.exit(1)

    category_path = sys.argv[1]
    file_path = sys.argv[2]

    # 解析目录路径（逗号分隔）
    category_path_list = [s.strip() for s in category_path.split(",") if s.strip()]
    if not category_path_list:
        print("ERROR: 目录路径不能为空")
        sys.exit(1)

    # 检查文件
    if not os.path.isfile(file_path):
        print(f"ERROR: 文件不存在: {file_path}")
        sys.exit(1)
    if not file_path.endswith(".md"):
        print(f"ERROR: 仅支持 .md 文件: {file_path}")
        sys.exit(1)

    # 读取文件内容
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 从文件内容提取标题（第一个 # 开头的行）
    title = None
    for line in content.split("\n"):
        line = line.strip()
        if line.startswith("# "):
            title = line[2:].strip()
            break
    if not title:
        title = os.path.splitext(os.path.basename(file_path))[0]

    print(f"[Step 1] 参数解析完成")
    print(f"  知识库: 股票分析 (id={REPO_ID})")
    print(f"  目录路径: {' > '.join(category_path_list)}")
    print(f"  文件: {file_path}")
    print(f"  标题: {title}")

    return file_path, content, title, category_path_list


# ══════════════════════════════════════════════════════════════
# Step 2: 读取目录，定位目标节点，找最大编号
# ══════════════════════════════════════════════════════════════

def step2_find_target_and_max_number(category_path_list):
    """读取 TOC，沿目录路径逐级定位，找到目标目录下编号最大的文章

    category_path_list: ["070-工程文档", "030-经验总结"] 表示逐级匹配

    返回: (next_num, target_uuid, category_display_name)
    """
    ok, toc = api_request("GET", f"/repos/{REPO_ID}/toc")
    if not ok:
        print(f"ERROR: 获取目录失败: {toc}")
        sys.exit(2)

    items = toc if isinstance(toc, list) else []

    # 逐级定位目标目录
    # 第一级: 在顶层(level=0)中按标题匹配
    # 第二级: 在第一级的子节点(level=1)中匹配
    # 以此类推
    current_parent_idx = None
    current_parent_level = -1  # 虚拟根级
    target_uuid = None

    for depth, segment in enumerate(category_path_list):
        found = False
        # 确定搜索范围
        if current_parent_idx is None:
            # 搜索顶层
            search_start = 0
            search_level = 0
        else:
            # 搜索当前父节点的子节点
            search_start = current_parent_idx + 1
            search_level = current_parent_level + 1

        for i in range(search_start, len(items)):
            item = items[i]
            item_level = item.get("level", 0)

            # 超出当前父节点的子节点范围
            if current_parent_idx is not None and item_level <= current_parent_level:
                break

            # 跳过非目标层级的节点
            if item_level != search_level:
                continue

            # 标题匹配（支持子串匹配，兼容目录名微调）
            item_title = item.get("title", "")
            if item_title == segment or segment in item_title or item_title in segment:
                current_parent_idx = i
                current_parent_level = item_level
                target_uuid = item.get("uuid")
                found = True
                break

        if not found:
            print(f"ERROR: 在目录中找不到 \"{segment}\"（第{depth+1}级）")
            print(f"  可用的同级目录:")
            # 列出同级可选项
            for i in range(search_start if current_parent_idx is None else current_parent_idx + 1, len(items)):
                item = items[i]
                item_level = item.get("level", 0)
                if current_parent_idx is not None and item_level <= current_parent_level:
                    break
                if item_level == search_level:
                    print(f"    - {item.get('title', '?')}")
            sys.exit(2)

    if target_uuid is None:
        print("ERROR: 目录定位失败")
        sys.exit(2)

    # 收集目标目录的直接子节点标题
    children = []
    for i in range(current_parent_idx + 1, len(items)):
        item = items[i]
        if item.get("level", 0) <= current_parent_level:
            break
        if item.get("level", 0) == current_parent_level + 1:
            children.append(item.get("title", ""))

    # 从标题中提取编号（格式: 010-xxx, 020-xxx 等）
    max_num = 0
    pattern = re.compile(r"^(\d{3})-")
    for title in children:
        m = pattern.match(title)
        if m:
            num = int(m.group(1))
            if num > max_num:
                max_num = num

    next_num = max_num + 10  # 步长10
    category_display = " > ".join(category_path_list)

    print(f"[Step 2] 目录定位成功: {category_display} (uuid={target_uuid})")
    print(f"  最大编号: {max_num:03d}, 新文档编号: {next_num:03d}")
    print(f"  已有 {len(children)} 篇文章: {children}")

    return next_num, target_uuid, category_display


# ══════════════════════════════════════════════════════════════
# Step 3: 生成带编号的标题
# ══════════════════════════════════════════════════════════════

def step3_make_title(next_num, title):
    """给标题加上编号前缀

    如果标题已有编号（如 040-xxx），替换为新编号
    如果没有，添加编号
    """
    # 去掉已有的编号前缀
    clean_title = re.sub(r"^\d{3}-", "", title)
    new_title = f"{next_num:03d}-{clean_title}"

    # 生成 slug（用于URL，只能包含英文/数字/横线）
    from datetime import datetime
    date_str = datetime.now().strftime("%Y%m%d")
    slug = f"doc-{next_num:03d}-{date_str}"

    print(f"[Step 3] 文档标题: {new_title}")
    print(f"  slug: {slug}")

    return new_title, slug


# ══════════════════════════════════════════════════════════════
# Step 4: 上传文档
# ══════════════════════════════════════════════════════════════

def step4_upload(new_title, slug, content):
    """创建语雀文档"""
    data = {
        "title": new_title,
        "slug": slug,
        "body": content,
        "format": "markdown",
        "public": 0,
    }

    ok, result = api_request("POST", f"/repos/{REPO_ID}/docs", data)
    if not ok:
        print(f"ERROR: 上传文档失败: {result}")
        sys.exit(2)

    doc_id = result.get("id")
    if not doc_id:
        print(f"ERROR: 上传返回数据中没有 id: {result}")
        sys.exit(2)

    print(f"[Step 4] 文档上传成功, doc_id={doc_id}")
    return doc_id


# ══════════════════════════════════════════════════════════════
# Step 5: 设置目录（挂入 TOC）
# ══════════════════════════════════════════════════════════════

def step5_set_toc(target_uuid, doc_id):
    """将文档挂入目标目录下"""
    toc_data = {
        "action": "appendNode",
        "action_mode": "child",
        "target_uuid": target_uuid,
        "doc_id": doc_id,
    }

    ok, result = api_request("PUT", f"/repos/{REPO_ID}/toc", toc_data)
    if not ok:
        print(f"ERROR: 设置目录失败: {result}")
        sys.exit(2)

    print(f"[Step 5] 目录设置完成")
    return result


# ══════════════════════════════════════════════════════════════
# Step 6: 验证目录结构
# ══════════════════════════════════════════════════════════════

def step6_verify(target_uuid, doc_id):
    """重新读取 TOC，验证文档是否正确挂入"""
    ok, toc = api_request("GET", f"/repos/{REPO_ID}/toc")
    if not ok:
        print(f"ERROR: 验证时获取目录失败: {toc}")
        sys.exit(2)

    items = toc if isinstance(toc, list) else []

    # 找目录位置
    cat_idx = None
    cat_level = None
    for i, item in enumerate(items):
        if item.get("uuid") == target_uuid:
            cat_idx = i
            cat_level = item.get("level", 0)
            break

    if cat_idx is None:
        print(f"ERROR: 验证失败 - 找不到目录")
        sys.exit(2)

    # 检查子节点中是否有新文档
    found = False
    for i in range(cat_idx + 1, len(items)):
        item = items[i]
        if item.get("level", 0) <= cat_level:
            break
        if item.get("doc_id") == doc_id:
            found = True
            actual_level = item.get("level", 0)
            expected_level = cat_level + 1
            if actual_level != expected_level:
                print(f"ERROR: 文档层级不正确, 期望={expected_level}, 实际={actual_level}")
                sys.exit(2)
            break

    if not found:
        print(f"ERROR: 验证失败 - 文档未出现在目标目录下")
        sys.exit(2)

    print(f"[Step 6] 验证通过 - 文档已正确挂入目录")


# ══════════════════════════════════════════════════════════════
# Step 7: 输出结果
# ══════════════════════════════════════════════════════════════

def step7_output(new_title, slug, doc_id, category_display):
    """输出最终结果"""
    url = f"https://www.yuque.com/{USER_LOGIN}/{REPO_SLUG}/{slug}"
    print(f"\n{'='*60}")
    print(f"上传成功!")
    print(f"  标题: {new_title}")
    print(f"  目录: {category_display}")
    print(f"  doc_id: {doc_id}")
    print(f"  链接: {url}")
    print(f"{'='*60}")


# ══════════════════════════════════════════════════════════════
# 主流程
# ══════════════════════════════════════════════════════════════

def main():
    # Step 1: 解析参数
    file_path, content, title, category_path_list = step1_parse_args()

    # Step 2: 定位目录 + 找最大编号
    next_num, target_uuid, category_display = step2_find_target_and_max_number(category_path_list)

    # Step 3: 生成带编号标题
    new_title, slug = step3_make_title(next_num, title)

    # Step 4: 上传文档
    doc_id = step4_upload(new_title, slug, content)

    # Step 5: 设置目录
    step5_set_toc(target_uuid, doc_id)

    # Step 6: 验证
    step6_verify(target_uuid, doc_id)

    # Step 7: 输出结果
    step7_output(new_title, slug, doc_id, category_display)


if __name__ == "__main__":
    main()
