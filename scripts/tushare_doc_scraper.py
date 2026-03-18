import argparse
import json
import os
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query = {k: sorted(v) for k, v in query.items()}
    normalized_query = urlencode(query, doseq=True)
    normalized = parsed._replace(query=normalized_query, fragment="")
    return urlunparse(normalized)


def is_same_site(url: str, base_netloc: str) -> bool:
    try:
        return urlparse(url).netloc == base_netloc
    except Exception:
        return False


def safe_filename(name: str) -> str:
    name = re.sub(r"[^\w\-.]+", "_", name.strip())
    name = name.strip("._")
    return name or "index"


def url_to_path(url: str, output_dir: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    query = parse_qs(parsed.query, keep_blank_values=True)
    filename_parts = []
    for key in sorted(query.keys()):
        for value in query[key]:
            filename_parts.append(f"{key}-{value}")
    if filename_parts:
        filename = safe_filename("_".join(filename_parts))
    else:
        filename = "index"
    rel_dir = os.path.join(*parts) if parts else ""
    return os.path.join(output_dir, rel_dir, f"{filename}.md")


def safe_path_segment(segment: str) -> str:
    return safe_filename(segment or "section")


def extract_li_label(li: Tag) -> str:
    parts = []
    for child in li.contents:
        if isinstance(child, NavigableString):
            parts.append(str(child).strip())
        elif isinstance(child, Tag):
            if child.name in {"ul", "ol"}:
                continue
            parts.append(child.get_text(" ", strip=True))
    label = re.sub(r"\s+", " ", " ".join([p for p in parts if p])).strip()
    return label


def pick_main_content(soup: BeautifulSoup) -> Tag:
    selectors = [
        "article",
        "main",
        ".doc-content",
        ".document-content",
        ".markdown-body",
        ".content",
        "#content",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            return node
    return soup.body or soup


def text_of(node: Tag) -> str:
    text = node.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def render_table(table: Tag) -> str:
    rows = []
    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        row_text = [text_of(cell) for cell in cells]
        if row_text:
            rows.append(row_text)
    if not rows:
        return ""
    max_cols = max(len(r) for r in rows)
    rows = [r + [""] * (max_cols - len(r)) for r in rows]
    header = rows[0]
    sep = ["---"] * max_cols
    body = rows[1:]
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join(sep) + " |"]
    for row in body:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines) + "\n\n"


def render_list(tag: Tag, level: int = 0) -> str:
    lines = []
    for li in tag.find_all("li", recursive=False):
        prefix = "  " * level + "- "
        text_parts = []
        for child in li.contents:
            if isinstance(child, NavigableString):
                text_parts.append(str(child).strip())
            elif isinstance(child, Tag) and child.name in {"ul", "ol"}:
                continue
            else:
                text_parts.append(text_of(child))
        line = prefix + " ".join([t for t in text_parts if t])
        lines.append(line.rstrip())
        for child in li.find_all(["ul", "ol"], recursive=False):
            lines.append(render_list(child, level + 1).rstrip())
    return "\n".join(lines) + "\n\n"


def render_node(node: Tag) -> str:
    if isinstance(node, NavigableString):
        return str(node)
    if not isinstance(node, Tag):
        return ""

    name = node.name.lower()
    if name in {"script", "style", "noscript"}:
        return ""
    if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
        level = int(name[1])
        return f"{'#' * level} {text_of(node)}\n\n"
    if name == "p":
        return f"{text_of(node)}\n\n"
    if name == "br":
        return "\n"
    if name == "pre":
        code = node.get_text("\n", strip=True)
        return f"```\n{code}\n```\n\n"
    if name == "code":
        return f"`{node.get_text(strip=True)}`"
    if name == "table":
        return render_table(node)
    if name in {"ul", "ol"}:
        return render_list(node)
    if name == "blockquote":
        lines = [f"> {line}" for line in text_of(node).splitlines() if line.strip()]
        return "\n".join(lines) + "\n\n"
    if name == "img":
        alt = node.get("alt", "")
        src = node.get("src", "")
        return f"![{alt}]({src})\n\n"
    if name == "a":
        href = node.get("href", "")
        label = text_of(node)
        return f"[{label}]({href})"

    content = []
    for child in node.contents:
        content.append(render_node(child))
    return "".join(content)


def html_to_markdown(soup: BeautifulSoup) -> str:
    main = pick_main_content(soup)
    content = []
    for child in main.contents:
        content.append(render_node(child))
    md = "".join(content)
    md = re.sub(r"\n{3,}", "\n\n", md).strip() + "\n"
    return md


def extract_links(soup: BeautifulSoup, base_url: str) -> list:
    base_netloc = urlparse(base_url).netloc
    candidates = set()

    nav_selectors = [
        "[class*='nav']",
        "[class*='menu']",
        "[class*='sidebar']",
        "[id*='nav']",
        "[id*='menu']",
        "[id*='sidebar']",
    ]
    nav_nodes = []
    for selector in nav_selectors:
        nav_nodes.extend(soup.select(selector))

    if not nav_nodes:
        nav_nodes = [soup]

    for node in nav_nodes:
        for link in node.find_all("a", href=True):
            href = urljoin(base_url, link["href"])
            if not is_same_site(href, base_netloc):
                continue
            if "/document/" not in urlparse(href).path:
                continue
            candidates.add(normalize_url(href))

    return sorted(candidates)


def extract_nav_tree_selenium(start_url: str) -> dict:
    """使用Selenium提取jstree渲染后的菜单结构"""
    print("[info] 正在启动Chrome浏览器...")

    chrome_options = Options()
    chrome_options.add_argument('--headless=new')  # 使用新版headless模式
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-software-rasterizer')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--window-size=1920,1080')

    # 添加日志选项
    chrome_options.add_argument('--log-level=3')  # 减少日志输出
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # 添加cookie支持
    cookie = os.environ.get("TUSHARE_COOKIE")

    driver = None
    nav_map = {}

    try:
        # 使用webdriver-manager自动下载和管理ChromeDriver
        print("[info] 正在初始化ChromeDriver...")
        try:
            service = Service(ChromeDriverManager().install())
        except Exception as e:
            print(f"[error] ChromeDriver下载失败: {e}")
            return {}

        print("[info] 正在启动Chrome（超时30秒）...")
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            print(f"[error] Chrome启动失败: {e}")
            print("[tip] 请确认Chrome浏览器已安装")
            return {}

        driver.set_page_load_timeout(30)
        driver.set_script_timeout(30)
        print("[info] Chrome浏览器已启动")

        print(f"[info] 正在访问 {start_url}")
        driver.get(start_url)
        print("[info] 页面加载完成")

        # 如果有cookie，设置cookie
        if cookie:
            print("[info] 正在设置Cookie...")
            for cookie_str in cookie.split(';'):
                cookie_str = cookie_str.strip()
                if '=' in cookie_str:
                    name, value = cookie_str.split('=', 1)
                    try:
                        driver.add_cookie({'name': name.strip(), 'value': value.strip()})
                    except:
                        pass
            driver.refresh()
            print("[info] Cookie设置完成")

        # 等待jstree加载完成
        print("[info] 等待页面菜单加载...")
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "jstree"))
            )
            print("[info] 菜单元素已找到")
        except Exception as e:
            print(f"[error] 等待菜单超时: {e}")
            # 保存页面截图用于调试
            try:
                screenshot_path = "debug_screenshot.png"
                driver.save_screenshot(screenshot_path)
                print(f"[debug] 页面截图已保存到 {screenshot_path}")
            except:
                pass
            raise

        time.sleep(3)  # 额外等待确保完全渲染

        # 展开所有节点
        print("[info] 正在展开菜单节点...")
        try:
            driver.execute_script("""
                if (typeof $ !== 'undefined' && $('#jstree').jstree) {
                    $('#jstree').jstree('open_all');
                    return true;
                }
                return false;
            """)
            time.sleep(3)
            print("[info] 菜单已展开")
        except Exception as e:
            print(f"[warn] 无法展开所有节点: {e}，将尝试提取可见节点")

        # 提取jstree结构
        print("[info] 正在提取菜单结构...")
        jstree_element = driver.find_element(By.ID, "jstree")

        def extract_node(element, parents=[]):
            """递归提取节点信息"""
            # 查找当前节点的链接
            try:
                anchor = element.find_element(By.TAG_NAME, "a")
                href = anchor.get_attribute("href")
                text = anchor.text.strip()

                if href and text and "/document/" in href:
                    normalized = normalize_url(href)
                    full_path = parents + [text]
                    nav_map[normalized] = full_path
                    if len(nav_map) % 10 == 0:  # 每10个打印一次进度
                        print(f"[progress] 已提取 {len(nav_map)} 个菜单项...")
            except:
                text = None

            # 查找子节点
            try:
                child_ul = element.find_element(By.XPATH, "./ul")
                child_lis = child_ul.find_elements(By.XPATH, "./li")

                new_parents = parents + [text] if text else parents
                for child_li in child_lis:
                    extract_node(child_li, new_parents)
            except:
                pass  # 没有子节点

        # 获取顶层li元素
        top_lis = jstree_element.find_elements(By.XPATH, "./ul/li")
        print(f"[info] 发现 {len(top_lis)} 个顶级菜单项")

        for i, li in enumerate(top_lis):
            print(f"[info] 正在处理顶级菜单 {i+1}/{len(top_lis)}")
            extract_node(li, [])

        print(f"[success] 成功提取 {len(nav_map)} 个菜单项")

    except Exception as e:
        print(f"[error] Selenium提取失败: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try:
                print("[info] 正在关闭浏览器...")
                driver.quit()
                print("[info] 浏览器已关闭")
            except Exception as e:
                print(f"[warn] 关闭浏览器时出错: {e}")
                # 强制终止chrome进程
                try:
                    import psutil
                    for proc in psutil.process_iter(['name']):
                        if 'chrome' in proc.info['name'].lower():
                            proc.kill()
                except:
                    pass

    return nav_map


def extract_nav_tree(soup: BeautifulSoup, base_url: str) -> dict:
    """备用方案：从静态HTML提取菜单（可能不完整）"""
    base_netloc = urlparse(base_url).netloc
    nav_map = {}

    nav_selectors = [
        "[class*='nav']",
        "[class*='menu']",
        "[class*='sidebar']",
        "[id*='nav']",
        "[id*='menu']",
        "[id*='sidebar']",
    ]
    nav_nodes = []
    for selector in nav_selectors:
        nav_nodes.extend(soup.select(selector))

    def add_mapping(href: str, segments: list) -> None:
        normalized = normalize_url(href)
        if normalized not in nav_map:
            nav_map[normalized] = segments

    def parse_list(list_tag: Tag, parents: list) -> None:
        for li in list_tag.find_all("li", recursive=False):
            label = extract_li_label(li) or "section"
            anchor = li.find("a", href=True)
            child_lists = li.find_all(["ul", "ol"], recursive=False)

            if anchor:
                href = urljoin(base_url, anchor["href"])
                if is_same_site(href, base_netloc) and "/document/" in urlparse(href).path:
                    anchor_text = anchor.get_text(" ", strip=True) or label
                    segments = parents + [anchor_text]
                    add_mapping(href, segments)
                    if child_lists:
                        for child_list in child_lists:
                            parse_list(child_list, parents + [anchor_text])
                    continue

            next_parents = parents + [label] if label else parents
            for child_list in child_lists:
                parse_list(child_list, next_parents)

    for node in nav_nodes:
        for list_tag in node.find_all(["ul", "ol"], recursive=False):
            parse_list(list_tag, [])

    return nav_map


def fetch(session: requests.Session, url: str) -> BeautifulSoup:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def build_output_path(
    url: str,
    output_dir: str,
    nav_map: dict,
    used_paths: dict,
) -> str:
    if nav_map and url in nav_map:
        segments = [safe_path_segment(seg) for seg in nav_map[url] if seg]
        if segments:
            dir_path = os.path.join(output_dir, *segments[:-1])
            filename = safe_path_segment(segments[-1])
            path = os.path.join(dir_path, f"{filename}.md")
        else:
            path = url_to_path(url, output_dir)
    else:
        path = url_to_path(url, output_dir)

    if path in used_paths and used_paths[path] != url:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        suffix = None
        if "doc_id" in query and query["doc_id"]:
            suffix = f"doc_id-{query['doc_id'][0]}"
        else:
            suffix = safe_filename(parsed.path.strip("/")) or "doc"
        base_name = os.path.splitext(os.path.basename(path))[0]
        dir_name = os.path.dirname(path)
        path = os.path.join(dir_name, f"{base_name}_{suffix}.md")
    used_paths[path] = url
    return path


def scrape(
    start_url: str,
    output_dir: str,
    sleep: float,
    max_pages: int,
    use_nav_tree: bool,
) -> None:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; TushareDocScraper/1.0)",
        }
    )
    cookie = os.environ.get("TUSHARE_COOKIE")
    if cookie:
        session.headers["Cookie"] = cookie

    visited = set()
    queue = deque([normalize_url(start_url)])
    url_map = {}
    nav_map = {}
    used_paths = {}

    # 如果启用nav_tree，首先使用Selenium提取完整菜单结构
    if use_nav_tree:
        print("[info] 使用Selenium提取菜单结构...")
        try:
            nav_map = extract_nav_tree_selenium(start_url)
            if not nav_map:
                print("[warn] Selenium提取失败，将使用备用方案")
                raise RuntimeError("Selenium extraction failed")
        except Exception as e:
            print(f"[warn] Selenium提取出错: {e}")
            print("[info] 使用备用方案提取菜单...")
            # 备用方案：从第一个页面提取
            try:
                soup = fetch(session, start_url)
                nav_map = extract_nav_tree(soup, start_url)
            except:
                pass

        if nav_map:
            # 将nav_map中的所有URL加入队列
            for nav_url in nav_map.keys():
                if nav_url not in visited:
                    queue.append(nav_url)
            print(f"[info] 从菜单中发现 {len(nav_map)} 个页面")

    while queue and (max_pages <= 0 or len(visited) < max_pages):
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        try:
            soup = fetch(session, url)
        except requests.HTTPError as exc:
            print(f"[warn] {url} -> {exc}")
            continue
        except requests.RequestException as exc:
            print(f"[warn] {url} -> {exc}")
            continue

        # 如果没有使用nav_tree，从页面提取链接
        if not use_nav_tree:
            links = extract_links(soup, url)
            for link in links:
                if link not in visited:
                    queue.append(link)

        md = html_to_markdown(soup)
        output_path = build_output_path(url, output_dir, nav_map, used_paths)
        ensure_dir(output_path)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(md)
        url_map[url] = output_path
        print(f"[ok] {url} -> {output_path}")

        if sleep > 0:
            time.sleep(sleep)

    map_path = os.path.join(output_dir, "_url_map.json")
    with open(map_path, "w", encoding="utf-8") as f:
        json.dump(url_map, f, ensure_ascii=False, indent=2)

    # 保存nav_map用于调试
    if nav_map:
        nav_map_path = os.path.join(output_dir, "_nav_map.json")
        with open(nav_map_path, "w", encoding="utf-8") as f:
            json.dump(nav_map, f, ensure_ascii=False, indent=2)
        print(f"[info] 菜单结构已保存到 {nav_map_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Tushare docs to Markdown.")
    parser.add_argument(
        "--start",
        default="https://tushare.pro/document/2?doc_id=25",
        help="Start URL for crawling",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(os.getcwd(), "../tushare_docs"),
        help="Output directory",
    )
    parser.add_argument("--sleep", type=float, default=0.5, help="Delay between requests")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages to fetch (0 = no limit)")
    parser.add_argument(
        "--nav-tree",
        action="store_true",
        default=True,
        help="Use left sidebar navigation tree for output hierarchy",
    )
    args = parser.parse_args()

    scrape(args.start, args.output, args.sleep, args.max_pages, args.nav_tree)


if __name__ == "__main__":
    main()
