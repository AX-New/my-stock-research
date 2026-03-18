"""Tushare Pro 数据检查工具

快速检查 Tushare 接口数据可用性，对比数据库已有数据，定位缺失原因。

用法:
  # 直接调用 Tushare 接口，查看返回数据
  python scripts/tushare_check.py index_daily ts_code=000990.SH
  python scripts/tushare_check.py sw_daily ts_code=801010.SI
  python scripts/tushare_check.py index_basic ts_code=000990.SH
  python scripts/tushare_check.py index_basic market=SW category=一级行业指数

  # 限制返回行数（默认10）
  python scripts/tushare_check.py sw_daily trade_date=20260312 --limit 5

  # 指定输出字段
  python scripts/tushare_check.py index_basic market=CSI --fields ts_code,name,category

  # 对比数据库表，检查缺失
  python scripts/tushare_check.py index_daily ts_code=000990.SH --db-table index_daily --db-key ts_code,trade_date

  # 查看接口文档（从本地文档库检索）
  python scripts/tushare_check.py sw_daily --doc
"""
import os
import sys
import argparse
import csv
import tushare as ts
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TUSHARE_TOKEN", "")
if not TOKEN:
    print("[ERROR] TUSHARE_TOKEN not found, check .env")
    sys.exit(1)

pro = ts.pro_api(TOKEN)

# 接口目录文件路径
CATALOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tushare_docs", "interface_catalog.csv",
)


def load_catalog():
    """加载接口目录，返回 {api_name: {doc_id, level1, doc_path, ...}}"""
    catalog = {}
    if not os.path.exists(CATALOG_PATH):
        return catalog
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            api = row.get("api_name", "").strip()
            if api:
                catalog[api] = row
    return catalog


def show_doc(api_name: str, catalog: dict):
    """显示接口的本地文档"""
    if api_name not in catalog:
        print(f"[WARN] '{api_name}' not found in catalog")
        # 模糊搜索
        matches = [k for k in catalog if api_name in k]
        if matches:
            print(f"  Similar: {', '.join(matches)}")
        return

    info = catalog[api_name]
    doc_path = info.get("doc_path", "")
    print(f"API: {api_name}")
    print(f"  Category: {info.get('level1', '?')} > {info.get('level2', '?')}")
    print(f"  URL: {info.get('url', '?')}")
    print(f"  Doc: {doc_path}")

    if doc_path and os.path.exists(doc_path):
        print(f"\n{'='*60}")
        with open(doc_path, "r", encoding="utf-8") as f:
            print(f.read())
    else:
        print("  [WARN] Local doc file not found")


def call_tushare(api_name: str, params: dict, fields: str = "", limit: int = 10):
    """调用 Tushare 接口，返回 DataFrame"""
    kwargs = dict(params)
    if fields:
        kwargs["fields"] = fields

    try:
        func = getattr(pro, api_name)
        df = func(**kwargs)
    except Exception as e:
        print(f"[ERROR] Tushare API call failed: {e}")
        return None

    if df is None or len(df) == 0:
        print(f"[INFO] Tushare '{api_name}' returned empty result")
        print(f"  Params: {params}")
        return None

    total = len(df)
    if limit and total > limit:
        df_show = df.head(limit)
    else:
        df_show = df

    print(f"\n[OK] Tushare '{api_name}' returned {total} rows")
    print(f"  Params: {params}")
    if fields:
        print(f"  Fields: {fields}")
    print(f"  Columns: {list(df.columns)}")
    print(f"\n{df_show.to_string(index=False)}")
    if total > limit:
        print(f"\n  ... ({total - limit} more rows, showing first {limit})")

    # 自动检测日期范围
    for col in ["trade_date", "list_date", "base_date"]:
        if col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) > 0:
                print(f"\n  {col} range: {non_null.min()} ~ {non_null.max()}")

    return df


def compare_db(api_name: str, tushare_df, table_name: str, key_cols: list[str]):
    """对比 Tushare 返回数据与数据库已有数据"""
    import pymysql

    db_config = {
        "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.getenv("MYSQL_PORT", 3307)),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "root"),
        "database": "my_stock",
        "charset": "utf8mb4",
    }

    # 验证 key_cols 在 tushare_df 中存在
    missing_cols = [c for c in key_cols if c not in tushare_df.columns]
    if missing_cols:
        print(f"[ERROR] Key columns {missing_cols} not in Tushare result")
        return

    conn = pymysql.connect(**db_config)
    try:
        with conn.cursor() as cur:
            # 构建 WHERE 条件，用 Tushare 返回的 key 值查数据库
            conditions = []
            values = []
            for _, row in tushare_df.iterrows():
                cond_parts = []
                for col in key_cols:
                    cond_parts.append(f"`{col}` = %s")
                    values.append(str(row[col]))
                conditions.append(f"({' AND '.join(cond_parts)})")

            if not conditions:
                print("[INFO] No data to compare")
                return

            where_clause = " OR ".join(conditions)
            sql = f"SELECT {','.join(f'`{c}`' for c in key_cols)} FROM `{table_name}` WHERE {where_clause}"

            try:
                cur.execute(sql, values)
            except Exception as e:
                print(f"[ERROR] DB query failed: {e}")
                return

            db_rows = cur.fetchall()
            db_keys = set()
            for row in db_rows:
                db_keys.add(tuple(str(v) for v in row))

    finally:
        conn.close()

    # 对比
    ts_keys = set()
    for _, row in tushare_df.iterrows():
        ts_keys.add(tuple(str(row[c]) for c in key_cols))

    in_both = ts_keys & db_keys
    only_tushare = ts_keys - db_keys
    only_db = db_keys - ts_keys

    print(f"\n{'='*60}")
    print(f"DB Compare: Tushare '{api_name}' vs DB '{table_name}'")
    print(f"  Key: {key_cols}")
    print(f"{'='*60}")
    print(f"  Tushare rows: {len(ts_keys)}")
    print(f"  DB rows:      {len(db_keys)}")
    print(f"  In both:      {len(in_both)}")
    print(f"  Only Tushare: {len(only_tushare)}  <- DB missing these")
    print(f"  Only DB:      {len(only_db)}")

    if only_tushare:
        print(f"\n  Missing from DB (first 20):")
        for i, key in enumerate(sorted(only_tushare)):
            if i >= 20:
                print(f"    ... and {len(only_tushare) - 20} more")
                break
            key_str = ", ".join(f"{c}={v}" for c, v in zip(key_cols, key))
            print(f"    {key_str}")


def parse_params(param_strs: list[str]) -> dict:
    """解析 key=value 参数"""
    params = {}
    for s in param_strs:
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        params[k.strip()] = v.strip()
    return params


def main():
    parser = argparse.ArgumentParser(
        description="Tushare Pro data checker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s index_daily ts_code=000990.SH
  %(prog)s sw_daily ts_code=801010.SI
  %(prog)s index_basic market=SW category=一级行业指数
  %(prog)s sw_daily trade_date=20260312 --limit 5
  %(prog)s index_daily ts_code=000990.SH --db-table index_daily --db-key ts_code,trade_date
  %(prog)s sw_daily --doc
""",
    )
    parser.add_argument("api_name", help="Tushare API name (e.g. index_daily, sw_daily)")
    parser.add_argument("params", nargs="*", help="API params as key=value pairs")
    parser.add_argument("--limit", type=int, default=10, help="Max rows to display (default: 10)")
    parser.add_argument("--fields", default="", help="Output fields (comma-separated)")
    parser.add_argument("--doc", action="store_true", help="Show API documentation")
    parser.add_argument("--db-table", default="", help="DB table to compare against")
    parser.add_argument("--db-key", default="", help="Key columns for comparison (comma-separated)")

    args = parser.parse_args()
    catalog = load_catalog()

    # 显示文档
    if args.doc:
        show_doc(args.api_name, catalog)
        return

    # 显示接口信息
    if args.api_name in catalog:
        info = catalog[args.api_name]
        print(f"[{info.get('level1', '?')} > {info.get('level2', '?')}] {args.api_name}")
    else:
        print(f"[INFO] '{args.api_name}' not in local catalog, calling anyway...")

    # 调用 Tushare
    params = parse_params(args.params)
    df = call_tushare(args.api_name, params, fields=args.fields, limit=args.limit)

    # 数据库对比
    if df is not None and args.db_table:
        key_cols = [c.strip() for c in args.db_key.split(",") if c.strip()]
        if not key_cols:
            # 自动猜测 key
            for guess in [["ts_code", "trade_date"], ["ts_code"], ["index_code"]]:
                if all(c in df.columns for c in guess):
                    key_cols = guess
                    break
            if not key_cols:
                print("[WARN] Cannot guess key columns, use --db-key to specify")
                return
            print(f"[INFO] Auto-detected key columns: {key_cols}")

        compare_db(args.api_name, df, args.db_table, key_cols)


if __name__ == "__main__":
    main()
