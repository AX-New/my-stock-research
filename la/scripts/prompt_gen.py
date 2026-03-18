"""
LA系统 prompt 生成脚本
从 la_indicator 读市场指标 + 加载策略模板 + 数据表描述 + 拼装完整prompt

用法:
    python la/scripts/prompt_gen.py --date 20260312
    python la/scripts/prompt_gen.py --date 20260312 --model doubao
"""
import sys
import os
import json
import argparse

# la/scripts/ → 项目根
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import text
from app.database import engine
from app.logger import get_logger

log = get_logger(__name__)

# 路径: 策略和配置在 la/ 下
LA_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STRATEGY_DIR = os.path.join(LA_ROOT, "strategies")
DATA_TABLES_FILE = os.path.join(LA_ROOT, "strategies", "data_tables.md")
DB_SCHEMA_FILE = os.path.join(PROJECT_ROOT, "config", "db_schema.md")

# 非策略文件（数据描述等辅助文件），扫描时排除
NON_STRATEGY_FILES = {"data_tables"}

# 全部可用策略（按文件自动发现）
def discover_strategies() -> list[str]:
    """扫描策略目录，返回可用策略名列表（排除非策略文件）"""
    if not os.path.isdir(STRATEGY_DIR):
        return []
    return sorted([
        f[:-3] for f in os.listdir(STRATEGY_DIR)
        if f.endswith(".md") and f[:-3] not in NON_STRATEGY_FILES
    ])


def get_latest_trade_date() -> str:
    """获取最新交易日"""
    with engine.connect() as conn:
        row = conn.execute(text("SELECT MAX(trade_date) FROM la_indicator")).fetchone()
        return row[0] if row else None



def load_indicator(trade_date: str) -> dict:
    """从 la_indicator 读取市场指标"""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT * FROM la_indicator WHERE trade_date = :td"
        ), {"td": trade_date}).fetchone()
        if not row:
            raise ValueError(f"la_indicator 无 {trade_date} 的数据，请先运行 calc_indicator")
        return dict(row._mapping)


def load_strategy(name: str) -> str:
    """加载单个策略模板"""
    path = os.path.join(STRATEGY_DIR, f"{name}.md")
    if not os.path.exists(path):
        raise FileNotFoundError(f"策略模板不存在: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _load_la_schema() -> str:
    """返回 la_pick 的 CSV 列定义，帮助模型正确填写字段"""
    return """## [3] LA系统 CSV 输出格式

选股结果写入 CSV 文件，列顺序：`methodology,ts_code,stock_name,direction,rating,score,buy_price,target_price,analysis`

| 字段 | 取值约束 |
|------|---------|
| methodology | value/growth/turnaround/trend/macd/comprehensive/hotspot/capital_flow/dividend_defense |
| ts_code | 股票代码，如 600519.SH |
| stock_name | 股票简称 |
| direction | long=看多, short=看空 |
| rating | buy=强烈推荐, hold=观望, sell=回避 |
| score | 1-100整数 |
| buy_price | 浮点数，保留2位小数 |
| target_price | 浮点数，保留2位小数 |
| analysis | ≤50字，**绝对不能含逗号**，多点用分号分隔 |"""


def load_data_tables_desc() -> str:
    """加载数据表描述文件，并附加db_schema.md的引用说明"""
    parts = []
    if os.path.exists(DATA_TABLES_FILE):
        with open(DATA_TABLES_FILE, "r", encoding="utf-8") as f:
            parts.append(f.read().strip())

    # 附加schema文件引用说明
    schema_hint = (
        "\n\n## 完整字段定义参考\n\n"
        f"详细字段名称和类型见: `config/db_schema.md`\n\n"
        "使用方式：**查询前先在该文件中搜索表名**，确认列名拼写（尤其是 stk_factor_pro 的技术指标"
        "均带 `_bfq/_hfq/_qfq` 复权后缀，例如 MACD-DIF 列名为 `macd_dif_bfq`），避免因列名错误"
        "导致查询失败。"
    )
    parts.append(schema_hint)
    return "\n".join(parts)


def format_indicator(ind: dict) -> str:
    """格式化市场指标为prompt文本"""
    top_ind = json.loads(ind.get("top_industries") or "[]")
    bottom_ind = json.loads(ind.get("bottom_industries") or "[]")
    top_str = ", ".join([f"{t['name']}({t['avg_chg']}%)" for t in top_ind])
    bottom_str = ", ".join([f"{t['name']}({t['avg_chg']}%)" for t in bottom_ind])

    def fmt(v, suffix="", pct=False):
        if v is None:
            return "N/A"
        if pct:
            return f"{'+' if v > 0 else ''}{v}%"
        return f"{v}{suffix}"

    return f"""## 当日市场指标（{ind['trade_date']}）

### 估值面
| 指标 | 值 |
|------|-----|
| 沪深300 PE | {fmt(ind.get('hs300_pe'))} |
| 沪深300 PE_TTM | {fmt(ind.get('hs300_pe_ttm'))} |
| 沪深300 PB | {fmt(ind.get('hs300_pb'))} |
| 中证500 PE | {fmt(ind.get('zz500_pe'))} |
| 中证500 PE_TTM | {fmt(ind.get('zz500_pe_ttm'))} |
| 中证500 PB | {fmt(ind.get('zz500_pb'))} |
| 全市场PE均值 | {fmt(ind.get('market_pe_avg'))} |
| 全市场PE中位数 | {fmt(ind.get('market_pe_median'))} |
| 全市场PB均值 | {fmt(ind.get('market_pb_avg'))} |
| 全市场PB中位数 | {fmt(ind.get('market_pb_median'))} |
| 全市场平均股息率 | {fmt(ind.get('market_dv_avg'), '%')} |
| 破净股数量 | {fmt(ind.get('broken_net_count'))} |
| 有效PE股票数 | {fmt(ind.get('total_count'))} |
| 当日交易股票总数 | {fmt(ind.get('trade_count'))} |

### 技术面
| 指标 | 值 |
|------|-----|
| 上证收盘 | {fmt(ind.get('sh_close'))} |
| 上证今日涨跌幅 | {fmt(ind.get('sh_pct_1d'), pct=True)} |
| 上证5日涨跌幅 | {fmt(ind.get('sh_pct_5d'), pct=True)} |
| 上证20日涨跌幅 | {fmt(ind.get('sh_pct_20d'), pct=True)} |
| 深证收盘 | {fmt(ind.get('sz_close'))} |
| 深证今日涨跌幅 | {fmt(ind.get('sz_pct_1d'), pct=True)} |
| 深证5日涨跌幅 | {fmt(ind.get('sz_pct_5d'), pct=True)} |
| 深证20日涨跌幅 | {fmt(ind.get('sz_pct_20d'), pct=True)} |
| 创业板收盘 | {fmt(ind.get('cy_close'))} |
| 创业板今日涨跌幅 | {fmt(ind.get('cy_pct_1d'), pct=True)} |
| 创业板5日涨跌幅 | {fmt(ind.get('cy_pct_5d'), pct=True)} |
| 创业板20日涨跌幅 | {fmt(ind.get('cy_pct_20d'), pct=True)} |
| 上涨家数 | {fmt(ind.get('up_count'))} |
| 下跌家数 | {fmt(ind.get('down_count'))} |
| 平盘家数 | {fmt(ind.get('flat_count'))} |
| 涨停 | {fmt(ind.get('limit_up'))} |
| 跌停 | {fmt(ind.get('limit_down'))} |
| 全市场平均换手率 | {fmt(ind.get('avg_turnover'), '%')} |

### 资金面
| 指标 | 值 |
|------|-----|
| 主力净流入 | {fmt(ind.get('net_amount'), '亿')} |
| 主力净流入占比 | {fmt(ind.get('net_amount_rate'), '%')} |
| 超大单净流入 | {fmt(ind.get('elg_amount'), '亿')} |
| 超大单占比 | {fmt(ind.get('elg_rate'), '%')} |
| 大单净流入 | {fmt(ind.get('lg_amount'), '亿')} |
| 大单占比 | {fmt(ind.get('lg_rate'), '%')} |
| 中单净流入 | {fmt(ind.get('md_amount'), '亿')} |
| 中单占比 | {fmt(ind.get('md_rate'), '%')} |
| 小单净流入 | {fmt(ind.get('sm_amount'), '亿')} |
| 小单占比 | {fmt(ind.get('sm_rate'), '%')} |
| 近5日主力累计净流入 | {fmt(ind.get('net_5d'), '亿')} |
| 全市场成交额 | {fmt(ind.get('today_amount'), '亿')} |
| 5日平均成交额 | {fmt(ind.get('avg_5d_amount'), '亿')} |
| 量比(今日/5日均) | {fmt(ind.get('amount_ratio'))} |

### 情绪面
| 指标 | 值 |
|------|-----|
| 涨跌比 | {fmt(ind.get('up_down_ratio'))} |
| 涨停跌停比 | {fmt(ind.get('limit_up_down_ratio'))} |
| 全市场平均涨跌幅 | {fmt(ind.get('avg_pct_chg'), pct=True)} |
| 量能变化(今日/20日均) | {fmt(ind.get('vol_ratio_20d'))} |
| 涨幅前10行业 | {top_str} |
| 跌幅前5行业 | {bottom_str} |"""


# ============================================================
# Prompt 模板（全策略执行，无选择步骤）
# ============================================================

SYSTEM_ROLE = """你是一位专业的A股量化分析师，拥有数据库直接查询能力（MySQL, 库名 my_stock）。

> 🔒 **独立任务声明**：你当前正在执行一个独立的分析任务。**不要尝试读取任何除`la/scripts/{model_name}`目录外的系统设计文档、项目文件或外部文档**。你的所有判断应仅基于本 prompt 提供的材料，以及通过 MySQL 查询数据库获取的补充数据，你可以
    使用脚本帮助处理任务，但是脚本必须放在my_stock项目目录下的，`la/scripts/{model_name}`内，同时在改目录下维护`00-脚本使用说明.md`文档。

**本次任务**: 评估日期 {eval_date} | 模型 {model_name}
> 版本号将在第二步通过脚本自动分配，无需提前获取。

本次提供以下材料，请按编号索引：
- **[1] 选股策略库** — {strategy_count}种策略，**全部执行，不做选择**
- **[2] 当日市场指标** — 评估日的估值/技术/资金/情绪四维数据
- **[3] LA写入目标表结构** — la_pick 的 CSV 列定义
- **[4] 可用数据表** — 数据库中可查询的表及说明（含 db_schema.md 字段参考）
- **[5] 任务指令** — 分析步骤和输出格式要求

目前系统只能提供基础的行情、因子和财务数据。如果不足以支撑分析，你可以自己编写Python脚本做更深入的计算（如自定义因子、回测验证、统计建模等）。

请按 [5] 任务指令 中的步骤依次执行。"""

TASK_INSTRUCTION = """## [5] 任务指令

### 评估日期: {eval_date} | 模型: {model_name}

你需要完成以下四个步骤。每一步都要认真分析，不要跳过。

### 第一步：市场环境研判

仔细阅读上方「当日市场指标」，从估值面、技术面、资金面、情绪面四个维度分析当前市场环境。
如果你认为需要更多数据来判断，可以查询数据库补充（如近期指数走势、板块轮动、北向资金等）。

你需要得出三个结论：
- **市场趋势（market_trend）**: 当前市场处于什么阶段？上涨趋势(up)、下跌趋势(down)、还是震荡整理(sideways)？
- **资金面强弱（market_capital）**: 资金面是积极的(strong)、消极的(weak)、还是中性的(neutral)？
- **市场情绪（market_sentiment）**: 整体情绪偏乐观(optimistic)、悲观(pessimistic)、还是中性(neutral)？

同时用2-3句话概括你对当前市场状态的总体看法（market_view）。

### 第二步：创建分析任务（写入市场研判）

将第一步的结论通过脚本写入数据库。执行以下命令：

```bash
python la/scripts/create_task.py \\
    --eval-date {eval_date} \\
    --model {model_name} \\
    --market-trend <你的判断> \\
    --market-capital <你的判断> \\
    --market-sentiment <你的判断> \\
    --market-view "<你的市场总体看法>"
```

参数取值：
- market-trend: up / down / sideways
- market-capital: strong / weak / neutral
- market-sentiment: optimistic / pessimistic / neutral
- market-view: 你的市场总体看法（2-3句话）

**脚本会输出一个版本号（纯数字），请记住这个版本号，后续步骤需要使用。**

### 第三步：逐策略选股

对 [1] 中的全部{strategy_count}种策略，逐一通过查询数据库完成选股：

**选股要求**：
- 每策略选出10只最看好的股票（direction=long）：最符合该策略逻辑，上涨概率最大
- 每策略选出10只最不看好的股票（direction=short）：最违背该策略逻辑，下跌概率最大
- 每只股票都要有明确的数据支撑，不要凭印象选股
- 买入价使用最新收盘价（查询 stk_factor_pro 或 market_daily）
- 目标价是你判断未来20个交易日内可能达到的价格

**排除规则**：
- 排除ST、*ST股票
- 排除停牌股票
- 排除上市不满1年的股票
- 排除北交所股票（ts_code不以.BJ结尾）
- 同一策略内做多和做空不可有同一只股票

### 第四步：写入选股结果

将全部选股结果写入 CSV 文件，共{total_picks}条（{strategy_count}策略 × 20条）。

**CSV 文件路径**（使用第二步获得的版本号）：
```
la/output/{eval_date}_{model_name}_v<版本号>_pick.csv
```

**CSV 格式**（第一行为表头）：
```csv
methodology,ts_code,stock_name,direction,rating,score,buy_price,target_price,analysis
value,600519.SH,贵州茅台,long,buy,92,1680.00,1850.00,低估值龙头;ROE>30%;机构增持
```

**字段定义见 [3] CSV 输出格式。**

**注意**：analysis 字段绝对不能包含逗号，否则会破坏CSV解析。用分号替代。

写完 CSV 后，执行导入脚本将数据写入数据库：

```bash
python la/scripts/import_result.py --file la/output/{eval_date}_{model_name}_v<版本号>_pick.csv
```

导入成功后，脚本会自动更新任务状态为 success。

**如果第三步或第四步失败**，请执行以下SQL标记任务失败：
```sql
UPDATE la_task SET status = 'failed', error_msg = '<失败原因>', finished_at = NOW()
WHERE eval_date = '{eval_date}' AND model_name = '{model_name}' AND version = <版本号>;
```"""


def build_prompt(eval_date: str = None, model_name: str = "claude") -> str:
    """
    构建完整prompt（全策略执行）

    Parameters:
        eval_date: 评估日期，默认la_indicator最新日期
        model_name: 模型名
    """
    if not eval_date:
        eval_date = get_latest_trade_date()
        if not eval_date:
            raise ValueError("la_indicator 表无数据")

    # 读取市场指标
    indicator = load_indicator(eval_date)
    log.info(f"[la_prompt] 市场指标已加载 | trade_date={eval_date}")

    # 加载所有策略模板
    all_strategies = discover_strategies()
    if not all_strategies:
        raise ValueError(f"策略目录为空或不存在: {STRATEGY_DIR}")
    strategy_texts = []
    for s in all_strategies:
        try:
            strategy_texts.append(load_strategy(s))
        except FileNotFoundError:
            log.warning(f"[la_prompt] 策略模板 {s}.md 不存在，跳过")

    if not strategy_texts:
        raise ValueError("没有成功加载任何策略模板")
    log.info(f"[la_prompt] 策略模板已加载 | 数量: {len(strategy_texts)} | 策略: {', '.join(all_strategies)}")

    # 加载数据表描述
    data_tables_desc = load_data_tables_desc()

    # 变量替换
    strategy_count = str(len(strategy_texts))
    total_picks = str(len(strategy_texts) * 20)
    all_strategies_str = ",".join(all_strategies)
    replacements = {
        "{eval_date}": eval_date, "{model_name}": model_name,
        "{strategy_count}": strategy_count,
        "{total_picks}": total_picks,
        "{all_strategies}": all_strategies_str,
    }
    def replace_vars(text):
        for k, v in replacements.items():
            text = text.replace(k, v)
        return text

    # 加载 CSV 输出格式定义
    la_schema = _load_la_schema()

    strategy_intro = (
        f"## [1] 选股策略库\n\n"
        f"以下是全部{strategy_count}种选股策略，**逐一执行所有策略，不做取舍**：\n\n"
        + "\n\n".join(strategy_texts)
    )

    # 拼装: 角色 → 策略 → 市场指标 → CSV格式 → 可用数据表 → 任务指令
    parts = [
        replace_vars(SYSTEM_ROLE.strip()),
        strategy_intro,
        format_indicator(indicator).replace("## 当日市场指标", "## [2] 当日市场指标"),
        la_schema,
        data_tables_desc.replace("# LA系统可用数据表", "## [4] 可用数据表"),
        replace_vars(TASK_INSTRUCTION.strip()),
    ]

    prompt = "\n\n---\n\n".join([p for p in parts if p])
    log.info(f"[la_prompt] prompt构建完成 | eval_date={eval_date} | "
             f"model={model_name} | 策略: {strategy_count}种 | 字符数: {len(prompt)}")
    return prompt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LA系统 prompt 生成")
    parser.add_argument("--date", help="评估日期 YYYYMMDD，默认最新交易日")
    parser.add_argument("--model", default="claude", help="模型名称")
    parser.add_argument("--output", "-o", help="输出文件路径，默认 la/temp/prompt_preview_{date}_{model}.md")
    args = parser.parse_args()

    prompt = build_prompt(args.date, args.model)

    eval_date = args.date or get_latest_trade_date()
    model_name = args.model or "claude"
    if not args.output:
        args.output = os.path.join(LA_ROOT, "temp", f"prompt_preview_{eval_date}_{model_name}.md")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(prompt)

    strategies = discover_strategies()
    print(f"prompt 已生成: {args.output}")
    print(f"  日期: {eval_date}")
    print(f"  策略: {', '.join(strategies)} ({len(strategies)}种)")
    print(f"  字符数: {len(prompt)}")
