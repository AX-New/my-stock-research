import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from app.config import Config
except ImportError:
    print("Could not import app.config. Ensure you are running this from the project root or script directory.")
    sys.exit(1)

def independent_selection_6_strategies():
    db_url = Config.SQLALCHEMY_DATABASE_URI
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # 1. Determine Latest Dates
        date_query = text("SELECT MAX(trade_date) FROM market_daily")
        latest_date = conn.execute(date_query).scalar()
        
        # Get start date for 5-day flow
        start_date_query = text(f"SELECT MIN(trade_date) FROM (SELECT DISTINCT trade_date FROM market_daily WHERE trade_date <= '{latest_date}' ORDER BY trade_date DESC LIMIT 5) t")
        start_date = conn.execute(start_date_query).scalar()
        
        print(f"# Independent Stock Selection (6 Strategies)")
        print(f"> Date: {latest_date}")
        print(f"> Data Source: Local Database")
        print(f"> Ranking Logic: Fundamentals + Technicals + Smart Money Flow\n")
        
        # Helper to print table
        def print_table(df, strategy_name, desc):
            print(f"## {strategy_name}")
            print(f"> {desc}")
            if df.empty:
                print("No stocks matched criteria.")
            else:
                print("| Stock | Name | Industry | Price | Key Metric | Net Inflow (5D) |")
                print("|---|---|---|---|---|---|")
                for _, row in df.iterrows():
                    inflow = f"{row['net_inflow']/10000:.2f} Yi" if abs(row['net_inflow']) > 10000 else f"{row['net_inflow']:.2f} Wan"
                    print(f"| {row['ts_code']} | **{row['name']}** | {row['industry']} | {row['close']:.2f} | {row['key_metric']} | {inflow} |")
            print("\n")

        # ---------------------------------------------------------
        # 1. Deep Value (深度价值)
        # ---------------------------------------------------------
        # PE < 15, PB < 1.5, Dividend > 3%
        query_value = text(f"""
            SELECT s.ts_code, s.name, s.industry, db.close, 
                   CONCAT('PE:', ROUND(db.pe_ttm,1), ' Div:', ROUND(db.dv_ttm,1), '%') as key_metric,
                   SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date >= '{start_date}'
              AND db.total_mv > 500000
              AND db.pe_ttm > 0 AND db.pe_ttm < 15
              AND db.pb < 1.5
              AND db.dv_ttm > 3
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.pe_ttm, db.dv_ttm
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        df_value = pd.read_sql(query_value, conn)
        print_table(df_value, "1. Deep Value (深度价值)", "Low PE/PB, High Dividend, Money Inflow.")

        # ---------------------------------------------------------
        # 2. High Growth (高成长)
        # ---------------------------------------------------------
        # ROE > 15, Profit Growth > 20, Revenue Growth > 10
        query_growth = text(f"""
            SELECT s.ts_code, s.name, s.industry, db.close, 
                   CONCAT('ROE:', ROUND(fi.roe,1), '% G:', ROUND(fi.netprofit_yoy,1), '%') as key_metric,
                   SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            JOIN (
                SELECT ts_code, roe, netprofit_yoy, or_yoy,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) as rn
                FROM finance_fina_indicator WHERE end_date >= '20240930'
            ) fi ON s.ts_code = fi.ts_code AND fi.rn = 1
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date >= '{start_date}'
              AND db.total_mv > 1000000
              AND fi.roe > 15
              AND fi.netprofit_yoy > 20
              AND fi.or_yoy > 10
              AND db.pe_ttm < 60
            GROUP BY s.ts_code, s.name, s.industry, db.close, fi.roe, fi.netprofit_yoy
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        df_growth = pd.read_sql(query_growth, conn)
        print_table(df_growth, "2. High Growth (高成长)", "High ROE, High Profit/Revenue Growth.")

        # ---------------------------------------------------------
        # 3. GARP (性价比成长)
        # ---------------------------------------------------------
        # PEG < 1, PE < 30
        query_garp = text(f"""
            SELECT s.ts_code, s.name, s.industry, db.close, 
                   CONCAT('PEG:', ROUND(db.pe_ttm/fi.netprofit_yoy, 2), ' PE:', ROUND(db.pe_ttm,1)) as key_metric,
                   SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            JOIN (
                SELECT ts_code, netprofit_yoy,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) as rn
                FROM finance_fina_indicator WHERE end_date >= '20240930'
            ) fi ON s.ts_code = fi.ts_code AND fi.rn = 1
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date >= '{start_date}'
              AND db.total_mv > 500000
              AND db.pe_ttm > 0 AND db.pe_ttm < 30
              AND fi.netprofit_yoy > 15
              AND (db.pe_ttm / fi.netprofit_yoy) < 1.0
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.pe_ttm, fi.netprofit_yoy
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        df_garp = pd.read_sql(query_garp, conn)
        print_table(df_garp, "3. GARP (性价比成长)", "PEG < 1, Reasonable PE, Good Growth.")

        # ---------------------------------------------------------
        # 4. Momentum (强势动量)
        # ---------------------------------------------------------
        # 20D Return > 10%, Volume Ratio > 1.2, Inflow > 0
        query_mom = text(f"""
            SELECT s.ts_code, s.name, s.industry, db.close, 
                   CONCAT('VolR:', ROUND(db.volume_ratio,1), ' 5D%:', ROUND((db.close-m5.close)/m5.close*100, 1), '%') as key_metric,
                   SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            JOIN market_daily m5 ON s.ts_code = m5.ts_code AND m5.trade_date = '{start_date}'
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date >= '{start_date}'
              AND db.total_mv > 500000
              AND (db.close - m5.close) / m5.close > 0.05
              AND db.volume_ratio > 1.2
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.volume_ratio, m5.close
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        df_mom = pd.read_sql(query_mom, conn)
        print_table(df_mom, "4. Momentum (强势动量)", "Price/Volume Breakout, Strong Inflow.")

        # ---------------------------------------------------------
        # 5. Reversal (超跌反转)
        # ---------------------------------------------------------
        # Price < 60D MA * 0.85, but 5D Inflow > 0
        query_rev = text(f"""
            SELECT s.ts_code, s.name, s.industry, db.close, 
                   CONCAT('Drop60D:', ROUND((db.close - m60.close)/m60.close*100, 1), '%') as key_metric,
                   SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            JOIN market_daily m60 ON s.ts_code = m60.ts_code AND m60.trade_date = (
                SELECT trade_date FROM market_daily WHERE trade_date < '{latest_date}' ORDER BY trade_date DESC LIMIT 1 OFFSET 60
            )
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date >= '{start_date}'
              AND db.total_mv > 100000 -- > 10 Yi (Relaxed)
              AND (db.close - m60.close) / m60.close < -0.05 -- Dropped 5% in 60 days
            GROUP BY s.ts_code, s.name, s.industry, db.close, m60.close
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        df_rev = pd.read_sql(query_rev, conn)
        print_table(df_rev, "5. Reversal (超跌反转)", "Oversold (60D Drop > 5%) but Smart Money Buying.")

        # ---------------------------------------------------------
        # 6. High Dividend Low Vol (红利低波)
        # ---------------------------------------------------------
        # Dividend > 4%, Beta Low (Proxied by Industry or simply large cap stable)
        # Let's use PB < 1 and Div > 4
        query_div = text(f"""
            SELECT s.ts_code, s.name, s.industry, db.close, 
                   CONCAT('Div:', ROUND(db.dv_ttm,1), '% PB:', ROUND(db.pb,2)) as key_metric,
                   SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date >= '{start_date}'
              AND db.total_mv > 1000000
              AND db.dv_ttm > 4
              AND db.pb < 1.0
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.dv_ttm, db.pb
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        df_div = pd.read_sql(query_div, conn)
        print_table(df_div, "6. High Dividend Low Vol (红利低波)", "High Yield (>4%), Low PB (<1.0), Stable.")

if __name__ == "__main__":
    independent_selection_6_strategies()
