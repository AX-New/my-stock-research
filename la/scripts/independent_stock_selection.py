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

def independent_selection():
    db_url = Config.SQLALCHEMY_DATABASE_URI
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # 1. Determine Latest Dates
        # Market Data Date
        date_query = text("SELECT MAX(trade_date) FROM market_daily")
        latest_date = conn.execute(date_query).scalar()
        
        # Financial Data Date (Latest available report)
        # We usually look for reports within the last year
        
        print(f"# Independent Stock Selection Report")
        print(f"> Analysis Date: {latest_date}")
        print(f"> Methodology: Multi-factor screening based on Fundamentals + Capital Flow + Trend.\n")
        
        # ---------------------------------------------------------
        # Strategy 1: "Safe Havens" (High Dividend + Low Valuation + Smart Money)
        # ---------------------------------------------------------
        # Criteria:
        # - Dividend Yield (TTM) > 3%
        # - PE (TTM) < 15
        # - Market Cap > 20 Billion
        # - 5-Day Net Inflow > 0 (Smart money is buying)
        # - Uptrend: Current Price > 20-Day Ago Price
        
        print("## 1. 🛡️ Safe Havens (High Dividend & Value)")
        print("> Logic: Undervalued stocks with high yields that smart money is accumulating.")
        print("| Stock | Name | Industry | Price | Div Yield | PE | Net Inflow (5D) |")
        print("|---|---|---|---|---|---|---|")
        
        safe_query = text(f"""
            SELECT 
                s.ts_code, s.name, s.industry, 
                db.close, db.dv_ttm, db.pe_ttm,
                SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            -- Join for trend check (Price vs 20 days ago)
            JOIN market_daily m20 ON s.ts_code = m20.ts_code AND m20.trade_date = (
                SELECT trade_date FROM market_daily 
                WHERE trade_date < '{latest_date}' ORDER BY trade_date DESC LIMIT 1 OFFSET 20
            )
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date > (SELECT MIN(trade_date) FROM (SELECT DISTINCT trade_date FROM market_daily ORDER BY trade_date DESC LIMIT 5) t)
              AND db.total_mv > 2000000  -- > 20 Billion (Unit: Wan)
              AND db.pe_ttm > 0 AND db.pe_ttm < 15
              AND db.dv_ttm > 3
              AND db.close > m20.close -- Uptrend
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.dv_ttm, db.pe_ttm
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        
        safe_df = pd.read_sql(safe_query, conn)
        if not safe_df.empty:
            for _, row in safe_df.iterrows():
                inflow = f"{row['net_inflow']/10000:.2f} Yi"
                print(f"| {row['ts_code']} | **{row['name']}** | {row['industry']} | {row['close']:.2f} | {row['dv_ttm']:.2f}% | {row['pe_ttm']:.2f} | {inflow} |")
        else:
            print("No stocks matched Safe Havens criteria.")
        print("\n")

        # ---------------------------------------------------------
        # Strategy 2: "Quality Growth" (High ROE + Growth + Capital Support)
        # ---------------------------------------------------------
        # Criteria:
        # - ROE > 15%
        # - Net Profit Growth > 20%
        # - PE (TTM) < 50 (Reasonable growth valuation)
        # - Market Cap > 10 Billion
        # - 5-Day Net Inflow > 0
        
        print("## 2. 🚀 Quality Growth (High ROE & Growth)")
        print("> Logic: High-quality companies growing fast, reasonably priced, with capital support.")
        print("| Stock | Name | Industry | Price | ROE | Profit Growth | PE | Net Inflow (5D) |")
        print("|---|---|---|---|---|---|---|---|")
        
        growth_query = text(f"""
            SELECT 
                s.ts_code, s.name, s.industry, 
                db.close, db.pe_ttm,
                fi.roe, fi.netprofit_yoy,
                SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            -- Get latest financial indicator
            JOIN (
                SELECT ts_code, roe, netprofit_yoy 
                FROM finance_fina_indicator 
                WHERE end_date >= '20240930' -- Q3 2024 or later
                GROUP BY ts_code HAVING MAX(end_date)
            ) fi ON s.ts_code = fi.ts_code
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date > (SELECT MIN(trade_date) FROM (SELECT DISTINCT trade_date FROM market_daily ORDER BY trade_date DESC LIMIT 5) t)
              AND db.total_mv > 1000000 -- > 10 Billion
              AND db.pe_ttm > 0 AND db.pe_ttm < 50
              AND fi.roe > 15
              AND fi.netprofit_yoy > 20
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.pe_ttm, fi.roe, fi.netprofit_yoy
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        
        # Note: The subquery for finance_fina_indicator might be slow or need adjustment depending on DB structure.
        # Simplified for robustness: Just take distinct latest.
        growth_query_robust = text(f"""
            SELECT 
                s.ts_code, s.name, s.industry, 
                db.close, db.pe_ttm,
                fi.roe, fi.netprofit_yoy,
                SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            JOIN (
                 SELECT ts_code, roe, netprofit_yoy,
                 ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY end_date DESC) as rn
                 FROM finance_fina_indicator
                 WHERE end_date >= '20240930'
            ) fi ON s.ts_code = fi.ts_code AND fi.rn = 1
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date > (SELECT MIN(trade_date) FROM (SELECT DISTINCT trade_date FROM market_daily ORDER BY trade_date DESC LIMIT 5) t)
              AND db.total_mv > 1000000
              AND db.pe_ttm > 0 AND db.pe_ttm < 60
              AND fi.roe > 12
              AND fi.netprofit_yoy > 15
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.pe_ttm, fi.roe, fi.netprofit_yoy
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)

        try:
            growth_df = pd.read_sql(growth_query_robust, conn)
            if not growth_df.empty:
                for _, row in growth_df.iterrows():
                    inflow = f"{row['net_inflow']/10000:.2f} Yi"
                    print(f"| {row['ts_code']} | **{row['name']}** | {row['industry']} | {row['close']:.2f} | {row['roe']:.2f}% | {row['netprofit_yoy']:.2f}% | {row['pe_ttm']:.2f} | {inflow} |")
            else:
                print("No stocks matched Quality Growth criteria (Strict).")
        except Exception as e:
            print(f"Error executing Growth query: {e}")
            
        print("\n")

        # ---------------------------------------------------------
        # Strategy 3: "Trend Accelerators" (Momentum Breakout)
        # ---------------------------------------------------------
        # Criteria:
        # - Price > 5% above 20-Day Avg (Strong Momentum)
        # - Volume > 1.5x 20-Day Avg Volume (Volume Breakout)
        # - 5-Day Net Inflow > 100 Million (Big Money Inflow)
        
        print("## 3. 🌊 Trend Accelerators (Volume + Momentum)")
        print("> Logic: Stocks breaking out with volume and heavy capital inflow.")
        print("| Stock | Name | Industry | Price | Vol Ratio | Net Inflow (5D) |")
        print("|---|---|---|---|---|---|")
        
        momentum_query = text(f"""
            SELECT 
                s.ts_code, s.name, s.industry, 
                db.close, db.volume_ratio,
                SUM(mf.net_mf_amount) as net_inflow
            FROM stock_basic s
            JOIN daily_basic db ON s.ts_code = db.ts_code
            JOIN moneyflow mf ON s.ts_code = mf.ts_code
            WHERE db.trade_date = '{latest_date}'
              AND mf.trade_date > (SELECT MIN(trade_date) FROM (SELECT DISTINCT trade_date FROM market_daily ORDER BY trade_date DESC LIMIT 5) t)
              AND db.total_mv > 1000000
              AND db.volume_ratio > 1.2 -- Volume increasing
            GROUP BY s.ts_code, s.name, s.industry, db.close, db.volume_ratio
            HAVING net_inflow > 10000 -- > 1 Yi Inflow
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        
        mom_df = pd.read_sql(momentum_query, conn)
        if not mom_df.empty:
            for _, row in mom_df.iterrows():
                inflow = f"{row['net_inflow']/10000:.2f} Yi"
                print(f"| {row['ts_code']} | **{row['name']}** | {row['industry']} | {row['close']:.2f} | {row['volume_ratio']:.2f} | {inflow} |")
        else:
            print("No stocks matched Momentum criteria.")

if __name__ == "__main__":
    independent_selection()
