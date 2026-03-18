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

def analyze_market_strategy():
    db_url = Config.SQLALCHEMY_DATABASE_URI
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # 1. Get Latest Trade Date and Start Date (5 days ago)
        dates_query = text("SELECT DISTINCT trade_date FROM market_daily ORDER BY trade_date DESC LIMIT 5")
        dates = [row[0] for row in conn.execute(dates_query).fetchall()]
        
        if not dates:
            print("No market data found.")
            return

        latest_date = dates[0]
        start_date = dates[-1] # 5th day back
        
        print(f"# Market Strategy Report - {latest_date}")
        print(f"> Data Source: Local Database (Tushare Pro)")
        print(f"> Analysis Period: {start_date} to {latest_date} (5 Trading Days)\n")
        
        # 2. Market Sentiment (Breadth & Volume)
        breadth_query = text(f"""
            SELECT 
                COUNT(CASE WHEN pct_chg > 0 THEN 1 END) as up_count,
                COUNT(CASE WHEN pct_chg < 0 THEN 1 END) as down_count,
                SUM(amount) as total_turnover
            FROM market_daily 
            WHERE trade_date = '{latest_date}'
        """)
        breadth = conn.execute(breadth_query).fetchone()
        
        up_count = breadth[0]
        down_count = breadth[1]
        total_turnover = breadth[2] / 100000 # Convert to 100M (Yi)
        
        sentiment = "Neutral"
        if up_count > down_count * 1.5:
            sentiment = "Bullish (Strong Breadth)"
        elif down_count > up_count * 1.5:
            sentiment = "Bearish (Weak Breadth)"
            
        print(f"## 1. Market Sentiment")
        print(f"- **Mood**: {sentiment}")
        print(f"- **Advancers/Decliners**: {up_count} Up / {down_count} Down")
        print(f"- **Total Turnover**: {total_turnover:.2f} Yi (CNY)")
        
        # 3. Capital Flow (Sector Rotation)
        print(f"\n## 2. Capital Flow (Smart Money)")
        print(f"> Analyzing Net Inflow (Smart Money) over the last 5 trading days.")
        
        # Simplified Sector Flow Query
        sector_flow_query = text(f"""
            SELECT 
                s.industry,
                SUM(mf.net_mf_amount) as net_inflow
            FROM moneyflow mf
            JOIN stock_basic s ON mf.ts_code = s.ts_code
            WHERE mf.trade_date >= '{start_date}' AND mf.trade_date <= '{latest_date}'
            GROUP BY s.industry
            HAVING net_inflow > 0
            ORDER BY net_inflow DESC
            LIMIT 5
        """)
        
        sector_df = pd.read_sql(sector_flow_query, conn)
        
        if sector_df.empty:
            print("No sector flow data found.")
        else:
            print(f"\n### Top 5 Sectors by Net Inflow (5-Day)")
            print(f"| Rank | Industry | Net Inflow (Wan) |")
            print(f"|---|---|---|")
            for i, row in sector_df.iterrows():
                print(f"| {i+1} | **{row['industry']}** | {row['net_inflow']:.2f} |")
                
        # 4. Strategy Picks (Smart Money Following)
        print(f"\n## 3. Strategy Picks (Smart Money Leaders)")
        print(f"> Strategy: Follow the money in hot sectors. Picking stocks with high net inflow and reasonable valuation.")
        
        top_industries = sector_df['industry'].head(3).tolist()
        
        for industry in top_industries:
            print(f"\n### Sector: {industry}")
            print(f"| Stock | Name | Price | Net Inflow (5D) | PE (TTM) | 5D Change |")
            print(f"|---|---|---|---|---|---|")
            
            # Optimized Stock Query
            # Pre-filter moneyflow by date range first (via WHERE)
            stock_query = text(f"""
                SELECT 
                    s.ts_code,
                    s.name,
                    db.close,
                    SUM(mf.net_mf_amount) as net_inflow,
                    db.pe_ttm,
                    (db.close - m5.close) / m5.close * 100 as pct_chg_5d
                FROM stock_basic s
                JOIN daily_basic db ON s.ts_code = db.ts_code
                JOIN moneyflow mf ON s.ts_code = mf.ts_code
                JOIN market_daily m5 ON s.ts_code = m5.ts_code
                WHERE s.industry = '{industry}'
                  AND db.trade_date = '{latest_date}'
                  AND mf.trade_date >= '{start_date}' AND mf.trade_date <= '{latest_date}'
                  AND m5.trade_date = '{start_date}'
                  AND db.total_mv > 500000  -- Market Cap > 50 Yi (Unit: Wan)
                  AND db.pe_ttm > 0 AND db.pe_ttm < 80
                GROUP BY s.ts_code, s.name, db.close, db.pe_ttm, m5.close
                HAVING net_inflow > 0
                ORDER BY net_inflow DESC
                LIMIT 3
            """)
            
            stocks_df = pd.read_sql(stock_query, conn)
            
            if stocks_df.empty:
                print(f"No suitable stocks found in {industry}.")
            else:
                for _, row in stocks_df.iterrows():
                    print(f"| {row['ts_code']} | **{row['name']}** | {row['close']:.2f} | {row['net_inflow']:.2f} | {row['pe_ttm']:.2f} | {row['pct_chg_5d']:.2f}% |")

if __name__ == "__main__":
    analyze_market_strategy()
