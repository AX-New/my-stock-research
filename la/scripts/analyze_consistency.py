import pandas as pd
import sys
import os

# Connect to database
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
try:
    from app.database import engine
except ImportError:
    # Fallback if run from scripts folder
    from app.database import engine

def analyze_consistency():
    # Load all picks
    sql = """
    SELECT p.eval_date, p.ts_code, b.name as stock_name, p.direction, p.model_name
    FROM la_pick p
    LEFT JOIN stock_basic b ON p.ts_code = b.ts_code
    """
    df = pd.read_sql(sql, engine)

    dates = sorted(df['eval_date'].unique())
    if len(dates) < 2:
        print(f"Not enough dates to compare. Found dates: {dates}")
        return

    # Compare the latest two dates
    date1 = dates[-2]
    date2 = dates[-1]

    print(f"Comparing dates: Date1 ({date1}) vs Date2 ({date2})")

    # Get unique stock + direction picks per date 
    # (If a stock was picked by multiple models on the same day in the same direction, we count it as 1 unique signal)
    picks_d1 = df[df['eval_date'] == date1].groupby(['ts_code', 'stock_name', 'direction']).size().reset_index()
    picks_d2 = df[df['eval_date'] == date2].groupby(['ts_code', 'stock_name', 'direction']).size().reset_index()

    # If a stock was picked as both Long and Short on the SAME day, it will appear twice in picks_d1.
    # We will handle this by merging on ts_code and stock_name.

    print(f"Total unique signals on {date1}: {len(picks_d1)}")
    print(f"Total unique signals on {date2}: {len(picks_d2)}")

    # Find intersection based on stock code
    merged = pd.merge(picks_d1, picks_d2, on=['ts_code', 'stock_name'], how='inner', suffixes=('_d1', '_d2'))
    
    # Clean up column names for readability
    merged = merged.rename(columns={'direction_d1': f'dir_{date1}', 'direction_d2': f'dir_{date2}'})
    
    # Calculate unique stocks in intersection
    unique_intersect_stocks = merged['ts_code'].nunique()
    print(f"\nStocks appearing on BOTH days: {unique_intersect_stocks}")
    
    if not merged.empty:
        # Check direction consistency
        consistent_dir = merged[merged[f'dir_{date1}'] == merged[f'dir_{date2}']]
        inconsistent_dir = merged[merged[f'dir_{date1}'] != merged[f'dir_{date2}']]
        
        print(f"Direction Consistent (Same Long/Short): {len(consistent_dir)}")
        print(f"Direction INCONSISTENT (Flipped): {len(inconsistent_dir)}")
        
        if not consistent_dir.empty:
            print("\n### Consistent Stocks Details")
            print(consistent_dir[['ts_code', 'stock_name', f'dir_{date1}']].to_markdown(index=False))
            
        if not inconsistent_dir.empty:
            print("\n### Inconsistent Stocks (Direction Flipped) Details")
            print(inconsistent_dir[['ts_code', 'stock_name', f'dir_{date1}', f'dir_{date2}']].to_markdown(index=False))
    
    # Calculate Retention/Overlap Ratio
    unique_stocks_d1 = picks_d1['ts_code'].nunique()
    unique_stocks_d2 = picks_d2['ts_code'].nunique()
    
    overlap_ratio_d1 = unique_intersect_stocks / unique_stocks_d1 * 100 if unique_stocks_d1 > 0 else 0
    overlap_ratio_d2 = unique_intersect_stocks / unique_stocks_d2 * 100 if unique_stocks_d2 > 0 else 0
    
    print("\n### Summary Statistics")
    print(f"- Unique stocks picked on {date1}: {unique_stocks_d1}")
    print(f"- Unique stocks picked on {date2}: {unique_stocks_d2}")
    print(f"- Intersection: {unique_intersect_stocks}")
    print(f"- {date1} Retention Rate (Stocks carried over to {date2}): {overlap_ratio_d1:.2f}%")
    print(f"- {date2} New Blood Rate (Stocks that are completely new): {100 - overlap_ratio_d2:.2f}%")

if __name__ == "__main__":
    analyze_consistency()
