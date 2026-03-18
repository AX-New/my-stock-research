import pandas as pd
import sys
import os
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.database import engine

def analyze_picks():
    # Load all picks
    sql = """
    SELECT * FROM la_pick 
    WHERE eval_date IN ('20260312', '20260316')
    """
    df = pd.read_sql(sql, engine)
    
    if df.empty:
        print("No data found in la_pick for dates 20260312 and 20260316.")
        return

    # Filter for the latest version per model per date to avoid duplicates if multiple versions exist
    # (Assuming higher version number is better/newer)
    # Actually, let's just look at what we have first.
    # Group by date, model, version to see counts
    print("Data distribution:")
    print(df.groupby(['eval_date', 'model_name', 'version']).size())
    print("-" * 50)

    # Use the latest version for each model on each date
    df_latest = df.sort_values('version', ascending=False).groupby(['eval_date', 'model_name', 'ts_code', 'methodology']).first().reset_index()
    
    # Separate by date
    df_12 = df_latest[df_latest['eval_date'] == '20260312']
    df_16 = df_latest[df_latest['eval_date'] == '20260316']
    
    print(f"20260312 Picks: {len(df_12)}")
    print(f"20260316 Picks: {len(df_16)}")
    print("-" * 50)

    # 1. Strategy Comparison
    print("### 1. Strategy Popularity (Count of picks per methodology)")
    strat_12 = df_12['methodology'].value_counts()
    strat_16 = df_16['methodology'].value_counts()
    strat_df = pd.DataFrame({'20260312': strat_12, '20260316': strat_16}).fillna(0).astype(int)
    print(strat_df)
    print("-" * 50)

    # 2. Top Picked Stocks (Most consensus across models)
    print("### 2. Top Consensus Stocks (Picked by multiple models/strategies)")
    
    def get_consensus(d_df, date_str):
        # Count how many times a stock appears (across models/strategies)
        # We also want to know if the direction is consistent
        counts = d_df.groupby(['ts_code', 'stock_name', 'direction']).size().reset_index(name='count')
        counts = counts.sort_values('count', ascending=False)
        print(f"\n[{date_str}] Top Consensus:")
        print(counts[counts['count'] > 1].head(10).to_markdown(index=False))
        return counts

    consensus_12 = get_consensus(df_12, '20260312')
    consensus_16 = get_consensus(df_16, '20260316')
    print("-" * 50)

    # 3. Overlap Analysis (Stocks present in both days)
    print("### 3. Stocks Picked on Both Days")
    
    # Simplify to (ts_code, direction) tuples for comparison
    # We take the set of unique (ts_code, direction) for each date
    # Note: A stock could be picked 'long' by one model and 'short' by another on the same day.
    # Let's check for conflicts first.
    
    def check_conflicts(d_df, date_str):
        conflicts = d_df.groupby('ts_code')['direction'].nunique()
        conflicts = conflicts[conflicts > 1]
        if not conflicts.empty:
            print(f"\n[{date_str}] Stocks with conflicting directions (Long & Short):")
            print(conflicts.index.tolist())
        else:
            print(f"\n[{date_str}] No conflicting directions found.")

    check_conflicts(df_12, '20260312')
    check_conflicts(df_16, '20260316')

    # Now find overlap
    # We'll focus on stocks that appear in at least one model on both days
    stocks_12 = set(df_12['ts_code'])
    stocks_16 = set(df_16['ts_code'])
    overlap = stocks_12.intersection(stocks_16)
    
    print(f"\nNumber of overlapping stocks: {len(overlap)}")
    
    if len(overlap) > 0:
        print("\nOverlapping Stocks Analysis (Direction Change?):")
        overlap_data = []
        for code in overlap:
            # Get direction(s) for 12
            dir_12 = set(df_12[df_12['ts_code'] == code]['direction'])
            # Get direction(s) for 16
            dir_16 = set(df_16[df_16['ts_code'] == code]['direction'])
            
            name = df_12[df_12['ts_code'] == code]['stock_name'].iloc[0]
            
            status = "Unchanged"
            if dir_12 != dir_16:
                if 'long' in dir_12 and 'short' in dir_16:
                    status = "Flip: Long -> Short"
                elif 'short' in dir_12 and 'long' in dir_16:
                    status = "Flip: Short -> Long"
                else:
                    status = f"Complex: {dir_12} -> {dir_16}"
            
            overlap_data.append({
                'ts_code': code,
                'stock_name': name,
                'dir_12': ','.join(dir_12),
                'dir_16': ','.join(dir_16),
                'status': status
            })
        
        overlap_df = pd.DataFrame(overlap_data)
        # Prioritize showing flips
        overlap_df['sort_key'] = overlap_df['status'].apply(lambda x: 0 if 'Flip' in x else 1)
        overlap_df = overlap_df.sort_values('sort_key')
        print(overlap_df[['ts_code', 'stock_name', 'dir_12', 'dir_16', 'status']].head(20).to_markdown(index=False))

if __name__ == "__main__":
    analyze_picks()
