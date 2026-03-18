import pandas as pd
import sys
import os
import numpy as np
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.database import engine

def evaluate_consensus():
    # Load all picks with T+2 returns (to ensure we can evaluate performance)
    sql = """
    SELECT * FROM la_pick 
    WHERE return_t2 IS NOT NULL
    """
    df = pd.read_sql(sql, engine)
    
    if df.empty:
        print("No picks with T+2 return data found.")
        return

    # Calculate Adjusted Return for T+2
    df['adj_return_t2'] = df.apply(
        lambda row: row['return_t2'] if row['direction'] == 'long' else -row['return_t2'], 
        axis=1
    )

    # Use the latest version per model per date to avoid double counting
    df_latest = df.sort_values('version', ascending=False).groupby(['eval_date', 'model_name', 'ts_code', 'methodology']).first().reset_index()

    print(f"Total picks with T+2 data: {len(df_latest)}")

    # Group by Date and Stock to find overlaps
    # We want to see how many models picked the same stock on the same day
    # and whether they agreed on direction.
    
    # Aggregating at (eval_date, ts_code) level
    # We aggregate:
    # - model_names: list of models
    # - directions: set of directions (to check consistency)
    # - methodologies: list of strategies
    # - avg_return: mean of adj_return_t2 (performance of the stock for that day)
    # Note: Since adj_return_t2 depends on direction, if directions differ, the raw stock return is fixed, 
    # but the 'strategy return' differs. 
    # Actually, for consensus analysis, we are evaluating the "signal".
    # If Model A says Long and Model B says Long -> Signal is Strong Long.
    # If Model A says Long and Model B says Short -> Signal is Conflicted.
    
    # Let's group by (eval_date, ts_code, direction) first to find "Same-Direction Consensus"
    grouped = df_latest.groupby(['eval_date', 'ts_code', 'direction']).agg({
        'model_name': lambda x: list(x.unique()),
        'methodology': lambda x: list(x),
        'adj_return_t2': 'mean', # All entries in this group have same direction, so returns should be identical (or close if buy prices differ slightly)
        'stock_name': 'first'
    }).reset_index()
    
    grouped['model_count'] = grouped['model_name'].apply(len)
    
    # 1. Consensus vs Single (Resonance Analysis)
    print("\n### 1. Resonance Performance (T+2)")
    print("Definition: Resonance = Picked by >= 2 models with SAME direction on the same day.")
    
    consensus_picks = grouped[grouped['model_count'] >= 2]
    single_picks = grouped[grouped['model_count'] == 1]
    
    def print_stats(name, data):
        if data.empty:
            print(f"{name}: No samples")
            return
        
        avg_ret = data['adj_return_t2'].mean()
        win_rate = (data['adj_return_t2'] > 0).mean() * 100
        count = len(data)
        print(f"{name}: Count={count}, Avg Return={avg_ret:.2f}%, Win Rate={win_rate:.1f}%")

    print_stats("All Consensus Picks", consensus_picks)
    print_stats("Single Model Picks ", single_picks)
    
    print("-" * 40)
    
    # 2. Long vs Short Consensus
    print("\n### 2. Long vs Short Resonance")
    long_consensus = consensus_picks[consensus_picks['direction'] == 'long']
    short_consensus = consensus_picks[consensus_picks['direction'] == 'short']
    
    print_stats("Long Consensus ", long_consensus)
    print_stats("Short Consensus", short_consensus)
    
    print("-" * 40)
    
    # 3. Conflict Analysis
    # Check if any stocks appear in both Long and Short for the same date
    print("\n### 3. Conflict Analysis (Divergence)")
    # Group by date and stock only
    stock_date_group = df_latest.groupby(['eval_date', 'ts_code']).agg({
        'direction': lambda x: set(x)
    }).reset_index()
    
    conflicts = stock_date_group[stock_date_group['direction'].apply(lambda x: len(x) > 1)]
    
    if not conflicts.empty:
        print(f"Found {len(conflicts)} stocks with conflicting signals (both Long and Short on same day):")
        # We can analyze the outcome of these volatile stocks.
        # Usually we look at absolute movement or volatility?
        # Or simply: does Long win or Short win?
        for idx, row in conflicts.iterrows():
            # Get the actual return of the stock (from Long entry)
            # Find the Long entry to get the 'real' stock movement direction
            # If stock went up, Long wins. If down, Short wins.
            date = row['eval_date']
            code = row['ts_code']
            
            # Find raw return from the original df
            entries = df_latest[(df_latest['eval_date'] == date) & (df_latest['ts_code'] == code)]
            stock_name = entries.iloc[0]['stock_name']
            
            # Raw return T2 (approximate from Long entry or negate Short entry)
            # Let's take the mean of return_t2 from Long entries if exist, else -1 * Short entries
            long_entries = entries[entries['direction'] == 'long']
            if not long_entries.empty:
                raw_ret = long_entries['return_t2'].mean()
            else:
                # Should not happen if conflict exists
                raw_ret = 0 
            
            outcome = "UP" if raw_ret > 0 else "DOWN"
            print(f"  - {date} {code} {stock_name}: Stock T+2 move {raw_ret:.2f}% ({outcome})")
            print(f"    Long Models: {entries[entries['direction']=='long']['model_name'].tolist()}")
            print(f"    Short Models: {entries[entries['direction']=='short']['model_name'].tolist()}")
    else:
        print("No conflicting signals found (no stock was both Long and Short on the same day).")

    print("-" * 40)

    # 4. Top Resonance Stocks Detail
    if not consensus_picks.empty:
        print("\n### 4. Top Resonance Stocks Details")
        # Sort by model_count desc, then absolute return desc
        consensus_picks['abs_return'] = consensus_picks['adj_return_t2'].abs()
        top_consensus = consensus_picks.sort_values(['model_count', 'abs_return'], ascending=[False, False])
        
        print(top_consensus[['eval_date', 'ts_code', 'stock_name', 'direction', 'model_count', 'model_name', 'adj_return_t2']].head(15).to_markdown(index=False))

if __name__ == "__main__":
    evaluate_consensus()
