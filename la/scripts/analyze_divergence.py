import pandas as pd
import sys
import os
import numpy as np
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.database import engine

def analyze_short_vs_long():
    # Load picks with T+2 returns
    sql = """
    SELECT p.*, b.industry 
    FROM la_pick p
    LEFT JOIN stock_basic b ON p.ts_code = b.ts_code
    WHERE p.return_t2 IS NOT NULL
    """
    df = pd.read_sql(sql, engine)
    
    if df.empty:
        print("No data found.")
        return

    # Calculate actual stock movement (unadjusted return)
    # return_t2 is already (close_t2 - buy) / buy * 100
    # For Short picks, positive return_t2 means stock went UP (loss for short strategy)
    # For analysis, we want to see the RAW stock movement.
    df['raw_return'] = df['return_t2'] 
    
    # 1. Distribution of Raw Returns by Direction
    print("\n### 1. Raw Stock Movement Distribution (T+2)")
    print("What actually happened to the stocks picked for Long vs Short?")
    
    stats = df.groupby('direction')['raw_return'].describe()
    print(stats.to_markdown())
    
    # Calculate "Big Drop" probability (Drop > 3%)
    df['big_drop'] = df['raw_return'] < -3.0
    df['big_rise'] = df['raw_return'] > 3.0
    
    print("\nProbability of Extreme Moves:")
    extreme_probs = df.groupby('direction')[['big_drop', 'big_rise']].mean() * 100
    print(extreme_probs.to_markdown())
    
    print("-" * 40)

    # 2. Industry Analysis for Short Picks
    print("\n### 2. Why are Short picks so accurate? (Industry Breakdown)")
    # Filter for Short picks that successfully dropped (raw_return < 0)
    short_success = df[(df['direction'] == 'short') & (df['raw_return'] < 0)]
    
    if not short_success.empty:
        ind_counts = short_success['industry'].value_counts().head(10)
        ind_avg_drop = short_success.groupby('industry')['raw_return'].mean().loc[ind_counts.index]
        
        ind_stats = pd.DataFrame({'Count': ind_counts, 'Avg Drop': ind_avg_drop})
        print("Top Industries in Successful Short Picks:")
        print(ind_stats.to_markdown())
    else:
        print("No successful short picks found.")

    print("-" * 40)

    # 3. Why are Long picks weak? (Industry Breakdown)
    print("\n### 3. Why are Long picks weak? (Industry Breakdown)")
    # Filter for Long picks that failed (raw_return < 0) or were weak (0 < raw_return < 2)
    long_weak = df[(df['direction'] == 'long') & (df['raw_return'] < 2)]
    
    if not long_weak.empty:
        ind_counts = long_weak['industry'].value_counts().head(10)
        ind_avg_ret = long_weak.groupby('industry')['raw_return'].mean().loc[ind_counts.index]
        
        ind_stats = pd.DataFrame({'Count': ind_counts, 'Avg Return': ind_avg_ret})
        print("Top Industries in Weak/Failed Long Picks:")
        print(ind_stats.to_markdown())
    
    print("-" * 40)
    
    # 4. Strategy Effectiveness Check
    print("\n### 4. Strategy Effectiveness (Avg Raw Return by Strategy & Direction)")
    strat_stats = df.groupby(['methodology', 'direction'])['raw_return'].mean().unstack()
    print(strat_stats.to_markdown())

if __name__ == "__main__":
    analyze_short_vs_long()
