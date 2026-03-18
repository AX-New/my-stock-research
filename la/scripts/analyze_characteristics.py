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

def analyze_latest(target_date='20260316'):
    print(f"Analyzing prediction results for: {target_date}")
    
    # 1. Load Data
    sql = f"""
    SELECT p.*, b.industry, b.name as stock_name
    FROM la_pick p
    LEFT JOIN stock_basic b ON p.ts_code = b.ts_code
    WHERE p.eval_date = '{target_date}'
    """
    try:
        df = pd.read_sql(sql, engine)
    except Exception as e:
        print(f"Error querying database: {e}")
        return

    if df.empty:
        print(f"No data found for date {target_date}")
        return

    # Drop duplicate columns if any
    df = df.loc[:, ~df.columns.duplicated()]

    print(f"Total picks found: {len(df)}")
    
    # Check if returns are available
    has_returns = 'return_t1' in df.columns and df['return_t1'].notna().sum() > 0
    if has_returns:
        print(f"Return data (T+1) available for {df['return_t1'].notna().sum()} records.")
    else:
        print("No T+1 return data available yet.")

    print("-" * 40)

    # 2. Long vs Short Distribution
    print("\n### 1. Long vs Short Distribution")
    dist = df['direction'].value_counts()
    print(dist.to_markdown())
    
    print("-" * 40)

    # 3. Resonance Analysis (Consensus)
    print("\n### 2. Resonance (Consensus) Analysis")
    # Group by stock and direction to find consensus
    # We want to see how many unique models picked the same stock in the same direction
    # Filter for the latest version per model
    df_latest = df.sort_values('version', ascending=False).groupby(['model_name', 'ts_code', 'direction']).first().reset_index()
    
    consensus = df_latest.groupby(['ts_code', 'stock_name', 'direction']).agg({
        'model_name': lambda x: list(x.unique()),
        'methodology': lambda x: list(x.unique())
    }).reset_index()
    
    consensus['model_count'] = consensus['model_name'].apply(len)
    
    # Filter for >= 2 models
    high_consensus = consensus[consensus['model_count'] >= 2].sort_values('model_count', ascending=False)
    
    if not high_consensus.empty:
        print(f"Found {len(high_consensus)} stocks with consensus (>= 2 models):")
        print(high_consensus[['ts_code', 'stock_name', 'direction', 'model_count', 'model_name']].to_markdown(index=False))
    else:
        print("No consensus stocks found (all single model picks).")

    print("-" * 40)

    # 4. Industry Analysis
    print("\n### 3. Top Industries by Direction")
    
    for direction in ['long', 'short']:
        print(f"\nDirection: {direction.upper()}")
        d_df = df[df['direction'] == direction]
        if d_df.empty:
            print("No picks.")
            continue
            
        ind_counts = d_df['industry'].value_counts().head(5)
        print(ind_counts.to_markdown())

    print("-" * 40)

    # 5. Strategy Breakdown
    print("\n### 4. Strategy Breakdown")
    strat_counts = df.groupby(['methodology', 'direction']).size().unstack(fill_value=0)
    print(strat_counts.to_markdown())

    print("-" * 40)

    # 6. Conflict Analysis (Long vs Short on same stock)
    print("\n### 5. Conflicting Signals")
    # Check if any stock appears in both Long and Short
    stock_dirs = df.groupby('ts_code')['direction'].unique()
    conflicts = stock_dirs[stock_dirs.apply(len) > 1]
    
    if not conflicts.empty:
        print(f"Found {len(conflicts)} stocks with conflicting signals:")
        for code in conflicts.index:
            stock_name = df[df['ts_code'] == code]['stock_name'].iloc[0]
            print(f"- {code} {stock_name}")
            # Show details
            details = df[df['ts_code'] == code][['model_name', 'direction', 'methodology']]
            print(details.to_markdown(index=False))
    else:
        print("No conflicting signals found.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        analyze_latest(sys.argv[1])
    else:
        analyze_latest()
