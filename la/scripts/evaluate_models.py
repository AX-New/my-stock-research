import pandas as pd
import sys
import os
import numpy as np
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.database import engine

def evaluate_models():
    # Load all picks with returns
    # We only care about T1 and T2 for now since T3+ might not be available for all
    sql = """
    SELECT * FROM la_pick 
    WHERE return_t1 IS NOT NULL
    """
    df = pd.read_sql(sql, engine)
    
    if df.empty:
        print("No picks with return data found.")
        return

    # Calculate Adjusted Return (Long = +Return, Short = -Return)
    # Note: return_tX is percentage (e.g. 1.5 for 1.5%)
    for t in [1, 2]:
        col = f'return_t{t}'
        if col in df.columns:
            df[f'adj_{col}'] = df.apply(
                lambda row: row[col] if row['direction'] == 'long' else -row[col], 
                axis=1
            )

    # Filter for the latest version per model per date
    # df_latest = df.sort_values('version', ascending=False).groupby(['eval_date', 'model_name', 'ts_code', 'methodology']).first().reset_index()
    # Actually, we should evaluate specific versions if we want to be precise, 
    # but aggregating by model across all versions/dates gives a broader view of "model capability".
    # Let's stick to the latest version per date/model to avoid double counting if re-runs happened.
    df_latest = df.sort_values('version', ascending=False).groupby(['eval_date', 'model_name', 'ts_code', 'methodology']).first().reset_index()

    print(f"Total picks evaluated: {len(df_latest)}")
    
    # Metrics per Model
    print("\n### Model Performance (T+1 & T+2)")
    
    metrics = []
    models = df_latest['model_name'].unique()
    
    for model in models:
        m_df = df_latest[df_latest['model_name'] == model]
        
        row = {'Model': model, 'Count': len(m_df)}
        
        for t in [1, 2]:
            col = f'adj_return_t{t}'
            if col in m_df.columns:
                # Mean Return
                mean_ret = m_df[col].mean()
                # Win Rate (Percent of picks with > 0 adjusted return)
                # Note: 0 return counts as loss here, or maybe push? Let's say > 0.
                win_rate = (m_df[col] > 0).mean() * 100
                
                row[f'T+{t} Avg Return'] = f"{mean_ret:.2f}%"
                row[f'T+{t} Win Rate'] = f"{win_rate:.1f}%"
        
        metrics.append(row)
        
    metrics_df = pd.DataFrame(metrics)
    print(metrics_df.to_markdown(index=False))
    
    # Breakdown by Direction (Long vs Short)
    print("\n### Performance by Direction")
    dir_metrics = []
    for model in models:
        m_df = df_latest[df_latest['model_name'] == model]
        for direction in ['long', 'short']:
            d_df = m_df[m_df['direction'] == direction]
            if d_df.empty: continue
            
            row = {'Model': model, 'Direction': direction, 'Count': len(d_df)}
            for t in [1, 2]:
                col = f'adj_return_t{t}'
                if col in d_df.columns:
                    mean_ret = d_df[col].mean()
                    win_rate = (d_df[col] > 0).mean() * 100
                    row[f'T+{t} Avg'] = f"{mean_ret:.2f}%"
                    row[f'T+{t} Win'] = f"{win_rate:.1f}%"
            dir_metrics.append(row)
            
    print(pd.DataFrame(dir_metrics).to_markdown(index=False))

    # Best Strategy per Model
    print("\n### Best Strategy per Model (T+2 Avg Return)")
    for model in models:
        m_df = df_latest[df_latest['model_name'] == model]
        if 'adj_return_t2' not in m_df.columns: continue
        
        strat_perf = m_df.groupby('methodology')['adj_return_t2'].agg(['mean', 'count'])
        strat_perf = strat_perf[strat_perf['count'] >= 5] # Min 5 picks to be relevant
        best_strat = strat_perf.sort_values('mean', ascending=False).head(3)
        
        print(f"\nModel: {model}")
        print(best_strat)

if __name__ == "__main__":
    evaluate_models()
