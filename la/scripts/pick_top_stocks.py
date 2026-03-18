import pandas as pd
from sqlalchemy import create_engine, text
import sys
import os
from batch_analyze_stocks import get_ts_code, calculate_price_range

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from app.config import Config
except ImportError:
    print("Could not import app.config. Ensure you are running this from the project root or script directory.")
    sys.exit(1)

def pick_top_stocks_per_strategy():
    # 1. Define Yuque Stocks (Copied from batch_analyze_stocks.py)
    yuque_strategies = {
        "Value": [
            ("中远海控", "601919"), ("成都银行", "601838"), ("申能股份", "600642"), ("中远海特", "600428"), ("皖能电力", "000543"),
            ("招商银行", "600036"), ("重庆百货", "600729"), ("华帝股份", "002035"), ("凤凰传媒", "601928"), ("常熟银行", "601128"),
            ("江苏银行", "600919"), ("苏农银行", "603323"), ("沪农商行", "601825"), ("赣粤高速", "600269"), ("南京银行", "601009")
        ],
        "Growth": [
            ("通化东宝", "600867"), ("同洲电子", "002052"), ("中邮科技", "688648"), ("万辰集团", "300972"), ("仕佳光子", "688313"),
            ("生益电子", "688183"), ("华钰矿业", "601020"), ("光线传媒", "300251"), ("胜宏科技", "300476"), ("寒武纪", "688256"),
            ("新易盛", "300502"), ("芭田股份", "002170"), ("星辉娱乐", "300043"), ("三美股份", "603379"), ("思特威", "688213"),
            ("长川科技", "300604"), ("世纪华通", "002602"), ("科沃斯", "603486"), ("瑞芯微", "603893"), ("兴齐眼药", "300573")
        ],
        "GARP": [
            ("新强联", "300850"), ("赛微电子", "300456"), ("朗姿股份", "002612"), ("鑫磊股份", "301317"),
            ("新联电子", "002546"), ("利民股份", "002734"), ("南方精工", "002553"), ("惠而浦", "600983"), ("傲农生物", "603363"),
            ("建投能源", "000600"), ("柘中股份", "002346"), ("回盛生物", "300871"), ("达仁堂", "600329"), ("冰川网络", "300533"),
            ("双象股份", "002395")
        ],
        "Momentum": [
            ("华工科技", "000988"), ("招商轮船", "601872"), ("杰瑞股份", "002353"), ("广汇能源", "600256"), ("宝丰能源", "600989"),
            ("特锐德", "300001"), ("厦门钨业", "600549"), ("特变电工", "600089"), ("新和成", "002001"), ("思源电气", "002028"),
            ("包钢股份", "600010"), ("电投能源", "002128"), ("中远海能", "600026"), ("大族激光", "002008"), ("苏州天脉", "301626"),
            ("盛弘股份", "300693"), ("固德威", "688390"), ("中煤能源", "601898"), ("欧陆通", "300870"), ("科泰电源", "300153")
        ],
        "Reversal": [
            ("农业银行", "601288"), ("阳光电源", "300274"), ("传音控股", "688036")
        ]
    }

    db_url = Config.SQLALCHEMY_DATABASE_URI
    engine = create_engine(db_url)
    
    print("# Top Picks Per Strategy (Based on Volatility & Smart Money)\n")
    print("> Selection Logic: Low Volatility (Safety) for Value/Reversal; Momentum/Flow for others.\n")
    
    with engine.connect() as conn:
        # Get 5-day money flow for all stocks to rank them
        # Note: Optimization - fetch all at once
        mf_query = text("""
            SELECT ts_code, SUM(net_mf_amount) as net_inflow
            FROM moneyflow
            WHERE trade_date >= (SELECT MIN(trade_date) FROM (SELECT DISTINCT trade_date FROM market_daily ORDER BY trade_date DESC LIMIT 5) t)
            GROUP BY ts_code
        """)
        mf_df = pd.read_sql(mf_query, conn)
        mf_map = mf_df.set_index('ts_code')['net_inflow'].to_dict()

        for strategy, stocks in yuque_strategies.items():
            print(f"## {strategy} Picks")
            print("| Stock | Name | Price | Volatility | Net Inflow (5D) | Suggestion |")
            print("|---|---|---|---|---|---|")
            
            candidates = []
            for name, code in stocks:
                ts_code = get_ts_code(code)
                # Get volatility & price range
                metrics = calculate_price_range(ts_code)
                if not metrics:
                    continue
                
                # Get net inflow
                inflow = mf_map.get(ts_code, 0)
                metrics['net_inflow'] = inflow
                candidates.append(metrics)
            
            # Ranking Logic per Strategy
            if strategy in ["Value", "Reversal"]:
                # Rank by: 1. Net Inflow (Safety) 2. Low Volatility
                candidates.sort(key=lambda x: x['net_inflow'], reverse=True)
                top_picks = candidates[:3] # Pick top 3 by inflow
            elif strategy in ["Growth", "GARP", "Momentum"]:
                # Rank by: 1. Net Inflow (Momentum)
                candidates.sort(key=lambda x: x['net_inflow'], reverse=True)
                top_picks = candidates[:3]
            
            for p in top_picks:
                inflow_str = f"{p['net_inflow']/10000:.2f} Yi" if abs(p['net_inflow']) > 100000000 else f"{p['net_inflow']:.2f} Wan"
                buy_target = p['pred_low'] * 1.01
                sell_target = p['pred_high'] * 0.99
                print(f"| {p['ts_code']} | **{p['name']}** | {p['current_price']:.2f} | {p['volatility']:.2f}% | {inflow_str} | Buy @ {buy_target:.2f} | Sell @ {sell_target:.2f} |")
            print("\n")

if __name__ == "__main__":
    pick_top_stocks_per_strategy()
