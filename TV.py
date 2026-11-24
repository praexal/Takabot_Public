import pandas as pd
import numpy as np
import json
import time
import os
from datetime import datetime
from tvDatafeed import TvDatafeed, Interval

def calculate_single_profile_poc(df_session, num_bins=100):
    min_price = df_session['low'].min()
    max_price = df_session['high'].max()
    
    if min_price == max_price:
        return min_price

    bins = np.linspace(min_price, max_price, num_bins + 1)
    price_step = (max_price - min_price) / num_bins
    volume_profile = np.zeros(num_bins)
    
    for _, row in df_session.iterrows():
        low = row['low']
        high = row['high']
        vol = row['volume']
        
        if high == low: 
            continue
            
        for i in range(num_bins):
            bin_low = bins[i]
            bin_high = bins[i+1]
            
            overlap_low = max(low, bin_low)
            overlap_high = min(high, bin_high)
            
            if overlap_high > overlap_low:
                proportion = (overlap_high - overlap_low) / (high - low)
                volume_profile[i] += vol * proportion
                
    poc_index = np.argmax(volume_profile)
    return bins[poc_index] + (price_step / 2)

def write_poc_json(poc_list):
    """Writes a LIST of all historical POCs to the JSON file"""
    data = {
        "pocs": [round(float(p), 2) for p in poc_list],
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    temp_file = "POC_temp.json"
    target_file = "POC.json"
    
    with open(temp_file, "w") as f:
        json.dump(data, f, indent=4)
        
    os.replace(temp_file, target_file)

if __name__ == "__main__":
    print("Connecting to TradingView feed...")
    tv = TvDatafeed() 
    
    symbol = 'NIFTY'  
    exchange = 'NSE'
    bars_lookback = 360 # Approx 4.8 days
    
    while True:
        try:
            df = tv.get_hist(
                symbol=symbol, exchange=exchange, 
                interval=Interval.in_5_minute, n_bars=bars_lookback
            )
            
            if df is None or df.empty:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: No data retrieved. Retrying in 10s...")
                time.sleep(10)
                continue

            df.columns = df.columns.str.lower()
            
            print("\n" + "="*50)
            print(f" MULTI-SESSION POC GRID (Updated: {datetime.now().strftime('%H:%M:%S')})")
            print("="*50)
            
            sessions = df.groupby(df.index.date)
            all_dates = list(sessions.groups.keys())
            
            active_pocs = [] # Array to hold all calculated POCs
            
            for date in all_dates:
                df_session = sessions.get_group(date)
                
                if len(df_session) < 3: continue
                    
                session_poc = calculate_single_profile_poc(df_session, num_bins=100)
                active_pocs.append(session_poc)
                
                print(f"Date: {date} | Bars: {len(df_session):3d}")
                print(f"  ↳ SESSION POC   : {session_poc:.2f}")
                print("-" * 50)
            
            # Write the entire grid to JSON
            print(f"  ↳ WRITING {len(active_pocs)} POCs TO POC.json")
            write_poc_json(active_pocs)

        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL ERROR: {e}")
            
        time.sleep(300)