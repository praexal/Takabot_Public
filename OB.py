import pandas as pd
import numpy as np
import json
import time
import os
from datetime import datetime
from tvDatafeed import TvDatafeed, Interval

def calculate_order_blocks(df, length=1, bull_ext_last=3, bear_ext_last=3, mitigation='Wick'):
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    vols = df['volume'].values
    times = df.index

    bull_obs = []
    bear_obs = []
    os_state = 0

    for i in range(length, len(df)):
        # Pine Script: upper = ta.highest(length), lower = ta.lowest(length)
        upper = np.max(highs[i - length + 1 : i + 1])
        lower = np.min(lows[i - length + 1 : i + 1])

        if mitigation == 'Close':
            target_bull = np.min(closes[i - length + 1 : i + 1])
            target_bear = np.max(closes[i - length + 1 : i + 1])
        else:
            target_bull = lower
            target_bear = upper

        # Pine Script: os := high[length] > upper ? 0 : low[length] < lower ? 1 : os[1]
        if highs[i - length] > upper:
            os_state = 0
        elif lows[i - length] < lower:
            os_state = 1

        # Pine Script: phv = ta.pivothigh(volume, length, length)
        # Pivot bar is at index p = i - length
        p = i - length
        is_phv = False
        if p >= length:
            pivot_vol = vols[p]
            left_vols = vols[p - length : p]
            right_vols = vols[p + 1 : p + length + 1]
            if pivot_vol > 0 and pivot_vol > np.max(left_vols) and pivot_vol > np.max(right_vols):
                is_phv = True

        # Add new Order Blocks (at bar p = i - length)
        if is_phv:
            hl2_p = (highs[p] + lows[p]) / 2.0
            ob_time = times[p].strftime('%Y-%m-%d %H:%M')

            if os_state == 1:
                bull_obs.insert(0, {
                    "time": ob_time,
                    "top": round(float(hl2_p), 2),
                    "bottom": round(float(lows[p]), 2)
                })
            elif os_state == 0:
                bear_obs.insert(0, {
                    "time": ob_time,
                    "top": round(float(highs[p]), 2),
                    "bottom": round(float(hl2_p), 2)
                })

        # Remove mitigated Order Blocks
        bull_obs = [ob for ob in bull_obs if not (target_bull < ob['bottom'])]
        bear_obs = [ob for ob in bear_obs if not (target_bear > ob['top'])]

    # Return the most recent N unmitigated OBs (index 0 is newest in Pine Script)
    return bull_obs[:bull_ext_last], bear_obs[:bear_ext_last]

def write_ob_json(bulls, bears):
    data = {
        "bull_order_blocks": bulls,
        "bear_order_blocks": bears,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    temp_file = "OB_temp.json"
    target_file = "OrderBlocks.json"
    
    with open(temp_file, "w") as f:
        json.dump(data, f, indent=4)
        
    os.replace(temp_file, target_file)

if __name__ == "__main__":
    print("Connecting to TradingView feed...")
    tv = TvDatafeed() 
    
    symbol = 'NIFTY'  
    exchange = 'NSE'
    bars_lookback = 1000 
    
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
            
            print("\n" + "="*60)
            print(f" LUXALGO ORDER BLOCK TRACKER (Updated: {datetime.now().strftime('%H:%M:%S')})")
            print("="*60)
            
            active_bulls, active_bears = calculate_order_blocks(
                df, length=1, bull_ext_last=3, bear_ext_last=3, mitigation='Wick'
            )
            
            print(f"  ↳ Active Bullish OBs : {len(active_bulls)}")
            for idx, ob in enumerate(active_bulls):
                print(f"      {idx+1}. [{ob['time']}] Top: {ob['top']} | Bottom: {ob['bottom']}")
                
            print(f"\n  ↳ Active Bearish OBs : {len(active_bears)}")
            for idx, ob in enumerate(active_bears):
                print(f"      {idx+1}. [{ob['time']}] Top: {ob['top']} | Bottom: {ob['bottom']}")
                
            print(f"\n  ↳ WRITING TO OrderBlocks.json")
            write_ob_json(active_bulls, active_bears)

        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL ERROR: {e}")
            
        time.sleep(300)