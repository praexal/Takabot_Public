import zmq
import json
import csv
import os
import time
import re
from datetime import datetime

CSV_FILE = "trade_logs/ml_raw_data.csv"
LOG_INTERVAL = 60  # Log once every 60 seconds
TARGET_PREMIUM = 100.0
MIN_PREMIUM = 50.0

context = zmq.Context()
sub = context.socket(zmq.SUB)
sub.connect("tcp://127.0.0.1:5555")
sub.setsockopt(zmq.SUBSCRIBE, b"SNAPSHOT")
sub.setsockopt(zmq.SUBSCRIBE, b"SPOT_SNAPSHOT")

market_state = {}
spot_state = {}
last_log_time = 0

def load_json_safe(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def get_active_pair():
    ce, pe = [], []
    for tk, s in market_state.items():
        if s.get("status") != "LIVE" or not s.get("ltp"):
            continue
        if float(s["ltp"]) < MIN_PREMIUM:
            continue
        m = re.search(r'([CP])\d+$', s.get("symbol", ""))
        if not m:
            continue
        diff = abs(float(s["ltp"]) - TARGET_PREMIUM)
        (ce if m.group(1) == 'C' else pe).append((tk, diff))
    c_tk = min(ce, key=lambda x: x[1])[0] if ce else None
    p_tk = min(pe, key=lambda x: x[1])[0] if pe else None
    return c_tk, p_tk

def init_csv():
    os.makedirs("trade_logs", exist_ok=True)
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, "w", newline="") as f:
            csv.writer(f).writerow([
                "timestamp", "minute_of_day", "token", "symbol", "opt_type", "ltp",
                "prev_rsi", "live_rsi", "opp_live_rsi", "ema_spread", "atr",
                "dist_vwap", "volume", "vol_war_ratio", "spot_ltp", "spot_rsi5",
                "spot_ema_spread", "dist_poc", "in_ob"
            ])

def main():
    global last_log_time
    init_csv()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Data Logger started. Writing to {CSV_FILE}...")

    while True:
        try:
            topic, payload = sub.recv_multipart()
            msg = json.loads(payload.decode("utf-8"))
            if topic == b"SNAPSHOT":
                market_state[msg["token"]] = msg
            elif topic == b"SPOT_SNAPSHOT":
                spot_state.update(msg)

            now_ts = time.time()
            if now_ts - last_log_time >= LOG_INTERVAL and spot_state:
                c_tk, p_tk = get_active_pair()
                if not c_tk or not p_tk:
                    continue

                pocs = load_json_safe("POC.json").get("pocs", [])
                obs = load_json_safe("OrderBlocks.json")
                spot_ltp = float(spot_state.get("ltp", 0.0))

                dist_poc = min([abs(spot_ltp - p) for p in pocs], default=999.0)
                all_obs = obs.get("bull_order_blocks", []) + obs.get("bear_order_blocks", [])
                in_ob = int(any(float(z["bottom"]) <= spot_ltp <= float(z["top"]) for z in all_obs))

                dt = datetime.now()
                min_of_day = dt.hour * 60 + dt.minute

                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.writer(f)
                    for tk, opp_tk, otype in [(c_tk, p_tk, "CE"), (p_tk, c_tk, "PE")]:
                        s, opp = market_state[tk], market_state[opp_tk]
                        o_vol = max(float(opp.get("volume", 1.0)), 1.0)
                        writer.writerow([
                            dt.strftime("%Y-%m-%d %H:%M:%S"), min_of_day, tk, s["symbol"], otype,
                            s["ltp"], s["prev_rsi"], s["live_rsi"], opp["live_rsi"],
                            round(float(s["ema9"]) - float(s["ema20"]), 2), s["atr"],
                            round(float(s["ltp"]) - float(s["vwap"]), 2), s["volume"],
                            round(float(s["volume"]) / o_vol, 2), spot_ltp,
                            spot_state.get("live_rsi5", 0),
                            round(float(spot_state.get("ema9", 0)) - float(spot_state.get("ema20", 0)), 2),
                            round(dist_poc, 2), in_ob
                        ])
                last_log_time = now_ts
        except KeyboardInterrupt:
            print("Data Logger stopped.")
            break
        except Exception as e:
            print(f"Logger error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()