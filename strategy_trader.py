import os
import csv
import time
import json
import threading
import zmq
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

BOT_NAME = "MOMENTUM_RSI_BOT"

LOG_FOLDER = f"{BOT_NAME}_logs"
STATE_FOLDER = f"{BOT_NAME}_state"
TRADE_CSV = os.path.join(LOG_FOLDER, f"{BOT_NAME}_paper_trades.csv")
REJECT_CSV = os.path.join(LOG_FOLDER, f"{BOT_NAME}_rejections.csv")
SPOT_CSV = os.path.join(LOG_FOLDER, f"{BOT_NAME}_nifty_spot_entry.csv")

ZMQ_SUB_URL = "tcp://localhost:5555"

LOT_SIZE = 65
MIN_PREMIUM = 50

ENTRY_START = "09:30"
ENTRY_END   = "19:59"
FORCE_EXIT  = "20:20"

DASHBOARD_DELAY = 1.0

# ============================================================
# GLOBALS & STATE
# ============================================================

market_state = {}      # token -> latest snapshot
spot_state = {}        # Nifty spot latest snapshot

trades = {}            # Active paper trades
trade_done_this_candle = {} 
last_candle_ft = {}    # Tracks the start_ft of the current candle to detect resets
last_signal_prev_candle = {}

status_map = {}
rsi_state = {}

lock = threading.Lock()

# ============================================================
# HELPERS
# ============================================================

def now():
    return datetime.now()

def tstr():
    return now().strftime("%Y-%m-%d %H:%M:%S")

def hm():
    return now().strftime("%H:%M")

def round2(x):
    return round(float(x), 2) if x is not None else 0.0

def time_in_range(a, b):
    x = hm()
    return a <= x <= b

def log(msg):
    print(f"[{tstr()}] {msg}")

def calculate_M(entry, qty):
    return ((3 * entry * qty) + 9375) / (311 * qty)

def set_status(token, msg):
    with lock:
        status_map[token] = msg

# ============================================================
# CSV INIT
# ============================================================

def init_csv():
    os.makedirs(LOG_FOLDER, exist_ok=True)
    os.makedirs(STATE_FOLDER, exist_ok=True)

    trade_need_header = not os.path.exists(TRADE_CSV) or os.path.getsize(TRADE_CSV) == 0
    if trade_need_header:
        with open(TRADE_CSV, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "date","entry_time","exit_time","symbol","token","qty",
                "entry","exit","prev_rsi","live_rsi","vwap","volume",
                "ema9","ema20","atr","dist_vwap","exit_reason",
                "gross_pnl","net_pnl","highest","m_value","duration_sec",
                "orig_exit","orig_reason","orig_gross","orig_net","orig_dur",
                "loose_exit","loose_reason","loose_gross","loose_net","loose_dur",
                "tight_exit","tight_reason","tight_gross","tight_net","tight_dur",
                "lcl_exit","lcl_reason","lcl_gross","lcl_net","lcl_dur"
            ])

    reject_need_header = not os.path.exists(REJECT_CSV) or os.path.getsize(REJECT_CSV) == 0
    if reject_need_header:
        with open(REJECT_CSV, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["time","symbol","token","reason","ltp","prev_rsi","live_rsi"])

    spot_need_header = not os.path.exists(SPOT_CSV) or os.path.getsize(SPOT_CSV) == 0
    if spot_need_header:
        with open(SPOT_CSV, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "timestamp", "epoch", "option_symbol", "option_token", 
                "option_entry", "spot_price", "spot_rsi_5", "spot_rsi_14", 
                "spot_ema9", "spot_ema20"
            ])

def log_reject(sym, token, reason, ltp="", prev="", live=""):
    with open(REJECT_CSV, "a", newline="") as f:
        csv.writer(f).writerow([tstr(), sym, token, reason, ltp, prev, live])

def log_spot_on_entry(option_symbol, option_token, option_entry_price):
    with lock:
        snap = spot_state.copy()
        
    if not snap or "ltp" not in snap:
        log(f"WARNING: No Spot data available to record for {option_symbol}")
        return
        
    if snap.get("feed_age", 99) > 10.0:
        log(f"WARNING: Spot feed is stale. Skipping Spot log entry for {option_symbol}")
        return

    try:
        epoch_time = int(time.time())
        formatted_time = tstr()
        
        with open(SPOT_CSV, "a", newline="") as f:
            csv.writer(f).writerow([
                formatted_time, epoch_time, option_symbol, option_token,
                option_entry_price, snap.get("ltp", 0), snap.get("live_rsi_5", 0), 
                snap.get("live_rsi_14", 0), snap.get("ema9", 0), snap.get("ema20", 0)
            ])
    except Exception as e:
        log(f"ERROR logging Spot state on entry: {e}")

# ============================================================
# SIGNAL ENGINE
# ============================================================

def evaluate_signal(snap):
    token = snap["token"]
    symbol = snap["symbol"]
    ltp = snap.get("ltp")
    
    if not snap.get("previous_candle"):
        set_status(token, "WAIT: NO_PREV_CANDLE")
        return None

    prev_rsi = snap.get("prev_rsi", 0)
    live_rsi = snap.get("live_rsi", 0)
    prev_candle_id = snap["previous_candle"].get("start_ft", 0)
    prev_candle_low = snap["previous_candle"].get("low", ltp)

    rsi_state[token] = {"prev": prev_rsi, "live": live_rsi}

    if ltp is None or ltp < MIN_PREMIUM:
        set_status(token, f"BLOCKED: < {MIN_PREMIUM}")
        return None

    if trade_done_this_candle.get(token, False):
        set_status(token, "DONE_THIS_CANDLE")
        return None

    if last_signal_prev_candle.get(token) == prev_candle_id:
        set_status(token, "WAIT NEW CLOSED CANDLE")
        return None

    # --- STRATEGY LOGIC ---
    trigger_entry = False
    setup_name = ""

    if prev_rsi < 60 and 60 <= live_rsi <= 64:
        trigger_entry = True
        setup_name = "MOMENTUM_60"
    elif prev_rsi < 35 and 40 < live_rsi < 44:
        trigger_entry = True
        setup_name = "RECOVERY_40"

    # --- STATUS MAP UPDATE ---
    if not trigger_entry:
        if 44 <= live_rsi < 60:
            set_status(token, "WAIT: RSI MID-ZONE")
        elif live_rsi > 64:
            set_status(token, "BLOCKED: RSI > 64")
        elif live_rsi <= 40:
            set_status(token, "WAIT: RSI LOW")
        else:
            set_status(token, "SCANNING...")
        return None

    set_status(token, f"ENTRY: {setup_name}")
    log(f"SIGNAL DETECTED: {setup_name} on {symbol} | Prev RSI: {prev_rsi}, Live RSI: {live_rsi}")

    return {
        "entry": ltp,
        "prev_rsi": prev_rsi,
        "live_rsi": live_rsi,
        "vwap": snap.get("vwap", 0),
        "volume": snap.get("volume", 0),
        "ema9": snap.get("ema9", 0),
        "ema20": snap.get("ema20", 0),
        "atr": snap.get("atr", 0),
        "dist_vwap": round2(ltp - snap.get("vwap", 0)),
        "prev_candle_low": prev_candle_low,
        "prev_candle_time": prev_candle_id,
        "setup_tag": setup_name,
        "symbol": symbol
    }

# ============================================================
# TRADE ENGINE
# ============================================================

def open_trade(token, sig):
    last_signal_prev_candle[token] = sig["prev_candle_time"]
    trade_done_this_candle[token] = True

    entry = sig["entry"]
    qty = LOT_SIZE
    m = calculate_M(entry, qty)

    def child(sl_dist, trail):
        return {
            "active": True,
            "highest": entry,
            "trigger": round2(entry - sl_dist),
            "locked": False,
            "trail": trail,
            "exit_price": "",
            "exit_reason": "",
            "exit_time": "",
            "gross": "",
            "net": "",
            "duration": ""
        }

    with lock:
        trades[token] = {
            "symbol": sig["symbol"],
            "token": token,
            "entry_time": tstr(),
            "entry": entry,
            "qty": qty,
            "m": round2(m),
            "meta": sig,
            "orig": child(8, 1),
            "loose": child(8, 2),
            "tight": child(5, 1),
            "lcl": {
                "active": True,
                "highest": entry,
                "trigger": sig["prev_candle_low"],  
                "last_completed_ft": sig["prev_candle_time"],  
                "exit_price": "", "exit_reason": "", "exit_time": "",
                "gross": "", "net": "", "duration": ""
            }
        }

    set_status(token, "TRADE ACTIVE (4)")
    log(f"BUY {sig['symbol']} @ {entry} | LCL SL Init: {sig['prev_candle_low']}")
    log_spot_on_entry(sig["symbol"], token, entry)

def finish_leg(tr, leg, reason, px):
    c = tr[leg]
    if not c["active"]: return

    c["active"] = False
    c["exit_price"] = round2(px)
    c["exit_reason"] = reason
    c["exit_time"] = tstr()

    gross = round2((px - tr["entry"]) * tr["qty"])
    charges = 10 + (0.0016 * tr["qty"] * (tr["entry"] + px))
    net = round2(gross - charges)

    c["gross"] = gross
    c["net"] = net

    et = datetime.strptime(c["exit_time"], "%Y-%m-%d %H:%M:%S")
    st = datetime.strptime(tr["entry_time"], "%Y-%m-%d %H:%M:%S")
    c["duration"] = int((et - st).total_seconds())

def close_trade(token):
    with lock:
        tr = trades[token]

    row = [
        now().strftime("%Y-%m-%d"),
        tr["entry_time"],
        max(tr["orig"]["exit_time"], tr["loose"]["exit_time"], tr["tight"]["exit_time"], tr["lcl"]["exit_time"]),
        tr["symbol"], token, tr["qty"], tr["entry"], "",  
        tr["meta"]["prev_rsi"], tr["meta"]["live_rsi"],
        tr["meta"]["vwap"], tr["meta"]["volume"],
        tr["meta"]["ema9"], tr["meta"]["ema20"], tr["meta"]["atr"],
        tr["meta"]["dist_vwap"], "MULTI", "", "", "", tr["m"], "",
        tr["orig"]["exit_price"], tr["orig"]["exit_reason"], tr["orig"]["gross"], tr["orig"]["net"], tr["orig"]["duration"],
        tr["loose"]["exit_price"], tr["loose"]["exit_reason"], tr["loose"]["gross"], tr["loose"]["net"], tr["loose"]["duration"],
        tr["tight"]["exit_price"], tr["tight"]["exit_reason"], tr["tight"]["gross"], tr["tight"]["net"], tr["tight"]["duration"],
        tr["lcl"]["exit_price"], tr["lcl"]["exit_reason"], tr["lcl"]["gross"], tr["lcl"]["net"], tr["lcl"]["duration"],
    ]

    with open(TRADE_CSV, "a", newline="") as f:
        csv.writer(f).writerow(row)

    with lock:
        del trades[token]
    set_status(token, "EXIT COMPLETE")

def manage_trade(snap):
    token = snap["token"]
    with lock:
        if token not in trades: return
        tr = trades[token]
    
    ltp = snap.get("ltp")
    if ltp is None: return

    # Standard Legs
    for leg in ["orig", "loose", "tight"]:
        c = tr[leg]
        if not c["active"]: continue

        if ltp > c["highest"]: c["highest"] = ltp

        if not c["locked"]:
            if ltp >= tr["entry"] + tr["m"] + 0.10:
                c["trigger"] = round2(tr["entry"] + tr["m"] + 0.05)
                c["locked"] = True
        else:
            nt = round2(c["highest"] - c["trail"])
            if nt > c["trigger"]:
                c["trigger"] = nt

        if ltp <= c["trigger"]:
            finish_leg(tr, leg, "SL_HIT", c["trigger"])

    # LCL Leg
    c_lcl = tr["lcl"]
    if c_lcl["active"]:
        if ltp > c_lcl["highest"]: 
            c_lcl["highest"] = ltp

        latest_closed_candle = snap.get("previous_candle")
        if latest_closed_candle:
            latest_ft = latest_closed_candle.get("start_ft", 0)
            if latest_ft > c_lcl["last_completed_ft"]:
                new_low = round2(latest_closed_candle.get("low", 0))
                if new_low > c_lcl["trigger"]:
                    c_lcl["trigger"] = new_low
                    log(f"LCL TRIGGER TRAILED UP -> {new_low} for {tr['symbol']}")
                c_lcl["last_completed_ft"] = latest_ft

        if ltp <= c_lcl["trigger"]:
            finish_leg(tr, "lcl", "LCL_SL_HIT", c_lcl["trigger"])

    remaining = sum(1 for x in ["orig", "loose", "tight", "lcl"] if tr[x]["active"])
    set_status(token, f"TRADE ACTIVE ({remaining})")

    if remaining == 0:
        close_trade(token)

# ============================================================
# ZMQ LISTENER LOOP
# ============================================================

def mq_listener():
    context = zmq.Context()
    sub = context.socket(zmq.SUB)
    sub.connect(ZMQ_SUB_URL)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    
    log(f"ZMQ Listener connected to {ZMQ_SUB_URL}")

    while True:
        try:
            msg = sub.recv_multipart()
            topic = msg[0].decode("utf-8")
            payload = json.loads(msg[1].decode("utf-8"))

            if topic == "SPOT_SNAPSHOT":
                with lock:
                    spot_state.update(payload)
                continue

            if topic == "SNAPSHOT":
                token = payload.get("token")
                if not token: continue
                
                # Check for candle boundary crossing to reset trade limit
                curr_ft = payload.get("current_candle", {}).get("start_ft")
                if curr_ft and curr_ft != last_candle_ft.get(token):
                    trade_done_this_candle[token] = False
                    last_candle_ft[token] = curr_ft

                with lock:
                    market_state[token] = payload

                if token in trades:
                    manage_trade(payload)
                elif time_in_range(ENTRY_START, ENTRY_END):
                    sig = evaluate_signal(payload)
                    if sig:
                        open_trade(token, sig)
                        
        except Exception as e:
            pass # Keep loop robust against malformed JSON/Data

# ============================================================
# DASHBOARD & BACKGROUND PROCESSES
# ============================================================

def dashboard_loop():
    while True:
        print(f"\n==================== {BOT_NAME} DASHBOARD ====================")
        print(f"{'SYMBOL':<18} {'LTP':<9} {'AGE':<8} {'PREV':<7} {'LIVE':<7} {'STATE'}")
        print("-" * 62)

        with lock:
            tokens = list(market_state.keys())

        for tk in tokens:
            with lock:
                snap = market_state.get(tk, {})
            
            name = snap.get("symbol", tk)
            ltp = snap.get("ltp", "-")
            age = snap.get("feed_age", "-")
            if isinstance(age, (int, float)):
                age_str = f"{round(age,1)}s" if age < 10 else "STALE"
            else:
                age_str = "NA"
                
            prev = rsi_state.get(tk, {}).get("prev", "-")
            live = rsi_state.get(tk, {}).get("live", "-")
            st = status_map.get(tk, "WAITING")
            
            if tk in trades:
                st = status_map.get(tk, "TRADE ACTIVE")

            print(f"{name:<18} {str(ltp):<9} {age_str:<8} {str(prev):<7} {str(live):<7} {st}")

        with lock:
            s_snap = spot_state.copy()
            
        s_px = s_snap.get("ltp", "-")
        s_age = s_snap.get("feed_age", "-")
        s_age_str = f"{round(s_age,1)}s" if isinstance(s_age, (int, float)) else "NA"
        
        print("-" * 62)
        print(f"{'NIFTY SPOT 50':<18} {str(s_px):<9} {s_age_str:<8} {'-':<7} {'-':<7} MASTER DATA")
        print("=" * 62)

        time.sleep(DASHBOARD_DELAY)

def fetch_watchlist_from_master():
    log("Requesting watchlist from Master Bot...")
    try:
        context = zmq.Context()
        req = context.socket(zmq.REQ)
        req.connect("tcp://localhost:5556")
        
        req.send_json({"cmd": "GET_SYMBOLS"})
        
        # Wait up to 2 seconds for the master to reply
        if req.poll(2000):
            resp = req.recv_json()
            symbols = resp.get("symbols", [])
            for sym in symbols:
                tk = sym["token"]
                with lock:
                    # Pre-populate state so it shows on the dashboard immediately
                    market_state[tk] = {"symbol": sym["tsym"], "token": tk, "ltp": "WAIT"}
                    status_map[tk] = "AWAITING FEED"
            log(f"Successfully loaded {len(symbols)} symbols from Master.")
        else:
            log("TIMEOUT: Master Bot is not responding on port 5556. Is it running?")
    except Exception as e:
        log(f"Error fetching symbols: {e}")

def exit_loop():
    while True:
        if hm() >= FORCE_EXIT:
            with lock:
                tks = list(trades.keys())
            for tk in tks:
                try: 
                    close_trade(tk)
                except: 
                    pass
            log("DAY END - FORCED EXIT")
            os._exit(0)
        time.sleep(5)

# ============================================================
# MAIN
# ============================================================

def main():
    log(f"STARTING {BOT_NAME}")
    init_csv()

    fetch_watchlist_from_master()

    threading.Thread(target=mq_listener, daemon=True).start()
    threading.Thread(target=dashboard_loop, daemon=True).start()
    threading.Thread(target=exit_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("SHUTTING DOWN...")

if __name__ == "__main__":
    main()