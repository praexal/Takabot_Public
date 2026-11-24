import os
import csv
import time
import math
import json
import zmq
import threading
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
LOT_SIZE = 65
MIN_PREMIUM = 50
DEFAULT_TICK = 0.05

LOG_FOLDER = "trade_logs"
TRADE_CSV = os.path.join(LOG_FOLDER, "live_trade_journal.csv")

ENTRY_START = "09:30"
ENTRY_END   = "19:59"
FORCE_EXIT  = "20:20"

# ZMQ Config
MASTER_IP = "127.0.0.1"
MASTER_PUB_PORT = 5555
MASTER_REP_PORT = 5556
REQ_TIMEOUT_MS = 5000

# Strategy Parameters
ERROR_MARGIN_PCT = 0.003  
MOM_MIN_ATR, MOM_MIN_VWAP_DIST = 2.5, -100      
REC_MIN_ATR, REC_MIN_VWAP_DIST, REC_MAX_VWAP_DIST = 1.8, -205.0, 100   

# ============================================================
# GLOBALS & STATE
# ============================================================
symbols = []
market_state = {}
trades = {} # STRICT RULE: Only 1 item allowed at a time.
last_traded_candle = {} # FIX: Tracks the specific candle ID

# Prompt & Mute State
muted_signals = set()
active_prompt = None
prompt_time = 0.0
manual_cancel = False

# Threading & ZMQ
lock = threading.Lock()
zmq_context = zmq.Context()
req_socket = None
req_lock = threading.Lock()

# ============================================================
# UTILITIES
# ============================================================
def hm(): return datetime.now().strftime("%H:%M")
def tstr(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def log(msg): print(f"[{tstr()}] {msg}")
def time_in_range(a, b): return a <= hm() <= b
def round2(x): return round(float(x), 2)
def get_tick(sym): return float(sym.get("ti", DEFAULT_TICK))
def floor_to_tick(p, t): return round(math.floor(p / t) * t, 2)
def round_to_tick(p, t): return round(round(p / t) * t, 2)
def calculate_M(entry, qty): return ((3 * entry * qty) + 9375) / (311 * qty)

def init_csv():
    os.makedirs(LOG_FOLDER, exist_ok=True)
    if not os.path.exists(TRADE_CSV):
        with open(TRADE_CSV, "w", newline="") as f:
            csv.writer(f).writerow([
                "date", "symbol", "buy_orderno", "sl_orderno", "lots", "qty",
                "entry_time", "entry_price", "exit_time", "exit_price", "gross_pnl", "result"
            ])

# ============================================================
# ZMQ COMMUNICATION
# ============================================================
def setup_req_socket():
    global req_socket
    if req_socket: req_socket.close(linger=0)
    req_socket = zmq_context.socket(zmq.REQ)
    req_socket.connect(f"tcp://{MASTER_IP}:{MASTER_REP_PORT}")

def send_master_request(payload, timeout=REQ_TIMEOUT_MS):
    with req_lock:
        try:
            req_socket.send_json(payload)
            if req_socket.poll(timeout): return req_socket.recv_json()
            log(f"⚠️ ZMQ REQ TIMEOUT. Reconnecting...")
            setup_req_socket()
            return {"status": "error"}
        except Exception as e:
            setup_req_socket()
            return {"status": "error"}

def zmq_subscriber_thread():
    """Background thread specifically for maintaining real-time data."""
    sub_socket = zmq_context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{MASTER_IP}:{MASTER_PUB_PORT}")
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"SNAPSHOT")
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"ORDER_UPDATE")
    
    while True:
        try:
            topic, payload_bytes = sub_socket.recv_multipart()
            msg = json.loads(payload_bytes.decode('utf-8'))
            
            if topic == b"SNAPSHOT":
                tk = msg.get("token")
                if tk:
                    with lock: market_state[tk] = msg
            elif topic == b"ORDER_UPDATE":
                process_order_update(msg)
        except Exception as e: 
            print(f"📡 RADIO ERROR: {e}")
            time.sleep(0.5)

# ============================================================
# ORDER MANAGEMENT
# ============================================================
def process_order_update(msg):
    orderno, status = msg.get("norenordno", ""), str(msg.get("status", "")).upper()
    with lock:
        for tk, tr in list(trades.items()):
            if orderno == tr["buy_orderno"]:
                if status == "COMPLETE" and tr["state"] == "ORDER_PLACED":
                    tr["state"] = "LIVE_POSITION"
                    tr["entry_price"] = float(msg.get("avgprc", tr["entry_price"]))
                    tr["highest_ltp"] = tr["entry_price"]
                    tick = get_tick(tr["symbol_obj"])
                    tr["initial_trigger"] = floor_to_tick(tr["entry_price"] - 8, tick)
                    tr["current_trigger"] = tr["initial_trigger"]
                    place_exchange_sl(tk, tr)
                elif status in ["REJECTED", "CANCELLED"]:
                    log(f"⚠️ Buy order REJECTED. Wiping trade memory.")
                    del trades[tk] 
            
            elif orderno == tr["sl_orderno"]:
                if status == "COMPLETE" and tr["state"] == "LIVE_POSITION":
                    log(f"🛑 STOPLOSS HIT EXCHANGED FILL @ {msg.get('avgprc', 'UKN')}")
                    last_traded_candle[tk] = tr["candle_ft"] # FIX: Mark the specific candle
                    del trades[tk]

def place_exchange_buy(token, tr):
    with lock: current_px = market_state.get(token, {}).get("ltp", 0.0)
    limit_price = round_to_tick(current_px, get_tick(tr["symbol_obj"]))
    
    log(f"📡 ROUTING BUY: {tr['symbol']} @ LMT: {limit_price}")
    resp = send_master_request({
        "cmd": "PLACE_ORDER", "side": "BUY", "exchange": tr["symbol_obj"]["exch"],
        "symbol": tr["symbol"], "token": token, "qty": tr["qty"], "price_type": "LMT", "price": limit_price
    })
    
    if resp and resp.get("status") == "ok":
        with lock:
            tr["buy_orderno"] = resp.get("orderno")
            tr["state"] = "ORDER_PLACED"
    else:
        log(f"❌ Master rejected buy order.")
        with lock: del trades[token]

def place_exchange_sl(token, tr):
    tick = get_tick(tr["symbol_obj"])
    trigger = tr["initial_trigger"]
    price = floor_to_tick(trigger - tick, tick)
    
    resp = send_master_request({
        "cmd": "PLACE_ORDER", "side": "SELL", "exchange": tr["symbol_obj"]["exch"],
        "symbol": tr["symbol"], "token": token, "qty": tr["qty"], "price_type": "SL-LMT",
        "price": price, "trigger_price": trigger
    })
    if resp and resp.get("status") == "ok": tr["sl_orderno"] = resp.get("orderno")

def modify_exchange_sl(tr, trigger, price):
    tick = get_tick(tr["symbol_obj"])
    trigger, price = floor_to_tick(trigger, tick), floor_to_tick(price, tick)
    resp = send_master_request({
        "cmd": "MODIFY_ORDER", "orderno": tr["sl_orderno"], "exchange": tr["symbol_obj"]["exch"],
        "symbol": tr["symbol"], "qty": tr["qty"], "new_trigger": trigger, "new_price": price
    })
    if resp and resp.get("status") == "ok":
        tr["current_trigger"] = trigger
        log(f"🔁 Stop Trailed Up: Trigger {trigger}")

# ============================================================
# STRATEGY & ASYNC LISTENER
# ============================================================
def scan_for_signals():
    with lock:
        if len(trades) > 0: return None
        state_copy = dict(market_state)
        
    for tk, snap in state_copy.items():
        if not snap or snap.get("status") != "LIVE": continue
        if "previous_candle" not in snap: continue

        prev_candle_id = int(snap["previous_candle"]["start_ft"])
        if last_traded_candle.get(tk) == prev_candle_id: continue # FIX: Check specific candle

        prev_rsi, live_rsi = snap.get("prev_rsi", 0.0), snap.get("live_rsi", 0.0)

        price, cur_vwap, cur_atr = snap.get("ltp", 0.0), snap.get("vwap", 0.0), snap.get("atr", 0.0)
        raw_vwap_dist = price - cur_vwap
        
        sym_obj = next((s for s in symbols if s["token"] == tk), None)
        if not sym_obj: continue

        # Setup 1: MOMENTUM
        if prev_rsi < 60 and (55.0 <= live_rsi < 60.0):
            if f"{tk}_{prev_candle_id}_MOMENTUM_60" not in muted_signals:
                stats = f"VWAP Dist: {round2(raw_vwap_dist)} | ATR: {round2(cur_atr)}"
                warns = []
                
                if raw_vwap_dist < (MOM_MIN_VWAP_DIST * (1 - ERROR_MARGIN_PCT)): warns.append("⚠️ Low VWAP Dist")
                if cur_atr < (MOM_MIN_ATR * (1 - ERROR_MARGIN_PCT)): warns.append("⚠️ Low ATR")
                
                status = " | ".join(warns) if warns else "✅ Ideal"
                
                return {"token": tk, "symbol": sym_obj, "setup": "MOMENTUM_60", "price": price, "candle_ft": prev_candle_id, "recs": f"{stats} [{status}]"}

        # Setup 2: RECOVERY
        elif prev_rsi < 35 and (35.0 < live_rsi < 40.0):
            if f"{tk}_{prev_candle_id}_RECOVERY_40" not in muted_signals:
                stats = f"VWAP Dist: {round2(raw_vwap_dist)} | ATR: {round2(cur_atr)}"
                warns = []
                
                if not (REC_MIN_VWAP_DIST * (1 + ERROR_MARGIN_PCT) <= raw_vwap_dist <= REC_MAX_VWAP_DIST * (1 - ERROR_MARGIN_PCT)): warns.append("⚠️ Suboptimal VWAP Dist")
                if cur_atr < (REC_MIN_ATR * (1 - ERROR_MARGIN_PCT)): warns.append("⚠️ Low ATR")
                
                status = " | ".join(warns) if warns else "✅ Ideal"
                
                return {"token": tk, "symbol": sym_obj, "setup": "RECOVERY_40", "price": price, "candle_ft": prev_candle_id, "recs": f"{stats} [{status}]"}
    return None

def keyboard_listener():
    global manual_cancel, active_prompt
    while True:
        try:
            user_in = input().strip().lower()
            if user_in == 'c':
                manual_cancel = True
            elif active_prompt and user_in == 'y':
                with lock:
                    trades[active_prompt['token']] = {
                        "state": "PENDING_BREAKOUT", "symbol": active_prompt['symbol']["tsym"], 
                        "qty": LOT_SIZE, "symbol_obj": active_prompt['symbol'], "setup": active_prompt["setup"], 
                        "candle_ft": active_prompt["candle_ft"], "entry_price": 0.0, "highest_ltp": 0.0,
                        "auth_time": time.time() # TTL Anchor
                    }
                print()
                log("✅ Authorized. Awaiting Breakout Confirmation (15m Limit)...")
                active_prompt = None
            elif active_prompt and user_in == 'n':
                print()
                log("❌ Rejected. Resuming scan...")
                muted_signals.add(f"{active_prompt['token']}_{active_prompt['candle_ft']}_{active_prompt['setup']}")
                active_prompt = None
        except: time.sleep(0.1)

# ============================================================
# MAIN ENGINE
# ============================================================
def main_engine_loop():
    while True:
        if hm() >= FORCE_EXIT: os._exit(0)
        if not time_in_range(ENTRY_START, ENTRY_END): 
            time.sleep(1); continue

        # 1. Manage Active Trade
        active_tk = None
        with lock:
            if len(trades) > 0: active_tk = list(trades.keys())[0]
            
        if active_tk:
            with lock:
                tr = trades[active_tk]
                snap = market_state.get(active_tk, {})
            
            # --- TRAILING LOGIC ---
            if tr["state"] == "LIVE_POSITION" and "ltp" in snap:
                ltp = snap["ltp"]
                tr["highest_ltp"] = max(tr["highest_ltp"], ltp)
                m_val = calculate_M(tr["entry_price"], tr["qty"])

                if not tr.get("sl_locked", False):
                    if ltp >= tr["entry_price"] + m_val + 0.10:
                        modify_exchange_sl(tr, tr["entry_price"] + m_val + 0.05, tr["entry_price"] + m_val)
                        tr["sl_locked"] = True
                        log(f"🔒 Profit Lock Secured.")
                else:
                    new_trigger = tr["highest_ltp"] - 1.0
                    if new_trigger > tr["current_trigger"] + 0.10:
                        modify_exchange_sl(tr, new_trigger, new_trigger - 0.5)
            
            # --- BREAKOUT LOGIC & TTL DECAY ---
            elif tr["state"] == "PENDING_BREAKOUT":
                global manual_cancel
                
                # Manual Abort
                if manual_cancel:
                    log(f"🚫 MANUAL CANCEL: Aborting breakout watch for {tr['symbol']}.")
                    with lock: del trades[active_tk]
                    manual_cancel = False
                    continue 

                # 15-Minute TTL Decay (3 Candles at 5m)
                if time.time() - tr.get("auth_time", time.time()) > 900:
                    log(f"⏳ TTL EXPIRED: Breakout failed to trigger within 15 mins. Aborting.")
                    with lock: del trades[active_tk]
                    continue

                # Execution Trigger
                live_rsi = snap.get("live_rsi", 0.0)
                if (tr["setup"] == "MOMENTUM_60" and live_rsi >= 60.0) or (tr["setup"] == "RECOVERY_40" and live_rsi >= 40.0):
                    with lock: tr["state"] = "ORDER_PLACED"
                    muted_signals.add(f"{active_tk}_{tr['candle_ft']}_{tr['setup']}")
                    place_exchange_buy(active_tk, tr)

        # 2. Look for new signals / Manage Prompts
        else:
            global active_prompt, prompt_time
            
            # 60-second stopwatch for prompt (NO MUTING)
            if active_prompt:
                if time.time() - prompt_time > 60.0:
                    print("\n⏳ 60s Timer Expired. Prompt cleared (not muted). Resuming scan...")
                    # The bot forgets the prompt, but the setup is NOT added to muted_signals.
                    # If the signal is still valid, it will re-trigger immediately on the next scan.
                    active_prompt = None
            
            # Scan the market
            else:
                sig = scan_for_signals()
                
                # Visual Heartbeat (Runs when flat and not prompting)
                if int(time.time()) % 10 == 0: 
                    with lock: print(f"Heartbeat: Watching {len(market_state)} symbols...", end="\r")
                
                if sig:
                    # \a triggers the system notification sound/beep
                    print(f"\a\n🔔 SIGNAL DETECTED: {sig['symbol']['tsym']} | Setup: {sig['setup']} | LTP: {sig['price']}")
                    print(f"💡 Filters: {sig['recs']}")
                    print("Authorize trade? (y/n/c) [60s Timeout]: ", end="", flush=True)
                    active_prompt = sig
                    prompt_time = time.time()
        
        time.sleep(0.05)
        
        

# ============================================================
# BOOT
# ============================================================
if __name__ == "__main__":
    init_csv()
    setup_req_socket()
    
    log("Syncing Initial State from Master...")
    resp = send_master_request({"cmd": "GET_SYMBOLS"})

    if resp and "symbols" in resp:
        symbols = resp["symbols"]
        for s in symbols: last_traded_candle[s["token"]] = None # FIX: Initialize all to None
        
    print(f"DEBUG - Master Response: {resp}") 
    print(f"DEBUG - Symbols loaded: {len(symbols)}") 
    
    threading.Thread(target=zmq_subscriber_thread, daemon=True).start()
    threading.Thread(target=keyboard_listener, daemon=True).start()

    log("System Online. Scanning...")
    main_engine_loop()