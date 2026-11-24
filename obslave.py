import csv
import os
import time
import threading
import pandas as pd
import json
import tempfile
import sys
import zmq
import re
from datetime import datetime
from collections import deque

# ============================================================
# CONFIG
# ============================================================

LOT_SIZE = 65
MIN_PREMIUM = 50

LOG_FOLDER = "trade_logs"
STATE_FOLDER = "temp_state"
TRADE_CSV = os.path.join(LOG_FOLDER, "ob_trades.csv")
STATE_FILE = os.path.join(STATE_FOLDER, "live_state.json")
POC_FILE = "OrderBlocks.json"

ENTRY_START = "09:30"
ENTRY_END   = "13:59"
FORCE_EXIT  = "14:20"

MIN_BARS = 7
SCAN_COOLDOWN = 0.20
IDLE_SLEEP = 0.02

ENTRY_SCAN_DELAY = 1.0
EXIT_LOOP_DELAY  = 0.05
DASHBOARD_DELAY  = 1.0

SPOT_TOKEN = "26000"

# --- ZMQ CONFIG ---
MASTER_IP = "127.0.0.1"
MASTER_PUB_PORT = 5555
MASTER_REP_PORT = 5556

# ANSI UI Constants
ENTER_ALT_SCREEN = "\033[?1049h"
EXIT_ALT_SCREEN = "\033[?1049l"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"
CURSOR_HOME = "\033[H"
CLEAR_FROM_CURSOR = "\033[J"
CLEAR_SCREEN = "\033[2J"

GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# ============================================================
# GLOBALS
# ============================================================

market_state = {}
market_state_spot = {}
rsi_state = {}

scan_pending = {}
last_scan_ts = {}
last_signal_prev_candle = {}

symbols = []
pair_map = {} 
volume_history = {} 
last_vol_ft = {}

active_ce_token = None
active_pe_token = None
TARGET_PREMIUM = 100.0

status_map = {}
trades = {}
trade_done_this_candle = {}
last_candle = None

lock = threading.Lock()
spot_lock = threading.Lock()

session_ob_zones = []
poc_last_modified = 0

cached_csv_stats = {
    "orig": {"real_net": 0.0, "wins": 0, "losses": 0},
    "loose": {"real_net": 0.0, "wins": 0, "losses": 0},
    "tight": {"real_net": 0.0, "wins": 0, "losses": 0},
    "lcl": {"real_net": 0.0, "wins": 0, "losses": 0},
    "completed": 0
}

terminal_logs = deque(maxlen=8)
ui_active = False

# ZMQ Globals
zmq_context = zmq.Context()
req_socket = None

# ============================================================
# HELPERS
# ============================================================

def init_terminal():
    if os.name == 'nt':
        import ctypes
        h_out = ctypes.windll.kernel32.GetStdHandle(-11)
        if h_out != -1:
            mode = ctypes.c_ulong()
            if ctypes.windll.kernel32.GetConsoleMode(h_out, ctypes.byref(mode)):
                ctypes.windll.kernel32.SetConsoleMode(h_out, mode.value | 0x0004)

def now(): return datetime.now()
def tstr(): return now().strftime("%Y-%m-%d %H:%M:%S")
def hm(): return now().strftime("%H:%M")
def round2(x): return round(float(x), 2)
def time_in_range(a, b): return a <= hm() <= b

def log(msg):
    formatted_msg = f"[{tstr()}] {msg}"
    terminal_logs.append(formatted_msg)
    if not ui_active: print(formatted_msg)

def calculate_M(entry, qty): return ((3 * entry * qty) + 9375) / (311 * qty)
def set_status(token, msg):
    with lock: status_map[token] = msg

def save_live_state():
    temp_path = None
    try:
        live_ltp_mock = {tk: snap["ltp"] for tk, snap in market_state.items() if "ltp" in snap}
        spot_snap = market_state_spot.get(SPOT_TOKEN, {})
        
        state = {
            "live_ltp": live_ltp_mock,
            "rsi_state": rsi_state,
            "status_map": status_map,
            "trades": trades,
            "spot": {
                "spot_live_ltp": spot_snap.get("ltp"),
                "spot_feed_alive_ts": spot_snap.get("feed_time"),
                "session_pocs": session_ob_zones
            }
        }
        temp_fd, temp_path = tempfile.mkstemp(dir=STATE_FOLDER)
        with os.fdopen(temp_fd, 'w') as tmp: json.dump(state, tmp)
        os.replace(temp_path, STATE_FILE)
    except Exception:
        if temp_path and os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass

# ============================================================
# ZMQ NETWORKING CLIENT COMPONENT
# ============================================================

def send_master_request(payload):
    try:
        req_socket.send_json(payload)
        if req_socket.poll(5000):
            return req_socket.recv_json()
        else:
            log(f"⚠️ ZMQ REQ Timeout on command: {payload.get('cmd')}")
            return None
    except Exception as e:
        log(f"⚠️ ZMQ Router Connection Exception: {e}")
        return None

def zmq_subscriber_loop():
    sub_socket = zmq_context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{MASTER_IP}:{MASTER_PUB_PORT}")
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"SNAPSHOT")
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"SPOT_SNAPSHOT")
    
    while True:
        try:
            topic, payload_bytes = sub_socket.recv_multipart()
            msg = json.loads(payload_bytes.decode('utf-8'))
            tk = msg.get("token")
            if not tk: continue

            if topic == b"SNAPSHOT":
                with lock:
                    market_state[tk] = msg
                    scan_pending[tk] = True
                    
                    prev_candle = msg.get("previous_candle", {})
                    if prev_candle and "start_ft" in prev_candle:
                        ft = int(prev_candle["start_ft"])
                        if ft != last_vol_ft.get(tk, 0):
                            if tk not in volume_history:
                                volume_history[tk] = deque(maxlen=10)
                            volume_history[tk].append(msg.get("volume", 0.0))
                            last_vol_ft[tk] = ft

            elif topic == b"SPOT_SNAPSHOT":
                with spot_lock:
                    market_state_spot[tk] = msg
        except Exception as e:
            time.sleep(0.1)

# ============================================================
# METRIC CACHE COMPILER & CSV
# ============================================================

def refresh_historical_csv_cache():
    global cached_csv_stats
    
    def get_empty_schema():
        return {
            "orig": {"real_net": 0.0, "wins": 0, "losses": 0},
            "loose": {"real_net": 0.0, "wins": 0, "losses": 0},
            "tight": {"real_net": 0.0, "wins": 0, "losses": 0},
            "lcl": {"real_net": 0.0, "wins": 0, "losses": 0},
            "completed": 0
        }

    # Create separate caches for today and all-time
    cached_csv_stats = {"today": get_empty_schema(), "all_time": get_empty_schema()}
    
    if os.path.exists(TRADE_CSV) and os.path.getsize(TRADE_CSV) > 0:
        try:
            df = pd.read_csv(TRADE_CSV)
            if not df.empty:
                today_str = now().strftime("%Y-%m-%d")
                
                df_all = df
                df_today = df[df['date'] == today_str] if 'date' in df.columns else pd.DataFrame(columns=df.columns)

                # Process both dataframes
                for scope, current_df in [("all_time", df_all), ("today", df_today)]:
                    cached_csv_stats[scope]["completed"] = len(current_df)
                    for leg in ["orig", "loose", "tight", "lcl"]:
                        net_col = pd.to_numeric(current_df[f'{leg}_net'], errors='coerce').fillna(0) if f'{leg}_net' in current_df.columns else pd.Series([0.0]*len(current_df))
                        cached_csv_stats[scope][leg]["real_net"] = net_col.sum()
                        cached_csv_stats[scope][leg]["wins"] = (net_col > 0).sum()
                        cached_csv_stats[scope][leg]["losses"] = (net_col <= 0).sum()
        except Exception:
            pass

def init_csv():
    os.makedirs(LOG_FOLDER, exist_ok=True)
    os.makedirs(STATE_FOLDER, exist_ok=True)
    
    if not os.path.exists(TRADE_CSV) or os.path.getsize(TRADE_CSV) == 0:
        with open(TRADE_CSV, "w", newline="") as f:
            csv.writer(f).writerow([
                "date","entry_time","exit_time","symbol","token","qty",
                "entry","exit","prev_rsi","live_rsi","opp_live_rsi","vwap","volume",
                "ema9","ema20","atr","dist_vwap","exit_reason",
                "gross_pnl","net_pnl","highest","m_value","duration_sec",
                "orig_exit","orig_reason","orig_gross","orig_net","orig_dur",
                "loose_exit","loose_reason","loose_gross","loose_net","loose_dur",
                "tight_exit","tight_reason","tight_gross","tight_net","tight_dur",
                "lcl_exit","lcl_reason","lcl_gross","lcl_net","lcl_dur",
                "spot_aligned", "vol_surge_met", "vwap_met", "vol_war_met"
            ])
    refresh_historical_csv_cache()

# ============================================================
# STRATEGY LOGIC: POC & DUAL SIGNAL
# ============================================================

def ob_watcher_loop():
    global session_ob_zones, poc_last_modified
    log("Started background OB JSON watcher...")
    while True:
        try:
            if os.path.exists(POC_FILE):
                mtime = os.path.getmtime(POC_FILE)
                if mtime > poc_last_modified:
                    with open(POC_FILE, "r") as f:
                        data = json.load(f)
                        zones = []
                        if "bull_order_blocks" in data:
                            zones.extend(data["bull_order_blocks"])
                        if "bear_order_blocks" in data:
                            zones.extend(data["bear_order_blocks"])
                        
                        session_ob_zones = zones
                        log(f"UPDATED OB GRID: {len(session_ob_zones)} Active Zones")
                    poc_last_modified = mtime
        except: pass
        time.sleep(2)

def evaluate_dual_signal(call_snap, put_snap, spot_snap):
    signals = []
    
    if not session_ob_zones or not spot_snap:
        return signals

    c_spot = float(spot_snap.get("ltp", 0.0))
    o_spot = float(spot_snap.get("current_candle", {}).get("open", 0.0))
    if o_spot == 0.0: return signals
    
    is_valid_location = False
    for zone in session_ob_zones:
        if float(zone['bottom']) <= c_spot <= float(zone['top']):
            is_valid_location = True
            break
            
    if not is_valid_location: return signals

    spot_is_green = c_spot > o_spot

    def check_side(target, opp, is_call):
        px = target.get("ltp", 0.0)
        if px < MIN_PREMIUM: return None
        if target.get("status") != "LIVE": return None
        if "previous_candle" not in target or "start_ft" not in target["previous_candle"]: return None

        vwap = target.get("vwap", 0.0)
        t_vol = target.get("volume", 0.0)
        o_vol = opp.get("volume", 0.0)
        
        t_tk = target.get("token")
        t_vol_hist = list(volume_history.get(t_tk, []))
        t_vol_sma10 = sum(t_vol_hist) / len(t_vol_hist) if len(t_vol_hist) >= MIN_BARS else t_vol
        
        vol_surge_met = t_vol > (1.5 * t_vol_sma10)
        vwap_met = px > vwap
        vol_war_met = t_vol > (1.2 * o_vol)
        
        t_live_rsi = target.get("live_rsi", 0.0)
        t_prev_rsi = target.get("prev_rsi", 0.0)
        
        is_breakout = (t_prev_rsi < 60) and (60 <= t_live_rsi <= 64)
        is_recovery = (t_prev_rsi < 35) and (40 <= t_live_rsi <= 44)
        if not (is_breakout or is_recovery): return None
        
        o_live_rsi = opp.get("live_rsi", 0.0)
        if o_live_rsi <= 30 or o_live_rsi > 60: return None

        spot_aligned = (is_call and spot_is_green) or (not is_call and not spot_is_green)

        return {
            "type": "CALL" if is_call else "PUT",
            "entry": round2(px),
            "prev_rsi": round2(t_prev_rsi),
            "live_rsi": round2(t_live_rsi),
            "opp_live_rsi": round2(o_live_rsi),
            "vwap": round2(vwap),
            "volume": round2(t_vol),
            "ema9": round2(target.get("ema9", 0.0)),
            "ema20": round2(target.get("ema20", 0.0)),
            "atr": round2(target.get("atr", 0.0)),
            "dist_vwap": round2(px - vwap),
            "prev_candle_low": round2(target["previous_candle"].get("low", 0.0)),
            "prev_candle_time": int(target["previous_candle"]["start_ft"]),
            "setup_tag": "BREAKOUT" if is_breakout else "RECOVERY",
            "spot_aligned": spot_aligned,
            "vol_surge_met": vol_surge_met,
            "vwap_met": vwap_met,
            "vol_war_met": vol_war_met
        }

    c_sig = check_side(call_snap, put_snap, is_call=True)
    if c_sig: signals.append(c_sig)

    p_sig = check_side(put_snap, call_snap, is_call=False)
    if p_sig: signals.append(p_sig)

    return signals

# ============================================================
# TRADE MANAGEMENT
# ============================================================

def open_trade(sym, sig):
    token = sym["token"]
    last_signal_prev_candle[token] = sig["prev_candle_time"]
    m = calculate_M(sig["entry"], LOT_SIZE)
    
    t_atr = float(sig.get("atr", 15.0))
    if t_atr == 0: t_atr = 15.0
    
    def child(sl, t): 
        return {"active": True, "highest": sig["entry"], "trigger": round2(sig["entry"] - sl), "locked": False, "trail": t, "exit_price": "", "exit_reason": "", "exit_time": "", "gross": "", "net": "", "duration": ""}
    
    with lock:
        trades[token] = {
            "symbol": sym["tsym"], 
            "token": token, 
            "entry_time": tstr(), 
            "entry": sig["entry"], 
            "qty": LOT_SIZE, 
            "m": round2(m), 
            "meta": sig, 
            "orig": child(8, 1), 
            "loose": {
                "active": True, 
                "highest": sig["entry"], 
                "trigger": round2(sig["entry"] - (t_atr * 0.6)), 
                "locked": False, 
                "trail": round2(t_atr * 0.5),                    
                "activation": round2(sig["entry"] + (t_atr * 0.8)), 
                "min_profit": round2(sig["entry"] + 1.5),        
                "exit_price": "", "exit_reason": "", "exit_time": "", "gross": "", "net": "", "duration": ""
            },
            "tight": child(5, 1), 
            "lcl": {
                "active": True, 
                "highest": sig["entry"], 
                "trigger": sig["prev_candle_low"], 
                "last_completed_ft": sig["prev_candle_time"], 
                "exit_price": "", "exit_reason": "", "exit_time": "", "gross": "", "net": "", "duration": ""
            }
        }
    log(f"SIM BUY {sym['tsym']} @ {sig['entry']} | M: {round2(m)} | Spot Aligned: {sig['spot_aligned']}")


def close_trade(token):
    with lock: tr = trades[token]
    row = [
        now().strftime("%Y-%m-%d"), tr["entry_time"], 
        max(tr["orig"]["exit_time"] or "0", tr["loose"]["exit_time"] or "0", tr["tight"]["exit_time"] or "0", tr["lcl"]["exit_time"] or "0"), 
        tr["symbol"], token, tr["qty"], tr["entry"], "", 
        tr["meta"]["prev_rsi"], tr["meta"]["live_rsi"], tr["meta"]["opp_live_rsi"], tr["meta"]["vwap"], tr["meta"]["volume"], 
        tr["meta"]["ema9"], tr["meta"]["ema20"], tr["meta"]["atr"], tr["meta"]["dist_vwap"], 
        "MULTI", "", "", tr["orig"]["highest"], tr["m"], "", 
        tr["orig"]["exit_price"], tr["orig"]["exit_reason"], tr["orig"]["gross"], tr["orig"]["net"], tr["orig"]["duration"], 
        tr["loose"]["exit_price"], tr["loose"]["exit_reason"], tr["loose"]["gross"], tr["loose"]["net"], tr["loose"]["duration"], 
        tr["tight"]["exit_price"], tr["tight"]["exit_reason"], tr["tight"]["gross"], tr["tight"]["net"], tr["tight"]["duration"], 
        tr["lcl"]["exit_price"], tr["lcl"]["exit_reason"], tr["lcl"]["gross"], tr["lcl"]["net"], tr["lcl"]["duration"], 
        tr["meta"]["spot_aligned"], tr["meta"]["vol_surge_met"], tr["meta"]["vwap_met"], tr["meta"]["vol_war_met"]
    ]
    with open(TRADE_CSV, "a", newline="") as f: csv.writer(f).writerow(row)
    with lock: del trades[token]
    log(f"CLOSED SIM POSITION {tr['symbol']}")
    refresh_historical_csv_cache()

def finish_leg(tr, leg, reason, px):
    c = tr[leg]
    if not c["active"]: return
    c["active"], c["exit_price"], c["exit_reason"], c["exit_time"] = False, round2(px), reason, tstr()
    gross = round2((px - tr["entry"]) * tr["qty"])
    c["gross"] = gross
    c["net"] = round2(gross - (10 + (0.0016 * tr["qty"] * (tr["entry"] + px))))
    try:
        c["duration"] = int((datetime.strptime(c["exit_time"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(tr["entry_time"], "%Y-%m-%d %H:%M:%S")).total_seconds())
    except:
        c["duration"] = 0

def manage_trade(token):
    with lock:
        if token not in trades: return
        snap = market_state.get(token)
        if not snap or "ltp" not in snap: return
        tr, ltp = trades[token], snap["ltp"]

    for leg in ["orig", "loose", "tight"]:
        c = tr[leg]
        if not c["active"]: continue
        if ltp > c["highest"]: c["highest"] = ltp
        
        if not c["locked"]:
            if leg == "loose":
                # Safely get activation thresholds, falling back to basic values if loading old state
                if ltp >= c.get("activation", tr["entry"] + 10): 
                    c["trigger"] = c.get("min_profit", tr["entry"] + 1.5)
                    c["locked"] = True
            else:
                if ltp >= tr["entry"] + tr["m"] + 0.10: 
                    c["trigger"], c["locked"] = round2(tr["entry"] + tr["m"] + 0.05), True
        else:
            nt = round2(c["highest"] - c["trail"])
            if nt > c["trigger"]: 
                c["trigger"] = nt
                
        if ltp <= c["trigger"]: finish_leg(tr, leg, "SL_HIT", c["trigger"])

    c_lcl = tr["lcl"]
    if c_lcl["active"]:
        if ltp > c_lcl["highest"]: c_lcl["highest"] = ltp
        
        latest_candle = snap.get("previous_candle", {})
        if latest_candle and "start_ft" in latest_candle:
            start_ft = int(latest_candle["start_ft"])
            if start_ft > c_lcl["last_completed_ft"]:
                candle_low = round2(latest_candle.get("low", 0.0))
                if candle_low > c_lcl["trigger"]: 
                    c_lcl["trigger"] = candle_low
                c_lcl["last_completed_ft"] = start_ft
                
        if ltp <= c_lcl["trigger"]: finish_leg(tr, "lcl", "LCL_SL_HIT", c_lcl["trigger"])

    if sum(1 for x in ["orig", "loose", "tight", "lcl"] if tr[x]["active"]) == 0:
        close_trade(token)
        trade_done_this_candle[token] = True

# ============================================================
# LOOPS
# ============================================================

def exit_loop():
    while True:
        for tk in list(trades.keys()):
            try: manage_trade(tk)
            except: pass
        time.sleep(EXIT_LOOP_DELAY)


def update_dynamic_pair():
    """Scans all available tokens from Master and selects the closest CE and PE to TARGET_PREMIUM."""
    global active_ce_token, active_pe_token
    
    ce_candidates = []
    pe_candidates = []
    
    with lock:
        for tk, snap in market_state.items():
            if "ltp" not in snap or "symbol" not in snap: continue
            
            ltp = float(snap["ltp"])
            symbol = snap["symbol"]
            diff = abs(ltp - TARGET_PREMIUM)
            
            if symbol.endswith("CE"):
                ce_candidates.append((tk, diff))
            elif symbol.endswith("PE"):
                pe_candidates.append((tk, diff))
                
    if ce_candidates:
        active_ce_token = min(ce_candidates, key=lambda x: x[1])[0]
    if pe_candidates:
        active_pe_token = min(pe_candidates, key=lambda x: x[1])[0]

def scanner_loop():
    global last_candle, active_ce_token, active_pe_token
    
    while True:
        c = hm()
        if now().minute % 5 == 0 and c != last_candle:
            last_candle = c
            for tk in list(trade_done_this_candle.keys()): 
                trade_done_this_candle[tk] = False
            
        if hm() >= FORCE_EXIT:
            for tk in list(trades.keys()):
                try: close_trade(tk)
                except: pass
            log("DAY END"); os._exit(0)

        if not time_in_range(ENTRY_START, ENTRY_END):
            time.sleep(IDLE_SLEEP); continue

        # 1. LOCK CHECK & PAIR UPDATE
        with lock:
            active_trade_count = len(trades)
            
        if active_trade_count == 0:
            update_dynamic_pair()
        else:
            time.sleep(IDLE_SLEEP)
            continue
            
        if not active_ce_token or not active_pe_token:
            time.sleep(IDLE_SLEEP)
            continue

        # 2. SIGNAL EVALUATION
        t = time.time()
        with spot_lock:
            spot_snap = market_state_spot.get(SPOT_TOKEN)
            
        with lock:
            c_snap = market_state.get(active_ce_token)
            p_snap = market_state.get(active_pe_token)
            
        if not c_snap or not p_snap: 
            time.sleep(IDLE_SLEEP)
            continue
            
        if scan_pending.get(active_ce_token) and (t - last_scan_ts.get(active_ce_token, 0) >= SCAN_COOLDOWN):
            scan_pending[active_ce_token], last_scan_ts[active_ce_token] = False, t
            
        if scan_pending.get(active_pe_token) and (t - last_scan_ts.get(active_pe_token, 0) >= SCAN_COOLDOWN):
            scan_pending[active_pe_token], last_scan_ts[active_pe_token] = False, t

        sigs = evaluate_dual_signal(c_snap, p_snap, spot_snap)
        
        for sig in sigs:
            target_token = active_ce_token if sig["type"] == "CALL" else active_pe_token
            target_sym = next((s for s in symbols if s["token"] == target_token), None)
            
            if not target_sym: continue
            if target_token in trades: continue
            if trade_done_this_candle.get(target_token): continue
            if last_signal_prev_candle.get(target_token) == sig["prev_candle_time"]: continue
            
            open_trade(target_sym, sig)
                
        time.sleep(IDLE_SLEEP)


# ============================================================
# DASHBOARD UI
# ============================================================

def format_cell(val, width=16):
    raw = f"{'+' if val > 0 else ''}{val:.2f}"
    pad = f"{raw:<{width}}"
    return f"{GREEN}{pad}{RESET}" if val > 0 else (f"{RED}{pad}{RESET}" if val < 0 else f"{RESET}{pad}{RESET}")

def format_percent(wins, losses, width=16):
    total = wins + losses
    val = (wins / total * 100.0) if total > 0 else 0.0
    raw = f"{val:.1f}%"
    pad = f"{raw:<{width}}"
    return f"{CYAN}{pad}{RESET}"

def get_leg_metrics():
    stats = {
        "today": {"orig": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "loose": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "tight": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "lcl": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "completed": 0},
        "all_time": {"orig": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "loose": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "tight": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "lcl": {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0}, "completed": 0}
    }
    
    # Map the separated cache metrics into the live stats dictionary
    for scope in ["today", "all_time"]:
        stats[scope]["completed"] = cached_csv_stats[scope]["completed"]
        for leg in ["orig", "loose", "tight", "lcl"]:
            stats[scope][leg]["real_net"] = cached_csv_stats[scope][leg]["real_net"]
            stats[scope][leg]["wins"] = cached_csv_stats[scope][leg]["wins"]
            stats[scope][leg]["losses"] = cached_csv_stats[scope][leg]["losses"]

    try:
        with lock: active_trades = dict(trades)
        for token, tr in active_trades.items():
            entry, qty = float(tr["entry"]), float(tr["qty"])
            snap = market_state.get(token)
            if not snap or "ltp" not in snap: continue
            ltp = snap["ltp"]
            for leg in ["orig", "loose", "tight", "lcl"]:
                if tr.get(leg, {}).get("active", False):
                    net = ((float(ltp) - entry) * qty) - (10 + (0.0016 * qty * (entry + float(ltp))))
                    for scope in ["today", "all_time"]:
                        stats[scope][leg]["active"] += 1
                        stats[scope][leg]["unreal_net"] += net
    except Exception: pass
    return stats

def dashboard_loop():
    while True:
        stats = get_leg_metrics()
        out = [CURSOR_HOME + CLEAR_FROM_CURSOR, f"{CYAN}================================== POC TRADER ENGINE ==================================={RESET}\n"]
        for title, key in [("Today's Simulation", "today"), ("All-Time Simulation", "all_time")]:
            sd = stats[key]
            out.extend([f" {YELLOW}[ {title.upper()} ]{RESET}  Setups: {sd['completed']}\n", "-" * 98 + "\n", f"{'METRIC':<22} | {'ORIG':<16} | {'LOOSE':<16} | {'TIGHT':<16} | {'LCL':<16}\n", "-" * 98 + "\n"])
            
            out.append(f"{'Active Legs':<22} | {format_cell(sd['orig']['active'])} | {format_cell(sd['loose']['active'])} | {format_cell(sd['tight']['active'])} | {format_cell(sd['lcl']['active'])}\n")
            out.append(f"{'Win Rate (%)':<22} | {format_percent(sd['orig']['wins'], sd['orig']['losses'])} | {format_percent(sd['loose']['wins'], sd['loose']['losses'])} | {format_percent(sd['tight']['wins'], sd['tight']['losses'])} | {format_percent(sd['lcl']['wins'], sd['lcl']['losses'])}\n")
            out.append(f"{'Realized Net':<22} | {format_cell(sd['orig']['real_net'])} | {format_cell(sd['loose']['real_net'])} | {format_cell(sd['tight']['real_net'])} | {format_cell(sd['lcl']['real_net'])}\n")
            out.append(f"{'Unrealized Net':<22} | {format_cell(sd['orig']['unreal_net'])} | {format_cell(sd['loose']['unreal_net'])} | {format_cell(sd['tight']['unreal_net'])} | {format_cell(sd['lcl']['unreal_net'])}\n")
            
            tot_o = sd['orig']['real_net'] + sd['orig']['unreal_net']
            tot_l = sd['loose']['real_net'] + sd['loose']['unreal_net']
            tot_t = sd['tight']['real_net'] + sd['tight']['unreal_net']
            tot_lc = sd['lcl']['real_net'] + sd['lcl']['unreal_net']
            
            out.extend(["-" * 98 + "\n", f"{YELLOW}{'NET RETURN':<22}{RESET} | {format_cell(tot_o)} | {format_cell(tot_l)} | {format_cell(tot_t)} | {format_cell(tot_lc)}\n", f"{CYAN}========================================================================================{RESET}\n"])
        
        out.append(f"\n{YELLOW}--- LIVE EXECUTION LOGS ---{RESET}\n")
        out.extend(f"{l}\n" for l in terminal_logs)
        sys.stdout.write("".join(out)); sys.stdout.flush()
        save_live_state()
        time.sleep(DASHBOARD_DELAY)

# ============================================================
# BOOT OVERSEER
# ============================================================

def main():
    # Removed pair_map from globals
    global ui_active, symbols, req_socket
    init_csv()
    
    req_socket = zmq_context.socket(zmq.REQ)
    req_socket.connect(f"tcp://{MASTER_IP}:{MASTER_REP_PORT}")
    
    print("Connecting synchronous parameters to Master Broker Module...")
    resp = send_master_request({"cmd": "GET_SYMBOLS"})
    if resp and "symbols" in resp:
        symbols = resp["symbols"]
        
        for s in symbols:
            tk = s["token"]
            trade_done_this_candle[tk] = False
            scan_pending[tk] = False
            last_scan_ts[tk] = 0
            
            match = re.search(r'([CP]E?)\d+$', s["tsym"])
            s["opt_type"] = 'C' if match and 'C' in match.group(1) else ('P' if match else None)
                
        print(f"Captured {len(symbols)} tracked options profiles from Master. Dynamic pairing active.")
    else:
        print("❌ Core Handshake Failure: Verify Master Bot status loop.")
        return

    print("Pre-seeding base structures & volume histories...")
    for s in symbols:
        tk = s["token"]
        snap = send_master_request({"cmd": "GET_SNAPSHOT", "token": tk})
        if snap and "error" not in snap:
            market_state[tk] = snap
            
        hist_resp = send_master_request({"cmd": "GET_HISTORY", "token": tk})
        if hist_resp and "history" in hist_resp:
            vols = [float(c.get("volume", 0.0)) for c in hist_resp["history"]]
            volume_history[tk] = deque(vols, maxlen=10)
        else:
            volume_history[tk] = deque(maxlen=10)
            
    spot_snap = send_master_request({"cmd": "GET_SNAPSHOT", "token": "26000"})
    if spot_snap and "error" not in spot_snap:
        market_state_spot["26000"] = spot_snap

    init_terminal()
    ui_active = True
    sys.stdout.write(ENTER_ALT_SCREEN + CLEAR_SCREEN + HIDE_CURSOR); sys.stdout.flush()
    
    try:
        threading.Thread(target=zmq_subscriber_loop, daemon=True).start()
        threading.Thread(target=ob_watcher_loop, daemon=True).start()
        threading.Thread(target=exit_loop, daemon=True).start()
        threading.Thread(target=scanner_loop, daemon=True).start()
        dashboard_loop()
    except KeyboardInterrupt: pass
    finally:
        sys.stdout.write(SHOW_CURSOR + EXIT_ALT_SCREEN); sys.stdout.flush()
        print(f"{YELLOW}Simulation framework offline.{RESET}")

if __name__ == "__main__": 
    main()
