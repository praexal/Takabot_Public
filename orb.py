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
import requests
from tvDatafeed import TvDatafeed, Interval
import numpy as np

from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
LOT_SIZE = 65            
TARGET_PREMIUM = 100.0   

LOG_FOLDER = "trade_logs"
STATE_FOLDER = "temp_state"
TRADE_CSV = os.path.join(LOG_FOLDER, "orb_920_trades.csv")
STATE_FILE = os.path.join(STATE_FOLDER, "orb_live_state.json")

# Telegram Credentials (from GitHub Secrets / Env)
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "")
IS_CI        = os.getenv("GITHUB_ACTIONS") == "true"

# Timings
ORB_MARKTIME = "09:20"   
ENTRY_END    = "15:00"   
FORCE_EXIT   = "15:20"   

# Delays
EXIT_LOOP_DELAY  = 0.05
DASHBOARD_DELAY  = 1.0
IDLE_SLEEP       = 0.05

SPOT_TOKEN = "26000"

# ZeroMQ Master Broker Addresses
MASTER_IP = "127.0.0.1"
MASTER_PUB_PORT = 5555
MASTER_REP_PORT = 5556

# ANSI UI Formatting Strings
CURSOR_HOME = "\033[H"
CLEAR_FROM_CURSOR = "\033[J"
CLEAR_SCREEN = "\033[2J"
ENTER_ALT_SCREEN = "\033[?1049h"
EXIT_ALT_SCREEN = "\033[?1049l"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"

GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"

# ============================================================
# STATE STORAGE
# ============================================================
LEGS = ["soft", "hard", "lcl"]

market_state = {}
market_state_spot = {}

# Breakout Range Values
orb_high = None
orb_low = None
spot_gap = 0.0
orb_initialized = False
options_selected = False

active_ce_token = None
active_pe_token = None
symbols = []

tv = None

trades = {}
trade_done_today = False
ui_active = False
terminal_logs = []

lock = threading.Lock()
spot_lock = threading.Lock()
restart_requested = threading.Event()

cached_csv_stats = {
    "today": {**{l: {"real_net": 0.0, "wins": 0, "losses": 0} for l in LEGS}, "completed": 0},
    "all_time": {**{l: {"real_net": 0.0, "wins": 0, "losses": 0} for l in LEGS}, "completed": 0}
}

# ============================================================
# HELPER FUNCTIONS
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
def round2(x): return round(float(x), 2) if x is not None else 0.0

def send_telegram(msg, sync=False):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    def _post():
        try:
            url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
            requests.post(
                url,
                json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
                proxies={"http": None, "https": None},
                timeout=5
            )
        except Exception as e:
            print(f"Telegram error: {e}", flush=True)
    if sync:
        _post()
    else:
        threading.Thread(target=_post, daemon=True).start()

def telegram_command_loop():
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates"
    offset = None

    # Flush old messages on startup to avoid restart loops
    try:
        r = requests.get(
            url, params={"offset": -1}, proxies={"http": None, "https": None}, timeout=10
        ).json()
        if r.get("ok") and r.get("result"):
            offset = r["result"][-1]["update_id"] + 1
    except Exception:
        pass

    while not restart_requested.is_set():
        try:
            params = {"timeout": 20}
            if offset is not None:
                params["offset"] = offset
            r = requests.get(
                url, params=params, proxies={"http": None, "https": None}, timeout=25
            ).json()
            if r.get("ok"):
                for update in r.get("result", []):
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    text = msg.get("text", "").strip().lower()

                    if chat_id == str(TG_CHAT_ID) and text == "/restart":
                        try:
                            requests.get(
                                url, params={"offset": offset},
                                proxies={"http": None, "https": None}, timeout=5
                            )
                        except Exception:
                            pass
                        send_telegram(f"[{tstr()}] 🔄 Restarting orb.py...", sync=True)
                        restart_requested.set()
                        return
        except Exception:
            time.sleep(5)

def connect_tv():
    global tv
    try:
        tv = TvDatafeed()
        return True
    except Exception as e:
        log(f"TradingView connect error: {e}")
        tv = None
        return False

def log(msg, notify=False):
    global terminal_logs
    formatted = f"[{tstr()}] {msg}"
    terminal_logs.append(formatted)
    terminal_logs = terminal_logs[-8:]
    if not ui_active:
        print(formatted, flush=True)
    if notify:
        send_telegram(formatted)

def save_live_state():
    try:
        state = {
            "orb_high": orb_high,
            "orb_low": orb_low,
            "active_ce": active_ce_token,
            "active_pe": active_pe_token,
            "trades": trades
        }
        temp_fd, temp_path = tempfile.mkstemp(dir=STATE_FOLDER)
        with os.fdopen(temp_fd, 'w') as tmp: json.dump(state, tmp)
        os.replace(temp_path, STATE_FILE)
    except Exception: pass

# ============================================================
# CSV LOGGING ENGINE
# ============================================================
def refresh_historical_csv_cache():
    global cached_csv_stats
    schema = lambda: {**{l: {"real_net": 0.0, "wins": 0, "losses": 0} for l in LEGS}, "completed": 0}
    cached_csv_stats = {"today": schema(), "all_time": schema()}
    
    if os.path.exists(TRADE_CSV) and os.path.getsize(TRADE_CSV) > 0:
        try:
            df = pd.read_csv(TRADE_CSV)
            if not df.empty:
                today_str = now().strftime("%Y-%m-%d")
                df_today = df[df['date'] == today_str] if 'date' in df.columns else pd.DataFrame()
                
                for scope, current_df in [("all_time", df), ("today", df_today)]:
                    cached_csv_stats[scope]["completed"] = len(current_df)
                    for leg in LEGS:
                        col = f'{leg}_net'
                        if col in current_df.columns:
                            net_col = pd.to_numeric(current_df[col], errors='coerce').fillna(0)
                            cached_csv_stats[scope][leg]["real_net"] = net_col.sum()
                            cached_csv_stats[scope][leg]["wins"] = (net_col > 0).sum()
                            cached_csv_stats[scope][leg]["losses"] = (net_col <= 0).sum()
        except Exception: pass

def init_csv():
    os.makedirs(LOG_FOLDER, exist_ok=True)
    os.makedirs(STATE_FOLDER, exist_ok=True)
    if not os.path.exists(TRADE_CSV) or os.path.getsize(TRADE_CSV) == 0:
        leg_cols = [f"{l}_{f}" for l in LEGS for f in ["entry", "exit", "reason", "gross", "net", "duration"]]
        with open(TRADE_CSV, "w", newline="") as f:
            csv.writer(f).writerow([
                "date", "entry_time", "strategy", "symbol", "token", "qty", 
                "spot_high", "spot_low", "opening_gap", *leg_cols
            ])
    refresh_historical_csv_cache()

# ============================================================
# NETWORKING LAYER (ZMQ CLIENT)
# ============================================================
zmq_context = zmq.Context()
req_socket = zmq_context.socket(zmq.REQ)

def send_master_request(payload):
    try:
        req_socket.send_json(payload)
        if req_socket.poll(5000): return req_socket.recv_json()
        return None
    except Exception: return None

def zmq_subscriber_loop():
    sub_socket = zmq_context.socket(zmq.SUB)
    sub_socket.connect(f"tcp://{MASTER_IP}:{MASTER_PUB_PORT}")
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"SNAPSHOT")
    sub_socket.setsockopt(zmq.SUBSCRIBE, b"SPOT_SNAPSHOT")
    
    while not restart_requested.is_set():
        try:
            topic, payload_bytes = sub_socket.recv_multipart()
            msg = json.loads(payload_bytes.decode('utf-8'))
            tk = msg.get("token")
            if not tk: continue
            if topic == b"SNAPSHOT":
                with lock: market_state[tk] = msg
            elif topic == b"SPOT_SNAPSHOT":
                with spot_lock: market_state_spot[tk] = msg
        except Exception: time.sleep(0.1)

# ============================================================
# ORB DYNAMIC SELECTION
# ============================================================
def select_contracts_closest_to_100():
    global active_ce_token, active_pe_token, options_selected
    ce_candidates, pe_candidates = [], []
    
    with lock:
        for tk, snap in market_state.items():
            symbol = snap.get("symbol", "")
            ltp = snap.get("ltp")
            
            if not ltp or not symbol: 
                continue
            
            diff = abs(float(ltp) - TARGET_PREMIUM)
            
            match = re.search(r'([CP])\d+$', symbol)
            if match:
                if match.group(1) == 'C':
                    ce_candidates.append((tk, diff))
                elif match.group(1) == 'P':
                    pe_candidates.append((tk, diff))
                
    if ce_candidates and pe_candidates:
        active_ce_token = min(ce_candidates, key=lambda x: x[1])[0]
        active_pe_token = min(pe_candidates, key=lambda x: x[1])[0]
        options_selected = True
        
        log(f"Selected CE: {market_state[active_ce_token]['symbol']} (@{market_state[active_ce_token]['ltp']})", notify=True)
        log(f"Selected PE: {market_state[active_pe_token]['symbol']} (@{market_state[active_pe_token]['ltp']})", notify=True)

# ============================================================
# ORDER EXECUTION ENGINE (SOFT, HARD, LCL)
# ============================================================
def _build_atr_leg(entry_px, atr_used):
    return {
        "active": True, 
        "highest": entry_px, 
        "entry": entry_px,
        "trigger": round2(entry_px - (atr_used * 0.6)),
        "activation": round2(entry_px + (atr_used * 0.8)),
        "min_profit": round2(entry_px + 1.5),
        "trail": round2(atr_used * 0.5), 
        "locked": False,
        "exit_price": "", "exit_reason": "", "exit_time": "", "gross": "", "net": "", "duration": ""
    }

def open_trade(target_token, side):
    global trade_done_today
    with lock:
        snap = market_state.get(target_token)
        if not snap or target_token in trades: return
        entry_px = float(snap["ltp"])
        atr_val = float(snap.get("atr", 15.0))
        if atr_val == 0: atr_val = 15.0
        
        soft_atr = atr_val                # Uncapped ATR
        hard_atr = min(atr_val, 10.0)     # Hard-capped ATR at 10.0
        
        prev_candle = snap.get("previous_candle", {})
        initial_lcl_sl = round2(prev_candle.get("low", entry_px - 10))
        
        trades[target_token] = {
            "symbol": snap["symbol"], 
            "token": target_token, 
            "side": side,
            "entry_time": tstr(), 
            "qty": LOT_SIZE,
            "entry": entry_px,
            "soft": _build_atr_leg(entry_px, soft_atr),
            "hard": _build_atr_leg(entry_px, hard_atr),
            "lcl": {
                "active": True, 
                "highest": entry_px, 
                "entry": entry_px,
                "trigger": initial_lcl_sl,
                "last_completed_ft": int(prev_candle.get("start_ft", 0)),
                "exit_price": "", "exit_reason": "", "exit_time": "", "gross": "", "net": "", "duration": ""
            }
        }
    trade_done_today = True
    log(f"🚀 BREAKOUT ENTRY: bought {snap['symbol']} at {entry_px}", notify=True)

def finish_leg(tr, leg, reason, px):
    c = tr[leg]
    if not c["active"]: return
    c["active"], c["exit_price"], c["exit_reason"], c["exit_time"] = False, round2(px), reason, tstr()
    gross = round2((px - c["entry"]) * tr["qty"])
    c["gross"] = gross
    c["net"] = round2(gross - (10 + (0.0016 * tr["qty"] * (c["entry"] + px))))
    try:
        c["duration"] = int((datetime.strptime(c["exit_time"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(tr["entry_time"], "%Y-%m-%d %H:%M:%S")).total_seconds())
    except Exception: c["duration"] = 0

def close_trade_registry(token):
    with lock: tr = trades[token]
    leg_data = []
    for l in LEGS:
        leg_data.extend([
            tr[l]["entry"], tr[l]["exit_price"], tr[l]["exit_reason"],
            tr[l]["gross"], tr[l]["net"], tr[l]["duration"]
        ])
    row = [
        now().strftime("%Y-%m-%d"), tr["entry_time"], "ORB_920", tr["symbol"], token, tr["qty"], 
        orb_high, orb_low, spot_gap, *leg_data
    ]
    with open(TRADE_CSV, "a", newline="") as f: csv.writer(f).writerow(row)
    with lock: del trades[token]

    lines = [f"<b>{tr['symbol']} ({tr['side']})</b>"]
    for l in LEGS:
        net = float(tr[l]["net"] or 0.0)
        lines.append(f"{l} = {net:+.2f}")
    send_telegram("\n".join(lines))

    log(f"✨ Strategy Evaluation complete for {tr['symbol']}.")
    refresh_historical_csv_cache()

def manage_active_trade(token):
    with lock:
        if token not in trades: return
        snap = market_state.get(token)
        if not snap or "ltp" not in snap: return
        tr, ltp = trades[token], float(snap["ltp"])
        
    # --- SOFT & HARD ATR TRAILING MODULES ---
    for leg in ["soft", "hard"]:
        c = tr[leg]
        if c["active"]:
            if ltp > c["highest"]: c["highest"] = ltp
            
            if not c["locked"]:
                if ltp >= c.get("activation", tr["entry"] + 10): 
                    c["trigger"] = c.get("min_profit", tr["entry"] + 1.5)
                    c["locked"] = True
            else:
                nt = round2(c["highest"] - c["trail"])
                if nt > c["trigger"]: 
                    c["trigger"] = nt
                    
            if ltp <= c["trigger"]: finish_leg(tr, leg, f"{leg.upper()}_SL_HIT", c["trigger"])

    # --- LAST CANDLE LOW (LCL) MODULE ---
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

    if all(not tr[l]["active"] for l in LEGS):
        close_trade_registry(token)

# ============================================================
# PROCESSING LOOPS
# ============================================================
def scanner_loop():
    global orb_high, orb_low, spot_gap, orb_initialized, options_selected, tv
    tv_fail_count = 0
    
    while not restart_requested.is_set():
        current_time_str = hm()

        # 1. Handle Intraday EOD Stop Execution
        if current_time_str >= FORCE_EXIT:
            for tk in list(trades.keys()):
                with lock:
                    tr = trades[tk]
                    ltp = float(market_state[tk]["ltp"])
                    for l in LEGS:
                        finish_leg(tr, l, "EOD_FORCE", ltp)
                close_trade_registry(tk)
            log("Market close threshold met. Bot offline.")
            send_telegram(f"[{tstr()}] Market close threshold met. Bot offline.", sync=True)
            time.sleep(2.0)
            os._exit(0)
        
        # 2. Fetch the strict 9:15 AM candle from TradingView (with auto-reconnect)
        if not orb_initialized:
            if "06:00" <= current_time_str < "09:20":
                log("Waiting for 9:20 AM to pull TradingView ORB data...")
                time.sleep(2.0)
                continue
                
            if current_time_str == "09:20" and now().second < 5:
                time.sleep(1.0)
                continue

            if tv is None:
                if not connect_tv():
                    tv_fail_count += 1
                    if tv_fail_count == 1:
                        send_telegram(f"[{tstr()}] ⚠️ TradingView connection failed. Auto-reconnecting...")
                    time.sleep(5.0)
                    continue
                
            try:
                df = tv.get_hist(symbol='NIFTY', exchange='NSE', interval=Interval.in_5_minute, n_bars=50)
                
                if df is not None and not df.empty:
                    df['time_str'] = df.index.strftime("%H:%M")
                    df_915 = df[df['time_str'] == "09:15"]
                    
                    if not df_915.empty:
                        latest_915 = df_915.iloc[-1]
                        orb_high = float(latest_915['high'])
                        orb_low = float(latest_915['low'])
                        
                        pos = np.where(df.index == latest_915.name)[0][-1]
                        if pos > 0:
                            prev_close = float(df.iloc[pos-1]['close'])
                            spot_gap = round2(float(latest_915['open']) - prev_close)
                        else:
                            spot_gap = 0.0
                            
                        orb_initialized = True
                        tv_fail_count = 0
                        log(f"🎯 TV ORB Locked -> High: {orb_high} | Low: {orb_low} | Gap: {spot_gap}", notify=True)
                    else:
                        log("TradingView 9:15 candle not found yet. Retrying...")
                        time.sleep(3.0)
                else:
                    tv_fail_count += 1
                    log("TradingView returned no data. Reconnecting in 5s...")
                    if tv_fail_count == 1:
                        send_telegram(f"[{tstr()}] ⚠️ TradingView disconnected (no data). Auto-reconnecting...")
                    tv = None
                    time.sleep(5.0)
            except Exception as e:
                tv_fail_count += 1
                log(f"TradingView fetch error: {e}. Reconnecting in 5s...")
                if tv_fail_count == 1:
                    send_telegram(f"[{tstr()}] ⚠️ TradingView error: {e}. Auto-reconnecting...")
                tv = None
                time.sleep(5.0)
                
            continue

        # 3. Select options contracts near Rs 100 once ORB range is set
        if orb_initialized and not options_selected:
            select_contracts_closest_to_100()
            time.sleep(1.0)
            continue

        # 4. Watch for Spot Breakout Signals ALL DAY via Shoonya ZeroMQ stream
        if orb_initialized and options_selected and not trade_done_today:
            if current_time_str > ENTRY_END:
                time.sleep(IDLE_SLEEP)
                continue
                
            with spot_lock:
                spot_ltp = float(market_state_spot.get(SPOT_TOKEN, {}).get("ltp", 0.0))
                
            if spot_ltp == 0.0:
                time.sleep(IDLE_SLEEP)
                continue

            if spot_ltp > orb_high:
                open_trade(active_ce_token, "CALL")
            elif spot_ltp < orb_low:
                open_trade(active_pe_token, "PUT")

        time.sleep(IDLE_SLEEP)

def exit_loop():
    while not restart_requested.is_set():
        for tk in list(trades.keys()):
            try: manage_active_trade(tk)
            except Exception: pass
        time.sleep(EXIT_LOOP_DELAY)

# ============================================================
# ANSI TERMINAL DASHBOARD
# ============================================================
def format_cell(val, width=20):
    raw = f"{'+' if val > 0 else ''}{val:.2f}"
    pad = f"{raw:<{width}}"
    return f"{GREEN}{pad}{RESET}" if val > 0 else (f"{RED}{pad}{RESET}" if val < 0 else f"{RESET}{pad}{RESET}")

def format_percent(wins, losses, width=20):
    total = wins + losses
    val = (wins / total * 100.0) if total > 0 else 0.0
    return f"{CYAN}{f'{val:.1f}%':<{width}}{RESET}"

def get_live_metrics():
    schema = lambda: {**{l: {"real_net": 0.0, "unreal_net": 0.0, "wins": 0, "losses": 0, "active": 0} for l in LEGS}, "completed": 0}
    stats = {"today": schema(), "all_time": schema()}
    
    for sc in ["today", "all_time"]:
        stats[sc]["completed"] = cached_csv_stats[sc]["completed"]
        for leg in LEGS:
            stats[sc][leg]["real_net"] = cached_csv_stats[sc][leg]["real_net"]
            stats[sc][leg]["wins"] = cached_csv_stats[sc][leg]["wins"]
            stats[sc][leg]["losses"] = cached_csv_stats[sc][leg]["losses"]

    with lock: active_positions = dict(trades)
    for tk, tr in active_positions.items():
        snap = market_state.get(tk)
        if not snap or "ltp" not in snap: continue
        ltp = float(snap["ltp"])
        for leg in LEGS:
            c_leg = tr[leg]
            if c_leg["active"]:
                unreal = ((ltp - c_leg["entry"]) * tr["qty"]) - (10 + (0.0016 * tr["qty"] * (c_leg["entry"] + ltp)))
                for sc in ["today", "all_time"]:
                    stats[sc][leg]["active"] += 1
                    stats[sc][leg]["unreal_net"] += unreal
    return stats

def dashboard_loop():
    while not restart_requested.is_set():
        stats = get_live_metrics()
        with spot_lock:
            spot_ltp = market_state_spot.get(SPOT_TOKEN, {}).get("ltp", "N/A")
            
        out = [
            CURSOR_HOME + CLEAR_FROM_CURSOR,
            f"{CYAN}================================ 9:20 ORB BUYING STRATEGY ENGINE =================================={RESET}\n",
            f" NIFTY Spot LTP: {YELLOW}{spot_ltp}{RESET}  |  ORB Range Locked: High={YELLOW}{orb_high}{RESET} Low={YELLOW}{orb_low}{RESET}\n",
            f" Target Premium Asset Pool: CE Token={CYAN}{active_ce_token}{RESET} PE Token={CYAN}{active_pe_token}{RESET}\n\n"
        ]
        
        for name, key in [("Today's Runs", "today"), ("Historical Performance", "all_time")]:
            sd = stats[key]
            out.extend([
                f" {YELLOW}[ {name.upper()} ]{RESET} Combined Executions: {sd['completed']}\n",
                "-" * 98 + "\n",
                f"{'METRIC STRAT COMPARISON':<28} | {'SOFT TRAIL (UNCAP)':<20} | {'HARD TRAIL (CAP 10)':<20} | {'LCL TRAIL LEGS':<20}\n",
                "-" * 98 + "\n",
                f"{'Active Instances':<28} | {sd['soft']['active']:<20} | {sd['hard']['active']:<20} | {sd['lcl']['active']:<20}\n",
                f"{'Calculated Win Rate':<28} | {format_percent(sd['soft']['wins'], sd['soft']['losses'])} | {format_percent(sd['hard']['wins'], sd['hard']['losses'])} | {format_percent(sd['lcl']['wins'], sd['lcl']['losses'])}\n",
                f"{'Realized PnL (Net)':<28} | {format_cell(sd['soft']['real_net'])} | {format_cell(sd['hard']['real_net'])} | {format_cell(sd['lcl']['real_net'])}\n",
                f"{'Floating PnL (Net)':<28} | {format_cell(sd['soft']['unreal_net'])} | {format_cell(sd['hard']['unreal_net'])} | {format_cell(sd['lcl']['unreal_net'])}\n",
                "-" * 98 + "\n",
                f"{YELLOW}{'TOTAL ACCOUNT RETURN':<28}{RESET} | {format_cell(sd['soft']['real_net'] + sd['soft']['unreal_net'])} | {format_cell(sd['hard']['real_net'] + sd['hard']['unreal_net'])} | {format_cell(sd['lcl']['real_net'] + sd['lcl']['unreal_net'])}\n",
                f"{CYAN}==================================================================================================={RESET}\n"
            ])
            
        out.append(f"\n{YELLOW}--- LIVE EXECUTION MONITOR LOGS ---{RESET}\n")
        out.extend(f"{m}\n" for m in terminal_logs)
        sys.stdout.write("".join(out)); sys.stdout.flush()
        save_live_state()
        time.sleep(DASHBOARD_DELAY)

# ============================================================
# SYSTEM MAIN ENTRY POINT
# ============================================================
def main():
    global ui_active, symbols
    init_csv()
    send_telegram(f"[{tstr()}] ✅ orb.py started.", sync=True)
    
    print("Binding communication vectors to Master Broker Engine...", flush=True)
    req_socket.connect(f"tcp://{MASTER_IP}:{MASTER_REP_PORT}")
    
    resp = send_master_request({"cmd": "GET_SYMBOLS"})
    if resp and "symbols" in resp:
        symbols = resp["symbols"]
        print(f"Verified connection. Monitoring {len(symbols)} dynamic options instruments.", flush=True)
    else:
        print("❌ Handshake failure: Confirm that masterbotauto.py is running.", flush=True)
        return

    print("Pre-seeding environment structures...", flush=True)
    for s in symbols:
        tk = s["token"]
        snap = send_master_request({"cmd": "GET_SNAPSHOT", "token": tk})
        if snap and "error" not in snap: market_state[tk] = snap
        
    spot_snap = send_master_request({"cmd": "GET_SNAPSHOT", "token": SPOT_TOKEN})
    if spot_snap and "error" not in spot_snap: market_state_spot[SPOT_TOKEN] = spot_snap

    send_telegram(f"[{tstr()}] ✅ Bot active.")

    if not IS_CI:
        init_terminal()
        ui_active = True
        sys.stdout.write(ENTER_ALT_SCREEN + CLEAR_SCREEN + HIDE_CURSOR); sys.stdout.flush()
    
    try:
        threading.Thread(target=telegram_command_loop, daemon=True).start()
        threading.Thread(target=zmq_subscriber_loop, daemon=True).start()
        threading.Thread(target=exit_loop, daemon=True).start()
        threading.Thread(target=scanner_loop, daemon=True).start()
        if IS_CI:
            while not restart_requested.is_set():
                save_live_state()
                time.sleep(DASHBOARD_DELAY)
        else:
            dashboard_loop()
    except KeyboardInterrupt: pass
    finally:
        if not IS_CI:
            sys.stdout.write(SHOW_CURSOR + EXIT_ALT_SCREEN); sys.stdout.flush()
        if restart_requested.is_set():
            os.execv(sys.executable, [sys.executable] + sys.argv)
        print(f"{YELLOW}ORB Engine disconnected smoothly.{RESET}", flush=True)

if __name__ == "__main__":
    main()
