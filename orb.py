import csv
import os
import time
import threading
import json
import tempfile
import zmq
import re
import requests
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

# Timings
ORB_MARKTIME = "09:20"
ENTRY_END    = "15:00"
FORCE_EXIT   = "15:20"

# Delays
EXIT_LOOP_DELAY  = 0.05
STATE_SAVE_DELAY = 1.0
IDLE_SLEEP       = 0.05

SPOT_TOKEN = "26000"

# ZeroMQ Master Broker Addresses
MASTER_IP = "127.0.0.1"
MASTER_PUB_PORT = 5555
MASTER_REP_PORT = 5556

# ============================================================
# STATE STORAGE
# ============================================================
LEGS = ["soft", "hard", "lcl"]

market_state = {}
market_state_spot = {}

orb_high = None
orb_low = None
spot_gap = 0.0
orb_initialized = False
options_selected = False

active_ce_token = None
active_pe_token = None
symbols = []

trades = {}
trade_done_today = False

lock = threading.Lock()
spot_lock = threading.Lock()
req_lock = threading.Lock()

# ============================================================
# HELPER FUNCTIONS
# ============================================================
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

def log(msg, notify=False):
    formatted = f"[{tstr()}] {msg}"
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
    except Exception:
        pass

# ============================================================
# CSV LOGGING ENGINE
# ============================================================
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

# ============================================================
# NETWORKING LAYER (ZMQ CLIENT)
# ============================================================
zmq_context = zmq.Context()
req_socket = None

def reset_req_socket():
    global req_socket
    if req_socket is not None:
        req_socket.close(linger=0)
    req_socket = zmq_context.socket(zmq.REQ)
    req_socket.connect(f"tcp://{MASTER_IP}:{MASTER_REP_PORT}")

def send_master_request(payload):
    with req_lock:
        if req_socket is None:
            reset_req_socket()
        try:
            req_socket.send_json(payload)
            if req_socket.poll(5000):
                return req_socket.recv_json()
            reset_req_socket()
            return None
        except Exception:
            reset_req_socket()
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
                with lock: market_state[tk] = msg
            elif topic == b"SPOT_SNAPSHOT":
                with spot_lock: market_state_spot[tk] = msg
        except Exception:
            time.sleep(0.1)

# ============================================================
# ORB CANDLE & CONTRACT SELECTION (SHOONYA MASTER)
# ============================================================
def fetch_shoonya_orb():
    resp = send_master_request({"cmd": "GET_SPOT_HISTORY"})
    if not resp or "history" not in resp:
        return None

    history = resp["history"]
    today_str = now().strftime("%Y-%m-%d")

    orb_candle = None
    orb_idx = -1
    for i, c in enumerate(history):
        ft = c.get("start_ft")
        if not ft:
            continue
        dt = datetime.fromtimestamp(int(ft))
        if dt.strftime("%Y-%m-%d") == today_str and dt.strftime("%H:%M") == "09:15":
            orb_candle = c
            orb_idx = i

    if orb_candle is None:
        return None

    prev_close = None
    for c in reversed(history[:orb_idx]):
        ft = c.get("start_ft")
        if not ft:
            continue
        if datetime.fromtimestamp(int(ft)).strftime("%Y-%m-%d") < today_str:
            prev_close = float(c["close"])
            break

    gap = round2(float(orb_candle["open"]) - prev_close) if prev_close is not None else 0.0
    return {
        "open": float(orb_candle["open"]),
        "high": float(orb_candle["high"]),
        "low": float(orb_candle["low"]),
        "close": float(orb_candle["close"]),
        "gap": gap
    }

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
    except Exception:
        c["duration"] = 0

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
    global orb_high, orb_low, spot_gap, orb_initialized, options_selected

    while True:
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

        # 2. Fetch today's closed 09:15-09:20 candle from Shoonya Master
        if not orb_initialized:
            if "06:00" <= current_time_str < ORB_MARKTIME:
                time.sleep(2.0)
                continue

            orb = fetch_shoonya_orb()
            if orb:
                orb_high = orb["high"]
                orb_low = orb["low"]
                spot_gap = orb["gap"]
                orb_initialized = True
                log(
                    f"🎯 Shoonya ORB Locked (09:15) -> O:{orb['open']} | H:{orb_high} | L:{orb_low} | C:{orb['close']} | Gap:{spot_gap}",
                    notify=True
                )
            else:
                log("Waiting for Shoonya 09:15 candle to close in Master...")
                time.sleep(2.0)
            continue

        # 3. Select options contracts near Rs 100 once ORB range is set
        if orb_initialized and not options_selected:
            select_contracts_closest_to_100()
            time.sleep(1.0)
            continue

        # 4. Watch for Spot Breakout Signals via Shoonya ZeroMQ stream
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
    while True:
        for tk in list(trades.keys()):
            try: manage_active_trade(tk)
            except Exception: pass
        time.sleep(EXIT_LOOP_DELAY)

# ============================================================
# SYSTEM MAIN ENTRY POINT
# ============================================================
def main():
    global symbols
    init_csv()
    send_telegram(f"[{tstr()}] ✅ orb.py started.", sync=True)

    print("Binding communication vectors to Master Broker Engine...", flush=True)
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
        if snap and "error" not in snap:
            market_state[tk] = snap

    send_telegram(f"[{tstr()}] ✅ Bot active.")

    try:
        threading.Thread(target=zmq_subscriber_loop, daemon=True).start()
        threading.Thread(target=exit_loop, daemon=True).start()
        threading.Thread(target=scanner_loop, daemon=True).start()
        while True:
            save_live_state()
            time.sleep(STATE_SAVE_DELAY)
    except KeyboardInterrupt:
        pass
    finally:
        print("ORB Engine disconnected smoothly.", flush=True)

if __name__ == "__main__":
    main()
