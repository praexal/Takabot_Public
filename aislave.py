import os
import re
import csv
import zmq
import json
import time
import threading
import pandas as pd
import xgboost as xgb
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================
LIVE_TRADING = False         # Keep False for paper testing; set True for real orders
CONFIDENCE_THRESHOLD = 0.75  # Minimum Brain 1 probability to enter
LOT_SIZE = 75
TARGET_PREMIUM = 100.0
MIN_PREMIUM = 50.0

# Safety Clamps (in ATR multiples)
MIN_SL_ATR    = 0.4
MAX_SL_ATR    = 1.2
MIN_TRAIL_ATR = 0.3
MAX_TRAIL_ATR = 1.0
MAX_ATR_CAP   = 10.0

ENTRY_START = "09:30"
ENTRY_END   = "14:59"
FORCE_EXIT  = "15:20"

MASTER_IP = "127.0.0.1"
MASTER_PUB_PORT = 5555
MASTER_REP_PORT = 5556

TRADE_CSV        = "trade_logs/ai_trades.csv"
ENTRY_MODEL_PATH = "ai_entry_brain.json"
SL_MODEL_PATH    = "ai_sl_brain.json"
TRAIL_MODEL_PATH = "ai_trail_brain.json"

FEATURES = [
    "minute_of_day", "opt_type_num", "prev_rsi", "live_rsi", "opp_live_rsi",
    "ema_spread", "atr", "dist_vwap", "volume", "vol_war_ratio",
    "spot_rsi5", "spot_ema_spread", "dist_poc", "in_ob"
]

# ============================================================
# GLOBALS & MODELS
# ============================================================
entry_model = xgb.XGBClassifier()
sl_model    = xgb.XGBRegressor()
trail_model = xgb.XGBRegressor()

market_state = {}
spot_state = {}
active_trade = None
lock = threading.Lock()

zmq_context = zmq.Context()
req_socket = zmq_context.socket(zmq.REQ)

def now(): return datetime.now()
def tstr(): return now().strftime("%Y-%m-%d %H:%M:%S")
def hm(): return now().strftime("%H:%M")
def round2(x): return round(float(x), 2)
def log(msg): print(f"[{tstr()}] {msg}")

def load_json_safe(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def init_csv():
    os.makedirs("trade_logs", exist_ok=True)
    if not os.path.exists(TRADE_CSV) or os.path.getsize(TRADE_CSV) == 0:
        with open(TRADE_CSV, "w", newline="") as f:
            csv.writer(f).writerow([
                "date", "entry_time", "exit_time", "symbol", "opt_type",
                "ai_prob", "ai_sl_mult", "ai_act_mult", "ai_trail_mult",
                "entry", "exit", "exit_reason", "net_pnl"
            ])

# ============================================================
# ZMQ NETWORKING
# ============================================================
def send_master_request(payload):
    try:
        req_socket.send_json(payload)
        if req_socket.poll(5000):
            return req_socket.recv_json()
    except Exception as e:
        log(f"ZMQ REQ Error: {e}")
    return None

def zmq_subscriber_loop():
    sub = zmq_context.socket(zmq.SUB)
    sub.connect(f"tcp://{MASTER_IP}:{MASTER_PUB_PORT}")
    sub.setsockopt(zmq.SUBSCRIBE, b"SNAPSHOT")
    sub.setsockopt(zmq.SUBSCRIBE, b"SPOT_SNAPSHOT")
    while True:
        try:
            topic, payload = sub.recv_multipart()
            msg = json.loads(payload.decode("utf-8"))
            if topic == b"SNAPSHOT":
                with lock:
                    market_state[msg["token"]] = msg
            elif topic == b"SPOT_SNAPSHOT":
                with lock:
                    spot_state.update(msg)
        except Exception:
            time.sleep(0.1)

# ============================================================
# FEATURE BUILDER & PAIR SELECTION
# ============================================================
def get_active_pair():
    ce, pe = [], []
    with lock:
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

def build_features(target, opp, opt_type):
    pocs = load_json_safe("POC.json").get("pocs", [])
    obs = load_json_safe("OrderBlocks.json")
    spot_ltp = float(spot_state.get("ltp", 0.0))

    dist_poc = min([abs(spot_ltp - p) for p in pocs], default=999.0)
    all_obs = obs.get("bull_order_blocks", []) + obs.get("bear_order_blocks", [])
    in_ob = int(any(float(z["bottom"]) <= spot_ltp <= float(z["top"]) for z in all_obs))

    dt = now()
    o_vol = max(float(opp.get("volume", 1.0)), 1.0)

    row = {
        "minute_of_day": dt.hour * 60 + dt.minute,
        "opt_type_num": 1 if opt_type == "CE" else 0,
        "prev_rsi": float(target.get("prev_rsi", 0)),
        "live_rsi": float(target.get("live_rsi", 0)),
        "opp_live_rsi": float(opp.get("live_rsi", 0)),
        "ema_spread": round2(float(target.get("ema9", 0)) - float(target.get("ema20", 0))),
        "atr": float(target.get("atr", 0)),
        "dist_vwap": round2(float(target.get("ltp", 0)) - float(target.get("vwap", 0))),
        "volume": float(target.get("volume", 0)),
        "vol_war_ratio": round2(float(target.get("volume", 0)) / o_vol),
        "spot_rsi5": float(spot_state.get("live_rsi5", 0)),
        "spot_ema_spread": round2(float(spot_state.get("ema9", 0)) - float(spot_state.get("ema20", 0))),
        "dist_poc": round2(dist_poc),
        "in_ob": in_ob
    }
    return pd.DataFrame([row])[FEATURES]

# ============================================================
# EXECUTION & EXIT MANAGEMENT
# ============================================================
def open_trade(snap, opt_type, prob, sl_mult, mfe_mult):
    global active_trade
    entry = round2(snap["ltp"])
    raw_atr = float(snap.get("atr", 10.0)) or 10.0
    capped_atr = min(raw_atr, MAX_ATR_CAP)

    # Brain 2: Dynamic Initial SL
    safe_sl_mult = round2(max(MIN_SL_ATR, min(MAX_SL_ATR, sl_mult)))
    sl_price = round2(entry - (capped_atr * safe_sl_mult))

    # Brain 3: Dynamic Profit Lock (50% of peak) & Trail (35% of peak)
    safe_act_mult   = round2(max(0.5, min(1.5, mfe_mult * 0.50)))
    safe_trail_mult = round2(max(MIN_TRAIL_ATR, min(MAX_TRAIL_ATR, mfe_mult * 0.35)))

    if LIVE_TRADING:
        resp = send_master_request({
            "cmd": "PLACE_ORDER", "side": "BUY", "exchange": "NFO",
            "symbol": snap["symbol"], "token": snap["token"],
            "qty": LOT_SIZE, "price_type": "MKT", "price": 0.0
        })
        log(f"LIVE BUY SENT: {resp}")

    with lock:
        active_trade = {
            "token": snap["token"], "symbol": snap["symbol"], "opt_type": opt_type,
            "entry_time": tstr(), "entry": entry, "highest": entry,
            "trigger": sl_price,
            "activation": round2(entry + (capped_atr * safe_act_mult)),
            "min_profit": round2(entry + 1.5),
            "trail": round2(capped_atr * safe_trail_mult),
            "locked": False, "ai_prob": round2(prob),
            "ai_sl_mult": safe_sl_mult, "ai_act_mult": safe_act_mult,
            "ai_trail_mult": safe_trail_mult
        }
    log(
        f"🚀 AI ENTRY [{opt_type}] {snap['symbol']} @ {entry} | "
        f"Prob: {prob:.2f} | SL: {safe_sl_mult}x | Act: {safe_act_mult}x | Trail: {safe_trail_mult}x"
    )

def close_trade(exit_px, reason):
    global active_trade
    with lock:
        tr = active_trade
        active_trade = None
    if not tr:
        return

    if LIVE_TRADING:
        resp = send_master_request({
            "cmd": "PLACE_ORDER", "side": "SELL", "exchange": "NFO",
            "symbol": tr["symbol"], "token": tr["token"],
            "qty": LOT_SIZE, "price_type": "MKT", "price": 0.0
        })
        log(f"LIVE SELL SENT: {resp}")

    gross = (exit_px - tr["entry"]) * LOT_SIZE
    net = round2(gross - (10 + (0.0016 * LOT_SIZE * (tr["entry"] + exit_px))))

    with open(TRADE_CSV, "a", newline="") as f:
        csv.writer(f).writerow([
            now().strftime("%Y-%m-%d"), tr["entry_time"], tstr(), tr["symbol"],
            tr["opt_type"], tr["ai_prob"], tr["ai_sl_mult"], tr["ai_act_mult"],
            tr["ai_trail_mult"], tr["entry"], round2(exit_px), reason, net
        ])
    log(f"🏁 AI EXIT {tr['symbol']} @ {exit_px} | Reason: {reason} | Net PnL: {net}")

# ============================================================
# MAIN LOOPS
# ============================================================
def scanner_loop():
    while True:
        if hm() >= FORCE_EXIT:
            if active_trade:
                ltp = market_state.get(active_trade["token"], {}).get("ltp", active_trade["entry"])
                close_trade(float(ltp), "EOD_FORCE")
            log("Market closed. Shutting down.")
            os._exit(0)

        if active_trade is None and (ENTRY_START <= hm() <= ENTRY_END) and spot_state:
            c_tk, p_tk = get_active_pair()
            if c_tk and p_tk:
                with lock:
                    c_snap, p_snap = market_state[c_tk], market_state[p_tk]

                for target, opp, otype in [(c_snap, p_snap, "CE"), (p_snap, c_snap, "PE")]:
                    X = build_features(target, opp, otype)
                    prob = float(entry_model.predict_proba(X)[0][1])

                    if prob >= CONFIDENCE_THRESHOLD:
                        sl_mult = float(sl_model.predict(X)[0])
                        mfe_mult = float(trail_model.predict(X)[0])
                        open_trade(target, otype, prob, sl_mult, mfe_mult)
                        break

        time.sleep(1.0)

def exit_loop():
    while True:
        if active_trade:
            with lock:
                tr = active_trade
                snap = market_state.get(tr["token"])
            if snap and "ltp" in snap:
                ltp = float(snap["ltp"])
                if ltp > tr["highest"]:
                    tr["highest"] = ltp

                if not tr["locked"]:
                    if ltp >= tr["activation"]:
                        tr["trigger"] = tr["min_profit"]
                        tr["locked"] = True
                        log(f"🔒 Profit Locked on {tr['symbol']} | New SL: {tr['trigger']}")
                else:
                    new_sl = round2(tr["highest"] - tr["trail"])
                    if new_sl > tr["trigger"]:
                        tr["trigger"] = new_sl

                if ltp <= tr["trigger"]:
                    close_trade(tr["trigger"], "AI_TRAIL_SL" if tr["locked"] else "AI_INIT_SL")
        time.sleep(0.05)

def main():
    for path in [ENTRY_MODEL_PATH, SL_MODEL_PATH, TRAIL_MODEL_PATH]:
        if not os.path.exists(path):
            print(f"❌ Missing model: {path}. Run train_model.py first.")
            return

    entry_model.load_model(ENTRY_MODEL_PATH)
    sl_model.load_model(SL_MODEL_PATH)
    trail_model.load_model(TRAIL_MODEL_PATH)
    init_csv()

    req_socket.connect(f"tcp://{MASTER_IP}:{MASTER_REP_PORT}")
    log(f"AI Slave Online | Mode: {'LIVE' if LIVE_TRADING else 'SIMULATION'} | Threshold: {CONFIDENCE_THRESHOLD}")

    threading.Thread(target=zmq_subscriber_loop, daemon=True).start()
    threading.Thread(target=exit_loop, daemon=True).start()
    scanner_loop()

if __name__ == "__main__":
    main()