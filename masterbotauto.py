import os, requests

_zero_key = (os.getenv("ZERO_IP_KEY") or "").strip()
if _zero_key:
    class _ZeroIPAdapter(requests.adapters.HTTPAdapter):
        def proxy_headers(self, proxy):
            h = super().proxy_headers(proxy)
            h["Proxy-Authorization"] = f"Bearer {_zero_key}"
            return h

    _orig_init = requests.Session.__init__
    def _patched_init(self, *args, **kwargs):
        _orig_init(self, *args, **kwargs)
        self.mount("https://trade.shoonya.com", _ZeroIPAdapter())
        self.proxies["https://trade.shoonya.com"] = "https://zeroip.snipy.in:8443"
    requests.Session.__init__ = _patched_init

    _orig_request = requests.Session.request
    def _patched_request(self, method, url, *args, **kwargs):
        if isinstance(url, str):
            url = url.replace("https://api.shoonya.com", "https://trade.shoonya.com")
        return _orig_request(self, method, url, *args, **kwargs)
    requests.Session.request = _patched_request
import yaml
import time
import threading
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import zmq
from collections import deque
from api_helper import NorenApiPy

# ============================================================
# CONFIGURATION
# ============================================================

CRED_PATH = "cred.yml"

# Indicators
RSI_LEN = 5
EMA_FAST = 9
EMA_SLOW = 20
ATR_LEN = 14
MIN_BARS = 1

# Networking (ZeroMQ)
PUB_PORT = 5555
REP_PORT = 5556

# Engine Delays
STATE_LOOP_DELAY = 1.0     # How often to compute and publish state
HEARTBEAT_DELAY  = 5.0     # Internal heartbeat interval

# Nifty Spot Constants
SPOT_TOKEN = "26000"
SPOT_EXCH = "NSE"

# ============================================================
# GLOBALS & SYSTEM STATE
# ============================================================

api = None
socket_opened = False
bot_start_time = time.time()

# ZeroMQ Context
zmq_context = zmq.Context()
pub_socket = zmq_context.socket(zmq.PUB)
rep_socket = zmq_context.socket(zmq.REP)
zmq_pub_lock = threading.Lock()  # Protects thread-safe publishing on pub_socket

symbols = []

# Canonical State Maps
market_state = {}      # Master source of truth for all Options
market_state_spot = {} # Master source of truth for NIFTY Spot

# Live Data & Bucket Globals (Thread-Safe with lock)
lock = threading.Lock()
live_ltp = {}
feed_alive_ts = {}
candle_history = {}
current_candle = {}
current_bucket = {}
last_ft_seen = {}

# Spot Data Globals (Isolated)
spot_lock = threading.Lock()
spot_candle_history = deque(maxlen=10000)
spot_current_candle = {}
spot_current_bucket = None
spot_last_ft_seen = None
spot_live_ltp = None
spot_feed_alive_ts = None
spot_daily_open = None

# Internal Heartbeat State
heartbeat_state = {}

# Master Order Registry
orders = {}
orders_lock = threading.Lock()

# ============================================================
# UTILITIES
# ============================================================

def now():
    return datetime.now()

def tstr():
    return now().strftime("%Y-%m-%d %H:%M:%S")

def round2(x):
    return round(float(x), 2) if x is not None and not np.isnan(x) else 0.0

def log(msg):
    print(f"[{tstr()}] {msg}")

# ============================================================
# INDICATOR MATH
# ============================================================

def rsi(series, period=5):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(0)

def atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# ============================================================
# SHOONYA CONNECTION
# ============================================================

def connect():
    global api
    api = NorenApiPy()
    with open(CRED_PATH) as f:
        cred = yaml.load(f, Loader=yaml.FullLoader)
    api.injectOAuthHeader(cred["Access_token"], cred["UID"], cred["Account_ID"])
    try:
        api.set_credentials(cred["Access_token"], cred["UID"], cred["Account_ID"])
    except:
        pass
    log("CONNECTED TO SHOONYA API")

def get_dynamic_watchlist():
    """Calculates ATM from live Spot and fetches +/- 8 strikes for the nearest expiry."""
    global spot_live_ltp
    
    # Wait up to 3 seconds for live WebSocket tick
    for _ in range(3):
        if spot_live_ltp is not None:
            break
        log("Waiting for Spot LTP to calculate ATM...")
        time.sleep(1)

    # Fallback to REST API (works on weekends/after-hours)
    if spot_live_ltp is None:
        q = api.get_quotes(exchange=SPOT_EXCH, token=SPOT_TOKEN)
        if q and "lp" in q:
            spot_live_ltp = float(q["lp"])
            log(f"Market closed/no ticks. Using REST Spot LTP: {spot_live_ltp}")
        else:
            log("FAILED TO GET SPOT LTP VIA REST.")
            return []
        
    atm_strike = int(round(spot_live_ltp / 50.0) * 50)
    log(f"Spot: {spot_live_ltp} -> ATM Strike: {atm_strike}")
    
    try:
        # STEP 1: Search for an actual NFO contract to get a valid expiry anchor
        search_res = api.searchscrip(exchange="NFO", searchtext=f"NIFTY {atm_strike} CE")
        
        if not search_res or search_res.get('stat') != 'Ok' or not search_res.get('values'):
            log("FAILED TO FIND BASE CONTRACT VIA SEARCHSCRIP.")
            return []
            
        base_tsym = search_res['values'][0]['tsym']
        
        # STEP 2: Feed the exact symbol into the option chain
        res = api.get_option_chain(
            exchange="NFO",
            tradingsymbol=base_tsym,
            strikeprice=str(atm_strike),
            count="8"
        )
        
        if not res or 'values' not in res:
            log("FAILED TO FETCH OPTION CHAIN.")
            return []
            
        formatted_symbols = []
        for item in res['values']:
            formatted_symbols.append({
                "exch": item["exch"],
                "token": item["token"],
                "tsym": item["tsym"],
                "opt_type": "C" if item["tsym"].endswith("CE") else "P"
            })
            
        return formatted_symbols
    except Exception as e:
        log(f"OPTION CHAIN ERROR: {e}")
        return []

# ============================================================
# HISTORICAL SEEDING
# ============================================================

def fetch_5m(sym):
    try:
        exch = sym["exch"]
        token = sym["token"]
        
        # Look back 5 days to guarantee enough candles even at market open
        st = (now() - timedelta(days=5)).replace(hour=9, minute=15, second=0, microsecond=0).timestamp()
        et = time.time()
        
        ret = api.get_time_price_series(
            exchange=exch, token=token,
            starttime=str(int(st)), endtime=str(int(et)), interval="5"
        )
        if not ret: return None

        rows = []
        for r in ret:
            rows.append({
                "time": r["time"],
                "open": float(r["into"]),
                "high": float(r["inth"]),
                "low": float(r["intl"]),
                "close": float(r["intc"]),
                "volume": float(r["v"]) if "v" in r else 0.0
            })
        df = pd.DataFrame(rows)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except:
        return None

def seed_history(sym):
    token = sym["token"]
    df = fetch_5m(sym)
    if df is None or len(df) < MIN_BARS:
        log(f"HISTORY SEEDING FAILED FOR {sym['tsym']}")
        return

    dq = deque(maxlen=10000)
    for _, r in df.iterrows():
        try:
            dt = datetime.strptime(r["time"], "%d-%m-%Y %H:%M:%S")
            ft = int(dt.timestamp())
            bucket = ft // 300
            dq.append({
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
                "start_ft": bucket * 300
            })
        except:
            continue

    with lock:
        candle_history[token] = dq
    log(f"Seeded {len(dq)} historical candles for {sym['tsym']}")

def seed_spot_history():
    global spot_daily_open, spot_candle_history
    log("SEEDING NIFTY SPOT HISTORICAL DATA...")
    
    spot_sym = {"exch": SPOT_EXCH, "token": SPOT_TOKEN}
    df = fetch_5m(spot_sym)
    if df is None or len(df) < MIN_BARS:
        log("SPOT HISTORY SEEDING FAILED")
        return

    try:
        spot_daily_open = float(df.iloc[0]["open"])
    except:
        spot_daily_open = None

    dq = deque(maxlen=10000)
    for _, r in df.iterrows():
        try:
            dt = datetime.strptime(r["time"], "%d-%m-%Y %H:%M:%S")
            ft = int(dt.timestamp())
            bucket = ft // 300
            dq.append({
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "start_ft": bucket * 300
            })
        except:
            continue

    with spot_lock:
        spot_candle_history = dq
    log(f"SPOT SEED COMPLETE. Daily Open reference set to: {spot_daily_open}")

# ============================================================
# WEBSOCKET HANDLING
# ============================================================

def order_update(msg):
    """Intercepts and broadcasts real-time order updates via ZMQ PUB socket."""
    try:
        payload = json.dumps(msg).encode('utf-8')
        with zmq_pub_lock:
            pub_socket.send_multipart([b"ORDER_UPDATE", payload])
    except Exception as e:
        log(f"ORDER UPDATE PUB ERROR: {e}")

def quote_update(msg):
    global live_ltp, feed_alive_ts
    global spot_live_ltp, spot_feed_alive_ts, spot_current_bucket, spot_last_ft_seen, spot_current_candle

    try:
        tk = msg.get("tk")
        if not tk or "lp" not in msg or "ft" not in msg:
            return

        px = float(msg["lp"])
        ft = int(msg["ft"])

        # ==========================================
        # SPOT PROCESSING PIPELINE
        # ==========================================
        if tk == SPOT_TOKEN:
            with spot_lock:
                if spot_last_ft_seen is not None and ft < spot_last_ft_seen:
                    return
                spot_last_ft_seen = ft
                spot_live_ltp = px
                spot_feed_alive_ts = time.time()

                bucket = ft // 300
                if spot_current_bucket is None or bucket != spot_current_bucket:
                    if spot_current_candle:
                        spot_candle_history.append(spot_current_candle.copy())
                    
                    spot_current_candle = {
                        "open": px, "high": px, "low": px, "close": px, "start_ft": bucket * 300
                    }
                    spot_current_bucket = bucket
                else:
                    spot_current_candle["high"] = max(spot_current_candle["high"], px)
                    spot_current_candle["low"] = min(spot_current_candle["low"], px)
                    spot_current_candle["close"] = px
            return

        # ==========================================
        # OPTIONS PROCESSING PIPELINE
        # ==========================================
        with lock:
            last_ft = last_ft_seen.get(tk)
            if last_ft is not None and ft < last_ft:
                return
            last_ft_seen[tk] = ft

            live_ltp[tk] = px
            feed_alive_ts[tk] = time.time()
            
            bucket = ft // 300
            prev_bucket = current_bucket.get(tk)

            if prev_bucket is None or bucket != prev_bucket:
                if tk in current_candle:
                    closed = current_candle[tk]
                    if tk not in candle_history:
                        candle_history[tk] = deque(maxlen=10000)
                    candle_history[tk].append(closed)

                current_candle[tk] = {
                    "open": px, "high": px, "low": px, "close": px, 
                    "volume": 0.0, "start_ft": bucket * 300
                }
                current_bucket[tk] = bucket
            else:
                c = current_candle[tk]
                c["high"] = max(c["high"], px)
                c["low"] = min(c["low"], px)
                c["close"] = px

    except:
        pass

def socket_open():
    global socket_opened
    socket_opened = True
    log("WEBSOCKET CONNECTED")

def start_ws():
    global socket_opened
    socket_opened = False
    try:
        api.start_websocket(order_update_callback=order_update, subscribe_callback=quote_update, socket_open_callback=socket_open)
    except Exception as e:
        log(f"WS START ERROR: {e}")
        return False
    
    wait_start = time.time()
    while not socket_opened:
        if time.time() - wait_start > 10:
            log("WS OPEN TIMEOUT")
            return False
        time.sleep(0.1)
    return True

def reconnect_ws():
    global socket_opened
    log("RECONNECTING WEBSOCKET...")
    socket_opened = False
    try:
        subs = [f"{s['exch']}|{s['token']}" for s in symbols] + [f"{SPOT_EXCH}|{SPOT_TOKEN}"]
        if not start_ws():
            log("RECONNECT FAILED")
            return False
        api.subscribe(subs, feed_type='d')
        log("RE-SUBSCRIBED AFTER RECONNECT")
        return True
    except Exception as e:
        log(f"RECONNECT ERROR: {e}")
        return False

# ============================================================
# DATA BUILDERS
# ============================================================

def build_dataframe(token):
    with lock:
        if token not in candle_history or token not in current_candle:
            return None
        hist = list(candle_history[token])
        live = current_candle[token].copy()

    if len(hist) < MIN_BARS: return None

    df = pd.DataFrame(hist)
    df = pd.concat([df, pd.DataFrame([live])], ignore_index=True)

    df["rsi"] = rsi(df["close"], RSI_LEN)
    df["ema9"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema20"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["atr"] = atr(df, ATR_LEN)

    tp = (df["high"] + df["low"] + df["close"]) / 3
    df["pv"] = tp * df["volume"]
    df["cum_pv"] = df["pv"].cumsum()
    df["cum_v"] = df["volume"].cumsum()
    df["vwap"] = df["cum_pv"] / df["cum_v"].replace(0, np.nan)
    df["vwap"] = df["vwap"].ffill().fillna(df["close"])

    return df

def build_spot_dataframe():
    with spot_lock:
        if len(spot_candle_history) < MIN_BARS or spot_live_ltp is None:
            return None
        hist = list(spot_candle_history)
        live = spot_current_candle.copy() if spot_current_candle else {
            "open": spot_live_ltp, "high": spot_live_ltp, "low": spot_live_ltp, "close": spot_live_ltp, "start_ft": int(time.time() // 300)*300
        }

    df = pd.DataFrame(hist)
    df = pd.concat([df, pd.DataFrame([live])], ignore_index=True)
    close = df["close"]
    
    df["ema9"] = close.ewm(span=9, adjust=False).mean()
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    
    def calc_rsi(series, period):
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return (100 - (100 / (1 + rs))).fillna(0)

    df["rsi5"] = calc_rsi(close, 5)
    df["rsi14"] = calc_rsi(close, 14)

    return df

# ============================================================
# STATE MANAGEMENT & PUB MODULE
# ============================================================

def setup_zmq():
    pub_socket.bind(f"tcp://*:{PUB_PORT}")
    rep_socket.bind(f"tcp://*:{REP_PORT}")
    log(f"ZMQ BIND -> PUB: {PUB_PORT}, REP: {REP_PORT}")

def publish_market_state():
    """Builds technical dataframes, updates canonical state, and publishes to ZeroMQ."""
    while True:
        try:
            # 1. Update and Publish Options
            for sym in symbols:
                token = sym["token"]
                
                with lock:
                    ltp = live_ltp.get(token)
                    feed_ts = feed_alive_ts.get(token)
                    c_candle = current_candle.get(token, {})
                    p_candle = candle_history[token][-1] if token in candle_history and len(candle_history[token]) > 0 else {}
                
                if ltp is None: continue
                
                feed_age = round2(time.time() - feed_ts) if feed_ts else 999.0
                status = "LIVE" if feed_age < 15 else "STALE"

                df = build_dataframe(token)
                
                if df is not None and len(df) >= 3:
                    live_row = df.iloc[-1]
                    prev_row = df.iloc[-2]
                    
                    market_state[token] = {
                        "type": "SNAPSHOT",
                        "token": token,
                        "symbol": sym["tsym"],
                        "exchange": sym["exch"],
                        "ltp": ltp,
                        "feed_time": feed_ts,
                        "server_time": time.time(),
                        "prev_rsi": round2(prev_row["rsi"]),
                        "live_rsi": round2(live_row["rsi"]),
                        "ema9": round2(live_row["ema9"]),
                        "ema20": round2(live_row["ema20"]),
                        "atr": round2(live_row["atr"]),
                        "vwap": round2(live_row["vwap"]),
                        "volume": round2(live_row["volume"]),
                        "current_candle": c_candle,
                        "previous_candle": p_candle,
                        "feed_age": feed_age,
                        "status": status
                    }
                    
                    with zmq_pub_lock:
                        pub_socket.send_multipart([b"SNAPSHOT", json.dumps(market_state[token]).encode('utf-8')])

            # 2. Update and Publish Spot
            df_spot = build_spot_dataframe()
            if df_spot is not None and len(df_spot) >= 2:
                with spot_lock:
                    spot_ltp = spot_live_ltp
                    spot_ts = spot_feed_alive_ts
                    s_c_candle = spot_current_candle.copy()
                    s_p_candle = spot_candle_history[-1] if len(spot_candle_history) > 0 else {}
                
                live_row = df_spot.iloc[-1]
                prev_row = df_spot.iloc[-2]
                feed_age = round2(time.time() - spot_ts) if spot_ts else 999.0
                
                market_state_spot[SPOT_TOKEN] = {
                    "type": "SPOT_SNAPSHOT",
                    "token": SPOT_TOKEN,
                    "symbol": "NIFTY",
                    "exchange": SPOT_EXCH,
                    "ltp": spot_ltp,
                    "feed_time": spot_ts,
                    "server_time": time.time(),
                    "prev_rsi5": round2(prev_row["rsi5"]),
                    "live_rsi5": round2(live_row["rsi5"]),
                    "live_rsi14": round2(live_row["rsi14"]),
                    "ema9": round2(live_row["ema9"]),
                    "ema20": round2(live_row["ema20"]),
                    "current_candle": s_c_candle,
                    "previous_candle": s_p_candle,
                    "feed_age": feed_age,
                    "status": "LIVE" if feed_age < 15 else "STALE"
                }

                with zmq_pub_lock:
                    pub_socket.send_multipart([b"SPOT_SNAPSHOT", json.dumps(market_state_spot[SPOT_TOKEN]).encode('utf-8')])

        except Exception as e:
            log(f"PUB LOOP ERROR: {e}")

        time.sleep(STATE_LOOP_DELAY)

# ============================================================
# ZMQ REP SERVER
# ============================================================

def rep_server_loop():
    """Serves synchronous requests from client bots."""
    while True:
        try:
            req = rep_socket.recv_json()
            cmd = req.get("cmd")
            
            if cmd == "GET_STATUS":
                uptime = int(time.time() - bot_start_time)
                resp = {
                    "connected": socket_opened,
                    "uptime": uptime,
                    "symbols_count": len(symbols)
                }
                rep_socket.send_json(resp)
            
            elif cmd == "GET_SYMBOLS":
                rep_socket.send_json({"symbols": symbols})
            
            elif cmd == "GET_HISTORY":
                tk = req.get("token")
                with lock:
                    if tk in candle_history:
                        hist_list = list(candle_history[tk])
                        rep_socket.send_json({"token": tk, "history": hist_list})
                    else:
                        rep_socket.send_json({"error": "No history found or invalid token"})

            elif cmd == "GET_SPOT_HISTORY":
                with spot_lock:
                    if len(spot_candle_history) > 0:
                        hist_list = list(spot_candle_history)
                        rep_socket.send_json({"token": SPOT_TOKEN, "history": hist_list})
                    else:
                        rep_socket.send_json({"error": "No spot history found"})
                        
            elif cmd == "GET_SNAPSHOT":
                tk = req.get("token")
                if tk in market_state:
                    rep_socket.send_json(market_state[tk])
                else:
                    rep_socket.send_json({"error": "No snapshot available"})
            
            # ----------------------------------------------------
            # ORDER ROUTING EXTENSIONS
            # ----------------------------------------------------
            elif cmd == "PLACE_ORDER":
                try:
                    side = req.get("side", "BUY")
                    buy_sell = 'B' if side.upper().startswith('B') else 'S'
                    exchange = req.get("exchange", "NFO")
                    symbol = req.get("symbol")
                    qty = req.get("qty")
                    price_type = req.get("price_type", "LMT")
                    price = req.get("price", 0.0)
                    trigger_price = req.get("trigger_price", None)
                    token = req.get("token", "")

                    resp = api.place_order(
                        buy_or_sell=buy_sell,
                        product_type='I',
                        exchange=exchange,
                        tradingsymbol=symbol,
                        quantity=qty,
                        discloseqty=0,
                        price_type=price_type,
                        price=price,
                        trigger_price=trigger_price,
                        retention='DAY',
                        remarks='BOT_MASTER'
                    )
                    
                    if resp and resp.get("stat") == "Ok":
                        orderno = resp.get("norenordno")
                        with orders_lock:
                            orders[orderno] = {
                                "symbol": symbol,
                                "token": token,
                                "qty": qty,
                                "side": side,
                                "exchange": exchange,
                                "timestamp": time.time(),
                                "status": "PLACED"
                            }
                        rep_socket.send_json({"status": "ok", "orderno": orderno})
                    else:
                        msg = resp.get("emsg", "Unknown API error") if resp else "Shoonya API returned None"
                        rep_socket.send_json({"status": "error", "message": msg})
                except Exception as e:
                    rep_socket.send_json({"status": "error", "message": str(e)})

            elif cmd == "MODIFY_ORDER":
                try:
                    orderno = req.get("orderno")
                    new_trigger = req.get("new_trigger")
                    new_price = req.get("new_price")
                    
                    with orders_lock:
                        order_info = orders.get(orderno, {})
                    
                    exchange = req.get("exchange", order_info.get("exchange", "NFO"))
                    symbol = req.get("symbol", order_info.get("symbol"))
                    qty = req.get("qty", order_info.get("qty"))
                    price_type = req.get("price_type", "SL-LMT" if new_trigger else "LMT")
                    
                    if not symbol or not qty:
                        rep_socket.send_json({"status": "error", "message": "Missing symbol or qty, and order not found in master registry."})
                    else:
                        resp = api.modify_order(
                            orderno=orderno,
                            exchange=exchange,
                            tradingsymbol=symbol,
                            newquantity=qty,
                            newprice_type=price_type,
                            newprice=new_price,
                            newtrigger_price=new_trigger
                        )
                        
                        if resp and resp.get("stat") == "Ok":
                            rep_socket.send_json({"status": "ok", "orderno": orderno})
                        else:
                            msg = resp.get("emsg", "Unknown API error") if resp else "Shoonya API returned None"
                            rep_socket.send_json({"status": "error", "message": msg})
                except Exception as e:
                    rep_socket.send_json({"status": "error", "message": str(e)})

            elif cmd == "CANCEL_ORDER":
                try:
                    orderno = req.get("orderno")
                    resp = api.cancel_order(orderno=orderno)
                    
                    if resp and resp.get("stat") == "Ok":
                        rep_socket.send_json({"status": "ok", "orderno": orderno})
                    else:
                        msg = resp.get("emsg", "Unknown API error") if resp else "Shoonya API returned None"
                        rep_socket.send_json({"status": "error", "message": msg})
                except Exception as e:
                    rep_socket.send_json({"status": "error", "message": str(e)})
                    
            else:
                rep_socket.send_json({"error": "UNKNOWN_COMMAND"})
                
        except Exception as e:
            log(f"REP LOOP ERROR: {e}")
            try:
                rep_socket.send_json({"error": "Internal server error"})
            except:
                pass

# ============================================================
# HEARTBEAT MANAGER
# ============================================================

def heartbeat_loop():
    """Maintains an internal heartbeat for overall system health."""
    global heartbeat_state
    while True:
        heartbeat_state = {
            "master": "alive",
            "uptime_seconds": int(time.time() - bot_start_time),
            "symbols_monitored": len(symbols),
            "timestamp": time.time(),
            "ws_connected": socket_opened
        }
        time.sleep(HEARTBEAT_DELAY)

# ============================================================
# MAIN
# ============================================================

def main():
    global symbols

    if os.path.exists("master_ready.flag"):
        os.remove("master_ready.flag")

    log("INITIALIZING MARKET DATA MASTER...")
    setup_zmq()
    connect()

    # 1. Start WebSocket and subscribe to Spot FIRST so ATM can be calculated
    start_ws()
    api.subscribe([f"{SPOT_EXCH}|{SPOT_TOKEN}"], feed_type='d')

    # 2. Fetch dynamic watchlist using live Spot LTP
    symbols = get_dynamic_watchlist()

    # 3. Seed historical candles
    for s in symbols:
        seed_history(s)
    
    seed_spot_history()

    # 4. Subscribe to Options feed
    subs = [f"{s['exch']}|{s['token']}" for s in symbols]
    if subs:
        api.subscribe(subs, feed_type='d')
    log("WS SUBSCRIBED TO WATCHLIST AND NIFTY SPOT")

    threading.Thread(target=publish_market_state, daemon=True).start()
    threading.Thread(target=rep_server_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    log("MASTER SERVER RUNNING. PRESS CTRL+C TO EXIT.")

    # Signal START.bat that seeding is complete
    with open("master_ready.flag", "w") as f:
        f.write("ready")

    while True:
        if now().hour > 15 or (now().hour == 15 and now().minute >= 30):
            log("Market closed. Shutting down.")
            break

        stale_count = 0
        total_count = len(symbols)

        with lock:
            for s in symbols:
                tk = s["token"]
                if tk not in feed_alive_ts:
                    stale_count += 1
                    continue
                if (time.time() - feed_alive_ts[tk]) > 15:
                    stale_count += 1

        if total_count == 0:
            log("WATCHLIST IS EMPTY. RE-FETCHING AND RECONNECTING...")
            new_symbols = get_dynamic_watchlist()
            if new_symbols:
                symbols.clear()
                symbols.extend(new_symbols)
                for s in symbols:
                    seed_history(s)
                log(f"Successfully recovered {len(symbols)} symbols.")
            
            reconnect_ws()
            time.sleep(5)
            continue
        
        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Dung dun dung dung.")
