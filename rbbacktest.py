import csv
import os
import re
import zmq
import pandas as pd
import numpy as np
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
LOT_SIZE = 65
TARGET_PREMIUM = 100.0
ATR_LEN = 14
LEGS = ["soft", "hard", "lcl"]

LOG_FOLDER = "trade_logs"
BACKTEST_CSV = os.path.join(LOG_FOLDER, "orb_920_backtest.csv")

# Timings
ORB_MARKTIME = "09:20"
ENTRY_END = "15:00"
FORCE_EXIT = "15:20"

# ZeroMQ Master Broker Addresses
MASTER_IP = "127.0.0.1"
MASTER_REP_PORT = 5556

# ============================================================
# HELPERS & INDICATORS
# ============================================================
def round2(x):
    return round(float(x), 2) if x is not None and not np.isnan(x) else 0.0

def compute_atr(df, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean().fillna(15.0)

def send_master_request(req_socket, payload):
    try:
        req_socket.send_json(payload)
        if req_socket.poll(5000):
            return req_socket.recv_json()
    except Exception as e:
        print(f"ZMQ Request Error: {e}")
    return None

def init_csv():
    os.makedirs(LOG_FOLDER, exist_ok=True)
    if not os.path.exists(BACKTEST_CSV) or os.path.getsize(BACKTEST_CSV) == 0:
        leg_cols = [f"{l}_{f}" for l in LEGS for f in ["entry", "exit", "reason", "gross", "net", "duration"]]
        with open(BACKTEST_CSV, "w", newline="") as f:
            csv.writer(f).writerow([
                "date", "entry_time", "strategy", "symbol", "token", "qty",
                "spot_high", "spot_low", "opening_gap", *leg_cols
            ])

def get_bar_ticks(bar):
    """Simulates realistic intra-candle price path from OHLC."""
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    if c >= o:
        return [o, l, h, c]  # Bullish candle path
    else:
        return [o, h, l, c]  # Bearish candle path

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

# ============================================================
# BACKTEST ENGINE
# ============================================================
def fetch_all_history():
    ctx = zmq.Context()
    req_socket = ctx.socket(zmq.REQ)
    req_socket.connect(f"tcp://{MASTER_IP}:{MASTER_REP_PORT}")

    print("Connecting to masterbotauto to pull seeded history...")
    sym_resp = send_master_request(req_socket, {"cmd": "GET_SYMBOLS"})
    if not sym_resp or "symbols" not in sym_resp:
        print("❌ Failed to fetch symbols. Ensure masterbotauto.py is running.")
        return None, None

    symbols = sym_resp["symbols"]
    spot_resp = send_master_request(req_socket, {"cmd": "GET_SPOT_HISTORY"})
    if not spot_resp or "history" not in spot_resp:
        print("❌ Failed to fetch NIFTY Spot history.")
        return None, None

    spot_df = pd.DataFrame(spot_resp["history"])
    spot_df["dt"] = pd.to_datetime(spot_df["start_ft"], unit="s") + pd.Timedelta(hours=5, minutes=30)
    spot_df["date"] = spot_df["dt"].dt.strftime("%Y-%m-%d")
    spot_df["hm"] = spot_df["dt"].dt.strftime("%H:%M")
    spot_df = spot_df.sort_values("start_ft").reset_index(drop=True)

    options_data = {}
    for s in symbols:
        tk = s["token"]
        tsym = s["tsym"]
        hist_resp = send_master_request(req_socket, {"cmd": "GET_HISTORY", "token": tk})
        if hist_resp and "history" in hist_resp and len(hist_resp["history"]) > 0:
            df = pd.DataFrame(hist_resp["history"])
            df["dt"] = pd.to_datetime(df["start_ft"], unit="s") + pd.Timedelta(hours=5, minutes=30)
            df["date"] = df["dt"].dt.strftime("%Y-%m-%d")
            df["hm"] = df["dt"].dt.strftime("%H:%M")
            df = df.sort_values("start_ft").reset_index(drop=True)
            df["atr"] = compute_atr(df, ATR_LEN)
            df["prev_low"] = df["low"].shift(1)
            df["prev_start_ft"] = df["start_ft"].shift(1)
            options_data[tk] = {"symbol": tsym, "df": df}

    print(f"Loaded {len(spot_df)} spot bars and {len(options_data)} option histories.")
    return spot_df, options_data

def select_contracts_for_day(options_data, date_str):
    """Selects CE and PE contracts closest to TARGET_PREMIUM at the 09:15 bar close."""
    ce_candidates, pe_candidates = [], []

    for tk, data in options_data.items():
        df = data["df"]
        sym = data["symbol"]
        bar_915 = df[(df["date"] == date_str) & (df["hm"] == "09:15")]
        if bar_915.empty:
            continue

        ltp = float(bar_915.iloc[0]["close"])
        diff = abs(ltp - TARGET_PREMIUM)

        match = re.search(r'([CP])\d+$', sym)
        if match:
            if match.group(1) == "C":
                ce_candidates.append((tk, diff, ltp))
            elif match.group(1) == "P":
                pe_candidates.append((tk, diff, ltp))

    if not ce_candidates or not pe_candidates:
        return None, None

    best_ce = min(ce_candidates, key=lambda x: x[1])[0]
    best_pe = min(pe_candidates, key=lambda x: x[1])[0]
    return best_ce, best_pe

def run_backtest():
    spot_df, options_data = fetch_all_history()
    if spot_df is None or not options_data:
        return

    init_csv()
    unique_dates = sorted(spot_df["date"].unique())
    print(f"\nRunning 9:20 ORB Backtest across {len(unique_dates)} historical days...\n")

    total_trades = 0
    for date_str in unique_dates:
        day_spot = spot_df[spot_df["date"] == date_str].reset_index(drop=True)
        bar_915 = day_spot[day_spot["hm"] == "09:15"]

        if bar_915.empty:
            print(f"[{date_str}] Skipping: No 09:15 candle found.")
            continue

        # 1. Lock ORB Range & Gap
        row_915 = bar_915.iloc[0]
        orb_high = float(row_915["high"])
        orb_low = float(row_915["low"])

        global_idx = spot_df[spot_df["start_ft"] == row_915["start_ft"]].index[0]
        if global_idx > 0:
            prev_close = float(spot_df.iloc[global_idx - 1]["close"])
            spot_gap = round2(float(row_915["open"]) - prev_close)
        else:
            spot_gap = 0.0

        # 2. Select Options Contracts at 09:20
        ce_tk, pe_tk = select_contracts_for_day(options_data, date_str)
        if not ce_tk or not pe_tk:
            print(f"[{date_str}] Skipping: Insufficient option data at 09:15.")
            continue

        print(f"[{date_str}] ORB Locked -> High: {orb_high} | Low: {orb_low} | Gap: {spot_gap} | CE: {options_data[ce_tk]['symbol']} | PE: {options_data[pe_tk]['symbol']}")

        # 3. Walk Forward Through Intraday Bars (09:20 onwards)
        intraday_spot = day_spot[day_spot["hm"] >= ORB_MARKTIME]
        active_trade = None
        trade_done_today = False

        for _, s_bar in intraday_spot.iterrows():
            hm = s_bar["hm"]
            s_ft = s_bar["start_ft"]
            bar_dt_str = s_bar["dt"].strftime("%Y-%m-%d %H:%M:%S")

            # Force EOD Exit at 15:20
            if hm >= FORCE_EXIT:
                if active_trade:
                    opt_df = options_data[active_trade["token"]]["df"]
                    opt_bar = opt_df[opt_df["start_ft"] == s_ft]
                    exit_px = float(opt_bar.iloc[0]["open"]) if not opt_bar.empty else active_trade["last_ltp"]
                    finalize_open_legs(active_trade, "EOD_FORCE", exit_px, bar_dt_str)
                    log_trade_to_csv(date_str, orb_high, orb_low, spot_gap, active_trade)
                    total_trades += 1
                    active_trade = None
                break

            # A. Check for Breakout Entry if no trade taken yet
            if not trade_done_today and hm <= ENTRY_END:
                for tick_px in get_bar_ticks(s_bar):
                    target_tk = None
                    side = None
                    if tick_px > orb_high:
                        target_tk, side = ce_tk, "CALL"
                    elif tick_px < orb_low:
                        target_tk, side = pe_tk, "PUT"

                    if target_tk:
                        opt_df = options_data[target_tk]["df"]
                        opt_bar_match = opt_df[opt_df["start_ft"] == s_ft]
                        if opt_bar_match.empty:
                            break
                        
                        opt_bar = opt_bar_match.iloc[0]
                        entry_px = float(opt_bar["open"])
                        atr_val = float(opt_bar["atr"]) if opt_bar["atr"] > 0 else 15.0
                        soft_atr = atr_val                # Uncapped ATR
                        hard_atr = min(atr_val, 10.0)     # Hard-capped ATR at 10.0
                        prev_low = round2(opt_bar["prev_low"]) if not np.isnan(opt_bar["prev_low"]) else round2(entry_px - 10)
                        prev_ft = int(opt_bar["prev_start_ft"]) if not np.isnan(opt_bar["prev_start_ft"]) else 0

                        active_trade = {
                            "symbol": options_data[target_tk]["symbol"],
                            "token": target_tk,
                            "side": side,
                            "entry_time": bar_dt_str,
                            "qty": LOT_SIZE,
                            "entry": entry_px,
                            "last_ltp": entry_px,
                            "soft": _build_atr_leg(entry_px, soft_atr),
                            "hard": _build_atr_leg(entry_px, hard_atr),
                            "lcl": {
                                "active": True,
                                "highest": entry_px,
                                "entry": entry_px,
                                "trigger": prev_low,
                                "last_completed_ft": prev_ft,
                                "exit_price": "", "exit_reason": "", "exit_time": "", "gross": "", "net": "", "duration": ""
                            }
                        }
                        trade_done_today = True
                        print(f"  🚀 ENTRY ({hm}): {active_trade['symbol']} @ {entry_px} (Spot {tick_px})")
                        break

            # B. Manage Active Trade on Current 5-Min Option Bar
            if active_trade:
                opt_df = options_data[active_trade["token"]]["df"]
                opt_bar_match = opt_df[opt_df["start_ft"] == s_ft]
                if opt_bar_match.empty:
                    continue

                opt_bar = opt_bar_match.iloc[0]
                prev_ft = int(opt_bar["prev_start_ft"]) if not np.isnan(opt_bar["prev_start_ft"]) else 0
                prev_low = round2(opt_bar["prev_low"]) if not np.isnan(opt_bar["prev_low"]) else 0.0

                # Update LCL trigger upon new completed candle (1:1 with live engine)
                c_lcl = active_trade["lcl"]
                if c_lcl["active"] and prev_ft > c_lcl["last_completed_ft"]:
                    if prev_low > c_lcl["trigger"]:
                        c_lcl["trigger"] = prev_low
                    c_lcl["last_completed_ft"] = prev_ft

                # Simulate intra-bar price ticks for Trailing Stop Losses
                for opt_tick in get_bar_ticks(opt_bar):
                    active_trade["last_ltp"] = opt_tick

                    # 1. SOFT & HARD ATR TRAIL MODULES
                    for leg in ["soft", "hard"]:
                        c = active_trade[leg]
                        if c["active"]:
                            if opt_tick > c["highest"]:
                                c["highest"] = opt_tick
                            if not c["locked"]:
                                if opt_tick >= c["activation"]:
                                    c["trigger"] = c["min_profit"]
                                    c["locked"] = True
                            else:
                                nt = round2(c["highest"] - c["trail"])
                                if nt > c["trigger"]:
                                    c["trigger"] = nt

                            if opt_tick <= c["trigger"]:
                                finish_leg(active_trade, leg, f"{leg.upper()}_SL_HIT", c["trigger"], bar_dt_str)

                    # 2. LCL TRAIL MODULE
                    if c_lcl["active"]:
                        if opt_tick > c_lcl["highest"]:
                            c_lcl["highest"] = opt_tick
                        if opt_tick <= c_lcl["trigger"]:
                            finish_leg(active_trade, "lcl", "LCL_SL_HIT", c_lcl["trigger"], bar_dt_str)

                    if all(not active_trade[l]["active"] for l in LEGS):
                        break

                # If all legs closed, write to CSV
                if all(not active_trade[l]["active"] for l in LEGS):
                    log_trade_to_csv(date_str, orb_high, orb_low, spot_gap, active_trade)
                    total_trades += 1
                    active_trade = None

        # Cleanup if data ends before 15:20 on the latest day
        if active_trade:
            last_dt_str = intraday_spot.iloc[-1]["dt"].strftime("%Y-%m-%d %H:%M:%S")
            finalize_open_legs(active_trade, "DATA_END_FORCE", active_trade["last_ltp"], last_dt_str)
            log_trade_to_csv(date_str, orb_high, orb_low, spot_gap, active_trade)
            total_trades += 1

    print(f"\n✅ Backtest Complete. {total_trades} trades logged to {BACKTEST_CSV}")
    print_summary()

def finish_leg(tr, leg, reason, px, exit_time_str):
    c = tr[leg]
    if not c["active"]:
        return
    c["active"] = False
    c["exit_price"] = round2(px)
    c["exit_reason"] = reason
    c["exit_time"] = exit_time_str
    gross = round2((px - c["entry"]) * tr["qty"])
    c["gross"] = gross
    c["net"] = round2(gross - (10 + (0.0016 * tr["qty"] * (c["entry"] + px))))
    try:
        dt_exit = datetime.strptime(exit_time_str, "%Y-%m-%d %H:%M:%S")
        dt_entry = datetime.strptime(tr["entry_time"], "%Y-%m-%d %H:%M:%S")
        c["duration"] = int((dt_exit - dt_entry).total_seconds())
    except Exception:
        c["duration"] = 0

def finalize_open_legs(tr, reason, px, exit_time_str):
    for l in LEGS:
        finish_leg(tr, l, reason, px, exit_time_str)

def log_trade_to_csv(date_str, orb_high, orb_low, spot_gap, tr):
    leg_data = []
    for l in LEGS:
        leg_data.extend([
            tr[l]["entry"], tr[l]["exit_price"], tr[l]["exit_reason"],
            tr[l]["gross"], tr[l]["net"], tr[l]["duration"]
        ])
    row = [
        date_str, tr["entry_time"], "ORB_920_HIST", tr["symbol"], tr["token"], tr["qty"],
        orb_high, orb_low, spot_gap, *leg_data
    ]
    with open(BACKTEST_CSV, "a", newline="") as f:
        csv.writer(f).writerow(row)
    print(f"  ✨ CLOSED: Soft Net={tr['soft']['net']} ({tr['soft']['exit_reason']}) | Hard Net={tr['hard']['net']} ({tr['hard']['exit_reason']}) | LCL Net={tr['lcl']['net']} ({tr['lcl']['exit_reason']})")

def print_summary():
    if not os.path.exists(BACKTEST_CSV):
        return
    df = pd.read_csv(BACKTEST_CSV)
    if df.empty:
        print("No trades triggered during the historical window.")
        return

    print("\n================ BACKTEST SUMMARY ================")
    for leg in LEGS:
        col = f"{leg}_net"
        if col not in df.columns:
            continue
        nets = pd.to_numeric(df[col], errors="coerce").fillna(0)
        wins = (nets > 0).sum()
        win_rate = (wins / len(nets) * 100) if len(nets) > 0 else 0
        print(f"{leg.upper():<6} | Trades: {len(nets)} | Win Rate: {win_rate:.1f}% | Net PnL: {nets.sum():+.2f}")
    print("==================================================")

if __name__ == "__main__":
    run_backtest()