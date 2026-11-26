import os
import sys
import time
import subprocess
import requests
from datetime import datetime

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID   = str(os.getenv("TG_CHAT_ID", ""))
EOD_TIME     = "1450"  # 2:50 PM IST

BOTS = {
    "master":     {"script": "masterbotauto.py", "quiet": False},
    "tv":         {"script": "TV.py",            "quiet": False},
    "ob":         {"script": "OB.py",            "quiet": False},
    "datalogger": {"script": "datalogger.py",    "quiet": False},
    "poc3":       {"script": "poc3slave.py",     "quiet": True},
    "obslave":    {"script": "obslave.py",       "quiet": True},
    "orb":        {"script": "orb.py",           "quiet": False},
}

procs = {}

# ---------------- PROCESS CONTROLS ----------------
def is_running(name):
    p = procs.get(name)
    return p is not None and p.poll() is None

def start_bot(name):
    if is_running(name):
        return False
    cfg = BOTS[name]
    out = subprocess.DEVNULL if cfg["quiet"] else None
    procs[name] = subprocess.Popen([sys.executable, cfg["script"]], stdout=out)
    return True

def stop_bot(name):
    p = procs.get(name)
    if p is None or p.poll() is not None:
        procs[name] = None
        return False
    p.terminate()
    try:
        p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        p.kill()
    procs[name] = None
    return True

def restart_bot(name):
    stop_bot(name)
    time.sleep(1)
    start_bot(name)

def stop_all():
    for name in reversed(list(BOTS.keys())):
        stop_bot(name)

# ---------------- TELEGRAM HELPERS ----------------
def tg_api(method, payload, timeout=15):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {}
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
        r = requests.post(url, json=payload, proxies={"http": None, "https": None}, timeout=timeout)
        return r.json()
    except Exception:
        return {}

def main_menu():
    buttons, row = [], []
    for name in BOTS:
        icon = "🟢" if is_running(name) else "🔴"
        row.append({"text": f"{icon} {name}", "callback_data": f"bot:{name}"})
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([
        {"text": "🔄 Refresh", "callback_data": "refresh"},
        {"text": "🛑 Shutdown All", "callback_data": "ask_shutdown"},
    ])
    return "<b>🎛️ Bot Control Panel</b>\nSelect a bot:", {"inline_keyboard": buttons}

def bot_menu(name):
    status = "🟢 Running" if is_running(name) else "🔴 Stopped"
    text = f"<b>⚙️ {name}</b> ({BOTS[name]['script']})\nStatus: {status}"
    markup = {"inline_keyboard": [
        [
            {"text": "🔄 Restart", "callback_data": f"act:restart:{name}"},
            {"text": "⏹️ Stop",    "callback_data": f"act:stop:{name}"},
            {"text": "▶️ Start",   "callback_data": f"act:start:{name}"},
        ],
        [{"text": "⬅️ Back", "callback_data": "refresh"}]
    ]}
    return text, markup

def show(text, markup, msg_id=None):
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "reply_markup": markup}
    if msg_id:
        payload["message_id"] = msg_id
        tg_api("editMessageText", payload)
    else:
        tg_api("sendMessage", payload)

# ---------------- MAIN LOOP ----------------
def main():
    if os.path.exists("master_ready.flag"):
        os.remove("master_ready.flag")

    # 1. Sequential startup (matches your workflow)
    start_bot("master")
    while not os.path.exists("master_ready.flag"):
        if datetime.now().strftime("%H%M") >= EOD_TIME:
            return
        time.sleep(2)

    start_bot("tv")
    start_bot("ob")
    time.sleep(10)

    for b in ["datalogger", "poc3", "obslave", "orb"]:
        start_bot(b)

    tg_api("sendMessage", {
        "chat_id": TG_CHAT_ID,
        "text": "✅ All bots launched. Send /menu anytime to control them."
    })

    # 2. Flush old Telegram messages
    offset = None
    res = tg_api("getUpdates", {"offset": -1})
    if res.get("ok") and res.get("result"):
        offset = res["result"][-1]["update_id"] + 1

    # 3. Poll for /menu and button clicks until EOD
    try:
        while datetime.now().strftime("%H%M") < EOD_TIME:
            params = {"timeout": 10}
            if offset is not None:
                params["offset"] = offset
            res = tg_api("getUpdates", params, timeout=15)

            for upd in res.get("result", []):
                offset = upd["update_id"] + 1

                # Text command (/menu or /start)
                if "message" in upd:
                    msg = upd["message"]
                    if str(msg.get("chat", {}).get("id")) != TG_CHAT_ID:
                        continue
                    cmd = msg.get("text", "").strip().lower()
                    if cmd in ("/menu", "/start", "menu"):
                        text, markup = main_menu()
                        show(text, markup)

                # Inline button tap
                elif "callback_query" in upd:
                    cb = upd["callback_query"]
                    if str(cb["message"]["chat"]["id"]) != TG_CHAT_ID:
                        continue
                    data = cb.get("data", "")
                    msg_id = cb["message"]["message_id"]
                    tg_api("answerCallbackQuery", {"callback_query_id": cb["id"]})

                    if data == "refresh":
                        text, markup = main_menu()
                        show(text, markup, msg_id)

                    elif data.startswith("bot:"):
                        name = data.split(":")[1]
                        text, markup = bot_menu(name)
                        show(text, markup, msg_id)

                    elif data.startswith("act:"):
                        _, action, name = data.split(":")
                        if action == "restart": restart_bot(name)
                        elif action == "stop":  stop_bot(name)
                        elif action == "start": start_bot(name)
                        text, markup = bot_menu(name)
                        show(text, markup, msg_id)

                    elif data == "ask_shutdown":
                        markup = {"inline_keyboard": [[
                            {"text": "✅ Yes, Stop All", "callback_data": "confirm_shutdown"},
                            {"text": "❌ Cancel",        "callback_data": "refresh"},
                        ]]}
                        show("⚠️ Stop ALL bots and save trade logs?", markup, msg_id)

                    elif data == "confirm_shutdown":
                        tg_api("getUpdates", {"offset": offset})
                        show("🛑 Shutting down all bots...", {"inline_keyboard": []}, msg_id)
                        return
    finally:
        stop_all()
        tg_api("sendMessage", {
            "chat_id": TG_CHAT_ID,
            "text": "🛑 All bots stopped. Workflow will now commit trade logs."
        })

if __name__ == "__main__":
    main()
