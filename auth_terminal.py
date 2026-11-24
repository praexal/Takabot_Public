import os
import time
import json
import msvcrt  

QUEUE_FOLDER = r"temp_state/auth_queue"

def get_keypress():
    if msvcrt.kbhit():
        ch = msvcrt.getch().decode('utf-8', errors='ignore').lower()
        return ch
    return None

def main():
    os.system("title TRADE ORDER AUTHORIZATION TERMINAL")
    print("==========================================================")
    print("📡 ASYNCHRONOUS ORDER CONFIRMATION ENGINE ONLINE")
    print("🤖 Standing by for incoming setups within 0.3% margin...")
    print("==========================================================")

    while True:
        if not os.path.exists(QUEUE_FOLDER):
            time.sleep(0.5)
            continue
            
        files = [f for f in os.listdir(QUEUE_FOLDER) if f.endswith(".json")]
        if not files:
            time.sleep(0.1)
            continue

        ticket_name = files[0]
        ticket_path = os.path.join(QUEUE_FOLDER, ticket_name)
        
        try:
            with open(ticket_path, "r") as f:
                req = json.load(f)
        except:
            time.sleep(0.05)
            continue

        # CRITICAL FILTER LOCK: If the ticket file was already answered, ignore it completely!
        if req.get("decision", "PENDING") != "PENDING":
            time.sleep(0.05)
            continue

        print("\a") 
        
        decision = "PENDING"
        start_time = req["timestamp"]
        timeout_limit = req["timeout"]

        while True:
            elapsed = time.time() - start_time
            remaining = int(timeout_limit - elapsed)

            if remaining <= 0:
                decision = "EXPIRED"
                break

            print(
                f"\r🚨 [{req['setup_tag']}] CONFIRM ORDER: {req['symbol']} "
                f"| Price: {req['price']} | ATR: {req['atr']:.2f} "
                f"| VWAP Dist: {req['vwap_dist']:.2f} "
                f"| ⏱️ TIMEOUT IN: {remaining}s [Y/N]? ", end="", flush=True
            )

            key = get_keypress()
            if key == 'y':
                decision = "APPROVED"
                break
            elif key == 'n':
                decision = "REJECTED"
                break

            time.sleep(0.05)

        print(f"\n\n➔ DECISION RECORDED: {decision}")
        
        req["decision"] = decision
        try:
            with open(ticket_path, "w") as f:
                json.dump(req, f)
            print("💾 Transmission updated successfully. Returning to standby.\n")
        except Exception as e:
            print(f"❌ Error communicating decision to main process: {e}")
            try: os.remove(ticket_path)
            except: pass

        print("==========================================================")

if __name__ == "__main__":
    main()