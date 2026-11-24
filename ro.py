import zmq
import random

def send_ltp_limit_order():
    context = zmq.Context()
    req_socket = context.socket(zmq.REQ)
    req_socket.connect("tcp://127.0.0.1:5556")

    # 1. Get symbols and pick one
    req_socket.send_json({"cmd": "GET_SYMBOLS"})
    symbols = req_socket.recv_json().get("symbols", [])
    
    if not symbols:
        print("No symbols found.")
        return

    target = random.choice([s for s in symbols if s.get("exch") == "NFO"] or symbols)
    print(f"Target: {target['tsym']}")

    # 2. Get the current snapshot to find the LTP
    req_socket.send_json({"cmd": "GET_SNAPSHOT", "token": target["token"]})
    snap = req_socket.recv_json()
    
    ltp = snap.get("ltp")
    if not ltp:
        print("Could not get LTP. Is the feed active?")
        return
        
    print(f"Current LTP: {ltp}")

    # 3. Send the Limit Order at LTP
    order_payload = {
        "cmd": "PLACE_ORDER",
        "side": "BUY", 
        "exchange": target["exch"],
        "symbol": target["tsym"],
        "token": target["token"],
        "qty": 65,           # Update to your actual lot size
        "price_type": "LMT", # Set to Limit
        "price": float(ltp)  # Inject the LTP here
    }

    print("Sending LMT order...")
    req_socket.send_json(order_payload)
    
    # 4. Print the result
    print("Master Response:", req_socket.recv_json())

if __name__ == "__main__":
    send_ltp_limit_order()