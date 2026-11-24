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


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from urllib.parse import urlparse, parse_qs
from api_helper import NorenApiPy
import pyotp
import time
import json
import hashlib
import requests
import yaml
import logging
import os, sys
from dotenv import load_dotenv

# --- testoauth.py Setup ---
logging.basicConfig(level=logging.WARNING)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────
# os.getenv() fetches the value from the cloud server's environment
CLIENT_ID   = os.getenv("SHOONYA_CLIENT_ID")
USER_ID     = os.getenv("SHOONYA_USER_ID")
PASSWORD    = os.getenv("SHOONYA_PASSWORD")
TOTP_SECRET = os.getenv("SHOONYA_TOTP_SECRET")
SECRET_CODE = os.getenv("SHOONYA_SECRET_CODE")

LOGIN_URL   = f"https://trade.shoonya.com/OAuthlogin/investor-entry-level/login?api_key={CLIENT_ID}&route_to={USER_ID}"
TOKEN_URL   = "https://trade.shoonya.com/NorenWClientAPI/GenAcsTok"


def scan_network_for_code(driver):
    try:
        logs = driver.get_log("performance")
        for entry in logs:
            try:
                message = json.loads(entry["message"])["message"]
                # Check both request and response events
                if message.get("method") in ["Network.requestWillBeSent", "Network.responseReceived"]:
                    url = message.get("params", {}).get("request", {}).get("url", "") or \
                          message.get("params", {}).get("response", {}).get("url", "")
                    
                    # FIX: Look for google.com instead of shoonya
                    if "code=" in url and "google.com" in url.lower():
                        parsed = urlparse(url)
                        code = parse_qs(parsed.query).get("code", [None])[0]
                        if code:
                            return code
            except Exception:
                continue
    except Exception:
        pass
    return None

def fast_fill(driver, element, value):
    element.click()
    time.sleep(0.1)
    element.clear()
    element.send_keys(value)
    time.sleep(0.1)

# ─── PHASE 1: Capture Auth Code (GetOAuth.py) ────────────────────────────────
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

driver = webdriver.Chrome(options=options)
wait   = WebDriverWait(driver, 30)

auth_code = None

try:
    print("Logging in to Shoonya (background)...")
    driver.get(LOGIN_URL)

    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='password']")))
    time.sleep(1)

    all_inputs     = driver.find_elements(By.CSS_SELECTOR, "input:not([type='hidden']):not([type='checkbox']):not([type='radio'])")
    visible_inputs = [inp for inp in all_inputs if inp.is_displayed()]

    fast_fill(driver, visible_inputs[0], USER_ID)
    fast_fill(driver, visible_inputs[1], PASSWORD)

    otp_value = pyotp.TOTP(TOTP_SECRET).now()
    fast_fill(driver, visible_inputs[2], otp_value)

    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='LOGIN']"))).click()
    print("Credentials submitted. Capturing auth code...")

    start = time.time()
    while True:
        auth_code = scan_network_for_code(driver)

        if auth_code:
            print(f"Auth Code Captured")
            break

        if time.time() - start > 60:
            new_otp = pyotp.TOTP(TOTP_SECRET).now()
            if new_otp != otp_value:
                fast_fill(driver, visible_inputs[2], new_otp)
                wait.until(EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='LOGIN']"))).click()
                start   = time.time()
                otp_value = new_otp
                continue
            print("[TIMEOUT] Could not capture auth code.")
            break

        time.sleep(0.5)

except Exception as e:
    print(f"[ERROR] {e}")
finally:
    try:
        driver.quit()
    except Exception:
        pass

# ─── PHASE 2: API Login (testoauth.py) ───────────────────────────────────────
if auth_code:
    api = NorenApiPy()

    with open('cred.yml', 'r') as f:
        cred = yaml.load(f, Loader=yaml.FullLoader) or {}

    apikey_url = api.getOAuthURL(cred['oauth_url'], cred['client_id'])
    logging.info(apikey_url)

    result = api.getAccessToken(auth_code, cred['Secret_Code'], cred['client_id'], cred['UID'])
    
    if result is not None:
        # BIG SUCCESS MESSAGE
        print("\n" + "="*50)
        print("*" + " "*48 + "*")
        print("*" + " LOGIN SUCCESSFUL ".center(48) + "*")
        print("*" + " "*48 + "*")
        print("="*50 + "\n")

        acc_tok, usrid, ref_tok, actid = result
        logging.info(f"""\nAccess token is : {acc_tok} \nRefresh token is : {ref_tok} \nUser ID token is : {usrid} \nAccount ID is : {actid} \n""")
        cred['Access_token'] = acc_tok
        cred['Account_ID'] = actid
        
        logging.info(cred)

        with open('cred.yml', 'w') as f:
            yaml.dump(cred, f)

        watchlistname = api.get_watch_list_names()
        logging.info(f'watchlistname : {watchlistname}')
        
        # TELL BASE.PY WE SUCCEEDED
        sys.exit(0)
    else:
        print("Failed to retrieve access token.")
        # TELL BASE.PY WE FAILED
        sys.exit(1)
else:
    print("Auth code was not captured. Skipping API phase.")
    # TELL BASE.PY WE FAILED
    sys.exit(1)
