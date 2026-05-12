"""
check_wallet_status.py — Full wallet registration diagnostic.
Checks contract deployment, balances, Polymarket API, and CLOB auth.

Run: python fly-trader/check_wallet_status.py
"""

import json
import os
import sys
import urllib.request
import urllib.error
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

EOA          = "0xaC531FF479F625A0b1836F575Fe44F20660d5f81"
PROXY_ADDR   = "0x51F0bA181721e3280e3FFDec230cef5Bc4b317Db"
SAFE_ADDR    = "0x3906dF8AFc07B951265C5B0d26dD3F20b2d05b16"
EXCHANGE_V2  = "0xE111180000d2663C0091e4f400237545B87B996B"
USDC         = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CLOB_HOST    = "https://clob.polymarket.com"
DATA_API     = "https://data-api.polymarket.com"
GAMMA_API    = "https://gamma-api.polymarket.com"
USDC_DEC     = 6
ZERO_ADDR    = "0x0000000000000000000000000000000000000000"

PRIVATE_KEY  = os.getenv("TRADING_PRIVATE_KEY", "")
CHAIN_ID     = 137

_USDC_ABI = [{
    "inputs": [{"name": "account", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view", "type": "function",
}]

_EXCHANGE_ABI = [
    {"type": "function", "name": "getProxyWalletAddress",
     "inputs": [{"name": "_addr", "type": "address"}],
     "outputs": [{"name": "", "type": "address"}], "stateMutability": "view"},
    {"type": "function", "name": "getSafeWalletAddress",
     "inputs": [{"name": "_addr", "type": "address"}],
     "outputs": [{"name": "", "type": "address"}], "stateMutability": "view"},
]


def sep(title=""):
    if title:
        print(f"\n{'='*60}\n  {title}\n{'='*60}")
    else:
        print("-" * 60)


def _http_get(url, label=""):
    tag = label or url
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "polymarket-diag/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode()
            return json.loads(body), r.status, None
    except urllib.error.HTTPError as e:
        return None, e.code, str(e)
    except Exception as e:
        return None, None, str(e)


def inspect_address(w3, usdc_contract, label, addr):
    cs = Web3.to_checksum_address(addr)
    code = w3.eth.get_code(cs)
    code_hex = code.hex() if isinstance(code, (bytes, bytearray)) else str(code)
    deployed = code_hex not in ("0x", "")
    matic_raw = w3.eth.get_balance(cs)
    matic = float(w3.from_wei(matic_raw, "ether"))
    try:
        usdc_raw = usdc_contract.functions.balanceOf(cs).call()
        usdc = usdc_raw / (10 ** USDC_DEC)
    except Exception as exc:
        usdc = f"ERR:{exc}"
    status = "DEPLOYED" if deployed else "EMPTY (counterfactual)"
    code_len = (len(code_hex) - 2) // 2 if deployed else 0
    print(f"\n  [{label}]")
    print(f"  Address  : {addr}")
    print(f"  get_code : {status}" + (f" ({code_len} bytes)" if deployed else ""))
    print(f"  MATIC    : {matic:.6f}")
    print(f"  USDC.e   : {usdc if isinstance(usdc, str) else f'${usdc:.6f}'}")
    return deployed, usdc


def probe_data_api(eoa):
    endpoints = [
        f"/profile?user={eoa}",
        f"/positions?user={eoa}",
        f"/activity?user={eoa}&limit=5",
    ]
    results = {}
    for path in endpoints:
        url = DATA_API + path
        print(f"\n  GET {url}")
        data, status, err = _http_get(url)
        if err and status is None:
            print(f"  -> ERROR: {err}")
            results[path] = None
        elif status and status >= 400:
            print(f"  -> HTTP {status}: {err}")
            results[path] = None
        else:
            if isinstance(data, list):
                print(f"  -> list with {len(data)} item(s)")
                if data:
                    print(f"     first item keys: {list(data[0].keys()) if data else '(empty)'}")
                    # print first item in full
                    print(f"     first item: {json.dumps(data[0], indent=5)}")
            else:
                print(f"  -> {json.dumps(data, indent=4)}")
            results[path] = data
    return results


def probe_gamma_api(eoa):
    # Gamma sometimes returns proxyWallet / tradingWallet for registered users
    endpoints = [
        f"/user?address={eoa}",
        f"/users/{eoa}",
        f"/wallet?address={eoa}",
    ]
    print()
    for path in endpoints:
        url = GAMMA_API + path
        print(f"  GET {url}")
        data, status, err = _http_get(url)
        if err and status is None:
            print(f"  -> ERROR: {err}")
        elif status and status >= 400:
            print(f"  -> HTTP {status}")
        else:
            print(f"  -> {json.dumps(data, indent=4)}")


def try_derive_api_key(w3):
    if not PRIVATE_KEY:
        print("  TRADING_PRIVATE_KEY not set — skipping CLOB auth test")
        return
    try:
        from py_clob_client_v2.client import ClobClient
        client = ClobClient(
            CLOB_HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID,
        )
        # Try derive first (non-destructive — doesn't create if missing)
        from py_clob_client_v2.endpoints import DERIVE_API_KEY
        from py_clob_client_v2.headers.headers import create_level_1_headers
        headers = create_level_1_headers(client.signer)
        from py_clob_client_v2.http_helpers.helpers import get
        raw = get(f"{CLOB_HOST}{DERIVE_API_KEY}", headers=headers)
        print(f"\n  Raw DERIVE response keys: {list(raw.keys()) if isinstance(raw, dict) else type(raw)}")
        print(f"  Full response: {json.dumps(raw, indent=4)}")
    except Exception as exc:
        print(f"\n  CLOB auth error: {exc}")


def check_clob_balance_allowance(eoa):
    # GET /balance-allowance with L2 headers — check what sig types return data
    if not PRIVATE_KEY:
        return
    for sig_type in [0, 1, 2, 3]:
        try:
            from py_clob_client_v2.client import ClobClient
            client = ClobClient(
                CLOB_HOST, key=PRIVATE_KEY, chain_id=CHAIN_ID,
                signature_type=sig_type,
                funder=PROXY_ADDR if sig_type in (1, 2, 3) else None,
            )
            creds = client.create_or_derive_api_key()
            client.set_api_creds(creds)
            bal = client.get_balance_allowance()
            print(f"\n  sig_type={sig_type}: {json.dumps(bal, indent=4)}")
            break  # stop at first one that works
        except Exception as exc:
            print(f"\n  sig_type={sig_type}: ERROR — {exc}")


def main():
    rpc = os.getenv("ALCHEMY_RPC_URL") or "https://polygon-rpc.com"
    print(f"RPC : {rpc}")
    print(f"EOA : {EOA}")

    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 15}))
    if not w3.is_connected():
        print("ERROR: cannot connect to RPC")
        sys.exit(1)
    print(f"Connected. Block: {w3.eth.block_number}\n")

    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC), abi=_USDC_ABI)
    exchange = w3.eth.contract(address=Web3.to_checksum_address(EXCHANGE_V2), abi=_EXCHANGE_ABI)

    # ------------------------------------------------------------------
    sep("1. On-chain deployment + balances")
    eoa_cs = Web3.to_checksum_address(EOA)

    # Re-query exchange view functions live
    proxy_live = exchange.functions.getProxyWalletAddress(eoa_cs).call()
    safe_live  = exchange.functions.getSafeWalletAddress(eoa_cs).call()
    print(f"\n  getProxyWalletAddress -> {proxy_live}")
    print(f"  getSafeWalletAddress  -> {safe_live}")

    inspect_address(w3, usdc, "EOA (signer/gas)", EOA)
    proxy_deployed, proxy_usdc = inspect_address(w3, usdc, "Proxy (getProxyWalletAddress)", PROXY_ADDR)
    safe_deployed,  safe_usdc  = inspect_address(w3, usdc, "Safe  (getSafeWalletAddress)", SAFE_ADDR)

    # ------------------------------------------------------------------
    sep("2. Polymarket data API — profile + positions + activity")
    api_results = probe_data_api(EOA)

    # Extract any wallet/proxy addresses Polymarket associates with this EOA
    profile = api_results.get(f"/profile?user={EOA}")
    discovered_wallet = None
    if isinstance(profile, dict):
        for key in ("proxyWallet", "depositWallet", "tradingWallet", "safeWallet",
                    "walletAddress", "proxy", "address", "deposit_wallet"):
            if profile.get(key) and profile.get(key) != ZERO_ADDR:
                discovered_wallet = (key, profile[key])
                print(f"\n  *** Found wallet field in profile: {key} = {profile[key]}")

    # ------------------------------------------------------------------
    sep("3. Gamma API — user profile lookup")
    probe_gamma_api(EOA)

    # ------------------------------------------------------------------
    sep("4. If new deposit wallet discovered — check its balances")
    if discovered_wallet:
        key, addr = discovered_wallet
        print(f"\n  Checking discovered wallet ({key}): {addr}")
        inspect_address(w3, usdc, f"Deposit wallet ({key})", addr)
    else:
        print("\n  No new deposit wallet address found in API responses.")
        print("  Checking EOA's own USDC.e (in case funds moved there):")
        eoa_code = w3.eth.get_code(eoa_cs).hex()
        eoa_usdc_raw = usdc.functions.balanceOf(eoa_cs).call()
        eoa_usdc = eoa_usdc_raw / (10 ** USDC_DEC)
        print(f"  EOA USDC.e: ${eoa_usdc:.6f}")

    # ------------------------------------------------------------------
    sep("5. CLOB auth test — derive_api_key raw response")
    try_derive_api_key(w3)

    # ------------------------------------------------------------------
    sep("Summary")
    print(f"\n  Proxy {PROXY_ADDR[:12]}...  deployed={proxy_deployed}  USDC=${proxy_usdc if not isinstance(proxy_usdc, str) else '?':.2f}" if not isinstance(proxy_usdc, str) else f"\n  Proxy: deployed={proxy_deployed}")
    print(f"  Safe  {SAFE_ADDR[:12]}...  deployed={safe_deployed}   USDC=${safe_usdc if not isinstance(safe_usdc, str) else '?':.2f}" if not isinstance(safe_usdc, str) else f"  Safe: deployed={safe_deployed}")
    if discovered_wallet:
        print(f"  Deposit wallet found: {discovered_wallet[1]}")
    print()
    sep()
    print("Done.")


if __name__ == "__main__":
    main()
