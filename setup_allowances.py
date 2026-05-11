"""
setup_allowances.py — Set unlimited USDC allowances for Polymarket contracts.

Sends two on-chain approve() transactions on Polygon:
  1. USDC → CTF Exchange          (standard markets)
  2. USDC → Neg Risk CTF Exchange (neg-risk markets)

Both are required for the trading bot to execute orders. This is a one-time
setup per wallet. Approvals persist on-chain until explicitly revoked.

Contract addresses are taken directly from py-clob-client to guarantee
they match what the CLOB client uses when routing orders.

Usage:
  python setup_allowances.py
  TRADING_PRIVATE_KEY=0x... python setup_allowances.py

Requirements:
  - TRADING_PRIVATE_KEY set (in .env or Railway env vars)
  - Wallet funded with MATIC on Polygon (~$0.01 worth is sufficient for gas)
"""

import sys
import time

# Load env vars and config (config.py handles dotenv loading as a side-effect).
try:
    import config
except ModuleNotFoundError:
    print("ERROR: run this from the project root where config.py lives.")
    sys.exit(1)

from web3 import Web3
from eth_account import Account

# ---------------------------------------------------------------------------
# Contract addresses (sourced from py-clob-client — do not change)
# ---------------------------------------------------------------------------

USDC_ADDRESS         = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
CTF_EXCHANGE         = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
NEG_RISK_CTF_EXCHANGE = Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a")

MAX_UINT256 = 2 ** 256 - 1

# Minimal ERC-20 ABI — only the functions we need.
ERC20_ABI = [
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount",  "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner",   "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def _connect(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise ConnectionError(f"Cannot connect to RPC: {rpc_url}")
    return w3


def _send_approve(
    w3: Web3,
    usdc,
    spender_address: str,
    spender_label: str,
    account,
) -> str:
    """
    Send an approve(spender, MAX_UINT256) transaction.
    Returns the transaction hash as a hex string.
    Skips if allowance is already MAX_UINT256.
    """
    existing = usdc.functions.allowance(account.address, spender_address).call()
    if existing >= MAX_UINT256 // 2:
        print(f"  ✓ {spender_label}: already approved (allowance={existing})")
        return ""

    nonce    = w3.eth.get_transaction_count(account.address)
    gas_price = w3.eth.gas_price

    tx = usdc.functions.approve(spender_address, MAX_UINT256).build_transaction({
        "from":     account.address,
        "nonce":    nonce,
        "gas":      80_000,
        "gasPrice": gas_price,
        "chainId":  137,
    })

    signed  = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    hex_hash = tx_hash.hex()

    print(f"  → {spender_label}: tx submitted — {hex_hash}")
    print(f"    Waiting for confirmation...", end="", flush=True)

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt["status"] == 1:
        print(f" confirmed (block {receipt['blockNumber']})")
    else:
        print(" REVERTED")
        raise RuntimeError(
            f"approve() reverted for {spender_label}. "
            f"Tx: https://polygonscan.com/tx/{hex_hash}"
        )

    return hex_hash


def main() -> None:
    # --- Validate private key ---
    if not config.TRADING_PRIVATE_KEY:
        print()
        print("ERROR: TRADING_PRIVATE_KEY is not set.")
        print("  Set it in your .env file or export it:")
        print("    TRADING_PRIVATE_KEY=0x... python setup_allowances.py")
        sys.exit(1)

    account = Account.from_key(config.TRADING_PRIVATE_KEY)

    print()
    print("=" * 60)
    print("  POLYMARKET ALLOWANCE SETUP")
    print("=" * 60)
    print()
    print(f"  Wallet:  {account.address}")
    print(f"  Network: Polygon (chain ID 137)")
    print(f"  USDC:    {USDC_ADDRESS}")
    print()

    # --- Connect to Polygon ---
    rpc_url = (
        config.ALCHEMY_RPC_URL
        or "https://polygon-rpc.com"
    )
    print(f"  RPC: {rpc_url.split('?')[0][:60]}...")  # truncate API key from URL
    print()

    try:
        w3 = _connect(rpc_url)
    except ConnectionError as exc:
        print(f"ERROR: {exc}")
        print("  Try setting ALCHEMY_RPC_URL in your .env file.")
        sys.exit(1)

    usdc = w3.eth.contract(address=USDC_ADDRESS, abi=ERC20_ABI)

    # --- Show current balances ---
    matic_balance = w3.eth.get_balance(account.address)
    usdc_balance  = usdc.functions.balanceOf(account.address).call()
    matic_eth     = w3.from_wei(matic_balance, "ether")
    usdc_human    = usdc_balance / 1_000_000  # USDC has 6 decimals

    print(f"  MATIC balance: {matic_eth:.4f} MATIC")
    print(f"  USDC balance:  ${usdc_human:.2f}")
    print()

    if matic_balance == 0:
        print("WARNING: wallet has 0 MATIC. Gas transactions will fail.")
        print("  Send ~$0.10 worth of MATIC to:")
        print(f"  {account.address}")
        print()

    # --- Send approve transactions ---
    print("  Setting allowances...")
    print()

    try:
        _send_approve(w3, usdc, CTF_EXCHANGE,          "CTF Exchange         ", account)
        time.sleep(3)  # brief pause between txs to avoid nonce collision
        _send_approve(w3, usdc, NEG_RISK_CTF_EXCHANGE, "Neg Risk CTF Exchange", account)
    except RuntimeError as exc:
        print()
        print(f"ERROR: {exc}")
        sys.exit(1)
    except Exception as exc:
        print()
        print(f"ERROR: Unexpected error: {exc}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("  ALLOWANCES SET SUCCESSFULLY")
    print("=" * 60)
    print()
    print("  The trading bot wallet is ready to execute orders.")
    print()
    print("  Verify on Polygonscan:")
    print(f"    https://polygonscan.com/address/{account.address}#tokentxns")
    print()
    print("  Enable trading in Railway:")
    print("    TRADING_ENABLED=true")
    print()


if __name__ == "__main__":
    main()
