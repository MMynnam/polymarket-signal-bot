"""
generate_wallet.py — Generate a new Polygon wallet for the trading bot.

Generates a random private key and derives the corresponding public address.
Run once, save the output securely, never share the private key.

Usage:
  python generate_wallet.py
"""

from eth_account import Account

acct = Account.create()
key  = acct.key.hex()
addr = acct.address

print()
print("=" * 60)
print("  NEW POLYMARKET TRADING WALLET")
print("=" * 60)
print()
print(f"  Address:     {addr}")
print(f"  Private Key: {key}")
print()
print("=" * 60)
print("  NEXT STEPS")
print("=" * 60)
print()
print("  1. SAVE THE PRIVATE KEY SECURELY.")
print("     You will NOT be able to recover it if lost.")
print("     Store it in a password manager, not a chat or email.")
print()
print(f"  2. Add to Railway environment variables:")
print(f"       TRADING_PRIVATE_KEY={key}")
print(f"       TRADING_ENABLED=true")
print()
print(f"  3. Fund this address with USDC on Polygon network:")
print(f"       {addr}")
print()
print("     Polymarket uses bridged USDC (USDC.e):")
print("       Contract: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
print("     You also need a small amount of MATIC for gas (~$1 worth).")
print()
print("  4. After funding, set token allowances:")
print("       python setup_allowances.py")
print()
print("  5. Verify the wallet on Polygonscan:")
print(f"       https://polygonscan.com/address/{addr}")
print()
