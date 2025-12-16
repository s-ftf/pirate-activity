#!/usr/bin/env python3
import json
import hashlib
from pathlib import Path
from typing import Dict, Any

# Komodo / Pirate transparent R addr (P2PKH) prefix byte (R... addresses)
P2PKH_VERSION = 60  # contentReference[oaicite:1]{index=1}

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def b58encode(b: bytes) -> str:
    # leading zero bytes become leading '1's
    n_pad = 0
    for c in b:
        if c == 0:
            n_pad += 1
        else:
            break

    num = int.from_bytes(b, "big")
    out = ""
    while num > 0:
        num, rem = divmod(num, 58)
        out = _B58_ALPHABET[rem] + out

    return ("1" * n_pad) + out

def checksum(payload: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]

def hash160(data: bytes) -> bytes:
    sha = hashlib.sha256(data).digest()
    rip = hashlib.new("ripemd160")
    rip.update(sha)
    return rip.digest()

def pubkey_hex_to_p2pkh_address(pubkey_hex: str, version: int = P2PKH_VERSION) -> str:
    h = pubkey_hex.strip().lower()
    if h.startswith("0x"):
        h = h[2:]
    pub = bytes.fromhex(h)

    # Expect compressed secp256k1 pubkey
    if len(pub) != 33 or pub[0] not in (2, 3):
        raise ValueError(f"Expected compressed pubkey (33 bytes, 02/03 prefix). Got: {pubkey_hex}")

    payload = bytes([version]) + hash160(pub)
    return b58encode(payload + checksum(payload))

def main() -> None:
    in_path = Path("notary_pubkeys.json")
    out_path = Path("notaries.json")

    data: Dict[str, Dict[str, str]] = json.loads(in_path.read_text(encoding="utf-8"))
    out: Dict[str, Any] = {}

    for season, name_to_pub in data.items():
        season_out: Dict[str, Any] = {}
        for name, pubhex in name_to_pub.items():
            season_out[name] = {
                "pubkey": pubhex,
                "taddr": pubkey_hex_to_p2pkh_address(pubhex),
            }
        out[season] = season_out

    out_path.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
