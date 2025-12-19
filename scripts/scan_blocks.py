#!/usr/bin/env python3
"""
Scan Pirate Chain blocks and classify transactions into buckets stored in SQLite.

Buckets:
- coinbase
- dpow (transparent notary txs)
- atomic_swap (transparent multisig/sapling hybrids that are not notary work)
- unknown_transparent (any other transparent tx that is not coinbase/dpow/atomic_swap)
- shielded (sapling/sapling only)

Usage:
    python scan_blocks.py --start 1 --end 100
If --start/--end are omitted you will be prompted (default start=1, end=current height).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import shelve
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Set, Tuple
from collections import OrderedDict

BASE_DIR = Path(__file__).resolve().parent

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import config

DEFAULT_DB_PATH = BASE_DIR / "pirate_activity.db"
DEFAULT_CLI = config.CLI


class LRUCache:
    def __init__(self, max_size: int = 20000):
        self.max_size = max_size
        self.data: OrderedDict[str, Dict[str, Any]] = OrderedDict()

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        val = self.data.get(key)
        if val is not None:
            self.data.move_to_end(key)
        return val

    def set(self, key: str, value: Dict[str, Any]) -> None:
        self.data[key] = value
        self.data.move_to_end(key)
        if len(self.data) > self.max_size:
            self.data.popitem(last=False)


@dataclass(frozen=True)
class Notary:
    name: str
    season: str
    address: str


class TxType:
    COINBASE = "coinbase"
    COINBASE_SHIELDING = "coinbase_shielding"
    DPOW = "dpow"
    ATOMIC_SWAP = "atomic_swap"  # shielded -> multisig/transparent
    ATOMIC_SWAP_COMPLETE = "atomic_swap_complete"  # multisig -> shielded
    TURNSTILE = "turnstile"  # shielded -> transparent taddr migration
    UNKNOWN_TRANSPARENT = "unknown_transparent"
    UNKNOWN = "unknown"
    SHIELDED = "shielded"


def run_cli(cli: str, *args: Any, parse_json: bool = True) -> Any:
    """Run pirate-cli (or compatible) and optionally parse JSON output."""
    cmd = [cli, f"-datadir={config.DATADIR}", *[str(a) for a in args]]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{res.stderr.strip()}")
    out = res.stdout.strip()
    return json.loads(out) if parse_json else out


def get_block(cli: str, height: int) -> Dict[str, Any]:
    return run_cli(cli, "getblock", height, 2)


def get_decoded_tx(cli: str, txid: str) -> Dict[str, Any]:
    # verbose=1 returns decoded JSON directly and avoids huge command lines
    return run_cli(cli, "getrawtransaction", txid, 1)


def prompt_int(message: str, default: int) -> int:
    raw = input(f"{message} [{default}]: ").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Expected integer for {message}") from exc


def load_notaries(path: Path) -> Dict[str, Notary]:
    """Return mapping of transparent address -> Notary(name, season, address)."""
    data = json.loads(path.read_text(encoding="utf-8"))
    lookup: Dict[str, Notary] = {}
    for season, entries in data.items():
        for name, info in entries.items():
            addr = info.get("taddr")
            if not addr:
                continue
            lookup[addr] = Notary(name=name, season=season, address=addr)
    return lookup


def ensure_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT NOT NULL,
            tx_type TEXT NOT NULL,
            tx_count INTEGER NOT NULL DEFAULT 0,
            total_fee REAL NOT NULL DEFAULT 0,
            total_amount REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (date, tx_type)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_blocks (
            block_height INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS coinbase_shielding_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            in_addresses TEXT,
            total_in REAL,
            fee REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS miners (
            address TEXT PRIMARY KEY,
            name TEXT,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            total_amount REAL NOT NULL DEFAULT 0,
            tx_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS coinbase_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            address TEXT,
            amount REAL,
            pool_name TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dpow_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            notary_name TEXT,
            notary_season TEXT,
            address TEXT,
            total_in REAL,
            total_out REAL,
            fee REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS atomic_swap_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            phase TEXT NOT NULL,
            swap_addr TEXT,
            complete_txid TEXT,
            complete_block_height INTEGER,
            complete_timestamp INTEGER,
            in_addresses TEXT,
            out_addresses TEXT,
            total_in REAL,
            total_out REAL,
            fee REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS turnstile_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            in_addresses TEXT,
            out_addresses TEXT,
            total_in REAL,
            total_out REAL,
            fee REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS unknown_transparent_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            in_addresses TEXT,
            out_addresses TEXT,
            total_in REAL,
            total_out REAL,
            fee REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS unknown_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL,
            note TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shielded_txs (
            txid TEXT PRIMARY KEY,
            block_height INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            date TEXT NOT NULL
        )
        """
    )
    conn.commit()


def update_daily_stats(conn: sqlite3.Connection, date: str, tx_type: str, fee: float, amount: float = 0.0) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO daily_stats (date, tx_type, tx_count, total_fee, total_amount)
        VALUES (?, ?, 1, ?, ?)
        ON CONFLICT(date, tx_type) DO UPDATE SET
            tx_count = tx_count + 1,
            total_fee = total_fee + excluded.total_fee,
            total_amount = total_amount + excluded.total_amount
        """,
        (date, tx_type, fee, amount),
    )


def upsert_miner(conn: sqlite3.Connection, address: str, name: str, ts: int, amount: float) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO miners (address, name, first_seen, last_seen, total_amount, tx_count)
        VALUES (?, ?, ?, ?, ?, 1)
        ON CONFLICT(address) DO UPDATE SET
            name = COALESCE(excluded.name, miners.name),
            last_seen = excluded.last_seen,
            total_amount = miners.total_amount + excluded.total_amount,
            tx_count = miners.tx_count + 1
        """,
        (address, name, ts, ts, amount),
    )


def utc_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def collect_vout_addresses(tx: Dict[str, Any]) -> Set[str]:
    addrs: Set[str] = set()
    for vout in tx.get("vout", []):
        spk = vout.get("scriptPubKey", {})
        for addr in spk.get("addresses", []) or []:
            addrs.add(addr)
    for dec_out in tx.get("decryptedoutputs", []):
        addr = dec_out.get("address")
        if addr:
            addrs.add(addr)
    return addrs


def collect_vin_addresses(tx: Dict[str, Any], prev_tx_lookup: Optional[Any] = None) -> Set[str]:
    addrs: Set[str] = set()
    for vin in tx.get("vin", []):
        addr = vin.get("address")
        if addr:
            addrs.add(addr)
            continue
        if prev_tx_lookup:
            prev_txid = vin.get("txid")
            idx = vin.get("vout")
            if prev_txid and idx is not None:
                prev_tx = prev_tx_lookup(prev_txid)
                if prev_tx:
                    prev_outs = prev_tx.get("vout", [])
                    if idx < len(prev_outs):
                        for a in prev_outs[idx].get("scriptPubKey", {}).get("addresses", []) or []:
                            addrs.add(a)
    return addrs


def is_coinbase_tx(tx: Dict[str, Any]) -> bool:
    vins = tx.get("vin", [])
    return bool(vins and "coinbase" in vins[0])


def has_shielded_parts(tx: Dict[str, Any]) -> bool:
    return bool(
        tx.get("vShieldedSpend")
        or tx.get("vShieldedOutput")
        or tx.get("vjoinsplit")
        or tx.get("valueBalance")
    )


def has_transparent_io(tx: Dict[str, Any]) -> bool:
    return bool(tx.get("vout"))


def has_transparent_inputs(tx: Dict[str, Any]) -> bool:
    return bool(collect_vin_addresses(tx))


def shielded_value(tx: Dict[str, Any]) -> float:
    # valueBalance is negative when value leaves the transparent pool
    vb = tx.get("valueBalance", 0) or 0
    try:
        return abs(float(vb))
    except Exception:
        return 0.0


def get_prev_tx(
    txid: str,
    cli: str,
    decoded_cache: "LRUCache",
    persistent_cache: Optional[MutableMapping[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    decoded_prev = decoded_cache.get(txid)
    if decoded_prev is None and persistent_cache is not None:
        decoded_prev = persistent_cache.get(txid)
        if decoded_prev:
            decoded_cache.set(txid, decoded_prev)
    if decoded_prev is None:
        decoded_prev = get_decoded_tx(cli, txid)
        decoded_cache.set(txid, decoded_prev)
        if persistent_cache is not None:
            persistent_cache[txid] = decoded_prev
    return decoded_prev


def detect_notary(addrs: Iterable[str], notary_lookup: Dict[str, Notary]) -> Optional[Notary]:
    for addr in addrs:
        if addr in notary_lookup:
            return notary_lookup[addr]
    return None


def classify_tx(
    tx: Dict[str, Any],
    vin_addrs: Set[str],
    vout_addrs: Set[str],
    notary_lookup: Dict[str, Notary],
    miner_addresses: Set[str],
    timestamp: Optional[int] = None,
) -> Tuple[str, Optional[Notary], Optional[str], Optional[str]]:
    all_addrs = vin_addrs | vout_addrs

    if is_coinbase_tx(tx):
        return TxType.COINBASE, None, None, None

    notary = detect_notary(all_addrs, notary_lookup)
    if notary:
        return TxType.DPOW, notary, None, None

    transparent = has_transparent_io(tx)
    transparent_inputs = bool(vin_addrs)
    shielded = has_shielded_parts(tx)
    multisig_like_out = any(
        (vout.get("scriptPubKey", {}).get("type") in {"multisig", "scripthash"})
        or (vout.get("scriptPubKey", {}).get("reqSigs", 0) and vout.get("scriptPubKey", {}).get("reqSigs", 0) > 1)
        or any(addr.startswith("b") for addr in vout.get("scriptPubKey", {}).get("addresses", []) or [])
        for vout in tx.get("vout", [])
    )
    multisig_like_in = any(addr.startswith("b") for addr in vin_addrs)

    def first_multisig_addr(addresses: Iterable[str]) -> Optional[str]:
        for a in addresses:
            if a.startswith("b"):
                return a
        return None
    swap_out_addr = first_multisig_addr(vout_addrs)
    swap_in_addr = first_multisig_addr(vin_addrs)

    # Atomic swap start: shielded -> multisig/transparent (has shielded parts and multisig-like output)
    if shielded and multisig_like_out:
        return TxType.ATOMIC_SWAP, None, "start", swap_out_addr

    # Atomic swap completion: multisig -> shielded with no transparent outputs
    if shielded and multisig_like_in and not transparent:
        return TxType.ATOMIC_SWAP_COMPLETE, None, "complete", swap_in_addr

    # Turnstile: shielded -> transparent (no multisig-like outputs, no transparent inputs)
    if shielded and transparent and not multisig_like_out and not transparent_inputs:
        return TxType.TURNSTILE, None, None, None

    # Coinbase shielding: transparent inputs (typically miner taddr) -> shielded only outputs
    if shielded and transparent_inputs and not transparent:
        in_turnstile_window = False
        if timestamp:
            dt = datetime.utcfromtimestamp(timestamp)
            in_turnstile_window = datetime(2018, 12, 15) <= dt <= datetime(2019, 1, 31)
        if vin_addrs & miner_addresses:
            return TxType.COINBASE_SHIELDING, None, None, None
        if in_turnstile_window:
            # late-2018 migration event: treat non-miner t->z shielding as turnstile
            return TxType.TURNSTILE, None, None, None
        # if not miner/notary, treat as unknown transparent shielding
        return TxType.UNKNOWN_TRANSPARENT, None, None, None

    if transparent and (shielded or multisig_like_out):
        return TxType.ATOMIC_SWAP, None, "start", swap_out_addr

    if transparent:
        return TxType.UNKNOWN_TRANSPARENT, None, None, None

    return TxType.SHIELDED, None, None, None


def sum_vout_values(tx: Dict[str, Any]) -> float:
    return float(sum(vout.get("value", 0) for vout in tx.get("vout", [])))


def fetch_input_total(
    cli: str,
    tx: Dict[str, Any],
    decoded_cache: "LRUCache",
    persistent_cache: Optional[MutableMapping[str, Dict[str, Any]]],
) -> Optional[float]:
    total = 0.0
    for vin in tx.get("vin", []):
        prev_txid = vin.get("txid")
        if not prev_txid:
            continue
        try:
            decoded_prev = get_prev_tx(prev_txid, cli, decoded_cache, persistent_cache)
            idx = vin.get("vout")
            if idx is None:
                continue
            prev_outs = decoded_prev.get("vout", [])
            if idx < len(prev_outs):
                total += float(prev_outs[idx].get("value", 0))
        except Exception:
            return None
    return total


def compute_fee(total_in: Optional[float], total_out: float, tx: Dict[str, Any]) -> float:
    """Estimate fee handling Sapling and Sprout joinsplits.

    Fee ~= vin_sum - vout_sum - vpub_old_sum + vpub_new_sum + valueBalance.
    For shielding t->z joinsplit, vpub_old is the transparent value entering shielded,
    so subtracting it yields the expected small fee (e.g., 0.0001 on Sprout-era shielding).
    """
    vin_sum = total_in or 0.0
    vb_raw = tx.get("valueBalance", 0) or 0
    try:
        vb = float(vb_raw)
    except Exception:
        vb = 0.0
    vpub_old = 0.0
    vpub_new = 0.0
    for js in tx.get("vjoinsplit", []) or []:
        try:
            vpub_old += float(js.get("vpub_old", 0) or 0)
            vpub_new += float(js.get("vpub_new", 0) or 0)
        except Exception:
            continue
    fee = vin_sum - total_out - vpub_old + vpub_new + vb
    if fee < 0:
        fee = 0.0
    return fee


def store_coinbase(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    pool_lookup: Dict[str, str],
    miner_addresses: Set[str],
) -> None:
    date = utc_date(ts)
    total_out = sum_vout_values(tx)
    addrs = collect_vout_addresses(tx)
    miner_addresses.update(addrs)
    addr = next(iter(addrs), None)
    pool_name = None
    if addr:
        for name, pool_addr in pool_lookup.items():
            if addr == pool_addr:
                pool_name = name
                break
        if addr:
            upsert_miner(conn, addr, pool_name or "unknown miner", ts, total_out)
    conn.execute(
        """
        INSERT OR IGNORE INTO coinbase_txs (txid, block_height, timestamp, date, address, amount, pool_name)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (tx.get("txid"), block_height, ts, date, addr, total_out, pool_name),
    )
    update_daily_stats(conn, date, TxType.COINBASE, 0.0, total_out)


def store_coinbase_shielding(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    total_in: Optional[float],
    fee: float,
    vin_addrs: Optional[Set[str]] = None,
) -> None:
    date = utc_date(ts)
    vin_addrs = vin_addrs if vin_addrs is not None else collect_vin_addresses(tx)
    conn.execute(
        """
        INSERT OR IGNORE INTO coinbase_shielding_txs (
            txid, block_height, timestamp, date, in_addresses, total_in, fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tx.get("txid"),
            block_height,
            ts,
            date,
            json.dumps(sorted(vin_addrs)),
            total_in,
            fee,
        ),
    )
    update_daily_stats(conn, date, TxType.COINBASE_SHIELDING, fee, shielded_value(tx))


def store_dpow(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    notary: Optional[Notary],
    total_in: Optional[float],
    total_out: float,
    fee: float,
) -> None:
    date = utc_date(ts)
    conn.execute(
        """
        INSERT OR IGNORE INTO dpow_txs (
            txid, block_height, timestamp, date,
            notary_name, notary_season, address,
            total_in, total_out, fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tx.get("txid"),
            block_height,
            ts,
            date,
            getattr(notary, "name", None),
            getattr(notary, "season", None),
            getattr(notary, "address", None),
            total_in,
            total_out,
            fee,
        ),
    )
    update_daily_stats(conn, date, TxType.DPOW, fee, total_out)


def store_atomic_swap(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    total_in: Optional[float],
    total_out: float,
    fee: float,
    phase: str,
    swap_addr: Optional[str],
    vin_addrs: Optional[Set[str]] = None,
    vout_addrs: Optional[Set[str]] = None,
) -> None:
    date = utc_date(ts)
    vin_addrs = vin_addrs if vin_addrs is not None else collect_vin_addresses(tx)
    vout_addrs = vout_addrs if vout_addrs is not None else collect_vout_addresses(tx)
    in_addrs = json.dumps(sorted(vin_addrs))
    out_addrs = json.dumps(sorted(vout_addrs))
    txid = tx.get("txid")
    cur = conn.cursor()

    if phase == "complete" and swap_addr:
        # Attempt to mark an existing swap by swap_addr
        cur.execute(
            """
            UPDATE atomic_swap_txs
            SET phase='complete',
                complete_txid=?,
                complete_block_height=?,
                complete_timestamp=?,
                fee = COALESCE(fee,0) + ?,
                total_in = COALESCE(total_in,0) + ?,
                total_out = COALESCE(total_out,0) + ?
            WHERE swap_addr=? AND phase='start'
            """,
            (txid, block_height, ts, fee, total_in or 0, total_out, swap_addr),
        )
        if cur.rowcount:
            conn.commit()
            return

    cur.execute(
        """
        INSERT OR IGNORE INTO atomic_swap_txs (
            txid, block_height, timestamp, date, phase, swap_addr,
            in_addresses, out_addresses, total_in, total_out, fee,
            complete_txid, complete_block_height, complete_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            txid,
            block_height,
            ts,
            date,
            phase,
            swap_addr,
            in_addrs,
            out_addrs,
            total_in,
            total_out,
            fee,
            None,
            None,
            None,
        ),
    )
    conn.commit()
    # Only count starts toward daily stats; completions do not increase totals
    if phase == "start":
        update_daily_stats(conn, date, TxType.ATOMIC_SWAP, fee, total_out)


def store_turnstile(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    total_in: Optional[float],
    total_out: float,
    fee: float,
    vin_addrs: Optional[Set[str]] = None,
    vout_addrs: Optional[Set[str]] = None,
) -> None:
    date = utc_date(ts)
    vin_addrs = vin_addrs if vin_addrs is not None else collect_vin_addresses(tx)
    vout_addrs = vout_addrs if vout_addrs is not None else collect_vout_addresses(tx)
    conn.execute(
        """
        INSERT OR IGNORE INTO turnstile_txs (
            txid, block_height, timestamp, date,
            in_addresses, out_addresses, total_in, total_out, fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tx.get("txid"),
            block_height,
            ts,
            date,
            json.dumps(sorted(vin_addrs)),
            json.dumps(sorted(vout_addrs)),
            total_in,
            total_out,
            fee,
        ),
    )
    update_daily_stats(conn, date, TxType.TURNSTILE, fee, total_out)


def store_unknown_transparent(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    total_in: Optional[float],
    total_out: float,
    fee: float,
    vin_addrs: Optional[Set[str]] = None,
    vout_addrs: Optional[Set[str]] = None,
) -> None:
    date = utc_date(ts)
    vin_addrs = vin_addrs if vin_addrs is not None else collect_vin_addresses(tx)
    vout_addrs = vout_addrs if vout_addrs is not None else collect_vout_addresses(tx)
    conn.execute(
        """
        INSERT OR IGNORE INTO unknown_transparent_txs (
            txid, block_height, timestamp, date,
            in_addresses, out_addresses, total_in, total_out, fee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tx.get("txid"),
            block_height,
            ts,
            date,
            json.dumps(sorted(vin_addrs)),
            json.dumps(sorted(vout_addrs)),
            total_in,
            total_out,
            fee,
        ),
    )
    update_daily_stats(conn, date, TxType.UNKNOWN, fee, total_out)


def store_unknown(
    conn: sqlite3.Connection,
    tx: Dict[str, Any],
    block_height: int,
    ts: int,
    note: str = "",
) -> None:
    date = utc_date(ts)
    conn.execute(
        """
        INSERT OR IGNORE INTO unknown_txs (txid, block_height, timestamp, date, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        (tx.get("txid"), block_height, ts, date, note),
    )
    update_daily_stats(conn, date, TxType.UNKNOWN, 0.0, 0.0)


def store_shielded(conn: sqlite3.Connection, tx: Dict[str, Any], block_height: int, ts: int, fee: float) -> None:
    date = utc_date(ts)
    conn.execute(
        """
        INSERT OR IGNORE INTO shielded_txs (txid, block_height, timestamp, date)
        VALUES (?, ?, ?, ?)
        """,
        (tx.get("txid"), block_height, ts, date),
    )
    update_daily_stats(conn, date, TxType.SHIELDED, fee)


def is_block_processed(conn: sqlite3.Connection, block_height: int) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM processed_blocks WHERE block_height=?", (block_height,))
    return cur.fetchone() is not None


def mark_block_processed(conn: sqlite3.Connection, block_height: int, ts: int) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO processed_blocks (block_height, timestamp, date)
        VALUES (?, ?, ?)
        """,
        (block_height, ts, utc_date(ts)),
    )


def last_processed_block(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT MAX(block_height) FROM processed_blocks")
    res = cur.fetchone()
    return res[0] or 0


def process_block(
    cli: str,
    block_height: int,
    block: Dict[str, Any],
    conn: sqlite3.Connection,
    notary_lookup: Dict[str, Notary],
    miner_addresses: Set[str],
    pool_lookup: Dict[str, str],
    decoded_cache: "LRUCache",
    persistent_cache: Optional[MutableMapping[str, Dict[str, Any]]],
) -> None:
    ts = int(block.get("time"))
    tx_entries: List[Any] = block.get("tx", [])
    prev_tx_lookup = lambda txid: get_prev_tx(txid, cli, decoded_cache, persistent_cache)
    for entry in tx_entries:
        if isinstance(entry, dict):
            decoded = entry
        else:
            # fallback if RPC returns txids only
            decoded = decoded_cache.get(entry)
            if decoded is None:
                decoded = get_decoded_tx(cli, entry)
                decoded_cache.set(decoded.get("txid", entry), decoded)
        txid = decoded.get("txid")
        if txid:
            decoded_cache.set(txid, decoded)
        vout_addrs = collect_vout_addresses(decoded)
        vin_addrs = collect_vin_addresses(decoded, prev_tx_lookup)
        total_out = sum_vout_values(decoded)
        total_in = fetch_input_total(cli, decoded, decoded_cache, persistent_cache)
        fee = compute_fee(total_in, total_out, decoded)

        tx_type, notary, phase, swap_addr = classify_tx(
            decoded, vin_addrs, vout_addrs, notary_lookup, miner_addresses, ts
        )
        if tx_type == TxType.COINBASE:
            store_coinbase(conn, decoded, block_height, ts, pool_lookup, miner_addresses)
        elif tx_type == TxType.COINBASE_SHIELDING:
            store_coinbase_shielding(conn, decoded, block_height, ts, total_in, fee, vin_addrs=vin_addrs)
        elif tx_type == TxType.DPOW:
            store_dpow(conn, decoded, block_height, ts, notary, total_in, total_out, fee)
        elif tx_type in (TxType.ATOMIC_SWAP, TxType.ATOMIC_SWAP_COMPLETE):
            store_atomic_swap(
                conn, decoded, block_height, ts, total_in, total_out, fee, phase or "start", swap_addr, vin_addrs, vout_addrs
            )
        elif tx_type == TxType.TURNSTILE:
            store_turnstile(conn, decoded, block_height, ts, total_in, total_out, fee, vin_addrs, vout_addrs)
        elif tx_type == TxType.UNKNOWN_TRANSPARENT:
            store_unknown_transparent(conn, decoded, block_height, ts, total_in, total_out, fee, vin_addrs, vout_addrs)
        elif tx_type == TxType.UNKNOWN:
            store_unknown(conn, decoded, block_height, ts, "uncategorized")
        else:
            store_shielded(conn, decoded, block_height, ts, fee)
    mark_block_processed(conn, block_height, ts)
    conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan Pirate Chain blocks into sqlite.")
    parser.add_argument("--start", type=int, help="Start block height (inclusive).")
    parser.add_argument("--end", type=int, help="End block height (inclusive).")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to sqlite db.")
    parser.add_argument(
        "--cli",
        type=str,
        default=DEFAULT_CLI,
        help="CLI binary to use (default pirate-cli; komodo-cli -ac=PIRATE also works).",
    )
    parser.add_argument(
        "--notaries",
        type=Path,
        default=BASE_DIR / "notaries.json",
        help="Path to notaries.json mapping.",
    )
    return parser.parse_args()


def next_coinbase_height(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT MAX(block_height) FROM coinbase_txs")
    last = cur.fetchone()[0]
    return (last or 0) + 1


def main() -> None:
    args = parse_args()
    cli = args.cli

    try:
        current_height = int(run_cli(cli, "getblockcount", parse_json=False))
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to query block count from CLI '{cli}': {exc}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(args.db)
    ensure_schema(conn)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    processed_last = last_processed_block(conn)
    resume_height = processed_last + 1 if processed_last else next_coinbase_height(conn)
    start = args.start if args.start is not None else prompt_int("Enter start height", resume_height)
    end_default = current_height
    end = args.end if args.end is not None else prompt_int("Enter end height", end_default)
    if start < 1 or end < start:
        print("Invalid height range.", file=sys.stderr)
        sys.exit(1)

    notary_lookup = load_notaries(args.notaries)
    pool_lookup = config.pool_addresses
    miner_addresses = set(pool_lookup.values())

    decoded_cache = LRUCache(max_size=20000)
    persistent_cache: Optional[MutableMapping[str, Dict[str, Any]]] = None
    try:
        persistent_cache = shelve.open(str(BASE_DIR / "decoded_cache"), flag="c")
    except Exception:
        persistent_cache = None

    print(f"Scanning blocks {start}..{end} (chain height {current_height})")
    for height in range(start, end + 1):
        try:
            if is_block_processed(conn, height):
                continue
            block = get_block(cli, height)
            process_block(
                cli=cli,
                block_height=height,
                block=block,
                conn=conn,
                notary_lookup=notary_lookup,
                miner_addresses=miner_addresses,
                pool_lookup=pool_lookup,
                decoded_cache=decoded_cache,
                persistent_cache=persistent_cache,
            )
            if height % 100 == 0:
                print(f"Processed up to height {height}")
        except Exception as exc:  # noqa: BLE001
            print(f"Error at height {height}: {exc}", file=sys.stderr)
            break
    conn.close()
    if persistent_cache is not None:
        persistent_cache.close()
    print("Done.")


if __name__ == "__main__":
    main()
