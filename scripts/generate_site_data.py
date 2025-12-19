#!/usr/bin/env python3
"""
Generate all JSON payloads for the static dashboard (activity, swaps, miners, notaries).

Outputs:
- data/activity_<range>.json
- data/swaps_<range>.json
- data/miners.json
- data/notaries_stats.json
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

if str(Path(__file__).resolve().parent) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(Path(__file__).resolve().parent))
import config

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
DEFAULT_DB = BASE_DIR / "pirate_activity.db"
TIMEFRAMES = ["7", "30", "60", "90", "180", "365", "all"]


# -------------- Activity (all tx types) --------------
def load_daily_all(conn: sqlite3.Connection) -> List[Tuple[str, str, int, float]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, tx_type, tx_count, total_fee
        FROM daily_stats
        ORDER BY date ASC
        """
    )
    return cur.fetchall()


def slice_rows(rows, days: int | None) -> List[Tuple[str, str, int, float]]:
    if days is None or not rows:
        return rows
    end_date = datetime.strptime(rows[-1][0], "%Y-%m-%d")
    start_date = end_date - timedelta(days=days - 1)
    start_str = start_date.strftime("%Y-%m-%d")
    return [r for r in rows if r[0] >= start_str]


def build_activity_series(rows: List[Tuple[str, str, int, float]]) -> Tuple[List[str], Dict[str, Dict[str, Dict[str, float]]]]:
    dates: List[str] = []
    per_day: Dict[str, Dict[str, Dict[str, float]]] = {}
    for date, tx_type, tx_count, total_fee in rows:
        if date not in per_day:
            per_day[date] = {}
            dates.append(date)
        per_day[date][tx_type] = {"tx": tx_count, "fee": total_fee}
    dates.sort()
    return dates, per_day


def aggregate_activity(dates: List[str], per_day: Dict[str, Dict[str, Dict[str, float]]], max_points: int = 180):
    """Bucket daily activity into larger windows (e.g. weekly) so charts stay readable."""
    if len(dates) <= max_points:
        return [{"start": d, "end": d, "categories": per_day.get(d, {})} for d in dates]
    bucket_size = math.ceil(len(dates) / max_points)
    buckets = []
    for i in range(0, len(dates), bucket_size):
        window = dates[i : i + bucket_size]
        if not window:
            continue
        bucket: Dict[str, Dict[str, float]] = {}
        for d in window:
            for cat, vals in per_day.get(d, {}).items():
                slot = bucket.setdefault(cat, {"tx": 0, "fee": 0.0})
                slot["tx"] += vals.get("tx", 0)
                slot["fee"] += vals.get("fee", 0.0)
        buckets.append({"start": window[0], "end": window[-1], "categories": bucket})
    return buckets


def summarize_activity(dates: List[str], per_day: Dict[str, Dict[str, Dict[str, float]]]) -> Dict:
    categories = set()
    for day in per_day.values():
        categories.update(day.keys())
    categories = sorted(categories)

    totals = {
        "total_tx": 0,
        "total_fees": 0.0,
        "avg_tx_per_day": 0.0,
        "avg_fees_per_day": 0.0,
        "max_tx_day": {"date": None, "count": 0},
        "max_fee_day": {"date": None, "fee": 0.0},
        "median_tx_per_day": 0.0,
        "days": len(dates),
    }
    per_cat: Dict[str, Dict[str, Any]] = {
        c: {
            "total_tx": 0,
            "total_fees": 0.0,
            "avg_tx_per_day": 0.0,
            "avg_fees_per_day": 0.0,
            "max_tx_day": {"date": None, "count": 0},
            "max_fee_day": {"date": None, "fee": 0.0},
        }
        for c in categories
    }

    per_day_totals: List[int] = []
    for d in dates:
        day_total_tx = 0
        day_total_fee = 0.0
        for cat in categories:
            vals = per_day.get(d, {}).get(cat, {"tx": 0, "fee": 0.0})
            tx = vals["tx"]
            fee = vals["fee"]
            per_cat[cat]["total_tx"] += tx
            per_cat[cat]["total_fees"] += fee
            if tx > per_cat[cat]["max_tx_day"]["count"]:
                per_cat[cat]["max_tx_day"] = {"date": d, "count": tx}
            if fee > per_cat[cat]["max_fee_day"]["fee"]:
                per_cat[cat]["max_fee_day"] = {"date": d, "fee": fee}
            day_total_tx += tx
            day_total_fee += fee
        totals["total_tx"] += day_total_tx
        totals["total_fees"] += day_total_fee
        per_day_totals.append(day_total_tx)
        if day_total_tx > totals["max_tx_day"]["count"]:
            totals["max_tx_day"] = {"date": d, "count": day_total_tx}
        if day_total_fee > totals["max_fee_day"]["fee"]:
            totals["max_fee_day"] = {"date": d, "fee": day_total_fee}

    if per_day_totals:
        per_day_totals.sort()
        mid = len(per_day_totals) // 2
        totals["median_tx_per_day"] = (
            (per_day_totals[mid - 1] + per_day_totals[mid]) / 2 if len(per_day_totals) % 2 == 0 else per_day_totals[mid]
        )

    days_count = len(dates) if dates else 0
    if days_count:
        totals["avg_tx_per_day"] = totals["total_tx"] / days_count
        totals["avg_fees_per_day"] = totals["total_fees"] / days_count
        for cat in categories:
            per_cat[cat]["avg_tx_per_day"] = per_cat[cat]["total_tx"] / days_count
            per_cat[cat]["avg_fees_per_day"] = per_cat[cat]["total_fees"] / days_count

    return {"categories": categories, "totals": totals, "per_category": per_cat}


def write_activity(conn: sqlite3.Connection, outdir: Path, timeframes: List[str]) -> None:
    rows = load_daily_all(conn)
    if not rows:
        print("No rows in daily_stats.")
        return
    for tf in timeframes:
        days = None if tf == "all" else int(tf)
        sliced = slice_rows(rows, days)
        dates, per_day = build_activity_series(sliced)
        meta = summarize_activity(dates, per_day)
        series_buckets = (
            aggregate_activity(dates, per_day)
            if tf == "all"
            else [{"start": d, "end": d, "categories": per_day.get(d, {})} for d in dates]
        )
        series = []
        for bucket in series_buckets:
            day_cats = bucket.get("categories", {})
            total_tx = sum(v["tx"] for v in day_cats.values())
            total_fee = sum(v["fee"] for v in day_cats.values())
            series.append(
                {
                    "date": bucket["start"],
                    "start_date": bucket["start"],
                    "end_date": bucket["end"],
                    "total_tx": total_tx,
                    "total_fee": total_fee,
                    "categories": day_cats,
                }
            )
        out_path = outdir / f"activity_{tf}.json"
        out_path.write_text(json.dumps({"meta": meta, "series": series}, indent=2), encoding="utf-8")
        print(f"Wrote {out_path}")


# -------------- Atomic swaps --------------
def load_daily_swaps(conn: sqlite3.Connection) -> List[Tuple[str, int, float, float]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, tx_count, total_amount, total_fee
        FROM daily_stats
        WHERE tx_type='atomic_swap'
        ORDER BY date ASC
        """
    )
    return cur.fetchall()


def slice_range(rows, days: int | None) -> List[Tuple[str, int, float, float]]:
    if days is None:
        return rows
    if not rows:
        return []
    end = datetime.strptime(rows[-1][0], "%Y-%m-%d")
    start = end - timedelta(days=days - 1)
    start_str = start.strftime("%Y-%m-%d")
    return [r for r in rows if r[0] >= start_str]


def load_swaps(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> List[Tuple[str, str, float, float]]:
    cur = conn.cursor()
    if start_date and end_date:
        cur.execute(
            """
            SELECT date, phase, total_out, fee
            FROM atomic_swap_txs
            WHERE date BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )
    else:
        cur.execute("SELECT date, phase, total_out, fee FROM atomic_swap_txs")
    return cur.fetchall()


def summarize_swaps(rows: List[Tuple[str, int, float, float]], tx_rows: List[Tuple[str, str, float, float]]) -> Dict:
    starts = [r for r in tx_rows if r[1] == "start"]
    max_single_swap = max((r[2] or 0 for r in starts), default=0)
    median_swap_amount = 0
    if starts:
        amounts = sorted([r[2] or 0 for r in starts])
        mid = len(amounts) // 2
        median_swap_amount = (amounts[mid - 1] + amounts[mid]) / 2 if len(amounts) % 2 == 0 else amounts[mid]

    if not rows:
        return {
            "total_swaps": 0,
            "total_amount": 0,
            "total_fees": 0,
            "avg_swap_amount": 0,
            "avg_swaps_per_day": 0,
            "avg_fee_per_swap": 0,
            "max_swaps_day": {"date": None, "count": 0},
            "max_amount_day": {"date": None, "amount": 0},
            "max_single_swap": max_single_swap,
            "median_swap_amount": median_swap_amount,
        }
    total_swaps = sum(r[1] for r in rows)
    total_amount = sum(r[2] for r in rows)
    total_fees = sum(r[3] for r in rows)
    days = len(rows)
    avg_swap_amount = total_amount / total_swaps if total_swaps else 0
    avg_swaps_per_day = total_swaps / days if days else 0
    max_swaps_day = max(rows, key=lambda r: r[1])
    max_amount_day = max(rows, key=lambda r: r[2])
    avg_fee_per_swap = total_fees / total_swaps if total_swaps else 0
    return {
        "total_swaps": total_swaps,
        "total_amount": total_amount,
        "total_fees": total_fees,
        "avg_swap_amount": avg_swap_amount,
        "avg_swaps_per_day": avg_swaps_per_day,
        "avg_fee_per_swap": avg_fee_per_swap,
        "max_swaps_day": {"date": max_swaps_day[0], "count": max_swaps_day[1]},
        "max_amount_day": {"date": max_amount_day[0], "amount": max_amount_day[2]},
        "max_single_swap": max_single_swap,
        "median_swap_amount": median_swap_amount,
    }


def aggregate_swaps(rows: List[Tuple[str, int, float, float]], max_points: int = 180) -> List[Tuple[str, int, float, float]]:
    """Bucket daily swap rows so the 'all' chart keeps a sane point count."""
    if len(rows) <= max_points:
        return [
            {"start_date": r[0], "end_date": r[0], "swaps": r[1], "amount": r[2], "fee": r[3]} for r in rows
        ]
    bucket_size = math.ceil(len(rows) / max_points)
    agg_rows: List[Dict[str, Any]] = []
    for i in range(0, len(rows), bucket_size):
        window = rows[i : i + bucket_size]
        if not window:
            continue
        date = window[0][0]
        swaps = sum(r[1] for r in window)
        amount = sum(r[2] for r in window)
        fee = sum(r[3] for r in window)
        agg_rows.append({"start_date": window[0][0], "end_date": window[-1][0], "swaps": swaps, "amount": amount, "fee": fee})
    return agg_rows


def write_swaps(conn: sqlite3.Connection, outdir: Path, timeframes: List[str]) -> None:
    rows = load_daily_swaps(conn)
    if not rows:
        print("No atomic swap rows found in daily_stats.")
        return
    for tf in timeframes:
        days = None if tf == "all" else int(tf)
        sliced = slice_range(rows, days)
        start_date = sliced[0][0] if sliced else None
        end_date = sliced[-1][0] if sliced else None
        tx_rows = load_swaps(conn, start_date, end_date)
        meta = summarize_swaps(sliced, tx_rows)
        chart_rows = (
            aggregate_swaps(sliced)
            if tf == "all"
            else [{"start_date": r[0], "end_date": r[0], "swaps": r[1], "amount": r[2], "fee": r[3]} for r in sliced]
        )
        payload = {
            "meta": meta,
            "series": [
                {
                    "date": r["start_date"],
                    "start_date": r["start_date"],
                    "end_date": r["end_date"],
                    "swaps": r["swaps"],
                    "amount": r["amount"],
                    "fee": r["fee"],
                }
                for r in chart_rows
            ],
        }
        out_path = outdir / f"swaps_{tf}.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Wrote {out_path}")


# -------------- Miners / Notaries --------------
def write_miners(conn: sqlite3.Connection, outdir: Path) -> None:
    # Refresh miner names from config pool_addresses for any unknown entries
    cur = conn.cursor()
    for pool_name, pool_addr in config.pool_addresses.items():
        cur.execute(
            """
            UPDATE miners SET name=?
            WHERE address=? AND (name IS NULL OR name LIKE 'unknown%')
            """,
            (pool_name, pool_addr),
        )
    conn.commit()

    cur = conn.cursor()
    cur.execute(
        """
        SELECT address, COALESCE(name,'unknown miner') AS name, tx_count, total_amount, last_seen
        FROM miners
        ORDER BY tx_count DESC
        """
    )
    rows = cur.fetchall()
    data = []
    for addr, name, txc, amt, last_seen in rows:
        avg_per_block = amt / txc if txc else 0
        data.append(
            {
                "address": addr,
                "name": name,
                "blocks_mined": txc,
                "total_arrr": amt,
                "avg_arrr_per_block": avg_per_block,
                "last_seen": last_seen,
            }
        )
    out_path = outdir / "miners.json"
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")


def write_notaries(conn: sqlite3.Connection, outdir: Path) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT address, COALESCE(notary_name,'?') AS name,
               COUNT(*) as tx_count, SUM(total_out) as total_out, SUM(fee) as total_fee, MAX(timestamp) as last_seen
        FROM dpow_txs
        GROUP BY address, notary_name
        ORDER BY tx_count DESC
        """
    )
    rows = cur.fetchall()
    data = []
    for addr, name, txc, tout, fee, last_seen in rows:
        data.append(
            {
                "address": addr,
                "name": name,
                "tx_count": txc or 0,
                "total_arrr": tout or 0,
                "total_fee": fee or 0,
                "last_seen": last_seen,
            }
        )
    out_path = outdir / "notaries_stats.json"
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate all JSON payloads for the static dashboard.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to pirate_activity.db")
    parser.add_argument("--outdir", type=Path, default=DATA_DIR, help="Output directory for JSON files")
    parser.add_argument("--timeframes", nargs="*", default=TIMEFRAMES, help="Timeframes to generate (default all presets)")
    args = parser.parse_args()

    args.outdir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)

    write_activity(conn, args.outdir, args.timeframes)
    write_swaps(conn, args.outdir, args.timeframes)
    write_miners(conn, args.outdir)
    write_notaries(conn, args.outdir)


if __name__ == "__main__":
    main()
