# Scripts

Utilities that power the Pirate activity datasets and static dashboard.

## Contents

- `config.py`: Paths and pool addresses used by the scanner.
- `pubkey_to_addr.py`: Converts `notary_pubkey.json` (pubkeys from the Pirate source) into `notaries.json` with Komodo/Pirate transparent addresses. Run when pubkey lists change.
- `scan_blocks.py`: RPC-driven scanner for the Pirate chain. Walks blocks, classifies transactions (coinbase, coinbase shielding, dPoW, atomic swap start/complete, shielded, unknown), and writes to `pirate_activity.db` (SQLite). Takes `--start/--end` heights (prompts if omitted), uses CLI from `config.py`, and auto-creates schema/tables (daily_stats plus detail tables for coinbase, shielding, dpow, swaps, unknowns, shielded, miners).
- `generate_site_data.py`: One-shot generator for all JSON used by the static site:
  - `data/activity_<range>.json` (network activity counts/fees per day, by category)
  - `data/swaps_<range>.json` (atomic swap counts/amounts/fees)
  - `data/miners.json` (coinbase recipients totals/last seen)
  - `data/notaries_stats.json` (dPoW tx totals/fees/last seen)
  Timeframes: 7/30/60/90/180/365/all (override via `--timeframes`). Defaults to `pirate_activity.db` in this folder.
- `tests/`: Helper scripts for validation (e.g., `count.py`, `peek_db.py`).

## scan_blocks usage

```
python scan_blocks.py --start 1 --end 1000
```
- Prompts for heights if omitted.
- Uses `config.CLI` (with `-datadir` from `config.py`) to call `getblockhash/getblock/getrawtransaction/decoderawtransaction`.
- Classifies transactions and updates:
  - `daily_stats` (tx_count, total_fee, total_amount by type per day)
  - Detail tables: `coinbase_txs`, `coinbase_shielding_txs`, `dpow_txs`, `atomic_swap_txs` (start/complete), `unknown_transparent_txs`, `shielded_txs`, `unknown_txs`, `miners`.
- DB: `pirate_activity.db` (default here; override with `--db`).

## Data generation

From repo root (ensure `pirate_activity.db` exists):
```
python scripts/generate_site_data.py
```
Writes JSON into `data/` for the static pages. Adjust DB/outdir with flags as needed.

## Database schema (pirate_activity.db)

- `daily_stats`: `date`, `tx_type`, `tx_count`, `total_fee`, `total_amount`.
- `coinbase_txs`: `txid`, `block_height`, `timestamp`, `date`, `address`, `amount`, `pool_name`.
- `coinbase_shielding_txs`: `txid`, `block_height`, `timestamp`, `date`, `in_addresses`, `total_in`, `fee`.
- `dpow_txs`: `txid`, `block_height`, `timestamp`, `date`, `notary_name`, `notary_season`, `address`, `total_in`, `total_out`, `fee`.
- `atomic_swap_txs`: `txid`, `block_height`, `timestamp`, `date`, `phase` (start/complete), `swap_addr`, `complete_txid`, `complete_block_height`, `complete_timestamp`, `in_addresses`, `out_addresses`, `total_in`, `total_out`, `fee`.
- `unknown_transparent_txs`: `txid`, `block_height`, `timestamp`, `date`, `in_addresses`, `out_addresses`, `total_in`, `total_out`, `fee`.
- `shielded_txs`: `txid`, `block_height`, `timestamp`, `date`.
- `unknown_txs`: `txid`, `block_height`, `timestamp`, `date`, `note`.
- `miners`: `address`, `name`, `first_seen`, `last_seen`, `total_amount`, `tx_count`.
