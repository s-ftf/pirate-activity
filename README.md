<img src="static/p.svg" alt="Pirate logo" width="200">

# Pirate Activity Dashboard

A lightweight toolchain to scan the Pirate Chain blockchain, bucket transactions by type, and visualize network activity (including atomic swaps) in a static GitHub Page.

**Live site:** https://s-ftf.github.io/pirate-activity/

## How it works

- `scripts/scan_blocks.py` walks the chain via `pirate-cli`/`komodo-cli -ac=PIRATE`, classifies every transaction (coinbase, coinbase shielding, dPoW, atomic swap start/complete, turnstile, shielded, unknown), and stores results in `pirate_activity.db` (SQLite). It tracks processed block heights to support resumable scans.
- `scripts/generate_site_data.py` reads the SQLite database and emits JSON into `data/` for multiple timeframes (7/30/60/90/180/365/all).
- `index.html` / `swaps.html` + `static/` render the charts, tables, and filters from the JSON files. No server is required.

## Prerequisites

- Python 3.8+ with access to `pirate-cli` (or `komodo-cli -ac=PIRATE`) and a fully synced node.
- Configure `scripts/config.py` with your CLI binary and `DATADIR`.

## Usage

1. **Scan blocks into SQLite**
   ```bash
   cd scripts
   python scan_blocks.py --start 1 --end <height>
   ```
   Omit `--start/--end` to be prompted. The scanner will skip heights already marked processed.

2. **Generate site data**
   ```bash
   cd scripts
   python generate_site_data.py
   ```
   This writes `data/activity_<range>.json`, `data/swaps_<range>.json`, `data/miners.json`, and `data/notaries_stats.json` in the repo root `data/` directory.

3. **View the dashboards**
   - Open `index.html` for transaction activity.
   - Open `swaps.html` for atomic swap metrics.
   - Or visit the GitHub Pages deployment: https://s-ftf.github.io/pirate-activity/

## Buckets & metrics

- **Coinbase** and **coinbase shielding** (t → z), **dPoW** notary txs, **atomic swaps** (start/complete), **turnstile** migrations (shielded → transparent), **shielded**, and **unknown/unknown transparent**.
- Daily aggregates: tx counts, fees, and amounts by type.
- Miners and notaries: totals and last-seen timestamps, filtered by the selected timeframe in the UI.