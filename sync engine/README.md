# TOMCL Sync Engine

Bidirectional sync between local SQLite (`DB_TOMCL/DB_Tomcl.db`) and Hostinger MySQL.

## Tables

| Local / Hostinger | Direction |
|-------------------|-----------|
| chiller_rooms | Both ways |
| destinations | Both ways |
| orders | Both ways |
| packing_types | Both ways |
| party_consignee | Both ways |
| pcs_types | Both ways |
| products | Both ways |
| users | Both ways |
| raw_chiller_data | Single way (SQLite → Hostinger) |

## Control tables (local SQLite)

- **sync_queue** — pending local changes waiting to push
- **sync_metadata** — last pull/push watermark per table
- **sync_log** — SENT / RECEIVED history

## Setup

1. Copy `secrets/mysql_sync.env.example` → `secrets/mysql_sync.env` and fill Hostinger credentials (gitignored).
2. Install dependency: `pip install PyMySQL` (already in `tomcl_python/requirements.txt`).

## Commands

From this folder:

```bat
python run_sync.py --ping
python run_sync.py --bootstrap
python run_sync.py --once
python run_sync.py --loop
```

Or double-click / run `start-sync.bat --loop`.

- `--bootstrap` enqueues current local rows (small tables) then runs one sync cycle.
- `--backfill-raw` also pushes historical `raw_chiller_data` (large). Without it, raw watermark starts at local MAX(id) so only new rows push.
- `--repair-chiller-ids` fixes Hostinger `raw_chiller_data.chiller_id` (changes column to VARCHAR and copies names like `Chiller 1` from SQLite).
- `--loop` repeats every `SYNC_INTERVAL_SECONDS` (default 5).

## How it works

1. Dashboard writes call `sync_bridge.enqueue_sync` → `sync_queue`.
2. Runner pushes PENDING/FAILED queue rows to MySQL, then pushes new `raw_chiller_data` by id watermark.
3. Runner pulls Hostinger `sync_changes` feed (if any), then pulls both-way tables by PK watermark (full merge for `destinations`).

Keep `start-sync.bat --loop` running while the dashboard is in use.
