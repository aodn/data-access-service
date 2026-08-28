# Parquet loading & refresh scheduling

How the `mooring` and `wave-buoy` datasets get from S3 into an in-memory
DuckDB table the `/sites` endpoints read from, and how that table stays
up to date.

| File | What it owns |
| --- | --- |
| `sites_repository.py` | `ParquetRepository` — one dataset's location + the SQL to (re)load it |
| `../core/duckdbclient.py` | `ParquetDuckDBClient` — the shared DuckDB connection + its tuning |
| `../core/scheduler.py` | `TaskScheduler` — the cron jobs that call into `ParquetRepository` |
| `../config/config.yaml` (`parquet.config`) | thread counts, memory limit, incremental lookback |

All repositories share one `ParquetDuckDBClient` connection, so every
endpoint sees the same in-memory tables. Datasets are Hive-partitioned by
`timestamp=<epoch>/`, many small `.parquet` files per partition.

## Loading: full vs. incremental

**Full — `ParquetRepository.load()`**

```sql
CREATE OR REPLACE TABLE <table> AS
SELECT <load_columns> FROM read_parquet('<dataset>/**/*.parquet', hive_partitioning=true)
```

Reads every file. Atomic — readers see the old table until this commits,
and a failure leaves it untouched. The only load that catches a
**retroactive correction** to old data, since it re-reads everything. Also
the expensive one: ~400s for wave-buoy's ~29K files, so the thread count is
temporarily raised to `full_load_threads` for the duration.

**Incremental — `ParquetRepository.load_incremental()`**

```sql
DELETE FROM <table> WHERE TIME >= <cutoff>;
INSERT INTO <table> SELECT ... WHERE TIME >= <cutoff>;
```

`<cutoff>` = `now - incremental_lookback_days` (shared config value). Only
reads the Hive partitions at-or-after the cutoff, found by globbing the
directory listing first (`_qualifying_partition_globs`) rather than
scanning + filtering the whole dataset. Much cheaper: ~19s vs ~400s for
wave-buoy.

**Backup — `write_backup()` / `load_backup()`**

After every successful load, `_refresh_repository` best-effort snapshots
the **entire** table to one flat Parquet file on S3. `_preload_from_backup()`
calls `load_backup()` to seed every table from that file *before*
`_initial_refresh_task` runs — not just so endpoints have something to
serve while the primary S3 refresh is still in progress, but because
without it, a startup refresh that fails (e.g. a transient S3 error) would
leave the table never created at all, and every endpoint erroring instead
of serving backup data. The "entire table" part matters too — it's what
the startup freshness logic below relies on.

## The cron job

```
startup ──► _initial_refresh_task (once)
             │
             ▼
        Sun 03:00 UTC ──► _refresh_task              (full, weekly)
        every 2h       ──► _incremental_refresh_task  (incremental)
```

- **`_refresh_task`** — full reload, weekly. Only job that catches
  retroactive corrections to data older than the lookback window.
- **`_incremental_refresh_task`** — incremental reload, every 2h.
- **`_initial_refresh_task`** — runs once at startup, after
  `_preload_from_backup()` seeds every table from its backup. Per
  repository, `_backup_is_fresh` decides:
  - **fresh** (backup's latest row within `incremental_lookback_days`) →
    incremental catch-up. Safe because the backup already has the whole
    table — only the recent window needs re-reading.
  - **not fresh** (no backup, empty, or stale) → full reload.

  This makes routine restarts cheap without weakening the weekly full
  reload's job — freshness only means "recent data exists," not "old data
  hasn't silently changed upstream."

This is the whole reason incremental loading exists. Before it, every
refresh — including the frequent ones — was a full reload: re-reading the
entire dataset on a short cadence was expensive enough in CPU and memory
to **OOM-kill the process**. Incremental loading gave the frequent refresh
a cheap path, and demoted the full reload to a weekly safety net (see
[The cron job](#the-cron-job)) purely for retroactive corrections, instead
of running it constantly.

Two real `_initial_refresh_task` runs, same host — one where the backup
was stale (full reload), one where it was fresh (incremental catch-up):

| | time | cpu avg/peak | rss avg/peak |
| --- | --- | --- | --- |
| full reload | 524.6s | 53% / 57% | 3.38 / 4.24 GB |
| incremental catch-up | 28.6s | 40% / 40% | 2.06 / 2.06 GB |

~18x faster and well under half the peak memory — exactly the CPU/memory
blowout that made frequent full reloads unsafe in the first place.

## Config knobs (`parquet.config` in `config.yaml`)

| Key | Used by |
| --- | --- |
| `threads` | steady-state DuckDB thread count |
| `full_load_threads` | thread count during a full `load()` only |
| `memory_limit` | DuckDB's memory cap (spills to `duckdb_temp_dir` beyond it) |
| `incremental_lookback_days` | shared window for `load_incremental()` and the startup freshness check |
