# Wave Buoy S3 Backup — Bug Fix Design

## The Bug

The `wave_buoy_realtime_nonqc` table lives in a named in-memory DuckDB database (`:memory:cloud_optimized`).
On every app restart (e.g. ECS redeployment), this memory is wiped clean.

On startup, `start_with_initial_run` immediately tries to rebuild the table by reading from the main S3 source.
If this fails — e.g. an **ETag mismatch** (S3 file changed while DuckDB was mid-read) — the table is never created.
Any request to `/data/feature-collection/wave-buoy` then throws a **500 error** because the table does not exist.

Since the service runs on AWS ECS, there is no persistent local storage — everything is lost on redeploy.
A local file-based fallback is therefore not an option.

---

## Solution: S3 Backup

Use a dedicated S3 path as a persistent backup of the last known good table state.

### Data flow

```
many partitioned parquet files (main S3)
        ↓  read + consolidate (hourly)
   in-memory DuckDB table
        ↓  COPY TO (on success)
   single parquet file (backup S3)   ← restored from on startup/failure
```

The backup is a single consolidated parquet file — not the raw hive-partitioned source — because:

- The source is split across many files by `site_name`, `timestamp`, `polygon`
- The in-memory table is already the consolidated result of reading all those files
- A single flat parquet file is simpler, smaller, and faster to restore from

### DuckDB data format conversion

DuckDB handles both directions of parquet serialisation:

```
source parquet files (S3)
        ↓  read_parquet() — decode into rows/columns
   in-memory relational table (DuckDB internal format)
        ↓  COPY TO — re-encode into parquet
   single parquet file (backup S3)
```

When `read_parquet()` runs, DuckDB decodes the parquet files into its own internal in-memory table format — rows and columns in RAM, no longer parquet at this point. When `COPY ... TO 's3://...backup.parquet'` runs, DuckDB re-encodes that in-memory table back into parquet and writes it to S3. The format is inferred from the `.parquet` extension, or can be made explicit:

```python
self.memconn.execute(f"COPY {target_table_name} TO '{BACKUP_S3_PATH}' (FORMAT PARQUET)")
```

---

## Implementation

### `hourly_task` — write backup on success, restore from backup on failure

```python
BACKUP_S3_PATH = "s3://<your-backup-bucket>/cache/wave_buoy_realtime_nonqc.parquet"

def hourly_task(self):
    temp_table_name = "wave_buoy_realtime_nonqc_temp"
    target_table_name = "wave_buoy_realtime_nonqc"
    dataset = f"s3://{self._instance.bucket_name}/wave_buoy_realtime_nonqc.parquet"

    try:
        # Read from main S3
        self.memconn.execute(f"""
            CREATE OR REPLACE TABLE {temp_table_name} AS
            SELECT * FROM read_parquet('{dataset}/**/*.parquet', hive_partitioning=true)
        """)

        # Atomic swap
        self.memconn.execute(f"""
            BEGIN TRANSACTION;
            DROP TABLE IF EXISTS {target_table_name};
            ALTER TABLE {temp_table_name} RENAME TO {target_table_name};
            COMMIT;
        """)

        # Keep backup fresh
        self.memconn.execute(f"COPY {target_table_name} TO '{BACKUP_S3_PATH}'")
        logger.info("Backup written to S3 successfully")

    except Exception as e:
        logger.error(f"Error in hourly task: {e}", exc_info=True)
        try:
            self.memconn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        except Exception:
            pass

        # Fallback: restore from backup
        try:
            self.memconn.execute(f"""
                CREATE OR REPLACE TABLE {target_table_name} AS
                SELECT * FROM read_parquet('{BACKUP_S3_PATH}')
            """)
            logger.info("Loaded from S3 backup successfully")
        except Exception as backup_error:
            logger.error(f"Failed to load from S3 backup: {backup_error}", exc_info=True)
```

### `start_with_initial_run` — pre-load from backup before hitting main S3

```python
async def start_with_initial_run(self):
    # Pre-load from backup so endpoint is available immediately on startup
    try:
        self.memconn.execute(f"""
            CREATE OR REPLACE TABLE wave_buoy_realtime_nonqc AS
            SELECT * FROM read_parquet('{BACKUP_S3_PATH}')
        """)
        logger.info("Pre-loaded wave buoy data from S3 backup")
    except Exception as e:
        logger.warning(f"No S3 backup found, will rely on initial S3 fetch: {e}")

    # Full refresh from main S3
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        await loop.run_in_executor(executor, lambda: self.hourly_task())
    self.start()
```

---

## Startup Behaviour

| Scenario                             | Result                                                       |
| ------------------------------------ | ------------------------------------------------------------ |
| First ever deploy (no backup yet)    | Loads from main S3 normally; backup created on success       |
| Restart, main S3 succeeds            | Backup pre-loaded → refreshed with fresh data                |
| Restart, main S3 ETag error          | Backup pre-loaded → endpoint stays healthy; retry in 2 hours |
| Restart, main S3 error AND no backup | 500 error (first deploy edge case only)                      |

---

## Data Size & Timing Analysis

Measured against the live `aodn-cloud-optimised` bucket (2026-03-16):

**Source data (main S3):**

- 27,969 hive-partitioned parquet files
- 225 MiB total on S3
- 2,052,346 rows, 21 columns
- Columns: `timeSeries`, `TIME`, `LATITUDE`, `LONGITUDE`, `WHTH`, `WPMH`, `WMXH`, `WPPE`, `WPDI`, `WPDS`, `WAVE_quality_control`, `filename`, `wmo_id`, `water_depth`, `WSSH`, `WPFM`, `WMDS`, `SSWMD`, `polygon`, `site_name`, `timestamp`

**Timing:**

| Step                                              | Duration  |
| ------------------------------------------------- | --------- |
| `read_parquet` from main S3 (27,969 files)        | ~7–8 min  |
| `COPY TO` backup S3 (single consolidated parquet) | ~1–10 sec |
| Pre-load from backup on startup                   | ~1–10 sec |

The backup write is negligible. The consolidated parquet is significantly smaller than 225 MiB because per-file overhead (footers, metadata) across 28k files is eliminated, and parquet compression on 2M rows of floats and repeated strings is efficient.

The key benefit of the backup is startup restore time: **~10 sec from backup** vs **~8 min from main S3**.

---

## What Was Implemented

Changes made to `data_access_service/core/scheduler.py`:

**Class constants** — table name and backup path defined once, used everywhere:

```python
WAVE_BUOY_BACKUP_BUCKET = "aodn-cloud-optimized-wave-buoy-backup-testing"
WAVE_BUOY_TABLE_NAME = "wave_buoy_realtime_nonqc"
WAVE_BUOY_BACKUP_S3_PATH = f"s3://{WAVE_BUOY_BACKUP_BUCKET}/{WAVE_BUOY_TABLE_NAME}.parquet"
```

**`_configure_duckdb_s3()`** — new method that creates a DuckDB named secret with credentials resolved via boto3. Called at startup and at the start of every `hourly_task` run to prevent credential expiry (ECS task role credentials expire after ~6 hours):

```python
session = boto3.Session()
creds = session.get_credentials().get_frozen_credentials()
self.memconn.execute(f"""
    CREATE OR REPLACE SECRET wave_buoy_s3 (
        TYPE S3, KEY_ID '...', SECRET '...', SESSION_TOKEN '...', REGION '...'
    )
""")
```

The source bucket (`aodn-cloud-optimised`) is public so never needed credentials. The backup bucket is private, so this is required. boto3 is used instead of DuckDB's own credential chain because it reliably handles SSO locally and ECS task roles on deployed environments.

**`hourly_task`** — two additions:

1. Calls `_configure_duckdb_s3()` at the top to refresh credentials
2. On success, writes backup: `COPY {table} TO '{WAVE_BUOY_BACKUP_S3_PATH}' (FORMAT PARQUET)`
3. On failure, attempts to restore from backup before giving up

**`start_with_initial_run`** — pre-loads from backup before launching the full S3 refresh in the background. Since the app starts accepting traffic immediately (via `asyncio.create_task`), this closes the window where the table doesn't exist but requests are already being served (~10 sec restore vs ~8 min cold read).

---

## Prerequisites

- **Backup S3 bucket/prefix** — a bucket or prefix you own, separate from the source data bucket
- **IAM permissions** — ECS task role needs `s3:PutObject` on the backup bucket in addition to existing `s3:GetObject`

## Dev local testing

- **Command** - AWS_PROFILE=AodnAdminAccess-615645230945 PROFILE=dev python3 data_access_service/server.py
