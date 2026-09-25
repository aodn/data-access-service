# Tiler architecture

Two independent sides that only meet on S3:

- **Batch** (`batch/tiler/`) converts zarr stores to sparse parquet and publishes
  the catalogue manifest. Runs as an AWS Batch job.
- **API** (`core/tiler_routes/` + `tiler/`) reads those files with DuckDB and
  renders tiles. Never opens a zarr.

The contract between them is the on-S3 layout and the dataclasses in
`models/tiler_parquet_types.py`.

## S3 layout

`tiler_root_dir` is `s3://{datavis_data bucket}/{tiler.config.root_prefix}`.

```
{tiler_root_dir}/
  root_metadata.json                       # catalogue: store -> products
  {store}/metadata.json                    # grid, lat/lon, timestamps, var attrs
  {store}/{variable}/{timestamp}.parquet   # sparse (i, j, value) rows
```

A store is the zarr dataset name minus `.zarr`. One parquet per variable per
timestamp, so a run only adds files and never rewrites them.

## Batch side

```mermaid
flowchart TD
    EP["entry_point.py<br/>generate-tiler-parquet"] --> GEN

    subgraph GEN["generator.generate_tiler_parquet_for_all_products"]
        DISC["discovery.discover_products"]
        GROUP["_group_by_store<br/>one conversion per store"]
        FORK["fork one worker per store<br/>(use_fork_process)"]
        ROOT["write_root_metadata<br/>upsert, drop empty stores"]
        DISC --> GROUP --> FORK --> ROOT
    end

    CFG[("config.yaml<br/>tiler.catalog.gridded_variables<br/>+ blacklist")] --> DISC
    META[("live metadata index<br/>API.iter_zarr_dataset_variables")] --> DISC

    FORK --> SYNC

    subgraph SYNC["parquet_generator.sync_store (in the worker)"]
        OPEN["zarr_registry.open_store<br/>normalise TIME/LAT/LON,<br/>reject non-grid / no-time"]
        BUILD["build_metadata<br/>coords + attrs only"]
        DIFF["_missing_by_chunk<br/>skip converted + empty ts,<br/>newest zarr chunk first"]
        FETCH["_fetch_batch<br/>read one zarr time chunk"]
        SPARSE["_sparse_rows_for_slice<br/>drop NaN, sort in BLOCK=256 order"]
        WRITE["TilerBatchDuckDBClient.write_parquet"]
        SIDE["write_metadata after each chunk"]
        OPEN --> BUILD --> DIFF --> FETCH --> SPARSE --> WRITE --> SIDE
        SIDE -.next chunk.-> FETCH
    end

    ZARR[("zarr on S3<br/>via aodn_cloud_optimised")] --> FETCH
    WRITE --> PQ[("{store}/{var}/{ts}.parquet")]
    SIDE --> SM[("{store}/metadata.json")]
    ROOT --> RM[("root_metadata.json")]
```

Key properties:

- **Incremental.** `metadata.json` lists converted timestamps and all-NaN
  (`empty_timestamps`) ones; both are skipped next run. A grid or variable
  change restarts the store.
- **One store in memory at a time.** Each store runs in a forked child that
  exits when done, so the parent's RSS doesn't accumulate.
- **Block ordering.** Rows are sorted into 256×256 blocks so one parquet row
  group covers a small area and a bbox query skips row groups in both
  directions.
- **Sidecar written after its files**, so a timestamp listed in `timestamps`
  always has parquet behind it.
- `--uuid` limits a run to one metadata record; `max_chunks_per_run` caps work
  per run.

## API side

### Startup and refresh

```mermaid
flowchart LR
    SRV["server.py lifespan"] --> WARM["startup.run_tiler_warmup"]
    SCHED["scheduler cron"] --> RC
    WARM --> RC["refresh_catalog"]

    RC --> LOAD["read root_metadata.json"]
    LOAD --> CAT["catalog.build_catalog<br/>identity + products_customisation<br/>(visual, ocean_masked, tiles)"]
    CAT --> PREG["product.registry.PRODUCTS"]
    RC --> SREG["store.registry.load_stores<br/>cache each metadata.json"]
    RC --> RETAIN["retain_stores<br/>forget removed stores"]

    WARM --> CM["load_colormaps"]
    WARM --> WK["warmup_kernels (numba)"]
    WARM --> WV["warmup_visual"]
    WARM --> READY["mark_tiler_ready<br/>else 503"]
```

Rendering config lives only here (`products_customisation`), so changing how a
product looks needs no batch rerun. The scheduler also calls `refresh_stores`
to re-read sidecars and pick up new timestamps.

### Request path

```mermaid
flowchart TD
    CLIENT["client"] --> RT

    subgraph RT["tiler_routes (FastAPI, api_key_auth + require_tiler_ready)"]
        DT["/tiler/data_tiles/{id}/{z}/{x}/{y}.png<br/>/manifest.json"]
        VT["/tiler/visual_tiles/{id}/{z}/{x}/{y}.{png,webp}<br/>/bbox, /animation, /colormaps"]
        PR["/products, /manifest, /point"]
    end

    RT --> VAL["shared: product 404, date parse,<br/>timestamp resolve, LOD bounds"]
    VAL --> RUN["run_cancellable<br/>TILE_THREAD_LIMITER, 499 on disconnect"]
    VT --> DEDUP["Deduper<br/>share one in-flight render"]
    DEDUP --> RUN

    RUN --> SLICE

    subgraph SLICE["store.slice_loader.load_slice"]
        SR["store.registry metadata<br/>(cached grid + time index)"]
        OCEAN["ocean cell_table<br/>when ocean_masked"]
        PGS["ParquetGridSource per variable<br/>lazy, holds no values"]
        SR --> PGS
        OCEAN --> PGS
    end

    SLICE --> REND

    subgraph REND["rendering"]
        RS["kernels.resample_window<br/>gather or block means"]
        MK["masks: land mask,<br/>inpaint_nearest coastal fill"]
        NM["kernels.normalize<br/>numba, 24-bit RGB / 8-bit per var"]
        CMAP["colormap + legend<br/>(visual only)"]
        ENC["encode PNG / WebP"]
        RS --> MK --> NM --> CMAP --> ENC
    end

    PGS --> REPO

    subgraph REPO["store.tiler_repository (shared TilerDuckDBClient)"]
        R1["fetch_value_range<br/>parquet footer stats when unmasked"]
        R2["fetch_point"]
        R3["fetch_gather<br/>sampled window"]
        R4["fetch_block_sums<br/>sum+count per block"]
    end

    REPO --> S3[("parquet on S3<br/>httpfs + external file cache")]
    REND --> RESP["image bytes<br/>immutable cache headers"]
```

Key properties:

- **Nothing loads a whole slice.** Every read is a DuckDB query sized by what it
  returns: a point, a sampled window, or block sums. `ParquetGridSource` keeps
  no array.
- **Predicate pushdown.** Queries carry an `i`/`j` range so DuckDB skips row
  groups, which is why batch writes in block order.
- **Ocean masking is a SEMI JOIN** against a `(i, j)` cell table built once per
  grid — it can't be applied after aggregation, so it stays in SQL.
- **One DuckDB connection, per-call cursors**, so the request threadpool reads
  concurrently. Arrow tables are registered per cursor.
- `value_range` is LRU-cached (batch never rewrites a file, so it can't go
  stale); reading it in `load_slice` turns a missing file into a 404 before
  rendering starts.
- The numba parallel kernels hold a process lock — the threading layer is not
  thread-safe.

## Tile types

| | data tiles | visual tiles |
|---|---|---|
| Grid | LOD pyramid over the native grid (`get_lod_grids`) | Web Mercator XYZ |
| Output | RGBA PNG encoding raw values for a WebGL shader | colourised PNG/WebP |
| Scalar | 24-bit value in R/G/B, mask in alpha | colormap applied server-side |
| Pair (e.g. UV) | one variable in R, one in G, mask in B | not served (`visual: false`) |
| Decoding | needs `/manifest.json` ranges | none |

`DataTileLodConfig` and `DataTileDefaults` in `config/tiler/constants.py` are
the server↔shader contract — the frontend bakes them into its shader, so
changing one without redeploying the frontend silently corrupts rendering.
