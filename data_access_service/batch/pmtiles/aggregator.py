import os
import time
import json
from typing import Dict, Sequence, Optional
import duckdb
from .types import HexLayerSpec
from .features import build_hex_feature
from .stager import _build_staged_parquet


def aggregate_single_layer(
    con: duckdb.DuckDBPyConnection,
    staged_path: str,
    max_resolution: int,
    layer: HexLayerSpec,
    include_tippecanoe_metadata: bool,
    fetch_size: int,
) -> int:
    from data_access_service import Config, init_log

    logger = init_log(Config.get_config())

    logger.info(
        f"[{layer.name}] Starting layer: h3_resolution={layer.h3_resolution}, "
        f"zoom={layer.minzoom}-{layer.maxzoom}, output={layer.output_path!r}"
    )
    logger.debug(
        f"[{layer.name}] staged_path={staged_path!r}, max_resolution={max_resolution}"
    )

    con.execute("DROP TABLE IF EXISTS monthly_counts")

    if layer.h3_resolution == max_resolution:
        sql = f"""
            CREATE TEMP TABLE monthly_counts AS
            SELECT h_high AS h, ym, c
            FROM read_parquet('{staged_path}')
            WHERE h_high IS NOT NULL
            ORDER BY h, ym
        """
    else:
        sql = f"""
            CREATE TEMP TABLE monthly_counts AS
            SELECT
                printf('%x', h3_cell_to_parent(('0x' || h_high)::UBIGINT, {int(layer.h3_resolution)})) AS h,
                ym,
                SUM(c)::UBIGINT AS c
            FROM read_parquet('{staged_path}')
            WHERE h_high IS NOT NULL
            GROUP BY h, ym
            HAVING h IS NOT NULL
            ORDER BY h, ym
        """

    logger.info(f"[{layer.name}] Starting DuckDB aggregation from staged parquet...")
    logger.debug(f"[{layer.name}] SQL:\n{sql}")
    t0 = time.monotonic()
    con.execute(sql)
    elapsed = time.monotonic() - t0
    logger.info(f"[{layer.name}] DuckDB aggregation done in {elapsed:.1f}s")

    monthly_rows = con.execute("SELECT COUNT(*) FROM monthly_counts").fetchone()[0]
    distinct_hexes = con.execute(
        "SELECT COUNT(DISTINCT h) FROM monthly_counts"
    ).fetchone()[0]
    logger.info(
        f"[{layer.name}] Aggregation result: {distinct_hexes:,} unique H3 cells, {monthly_rows:,} (cell, month) rows"
    )

    logger.info(f"[{layer.name}] Streaming results → GeoJSONSeq: {layer.output_path!r}")
    cursor = con.execute("SELECT h, ym, c FROM monthly_counts ORDER BY h, ym")

    os.makedirs(os.path.dirname(layer.output_path) or ".", exist_ok=True)

    written = 0
    current_h: Optional[str] = None
    current_counts: Dict[int, int] = {}
    t1 = time.monotonic()

    with open(layer.output_path, "w", encoding="utf-8") as output_file:
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break

            for h_cell, ym_value, count_value in rows:
                if current_h is None:
                    current_h = h_cell

                if h_cell != current_h:
                    try:
                        feature = build_hex_feature(
                            cell=current_h,
                            month_counts=current_counts,
                            layer_name=layer.name,
                            minzoom=layer.minzoom,
                            maxzoom=layer.maxzoom,
                            include_tippecanoe_metadata=include_tippecanoe_metadata,
                        )
                        output_file.write(
                            json.dumps(feature, separators=(",", ":")) + "\n"
                        )
                        written += 1
                    except ValueError as e:
                        logger = init_log(Config.get_config())
                        logger.warning(
                            f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                        )
                    current_h = h_cell
                    current_counts = {}

                current_counts[int(ym_value)] = int(count_value)

        if current_h is not None:
            try:
                feature = build_hex_feature(
                    cell=current_h,
                    month_counts=current_counts,
                    layer_name=layer.name,
                    minzoom=layer.minzoom,
                    maxzoom=layer.maxzoom,
                    include_tippecanoe_metadata=include_tippecanoe_metadata,
                )
                output_file.write(json.dumps(feature, separators=(",", ":")) + "\n")
                written += 1
            except ValueError as e:
                logger = init_log(Config.get_config())
                logger.warning(
                    f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                )

    stream_elapsed = time.monotonic() - t1
    con.execute("DROP TABLE IF EXISTS monthly_counts")
    logger.info(
        f"[{layer.name}] Done: wrote {written:,} GeoJSON features to {layer.output_path!r} in {stream_elapsed:.1f}s"
    )

    return written


def aggregate_from_point_to_hex(
    input_path: str,
    layers: Sequence[HexLayerSpec],
    lon_col: str = "lon",
    lat_col: str = "lat",
    time_col: str = "timestamp",
    duckdb_database: str = ":memory:",
    memory_limit: str = "8GB",
    temp_dir: str = "./duckdb_tmp",
    staged_parquet_dir: str = "./parquet_staged",
    threads: int = 4,
    fetch_size: int = 100_000,
    include_tippecanoe_metadata: bool = True,
) -> Dict[str, int]:
    if not layers:
        raise ValueError("layers must contain at least one HexLayerSpec.")

    max_resolution = max(layer.h3_resolution for layer in layers)
    staged_path = os.path.join(staged_parquet_dir, "staged_high_res.parquet")

    from data_access_service import Config, init_log

    config = Config.get_config()
    logger = init_log(config)

    logger.info(
        f"aggregate_from_point_to_hex: input={input_path!r}, "
        f"layers={[l.name for l in layers]}, "
        f"lon={lon_col!r}, lat={lat_col!r}, time={time_col!r}, "
        f"max_resolution={max_resolution}, memory_limit={memory_limit}, threads={threads}"
    )

    con = duckdb.connect(database=duckdb_database)
    logger.debug(
        f"aggregate_from_point_to_hex: DuckDB connection opened (database={duckdb_database!r})"
    )
    try:
        t_cfg = time.monotonic()
        from .sql_utils import configure_duckdb, detect_time_type

        configure_duckdb(
            con=con, memory_limit=memory_limit, temp_dir=temp_dir, threads=threads
        )
        logger.info(
            f"aggregate_from_point_to_hex: DuckDB setup done in {time.monotonic() - t_cfg:.1f}s"
        )

        t_detect = time.monotonic()
        time_type = detect_time_type(con, input_path, time_col)
        logger.info(
            f"aggregate_from_point_to_hex: time_type={time_type!r} detected in {time.monotonic() - t_detect:.1f}s"
        )

        elapsed_size = _build_staged_parquet(
            con=con,
            s3_path=input_path,
            lon_col=lon_col,
            lat_col=lat_col,
            time_col=time_col,
            time_type=time_type,
            max_resolution=max_resolution,
            staged_path=staged_path,
        )
        if isinstance(elapsed_size, tuple):
            elapsed, size_mb = elapsed_size
        else:
            elapsed = elapsed_size
            size_mb = 0.0

        logger.info(
            f"_build_staged_parquet: done in {elapsed:.1f}s — staged file is {size_mb:.1f} MB"
        )

        result: Dict[str, int] = {}
        total_t0 = time.monotonic()
        for i, layer in enumerate(layers, 1):
            logger.info(f"--- Layer {i}/{len(layers)}: {layer.name} ---")
            result[layer.name] = aggregate_single_layer(
                con=con,
                staged_path=staged_path,
                max_resolution=max_resolution,
                layer=layer,
                include_tippecanoe_metadata=include_tippecanoe_metadata,
                fetch_size=fetch_size,
            )

        logger.info(
            f"aggregate_from_point_to_hex: all {len(layers)} layers done in "
            f"{time.monotonic() - total_t0:.1f}s — {result}"
        )
        return result
    finally:
        con.close()
        logger.debug("aggregate_from_point_to_hex: DuckDB connection closed")
