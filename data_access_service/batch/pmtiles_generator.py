import json
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

import duckdb
import h3
from aodn_cloud_optimised.lib.DataQuery import BUCKET_OPTIMISED_DEFAULT
from shapely.geometry import Polygon, MultiPolygon, mapping
from shapely.ops import unary_union

from data_access_service import Config, init_log
from data_access_service.core.api import BaseAPI
from data_access_service.core.constants import (
    STR_LONGITUDE_UPPER_CASE,
    STR_LATITUDE_UPPER_CASE,
    STR_TIME_UPPER_CASE,
)

config = Config.get_config()
logger = init_log(config)


@dataclass(frozen=True)
class HexLayerSpec:
    """
    One output layer/resolution definition.

    Example:
        HexLayerSpec(
            name="hex_low",
            h3_resolution=4,
            minzoom=0,
            maxzoom=5,
            output_path="hex_low.geojsonseq",
        )
    """

    name: str
    h3_resolution: int
    minzoom: int
    maxzoom: int
    output_path: str


def quote_identifier(name: str) -> str:
    """Quote a SQL identifier for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def build_ym_expression(time_col: str, time_type: str) -> str:
    """
    Build a DuckDB SQL expression that converts a timestamp column to a YYYYMM integer.

    Supported time_type values:
        - "timestamp": native DuckDB TIMESTAMP / DATE column
        - "epoch_ms":  integer milliseconds since Unix epoch
        - "epoch_s":   numeric seconds since Unix epoch
    """
    col = quote_identifier(time_col)

    if time_type == "timestamp":
        ts = f"CAST({col} AS TIMESTAMP)"
    elif time_type == "epoch_ms":
        ts = f"to_timestamp(CAST({col} AS DOUBLE) / 1000.0)"
    elif time_type == "epoch_s":
        ts = f"to_timestamp(CAST({col} AS DOUBLE))"
    else:
        raise ValueError(
            f"Unsupported time_type={time_type!r}. "
            "Expected one of: 'timestamp', 'epoch_ms', 'epoch_s'."
        )

    return f"CAST(strftime({ts}, '%Y%m') AS INTEGER)"


def detect_time_type(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    time_col: str,
) -> str:
    """
    Auto-detect the time_type of a parquet column by inspecting its DuckDB SQL type.

    Returns one of:
        - "timestamp"  — TIMESTAMP / DATE / TIMESTAMPTZ columns
        - "epoch_ms"   — BIGINT / HUGEINT / UBIGINT columns (assumed milliseconds)
        - "epoch_s"    — DOUBLE / FLOAT / INTEGER columns (assumed seconds)

    Schema is read from a single representative file (via glob) to avoid fetching
    footers from every partition file — which would cause thousands of S3 requests.
    """
    col_quoted = quote_identifier(time_col)
    logger.debug(
        f"detect_time_type: inspecting column={time_col!r} from {input_path!r}"
    )

    # Resolve a single sample file so we only make one S3 footer request.
    # _metadata path (from _resolve_s3_path) is not a parquet file itself, so handle both.
    sample_path = input_path
    if not (
        input_path.endswith("_metadata") or input_path.endswith("_common_metadata")
    ):
        try:
            files = con.execute(
                f"SELECT * FROM glob('{input_path}') LIMIT 1"
            ).fetchall()
            if files:
                sample_path = files[0][0]
                logger.debug(
                    f"detect_time_type: using single sample file for schema → {sample_path!r}"
                )
        except Exception as e:
            logger.debug(
                f"detect_time_type: glob failed ({e}), falling back to full path"
            )

    try:
        rows = con.execute(
            f"DESCRIBE SELECT {col_quoted} FROM read_parquet('{sample_path}') LIMIT 0"
        ).fetchall()
    except Exception as e:
        # Some partition files may be corrupted/incomplete on S3 (e.g. mid-upload).
        # Fall back to reading schema from _common_metadata or just assume timestamp.
        logger.warning(
            f"detect_time_type: failed to read schema from {sample_path!r} ({e}). "
            f"Falling back to time_type='timestamp'."
        )
        return "timestamp"

    col_type = None
    for row in rows:
        if row[0].lower() == time_col.lower():
            col_type = row[1].upper()
            break

    if col_type is None:
        raise ValueError(f"Column '{time_col}' not found in schema of '{input_path}'.")

    logger.debug(f"detect_time_type: column={time_col!r} has SQL type={col_type!r}")

    if any(t in col_type for t in ("TIMESTAMP", "DATE", "INTERVAL")):
        return "timestamp"
    if any(t in col_type for t in ("BIGINT", "HUGEINT", "UBIGINT")):
        return "epoch_ms"
    return "epoch_s"


def h3_boundary_lnglat(cell: str) -> List[List[float]]:
    """Return a closed GeoJSON polygon ring in [lng, lat] order for an H3 cell."""
    try:
        valid = bool(cell) and h3.is_valid_cell(cell)
    except (OverflowError, ValueError):
        valid = False
    if not valid:
        raise ValueError(f"Invalid H3 cell: {cell!r}")

    if hasattr(h3, "cell_to_boundary"):  # h3-py v4
        boundary = h3.cell_to_boundary(cell)
    else:  # h3-py v3
        boundary = h3.h3_to_geo_boundary(cell)

    # h3 returns (lat, lng); GeoJSON requires [lng, lat]
    ring = [[float(lng), float(lat)] for lat, lng in boundary]

    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])

    return ring


_WORLD_BOUNDS = Polygon([(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)])


def _crosses_antimeridian(ring: List[List[float]]) -> bool:
    """Return True if any consecutive pair of vertices spans more than 180° in longitude."""
    for i in range(len(ring) - 1):
        if abs(ring[i][0] - ring[i + 1][0]) > 180:
            return True
    return False


def _unwrap_ring(ring: List[List[float]]) -> List[List[float]]:
    """
    Unwrap a polygon ring so longitudes are continuous (no 360° jumps).
    The first vertex is kept as-is; each subsequent vertex is shifted by ±360
    to minimise the longitude jump from the previous vertex.
    """
    unwrapped = [ring[0][:]]
    for i in range(1, len(ring)):
        prev_lng = unwrapped[-1][0]
        curr_lng = ring[i][0]
        delta = curr_lng - prev_lng
        if delta > 180:
            curr_lng -= 360
        elif delta < -180:
            curr_lng += 360
        unwrapped.append([curr_lng, ring[i][1]])
    return unwrapped


def build_hex_geometry(cell: str) -> Dict:
    """
    Build a valid GeoJSON geometry dict for an H3 cell.

    - Normal cells → ``{"type": "Polygon", ...}``
    - Antimeridian-crossing cells → ``{"type": "MultiPolygon", ...}``
      (split into the two halves clipped to [-180,180]).
    """
    ring = h3_boundary_lnglat(cell)

    if not _crosses_antimeridian(ring):
        return {"type": "Polygon", "coordinates": [ring]}

    # Unwrap ring to a continuous coordinate space, then clip back into [-180,180]
    unwrapped = _unwrap_ring(ring)
    poly = Polygon(unwrapped)

    # Shift the polygon so it sits fully in one hemisphere, then intersect with ±180 slabs
    pieces = []
    for shift in (0, -360, 360):
        shifted = Polygon([(x + shift, y) for x, y in poly.exterior.coords])
        clipped = shifted.intersection(_WORLD_BOUNDS)
        if not clipped.is_empty and clipped.area > 0:
            pieces.append(clipped)

    if not pieces:
        # Fallback: return the original ring as-is
        return {"type": "Polygon", "coordinates": [ring]}

    merged = unary_union(pieces)

    # Normalise to MultiPolygon for antimeridian cells
    if isinstance(merged, Polygon):
        merged = MultiPolygon([merged])

    geo = mapping(merged)
    return {"type": geo["type"], "coordinates": geo["coordinates"]}


def build_hex_feature(
    cell: str,
    month_counts: Dict[int, int],
    layer_name: str,
    minzoom: int,
    maxzoom: int,
    include_tippecanoe_metadata: bool,
) -> Dict:
    """
    Build one GeoJSON Feature for an H3 cell.

    Properties example:
        {
            "h": "872830828ffffff",
            "m202011": 200,
            "m202012": 300,
            "m202101": 180
        }
    """
    properties: Dict = {"h": cell}

    for ym in sorted(month_counts):
        count = int(month_counts[ym])
        if count != 0:
            properties[f"m{ym}"] = count

    feature = {
        "type": "Feature",
        "id": cell,
        "properties": properties,
        "geometry": build_hex_geometry(cell),
    }

    if include_tippecanoe_metadata:
        feature["tippecanoe"] = {
            "layer": layer_name,
            "minzoom": int(minzoom),
            "maxzoom": int(maxzoom),
        }

    return feature


def configure_duckdb(
    con: duckdb.DuckDBPyConnection,
    memory_limit: str,
    temp_dir: str,
    threads: int,
) -> None:
    """Configure DuckDB settings to reduce OOM risk and set up S3 + H3 access."""
    logger.debug(
        f"configure_duckdb: memory_limit={memory_limit!r}, temp_dir={temp_dir!r}, threads={threads}"
    )
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute(f"SET threads = {int(threads)}")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET unsafe_disable_etag_checks = true")
    logger.debug("configure_duckdb: installing and loading httpfs...")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'ap-southeast-2'")
    con.execute("SET s3_access_key_id = ''")
    con.execute("SET s3_secret_access_key = ''")
    con.execute("SET s3_session_token = ''")
    con.execute("SET s3_use_ssl = true")
    con.execute("SET s3_url_style = 'path'")
    # Cache HTTP metadata (parquet footers) so re-runs don't re-fetch from S3
    con.execute("SET enable_http_metadata_cache = true")
    # Increase concurrent S3 connections — default is low, this parallelises footer fetches
    con.execute("SET http_keep_alive = true")
    logger.debug("configure_duckdb: installing and loading h3 community extension...")
    con.execute("INSTALL h3 FROM community; LOAD h3;")
    logger.info(
        f"DuckDB configured: memory_limit={memory_limit}, threads={threads}, temp_dir={temp_dir!r}, httpfs+H3 loaded"
    )


def _build_staged_parquet(
    con: duckdb.DuckDBPyConnection,
    s3_path: str,
    lon_col: str,
    lat_col: str,
    time_col: str,
    time_type: str,
    max_resolution: int,
    staged_path: str,
) -> None:
    """
    Read S3 parquet ONCE.

    Selects only lon/lat/time, converts time to YYYYMM, computes H3 at
    ``max_resolution``, then writes a compact staged local parquet:

        columns: h_high (VARCHAR), ym (INTEGER), c (UBIGINT)

    All lower-resolution layers are derived from this staged file via
    ``h3_cell_to_parent`` roll-up — S3 is never read again.
    """
    os.makedirs(os.path.dirname(staged_path), exist_ok=True)

    lon = quote_identifier(lon_col)
    lat = quote_identifier(lat_col)
    time_col_quoted = quote_identifier(time_col)
    ym = build_ym_expression(time_col, time_type)

    sql = f"""
        COPY (
            SELECT
                printf('%x', h3_latlng_to_cell(
                    CAST({lat} AS DOUBLE),
                    CAST({lon} AS DOUBLE),
                    {int(max_resolution)}
                )) AS h_high,
                {ym} AS ym,
                COUNT(*)::UBIGINT AS c
            FROM read_parquet('{s3_path}', hive_partitioning=true, union_by_name=true)
            WHERE
                {lon} IS NOT NULL
                AND {lat} IS NOT NULL
                AND {time_col_quoted} IS NOT NULL
                AND CAST({lon} AS DOUBLE) BETWEEN -180 AND 180
                AND CAST({lat} AS DOUBLE) BETWEEN -90 AND 90
            GROUP BY h_high, ym
            HAVING h_high IS NOT NULL
        ) TO '{staged_path}' (FORMAT PARQUET)
    """
    logger.debug(f"_build_staged_parquet: SQL:\n{sql}")

    logger.info(
        f"_build_staged_parquet: reading S3, computing H3 res={max_resolution} + YYYYMM, "
        f"writing staged parquet → {staged_path!r}"
    )
    t0 = time.monotonic()
    con.execute(sql)
    elapsed = time.monotonic() - t0

    size_mb = os.path.getsize(staged_path) / (1024**2)
    logger.info(
        f"_build_staged_parquet: done in {elapsed:.1f}s — staged file is {size_mb:.1f} MB"
    )


def aggregate_single_layer(
    con: duckdb.DuckDBPyConnection,
    staged_path: str,
    max_resolution: int,
    layer: HexLayerSpec,
    include_tippecanoe_metadata: bool,
    fetch_size: int,
) -> int:
    """
    Aggregate one H3 resolution layer from the staged local parquet into a GeoJSONSeq file.

    - If layer.h3_resolution == max_resolution: query staged directly (h_high, ym, c).
    - Otherwise: roll up via h3_cell_to_parent(h_high, target_res), SUM(c) GROUP BY parent+ym.

    Returns:
        Number of written GeoJSON features.
    """
    logger.info(
        f"[{layer.name}] Starting layer: h3_resolution={layer.h3_resolution}, "
        f"zoom={layer.minzoom}-{layer.maxzoom}, output={layer.output_path!r}"
    )
    logger.debug(
        f"[{layer.name}] staged_path={staged_path!r}, max_resolution={max_resolution}"
    )

    con.execute("DROP TABLE IF EXISTS monthly_counts")

    if layer.h3_resolution == max_resolution:
        logger.debug(
            f"[{layer.name}] Using staged parquet directly (h3_resolution matches max_resolution)"
        )
        sql = f"""
            CREATE TEMP TABLE monthly_counts AS
            SELECT h_high AS h, ym, c
            FROM read_parquet('{staged_path}')
            WHERE h_high IS NOT NULL
            ORDER BY h, ym
        """
    else:
        logger.debug(
            f"[{layer.name}] Rolling up h_high → h3_cell_to_parent(h_high, {layer.h3_resolution})"
        )
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
        f"[{layer.name}] Aggregation result: "
        f"{distinct_hexes:,} unique H3 cells, "
        f"{monthly_rows:,} (cell, month) rows"
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
                        if written % 100_000 == 0:
                            logger.info(
                                f"[{layer.name}] GeoJSON streaming: {written:,} features written so far..."
                            )
                    except ValueError as e:
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
                logger.warning(
                    f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                )

    stream_elapsed = time.monotonic() - t1
    con.execute("DROP TABLE IF EXISTS monthly_counts")
    logger.info(
        f"[{layer.name}] Done: wrote {written:,} GeoJSON features to {layer.output_path!r} "
        f"in {stream_elapsed:.1f}s"
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
    """
    Aggregate point parquet data into H3 hexagon GeoJSONSeq files.

    Pipeline:
        1. Read S3 once → detect time type → build staged local parquet at max H3 resolution
           (columns: h_high, ym, c)
        2. For each layer:
           - highest resolution: query staged directly
           - lower resolutions: roll up via h3_cell_to_parent, SUM(c) GROUP BY parent+ym

    Args:
        input_path:          S3 glob or local parquet path.
        layers:              List of HexLayerSpec — one per H3 resolution / zoom band.
        lon_col:             Longitude column name.
        lat_col:             Latitude column name.
        time_col:            Timestamp column name (type is auto-detected).
        duckdb_database:     DuckDB database path. Default ``":memory:"``.
        memory_limit:        DuckDB memory limit, e.g. ``"8GB"``.
        temp_dir:            Directory for DuckDB spill files.
        staged_parquet_dir:  Directory for the intermediate staged parquet file
                             (h_high, ym, c at max resolution).
        threads:             DuckDB thread count.
        fetch_size:          Rows fetched from DuckDB per Python iteration.
        include_tippecanoe_metadata:
                             Attach ``tippecanoe`` layer/zoom metadata to each feature.

    Returns:
        Dict mapping layer name → number of written GeoJSON features.
    """
    if not layers:
        raise ValueError("layers must contain at least one HexLayerSpec.")

    max_resolution = max(layer.h3_resolution for layer in layers)
    staged_path = os.path.join(staged_parquet_dir, "staged_high_res.parquet")

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
        configure_duckdb(
            con=con, memory_limit=memory_limit, temp_dir=temp_dir, threads=threads
        )
        logger.info(
            f"aggregate_from_point_to_hex: DuckDB setup done in {time.monotonic() - t_cfg:.1f}s"
        )

        # Step A — detect time type (reads schema from a single sample file, no full scan)
        t_detect = time.monotonic()
        time_type = detect_time_type(con, input_path, time_col)
        logger.info(
            f"aggregate_from_point_to_hex: time_type={time_type!r} detected in {time.monotonic() - t_detect:.1f}s"
        )

        # Step B — read S3 ONCE, compute H3 at max resolution + YYYYMM, write staged parquet
        _build_staged_parquet(
            con=con,
            s3_path=input_path,
            lon_col=lon_col,
            lat_col=lat_col,
            time_col=time_col,
            time_type=time_type,
            max_resolution=max_resolution,
            staged_path=staged_path,
        )

        # Step C — process each layer from the staged parquet (local disk only, no S3)
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


def run_tippecanoe(
    geojsonseq_paths: List[str],
    output_pmtiles: str,
    extra_args: Optional[List[str]] = None,
) -> None:
    """
    Run tippecanoe to convert GeoJSONSeq files into a single .pmtiles file.

    Each GeoJSONSeq file must carry per-feature ``tippecanoe`` metadata so that
    tippecanoe assigns the correct layer and zoom range automatically.

    Args:
        geojsonseq_paths: List of .geojsonseq paths from ``aggregate_from_point_to_hex``.
        output_pmtiles:   Destination .pmtiles path.
        extra_args:       Extra tippecanoe CLI flags, e.g. ``["--attribution=IMOS"]``.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_pmtiles)), exist_ok=True)

    cmd = [
        "tippecanoe",
        f"--output={output_pmtiles}",
        "--force",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--read-parallel",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        *geojsonseq_paths,
    ]

    if extra_args:
        cmd.extend(extra_args)

    logger.info(
        f"run_tippecanoe: generating {output_pmtiles!r} from {len(geojsonseq_paths)} GeoJSONSeq file(s)"
    )
    logger.debug(f"run_tippecanoe: full command: {' '.join(cmd)}")
    t0 = time.monotonic()
    subprocess.run(cmd, check=True)
    logger.info(
        f"run_tippecanoe: done in {time.monotonic() - t0:.1f}s → {output_pmtiles!r}"
    )


def generate_pmtiles(
    parquet_s3_uri: str,
    output_pmtiles: str,
    layers: Sequence[HexLayerSpec],
    lon_col: str = "lon",
    lat_col: str = "lat",
    time_col: str = "timestamp",
    geojsonseq_dir: str = "./geojsonseq_tmp",
    staged_parquet_dir: str = "./parquet_staged",
    duckdb_database: str = ":memory:",
    memory_limit: str = "8GB",
    temp_dir: str = "./duckdb_tmp",
    threads: int = 4,
    fetch_size: int = 100_000,
    tippecanoe_extra_args: Optional[List[str]] = None,
) -> str:
    """
    Full pipeline: S3 parquet → staged local parquet → H3 GeoJSONSeq files → tippecanoe → .pmtiles.

    S3 is read exactly once regardless of the number of layers.

    Args:
        parquet_s3_uri:        S3 parquet glob URI passed to DuckDB.
        output_pmtiles:        Destination .pmtiles file path.
        layers:                List of HexLayerSpec — one per resolution band.
        lon_col:               Longitude column name.
        lat_col:               Latitude column name.
        time_col:              Timestamp column name (type is auto-detected).
        geojsonseq_dir:        Directory for intermediate GeoJSONSeq files.
        staged_parquet_dir:    Directory for the staged high-resolution parquet file.
        duckdb_database:       DuckDB database path. Default ``":memory:"``.
        memory_limit:          DuckDB memory limit, e.g. ``"8GB"``.
        temp_dir:              Directory for DuckDB spill files.
        threads:               DuckDB thread count.
        fetch_size:            Rows fetched per Python iteration.
        tippecanoe_extra_args: Extra tippecanoe CLI flags.

    Returns:
        Absolute path to the written .pmtiles file.
    """
    logger.info(
        f"generate_pmtiles: starting pipeline — "
        f"input={parquet_s3_uri!r}, output={output_pmtiles!r}, "
        f"layers={[l.name for l in layers]}"
    )
    logger.debug(
        f"generate_pmtiles: lon={lon_col!r}, lat={lat_col!r}, time={time_col!r}, "
        f"memory_limit={memory_limit}, threads={threads}, fetch_size={fetch_size}"
    )

    resolved_layers = []
    for layer in layers:
        resolved_path = (
            layer.output_path
            if os.path.dirname(layer.output_path)
            else os.path.join(geojsonseq_dir, layer.output_path)
        )
        logger.debug(
            f"generate_pmtiles: layer={layer.name!r} output_path resolved to {resolved_path!r}"
        )
        resolved_layers.append(
            HexLayerSpec(
                name=layer.name,
                h3_resolution=layer.h3_resolution,
                minzoom=layer.minzoom,
                maxzoom=layer.maxzoom,
                output_path=resolved_path,
            )
        )

    # Step 1 — S3 → staged parquet → GeoJSONSeq per layer
    logger.info("generate_pmtiles: [Step 1/2] Aggregating parquet → GeoJSONSeq...")
    t0 = time.monotonic()
    layer_counts = aggregate_from_point_to_hex(
        input_path=parquet_s3_uri,
        layers=resolved_layers,
        lon_col=lon_col,
        lat_col=lat_col,
        time_col=time_col,
        duckdb_database=duckdb_database,
        memory_limit=memory_limit,
        temp_dir=temp_dir,
        staged_parquet_dir=staged_parquet_dir,
        threads=threads,
        fetch_size=fetch_size,
        include_tippecanoe_metadata=True,
    )
    logger.info(
        f"generate_pmtiles: [Step 1/2] Done in {time.monotonic() - t0:.1f}s. Feature counts: {layer_counts}"
    )

    # Step 2 — GeoJSONSeq → .pmtiles
    logger.info("generate_pmtiles: [Step 2/2] Running tippecanoe → .pmtiles...")
    run_tippecanoe(
        geojsonseq_paths=[layer.output_path for layer in resolved_layers],
        output_pmtiles=output_pmtiles,
        extra_args=tippecanoe_extra_args,
    )

    abs_path = os.path.abspath(output_pmtiles)
    logger.info(f"generate_pmtiles: pipeline complete → {abs_path!r}")
    return abs_path


def generate_pmtiles_for(dataset_uuid: str, parquet_name: str, api: BaseAPI):
    """Entry point: resolve column names from the API and run the full pmtiles pipeline."""
    logger.info(
        f"generate_pmtiles_for: uuid={dataset_uuid!r}, parquet={parquet_name!r}"
    )

    if not isinstance(parquet_name, str) or not parquet_name.endswith(".parquet"):
        raise ValueError("parquet_name must be a string ending with '.parquet'")

    s3_uri = f"s3://{BUCKET_OPTIMISED_DEFAULT}/{parquet_name}/**/*.parquet"
    parquet_stem = parquet_name.removesuffix(".parquet")
    logger.debug(
        f"generate_pmtiles_for: s3_uri={s3_uri!r}, parquet_stem={parquet_stem!r}"
    )

    layers = [
        HexLayerSpec(
            name="hex_low",
            h3_resolution=3,
            minzoom=0,
            maxzoom=4,
            output_path=f"data/intermediate/{parquet_stem}_hex_low.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_mid",
            h3_resolution=5,
            minzoom=5,
            maxzoom=8,
            output_path=f"data/intermediate/{parquet_stem}_hex_mid.geojsonseq",
        ),
        HexLayerSpec(
            name="hex_high",
            h3_resolution=7,
            minzoom=9,
            maxzoom=12,
            output_path=f"data/intermediate/{parquet_stem}_hex_high.geojsonseq",
        ),
    ]

    lon_col_mapped = api.map_column_names(
        uuid=dataset_uuid, key=parquet_name, columns=[STR_LONGITUDE_UPPER_CASE]
    )
    lat_col_mapped = api.map_column_names(
        uuid=dataset_uuid, key=parquet_name, columns=[STR_LATITUDE_UPPER_CASE]
    )
    time_col_mapped = api.map_column_names(
        uuid=dataset_uuid, key=parquet_name, columns=[STR_TIME_UPPER_CASE]
    )

    if not lon_col_mapped:
        raise ValueError(
            f"Cannot generate pmtiles for '{parquet_name}': no LONGITUDE column found in dataset metadata."
        )
    if not lat_col_mapped:
        raise ValueError(
            f"Cannot generate pmtiles for '{parquet_name}': no LATITUDE column found in dataset metadata."
        )
    if not time_col_mapped:
        raise ValueError(
            f"Cannot generate pmtiles for '{parquet_name}': no TIME column found in dataset metadata. "
            f"This dataset may not be a time-series dataset (e.g. it is a site information table). "
            f"pmtiles generation requires a time column to aggregate monthly observation counts."
        )

    lon_col = lon_col_mapped[0]
    lat_col = lat_col_mapped[0]
    time_col = time_col_mapped[0]
    logger.info(
        f"generate_pmtiles_for: mapped columns — lon={lon_col!r}, lat={lat_col!r}, time={time_col!r}"
    )

    t0 = time.monotonic()
    generate_pmtiles(
        parquet_s3_uri=s3_uri,
        output_pmtiles=f"data/output/{parquet_stem}.pmtiles",
        layers=layers,
        lon_col=lon_col,
        lat_col=lat_col,
        time_col=time_col,
        staged_parquet_dir=f"data/parquet_staged/{parquet_stem}",
        memory_limit="20GB" if config.is_batch() else "4GB",
        threads=8 if config.is_batch() else 4,
        fetch_size=500_000 if config.is_batch() else 100_000,
    )
    logger.info(f"generate_pmtiles_for: finished in {time.monotonic() - t0:.1f}s")
