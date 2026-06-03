import os
import time
import subprocess
from typing import List, Optional, Sequence
from .aggregator import aggregate_from_point_to_hex
from .types import HexLayerSpec


def run_tippecanoe(
    geojsonseq_paths: List[str],
    output_pmtiles: str,
    extra_args: Optional[List[str]] = None,
) -> None:
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

    from data_access_service import Config, init_log

    logger = init_log(Config.get_config())
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
    geojsonseq_dir: str,
    staged_parquet_dir: str,
    duckdb_temp_dir: str,
    lon_col: str = "lon",
    lat_col: str = "lat",
    time_col: str = "timestamp",
    duckdb_database: str = ":memory:",
    memory_limit: str = "8GB",
    threads: int = 4,
    fetch_size: int = 100_000,
    tippecanoe_extra_args: Optional[List[str]] = None,
) -> str:
    from data_access_service import Config, init_log

    logger = init_log(Config.get_config())

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
        resolved_layers.append(
            HexLayerSpec(
                name=layer.name,
                h3_resolution=layer.h3_resolution,
                minzoom=layer.minzoom,
                maxzoom=layer.maxzoom,
                output_path=resolved_path,
            )
        )

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
        temp_dir=duckdb_temp_dir,
        staged_parquet_dir=staged_parquet_dir,
        threads=threads,
        fetch_size=fetch_size,
        include_tippecanoe_metadata=True,
    )
    logger.info(
        f"generate_pmtiles: [Step 1/2] Done in {time.monotonic() - t0:.1f}s. Feature counts: {layer_counts}"
    )

    logger.info("generate_pmtiles: [Step 2/2] Running tippecanoe → .pmtiles...")
    run_tippecanoe(
        geojsonseq_paths=[layer.output_path for layer in resolved_layers],
        output_pmtiles=output_pmtiles,
        extra_args=tippecanoe_extra_args,
    )

    abs_path = os.path.abspath(output_pmtiles)
    logger.info(f"generate_pmtiles: pipeline complete → {abs_path!r}")
    return abs_path
