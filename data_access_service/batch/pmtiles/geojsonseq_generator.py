import json
import os.path
import time
from typing import Sequence, Optional, Dict

import duckdb

from data_access_service.batch.pmtiles.features import build_hex_feature
from data_access_service.batch.pmtiles.sql_utils import (
    configure_duckdb,
    detect_time_type,
)
from data_access_service.batch.pmtiles.stager import build_h3_staging_parquet
from data_access_service.models.pmtiles_types import (
    HexLayerSpec,
    PmtilesGenerationConfig,
)


def generate_hex_geojsonseq_flles(
    parquet_s3_uri: str,
    layers: Sequence[HexLayerSpec],
    lon_col: str,
    lat_col: str,
    time_col: str,
    root_work_dir: str,
) -> str:
    from data_access_service import Config, init_log

    if not layers:
        raise ValueError("layers must contain at least one HexLayerSpec.")
    config = Config.get_config()
    logger = init_log(config)
    pmtiles_configs: PmtilesGenerationConfig = config.get_pmtiles_generation_config()

    logger.info(f"generate_hex_geojsonseq: input={parquet_s3_uri}, layers={layers}")
    t0 = time.monotonic()
    staged_dir = pmtiles_configs.staged_parquet_dir
    duckdb_temp_dir = pmtiles_configs.duckdb_temp_dir
    if staged_dir.startswith("/") or duckdb_temp_dir.startswith("/"):
        raise ValueError("dir must be a relative path")
    staged_path = os.path.join(root_work_dir, staged_dir, "staged_high_res.parquet")
    duckdb_temp_path = os.path.join(root_work_dir, duckdb_temp_dir)
    logger.info(
        f"generate_hex_geojsonseq: staging high-res parquet to {staged_path}..."
    )

    con = duckdb.connect(database=pmtiles_configs.duckdb_database)
    try:
        configure_duckdb(
            con=con,
            memory_limit=pmtiles_configs.memory_limit,
            temp_dir=duckdb_temp_path,
            threads=pmtiles_configs.threads,
        )
        time_type = detect_time_type(con, input_path=parquet_s3_uri, time_col=time_col)

        max_res = max(layers.h3_resolution for layers in layers)
        build_h3_staging_parquet(
            con=con,
            s3_path=parquet_s3_uri,
            lon_col=lon_col,
            lat_col=lat_col,
            time_col=time_col,
            time_type=time_type,
            max_resolution=max_res,
            staged_path=staged_path,
        )
        for layer in layers:
            __generate_hex_geojsonseq_file(
                con=con,
                staged_dataset_path=staged_path,
                max_resolution=max_res,
                layer=layer,
                fetch_size=pmtiles_configs.fetch_size,
                pmtiles_config=pmtiles_configs,
            )

    finally:
        con.close()
        logger.debug("aggregate_from_point_to_hex: DuckDB connection closed")


def __generate_hex_geojsonseq_file(
    con: duckdb.DuckDBPyConnection,
    staged_dataset_path: str,
    max_resolution: int,
    layer: HexLayerSpec,
    fetch_size: int,
    pmtiles_config: PmtilesGenerationConfig,
):
    from data_access_service import Config, init_log

    config = Config.get_config()
    logger = init_log(config)

    logger.info(f"{layer.name} Starding layer: h3 resolution: {layer.h3_resolution}")

    con.execute("DROP TABLE IF EXISTS monthly_counts")

    if layer.h3_resolution == max_resolution:
        sql = f"""
            CREATE TEMP TABLE monthly_counts AS
            SELECT h_high AS h, ym, c
            FROM read_parquet('{staged_dataset_path}')
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
            FROM read_parquet('{staged_dataset_path}')
            WHERE h_high IS NOT NULL
            GROUP BY h, ym
            HAVING h IS NOT NULL
            ORDER BY h, ym
        """

    con.execute(sql)

    monthly_rows = con.execute("SELECT COUNT(*) FROM monthly_counts").fetchone()[0]
    distinct_hexes = con.execute(
        "SELECT COUNT(DISTINCT h) FROM monthly_counts"
    ).fetchone()[0]
    logger.info(
        f"{layer.name} Aggregation result: {distinct_hexes:,} unique H3 cells, {monthly_rows:,} (cell, month) rows"
    )

    cursor = con.execute("SELECT h, ym, c FROM monthly_counts ORDER BY h, ym")

    os.makedirs(os.path.dirname(layer.layer_geojsonseq_file_name) or ".", exist_ok=True)

    current_h: Optional[str] = None
    current_counts: Dict[int, int] = {}
    geojsonseq_file_path = os.path.join(
        pmtiles_config.geojsonseq_dir, layer.layer_geojsonseq_file_name
    )
    with open(geojsonseq_file_path, "w", encoding="utf-8") as output_file:
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
                            include_tippecanoe_metadata=True,
                        )
                        output_file.write(
                            json.dumps(feature, separators=(",", ":")) + "\n"
                        )
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
                    include_tippecanoe_metadata=True,
                )
                output_file.write(json.dumps(feature, separators=(",", ":")) + "\n")
            except ValueError as e:
                logger = init_log(Config.get_config())
                logger.warning(
                    f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                )
