import gzip
import json
import os
from typing import Sequence, List, Optional, Dict, Tuple

from data_access_service.batch.pmtiles.helpers.features_help import build_hex_feature
from data_access_service.batch.pmtiles.processors.abstract_processor import (
    AbstractProcessor,
)
from data_access_service.core.duckdbclient import PmTileDuckDBClient

from data_access_service.models.pmtiles_types import HexLayerSpec, TimeGroupBy
from data_access_service.utils.memory_utils import log_memory_usage


class HexbinProcessor(AbstractProcessor):

    def _time_key_sql_and_column(
        self, time_col_name: str, time_type: str
    ) -> Tuple[str, str]:
        """Return (sql_expression, staged_column_alias) for the configured time bucket."""
        group_by = self.pmtiles_config.time_group_by
        if group_by == TimeGroupBy.DATE:
            return (
                PmTileDuckDBClient.build_date_key_expression(
                    time_col=time_col_name, time_type=time_type
                ),
                "d",
            )
        # Default / month
        return (
            PmTileDuckDBClient.build_ym_expression(
                time_col=time_col_name, time_type=time_type
            ),
            "ym",
        )

    def build_staging_parquet(self):
        layers = self.get_layers()
        if not isinstance(layers[0], HexLayerSpec):
            raise ValueError(f"Expected layers to be HexLayerSpec, got {type(layers)}")
        os.makedirs(os.path.dirname(self.get_staged_path()), exist_ok=True)
        lon_name = self.get_lon_col_name()
        lat_name = self.get_lat_col_name()
        time_col_name = self.get_time_col_name()
        quoted_lon = PmTileDuckDBClient.quote_identifier(lon_name)
        quoted_lat = PmTileDuckDBClient.quote_identifier(lat_name)
        quoted_time = PmTileDuckDBClient.quote_identifier(time_col_name)
        time_type = self.pm_client.detect_time_type(
            input_path=self.get_s3_uri(), time_col=time_col_name
        )
        time_key_sql, time_col = self._time_key_sql_and_column(time_col_name, time_type)

        self.logger.info(
            "Staging high-res parquet file (time_group_by=%s, column=%s)...",
            self.pmtiles_config.time_group_by.value,
            time_col,
        )
        log_memory_usage(self.logger, "before staging scan")

        sql = f"""
                COPY (
                    SELECT
                        printf('%x', h3_latlng_to_cell(
                            CAST({quoted_lat} AS DOUBLE),
                            CAST({quoted_lon} AS DOUBLE),
                            {int(self.__get_max_res())}
                        )) AS h_high,
                        {time_key_sql} AS {time_col},
                        COUNT(*)::UBIGINT AS c
                    FROM read_parquet('{self.get_s3_uri()}', hive_partitioning=true, union_by_name=true)
                    WHERE
                        {quoted_lon} IS NOT NULL
                        AND {quoted_lat} IS NOT NULL
                        AND {quoted_time} IS NOT NULL
                        AND CAST({quoted_lon} AS DOUBLE) BETWEEN -180 AND 180
                        AND CAST({quoted_lat} AS DOUBLE) BETWEEN -90 AND 90
                    GROUP BY h_high, {time_col}
                    HAVING h_high IS NOT NULL
                ) TO '{self.get_staged_path()}' (FORMAT PARQUET)
            """
        self.pm_client.execute(sql)
        self.logger.info("High-res parquet file complete.")
        log_memory_usage(self.logger, "after staging scan")

    def get_layers(self) -> Sequence[HexLayerSpec]:
        return self.config.get_hex_layer_specs(self.dataset_name)

    def generate_geojsonseq_files(self) -> List[str]:
        """
        Query and generate files for combine into PMTiles
        :return:
        """

        layers = self.get_layers()
        if not isinstance(layers[0], HexLayerSpec):
            raise ValueError(f"Expected layers to be HexLayerSpec, got {type(layers)}")

        geojsonseq_file_paths: List[str] = []

        for layer in layers:
            geojsonseq_file_paths.append(
                self.__generate_hex_geojsonseq_file(
                    layer=layer, max_res=self.__get_max_res()
                )
            )

        return geojsonseq_file_paths

    def __get_max_res(self) -> int:
        return max(layer.h3_resolution for layer in self.get_layers())

    def __generate_hex_geojsonseq_file(self, layer: HexLayerSpec, max_res: int) -> str:
        time_col = (
            "d" if self.pmtiles_config.time_group_by == TimeGroupBy.DATE else "ym"
        )
        period_label = (
            "day" if self.pmtiles_config.time_group_by == TimeGroupBy.DATE else "month"
        )

        self.pm_client.execute("DROP TABLE IF EXISTS period_counts")

        if layer.h3_resolution == max_res:
            sql = f"""
                        CREATE TEMP TABLE period_counts AS
                        SELECT h_high AS h, {time_col}, c
                        FROM read_parquet('{self.get_staged_path()}')
                        WHERE h_high IS NOT NULL
                        ORDER BY h, {time_col}
                    """
        else:
            sql = f"""
                        CREATE TEMP TABLE period_counts AS
                        SELECT
                            printf('%x', h3_cell_to_parent(('0x' || h_high)::UBIGINT, {int(layer.h3_resolution)})) AS h,
                            {time_col},
                            SUM(c)::UBIGINT AS c
                        FROM read_parquet('{self.get_staged_path()}')
                        WHERE h_high IS NOT NULL
                        GROUP BY h, {time_col}
                        HAVING h IS NOT NULL
                        ORDER BY h, {time_col}
                    """

        self.pm_client.execute(sql)

        period_rows = self.pm_client.execute(
            "SELECT COUNT(*) FROM period_counts"
        ).fetchone()[0]
        distinct_hexes = self.pm_client.execute(
            "SELECT COUNT(DISTINCT h) FROM period_counts"
        ).fetchone()[0]
        self.logger.info(
            f"{layer.name} Aggregation result: {distinct_hexes:,} unique H3 cells, "
            f"{period_rows:,} (cell, {period_label}) rows"
        )

        cursor = self.pm_client.execute(
            f"SELECT h, {time_col}, c FROM period_counts ORDER BY h, {time_col}"
        )

        os.makedirs(
            os.path.dirname(layer.layer_geojsonseq_file_name) or ".", exist_ok=True
        )

        current_h: Optional[str] = None
        current_counts: Dict[int, int] = {}
        if self.pmtiles_config.geojsonseq_dir.startswith(
            "/"
        ) or layer.layer_geojsonseq_file_name.startswith("/"):
            raise ValueError("dir must be a relative path")

        geojsonseq_file_path = os.path.join(
            self.get_geojsonseq_dir(), layer.layer_geojsonseq_file_name + ".gz"
        )
        os.makedirs(os.path.dirname(geojsonseq_file_path) or ".", exist_ok=True)
        with gzip.open(geojsonseq_file_path, "wt", encoding="utf-8") as output_file:
            while True:
                rows = cursor.fetchmany(self.pmtiles_config.fetch_size)
                if not rows:
                    break

                for h_cell, period_value, count_value in rows:
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
                            self.logger.warning(
                                f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                            )
                        current_h = h_cell
                        current_counts = {}

                    current_counts[int(period_value)] = int(count_value)

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
                    self.logger.warning(
                        f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                    )

        log_memory_usage(self.logger, f"after {layer.name} geojsonseq written")

        # After writing (before return)
        size_mb = os.path.getsize(geojsonseq_file_path) / (1024 * 1024)
        self.logger.info(f"Generated {geojsonseq_file_path}: {size_mb:.2f} MB")

        return geojsonseq_file_path
