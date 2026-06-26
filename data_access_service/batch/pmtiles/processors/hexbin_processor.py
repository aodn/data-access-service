import json
import os
from typing import Sequence, List, Optional, Dict

from data_access_service.batch.pmtiles.helpers.features_help import build_hex_feature
from data_access_service.batch.pmtiles.processors.abstract_processor import (
    AbstractProcessor,
)
from data_access_service.core.duckdbclient import PmTileDuckDBClient

from data_access_service.models.pmtiles_types import HexLayerSpec


class HexbinProcessor(AbstractProcessor):

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
        ym = PmTileDuckDBClient.build_ym_expression(
            time_col=time_col_name, time_type=time_type
        )

        self.logger.info("Staging high-res parquet file...")

        sql = f"""
                COPY (
                    SELECT
                        printf('%x', h3_latlng_to_cell(
                            CAST({quoted_lat} AS DOUBLE),
                            CAST({quoted_lon} AS DOUBLE),
                            {int(self.__get_max_res())}
                        )) AS h_high,
                        {ym} AS ym,
                        COUNT(*)::UBIGINT AS c
                    FROM read_parquet('{self.get_s3_uri()}', hive_partitioning=true, union_by_name=true)
                    WHERE
                        {quoted_lon} IS NOT NULL
                        AND {quoted_lat} IS NOT NULL
                        AND {quoted_time} IS NOT NULL
                        AND CAST({quoted_lon} AS DOUBLE) BETWEEN -180 AND 180
                        AND CAST({quoted_lat} AS DOUBLE) BETWEEN -90 AND 90
                    GROUP BY h_high, ym
                    HAVING h_high IS NOT NULL
                ) TO '{self.get_staged_path()}' (FORMAT PARQUET)
            """
        self.pm_client.execute(sql)
        self.logger.info("High-res parquet file complete.")

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
        self.pm_client.execute("DROP TABLE IF EXISTS monthly_counts")

        if layer.h3_resolution == max_res:
            sql = f"""
                        CREATE TEMP TABLE monthly_counts AS
                        SELECT h_high AS h, ym, c
                        FROM read_parquet('{self.get_staged_path()}')
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
                        FROM read_parquet('{self.get_staged_path()}')
                        WHERE h_high IS NOT NULL
                        GROUP BY h, ym
                        HAVING h IS NOT NULL
                        ORDER BY h, ym
                    """

        self.pm_client.execute(sql)

        monthly_rows = self.pm_client.execute(
            "SELECT COUNT(*) FROM monthly_counts"
        ).fetchone()[0]
        distinct_hexes = self.pm_client.execute(
            "SELECT COUNT(DISTINCT h) FROM monthly_counts"
        ).fetchone()[0]
        self.logger.info(
            f"{layer.name} Aggregation result: {distinct_hexes:,} unique H3 cells, {monthly_rows:,} (cell, month) rows"
        )

        cursor = self.pm_client.execute(
            "SELECT h, ym, c FROM monthly_counts ORDER BY h, ym"
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
            self.get_geojsonseq_dir(), layer.layer_geojsonseq_file_name
        )
        os.makedirs(os.path.dirname(geojsonseq_file_path) or ".", exist_ok=True)
        with open(geojsonseq_file_path, "w", encoding="utf-8") as output_file:
            while True:
                rows = cursor.fetchmany(self.pmtiles_config.fetch_size)
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
                            self.logger.warning(
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
                    self.logger.warning(
                        f"[{layer.name}] Skipping invalid H3 cell {current_h!r}: {e}"
                    )

        return geojsonseq_file_path
