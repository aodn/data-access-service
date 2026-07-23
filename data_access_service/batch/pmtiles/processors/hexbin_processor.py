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
        self._log_staged_parquet_preview(time_col=time_col)
        log_memory_usage(self.logger, "after staging scan")

    def _log_staged_parquet_preview(self, time_col: str) -> None:
        """Log staged table contents so month vs date aggregation can be compared.

        Prints all rows when the table is small; otherwise a summary plus a
        contiguous window covering at least one calendar month of time keys.
        """
        staged_path = self.get_staged_path()
        try:
            stats = self.pm_client.execute(
                f"""
                SELECT
                    COUNT(*)::BIGINT AS n_rows,
                    COUNT(DISTINCT h_high)::BIGINT AS n_hex,
                    COUNT(DISTINCT {time_col})::BIGINT AS n_periods,
                    MIN({time_col}) AS min_period,
                    MAX({time_col}) AS max_period,
                    SUM(c)::BIGINT AS total_c
                FROM read_parquet('{staged_path}')
                """
            ).fetchone()
            n_rows, n_hex, n_periods, min_period, max_period, total_c = stats
            self.logger.info(
                "Staged parquet summary (time_group_by=%s, column=%s): "
                "rows=%s unique_hex=%s unique_periods=%s period_range=[%s, %s] total_count=%s",
                self.pmtiles_config.time_group_by.value,
                time_col,
                f"{n_rows:,}",
                f"{n_hex:,}",
                f"{n_periods:,}",
                min_period,
                max_period,
                f"{total_c:,}",
            )

            # Prefer full dump for small staging results (typical in tests).
            # For larger tables, dump a window of periods that covers at least
            # one month so daily keys can be compared to monthly buckets.
            max_preview_rows = 500
            if n_rows <= max_preview_rows:
                preview_sql = f"""
                    SELECT h_high, {time_col}, c
                    FROM read_parquet('{staged_path}')
                    ORDER BY {time_col}, h_high
                """
            else:
                # Take the first calendar month present (YYYYMM for month mode;
                # for date mode YYYYMMDD, filter keys sharing the first YYYYMM).
                if time_col == "ym":
                    period_filter = f"{time_col} = {int(min_period)}"
                else:
                    first_month = int(min_period) // 100  # YYYYMM from YYYYMMDD
                    period_filter = f"({time_col} // 100) = {first_month}"
                preview_sql = f"""
                    SELECT h_high, {time_col}, c
                    FROM read_parquet('{staged_path}')
                    WHERE {period_filter}
                    ORDER BY {time_col}, h_high
                    LIMIT {max_preview_rows}
                """

            rows = self.pm_client.execute(preview_sql).fetchall()
            if not rows:
                self.logger.info("Staged parquet preview: (empty)")
                print("Staged parquet preview: (empty)", flush=True)
                return

            header = f"{'h_high':<20} {time_col:<12} {'c':>12}"
            lines = [
                f"Staged parquet preview ({len(rows)} row(s), ordered by {time_col}, h_high):",
                header,
                "-" * len(header),
            ]
            for h_high, period, count in rows:
                lines.append(f"{str(h_high):<20} {period:<12} {int(count):>12}")
            # Log/print line-by-line so multi-line content is not truncated by
            # formatters and is easy to copy for month-vs-date comparison.
            for line in lines:
                self.logger.info(line)
            print("\n".join(lines), flush=True)
        except Exception as e:
            self.logger.warning("Failed to preview staged parquet: %s", e)

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

    def generate_metadata_json(self) -> str:
        """Write min/max period from staging parquet to {dname}.metadata."""
        staged_path = self.get_staged_path()
        if not os.path.exists(staged_path):
            raise FileNotFoundError(
                f"Staged parquet not found at {staged_path!r}; "
                "cannot generate metadata"
            )

        time_col = (
            "d" if self.pmtiles_config.time_group_by == TimeGroupBy.DATE else "ym"
        )
        row = self.pm_client.execute(
            f"""
            SELECT MIN({time_col}), MAX({time_col})
            FROM read_parquet('{staged_path}')
            """
        ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            raise ValueError(
                f"Staged parquet at {staged_path!r} has no period values; "
                "cannot generate metadata"
            )

        min_date = int(row[0])
        max_date = int(row[1])
        metadata = {
            "min_date": min_date,
            "max_date": max_date,
            "time_group_by": self.pmtiles_config.time_group_by.value,
        }

        metadata_path = self.get_metadata_path()
        os.makedirs(os.path.dirname(os.path.abspath(metadata_path)), exist_ok=True)
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, separators=(",", ":"))

        self.logger.info(
            "Wrote metadata to %s (min_date=%s, max_date=%s, time_group_by=%s)",
            metadata_path,
            min_date,
            max_date,
            self.pmtiles_config.time_group_by.value,
        )
        return metadata_path

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
