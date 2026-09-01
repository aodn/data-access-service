"""Build the estimation index for one parquet dataset.

Two passes over the source dataset, both in the same DuckDB session:

* **Pass A** reads only latitude / longitude / time and groups them into
  ``(day, lat cell, lon cell) -> row count``. Three columns, so parquet
  projection pushdown keeps the S3 traffic small. A dataset with many source
  files is scanned a chunk of files at a time, each chunk in its own process
  (see :meth:`EstimationIndexBuilder._scan_counts`).
* **Pass B** takes a sample of REAL rows, writes them to a CSV and zips it, so
  bytes-per-row and the zip ratio are measured rather than guessed.

Unlike the pmtiles staging query this must not drop rows: a dropped row is an
undercounted estimate. Rows with no position keep NULL bins, and longitudes
outside -180..180 are kept raw (the download slices raw longitudes too).
"""

import json
import math
import os
import shutil
import zipfile
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from data_access_service import Config
from data_access_service.batch.common.dataset_scan import DatasetScanBase
from data_access_service.core.duckdbclient import PmTileDuckDBClient
from data_access_service.models.estimation_types import (
    ESTIMATION_INDEX_VERSION,
    EstimationIndexConfig,
    EstimationSidecarMetadata,
    schema_fingerprint,
)
from data_access_service.models.pmtiles_types import TIMELESS_DATE_PERIOD
from data_access_service.utils.memory_utils import log_memory_usage

# Name of the temp table holding the sampled rows (Pass B).
_SAMPLE_TABLE = "estimation_sample"


class EmptyDatasetError(RuntimeError):
    """Raised when the source dataset has no rows, so there is nothing to index."""


class EstimationIndexBuilder(DatasetScanBase):
    """Writes ``{dname}.parquet`` + ``{dname}.metadata`` into the work dir."""

    def __init__(self, *args, **kwargs):
        estimation_config: EstimationIndexConfig = (
            Config.get_config().get_estimation_config()
        )
        # Own DuckDB settings, not the pmtiles job's: this build has no
        # tippecanoe subprocess to leave memory for.
        kwargs.setdefault("duckdb_tuning", estimation_config.duckdb)
        super().__init__(*args, **kwargs)
        self.estimation_config = estimation_config
        if self.estimation_config.output_dir.startswith("/"):
            raise ValueError("dir must be a relative path")
        # Set in _build_counts: False when the source has no TIME column.
        self._has_time: bool = True
        # How many base cells wide the cells in the file are; the size guard
        # raises it when the index comes out too big.
        self._bin_merge_factor: int = 1
        # Source file listing, kept because both the chunked scan and the
        # sampling pass need it and it costs one S3 listing of every file.
        self._source_files_cache: Optional[List[str]] = None

    def get_index_path(self) -> str:
        return os.path.join(
            self.work_dir,
            self.estimation_config.output_dir,
            f"{self.dataset_name}.parquet",
        )

    def get_metadata_path(self) -> str:
        """Same folder as the index output: {dname}.metadata."""
        return os.path.join(
            self.work_dir,
            self.estimation_config.output_dir,
            f"{self.dataset_name}.metadata",
        )

    def build(self) -> Tuple[str, str]:
        """Build both files and return (index_path, metadata_path)."""
        try:
            index_path = self._build_counts()
            summary = self._summarise_index(index_path)
            sample = self._sample_row_bytes()
            metadata_path = self._write_sidecar(summary, sample)
            return index_path, metadata_path
        finally:
            self._release_duckdb("after estimation index build")

    # ------------------------------------------------------------------
    # Pass A - counts
    # ------------------------------------------------------------------

    def _build_counts(self) -> str:
        """Write the (day, cell) -> count parquet, coarsening the grid if it is too big.

        The source data on S3 is read exactly ONCE, at the base bin size. When
        the result has too many rows, the coarser grid is re-aggregated from
        that local file instead of scanning S3 again - merging whole cells is
        exact for an integer factor:

            floor(floor(x / a) / n) == floor(x / (a * n))     (n a whole number)

        Re-aggregation always starts from the base file, never from the
        previous rung: 1 -> 2 -> 5 would need a step of 2.5, which is not a
        whole number and would not be exact.
        """
        index_path = self.get_index_path()
        os.makedirs(os.path.dirname(os.path.abspath(index_path)), exist_ok=True)
        base_path = index_path + ".base"

        date_key_sql = self._date_key_sql()
        log_memory_usage(self.logger, "before estimation index scan")

        self.logger.info(
            "Building estimation index for %s (bin_size=%s, has_time=%s)",
            self.dataset_name,
            self.estimation_config.bin_size,
            self._has_time,
        )
        self._scan_counts(date_key_sql, base_path)
        log_memory_usage(self.logger, "after estimation index scan")

        factors = self.estimation_config.bin_merge_factors
        for attempt, factor in enumerate(factors):
            if factor == 1:
                candidate = base_path
            else:
                self._merge_cells(base_path, index_path, factor)
                candidate = index_path

            rows = self._row_count(candidate)
            self.logger.info(
                "Estimation index for %s has %s row(s) at %s deg cells "
                "(bin_size=%s x %s)",
                self.dataset_name,
                f"{rows:,}",
                self.estimation_config.bin_size * factor,
                self.estimation_config.bin_size,
                factor,
            )

            is_last = attempt == len(factors) - 1
            if rows <= self.estimation_config.max_index_rows or is_last:
                if rows > self.estimation_config.max_index_rows:
                    self.logger.warning(
                        "Estimation index for %s still has %s rows at the "
                        "coarsest grid (%s deg, limit %s rows); keeping it",
                        self.dataset_name,
                        f"{rows:,}",
                        self.estimation_config.bin_size * factor,
                        f"{self.estimation_config.max_index_rows:,}",
                    )
                self._bin_merge_factor = factor
                if candidate == base_path:
                    os.replace(base_path, index_path)
                elif os.path.exists(base_path):
                    os.remove(base_path)
                return index_path

            self.logger.info(
                "Estimation index too big (%s rows > %s); merging cells "
                "%sx from the file already built (no S3 re-scan)",
                f"{rows:,}",
                f"{self.estimation_config.max_index_rows:,}",
                factors[attempt + 1],
            )

        # Unreachable: the loop always returns on its last rung.
        raise RuntimeError("no bin merge factor configured")

    def _scan_counts(self, date_key_sql: str, base_path: str) -> None:
        """Run pass A into ``base_path``, a chunk of source files at a time.

        DuckDB keeps per-file state (parquet metadata, reader and httpfs
        handles) for as long as the connection lives, not the statement, so one
        scan over a dataset with hundreds of thousands of files needs far more
        memory than the container has - argo needs ~15 GB on an 8 GB host.
        Scanning ``chunk_files`` files per child process makes the peak depend
        on the chunk size instead of the dataset size, and makes a transient S3
        error cost one chunk rather than the whole scan.

        Small datasets take the single-statement path and behave as before.
        """
        chunk_files = int(self.estimation_config.chunk_files)
        files = self._source_files() if chunk_files > 0 else []

        if chunk_files <= 0 or len(files) <= chunk_files:
            self.pm_client.execute(
                self._counts_sql(
                    date_key_sql,
                    self.estimation_config.bin_size,
                    base_path,
                    self.get_source_sql(),
                )
            )
            return

        chunks = [
            files[start : start + chunk_files]
            for start in range(0, len(files), chunk_files)
        ]
        self.logger.info(
            "Scanning %s in %s chunk(s) of up to %s file(s) (%s file(s) total)",
            self.dataset_name,
            len(chunks),
            f"{chunk_files:,}",
            f"{len(files):,}",
        )

        parts_dir = base_path + ".parts"
        os.makedirs(parts_dir, exist_ok=True)
        # Asked once here, not per chunk: the answer is a property of the
        # dataset, and the probe needs a connection the children do not have yet.
        union_by_name = self.pm_client.needs_union_by_name(self.get_s3_uri())

        for number, chunk in enumerate(chunks):
            part_path = os.path.join(parts_dir, f"part_{number:05d}.parquet")
            if os.path.exists(part_path):
                self.logger.info(
                    "Chunk %s/%s of %s already built; skipping",
                    number + 1,
                    len(chunks),
                    self.dataset_name,
                )
                continue
            self._scan_chunk(
                date_key_sql, chunk, part_path, union_by_name, number, len(chunks)
            )
            log_memory_usage(
                self.logger, f"after estimation chunk {number + 1}/{len(chunks)}"
            )

        self._merge_parts(parts_dir, base_path)
        shutil.rmtree(parts_dir, ignore_errors=True)

    def _scan_chunk(
        self,
        date_key_sql: str,
        files: List[str],
        part_path: str,
        union_by_name: bool,
        number: int,
        total: int,
    ) -> None:
        """Scan one chunk of files into ``part_path``.

        What makes the chunking work is that every chunk gets a connection that
        starts empty. Forking a child gives that and hands the memory back to
        the OS on exit, which closing a connection here cannot: glibc keeps
        freed memory in its own arenas. ``use_fork_process`` false (local
        debugging) runs the chunk in this process and rebuilds the connection
        afterwards instead - DuckDB releases the per-file state, even though
        the process may not shrink.
        """
        self.logger.info(
            "Estimation index chunk %s/%s of %s: %s file(s)",
            number + 1,
            total,
            self.dataset_name,
            f"{len(files):,}",
        )

        if not self.estimation_config.use_fork_process:
            self._write_chunk_counts(
                self.pm_client, date_key_sql, files, part_path, union_by_name
            )
            self._recycle_duckdb(f"between chunks ({number + 1}/{total})")
            return

        pid = os.fork()
        if pid == 0:
            # Child: never return into the parent loop.
            try:
                PmTileDuckDBClient.reset_after_fork()
                client = PmTileDuckDBClient(tuning=self.estimation_config.duckdb)
                self._write_chunk_counts(
                    client, date_key_sql, files, part_path, union_by_name
                )
                log_memory_usage(self.logger, f"chunk worker exit ({number + 1})")
                # os._exit runs no finalizer, so the temp directory has to go
                # explicitly. The connection is deliberately left open: the
                # native teardown path has SIGSEGV'd forked workers before.
                PmTileDuckDBClient.discard_temp_directory()
                os._exit(0)
            except BaseException:
                self.logger.exception(
                    "Estimation index chunk %s/%s of %s crashed",
                    number + 1,
                    total,
                    self.dataset_name,
                )
                os._exit(1)

        _, status = os.waitpid(pid, 0)
        if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
            return

        raise RuntimeError(
            f"Estimation index chunk {number + 1}/{total} of {self.dataset_name} "
            f"failed (wait status {status}); no {part_path} written"
        )

    def _write_chunk_counts(
        self,
        client: PmTileDuckDBClient,
        date_key_sql: str,
        files: List[str],
        part_path: str,
        union_by_name: bool,
    ) -> None:
        """Aggregate one chunk of files, then publish it under ``part_path``.

        Written to a temp name first so a chunk that dies half way cannot be
        mistaken for a finished one on a re-run. Not sorted: the merge below
        sorts the whole index once.
        """
        tmp_path = part_path + ".tmp"
        client.execute(
            self._counts_sql(
                date_key_sql,
                self.estimation_config.bin_size,
                tmp_path,
                client.parquet_file_list_sql(files, union_by_name=union_by_name),
                ordered=False,
            )
        )
        os.replace(tmp_path, part_path)

    def _recycle_duckdb(self, checkpoint: str) -> None:
        """Close the session and open a fresh one for the next chunk.

        Only for the in-process chunk path: the per-file state a scan builds up
        lives as long as the connection, so the next chunk has to start on a
        new one.
        """
        self._release_duckdb(checkpoint)
        self.pm_client = PmTileDuckDBClient(tuning=self.estimation_config.duckdb)

    def _merge_parts(self, parts_dir: str, base_path: str) -> None:
        """Add the chunk counts together into one base-grid file, locally.

        Every chunk used the same grid, so this is a plain SUM per cell - no
        cell arithmetic (that is :meth:`_merge_cells`) and no S3.
        """
        self.pm_client.execute(
            f"""
            COPY (
                SELECT d, lat_bin, lon_bin, SUM(c)::UBIGINT AS c
                FROM read_parquet('{parts_dir}/part_*.parquet')
                GROUP BY 1, 2, 3
                ORDER BY d, lat_bin, lon_bin
            ) TO '{base_path}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE {int(self.estimation_config.row_group_size)}
            )
            """
        )

    def _source_files(self) -> List[str]:
        """Every source parquet path, listed once and reused.

        Both the chunked scan and the Pass B sample need the listing, and for a
        dataset like argo it is 300k paths over S3 - too expensive, and too big
        in memory, to build twice.
        """
        if self._source_files_cache is None:
            rows = self.pm_client.execute(
                f"SELECT file FROM glob('{self.get_s3_uri()}') ORDER BY file"
            ).fetchall()
            self._source_files_cache = [row[0] for row in rows]
            self.logger.info(
                "Source dataset %s has %s parquet file(s)",
                self.dataset_name,
                f"{len(self._source_files_cache):,}",
            )
        return self._source_files_cache

    def _row_count(self, path: str) -> int:
        return int(
            self.pm_client.execute(
                f"SELECT COUNT(*) FROM read_parquet('{path}')"
            ).fetchone()[0]
        )

    def _merge_cells(self, base_path: str, out_path: str, factor: int) -> None:
        """Roll the base grid up into cells ``factor`` times wider, locally.

        A few million local rows instead of the whole dataset on S3. NULL bins
        stay NULL, so rows with no position keep being counted.
        """
        # Written to a temp file first: reading and writing the same parquet in
        # one statement is not safe.
        tmp_path = out_path + ".tmp"
        self.pm_client.execute(
            f"""
            COPY (
                SELECT
                    d,
                    CAST(floor(CAST(lat_bin AS DOUBLE) / {int(factor)}) AS INTEGER) AS lat_bin,
                    CAST(floor(CAST(lon_bin AS DOUBLE) / {int(factor)}) AS INTEGER) AS lon_bin,
                    SUM(c)::UBIGINT AS c
                FROM read_parquet('{base_path}')
                GROUP BY 1, 2, 3
                ORDER BY d, lat_bin, lon_bin
            ) TO '{tmp_path}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE {int(self.estimation_config.row_group_size)}
            )
            """
        )
        os.replace(tmp_path, out_path)

    def _date_key_sql(self) -> str:
        """SQL for the ``d`` (YYYYMMDD) column, or the synthetic key when timeless.

        Uses the same helpers the pmtiles job uses, so the two artefacts agree
        on what day a row belongs to.
        """
        time_col_name = self.get_time_col_name()
        if time_col_name is None:
            self._has_time = False
            self.logger.info(
                "Dataset %s has no TIME column; using synthetic day key %s "
                "(has_time=false)",
                self.dataset_name,
                TIMELESS_DATE_PERIOD,
            )
            return str(int(TIMELESS_DATE_PERIOD))

        self._has_time = True
        time_type = self.pm_client.detect_time_type(
            input_path=self.get_s3_uri(), time_col=time_col_name
        )
        return PmTileDuckDBClient.build_date_key_expression(
            time_col=time_col_name, time_type=time_type
        )

    def _counts_sql(
        self,
        date_key_sql: str,
        bin_size: float,
        index_path: str,
        source_sql: str,
        ordered: bool = True,
    ) -> str:
        """The pass A aggregation over ``source_sql``, written to ``index_path``.

        ``source_sql`` is the whole dataset for a single scan, or one chunk's
        file list. ``ordered`` is False for a chunk: only the merged index has
        to come out sorted.
        """
        quoted_lat = PmTileDuckDBClient.quote_identifier(self.get_lat_col_name())
        quoted_lon = PmTileDuckDBClient.quote_identifier(self.get_lon_col_name())
        order_by = "ORDER BY d, lat_bin, lon_bin" if ordered else ""

        # INTEGER, not SMALLINT: a dataset stored on the 0-360 longitude
        # convention reaches 3600 at bin 0.1 and would overflow a finer grid.
        # ZSTD makes the wider type almost free on disk.
        return f"""
            COPY (
                SELECT
                    {date_key_sql} AS d,
                    CAST(floor(CAST({quoted_lat} AS DOUBLE) / {bin_size}) AS INTEGER) AS lat_bin,
                    CAST(floor(CAST({quoted_lon} AS DOUBLE) / {bin_size}) AS INTEGER) AS lon_bin,
                    COUNT(*)::UBIGINT AS c
                FROM {source_sql}
                GROUP BY 1, 2, 3
                {order_by}
            ) TO '{index_path}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE {int(self.estimation_config.row_group_size)}
            )
        """

    def _summarise_index(self, index_path: str) -> dict:
        """Totals for the sidecar, read back from the index we just wrote."""
        # Bin edges of the valid lon/lat range, used to count the rows whose
        # position the download's spatial filter could never match.
        effective_bin = self.estimation_config.bin_size * self._bin_merge_factor
        lon_edge = int(math.floor(180.0 / effective_bin))
        lat_edge = int(math.floor(90.0 / effective_bin))

        row = self.pm_client.execute(
            f"""
            SELECT
                COALESCE(SUM(c), 0)::BIGINT AS total_rows,
                MIN(d) AS min_date,
                MAX(d) AS max_date,
                COALESCE(SUM(CASE WHEN lat_bin IS NULL OR lon_bin IS NULL
                                  THEN c ELSE 0 END), 0)::BIGINT AS null_position_rows,
                COALESCE(SUM(CASE WHEN lon_bin < {-lon_edge} OR lon_bin > {lon_edge}
                                    OR lat_bin < {-lat_edge} OR lat_bin > {lat_edge}
                                  THEN c ELSE 0 END), 0)::BIGINT AS out_of_range_rows,
                COALESCE(SUM(CASE WHEN d IS NULL THEN c ELSE 0 END), 0)::BIGINT AS null_time_rows
            FROM read_parquet('{index_path}')
            """
        ).fetchone()

        total_rows = int(row[0])
        if total_rows == 0:
            raise EmptyDatasetError(
                f"Source dataset {self.dataset_name} produced no rows; "
                "no estimation index written"
            )
        if row[1] is None or row[2] is None:
            raise EmptyDatasetError(
                f"Source dataset {self.dataset_name} has no usable date values; "
                "no estimation index written"
            )

        summary = {
            "total_rows": total_rows,
            "min_date": int(row[1]),
            "max_date": int(row[2]),
            "null_position_rows": int(row[3]),
            "out_of_range_position_rows": int(row[4]),
            "null_time_rows": int(row[5]),
        }
        self.logger.info(
            "Estimation index summary for %s: rows=%s dates=[%s..%s] "
            "null_position=%s out_of_range_position=%s null_time=%s",
            self.dataset_name,
            f"{summary['total_rows']:,}",
            summary["min_date"],
            summary["max_date"],
            f"{summary['null_position_rows']:,}",
            f"{summary['out_of_range_position_rows']:,}",
            f"{summary['null_time_rows']:,}",
        )
        return summary

    # ------------------------------------------------------------------
    # Pass B - bytes per row and zip ratio
    # ------------------------------------------------------------------

    def _sample_row_bytes(self) -> dict:
        """Measure bytes per CSV row and the zip ratio on a sample of real rows.

        Writing a real CSV (rather than adding up string lengths in SQL) means
        quoting, escaping, the header and float formatting are all counted. The
        zip settings match ``AWSHelper.write_csv_to_s3``, so the ratio describes
        the file the user actually downloads.
        """
        files = self._pick_sample_files()
        if not files:
            raise EmptyDatasetError(
                f"No source parquet files found for {self.dataset_name}; "
                "cannot measure bytes per row"
            )

        per_file = max(1, math.ceil(self.estimation_config.sample_rows / len(files)))
        # UNION ALL BY NAME so files whose schema drifted still line up by column.
        parts = [
            f"(SELECT * FROM read_parquet('{path}', hive_partitioning=true, "
            f"union_by_name=true) LIMIT {per_file})"
            for path in files
        ]
        self.pm_client.execute(f"DROP TABLE IF EXISTS {_SAMPLE_TABLE}")
        self.pm_client.execute(
            f"CREATE TEMP TABLE {_SAMPLE_TABLE} AS "
            + "\nUNION ALL BY NAME\n".join(parts)
        )

        sample_rows = int(
            self.pm_client.execute(f"SELECT COUNT(*) FROM {_SAMPLE_TABLE}").fetchone()[
                0
            ]
        )
        column_count = len(
            self.pm_client.execute(f"DESCRIBE {_SAMPLE_TABLE}").fetchall()
        )
        if sample_rows == 0:
            raise EmptyDatasetError(
                f"Sample of {self.dataset_name} came back empty; "
                "cannot measure bytes per row"
            )

        csv_path = os.path.join(
            self.work_dir, self.estimation_config.output_dir, "sample.csv"
        )
        os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)
        self.pm_client.execute(
            f"COPY {_SAMPLE_TABLE} TO '{csv_path}' (FORMAT CSV, HEADER)"
        )
        self.pm_client.execute(f"DROP TABLE IF EXISTS {_SAMPLE_TABLE}")

        csv_bytes = os.path.getsize(csv_path)
        header_bytes = self._first_line_bytes(csv_path)
        zip_ratio = self._zip_ratio(csv_path, csv_bytes)
        csv_bytes_per_row = max(0.0, (csv_bytes - header_bytes) / sample_rows)

        os.remove(csv_path)

        self.logger.info(
            "Sampled %s row(s) from %s file(s) of %s: %.2f CSV bytes/row, "
            "header %s bytes, zip ratio %.4f, %s column(s)",
            f"{sample_rows:,}",
            len(files),
            self.dataset_name,
            csv_bytes_per_row,
            header_bytes,
            zip_ratio,
            column_count,
        )
        return {
            "csv_bytes_per_row": round(csv_bytes_per_row, 3),
            "csv_header_bytes": header_bytes,
            "zip_ratio": round(zip_ratio, 5),
            "sample_rows": sample_rows,
            "sample_files": len(files),
            "column_count": column_count,
        }

    def _pick_sample_files(self) -> List[str]:
        """Up to ``sample_files`` source files, spread evenly over the listing.

        Evenly spread rather than the first N: the oldest files of a dataset
        often have different string columns from the newest ones.
        """
        paths = self._source_files()
        wanted = max(1, int(self.estimation_config.sample_files))
        if len(paths) <= wanted:
            return paths
        step = len(paths) / wanted
        return [paths[min(len(paths) - 1, int(i * step))] for i in range(wanted)]

    @staticmethod
    def _first_line_bytes(csv_path: str) -> int:
        """Byte length of the CSV header line, including its newline."""
        with open(csv_path, "rb") as f:
            line = f.readline()
        return len(line)

    @staticmethod
    def _zip_ratio(csv_path: str, csv_bytes: int) -> float:
        """zipped / uncompressed, using the download's own zip settings."""
        zip_path = csv_path + ".zip"
        with zipfile.ZipFile(
            zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as zf:
            zf.write(csv_path, arcname=os.path.basename(csv_path))
        zipped_bytes = os.path.getsize(zip_path)
        os.remove(zip_path)
        if csv_bytes <= 0:
            return 1.0
        return zipped_bytes / csv_bytes

    # ------------------------------------------------------------------
    # Sidecar
    # ------------------------------------------------------------------

    def _write_sidecar(self, summary: dict, sample: dict) -> str:
        metadata = EstimationSidecarMetadata(
            version=ESTIMATION_INDEX_VERSION,
            uuid=self.uuid,
            key=self.dataset_name,
            bin_size=self.estimation_config.bin_size,
            bin_merge_factor=self._bin_merge_factor,
            min_date=summary["min_date"],
            max_date=summary["max_date"],
            has_time=self._has_time,
            total_rows=summary["total_rows"],
            csv_bytes_per_row=sample["csv_bytes_per_row"],
            csv_header_bytes=sample["csv_header_bytes"],
            zip_ratio=sample["zip_ratio"],
            sample_rows=sample["sample_rows"],
            sample_files=sample["sample_files"],
            null_position_rows=summary["null_position_rows"],
            out_of_range_position_rows=summary["out_of_range_position_rows"],
            null_time_rows=summary["null_time_rows"],
            column_count=sample["column_count"],
            schema_fingerprint=self._schema_fingerprint(),
            last_updated=datetime.now(timezone.utc).isoformat(),
        )

        metadata_path = self.get_metadata_path()
        os.makedirs(os.path.dirname(os.path.abspath(metadata_path)), exist_ok=True)
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata.to_dict(), f, separators=(",", ":"))

        self.logger.info("Wrote estimation sidecar to %s", metadata_path)
        return metadata_path

    def _schema_fingerprint(self) -> str:
        """Fingerprint of the source columns, from the API's cached field names.

        Deliberately the same (cheap) source the request side reads, so the two
        can be compared. Unknown names give "", which means "cannot compare".
        """
        field_names: Optional[frozenset] = None
        get_variables = getattr(self.api, "get_dataset_variables", None)
        if callable(get_variables):
            field_names = get_variables().get(self.uuid, {}).get(self.dataset_name)
        return schema_fingerprint(field_names)
