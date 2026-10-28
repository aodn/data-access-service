from __future__ import annotations
from __future__ import annotations
from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from typing import Any
from collections.abc import Sequence
from typing import TYPE_CHECKING
from data_access_service.models.tiler_types import TilerBatchDuckDBConfig
from tempfile import TemporaryDirectory
from data_access_service.utils.sql_utils import sql_literal

import duckdb

if TYPE_CHECKING:
    import pandas as pd


class TilerBatchDuckDBClient(DuckDBClient):
    """Writes local parquet files for the tiler batch job.

    One per store, used from one thread, so SQL runs on the connection
    directly. Removes its spill directory on close. ``parquet_generator``
    uploads the files with boto3.
    """

    def __init__(self, config: TilerBatchDuckDBConfig) -> None:
        super().__init__()
        self._temp_dir = TemporaryDirectory(prefix=config.duckdb_temp_dir)
        self._con = self._connect(
            database=":memory:",
            config={
                "memory_limit": config.memory_limit,
                "threads": str(int(config.threads)),
                "temp_directory": self._temp_dir.name,
            },
        )

    def get_instance(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("TilerBatchDuckDBClient is closed")
        return self._con

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        con = self.get_instance()
        if params is None:
            return con.execute(sql)
        return con.execute(sql, params)

    def write_parquet(self, frame: pd.DataFrame, path: str) -> None:
        """Write ``frame`` to one local parquet file at ``path``."""
        con = self.get_instance()
        con.register("_frame", frame)
        try:
            # COPY ... TO takes no bound parameter for the path.
            con.execute(
                f"COPY (SELECT * FROM _frame) TO {sql_literal(path)} (FORMAT PARQUET)"
            )
        finally:
            con.unregister("_frame")

    def merge_parquet(self, paths: Sequence[str], path: str) -> None:
        """Join the parquet files ``paths``, rows in order, into one at ``path``."""
        files = ", ".join(sql_literal(p) for p in paths)
        self.get_instance().execute(
            f"COPY (SELECT * FROM read_parquet([{files}])) "
            f"TO {sql_literal(path)} (FORMAT PARQUET)"
        )

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None
        self._temp_dir.cleanup()

    def __enter__(self) -> TilerBatchDuckDBClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
