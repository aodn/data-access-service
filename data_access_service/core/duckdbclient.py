import duckdb

from tempfile import TemporaryDirectory
from data_access_service.models.pmtiles_types import PmtilesGenerationConfig
from data_access_service import Config
from threading import Lock


class DuckDBClient:
    def __init__(self):
        self._config: PmtilesGenerationConfig = Config.get_config().get_pmtiles_config()
        self._duckdb_client = None

    def get_instance(self):
        pass


class PmTileDuckDBClient(DuckDBClient):
    # Process-global singletons
    _global_db_connection = None
    _temp_dir_object = None
    _lock = Lock()

    def __init__(self):
        super().__init__()
        self._con = self.get_instance()

    def get_instance(self):
        """Initializes the single global DB instance if it does not exist."""
        if PmTileDuckDBClient._global_db_connection is None:
            with PmTileDuckDBClient._lock:
                if PmTileDuckDBClient._global_db_connection is None:
                    # Keep temporary directory alive at the class level
                    PmTileDuckDBClient._temp_dir_object = TemporaryDirectory(
                        prefix=self._config.duckdb_temp_dir
                    )

                    db_config = {
                        "temp_directory": PmTileDuckDBClient._temp_dir_object.name,
                        "memory_limit": self._config.memory_limit,
                        "threads": str(int(self._config.threads)),
                        "preserve_insertion_order": "false",
                        "unsafe_disable_etag_checks": "true",
                        "s3_region": "ap-southeast-2",
                        "s3_access_key_id": "",
                        "s3_secret_access_key": "",
                        "s3_session_token": "",
                        "s3_use_ssl": "true",
                        "s3_url_style": "path",
                        "enable_http_metadata_cache": "true",
                        "http_keep_alive": "true",
                    }

                    # Establish the primary process-global connection
                    db = duckdb.connect(self._config.duckdb_database, config=db_config)
                    db.execute("INSTALL httpfs; LOAD httpfs;")
                    db.execute("INSTALL h3 FROM community; LOAD h3;")

                    PmTileDuckDBClient._global_db_connection = db

        # CRITICAL: Return a thread-safe, independent cursor from the global connection
        self._duckdb_client = PmTileDuckDBClient._global_db_connection.cursor()
        return self._duckdb_client

    def close(self):
        self._duckdb_client.close()
        self._duckdb_client = None

    def execute(self, query: str):
        self._con.execute(query)

    def detect_time_type(
        self,
        input_path: str,
        time_col: str,
    ) -> str:
        col_quoted = PmTileDuckDBClient.quote_identifier(time_col)

        sample_path = input_path
        if not (
            input_path.endswith("_metadata") or input_path.endswith("_common_metadata")
        ):
            try:
                files = self._con.execute(
                    f"SELECT * FROM glob('{input_path}') LIMIT 1"
                ).fetchall()
                if files:
                    sample_path = files[0][0]
            except Exception:
                pass

        try:
            rows = self._con.execute(
                f"DESCRIBE SELECT {col_quoted} FROM read_parquet('{sample_path}') LIMIT 0"
            ).fetchall()
        except Exception:
            return "timestamp"

        col_type = None
        for row in rows:
            if row[0].lower() == time_col.lower():
                col_type = row[1].upper()
                break

        if col_type is None:
            raise ValueError(
                f"Column '{time_col}' not found in schema of '{input_path}'."
            )

        if any(t in col_type for t in ("TIMESTAMP", "DATE", "INTERVAL")):
            return "timestamp"
        if any(t in col_type for t in ("BIGINT", "HUGEINT", "UBIGINT")):
            return "epoch_ms"
        return "epoch_s"

    @staticmethod
    def quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def build_ym_expression(time_col: str, time_type: str) -> str:
        col = PmTileDuckDBClient.quote_identifier(time_col)

        if time_type == "timestamp":
            ts = f"CAST({col} AS TIMESTAMP)"
        elif time_type == "epoch_ms":
            ts = f"to_timestamp(CAST({col} AS DOUBLE) / 1000.0)"
        elif time_type == "epoch_s":
            ts = f"to_timestamp(CAST({col} AS DOUBLE))"
        else:
            raise ValueError(
                f"Unsupported time_type={time_type!r}. Expected one of: 'timestamp', 'epoch_ms', 'epoch_s'."
            )

        return f"CAST(strftime({ts}, '%Y%m') AS INTEGER)"
