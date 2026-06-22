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


class PmtileDuckDBClient(DuckDBClient):
    # Class-level variables shared across ALL instances in the process
    _instance_client: duckdb.DuckDBPyConnection = None
    _temp_dir_object = None
    _lock = Lock()  # Prevents race conditions if multiple threads call this at once

    def __init__(self):
        super().__init__()

    def get_instance(self):
        # First check (fast, no lock overhead)
        if PmtileDuckDBClient._instance_client is None:
            # Thread-safety lock to ensure only one thread spins up the client
            with PmtileDuckDBClient._lock:
                # Second check (critical for thread safety)
                if PmtileDuckDBClient._instance_client is None:

                    # 1. Instantiate the temp directory object cleanly without a 'with' block
                    # This ensures the directory stays alive for the life of the process
                    PmtileDuckDBClient._temp_dir_object = TemporaryDirectory(
                        prefix=self._config.duckdb_temp_dir
                    )
                    temp_dir_path = PmtileDuckDBClient._temp_dir_object.name

                    # 2. Build configuration mapping
                    db_config = {
                        "temp_directory": temp_dir_path,
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

                    # 3. Create the process-global client connection
                    client = duckdb.connect(
                        self._config.duckdb_database, config=db_config
                    )
                    client.execute("INSTALL httpfs; LOAD httpfs;")
                    client.execute("INSTALL h3 FROM community; LOAD h3;")

                    # 4. Save to the class-level variable
                    PmtileDuckDBClient._instance_client = client

        # Assign it back to the instance variable for your base class architecture
        self._duckdb_client = PmtileDuckDBClient._instance_client
        return self._duckdb_client

    @staticmethod
    def quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def build_ym_expression(time_col: str, time_type: str) -> str:
        col = PmtileDuckDBClient.quote_identifier(time_col)

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

    @staticmethod
    def detect_time_type(
        input_path: str,
        time_col: str,
    ) -> str:
        col_quoted = PmtileDuckDBClient.quote_identifier(time_col)

        sample_path = input_path
        if not (
            input_path.endswith("_metadata") or input_path.endswith("_common_metadata")
        ):
            try:
                files = PmtileDuckDBClient._instance_client.execute(
                    f"SELECT * FROM glob('{input_path}') LIMIT 1"
                ).fetchall()
                if files:
                    sample_path = files[0][0]
            except Exception:
                pass

        try:
            rows = PmtileDuckDBClient._instance_client.execute(
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
