import os

import duckdb


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_ym_expression(time_col: str, time_type: str) -> str:
    col = quote_identifier(time_col)

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


def detect_time_type(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    time_col: str,
) -> str:
    col_quoted = quote_identifier(time_col)

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
        except Exception:
            pass

    try:
        rows = con.execute(
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
        raise ValueError(f"Column '{time_col}' not found in schema of '{input_path}'.")

    if any(t in col_type for t in ("TIMESTAMP", "DATE", "INTERVAL")):
        return "timestamp"
    if any(t in col_type for t in ("BIGINT", "HUGEINT", "UBIGINT")):
        return "epoch_ms"
    return "epoch_s"


def configure_duckdb(
    con: duckdb.DuckDBPyConnection,
    memory_limit: str,
    temp_dir: str,
    threads: int,
) -> None:
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{temp_dir}'")
    con.execute(f"SET threads = {int(threads)}")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET unsafe_disable_etag_checks = true")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'ap-southeast-2'")
    con.execute("SET s3_access_key_id = ''")
    con.execute("SET s3_secret_access_key = ''")
    con.execute("SET s3_session_token = ''")
    con.execute("SET s3_use_ssl = true")
    con.execute("SET s3_url_style = 'path'")
    con.execute("SET enable_http_metadata_cache = true")
    con.execute("SET http_keep_alive = true")
    con.execute("INSTALL h3 FROM community; LOAD h3;")
