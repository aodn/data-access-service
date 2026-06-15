import os
from .sql_utils import quote_identifier, build_ym_expression


def _build_staged_parquet(
    con,
    s3_path: str,
    lon_col: str,
    lat_col: str,
    time_col: str,
    time_type: str,
    max_resolution: int,
    staged_path: str,
) -> None:
    os.makedirs(os.path.dirname(staged_path), exist_ok=True)

    lon = quote_identifier(lon_col)
    lat = quote_identifier(lat_col)
    time_col_quoted = quote_identifier(time_col)
    ym = build_ym_expression(time_col, time_type)

    sql = f"""
        COPY (
            SELECT
                printf('%x', h3_latlng_to_cell(
                    CAST({lat} AS DOUBLE),
                    CAST({lon} AS DOUBLE),
                    {int(max_resolution)}
                )) AS h_high,
                {ym} AS ym,
                COUNT(*)::UBIGINT AS c
            FROM read_parquet('{s3_path}', hive_partitioning=true, union_by_name=true)
            WHERE
                {lon} IS NOT NULL
                AND {lat} IS NOT NULL
                AND {time_col_quoted} IS NOT NULL
                AND CAST({lon} AS DOUBLE) BETWEEN -180 AND 180
                AND CAST({lat} AS DOUBLE) BETWEEN -90 AND 90
            GROUP BY h_high, ym
            HAVING h_high IS NOT NULL
        ) TO '{staged_path}' (FORMAT PARQUET)
    """

    con.execute(sql)

    size_mb = os.path.getsize(staged_path) / (1024**2)
    return size_mb


def build_h3_staging_parquet(
    con,
    s3_path: str,
    lon_col: str,
    lat_col: str,
    time_col: str,
    time_type: str,
    max_resolution: int,
    staged_path: str,
):
    """It reads a Parquet file from S3, and pre aggregate points into hexagons using h3, and then writes it to a local temp file. It saves the IO time and resource consumption by avoiding the need to read the file from remote S3 multiple times."""
    os.makedirs(os.path.dirname(staged_path), exist_ok=True)
    lon = quote_identifier(lon_col)
    lat = quote_identifier(lat_col)
    time = quote_identifier(time_col)
    ym = build_ym_expression(time_col, time_type)

    sql = f"""
            COPY (
                SELECT
                    printf('%x', h3_latlng_to_cell(
                        CAST({lat} AS DOUBLE),
                        CAST({lon} AS DOUBLE),
                        {int(max_resolution)}
                    )) AS h_high,
                    {ym} AS ym,
                    COUNT(*)::UBIGINT AS c
                FROM read_parquet('{s3_path}', hive_partitioning=true, union_by_name=true)
                WHERE
                    {lon} IS NOT NULL
                    AND {lat} IS NOT NULL
                    AND {time} IS NOT NULL
                    AND CAST({lon} AS DOUBLE) BETWEEN -180 AND 180
                    AND CAST({lat} AS DOUBLE) BETWEEN -90 AND 90
                GROUP BY h_high, ym
                HAVING h_high IS NOT NULL
            ) TO '{staged_path}' (FORMAT PARQUET)
        """

    con.execute(sql)
