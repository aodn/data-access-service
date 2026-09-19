from data_access_service.models.tiler_parquet_types import (
    root_metadata_path,
    store_metadata_path,
    variable_parquet_path,
)


def test_layout_paths():
    base = "s3://bucket/tiler/"
    assert root_metadata_path(base) == "s3://bucket/tiler/root_metadata.json"
    assert store_metadata_path(base, "foo") == "s3://bucket/tiler/foo/metadata.json"
    assert (
        variable_parquet_path(base, "foo", "sst", "2024-01-15T13:00:00.000000000Z")
        == "s3://bucket/tiler/foo/sst/2024-01-15T130000.000000000Z.parquet"
    )
