"""The metadata.json-backed store registry: no zarr, just the S3 sidecar
batch writes plus root_metadata.json's product catalogue.
"""

from unittest.mock import MagicMock

import pytest

from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    dataset_stem,
)
from data_access_service.tiler.services.product.product import (
    DataTileConfig,
    Product,
    get_lod_grids,
)
from data_access_service.tiler.services.store.registry import (
    get_available_dates,
    get_store,
    get_store_metadata,
    is_store_available,
    resolve_timestamp,
    store_registry,
    unavailable_date_message,
)

STORE_URL = "s3://aodn-cloud-optimised/foo.zarr"
OUTPUT_DIR = "s3://my-bucket/tiler"


@pytest.fixture(autouse=True)
def clear_stores():
    store_registry.clear()
    yield
    store_registry.clear()


def _meta(
    n_i: int = 2,
    n_j: int = 2,
    lat=None,
    lon=None,
    times: list[str] | None = None,
    variables: dict[str, TilerVariableMetadata] | None = None,
) -> TilerParquetMetadata:
    lat = lat if lat is not None else list(range(n_i))
    lon = lon if lon is not None else list(range(n_j))
    times = times if times is not None else ["2024-01-15T13:00:00"]
    return TilerParquetMetadata(
        uuid="u",
        dataset="foo.zarr",
        source_path=STORE_URL,
        n_i=n_i,
        n_j=n_j,
        lat=[float(x) for x in lat],
        lon=[float(x) for x in lon],
        timestamps=[f"{t}.000000000Z" for t in times],
        variables=variables
        or {
            "v": TilerVariableMetadata(
                dtype="float32", attrs={}, parquet_path="v.parquet"
            )
        },
        schema_fingerprint="",
        generated_at="",
    )


@pytest.fixture(autouse=True)
def output_dir(monkeypatch):
    """Point the registry at a fake S3 base and give it an in-memory,
    dict-backed stand-in for S3 objects instead of real S3."""
    import data_access_service.tiler.services.store.registry as registry_module

    monkeypatch.setattr(
        registry_module.Config.get_config(),
        "get_tiler_parquet_config",
        lambda: MagicMock(output_dir=OUTPUT_DIR),
    )
    s3_store: dict[str, dict] = {}

    def fake_read_json(path):
        if path not in s3_store:
            raise FileNotFoundError(f"{path!r} not found")
        return s3_store[path]

    monkeypatch.setattr(registry_module, "read_json", fake_read_json)
    return s3_store


def _write_metadata(
    s3_store: dict, dataset_stem: str, meta: TilerParquetMetadata
) -> None:
    s3_store[f"{OUTPUT_DIR}/{dataset_stem}/metadata.json"] = meta.to_dict()


def test_dataset_stem_strips_zarr_suffix():
    assert dataset_stem("s3://aodn-cloud-optimised/foo.zarr/") == "foo"
    assert dataset_stem("s3://bucket/prefix/bar.zarr") == "bar"


def test_get_store_raises_when_metadata_json_missing(output_dir):
    with pytest.raises(FileNotFoundError):
        get_store("s3://x/never-written.zarr")


def test_get_store_returns_coords_only_dataset(output_dir):
    _write_metadata(output_dir, "foo", _meta(n_i=3, n_j=4))
    result = get_store(STORE_URL)
    assert result.sizes["lat"] == 3
    assert result.sizes["lon"] == 4
    assert "time" in result.dims


def test_get_store_metadata_round_trips_variable_attrs(output_dir):
    variables = {
        "v": TilerVariableMetadata(
            dtype="float32", attrs={"units": "degree_C"}, parquet_path="v.parquet"
        )
    }
    _write_metadata(output_dir, "foo", _meta(variables=variables))
    meta = get_store_metadata(STORE_URL)
    assert meta.variables["v"].attrs["units"] == "degree_C"


def test_is_store_available_true_after_successful_load(output_dir):
    _write_metadata(output_dir, "foo", _meta())
    get_store(STORE_URL)
    assert is_store_available(STORE_URL) is True


def test_is_store_available_false_when_metadata_json_missing(output_dir):
    assert is_store_available("s3://x/never-written.zarr") is True  # optimistic default
    with pytest.raises(FileNotFoundError):
        get_store("s3://x/never-written.zarr")
    assert is_store_available("s3://x/never-written.zarr") is False


def test_resolve_timestamp_returns_native_string_for_known_date(output_dir):
    _write_metadata(output_dir, "foo", _meta(times=["2024-01-15T13:00:00"]))
    import pandas as pd

    raw = resolve_timestamp(STORE_URL, pd.Timestamp("2024-01-15T13:00:00"))
    assert raw == "2024-01-15T13:00:00.000000000Z"


def test_resolve_timestamp_returns_none_for_unknown_date(output_dir):
    _write_metadata(output_dir, "foo", _meta(times=["2024-01-15T13:00:00"]))
    import pandas as pd

    assert resolve_timestamp(STORE_URL, pd.Timestamp("1999-01-01")) is None


def test_get_available_dates_reflects_every_timestamp(output_dir):
    _write_metadata(
        output_dir,
        "foo",
        _meta(times=["2024-01-15T13:00:00", "2024-01-16T13:00:00"]),
    )
    dates = get_available_dates(STORE_URL)
    assert len(dates) == 2
    assert dates[0][0] == "2024-01-15T13:00:00Z"


def test_unavailable_date_message_hints_latest_date(output_dir):
    _write_metadata(output_dir, "foo", _meta(times=["2024-01-15T13:00:00"]))
    import pandas as pd

    # Real callers always resolve_timestamp (which loads the sidecar) first,
    # and only reach for the message when that returns None.
    resolve_timestamp(STORE_URL, pd.Timestamp("1999-01-01"))
    msg = unavailable_date_message(STORE_URL, pd.Timestamp("1999-01-01"))
    assert "Latest available date is '2024-01-15T13:00:00Z'" in msg


def test_get_lod_grids_populates_product(output_dir):
    _write_metadata(output_dir, "foo", _meta(n_i=74, n_j=102))
    product = Product(id="t1", source_path=STORE_URL, variable="v")
    assert product.data_tile.lod_grids == {}
    grids = get_lod_grids(product)
    assert grids
    assert product.data_tile.lod_grids is grids


def test_get_lod_grids_fast_path_skips_metadata_load(output_dir):
    product = Product(
        id="t2",
        source_path="s3://never/read.zarr",
        variable="v",
        data_tile=DataTileConfig(lod_grids={1: (2, 2)}),
    )
    grids = get_lod_grids(product)
    assert grids == {1: (2, 2)}
    # No metadata.json exists for this store at all; reaching for it would raise.
    assert is_store_available("s3://never/read.zarr") is True
