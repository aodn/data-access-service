"""loader.load_slice + exact-instant resolution.

Existing tests in test_registry.py cover get_store_metadata + get_lod_grids. These cover
the duckdb parquet reads behind the slice, and multi-timestamp resolution.

The tiler addresses data by exact UTC instant — callers pass an already-parsed
pd.Timestamp (as core.tiler_routes.shared.parse_date_or_422 produces), and it
must match a store timestamp exactly, not a calendar-day bucket.

No zarr here: each test seeds the registry directly with a fake
TilerParquetMetadata (bypassing the metadata.json file read) and writes a real
small parquet file under tmp_path (bypassing nothing — slice_loader reads
value parquet straight off disk via duckdb, so there is no monkeypatch seam
for that half).
"""

import os

import duckdb
import numpy as np
import pandas as pd
import pytest

import data_access_service.tiler.services.store.slice_loader as loader
import data_access_service.tiler.services.store.tiler_repository as repo_module
from data_access_service.tiler.services.store.parquet_grid_source import value_range
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    variable_parquet_path,
)
from data_access_service.tiler.services.store.registry import store_registry
from tests.tiler.sparse_helpers import dense_of

STORE = "x"


@pytest.fixture(autouse=True)
def isolate_caches():
    """Clear the store registry and the value ranges before/after each test:
    every test writes different values under the same store and date."""
    store_registry.clear()
    value_range.cache_clear()
    yield
    store_registry.clear()
    value_range.cache_clear()


@pytest.fixture(autouse=True)
def tiler_root_dir(tmp_path, monkeypatch):
    """Point the registry + slice_loader at a scratch output directory."""
    monkeypatch.setattr(
        repo_module.Config.get_config(),
        "get_tiler_root_dir",
        lambda: str(tmp_path),
    )
    return tmp_path


def _seed_metadata(
    times: list[str],
    lat: list[float],
    lon: list[float],
    variables: dict[str, dict] | None = None,
) -> None:
    variables = variables or {"v": {"dtype": "float32", "attrs": {}}}
    meta = TilerParquetMetadata(
        uuid="u",
        dataset="x.zarr",
        n_i=len(lat),
        n_j=len(lon),
        lat=lat,
        lon=lon,
        timestamps=[f"{t}.000000000Z" for t in times],
        variables={
            k: TilerVariableMetadata(dtype=v["dtype"], attrs=v["attrs"])
            for k, v in variables.items()
        },
        generated_at="",
    )
    store_registry._publish(STORE, meta)


def _write_variable_parquet(tiler_root_dir, variable: str, rows: list[tuple]) -> None:
    """Write ``(timestamp, i, j, value)`` rows as batch does: one file per
    timestamp under ``x/{variable}/``."""
    by_ts: dict[str, list[tuple]] = {}
    for ts, i, j, value in rows:
        by_ts.setdefault(ts, []).append((i, j, value))
    con = duckdb.connect(":memory:")
    for ts, ts_rows in by_ts.items():
        path = variable_parquet_path(str(tiler_root_dir), STORE, variable, ts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        con.execute("CREATE OR REPLACE TABLE t (i INTEGER, j INTEGER, value FLOAT)")
        for row in ts_rows:
            con.execute("INSERT INTO t VALUES (?, ?, ?)", row)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    con.close()


def _ts(t: str) -> str:
    return f"{t}.000000000Z"


def test_load_slice_returns_the_slice_for_known_date(tiler_root_dir):
    _seed_metadata(["2024-01-15T13:00:00"], [0.0, 1.0], [0.0, 1.0])
    _write_variable_parquet(
        tiler_root_dir, "v", [(_ts("2024-01-15T13:00:00"), 0, 0, 1.0)]
    )

    result = loader.load_slice(STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"])
    assert list(result.grids) == ["v"]
    values = dense_of(result.grids["v"])
    assert values.shape == (2, 2)
    assert values[0, 0] == 1.0
    assert np.isnan(values[0, 1])


def test_load_slice_unknown_date_raises_file_not_found(tiler_root_dir):
    _seed_metadata(["2024-01-15T13:00:00"], [0.0], [0.0])
    _write_variable_parquet(tiler_root_dir, "v", [])

    with pytest.raises(
        FileNotFoundError, match="Latest available date is '2024-01-15T13:00:00Z'"
    ):
        loader.load_slice(STORE, pd.Timestamp("1999-01-01"), ["v"])


def test_load_slice_unknown_variable_names_the_variable_not_the_date(tiler_root_dir):
    _seed_metadata(["2024-01-15T13:00:00"], [0.0], [0.0])

    with pytest.raises(FileNotFoundError, match=r"NOT_A_REAL_VAR"):
        loader.load_slice(
            STORE, pd.Timestamp("2024-01-15T13:00:00"), ["NOT_A_REAL_VAR"]
        )


def test_load_slice_raises_when_resolved_timestamp_was_never_converted(tiler_root_dir):
    """The sidecar lists a timestamp whose file is missing (removed out from
    under it). Must 404 (via FileNotFoundError), not return an all-NaN
    slice."""
    _seed_metadata(["2024-01-15T13:00:00", "2024-01-16T13:00:00"], [0.0], [0.0])
    _write_variable_parquet(
        tiler_root_dir, "v", [(_ts("2024-01-15T13:00:00"), 0, 0, 1.0)]
    )

    with pytest.raises(FileNotFoundError, match="2024-01-16T130000"):
        loader.load_slice(STORE, pd.Timestamp("2024-01-16T13:00:00"), ["v"])


def test_load_slice_resolves_exact_timestamp_among_several(tiler_root_dir):
    """Multiple timestamps in one store are each independently addressable —
    requesting one exactly must serve that instant's data, not the first."""
    _seed_metadata(
        ["2024-01-15T13:00:00", "2024-01-15T14:00:00"], [0.0, 1.0], [0.0, 1.0]
    )
    _write_variable_parquet(
        tiler_root_dir,
        "v",
        [
            (_ts("2024-01-15T13:00:00"), 0, 0, 1.0),
            (_ts("2024-01-15T14:00:00"), 0, 0, 2.0),
        ],
    )

    result = loader.load_slice(STORE, pd.Timestamp("2024-01-15T14:00:00"), ["v"])

    assert result.grids["v"].value_at(0, 0) == 2.0


def test_load_slice_reads_only_the_requested_timestamp(tiler_root_dir):
    """The duckdb filter must select rows for the exact requested instant,
    never leak another timestamp's values into the reconstructed slice."""
    _seed_metadata(["2024-01-15T13:00:00", "2024-01-15T14:00:00"], [0.0], [0.0])
    _write_variable_parquet(
        tiler_root_dir,
        "v",
        [
            (_ts("2024-01-15T13:00:00"), 0, 0, 1.0),
            (_ts("2024-01-15T14:00:00"), 0, 0, 2.0),
        ],
    )

    result = loader.load_slice(STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"])
    assert result.grids["v"].value_at(0, 0) == 1.0


# --- ocean_masked flag ---

# Geographic cells relative to the committed ocean mask (lon 50–190°E, lat −60–10°):
# (-40, 150) is open Southern Ocean (valid); (-6.4, 137) is over New Guinea (masked).


def _seed_ocean(times: list[str], lats: list[float], lons: list[float]) -> None:
    _seed_metadata(times, lats, lons)
    rows = [
        (_ts(t), i, j, 1.0)
        for t in times
        for i in range(len(lats))
        for j in range(len(lons))
    ]
    return rows


def test_load_slice_ocean_masked_nulls_invalid_cells(tiler_root_dir):
    rows = _seed_ocean(["2024-01-15T13:00:00"], [-40.0, -6.4], [150.0, 137.0])
    _write_variable_parquet(tiler_root_dir, "v", rows)

    result = loader.load_slice(
        STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"], ocean_masked=True
    )
    # Open-ocean cell survives; the New Guinea land cell is nulled.
    # lat [-40, -6.4] x lon [150, 137]: (0, 0) is ocean, (1, 1) New Guinea.
    assert result.grids["v"].value_at(0, 0) == 1.0
    assert np.isnan(result.grids["v"].value_at(1, 1))


def test_load_slice_without_ocean_masked_keeps_all_cells(tiler_root_dir):
    rows = _seed_ocean(["2024-01-15T13:00:00"], [-40.0, -6.4], [150.0, 137.0])
    _write_variable_parquet(tiler_root_dir, "v", rows)

    result = loader.load_slice(
        STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"]
    )  # flag defaults off
    assert not np.isnan(dense_of(result.grids["v"])).any()


def test_load_slice_ocean_masked_range_ignores_masked_cells(tiler_root_dir):
    """The footer's min/max covers every cell in the file, so a masked
    product must work its range out from the kept cells only."""
    _seed_metadata(["2024-01-15T13:00:00"], [-40.0, -6.4], [150.0, 137.0])
    _write_variable_parquet(
        tiler_root_dir,
        "v",
        [
            (_ts("2024-01-15T13:00:00"), 0, 0, 1.0),  # ocean
            (_ts("2024-01-15T13:00:00"), 1, 1, 99.0),  # New Guinea, masked
        ],
    )
    ts = pd.Timestamp("2024-01-15T13:00:00")

    masked = loader.load_slice(STORE, ts, ["v"], ocean_masked=True).grids["v"]
    unmasked = loader.load_slice(STORE, ts, ["v"]).grids["v"]

    assert (masked.vmin, masked.vmax) == (1.0, 1.0)
    assert (unmasked.vmin, unmasked.vmax) == (1.0, 99.0)


def test_load_slice_carries_the_store_coords_and_attrs(tiler_root_dir):
    _seed_metadata(
        ["2024-01-15T13:00:00"],
        [0.0, 1.0],
        [10.0, 11.0],
        variables={"v": {"dtype": "float32", "attrs": {"units": "m"}}},
    )
    _write_variable_parquet(
        tiler_root_dir, "v", [(_ts("2024-01-15T13:00:00"), 1, 0, 5.0)]
    )

    sparse = loader.load_slice(STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"])

    assert list(sparse.lat) == [0.0, 1.0]
    assert list(sparse.lon) == [10.0, 11.0]
    assert sparse.attrs == {"v": {"units": "m"}}
    assert sparse.bounds() == (10.0, 11.0, 0.0, 1.0)
