"""loader.load_slice + exact-instant resolution.

Existing tests in test_registry.py cover get_store_metadata + get_lod_grids. These cover
the L1 cache interaction, the duckdb parquet read, and multi-timestamp
resolution.

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
import threading
import time
from unittest.mock import MagicMock

import duckdb
import numpy as np
import pandas as pd
import pytest

import data_access_service.tiler.services.store.slice_loader as loader
import data_access_service.tiler.services.store.tiler_repository as repo_module
from data_access_service.models.tiler_parquet_types import (
    TilerParquetMetadata,
    TilerVariableMetadata,
    variable_parquet_path,
)
from data_access_service.tiler.services.store.registry import store_registry

STORE = "x"


@pytest.fixture(autouse=True)
def isolate_caches():
    """Clear the store registry before/after each test."""
    store_registry.clear()
    yield
    store_registry.clear()


@pytest.fixture(autouse=True)
def output_dir(tmp_path, monkeypatch):
    """Point the registry + slice_loader at a scratch output directory."""
    monkeypatch.setattr(
        repo_module.Config.get_config(),
        "get_tiler_parquet_config",
        lambda: MagicMock(output_dir=str(tmp_path)),
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
        schema_fingerprint="",
        generated_at="",
    )
    store_registry._publish(STORE, meta)


def _write_variable_parquet(output_dir, variable: str, rows: list[tuple]) -> None:
    """Write ``(timestamp, i, j, value)`` rows as batch does: one file per
    timestamp under ``x/{variable}/``."""
    by_ts: dict[str, list[tuple]] = {}
    for ts, i, j, value in rows:
        by_ts.setdefault(ts, []).append((i, j, value))
    con = duckdb.connect(":memory:")
    for ts, ts_rows in by_ts.items():
        path = variable_parquet_path(str(output_dir), STORE, variable, ts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        con.execute("CREATE OR REPLACE TABLE t (i INTEGER, j INTEGER, value FLOAT)")
        for row in ts_rows:
            con.execute("INSERT INTO t VALUES (?, ?, ?)", row)
        con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    con.close()


def _ts(t: str) -> str:
    return f"{t}.000000000Z"


def test_load_slice_returns_dataset_for_known_date(output_dir):
    _seed_metadata(["2024-01-15T13:00:00"], [0.0, 1.0], [0.0, 1.0])
    _write_variable_parquet(output_dir, "v", [(_ts("2024-01-15T13:00:00"), 0, 0, 1.0)])

    result = loader.load_slice(STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"])
    assert "v" in result.data_vars
    assert result["v"].shape == (2, 2)
    assert float(result["v"].isel(lat=0, lon=0)) == 1.0
    assert np.isnan(float(result["v"].isel(lat=0, lon=1)))


def test_load_slice_unknown_date_raises_file_not_found(output_dir):
    _seed_metadata(["2024-01-15T13:00:00"], [0.0], [0.0])
    _write_variable_parquet(output_dir, "v", [])

    with pytest.raises(
        FileNotFoundError, match="Latest available date is '2024-01-15T13:00:00Z'"
    ):
        loader.load_slice(STORE, pd.Timestamp("1999-01-01"), ["v"])


def test_load_slice_unknown_variable_names_the_variable_not_the_date(output_dir):
    _seed_metadata(["2024-01-15T13:00:00"], [0.0], [0.0])

    with pytest.raises(FileNotFoundError, match=r"NOT_A_REAL_VAR"):
        loader.load_slice(
            STORE, pd.Timestamp("2024-01-15T13:00:00"), ["NOT_A_REAL_VAR"]
        )


def test_load_slice_raises_when_resolved_timestamp_was_never_converted(output_dir):
    """The sidecar lists a timestamp whose file is missing (removed out from
    under it). Must 404 (via FileNotFoundError), not return an all-NaN
    slice."""
    _seed_metadata(["2024-01-15T13:00:00", "2024-01-16T13:00:00"], [0.0], [0.0])
    _write_variable_parquet(output_dir, "v", [(_ts("2024-01-15T13:00:00"), 0, 0, 1.0)])

    with pytest.raises(FileNotFoundError, match="2024-01-16T130000"):
        loader.load_slice(STORE, pd.Timestamp("2024-01-16T13:00:00"), ["v"])


def test_load_slice_resolves_exact_timestamp_among_several(output_dir):
    """Multiple timestamps in one store are each independently addressable —
    requesting one exactly must serve that instant's data, not the first."""
    _seed_metadata(
        ["2024-01-15T13:00:00", "2024-01-15T14:00:00"], [0.0, 1.0], [0.0, 1.0]
    )
    _write_variable_parquet(
        output_dir,
        "v",
        [
            (_ts("2024-01-15T13:00:00"), 0, 0, 1.0),
            (_ts("2024-01-15T14:00:00"), 0, 0, 2.0),
        ],
    )

    result = loader.load_slice(STORE, pd.Timestamp("2024-01-15T14:00:00"), ["v"])

    assert float(result["v"].isel(lat=0, lon=0)) == 2.0


def test_load_slice_reads_only_the_requested_timestamp(output_dir):
    """The duckdb filter must select rows for the exact requested instant,
    never leak another timestamp's values into the reconstructed slice."""
    _seed_metadata(["2024-01-15T13:00:00", "2024-01-15T14:00:00"], [0.0], [0.0])
    _write_variable_parquet(
        output_dir,
        "v",
        [
            (_ts("2024-01-15T13:00:00"), 0, 0, 1.0),
            (_ts("2024-01-15T14:00:00"), 0, 0, 2.0),
        ],
    )

    result = loader.load_slice_uncached(
        STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"]
    )
    assert float(result["v"].isel(lat=0, lon=0)) == 1.0


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


def test_load_slice_ocean_masked_nulls_invalid_cells(output_dir):
    rows = _seed_ocean(["2024-01-15T13:00:00"], [-40.0, -6.4], [150.0, 137.0])
    _write_variable_parquet(output_dir, "v", rows)

    result = loader.load_slice(
        STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"], ocean_masked=True
    )
    # Open-ocean cell survives; the New Guinea land cell is nulled.
    assert float(result["v"].sel(lat=-40.0, lon=150.0)) == 1.0
    assert np.isnan(float(result["v"].sel(lat=-6.4, lon=137.0)))


def test_load_slice_without_ocean_masked_keeps_all_cells(output_dir):
    rows = _seed_ocean(["2024-01-15T13:00:00"], [-40.0, -6.4], [150.0, 137.0])
    _write_variable_parquet(output_dir, "v", rows)

    result = loader.load_slice(
        STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"]
    )  # flag defaults off
    assert not np.isnan(result["v"]).any()


# --- concurrent stampede protection (always in-process, independent of CACHE_BACKEND) ---


def test_concurrent_identical_loads_share_one_compute(output_dir, monkeypatch):
    """Even under CACHE_BACKEND=none (no cache backend), concurrent identical
    load_slice calls must share one _compute_slice_from_store, not each redo the
    parquet read independently. This is what `_slice_dedup` (services.caching.deduper)
    protects — see its docstring for why this matters even without a cache."""
    _seed_metadata(["2024-01-15T13:00:00"], [0.0], [0.0])
    _write_variable_parquet(output_dir, "v", [(_ts("2024-01-15T13:00:00"), 0, 0, 1.0)])

    calls = 0
    proceed = threading.Event()
    real_compute = loader._compute_slice_from_store

    def slow_compute(*args, **kwargs):
        nonlocal calls
        calls += 1
        proceed.wait(timeout=2)
        return real_compute(*args, **kwargs)

    monkeypatch.setattr(loader, "_compute_slice_from_store", slow_compute)

    results: list = []

    def worker():
        results.append(
            loader.load_slice(STORE, pd.Timestamp("2024-01-15T13:00:00"), ["v"])
        )

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.1)  # let all threads register on the in-flight key
    proceed.set()
    for t in threads:
        t.join(timeout=2)

    assert (
        calls == 1
    ), "expected exactly one compute; the rest should share it via _slice_dedup"
    assert len(results) == 4
