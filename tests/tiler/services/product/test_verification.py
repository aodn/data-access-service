"""Candidate verification against the stores that actually opened.

Two things are being pinned here. The guards are per *product*, so one bad
variable must not take out its siblings on the same store. And a store failure
is classified by what the store said, never by which products sit on it — no
product is privileged, so nothing here can take the whole tiler down.
"""

import numpy as np
import pytest
import xarray as xr

from data_access_service.tiler.services.product import verification
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.verification import (
    NO_TIME_DIMENSION,
    NOT_GRIDDED,
    STORE_ABSENT,
    VARIABLE_ABSENT,
    verify_candidate_products,
)
from data_access_service.tiler.services.store.registry import NotGriddedStoreError

GRID = "s3://b/grid.zarr"
OTHER = "s3://b/other.zarr"


def _dataset(variables=("sst",), *, with_time=True) -> xr.Dataset:
    dims = {"time": 3, "lat": 4, "lon": 5} if with_time else {"lat": 4, "lon": 5}
    coords = {k: np.arange(v, dtype=float) for k, v in dims.items()}
    return xr.Dataset(
        {
            name: xr.DataArray(
                np.zeros(list(dims.values())), dims=list(dims), coords=coords
            )
            for name in variables
        }
    )


def _product(product_id, source_path=GRID, variable="sst") -> Product:
    return Product(id=product_id, source_path=source_path, variable=variable)


@pytest.fixture
def stores(monkeypatch):
    """Back `cached_store` with a plain dict of already-open datasets."""
    opened: dict[str, xr.Dataset] = {}
    monkeypatch.setattr(verification, "cached_store", opened.get)
    return opened


# --- happy path -------------------------------------------------------------


def test_all_guards_passing_keeps_every_candidate(stores):
    stores[GRID] = _dataset(("sst", "ssta"))
    candidates = {
        "a:sst": _product("a:sst", variable="sst"),
        "a:ssta": _product("a:ssta", variable="ssta"),
    }

    result = verify_candidate_products(candidates, {GRID: None})

    assert set(result.products) == {"a:sst", "a:ssta"}
    assert result.rejections == []
    assert result.unresolved_stores == []


def test_pair_with_both_variables_present_is_kept(stores):
    stores[GRID] = _dataset(("UCUR", "VCUR"))
    candidates = {"a:ucur+vcur": _product("a:ucur+vcur", variable=["UCUR", "VCUR"])}

    result = verify_candidate_products(candidates, {GRID: None})

    assert set(result.products) == {"a:ucur+vcur"}


# --- store not gridded ------------------------------------------------------


def test_not_gridded_store_drops_every_candidate_on_it(stores):
    stores[GRID] = _dataset(("sst",))
    candidates = {
        "flat:sst": _product("flat:sst", source_path=OTHER),
        "flat:ssta": _product("flat:ssta", source_path=OTHER, variable="ssta"),
        "grid:sst": _product("grid:sst"),
    }

    result = verify_candidate_products(
        candidates, {GRID: None, OTHER: NotGriddedStoreError("no lat/lon")}
    )

    # Only those, and all of those.
    assert set(result.products) == {"grid:sst"}
    assert {r.product_id for r in result.rejections} == {"flat:sst", "flat:ssta"}
    assert {r.category for r in result.rejections} == {NOT_GRIDDED}


# --- stores that do not exist ----------------------------------------------


def test_absent_store_drops_every_candidate_on_it(stores):
    stores[GRID] = _dataset(("sst",))
    candidates = {
        "gone:sst": _product("gone:sst", source_path=OTHER),
        "gone:ssta": _product("gone:ssta", source_path=OTHER, variable="ssta"),
        "grid:sst": _product("grid:sst"),
    }

    result = verify_candidate_products(
        candidates, {GRID: None, OTHER: FileNotFoundError("No such file")}
    )

    assert set(result.products) == {"grid:sst"}
    assert {r.product_id for r in result.rejections} == {"gone:sst", "gone:ssta"}
    assert {r.category for r in result.rejections} == {STORE_ABSENT}
    # Not counted as operationally unknown — there is no uncertainty here.
    assert result.unresolved_stores == []


def test_absent_store_is_dropped_whatever_the_product_id(stores):
    pid = "model_sea_level_anomaly_gridded_realtime:gsla"
    candidates = {pid: _product(pid, variable="GSLA")}

    result = verify_candidate_products(
        candidates, {GRID: FileNotFoundError("No such file")}
    )

    assert result.products == {}
    assert result.rejections[0].category == STORE_ABSENT
    assert result.rejections[0].product_id == pid


# --- guard 1: variable presence --------------------------------------------


def test_absent_variable_is_rejected_and_named(stores):
    stores[GRID] = _dataset(("sst",))
    candidates = {"a:missing": _product("a:missing", variable="missing")}

    result = verify_candidate_products(candidates, {GRID: None})

    assert result.products == {}
    assert len(result.rejections) == 1
    rejection = result.rejections[0]
    assert rejection.category == VARIABLE_ABSENT
    assert "missing" in rejection.reason


def test_pair_with_only_one_variable_present_is_rejected(stores):
    stores[GRID] = _dataset(("UCUR",))
    candidates = {"a:ucur+vcur": _product("a:ucur+vcur", variable=["UCUR", "VCUR"])}

    result = verify_candidate_products(candidates, {GRID: None})

    assert result.products == {}
    assert result.rejections[0].category == VARIABLE_ABSENT
    assert "VCUR" in result.rejections[0].reason
    assert "UCUR" not in result.rejections[0].reason


def test_rejection_drops_only_the_affected_product(stores):
    """The sibling on the same store still publishes — this is the difference
    between "one variable drifted" and "a store's worth of products vanished"."""
    stores[GRID] = _dataset(("sst",))
    candidates = {
        "a:sst": _product("a:sst", variable="sst"),
        "a:gone": _product("a:gone", variable="gone"),
    }

    result = verify_candidate_products(candidates, {GRID: None})

    assert set(result.products) == {"a:sst"}
    assert [r.product_id for r in result.rejections] == ["a:gone"]


# --- guard 2: time dimension ------------------------------------------------


def test_store_without_time_dimension_is_rejected(stores):
    stores[GRID] = _dataset(("sst",), with_time=False)
    candidates = {"a:sst": _product("a:sst")}

    result = verify_candidate_products(candidates, {GRID: None})

    assert result.products == {}
    assert result.rejections[0].category == NO_TIME_DIMENSION


def test_variable_presence_is_checked_before_time_dimension(stores):
    stores[GRID] = _dataset(("sst",), with_time=False)
    candidates = {"a:gone": _product("a:gone", variable="gone")}

    result = verify_candidate_products(candidates, {GRID: None})

    assert result.rejections[0].category == VARIABLE_ABSENT


# --- graded store failure policy -------------------------------------------


def test_unresolved_store_keeps_its_candidates_and_logs_at_error(stores, caplog):
    candidates = {"a:sst": _product("a:sst")}

    with caplog.at_level("ERROR"):
        result = verify_candidate_products(candidates, {GRID: RuntimeError("s3 down")})

    assert set(result.products) == {"a:sst"}
    assert result.unresolved_stores == [GRID]
    assert result.rejections == []
    assert any(r.levelname == "ERROR" for r in caplog.records)


def test_no_product_id_makes_an_unresolved_store_fatal(stores):
    """Verification never raises on a store failure. An unresolved store degrades
    its own products and nothing else, however long-standing they are."""
    pid = "model_sea_level_anomaly_gridded_realtime:gsla"
    candidates = {pid: _product(pid, variable="GSLA")}

    result = verify_candidate_products(candidates, {GRID: RuntimeError("s3 down")})

    assert set(result.products) == {pid}
    assert result.unresolved_stores == [GRID]


def test_degradation_is_scoped_to_the_store_that_failed(stores):
    """Only the store that failed matters; a healthy store's products are
    untouched by an unrelated degradation."""
    stores[GRID] = _dataset(("GSLA",))
    candidates = {
        "sla:gsla": _product("sla:gsla", variable="GSLA"),
        "radar:sst": _product("radar:sst", source_path=OTHER),
    }

    result = verify_candidate_products(
        candidates, {GRID: None, OTHER: RuntimeError("s3 down")}
    )

    assert set(result.products) == {"sla:gsla", "radar:sst"}
    assert result.unresolved_stores == [OTHER]


def test_success_reported_but_nothing_cached_is_treated_as_unresolved(stores):
    """Same uncertainty as a failed open, so it takes the same graded path
    rather than silently publishing a product nobody verified."""
    candidates = {"a:sst": _product("a:sst")}

    result = verify_candidate_products(candidates, {GRID: None})

    assert set(result.products) == {"a:sst"}
    assert result.unresolved_stores == [GRID]


def test_missing_prewarm_outcome_is_a_wiring_error(stores):
    candidates = {"a:sst": _product("a:sst")}

    with pytest.raises(ValueError, match="No prewarm outcome"):
        verify_candidate_products(candidates, {})


# --- reporting --------------------------------------------------------------


def test_log_rejections_breaks_counts_down_by_cause(stores, caplog):
    stores[GRID] = _dataset(("sst",))
    candidates = {
        "a:sst": _product("a:sst"),
        "a:gone": _product("a:gone", variable="gone"),
        "flat:sst": _product("flat:sst", source_path=OTHER),
    }
    result = verify_candidate_products(
        candidates, {GRID: None, OTHER: NotGriddedStoreError("no lat/lon")}
    )

    with caplog.at_level("INFO"):
        result.log_rejections()

    summary = [r.getMessage() for r in caplog.records if "Verification:" in r.message]
    assert len(summary) == 1
    assert "1 products kept, 2 rejected" in summary[0]
    assert (
        "store not gridded 1, store absent 0, variable absent 1, no time dimension 0"
        in summary[0]
    )
    # One line per dropped product, naming it.
    assert any("a:gone" in r.getMessage() for r in caplog.records)
