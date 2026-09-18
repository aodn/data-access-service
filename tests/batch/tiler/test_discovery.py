"""Candidate product identity derivation from the metadata schema index.

Runs against a hand-built fake index — no API instance, no S3, no registry.
Mistakes here are invisible at runtime (a wrong grid, an ID that moved under a
frontend cache), so coverage is deliberately dense. This only covers
identity — no rendering config; products_customisation resolution lives on
the live tiler side (see tests/tiler/services/product/test_catalog.py).
build_candidate_products no longer filters to zarr itself — that's
API.iter_zarr_dataset_variables's job (see test_api.py) — so every index here
is written as if already filtered.
"""

import pytest

from data_access_service.batch.tiler import discovery
from data_access_service.batch.tiler.discovery import (
    build_candidate_products,
    discover_products,
    product_id,
    source_path,
)

BASE_URL = "s3://aodn-cloud-optimised"


def _flatten(index):
    """uuid -> dataset_name -> fields, as API.iter_zarr_dataset_variables
    yields it flattened — the shape every fixture in this file is written in,
    since it reads better grouped by uuid/dataset than as a flat tuple list."""
    return [
        (uuid, dname, fields)
        for uuid, datasets in index.items()
        for dname, fields in datasets.items()
    ]


def _build(index, specs, base_url=BASE_URL):
    return build_candidate_products(_flatten(index), specs, base_url)


# --- ID and path formulas ---------------------------------------------------


def test_product_id_lowercases_variables_only():
    assert product_id("satellite_austemp_heatwave_14day.zarr", ["MCS_category"]) == (
        "satellite_austemp_heatwave_14day:mcs_category"
    )


def test_product_id_joins_pair_in_configured_order():
    assert product_id("x.zarr", ["UCUR", "VCUR"]) == "x:ucur+vcur"
    assert product_id("x.zarr", ["VCUR", "UCUR"]) == "x:vcur+ucur"


def test_source_path_is_canonical_without_trailing_slash():
    assert source_path("x.zarr", "s3://bucket") == "s3://bucket/x.zarr"
    # A base URL that carries one is normalised rather than doubled up.
    assert source_path("x.zarr", "s3://bucket/") == "s3://bucket/x.zarr"


# --- matching ---------------------------------------------------------------


def test_scalar_matches_on_exact_case():
    index = {"u1": {"a.zarr": frozenset({"sst"}), "b.zarr": frozenset({"SST"})}}
    candidates = _build(index, ["sst"])
    assert set(candidates) == {"a:sst"}


def test_pair_requires_both_names():
    index = {
        "u1": {
            "both.zarr": frozenset({"UCUR", "VCUR"}),
            "ucur_only.zarr": frozenset({"UCUR"}),
            "vcur_only.zarr": frozenset({"VCUR"}),
        }
    }
    candidates = _build(index, [["UCUR", "VCUR"]])
    assert set(candidates) == {"both:ucur+vcur"}


def test_pair_requires_exact_case_on_both_names():
    index = {"u1": {"a.zarr": frozenset({"UCUR", "vcur"})}}
    candidates = _build(index, [["UCUR", "VCUR"], "UCUR"])
    assert set(candidates) == {"a:ucur"}


def test_one_uuid_with_two_datasets_yields_two_products():
    index = {"u1": {"a.zarr": frozenset({"GSLA"}), "b.zarr": frozenset({"GSLA"})}}
    candidates = _build(index, ["GSLA"])
    assert set(candidates) == {"a:gsla", "b:gsla"}
    assert {p.metadata_uuid for p in candidates.values()} == {"u1"}


def test_metadata_uuid_propagates_from_the_outer_index_key():
    index = {
        "uuid-a": {"a.zarr": frozenset({"GSLA"})},
        "uuid-b": {"b.zarr": frozenset({"GSLA"})},
    }
    candidates = _build(index, ["GSLA"])
    assert candidates["a:gsla"].metadata_uuid == "uuid-a"
    assert candidates["b:gsla"].metadata_uuid == "uuid-b"


def test_two_specifications_on_one_dataset_yield_two_products():
    index = {"u1": {"sla.zarr": frozenset({"GSLA", "GSL"})}}
    candidates = _build(index, ["GSLA", "GSL"])
    assert set(candidates) == {"sla:gsla", "sla:gsl"}
    assert {p.source_path for p in candidates.values()} == {f"{BASE_URL}/sla.zarr"}


# --- representation ---------------------------------------------------------


def test_scalar_variable_stays_a_str():
    candidates = _build({"u1": {"a.zarr": frozenset({"GSLA"})}}, ["GSLA"])
    assert candidates["a:gsla"].variable == "GSLA"
    assert isinstance(candidates["a:gsla"].variable, str)


def test_pair_variable_stays_an_ordered_list():
    index = {"u1": {"a.zarr": frozenset({"UCUR", "VCUR"})}}
    candidates = _build(index, [["VCUR", "UCUR"]])
    # Configured order is the shader's R/G channel order and is never sorted.
    assert candidates["a:vcur+ucur"].variable == ["VCUR", "UCUR"]


def test_source_path_built_from_the_configured_base_url():
    candidates = _build(
        {"u1": {"a.zarr": frozenset({"GSLA"})}}, ["GSLA"], base_url="s3://other/"
    )
    assert candidates["a:gsla"].source_path == "s3://other/a.zarr"


# --- failure modes ----------------------------------------------------------


def test_duplicate_generated_id_from_two_uuids_raises():
    """The same dataset name under two UUIDs would map one ID to two
    collections. That is a metadata identity error, not a keep-first case."""
    index = {
        "uuid-a": {"a.zarr": frozenset({"GSLA"})},
        "uuid-b": {"a.zarr": frozenset({"GSLA"})},
    }
    with pytest.raises(ValueError, match="Duplicate product id"):
        _build(index, ["GSLA"])


def test_zero_match_specification_warns_but_does_not_raise(caplog):
    index = {"u1": {"a.zarr": frozenset({"GSLA"})}}
    with caplog.at_level("WARNING"):
        candidates = _build(index, ["GSLA", "NOT_PRESENT"])
    assert set(candidates) == {"a:gsla"}
    assert "NOT_PRESENT" in caplog.text


def test_empty_result_from_non_empty_config_raises():
    index = {"u1": {"a.zarr": frozenset({"OTHER"})}}
    with pytest.raises(ValueError, match="No candidate products"):
        _build(index, ["GSLA"])


def test_empty_dataset_variables_raises():
    """Whether because the catalogue is empty or API.iter_zarr_dataset_variables
    filtered everything out (all-parquet), the effect from here is the same."""
    with pytest.raises(ValueError, match="No candidate products"):
        build_candidate_products([], ["GSLA"], BASE_URL)


# --- discover_products (the batch entry point) -------------------------------


class FakeAPI:
    def __init__(self, index):
        self._index = index

    def iter_zarr_dataset_variables(self):
        return iter(_flatten(self._index))


def test_discover_products_loads_config(monkeypatch):
    monkeypatch.setattr(discovery, "_load_gridded_variable_specs", lambda: ["GSLA"])
    monkeypatch.setattr(discovery, "_load_store_blacklist", lambda: frozenset())

    api = FakeAPI({"u1": {"a.zarr": frozenset({"GSLA"})}})
    products = discover_products(api, BASE_URL)

    assert products["a:gsla"].source_path == f"{BASE_URL}/a.zarr"
    assert products["a:gsla"].metadata_uuid == "u1"


# --- store blacklist ----------------------------------------------------------


def test_blacklisted_store_is_excluded_from_dataset_variables():
    index = {
        "u1": {
            "keep.zarr": frozenset({"GSLA"}),
            "drop.zarr": frozenset({"GSLA"}),
        }
    }
    filtered = list(
        discovery._exclude_blacklisted_stores(_flatten(index), frozenset({"drop"}))
    )
    assert [dname for _, dname, _ in filtered] == ["keep.zarr"]


def test_blacklist_matches_suffix_stripped_dataset_name():
    """blacklist entries read the same as products_customisation ids'
    dataset_name prefix — no trailing .zarr — even though the raw index
    carries it."""
    index = {
        "u1": {"model_sea_level_anomaly_gridded_realtime.zarr": frozenset({"GSLA"})}
    }
    filtered = list(
        discovery._exclude_blacklisted_stores(
            _flatten(index),
            frozenset({"model_sea_level_anomaly_gridded_realtime"}),
        )
    )
    assert filtered == []


def test_empty_blacklist_excludes_nothing():
    index = {"u1": {"a.zarr": frozenset({"GSLA"})}}
    filtered = list(discovery._exclude_blacklisted_stores(_flatten(index), frozenset()))
    assert [dname for _, dname, _ in filtered] == ["a.zarr"]


def test_discover_products_drops_blacklisted_store(monkeypatch):
    monkeypatch.setattr(discovery, "_load_gridded_variable_specs", lambda: ["GSLA"])
    monkeypatch.setattr(discovery, "_load_store_blacklist", lambda: frozenset({"drop"}))

    api = FakeAPI(
        {
            "u1": {
                "keep.zarr": frozenset({"GSLA"}),
                "drop.zarr": frozenset({"GSLA"}),
            }
        }
    )
    products = discover_products(api, BASE_URL)

    assert set(products) == {"keep:gsla"}


# --- the five product IDs that predate derivation ---------------------------

ORIGINAL_INDEX = {
    "2ffccdad-1197-4e41-b412-a9033517cfb2": {
        "satellite_austemp_heatwave_14day.zarr": frozenset(
            {"sst_mosaic", "ssta_mosaic", "MCS_category"}
        )
    },
    "0c9eb39c-9cbe-4c6a-8a10-5867087e703a": {
        "model_sea_level_anomaly_gridded_realtime.zarr": frozenset(
            {"GSLA", "GSL", "UCUR", "VCUR"}
        )
    },
}

# Pinned literals, not derived from the formula — these IDs are opaque to
# ogcapi-java, so the point is to catch the formula changing, which a derived
# expectation would not. The heatwave ids track the store's current name: it was
# renamed upstream from _8day to _14day, which is what broke the hand-written
# config these products used to come from.
ORIGINAL_PRODUCT_IDS = [
    "satellite_austemp_heatwave_14day:sst_mosaic",
    "satellite_austemp_heatwave_14day:ssta_mosaic",
    "satellite_austemp_heatwave_14day:mcs_category",
    "model_sea_level_anomaly_gridded_realtime:gsla",
    "model_sea_level_anomaly_gridded_realtime:ucur+vcur",
]

ORIGINAL_CONFIG = [
    "GSLA",
    "GSL",
    ["UCUR", "VCUR"],
    "sst_mosaic",
    "ssta_mosaic",
    "MCS_category",
]


def _build_original():
    return _build(ORIGINAL_INDEX, ORIGINAL_CONFIG)


def test_five_existing_product_ids_reproduce_byte_for_byte():
    candidates = _build_original()
    for pid in ORIGINAL_PRODUCT_IDS:
        assert pid in candidates, f"original product id {pid} was not derived"


def test_original_products_keep_their_metadata_uuids():
    candidates = _build_original()
    assert (
        candidates["satellite_austemp_heatwave_14day:sst_mosaic"].metadata_uuid
        == "2ffccdad-1197-4e41-b412-a9033517cfb2"
    )
    assert (
        candidates["model_sea_level_anomaly_gridded_realtime:gsla"].metadata_uuid
        == "0c9eb39c-9cbe-4c6a-8a10-5867087e703a"
    )


def test_original_source_paths_are_canonicalised():
    """Intended change: today's two SLA entries carry a trailing slash. The IDs
    are what clients key on and those are unchanged."""
    candidates = _build_original()
    assert (
        candidates["model_sea_level_anomaly_gridded_realtime:gsla"].source_path
        == f"{BASE_URL}/model_sea_level_anomaly_gridded_realtime.zarr"
    )
    assert (
        candidates["satellite_austemp_heatwave_14day:sst_mosaic"].source_path
        == f"{BASE_URL}/satellite_austemp_heatwave_14day.zarr"
    )
