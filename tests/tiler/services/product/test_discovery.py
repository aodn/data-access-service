"""Candidate product derivation from the metadata schema index, and
products.json override application on top of it.

Runs against a hand-built fake index — no API instance, no S3, no registry.
Mistakes here are invisible at runtime (a wrong grid, an ID that moved under a
frontend cache), so coverage is deliberately dense. Discovery and override
application are separate steps (build_candidate_products never sees
products.json), so they're tested separately too. build_candidate_products no
longer filters to zarr itself — that's API.iter_zarr_dataset_variables's job
(see test_api.py) — so every index here is written as if already filtered.
"""

import pytest

from data_access_service.tiler.schemas.products import parse_product_overrides
from data_access_service.tiler.services.product import discovery
from data_access_service.tiler.services.product.discovery import (
    apply_product_overrides,
    build_candidate_products,
    discover_products,
    product_id,
    log_unmatched_overrides,
    source_path,
)
from data_access_service.tiler.services.product.product import Product, get_lod_grids

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


def test_visual_defaults_from_arity():
    index = {"u1": {"a.zarr": frozenset({"GSLA", "UCUR", "VCUR"})}}
    candidates = _build(index, ["GSLA", ["UCUR", "VCUR"]])
    assert candidates["a:gsla"].visual is True
    assert candidates["a:ucur+vcur"].visual is False


def test_source_path_built_from_the_configured_base_url():
    candidates = _build(
        {"u1": {"a.zarr": frozenset({"GSLA"})}}, ["GSLA"], base_url="s3://other/"
    )
    assert candidates["a:gsla"].source_path == "s3://other/a.zarr"


def test_candidates_take_plain_defaults_with_no_overrides_involved():
    """build_candidate_products never sees products.json — every candidate is
    at plain defaults until apply_product_overrides runs."""
    candidates = _build({"u1": {"a.zarr": frozenset({"GSLA"})}}, ["GSLA"])
    candidate = candidates["a:gsla"]
    assert candidate.ocean_masked is False
    assert candidate.data_tile.coastal_fill is None
    assert candidate.visual_tile.coastal_fill is None


# --- config instance identity (Step 3 rule 9) -------------------------------


def test_each_product_gets_its_own_tile_config_instances():
    index = {
        "u1": {"a.zarr": frozenset({"sst"}), "b.zarr": frozenset({"sst"})},
        "u2": {"c.zarr": frozenset({"sst"})},
    }
    candidates = _build(index, ["sst"])
    data_tiles = [p.data_tile for p in candidates.values()]
    visual_tiles = [p.visual_tile for p in candidates.values()]

    assert len({id(cfg) for cfg in data_tiles}) == len(data_tiles)
    assert len({id(cfg) for cfg in visual_tiles}) == len(visual_tiles)
    # Also distinct from the config entry's own Pydantic objects.
    assert len({id(cfg.lod_grids) for cfg in data_tiles}) == len(data_tiles)


def test_populating_lod_grids_on_one_product_leaves_siblings_empty(monkeypatch):
    """lod_grids is a mutable dict on a frozen dataclass, filled in place from
    the product's own store dimensions and never recomputed. A shared instance
    would make every fanned-out product inherit whichever store was requested
    first — wrong grids in /manifest and in every data tile, nothing raised.
    """
    index = {"u1": {"small.zarr": frozenset({"sst"}), "big.zarr": frozenset({"sst"})}}
    candidates = _build(index, ["sst"])

    sizes = {
        f"{BASE_URL}/small.zarr": {"lat": 100, "lon": 100},
        f"{BASE_URL}/big.zarr": {"lat": 4000, "lon": 4000},
    }

    class FakeStore:
        def __init__(self, url):
            self.sizes = sizes[url]

    monkeypatch.setattr(
        "data_access_service.tiler.services.product.product.get_store",
        lambda url: FakeStore(url),
    )

    small_grids = get_lod_grids(candidates["small:sst"])
    assert small_grids
    # The sibling has not been touched by the small store's computation.
    assert candidates["big:sst"].data_tile.lod_grids == {}

    big_grids = get_lod_grids(candidates["big:sst"])
    assert big_grids != small_grids


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


# --- apply_product_overrides -------------------------------------------------


def _product(pid, **kwargs) -> Product:
    kwargs.setdefault("variable", "V")
    return Product(id=pid, source_path=f"s3://b/{pid}.zarr", **kwargs)


def test_a_product_without_an_override_is_returned_unchanged():
    candidates = {"a:gsla": _product("a:gsla")}
    overrides = parse_product_overrides([{"id": "b:gsla", "ocean_masked": True}])
    resolved = apply_product_overrides(candidates, overrides)
    assert resolved["a:gsla"] is candidates["a:gsla"]


def test_override_carries_every_setting_for_its_product():
    candidates = {"a:gsla": _product("a:gsla"), "b:gsla": _product("b:gsla")}
    overrides = parse_product_overrides(
        [
            {
                "id": "b:gsla",
                "data_tile": {"padding": 9, "coastal_fill": {"max_dist_px": 4}},
            }
        ]
    )
    resolved = apply_product_overrides(candidates, overrides)
    assert resolved["b:gsla"].data_tile.padding == 9
    assert resolved["b:gsla"].data_tile.coastal_fill.max_dist_px == 4
    assert resolved["a:gsla"].data_tile.padding == 1


def test_override_applies_only_to_its_own_product_id():
    """The live case: the committed ocean mask is built from the SLA grid, so
    the 18 HF-radar grids matched by the same specification must not use it."""
    sla_id = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"
    candidates = {
        sla_id: _product(sla_id),
        "radar_site:ucur+vcur": _product("radar_site:ucur+vcur"),
    }
    overrides = parse_product_overrides([{"id": sla_id, "ocean_masked": True}])
    resolved = apply_product_overrides(candidates, overrides)
    assert resolved[sla_id].ocean_masked is True
    assert resolved["radar_site:ucur+vcur"].ocean_masked is False


def test_override_can_flip_visual_on_a_scalar():
    candidates = {"a:wdir": _product("a:wdir", visual=True)}
    overrides = parse_product_overrides([{"id": "a:wdir", "visual": False}])
    resolved = apply_product_overrides(candidates, overrides)
    assert resolved["a:wdir"].visual is False


def test_override_setting_visual_true_on_a_pair_raises():
    candidates = {
        "a:ucur+vcur": _product("a:ucur+vcur", variable=["U", "V"], visual=False)
    }
    overrides = parse_product_overrides([{"id": "a:ucur+vcur", "visual": True}])
    with pytest.raises(ValueError, match="visual: true"):
        apply_product_overrides(candidates, overrides)


def test_unmatched_override_is_reported_but_not_fatal(caplog):
    candidates = {"a:gsla": _product("a:gsla")}
    overrides = parse_product_overrides([{"id": "renamed:gsla", "ocean_masked": True}])

    with caplog.at_level("ERROR"):
        log_unmatched_overrides(candidates, overrides)

    assert "renamed:gsla" in caplog.text
    assert any(r.levelname == "ERROR" for r in caplog.records)


def test_matched_override_logs_nothing(caplog):
    candidates = {"a:gsla": _product("a:gsla")}
    overrides = parse_product_overrides([{"id": "a:gsla", "ocean_masked": True}])

    with caplog.at_level("ERROR"):
        log_unmatched_overrides(candidates, overrides)

    assert not [r for r in caplog.records if r.levelname == "ERROR"]


# --- discover_products (the run_tiler_warmup entry point) -------------------


class FakeAPI:
    def __init__(self, index):
        self._index = index

    def iter_zarr_dataset_variables(self):
        return iter(_flatten(self._index))


def test_discover_products_loads_config_and_layers_overrides(monkeypatch):
    monkeypatch.setattr(discovery, "_load_gridded_variable_specs", lambda: ["GSLA"])
    monkeypatch.setattr(
        discovery,
        "load_product_overrides",
        lambda: parse_product_overrides([{"id": "a:gsla", "ocean_masked": True}]),
    )

    api = FakeAPI({"u1": {"a.zarr": frozenset({"GSLA"})}})
    products = discover_products(api, BASE_URL)

    assert products["a:gsla"].ocean_masked is True
    assert products["a:gsla"].source_path == f"{BASE_URL}/a.zarr"


def test_discover_products_logs_a_stale_override(monkeypatch, caplog):
    monkeypatch.setattr(discovery, "_load_gridded_variable_specs", lambda: ["GSLA"])
    monkeypatch.setattr(
        discovery,
        "load_product_overrides",
        lambda: parse_product_overrides([{"id": "renamed:gsla", "ocean_masked": True}]),
    )

    api = FakeAPI({"u1": {"a.zarr": frozenset({"GSLA"})}})
    with caplog.at_level("ERROR"):
        discover_products(api, BASE_URL)

    assert "renamed:gsla" in caplog.text


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

ORIGINAL_OVERRIDES = parse_product_overrides(
    [
        {
            "id": "model_sea_level_anomaly_gridded_realtime:gsla",
            "data_tile": {"coastal_fill": {"max_dist_px": 4}},
        },
        {
            "id": "model_sea_level_anomaly_gridded_realtime:ucur+vcur",
            "ocean_masked": True,
        },
    ]
)


def _build_original():
    candidates = _build(ORIGINAL_INDEX, ORIGINAL_CONFIG)
    return apply_product_overrides(candidates, ORIGINAL_OVERRIDES)


def test_five_existing_product_ids_reproduce_byte_for_byte():
    candidates = _build_original()
    for pid in ORIGINAL_PRODUCT_IDS:
        assert pid in candidates, f"original product id {pid} was not derived"


def test_original_products_keep_their_correctness_settings():
    candidates = _build_original()

    gsla = candidates["model_sea_level_anomaly_gridded_realtime:gsla"]
    assert gsla.data_tile.coastal_fill.max_dist_px == 4
    assert gsla.variable == "GSLA"

    currents = candidates["model_sea_level_anomaly_gridded_realtime:ucur+vcur"]
    assert currents.ocean_masked is True
    assert currents.variable == ["UCUR", "VCUR"]
    assert currents.visual is False


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
