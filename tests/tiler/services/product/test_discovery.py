"""Candidate product derivation from the metadata schema index.

Discovery is a pure function of the index, the config entries, and the base
URL, so everything here runs against a hand-built fake index — no API instance,
no S3, no registry. That is deliberate: this is the module where a mistake is
invisible (a product that renders the wrong grid, an ID that changed under a
frontend cache), so it gets the densest coverage in the change.
"""

import pytest

from data_access_service.tiler.schemas.gridded_variables import parse_gridded_variables
from data_access_service.tiler.services.product.discovery import (
    build_candidate_products,
    product_id,
    reject_unmatched_overrides,
    source_path,
    unmatched_overrides,
)
from data_access_service.tiler.services.product.product import get_lod_grids

BASE_URL = "s3://aodn-cloud-optimised"


def _build(index, config, base_url=BASE_URL):
    return build_candidate_products(index, parse_gridded_variables(config), base_url)


# --- ID and path formulas ---------------------------------------------------


def test_product_id_lowercases_variables_only():
    assert product_id("satellite_austemp_heatwave_8day.zarr", ["MCS_category"]) == (
        "satellite_austemp_heatwave_8day:mcs_category"
    )


def test_product_id_joins_pair_in_configured_order():
    assert product_id("x.zarr", ["UCUR", "VCUR"]) == "x:ucur+vcur"
    assert product_id("x.zarr", ["VCUR", "UCUR"]) == "x:vcur+ucur"


def test_source_path_is_canonical_without_trailing_slash():
    assert source_path("x.zarr", "s3://bucket") == "s3://bucket/x.zarr"
    # A base URL that carries one is normalised rather than doubled up.
    assert source_path("x.zarr", "s3://bucket/") == "s3://bucket/x.zarr"


# --- matching ---------------------------------------------------------------


def test_parquet_datasets_are_ignored_before_matching():
    """The schema index holds Parquet datasets too. The tiler only opens Zarr,
    so they are excluded before any variable is looked at."""
    index = {
        "u1": {
            "a.parquet": frozenset({"GSLA"}),
            "a.zarr": frozenset({"GSLA"}),
        }
    }
    candidates = _build(index, ["GSLA"])
    assert set(candidates) == {"a:gsla"}


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
    """GSL and GSLA both live on the SLA store; two products sharing one store
    with different variable sets is expected."""
    index = {"u1": {"sla.zarr": frozenset({"GSLA", "GSL"})}}
    candidates = _build(index, ["GSLA", "GSL"])
    assert set(candidates) == {"sla:gsla", "sla:gsl"}
    assert {p.source_path for p in candidates.values()} == {f"{BASE_URL}/sla.zarr"}


# --- representation ---------------------------------------------------------


def test_scalar_variable_stays_a_str():
    """A one-element list would turn a scalar into a broken vector product."""
    candidates = _build({"u1": {"a.zarr": frozenset({"GSLA"})}}, ["GSLA"])
    assert candidates["a:gsla"].variable == "GSLA"
    assert isinstance(candidates["a:gsla"].variable, str)


def test_pair_variable_stays_an_ordered_list():
    index = {"u1": {"a.zarr": frozenset({"UCUR", "VCUR"})}}
    candidates = _build(index, [["VCUR", "UCUR"]])
    # Configured order is the shader's R/G channel order and is never sorted.
    assert candidates["a:vcur+ucur"].variable == ["VCUR", "UCUR"]


def test_capability_flows_from_the_config_entry():
    index = {"u1": {"a.zarr": frozenset({"GSLA", "WDIR", "UCUR", "VCUR"})}}
    candidates = _build(
        index, ["GSLA", {"variable": "WDIR", "visual": False}, ["UCUR", "VCUR"]]
    )
    assert candidates["a:gsla"].visual is True
    assert candidates["a:wdir"].visual is False
    assert candidates["a:ucur+vcur"].visual is False


def test_source_path_built_from_the_configured_base_url():
    candidates = _build(
        {"u1": {"a.zarr": frozenset({"GSLA"})}}, ["GSLA"], base_url="s3://other/"
    )
    assert candidates["a:gsla"].source_path == "s3://other/a.zarr"


# --- defaults and overrides -------------------------------------------------


def test_entry_defaults_apply_to_every_matched_dataset():
    index = {"u1": {"a.zarr": frozenset({"GSLA"}), "b.zarr": frozenset({"GSLA"})}}
    candidates = _build(
        index,
        [
            {
                "variable": "GSLA",
                "defaults": {"data_tile": {"coastal_fill": {"max_dist_px": 4}}},
            }
        ],
    )
    for product in candidates.values():
        assert product.data_tile.coastal_fill.max_dist_px == 4


def test_dataset_override_applies_only_to_its_own_dataset():
    """The live case: the committed ocean mask is built from the SLA grid, so
    the 18 HF-radar grids matched by the same specification must not use it."""
    index = {
        "u1": {
            "model_sea_level_anomaly_gridded_realtime.zarr": frozenset({"UCUR", "VCUR"})
        },
        "u2": {"radar_site.zarr": frozenset({"UCUR", "VCUR"})},
    }
    candidates = _build(
        index,
        [
            {
                "variable": ["UCUR", "VCUR"],
                "overrides": {
                    "model_sea_level_anomaly_gridded_realtime.zarr": {
                        "ocean_masked": True
                    }
                },
            }
        ],
    )
    assert (
        candidates["model_sea_level_anomaly_gridded_realtime:ucur+vcur"].ocean_masked
        is True
    )
    assert candidates["radar_site:ucur+vcur"].ocean_masked is False


def test_override_overlays_recursively_onto_entry_defaults():
    index = {"u1": {"a.zarr": frozenset({"GSLA"}), "b.zarr": frozenset({"GSLA"})}}
    candidates = _build(
        index,
        [
            {
                "variable": "GSLA",
                "defaults": {
                    "data_tile": {"padding": 3, "coastal_fill": {"max_dist_px": 4}}
                },
                "overrides": {"b.zarr": {"data_tile": {"padding": 9}}},
            }
        ],
    )
    assert candidates["a:gsla"].data_tile.padding == 3
    assert candidates["b:gsla"].data_tile.padding == 9
    # Naming padding does not reset the sibling coastal_fill.
    assert candidates["b:gsla"].data_tile.coastal_fill.max_dist_px == 4


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


def test_duplicate_generated_id_from_two_specifications_raises():
    index = {"u1": {"a.zarr": frozenset({"GSLA"})}}
    with pytest.raises(ValueError, match="Duplicate product id"):
        _build(index, ["GSLA", {"variable": "GSLA", "visual": False}])


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


def test_index_with_only_parquet_raises():
    index = {"u1": {"a.parquet": frozenset({"GSLA"})}}
    with pytest.raises(ValueError, match="No candidate products"):
        _build(index, ["GSLA"])


# --- override matching ------------------------------------------------------


def test_unmatched_override_is_reported_and_fatal():
    index = {"u1": {"a.zarr": frozenset({"GSLA"})}}
    entries = parse_gridded_variables(
        [{"variable": "GSLA", "overrides": {"renamed.zarr": {"ocean_masked": True}}}]
    )
    candidates = build_candidate_products(index, entries, BASE_URL)

    assert unmatched_overrides(candidates, entries) == [("GSLA", "renamed.zarr")]
    with pytest.raises(ValueError, match="renamed.zarr"):
        reject_unmatched_overrides(candidates, entries)


def test_matched_override_passes():
    index = {"u1": {"a.zarr": frozenset({"GSLA"})}}
    entries = parse_gridded_variables(
        [{"variable": "GSLA", "overrides": {"a.zarr": {"ocean_masked": True}}}]
    )
    candidates = build_candidate_products(index, entries, BASE_URL)

    assert unmatched_overrides(candidates, entries) == []
    reject_unmatched_overrides(candidates, entries)


def test_override_matching_is_per_specification_not_global():
    """An override on the pair entry is unmatched when the *pair* produced no
    product on that dataset, even though another specification did."""
    index = {"u1": {"a.zarr": frozenset({"GSLA"})}}
    entries = parse_gridded_variables(
        [
            "GSLA",
            {
                "variable": ["UCUR", "VCUR"],
                "overrides": {"a.zarr": {"ocean_masked": True}},
            },
        ]
    )
    candidates = build_candidate_products(index, entries, BASE_URL)
    assert unmatched_overrides(candidates, entries) == [("['UCUR', 'VCUR']", "a.zarr")]


# --- the five existing public product IDs -----------------------------------

LEGACY_INDEX = {
    "2ffccdad-1197-4e41-b412-a9033517cfb2": {
        "satellite_austemp_heatwave_8day.zarr": frozenset(
            {"sst_mosaic", "ssta_mosaic", "MCS_category"}
        )
    },
    "0c9eb39c-9cbe-4c6a-8a10-5867087e703a": {
        "model_sea_level_anomaly_gridded_realtime.zarr": frozenset(
            {"GSLA", "GSL", "UCUR", "VCUR"}
        )
    },
}

# Pinned literals, not derived from the formula — these IDs are cached by the
# frontend and opaque to ogcapi-java, so the point is to catch the formula
# changing, which a derived expectation would not.
LEGACY_PRODUCT_IDS_PINNED = [
    "satellite_austemp_heatwave_8day:sst_mosaic",
    "satellite_austemp_heatwave_8day:ssta_mosaic",
    "satellite_austemp_heatwave_8day:mcs_category",
    "model_sea_level_anomaly_gridded_realtime:gsla",
    "model_sea_level_anomaly_gridded_realtime:ucur+vcur",
]

LEGACY_CONFIG = [
    {
        "variable": "GSLA",
        "defaults": {"data_tile": {"coastal_fill": {"max_dist_px": 4}}},
    },
    "GSL",
    {
        "variable": ["UCUR", "VCUR"],
        "visual": False,
        "overrides": {
            "model_sea_level_anomaly_gridded_realtime.zarr": {"ocean_masked": True}
        },
    },
    "sst_mosaic",
    "ssta_mosaic",
    "MCS_category",
]


def test_pinned_ids_match_the_constant_the_startup_gate_uses():
    """The literals above are an independent pin on the derivation formula; the
    constant is what fails a boot. They only stay honest if they agree."""
    from data_access_service.tiler.services.product.verification import (
        LEGACY_PRODUCT_IDS,
    )

    assert set(LEGACY_PRODUCT_IDS) == set(LEGACY_PRODUCT_IDS_PINNED)


def test_five_existing_product_ids_reproduce_byte_for_byte():
    candidates = _build(LEGACY_INDEX, LEGACY_CONFIG)
    for pid in LEGACY_PRODUCT_IDS_PINNED:
        assert pid in candidates, f"legacy product id {pid} was not derived"


def test_legacy_products_keep_their_correctness_settings():
    candidates = _build(LEGACY_INDEX, LEGACY_CONFIG)

    gsla = candidates["model_sea_level_anomaly_gridded_realtime:gsla"]
    assert gsla.data_tile.coastal_fill.max_dist_px == 4
    assert gsla.variable == "GSLA"

    currents = candidates["model_sea_level_anomaly_gridded_realtime:ucur+vcur"]
    assert currents.ocean_masked is True
    assert currents.variable == ["UCUR", "VCUR"]
    assert currents.visual is False


def test_legacy_products_keep_their_metadata_uuids():
    candidates = _build(LEGACY_INDEX, LEGACY_CONFIG)
    assert (
        candidates["satellite_austemp_heatwave_8day:sst_mosaic"].metadata_uuid
        == "2ffccdad-1197-4e41-b412-a9033517cfb2"
    )
    assert (
        candidates["model_sea_level_anomaly_gridded_realtime:gsla"].metadata_uuid
        == "0c9eb39c-9cbe-4c6a-8a10-5867087e703a"
    )


def test_legacy_source_paths_are_canonicalised():
    """Intended change: today's two SLA entries carry a trailing slash. The IDs
    are what clients key on and those are unchanged."""
    candidates = _build(LEGACY_INDEX, LEGACY_CONFIG)
    assert (
        candidates["model_sea_level_anomaly_gridded_realtime:gsla"].source_path
        == f"{BASE_URL}/model_sea_level_anomaly_gridded_realtime.zarr"
    )
    assert (
        candidates["satellite_austemp_heatwave_8day:sst_mosaic"].source_path
        == f"{BASE_URL}/satellite_austemp_heatwave_8day.zarr"
    )
