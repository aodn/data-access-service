"""Resolving the live product catalogue: default rendering config from
variable arity, then products_customisation layered on top by id.

Moved from tests/batch/tiler/test_discovery.py when override resolution moved
off the batch side — see catalog.py's module docstring for why.
"""

import pytest

from data_access_service.models.tiler_parquet_types import ProductIdentity
from data_access_service.tiler.schemas.products import parse_product_overrides
from data_access_service.tiler.services.product.catalog import (
    _default_product,
    apply_product_overrides,
    build_catalog,
    log_unmatched_overrides,
)


def _identity(pid, variable="V", uuid="uuid-a") -> ProductIdentity:
    return ProductIdentity(
        id=pid, source_path=f"s3://b/{pid}.zarr", variable=variable, metadata_uuid=uuid
    )


def _product(pid, **kwargs):
    return _default_product(_identity(pid, **kwargs))


# --- default product from identity -------------------------------------------


def test_scalar_identity_defaults_to_visual_true():
    product = _default_product(_identity("a:gsla"))
    assert product.visual is True


def test_pair_identity_defaults_to_visual_false():
    product = _default_product(_identity("a:ucur+vcur", variable=["UCUR", "VCUR"]))
    assert product.visual is False


def test_default_product_takes_plain_defaults_with_no_overrides_involved():
    product = _default_product(_identity("a:gsla"))
    assert product.ocean_masked is False
    assert product.data_tile.coastal_fill is None
    assert product.visual_tile.coastal_fill is None


def test_each_identity_gets_its_own_tile_config_instances():
    products = [_default_product(_identity(pid)) for pid in ("a:sst", "b:sst", "c:sst")]
    data_tiles = [p.data_tile for p in products]
    visual_tiles = [p.visual_tile for p in products]

    assert len({id(cfg) for cfg in data_tiles}) == len(data_tiles)
    assert len({id(cfg) for cfg in visual_tiles}) == len(visual_tiles)


# --- apply_product_overrides -------------------------------------------------


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
    candidates = {"a:wdir": _product("a:wdir")}
    overrides = parse_product_overrides([{"id": "a:wdir", "visual": False}])
    resolved = apply_product_overrides(candidates, overrides)
    assert resolved["a:wdir"].visual is False


def test_override_setting_visual_true_on_a_pair_raises():
    candidates = {"a:ucur+vcur": _product("a:ucur+vcur", variable=["U", "V"])}
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


# --- build_catalog (the tiler startup entry point) ---------------------------


def test_build_catalog_loads_config_and_layers_overrides(monkeypatch):
    import data_access_service.tiler.services.product.catalog as catalog_module

    monkeypatch.setattr(
        catalog_module,
        "load_product_overrides",
        lambda: parse_product_overrides([{"id": "a:gsla", "ocean_masked": True}]),
    )

    identities = {"a:gsla": _identity("a:gsla")}
    products = build_catalog(identities)

    assert products["a:gsla"].ocean_masked is True
    assert products["a:gsla"].source_path == "s3://b/a:gsla.zarr"


def test_build_catalog_logs_a_stale_override(monkeypatch, caplog):
    import data_access_service.tiler.services.product.catalog as catalog_module

    monkeypatch.setattr(
        catalog_module,
        "load_product_overrides",
        lambda: parse_product_overrides([{"id": "renamed:gsla", "ocean_masked": True}]),
    )

    identities = {"a:gsla": _identity("a:gsla")}
    with caplog.at_level("ERROR"):
        build_catalog(identities)

    assert "renamed:gsla" in caplog.text


# --- the five existing products' override settings ---------------------------

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


def test_original_products_keep_their_correctness_settings():
    candidates = {
        "model_sea_level_anomaly_gridded_realtime:gsla": _product(
            "model_sea_level_anomaly_gridded_realtime:gsla", variable="GSLA"
        ),
        "model_sea_level_anomaly_gridded_realtime:ucur+vcur": _product(
            "model_sea_level_anomaly_gridded_realtime:ucur+vcur",
            variable=["UCUR", "VCUR"],
        ),
    }
    resolved = apply_product_overrides(candidates, ORIGINAL_OVERRIDES)

    gsla = resolved["model_sea_level_anomaly_gridded_realtime:gsla"]
    assert gsla.data_tile.coastal_fill.max_dist_px == 4
    assert gsla.variable == "GSLA"

    currents = resolved["model_sea_level_anomaly_gridded_realtime:ucur+vcur"]
    assert currents.ocean_masked is True
    assert currents.variable == ["UCUR", "VCUR"]
    assert currents.visual is False
