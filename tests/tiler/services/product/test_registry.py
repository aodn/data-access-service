"""product/registry: discovered-product + products.json-override merge, and
the no-empty-state guarantee from the docstring.

load_products documents that concurrent readers never see an empty PRODUCTS
dict during reload — additions happen before removals. We pin that ordering
along with basic merge behavior. products.json now holds only optional
per-id overrides (ocean_masked/data_tile/visual_tile) — id/source_path/
variable/metadata_uuid come from the ``discovered`` list passed in (what
discovery.discover_products would have built from the zarr catalog), so
these tests build that list directly rather than going through discovery.
"""

import json

import pytest
from pydantic import ValidationError

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import PRODUCTS


@pytest.fixture
def isolated_products(tmp_path, monkeypatch):
    """Redirect registry at a tmp file and snapshot PRODUCTS."""
    cfg = tmp_path / "products.json"
    monkeypatch.setattr(registry, "_config_path", cfg)
    saved = dict(PRODUCTS)
    PRODUCTS.clear()
    yield cfg
    PRODUCTS.clear()
    PRODUCTS.update(saved)


def _product(product_id="p1", source="s3://bucket/x.zarr", variable="V", **kwargs):
    return Product(id=product_id, source_path=source, variable=variable, **kwargs)


def _override(product_id="p1", **kwargs):
    return {"id": product_id, **kwargs}


def _write(cfg, entries):
    cfg.write_text(json.dumps(entries))


def test_load_products_no_file_raises(isolated_products):
    with pytest.raises(FileNotFoundError):
        registry.load_products([])


def test_load_populates_products(isolated_products):
    _write(isolated_products, [])
    registry.load_products([_product("p1")])
    assert PRODUCTS["p1"].source_path == "s3://bucket/x.zarr"


def test_load_with_multi_variable(isolated_products):
    """variable can be a list — exercised by the ocean_current fixture."""
    _write(isolated_products, [])
    registry.load_products([_product("multi", variable=["U", "V"])])
    assert PRODUCTS["multi"].variables == ["U", "V"]


def test_load_with_chunk_px_and_padding(isolated_products):
    _write(
        isolated_products,
        [
            {
                "id": "tuned",
                "data_tile": {"chunk_px": [128, 96], "padding": 4},
            }
        ],
    )
    registry.load_products([_product("tuned")])
    p = PRODUCTS["tuned"]
    assert p.data_tile.chunk_px == (128, 96)
    assert p.data_tile.padding == 4


def test_load_rejects_unknown_field(isolated_products):
    """max_lods/min_coarsest are global-only (see DataTileLodConfig) — not a per-product key."""
    _write(isolated_products, [_override("bad", max_lods=6)])
    with pytest.raises(ValidationError):
        registry.load_products([_product("bad")])


def test_load_with_coastal_fill(isolated_products):
    _write(
        isolated_products,
        [{"id": "sparse", "data_tile": {"coastal_fill": {"max_dist_px": 4}}}],
    )
    registry.load_products([_product("sparse")])
    p = PRODUCTS["sparse"]
    assert p.data_tile.coastal_fill is not None
    assert p.data_tile.coastal_fill.max_dist_px == 4


def test_coastal_fill_absent_defaults_to_none(isolated_products):
    _write(isolated_products, [])
    registry.load_products([_product("plain")])
    assert PRODUCTS["plain"].data_tile.coastal_fill is None


def test_load_with_visual_tile_coastal_fill(isolated_products):
    """visual_tile.coastal_fill is independent of data_tile.coastal_fill —
    a product can set one, both, or neither."""
    _write(
        isolated_products,
        [
            {
                "id": "sparse",
                "data_tile": {"coastal_fill": {"max_dist_px": 4}},
                "visual_tile": {"coastal_fill": {"max_dist_px": 8}},
            }
        ],
    )
    registry.load_products([_product("sparse")])
    p = PRODUCTS["sparse"]
    assert p.data_tile.coastal_fill.max_dist_px == 4
    assert p.visual_tile.coastal_fill.max_dist_px == 8


def test_visual_tile_coastal_fill_absent_defaults_to_none(isolated_products):
    _write(isolated_products, [])
    registry.load_products([_product("plain")])
    assert PRODUCTS["plain"].visual_tile.coastal_fill is None


def test_ocean_masked_absent_defaults_to_false(isolated_products):
    _write(isolated_products, [])
    registry.load_products([_product("plain")])
    assert PRODUCTS["plain"].ocean_masked is False


def test_ocean_masked_explicit_true_overrides_default(isolated_products):
    pid = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"
    _write(isolated_products, [_override(pid, ocean_masked=True)])
    registry.load_products([_product(pid, variable=["UCUR", "VCUR"])])
    assert PRODUCTS[pid].ocean_masked is True


def test_metadata_uuid_absent_defaults_to_none(isolated_products):
    _write(isolated_products, [])
    registry.load_products([_product("plain")])
    assert PRODUCTS["plain"].metadata_uuid is None


def test_metadata_uuid_comes_from_discovered_product(isolated_products):
    """metadata_uuid is no longer a products.json field — it's carried
    straight through from the discovered Product (see discovery.py)."""
    _write(isolated_products, [])
    registry.load_products([_product("linked", metadata_uuid="uuid-123")])
    assert PRODUCTS["linked"].metadata_uuid == "uuid-123"


def test_ocean_masked_explicit_false_overrides_default(isolated_products):
    pid = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"
    _write(isolated_products, [_override(pid, ocean_masked=False)])
    registry.load_products([_product(pid, variable=["UCUR", "VCUR"])])
    assert PRODUCTS[pid].ocean_masked is False


def test_unmatched_override_logs_warning_and_is_skipped(isolated_products, caplog):
    """An override id with no matching discovered product (stale config, or an
    upstream AODN rename) is skipped with a warning, not a startup failure."""
    _write(isolated_products, [_override("ghost", ocean_masked=True)])
    with caplog.at_level("WARNING"):
        registry.load_products([_product("real")])
    assert "real" in PRODUCTS
    assert "ghost" not in PRODUCTS
    assert any("ghost" in record.message for record in caplog.records)


def test_discovered_product_without_override_uses_defaults(isolated_products):
    """A products.json entry for one product doesn't affect another discovered
    product that has no entry of its own."""
    _write(isolated_products, [_override("tuned", ocean_masked=True)])
    registry.load_products([_product("tuned"), _product("plain")])
    assert PRODUCTS["tuned"].ocean_masked is True
    assert PRODUCTS["plain"].ocean_masked is False


def test_load_products_never_exposes_empty_state(isolated_products, monkeypatch):
    """Documented invariant: additions first, then removals — readers never see {}.

    Strategy: replace PRODUCTS with a subclass that snapshots keys on every
    __delitem__, then verify the new entry is already present at every
    removal point.
    """
    observed_snapshots: list[set[str]] = []

    class SpyDict(dict):
        def __delitem__(self, key):
            observed_snapshots.append(set(self.keys()))
            super().__delitem__(key)

    spy = SpyDict()
    spy["a"] = _product("a")
    spy["b"] = _product("b", source="s3://bucket/y.zarr")

    # Replace the module-level PRODUCTS dict so load_products mutates the spy.
    monkeypatch.setattr(registry, "PRODUCTS", spy)

    # New discovered list replaces both with 'c'.
    isolated_products.write_text("[]")
    registry.load_products([_product("c", source="s3://bucket/z.zarr")])

    # By the time a stale key is removed, the new entry must already be present.
    assert observed_snapshots, "expected at least one removal during reload"
    for snapshot in observed_snapshots:
        assert "c" in snapshot, (
            "PRODUCTS exposed a state without the new entry — "
            "remove-before-add ordering breaks the no-empty-state invariant"
        )

    assert set(spy.keys()) == {"c"}


def test_load_malformed_json_raises(isolated_products):
    isolated_products.write_text("not json at all")
    with pytest.raises(json.JSONDecodeError):
        registry.load_products([])


def test_apply_override_returns_frozen_product():
    from dataclasses import FrozenInstanceError

    p = registry._apply_override(_product("frozen"), None)
    assert isinstance(p, Product)
    # Frozen dataclass: assignment must raise.
    with pytest.raises(FrozenInstanceError):
        p.id = "changed"  # type: ignore[misc]
