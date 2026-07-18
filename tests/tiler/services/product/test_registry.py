"""product/registry: JSON load + the no-empty-state guarantee from the docstring.

load_products documents that concurrent readers never see an empty PRODUCTS
dict during reload — additions happen before removals. We pin that ordering
along with basic load behavior. Products are static config now (no admin
API), so these tests write products.json directly rather than going through
a registration call.
"""

import json

import pytest

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


def _entry(product_id="p1", source="s3://bucket/x.zarr", variable="V", **kwargs):
    return {"id": product_id, "source_path": source, "variable": variable, **kwargs}


def _write(cfg, entries):
    cfg.write_text(json.dumps(entries))


def test_load_products_no_file_is_noop(isolated_products):
    registry.load_products()
    assert PRODUCTS == {}


def test_load_populates_products(isolated_products):
    _write(isolated_products, [_entry("p1")])
    registry.load_products()
    assert PRODUCTS["p1"].source_path == "s3://bucket/x.zarr"


def test_load_with_multi_variable(isolated_products):
    """variable can be a list — exercised by the ocean_current fixture."""
    _write(isolated_products, [_entry("multi", variable=["U", "V"])])
    registry.load_products()
    assert PRODUCTS["multi"].variables == ["U", "V"]


def test_load_with_chunk_px_and_padding(isolated_products):
    _write(
        isolated_products,
        [
            {
                "id": "tuned",
                "source_path": "s3://bucket/x.zarr",
                "variable": "V",
                "chunk_px": [128, 96],
                "padding": 4,
            }
        ],
    )
    registry.load_products()
    p = PRODUCTS["tuned"]
    assert p.chunk_px == (128, 96)
    assert p.padding == 4


def test_load_with_coastal_fill(isolated_products):
    _write(
        isolated_products,
        [
            {
                "id": "sparse",
                "source_path": "s3://bucket/x.zarr",
                "variable": "V",
                "coastal_fill": {"max_dist_px": 4},
            }
        ],
    )
    registry.load_products()
    p = PRODUCTS["sparse"]
    assert p.coastal_fill is not None
    assert p.coastal_fill.max_dist_px == 4


def test_coastal_fill_absent_defaults_to_none(isolated_products):
    _write(isolated_products, [_entry("plain")])
    registry.load_products()
    assert PRODUCTS["plain"].coastal_fill is None


def test_ocean_masked_absent_defaults_to_false(isolated_products):
    _write(isolated_products, [_entry("plain")])
    registry.load_products()
    assert PRODUCTS["plain"].ocean_masked is False


def test_ocean_masked_defaults_true_for_listed_product(isolated_products):
    # The currents product is masked by default even without the config flag.
    pid = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"
    _write(isolated_products, [_entry(pid, variable=["UCUR", "VCUR"])])
    registry.load_products()
    assert PRODUCTS[pid].ocean_masked is True


def test_ocean_masked_explicit_false_overrides_default(isolated_products):
    pid = "model_sea_level_anomaly_gridded_realtime:ucur+vcur"
    _write(
        isolated_products, [_entry(pid, variable=["UCUR", "VCUR"], ocean_masked=False)]
    )
    registry.load_products()
    assert PRODUCTS[pid].ocean_masked is False


def test_list_products_reflects_file_contents(isolated_products):
    _write(isolated_products, [_entry("a"), _entry("b", source="s3://bucket/y.zarr")])
    listed = registry.list_products()
    assert [e["id"] for e in listed] == ["a", "b"]


def test_list_products_no_file_returns_empty(isolated_products):
    assert registry.list_products() == []


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
    spy["a"] = registry._from_dict(_entry("a"))
    spy["b"] = registry._from_dict(_entry("b", source="s3://bucket/y.zarr"))

    # Replace the module-level PRODUCTS dict so load_products mutates the spy.
    monkeypatch.setattr(registry, "PRODUCTS", spy)

    # On-disk replaces both with 'c'.
    isolated_products.write_text(json.dumps([_entry("c", source="s3://bucket/z.zarr")]))
    registry.load_products()

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
        registry.load_products()


def test_from_dict_returns_frozen_product():
    from dataclasses import FrozenInstanceError

    p = registry._from_dict(_entry("frozen"))
    assert isinstance(p, Product)
    # Frozen dataclass: assignment must raise.
    with pytest.raises(FrozenInstanceError):
        p.id = "changed"  # type: ignore[misc]


# --- portal / coverage association validation (build spec §5) ----------------


_UUID = "2ffccdad-1197-4e41-b412-a9033517cfb2"


def _coverage_entry(
    product_id="cov1",
    variable="V",
    source="s3://bucket/x.zarr",
    collection_id=_UUID,
    default=True,
    tms_id="tms_a",
    **kwargs,
):
    return _entry(
        product_id,
        source=source,
        variable=variable,
        portal={
            "collection_id": collection_id,
            "dataset_key": "ds_key",
            "default": default,
        },
        coverage={"enabled": True, "tile_matrix_set_id": tms_id},
        **kwargs,
    )


def test_load_coverage_product_parses_association(isolated_products):
    _write(isolated_products, [_coverage_entry(title="Nice name")])
    registry.load_products()
    p = PRODUCTS["cov1"]
    assert p.title == "Nice name"
    assert p.portal.collection_id == _UUID
    assert p.portal.dataset_key == "ds_key"
    assert p.portal.default is True
    assert p.coverage.enabled is True
    assert p.coverage.tile_matrix_set_id == "tms_a"


def test_products_without_portal_still_load(isolated_products):
    _write(isolated_products, [_entry("plain")])
    registry.load_products()
    assert PRODUCTS["plain"].portal is None
    assert PRODUCTS["plain"].coverage is None


def test_coverage_requires_portal(isolated_products):
    entry = _entry("cov1", coverage={"enabled": True, "tile_matrix_set_id": "tms_a"})
    _write(isolated_products, [entry])
    with pytest.raises(ValueError, match="requires portal"):
        registry.load_products()


def test_coverage_requires_tms_id(isolated_products):
    entry = _coverage_entry()
    entry["coverage"] = {"enabled": True}
    _write(isolated_products, [entry])
    with pytest.raises(ValueError, match="tile_matrix_set_id"):
        registry.load_products()


def test_coverage_rejects_invalid_collection_uuid(isolated_products):
    _write(isolated_products, [_coverage_entry(collection_id="not-a-uuid")])
    with pytest.raises(ValueError, match="not a valid UUID"):
        registry.load_products()


def test_coverage_rejects_three_variables(isolated_products):
    _write(isolated_products, [_coverage_entry(variable=["A", "B", "C"])])
    with pytest.raises(ValueError, match="1 or 2 variables"):
        registry.load_products()


def test_collection_requires_exactly_one_default(isolated_products):
    _write(
        isolated_products,
        [
            _coverage_entry("cov1", variable="A", default=True),
            _coverage_entry("cov2", variable="B", default=True),
        ],
    )
    with pytest.raises(ValueError, match="exactly one default"):
        registry.load_products()

    _write(
        isolated_products,
        [
            _coverage_entry("cov1", variable="A", default=False),
            _coverage_entry("cov2", variable="B", default=False),
        ],
    )
    with pytest.raises(ValueError, match="exactly one default"):
        registry.load_products()


def test_collection_rejects_duplicate_variable_sets(isolated_products):
    _write(
        isolated_products,
        [
            _coverage_entry("cov1", variable="V", default=True),
            _coverage_entry("cov2", variable="V", default=False, tms_id="tms_b"),
        ],
    )
    with pytest.raises(ValueError, match="variable set"):
        registry.load_products()


def test_shared_tms_id_requires_identical_grid_geometry(isolated_products):
    _write(
        isolated_products,
        [
            _coverage_entry("cov1", variable="A", default=True),
            _coverage_entry(
                "cov2", variable="B", default=False, source="s3://bucket/OTHER.zarr"
            ),
        ],
    )
    with pytest.raises(ValueError, match="different grid geometry"):
        registry.load_products()


def test_valid_multi_product_collection_loads(isolated_products):
    _write(
        isolated_products,
        [
            _coverage_entry("cov1", variable="A", default=True),
            _coverage_entry("cov2", variable="B", default=False),
            _coverage_entry("cov3", variable=["U", "W"], default=False),
        ],
    )
    registry.load_products()
    assert set(PRODUCTS) == {"cov1", "cov2", "cov3"}


def test_validation_failure_leaves_existing_products_untouched(isolated_products):
    _write(isolated_products, [_entry("keeper")])
    registry.load_products()
    _write(isolated_products, [_coverage_entry(collection_id="broken")])
    with pytest.raises(ValueError):
        registry.load_products()
    assert set(PRODUCTS) == {"keeper"}  # validation happens before the swap


def test_shipped_products_config_passes_validation():
    # The real deployed products.json must always satisfy the coverage rules —
    # a config regression should fail CI, not startup in an environment.
    from pathlib import Path

    from data_access_service.config.tiler.paths import PRODUCTS_CONFIG_PATH

    entries = json.loads(Path(PRODUCTS_CONFIG_PATH).read_text())
    registry.validate_coverage_config([registry._from_dict(e) for e in entries])
