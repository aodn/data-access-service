"""product/registry: publication semantics.

The registry no longer parses anything — products arrive already derived and
verified. What is left to pin is how state is swapped (readers never see an
empty dict).
"""

import pytest

import data_access_service.tiler.services.product.registry as registry
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import (
    PRODUCTS,
    get_product,
    iter_product_items,
    iter_products,
    publish_products,
)


@pytest.fixture
def isolated_products():
    """Snapshot PRODUCTS so a publish in one test can't leak into the next."""
    saved = dict(PRODUCTS)
    PRODUCTS.clear()
    yield
    PRODUCTS.clear()
    PRODUCTS.update(saved)


def _product(product_id="p1", source="s3://bucket/x.zarr", variable="V", **kwargs):
    return Product(id=product_id, source_path=source, variable=variable, **kwargs)


# --- publication ------------------------------------------------------------


def test_publish_populates_products(isolated_products):
    publish_products({"p1": _product("p1")})
    assert PRODUCTS["p1"].source_path == "s3://bucket/x.zarr"


def test_publish_replaces_the_previous_set(isolated_products):
    publish_products({"old": _product("old")})
    publish_products({"new": _product("new", source="s3://bucket/y.zarr")})
    assert set(PRODUCTS) == {"new"}


def test_publish_preserves_dict_identity(isolated_products):
    before = registry.PRODUCTS
    publish_products({"p1": _product("p1")})
    assert registry.PRODUCTS is before
    assert PRODUCTS is before


def test_publish_rejects_an_empty_set(isolated_products):
    publish_products({"p1": _product("p1")})
    with pytest.raises(ValueError, match="empty product set"):
        publish_products({})
    # The previous set is untouched by the refusal.
    assert set(PRODUCTS) == {"p1"}


def test_publish_never_exposes_empty_state(isolated_products, monkeypatch):
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
    monkeypatch.setattr(registry, "PRODUCTS", spy)

    publish_products({"c": _product("c", source="s3://bucket/z.zarr")})

    assert observed_snapshots, "expected at least one removal during publish"
    for snapshot in observed_snapshots:
        assert "c" in snapshot, (
            "PRODUCTS exposed a state without the new entry — "
            "remove-before-add ordering breaks the no-empty-state invariant"
        )
    assert set(spy.keys()) == {"c"}


# --- read facades -----------------------------------------------------------


def test_read_facades_reflect_published_state(isolated_products):
    publish_products({"p1": _product("p1"), "p2": _product("p2")})

    assert get_product("p1").id == "p1"
    assert get_product("absent") is None
    assert {p.id for p in iter_products()} == {"p1", "p2"}
    assert dict(iter_product_items()).keys() == {"p1", "p2"}


def test_iter_facades_return_snapshots_not_views(isolated_products):
    publish_products({"p1": _product("p1")})
    products = iter_products()
    items = iter_product_items()

    publish_products({"p2": _product("p2")})

    assert [p.id for p in products] == ["p1"]
    assert [pid for pid, _ in items] == ["p1"]
