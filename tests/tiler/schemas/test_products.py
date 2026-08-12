"""ProductConfig <-> Product field parity.

ProductConfig.from_product is a manual mapping (see products.py), so nothing
stops a new Product field from being added without a matching ProductConfig
field + mapping line. This test is the enforcement: it fails loudly the
moment the two drift, instead of the new field silently missing from
GET /products.
"""

import dataclasses

import pytest
from pydantic import ValidationError

from data_access_service.tiler.schemas.products import ProductConfig, ProductOverride
from data_access_service.tiler.schemas.products import (
    DataTileConfig as DataTileConfigSchema,
)
from data_access_service.tiler.schemas.products import (
    VisualTileConfig as VisualTileConfigSchema,
)
from data_access_service.tiler.schemas.products import (
    load_product_overrides,
    parse_product_overrides,
)
from data_access_service.tiler.services.product.product import (
    DataTileConfig,
    Product,
    VisualTileConfig,
)

# lod_grids is the one deliberate exception: it's computed from the store's
# native dimensions (requires an S3 round-trip to resolve), not per-product
# config, and is already served per-date via /manifest.json instead.
_COMPUTED_ONLY_FIELDS = {"lod_grids"}


def test_product_config_fields_match_product():
    product_fields = {f.name for f in dataclasses.fields(Product)}
    config_fields = set(ProductConfig.model_fields)
    assert product_fields == config_fields


def test_from_product_carries_visual_capability():
    """Parity above proves the field exists on both models; this proves
    from_product actually maps it. ogcapi-java keys tile_types on it, so a
    dropped mapping would silently downgrade every product to the arity rule.
    """
    scalar = Product(id="s", source_path="s3://b/x.zarr", variable="V")
    assert ProductConfig.from_product(scalar).visual is True

    non_visual = Product(
        id="n", source_path="s3://b/x.zarr", variable="WDIR", visual=False
    )
    assert ProductConfig.from_product(non_visual).visual is False

    pair = Product(
        id="p", source_path="s3://b/x.zarr", variable=["U", "V"], visual=False
    )
    assert ProductConfig.from_product(pair).visual is False


def test_data_tile_config_fields_match_product_data_tile_except_computed():
    """Product.data_tile and ProductConfig.data_tile mirror each other except
    for lod_grids — same exception as the top-level parity check, just scoped
    to the data-tile-only sub-object.
    """
    data_tile_fields = {f.name for f in dataclasses.fields(DataTileConfig)}
    config_fields = set(DataTileConfigSchema.model_fields)
    assert data_tile_fields - _COMPUTED_ONLY_FIELDS == config_fields


def test_visual_tile_config_fields_match_product_visual_tile():
    visual_tile_fields = {f.name for f in dataclasses.fields(VisualTileConfig)}
    config_fields = set(VisualTileConfigSchema.model_fields)
    assert visual_tile_fields == config_fields


# --- ProductOverride / products.json ----------------------------------------


def test_override_defaults_to_no_opinion():
    override = ProductOverride(id="a:gsla")
    assert override.ocean_masked is None
    assert override.visual is None
    assert override.data_tile.coastal_fill is None
    assert override.visual_tile.coastal_fill is None


def test_override_unknown_field_rejected():
    with pytest.raises(ValidationError):
        ProductOverride(id="a:gsla", ocean_maskd=True)


def test_parse_product_overrides_keys_by_id():
    overrides = parse_product_overrides(
        [{"id": "a:gsla", "ocean_masked": True}, {"id": "b:gsla"}]
    )
    assert set(overrides) == {"a:gsla", "b:gsla"}
    assert overrides["a:gsla"].ocean_masked is True


def test_parse_product_overrides_rejects_duplicate_id():
    with pytest.raises(ValueError, match="Duplicate"):
        parse_product_overrides([{"id": "a:gsla"}, {"id": "a:gsla"}])


def test_parse_product_overrides_rejects_non_array():
    with pytest.raises(ValueError):
        parse_product_overrides({"id": "a:gsla"})


def test_parse_product_overrides_accepts_empty_array():
    assert parse_product_overrides([]) == {}


def test_load_missing_products_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_product_overrides(tmp_path / "absent.json")
