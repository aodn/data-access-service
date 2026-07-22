"""ProductConfig <-> Product field parity.

ProductConfig.from_product is a manual mapping (see products.py), so nothing
stops a new Product field from being added without a matching ProductConfig
field + mapping line. This test is the enforcement: it fails loudly the
moment the two drift, instead of the new field silently missing from
GET /products.
"""

import dataclasses

from data_access_service.tiler.schemas.products import ProductConfig
from data_access_service.tiler.services.product.product import Product
from data_access_service.tiler.services.product.registry import _ProductEntry

# lod_grids is the one deliberate exception: it's computed from the store's
# native dimensions (requires an S3 round-trip to resolve), not per-product
# config, and is already served per-date via /manifest.json instead.
_COMPUTED_ONLY_FIELDS = {"lod_grids"}


def test_product_config_fields_match_product_except_computed():
    product_fields = {f.name for f in dataclasses.fields(Product)}
    config_fields = set(ProductConfig.model_fields)
    assert product_fields - _COMPUTED_ONLY_FIELDS == config_fields


def test_product_entry_fields_match_product_config():
    """_ProductEntry (products.json's allowlisted input shape, extra="forbid")
    must expose the same fields as ProductConfig (the resolved output shape),
    or _from_dict's ``parsed.<field>`` access raises AttributeError deep inside
    load_products — before it reaches its own success log line — the moment
    products.json is edited to add a field ProductConfig already knows about.
    """
    entry_fields = set(_ProductEntry.model_fields)
    config_fields = set(ProductConfig.model_fields)
    assert entry_fields == config_fields
