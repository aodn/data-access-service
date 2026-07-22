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

# lod_grids is the one deliberate exception: it's computed from the store's
# native dimensions (requires an S3 round-trip to resolve), not per-product
# config, and is already served per-date via /manifest.json instead.
_COMPUTED_ONLY_FIELDS = {"lod_grids"}


def test_product_config_fields_match_product_except_computed():
    product_fields = {f.name for f in dataclasses.fields(Product)}
    config_fields = set(ProductConfig.model_fields)
    assert product_fields - _COMPUTED_ONLY_FIELDS == config_fields
