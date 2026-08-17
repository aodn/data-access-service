"""The committed gridded_variables.json is valid and still says what it means.

Discovery only ever sees this file at boot, against real metadata. Loading it
here makes a semantically-drifted seed fail in CI rather than leaving the
tiler at 503 on deploy. There's no schema validation layer any more — the
file is just a flat list of variable names/pairs — so this exercises the real
loader (services/product/discovery.py::_load_gridded_variable_specs) directly.
Per-product tuning lives in products.json — see test_products_config.py.
"""

from data_access_service.tiler.services.product.discovery import (
    _load_gridded_variable_specs,
)


def test_committed_config_loads():
    specs = _load_gridded_variable_specs()
    assert specs, "the committed seed must not be empty"


def test_seed_has_the_expected_specification_shape():
    """19 specifications: 18 scalars plus one ordered pair. The pair is the one
    that fans out across 19 differently-gridded stores, so its arity is what the
    per-product override in products.json exists to work around."""
    specs = _load_gridded_variable_specs()
    pairs = [s for s in specs if isinstance(s, list)]
    scalars = [s for s in specs if isinstance(s, str)]

    assert len(specs) == 19
    assert len(scalars) == 18
    assert pairs == [["UCUR", "VCUR"]]


def test_no_duplicate_specifications():
    specs = _load_gridded_variable_specs()
    keys = [str(s) for s in specs]
    assert len(keys) == len(set(keys))
