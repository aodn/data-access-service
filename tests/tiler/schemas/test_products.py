"""ProductOverride parsing and products_customisation config loading."""

import pytest
from pydantic import ValidationError

from data_access_service.config.config import Config
from data_access_service.tiler.schemas.products import (
    ProductOverride,
    load_product_overrides,
    parse_product_overrides,
)

# --- ProductOverride / products config ----------------------------------------


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


def test_load_missing_products_file_raises(monkeypatch):
    """load_product_overrides has no path to inject — it always reads the
    committed config.yaml — so a missing/misconfigured file is simulated at
    Config.load_config, the one place that actually opens it."""

    def _raise_missing(path):
        raise FileNotFoundError(path)

    monkeypatch.setattr(Config, "load_config", _raise_missing)
    with pytest.raises(FileNotFoundError):
        load_product_overrides()
