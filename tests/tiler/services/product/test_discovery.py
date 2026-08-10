"""product/discovery: matching zarr dataset variables against
gridded_variables.json, and turning matches into Products.

_products_for_dataset is a pure function (no API dependency) so most of the
matching logic is exercised directly with synthetic variable sets/specs.
discover_products itself gets one thin integration test against a real API
instance with the S3 catalog call patched, reusing the existing canned
catalog fixture from tests/core/test_api.py.
"""

import json

import pytest

import data_access_service.tiler.services.product.discovery as discovery
from data_access_service.tiler.services.product.discovery import (
    _load_gridded_variable_specs,
    _product_id,
    _products_for_dataset,
    discover_products,
)


@pytest.fixture
def isolated_specs(tmp_path, monkeypatch):
    """Point discovery at a tmp gridded_variables.json."""
    cfg = tmp_path / "gridded_variables.json"
    monkeypatch.setattr(discovery, "_config_path", cfg)
    return cfg


def _write(cfg, entries):
    cfg.write_text(json.dumps(entries))


def test_product_id_scalar():
    assert _product_id("some_store.zarr".removesuffix(".zarr"), ["SST"]) == (
        "some_store:sst"
    )


def test_product_id_group_preserves_order():
    assert _product_id("store", ["UCUR", "VCUR"]) == "store:ucur+vcur"


def test_products_for_dataset_scalar_match():
    products = _products_for_dataset(
        uuid="u1",
        dname="my_store.zarr",
        source_path="s3://bucket/my_store.zarr/",
        variables=frozenset({"sst_mosaic", "time", "lat", "lon"}),
        specs=["sst_mosaic"],
    )
    assert len(products) == 1
    p = products[0]
    assert p.id == "my_store:sst_mosaic"
    assert p.source_path == "s3://bucket/my_store.zarr/"
    assert p.variable == "sst_mosaic"
    assert p.metadata_uuid == "u1"


def test_products_for_dataset_group_match():
    products = _products_for_dataset(
        uuid="u1",
        dname="currents.zarr",
        source_path="s3://bucket/currents.zarr/",
        variables=frozenset({"UCUR", "VCUR", "time"}),
        specs=[["UCUR", "VCUR"]],
    )
    assert len(products) == 1
    p = products[0]
    assert p.id == "currents:ucur+vcur"
    assert p.variable == ["UCUR", "VCUR"]


def test_products_for_dataset_partial_group_is_ignored():
    """Only one of a two-variable group present — no product for it, and it's
    not treated as a standalone scalar match either."""
    products = _products_for_dataset(
        uuid="u1",
        dname="currents.zarr",
        source_path="s3://bucket/currents.zarr/",
        variables=frozenset({"UCUR", "time"}),
        specs=[["UCUR", "VCUR"]],
    )
    assert products == []


def test_products_for_dataset_variable_not_in_allowlist_is_ignored():
    products = _products_for_dataset(
        uuid="u1",
        dname="my_store.zarr",
        source_path="s3://bucket/my_store.zarr/",
        variables=frozenset({"some_unlisted_variable", "time"}),
        specs=["sst_mosaic"],
    )
    assert products == []


def test_products_for_dataset_multiple_specs_can_each_match():
    products = _products_for_dataset(
        uuid="u1",
        dname="heatwave.zarr",
        source_path="s3://bucket/heatwave.zarr/",
        variables=frozenset({"sst_mosaic", "ssta_mosaic", "time"}),
        specs=["sst_mosaic", "ssta_mosaic", "not_present"],
    )
    ids = {p.id for p in products}
    assert ids == {"heatwave:sst_mosaic", "heatwave:ssta_mosaic"}


def test_load_gridded_variable_specs_missing_file_raises(isolated_specs):
    with pytest.raises(FileNotFoundError):
        _load_gridded_variable_specs()


def test_load_gridded_variable_specs_round_trips(isolated_specs):
    _write(isolated_specs, ["GSLA", ["UCUR", "VCUR"]])
    assert _load_gridded_variable_specs() == ["GSLA", ["UCUR", "VCUR"]]


def test_discover_products_from_real_api(isolated_specs):
    """Integration: discover_products against a real API instance with the S3
    catalog call patched to the canned fixture (same pattern as
    tests/core/test_api.py). The canned zarr dataset exposes analysed_sst,
    sea_ice_fraction, and others — only the allowlisted one becomes a product.
    """
    from pathlib import Path
    from unittest.mock import patch

    from aodn_cloud_optimised.lib import DataQuery

    from data_access_service.core.api import API

    _write(isolated_specs, ["analysed_sst"])

    canned = Path(__file__).resolve().parents[3] / "canned/catalog_uncached.json"
    with open(canned) as f:
        catalog = json.load(f)

    with patch.object(
        DataQuery.Metadata, "metadata_catalog_uncached", return_value=catalog
    ):
        api = API()
        api.initialize_metadata()

    products = discover_products(api)

    assert len(products) == 1
    p = products[0]
    assert p.id == "satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia:analysed_sst"
    assert p.variable == "analysed_sst"
    assert p.metadata_uuid == "a4170ca8-0942-4d13-bdb8-ad4718ce14bb"
    assert (
        p.source_path
        == "s3://aodn-cloud-optimised/satellite_ghrsst_l4_ramssa_1day_multi_sensor_australia.zarr/"
    )
