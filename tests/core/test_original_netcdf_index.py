"""find_original_netcdfs on a local table shaped like the data-index S3 Table."""

import logging

import pandas as pd
import pytest

from data_access_service.config.config import Config
from data_access_service.core.duckdbclients.dataindexclients import (
    DataIndexDuckDBClient,
)
from data_access_service.core.original_netcdf_index import find_original_netcdfs
from data_access_service.models.bounding_box import BoundingBox

UUID = "uuid-1"
START = pd.Timestamp("2020-01-10", tz="UTC")
END = pd.Timestamp("2020-01-20 23:59:59.999999999", tz="UTC")
# Covers the default file area (lat -40..-30, lon 140..150) in part.
AREA = [BoundingBox(min_lon=145, min_lat=-35, max_lon=155, max_lat=-25)]


@pytest.fixture
def client():
    cfg = Config.get_config().get_data_index_config()
    with DataIndexDuckDBClient() as c:
        # Under IntTestConfig the client does not attach the S3 Table, so a
        # local table takes its name.
        c.execute(f"ATTACH ':memory:' AS {cfg.catalog_name}")
        c.execute(f"CREATE SCHEMA {cfg.catalog_name}.{cfg.namespace}")
        c.execute(
            f"""CREATE TABLE {c.qualified_table} (
                metadata_uuid VARCHAR, bucket VARCHAR, key VARCHAR, format VARCHAR,
                time_coverage_start VARCHAR, time_coverage_end VARCHAR,
                geospatial_lat_min DOUBLE, geospatial_lat_max DOUBLE,
                geospatial_lon_min DOUBLE, geospatial_lon_max DOUBLE)"""
        )
        yield c


def _insert(
    client,
    key,
    start="2020-01-01T00:00:00Z",
    end="2020-01-31T00:00:00Z",
    lat=(-40.0, -30.0),
    lon=(140.0, 150.0),
    uuid=UUID,
    file_format="nc",
):
    client.execute(
        f"INSERT INTO {client.qualified_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [uuid, "imos-data", key, file_format, start, end, *lat, *lon],
    )


def _keys(files):
    return [f.key for f in files]


def test_picks_files_that_overlap_time_and_area(client):
    _insert(client, "inside.nc")
    _insert(
        client, "before.nc", start="2019-01-01T00:00:00Z", end="2019-12-31T00:00:00Z"
    )
    _insert(
        client, "after.nc", start="2020-02-01T00:00:00Z", end="2020-03-01T00:00:00Z"
    )
    _insert(client, "outside_area.nc", lon=(10.0, 20.0))
    _insert(client, "other_uuid.nc", uuid="uuid-2")
    _insert(client, "not_netcdf.csv", file_format="csv")

    files = find_original_netcdfs(client, UUID, START, END, AREA)

    assert _keys(files) == ["inside.nc"]
    assert files[0].bucket == "imos-data"
    assert files[0].time_coverage_start == pd.Timestamp("2020-01-01", tz="UTC")


def test_reads_every_parseable_time_format(client):
    _insert(client, "iso.nc", start="2020-01-15T00:00:00Z", end="2020-01-16T00:00:00Z")
    _insert(client, "compact.nc", start="20200115T000000Z", end="20200116T000000Z")
    _insert(client, "space.nc", start="2020-01-15 01:00:00+00:00", end="2020-01-16")

    files = find_original_netcdfs(client, UUID, START, END, AREA)

    assert _keys(files) == ["compact.nc", "iso.nc", "space.nc"]


def test_file_stored_in_0_360_matches_request_in_minus_180_180(client):
    # lon 200..250 is -160..-110
    _insert(client, "lon_0_360.nc", lon=(200.0, 250.0))
    area = [BoundingBox(min_lon=-150, min_lat=-35, max_lon=-140, max_lat=-25)]

    files = find_original_netcdfs(client, UUID, START, END, area)

    assert _keys(files) == ["lon_0_360.nc"]


def test_any_of_several_areas(client):
    _insert(client, "east.nc")
    _insert(client, "west.nc", lon=(10.0, 20.0))
    _insert(client, "neither.nc", lon=(60.0, 70.0))
    areas = AREA + [BoundingBox(min_lon=15, min_lat=-35, max_lon=25, max_lat=-25)]

    files = find_original_netcdfs(client, UUID, START, END, areas)

    assert _keys(files) == ["east.nc", "west.nc"]


def test_no_area_keeps_files_without_bbox(client):
    _insert(client, "no_bbox.nc", lat=(None, None), lon=(None, None))

    assert _keys(find_original_netcdfs(client, UUID, START, END)) == ["no_bbox.nc"]
    assert find_original_netcdfs(client, UUID, START, END, AREA) == []


def test_files_without_time_are_left_out_and_counted(client, caplog):
    _insert(client, "inside.nc")
    _insert(client, "no_time.nc", start=None, end=None)
    # Days since some epoch: cannot be parsed safely.
    _insert(client, "number_time.nc", start="23456.789", end="23457.789")

    with caplog.at_level(logging.WARNING):
        files = find_original_netcdfs(client, UUID, START, END, AREA)

    assert _keys(files) == ["inside.nc"]
    assert "2 files skipped" in caplog.text
