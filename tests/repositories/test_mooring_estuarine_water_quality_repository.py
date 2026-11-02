"""Unit tests for MooringEstuarineWaterQualityRepository.

The generic ParquetRepository behaviour is covered in test_sites_repository.py.
These tests check this dataset's own config, and run load / write_snapshot /
load_snapshot / reads against a small local copy shaped like the real one:
Hive-partitioned by site_code, float32 NOMINAL_DEPTH and float32 QC flags.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from data_access_service.core.duckdbclients.sitedbclient import SitesDuckDBClient
from data_access_service.sites.sites_repository import (
    REPOSITORY_CLASSES,
    MooringEstuarineWaterQualityRepository,
    build_repositories,
)

Repo = MooringEstuarineWaterQualityRepository
PRODUCT = "mooring-estuarine-water-quality"
VALUE_COLUMNS = ("TEMP", "PSAL", "CPHL", "DOXS", "FDOM", "PHYCO", "TURB", "FLUSI")


@pytest.fixture
def session(monkeypatch):
    monkeypatch.setattr(DuckDBClient, "create_s3_secret", lambda self, bucket: None)
    s = SitesDuckDBClient()
    yield s
    s.close()


@pytest.fixture
def source_rows():
    """Two sites, one depth each (as in the real data), plus a column load() drops."""
    n = 4
    df = pd.DataFrame(
        {
            "site_code": ["SA-BOSTON-BAY"] * 3 + ["VIC-WESTERN-PORT"],
            "TIME": pd.to_datetime(
                ["2026-10-01", "2026-10-02", "2026-10-03", "2026-10-02"]
            ),
            "LATITUDE": [-34.7, -34.7, -34.7, -38.4],
            "LONGITUDE": [135.9, 135.9, 135.9, 145.2],
            "NOMINAL_DEPTH": np.array([0.9, 0.9, 0.9, 0.7], dtype=np.float32),
            "filename": ["f"] * n,
        }
    )
    for i, col in enumerate(VALUE_COLUMNS):
        df[col] = np.arange(n, dtype=np.float32) + 10 * i
        df[f"{col}_quality_control"] = np.ones(n, dtype=np.float32)
    # Row 1 (Boston Bay, 2026-10-02): TEMP questionable (3), PHYCO bad (4).
    df.loc[1, "TEMP_quality_control"] = 3.0
    df.loc[1, "PHYCO_quality_control"] = 4.0
    return df


@pytest.fixture
def repo(session, tmp_path, monkeypatch, source_rows):
    """Repo pointed at a local Hive-partitioned dataset and a local snapshot path."""
    dataset = tmp_path / f"{Repo.table}.parquet"
    pq.write_to_dataset(
        pa.Table.from_pandas(source_rows, preserve_index=False),
        root_path=str(dataset),
        partition_cols=["site_code"],
    )
    monkeypatch.setattr(Repo, "dataset", str(dataset))
    monkeypatch.setattr(Repo, "snapshot_dataset", str(tmp_path / "snapshot.parquet"))
    return Repo(session)


# --- config ------------------------------------------------------------------


def test_registered_under_product_key(session):
    assert REPOSITORY_CLASSES[PRODUCT] is Repo
    assert isinstance(build_repositories(session)[PRODUCT], Repo)


def test_locations_follow_sites_convention():
    assert Repo.dataset == f"s3://{Repo.bucket}/{Repo.table}.parquet"
    assert Repo.snapshot_dataset == (
        f"s3://{Repo.snapshot_bucket}/imoslive/MOORING_ESTUARINE_WATER_QUALITY/"
        f"{Repo.table}.parquet"
    )


def test_schema_and_value_columns():
    assert Repo.site_column == "site_code"
    assert Repo.time_column == "TIME"
    assert Repo.group_column == "NOMINAL_DEPTH"
    assert Repo.value_columns == VALUE_COLUMNS


def test_every_value_column_paired_with_its_own_qc_column():
    assert Repo.value_columns_quality_control_columns == tuple(
        f"{c}_quality_control" for c in VALUE_COLUMNS
    )


# --- load / snapshot ---------------------------------------------------------


def test_load_keeps_only_load_columns(repo):
    repo.load()
    cols = [r[0] for r in repo.session.execute(f'DESCRIBE "{Repo.table}"').fetchall()]
    assert set(cols) == set(repo.load_columns)
    assert "filename" not in cols
    (count,) = repo.session.execute(f'SELECT count(*) FROM "{Repo.table}"').fetchone()
    assert count == 4


def test_snapshot_round_trip(repo):
    repo.load()
    before = repo.site_details("SA-BOSTON-BAY")
    repo.write_snapshot()

    # The API reads the snapshot in a fresh process with no loaded table.
    repo.session.execute(f'DROP TABLE "{Repo.table}"')
    repo.load_snapshot()
    after = repo.site_details("SA-BOSTON-BAY")
    pd.testing.assert_frame_equal(before, after)


# --- reads -------------------------------------------------------------------


def test_sites_in_date_range(repo):
    repo.load()
    sites = repo.sites_in_date_range()
    assert list(sites["site_code"]) == ["SA-BOSTON-BAY", "VIC-WESTERN-PORT"]

    sites = repo.sites_in_date_range(start="2026-10-03")
    assert list(sites["site_code"]) == ["SA-BOSTON-BAY"]


def test_site_details_nulls_values_whose_float_qc_flag_is_not_1(repo):
    repo.load()
    rows = repo.site_details("SA-BOSTON-BAY")
    assert len(rows) == 3
    # Only TEMP and PHYCO on row 1 failed QC; every other value is kept.
    assert list(rows["TEMP"].isna()) == [False, True, False]
    assert list(rows["PHYCO"].isna()) == [False, True, False]
    for col in set(VALUE_COLUMNS) - {"TEMP", "PHYCO"}:
        assert rows[col].notna().all(), col
    assert not any(c.endswith("_quality_control") for c in rows.columns)


def test_site_details_returns_site_depth(repo):
    repo.load()
    rows = repo.site_details("VIC-WESTERN-PORT")
    assert rows["NOMINAL_DEPTH"].tolist() == [pytest.approx(0.7)]
