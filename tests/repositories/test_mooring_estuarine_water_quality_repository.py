"""Unit tests for MooringEstuarineWaterQualityRepository.

The generic ParquetRepository behaviour is covered in test_sites_repository.py.
These tests check this dataset's own config, and run load / write_snapshot /
load_snapshot / reads against a small local copy shaped like the real one:
Hive-partitioned by site_code, one row set per instrument (named only in the
filename), float32 NOMINAL_DEPTH and float32 QC flags.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_access_service.core.duckdbclients.basedbclient import DuckDBClient
from data_access_service.core.duckdbclients.sitedbclient import SitesDuckDBClient
from data_access_service.core.routes import (
    helpers,
)  # noqa: F401  (avoids an import cycle)
from data_access_service.sites.site_feature_service import SiteFeatureService
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


SONDE = "EXO2-SONDE"
CAD = "HAI-AHIW2A-CAD"
SONDE_VARS = set(VALUE_COLUMNS) - {"FLUSI"}
CAD_VARS = {"TEMP", "FLUSI"}


def _filename(site: str, instrument: str) -> str:
    return (
        f"SARDI_ESTUARINE-COASTAL-MOORINGS_CKOSTUZ_20260220T033000Z_{site}"
        f"_FV00_SACM-XYZ-20260220_{instrument}.nc"
    )


@pytest.fixture
def source_rows():
    """Shaped like the real data: each instrument writes its own rows.

    Boston Bay has the sonde and the CAD at the same depth; Western Port has
    only the sonde. Each instrument row only fills the variables it measures.
    """
    rows = [
        ("SA-BOSTON-BAY", "2026-10-01", 0.9, SONDE),
        ("SA-BOSTON-BAY", "2026-10-02", 0.9, SONDE),
        ("SA-BOSTON-BAY", "2026-10-03", 0.9, SONDE),
        ("SA-BOSTON-BAY", "2026-10-01", 0.9, CAD),
        ("SA-BOSTON-BAY", "2026-10-02", 0.9, CAD),
        ("VIC-WESTERN-PORT", "2026-10-02", 0.7, SONDE),
    ]
    df = pd.DataFrame(
        {
            "site_code": [r[0] for r in rows],
            "TIME": pd.to_datetime([r[1] for r in rows]),
            "LATITUDE": [-38.4 if r[0] == "VIC-WESTERN-PORT" else -34.7 for r in rows],
            "LONGITUDE": [145.2 if r[0] == "VIC-WESTERN-PORT" else 135.9 for r in rows],
            "NOMINAL_DEPTH": np.array([r[2] for r in rows], dtype=np.float32),
            "filename": [_filename(r[0], r[3]) for r in rows],
        }
    )
    for i, col in enumerate(VALUE_COLUMNS):
        measured = [col in (SONDE_VARS if r[3] == SONDE else CAD_VARS) for r in rows]
        df[col] = np.where(measured, np.arange(len(rows)) + 10.0 * i, np.nan).astype(
            np.float32
        )
        df[f"{col}_quality_control"] = np.where(measured, 1.0, np.nan).astype(
            np.float32
        )
    # Boston Bay sonde 2026-10-02: TEMP questionable (3), PHYCO bad (4).
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
    assert Repo.group_columns == ("instrument", "NOMINAL_DEPTH")
    assert set(Repo.derived_columns) == {"instrument"}
    assert Repo.value_columns == VALUE_COLUMNS


def test_every_value_column_paired_with_its_own_qc_column():
    assert Repo.value_columns_quality_control_columns == tuple(
        f"{c}_quality_control" for c in VALUE_COLUMNS
    )


@pytest.mark.parametrize(
    "filename, expected",
    [
        (_filename("SA-BOSTON-BAY", SONDE), SONDE),
        (_filename("SA-BOSTON-BAY", CAD), CAD),
        ("not_a_known_pattern.nc", "unknown"),
        (None, "unknown"),
    ],
)
def test_instrument_from_filename(session, filename, expected):
    expr = Repo.derived_columns["instrument"]
    (value,) = session.execute(
        f"SELECT {expr} FROM (SELECT ?::VARCHAR AS filename)", [filename]
    ).fetchone()
    assert value == expected


# --- load / snapshot ---------------------------------------------------------


def test_load_derives_instrument_and_drops_filename(repo):
    repo.load()
    cols = [r[0] for r in repo.session.execute(f'DESCRIBE "{Repo.table}"').fetchall()]
    assert set(cols) == set(repo.load_columns)
    assert "filename" not in cols
    counts = dict(
        repo.session.execute(
            f"SELECT site_code || '/' || instrument, count(*) FROM \"{Repo.table}\" "
            "GROUP BY 1"
        ).fetchall()
    )
    assert counts == {
        f"SA-BOSTON-BAY/{SONDE}": 3,
        f"SA-BOSTON-BAY/{CAD}": 2,
        f"VIC-WESTERN-PORT/{SONDE}": 1,
    }


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


def test_site_details_ordered_by_instrument_then_time(repo):
    repo.load()
    rows = repo.site_details("SA-BOSTON-BAY")
    assert list(rows["instrument"]) == [SONDE] * 3 + [CAD] * 2
    assert list(rows["TIME"].dt.day) == [1, 2, 3, 1, 2]


def test_site_details_nulls_values_whose_float_qc_flag_is_not_1(repo):
    repo.load()
    sonde = repo.site_details("SA-BOSTON-BAY").query("instrument == @SONDE")
    # Only TEMP and PHYCO on the 2026-10-02 row failed QC; every other value is kept.
    assert list(sonde["TEMP"].isna()) == [False, True, False]
    assert list(sonde["PHYCO"].isna()) == [False, True, False]
    for col in SONDE_VARS - {"TEMP", "PHYCO"}:
        assert sonde[col].notna().all(), col
    assert not any(c.endswith("_quality_control") for c in sonde.columns)


def test_details_nest_by_instrument_then_depth(repo):
    repo.load()
    feature = SiteFeatureService(repo).site_details("SA-BOSTON-BAY", None, None)
    props = feature["properties"]
    assert set(props) == {f"instrument_{SONDE}", f"instrument_{CAD}"}
    sonde = props[f"instrument_{SONDE}"]["NOMINAL_DEPTH_0.9"]
    cad = props[f"instrument_{CAD}"]["NOMINAL_DEPTH_0.9"]
    # TEMP is no longer merged across the two instruments.
    assert len(sonde["TEMP"]) == 2  # one of three failed QC
    assert len(cad["TEMP"]) == 2
    assert len(sonde["FLUSI"]) == 0
    assert len(cad["FLUSI"]) == 2
    assert len(cad["PSAL"]) == 0


def test_details_sonde_only_site(repo):
    repo.load()
    feature = SiteFeatureService(repo).site_details("VIC-WESTERN-PORT", None, None)
    assert list(feature["properties"]) == [f"instrument_{SONDE}"]
    assert list(feature["properties"][f"instrument_{SONDE}"]) == ["NOMINAL_DEPTH_0.7"]
