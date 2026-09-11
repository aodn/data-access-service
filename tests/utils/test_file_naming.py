"""Names of the files the user downloads - see backlog issue 8600."""

import pytest

from data_access_service.utils.file_naming import (
    build_download_base_name,
    dataset_base_name,
    sanitise_for_filename,
)

COLLECTION = "IMOS - ANMN - CTD Profiles"
SANITISED_COLLECTION = "IMOS_-_ANMN_-_CTD_Profiles"


@pytest.mark.parametrize(
    "key, expected",
    [
        ("anmn_ctd.parquet", "anmn_ctd"),
        (
            "radar_CoffsHarbour_wind_delayed_qc.zarr",
            "radar_CoffsHarbour_wind_delayed_qc",
        ),
        ("no_suffix", "no_suffix"),
        # Only the storage suffix goes, not every dot in the name.
        ("a.b.parquet", "a.b"),
    ],
)
def test_dataset_base_name_drops_only_the_storage_suffix(key, expected):
    assert dataset_base_name(key) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("IMOS - ANMN - CTD Profiles", SANITISED_COLLECTION),
        # A "/" would make a fake s3 folder, a space would break the email href.
        ("Wave / Buoy (NSW), 2010-2020", "Wave_Buoy_NSW_2010-2020"),
        ("  padded  ", "padded"),
        ("", ""),
        ("///", ""),
        ("温度", ""),
    ],
)
def test_sanitise_for_filename(text, expected):
    assert sanitise_for_filename(text) == expected


def test_sanitise_for_filename_caps_the_length():
    assert len(sanitise_for_filename("a" * 500)) == 150


def test_single_dataset_collection_is_named_after_the_collection_alone():
    assert (
        build_download_base_name(COLLECTION, "anmn_ctd.parquet", False)
        == SANITISED_COLLECTION
    )


def test_multi_dataset_collection_names_the_selected_dataset_too():
    assert (
        build_download_base_name(COLLECTION, "anmn_ctd.parquet", True)
        == f"{SANITISED_COLLECTION}-anmn_ctd"
    )


@pytest.mark.parametrize("title", [None, "", "   ", "///"])
def test_unusable_title_falls_back_to_the_dataset(title):
    assert build_download_base_name(title, "anmn_ctd.zarr", True) == "anmn_ctd"
