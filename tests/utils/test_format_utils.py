"""Unit tests for the output format rules shared by the size estimate and the
download (utils/format_utils.py)."""

import pytest

from data_access_service.utils.format_utils import (
    KEY_SUFFIX_PARQUET,
    KEY_SUFFIX_ZARR,
    check_key_supports_format,
    check_storage_supports_format,
)


class TestFormatChecks:
    """The single owner of the key/format rule, shared by the size estimate
    (check_datasource_supports_format) and the download
    (subsetting_main.init)."""

    @pytest.mark.parametrize(
        "key, output_format",
        [
            ("a.parquet", "csv"),
            ("a.zarr", "netcdf"),
            ("a.zarr", "geotiff"),
        ],
    )
    def test_matching_pairs_pass(self, key, output_format):
        check_key_supports_format(key, output_format)

    @pytest.mark.parametrize(
        "key, output_format, expected_suffix",
        [
            ("a.parquet", "netcdf", r"\.zarr"),
            ("a.parquet", "geotiff", r"\.zarr"),
            ("a.zarr", "csv", r"\.parquet"),
        ],
    )
    def test_mismatched_pairs_raise(self, key, output_format, expected_suffix):
        with pytest.raises(ValueError, match=f"downloads from {expected_suffix} keys"):
            check_key_supports_format(key, output_format)

    @pytest.mark.parametrize("output_format", ["csv", "netcdf", "geotiff"])
    def test_key_of_unknown_storage_type_is_not_judged(self, output_format):
        # API.get_mapped_meta_data answers an unknown uuid with a "not_exist"
        # sentinel key. It carries no suffix, so nothing can be concluded about
        # its storage type and it must not be rejected here.
        check_key_supports_format("not_exist", output_format)

    def test_storage_type_can_be_supplied_directly(self):
        # The size estimate knows the storage type from the resolved
        # datasource, so it judges a key whose name gives nothing away.
        check_storage_supports_format("no-suffix", KEY_SUFFIX_ZARR, "netcdf")
        with pytest.raises(ValueError, match=r"downloads from \.parquet keys"):
            check_storage_supports_format("no-suffix", KEY_SUFFIX_ZARR, "csv")
        with pytest.raises(ValueError, match=r"downloads from \.zarr keys"):
            check_storage_supports_format("no-suffix", KEY_SUFFIX_PARQUET, "netcdf")

    def test_unknown_storage_type_runs_no_check(self):
        check_storage_supports_format("no-suffix", None, "csv")
