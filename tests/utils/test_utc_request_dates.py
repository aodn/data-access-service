"""Dates in a request must mean UTC - issue 9061.

Principle: a datetime with no offset is read as UTC; one that carries `Z` or an
offset is converted to UTC. The process timezone is pinned to UTC as a safety
net (see tests/test_timezone_defaults.py), but that only covers naive clock
calls - it does not parse input, format output, or stop a value being frozen at
import time. These are the parts it cannot reach.
"""

import pandas as pd
import pytest

from unittest.mock import MagicMock

from data_access_service.core.constants import UNIX_EPOCH_UTC
from data_access_service.models.subset_request import SubsetRequest
from data_access_service.utils.date_time_utils import (
    end_of_day_nano,
    ensure_timezone,
    parse_date,
    has_explicit_time,
    resolve_non_specified_dates,
    to_utc_bounds,
    to_naive_utc_string,
    to_utc_iso_z,
)
from data_access_service.utils.subset_request_resolver import resolve_date_range


class TestAcceptsTimezoneAwareInput:
    """parse_date used to call tz_localize() unconditionally, which raises on an
    already-aware value - so the ISO-8601 a JS date picker emits crashed."""

    @pytest.mark.parametrize(
        "value",
        ["2024-01-15", "2024-01-15T00:00:00Z", "2024-01-15T10:00:00+10:00"],
    )
    def test_does_not_raise(self, value):
        start, end = resolve_date_range(value, value)
        assert start.tz is not None and end.tz is not None

    def test_offset_is_converted_not_relabelled(self):
        # 10:00+10:00 is midnight UTC, not 10:00 UTC
        start, _ = resolve_date_range("2024-01-15T10:00:00+10:00", "2024-01-16")
        assert start == pd.Timestamp("2024-01-15T00:00:00Z")

    def test_naive_is_read_as_utc(self):
        start, _ = resolve_date_range("2024-01-15", "2024-01-16")
        assert start == pd.Timestamp("2024-01-15T00:00:00Z")


class TestEndBoundCoversTheUnitItWasGivenTo:
    """An inclusive end bound includes the whole unit the caller specified: a
    date means that day, a whole second means that second.

    The portal (aodn-portal-v2 PR #957) sends toUtcEndOfDay - which is
    23:59:59.999 - through the format "YYYY-MM-DDTHH:mm:ss[Z]", so the
    milliseconds never reach us. Reading 23:59:59Z literally would leave the
    last second of the day outside this range and outside the next one.
    """

    def test_portal_end_of_day_covers_the_whole_second(self):
        start, end = to_utc_bounds("2020-01-01T00:00:00Z", "2020-01-02T23:59:59Z")
        assert start == pd.Timestamp("2020-01-01T00:00:00Z")
        assert end == pd.Timestamp("2020-01-02T23:59:59.999999999Z")

    def test_date_only_is_widened_to_the_whole_utc_day(self):
        start, end = to_utc_bounds("2024-01-15", "2024-01-15")
        assert start == pd.Timestamp("2024-01-15T00:00:00Z")
        assert end == pd.Timestamp("2024-01-15T23:59:59.999999999Z")

    def test_a_mid_day_second_is_not_widened_past_that_second(self):
        _, end = to_utc_bounds("2024-01-15", "2024-01-15T10:00:00Z")
        assert end == pd.Timestamp("2024-01-15T10:00:00.999999999Z")

    def test_milliseconds_are_widened_only_below_the_millisecond(self):
        _, end = to_utc_bounds("2024-01-15", "2024-01-15T10:00:00.123Z")
        assert end == pd.Timestamp("2024-01-15T10:00:00.123999999Z")

    def test_full_nanosecond_precision_is_left_alone(self):
        _, end = to_utc_bounds("2024-01-15", "2024-01-15T10:00:00.123456789Z")
        assert end == pd.Timestamp("2024-01-15T10:00:00.123456789Z")

    def test_no_gap_between_consecutive_day_ranges(self):
        """The end of one day and the start of the next must leave nothing
        between them, or that sliver is undownloadable."""
        _, day1_end = to_utc_bounds("2024-01-15T00:00:00Z", "2024-01-15T23:59:59Z")
        day2_start, _ = to_utc_bounds("2024-01-16T00:00:00Z", "2024-01-16T23:59:59Z")
        assert day2_start - day1_end == pd.Timedelta(nanoseconds=1)

    def test_month_input_still_covers_the_whole_month(self):
        start, end = to_utc_bounds("02-2024", "02-2024")
        assert start == pd.Timestamp("2024-02-01T00:00:00Z")
        assert end == pd.Timestamp("2024-02-29T23:59:59.999999999Z")

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("2024-01-15", False),
            ("02-2024", False),
            ("2024-01-15T10:00:00Z", True),
            ("2024-01-15 10:00:00", True),
        ],
    )
    def test_has_explicit_time(self, value, expected):
        assert has_explicit_time(value) is expected


class TestCustomFormatKeepsNanoseconds:
    """pd.to_datetime parses all 9 digits of a "%f" fraction. The manual fix-up
    that used to add the last 3 back was written for datetime.strptime, which
    stops at microseconds - on pandas it added them twice."""

    def test_nine_digit_fraction_is_parsed_exactly(self):
        result = parse_date(
            "2024-01-15 23:59:59.123456789",
            format_to_convert="%Y-%m-%d %H:%M:%S.%f",
        )
        assert result == pd.Timestamp("2024-01-15 23:59:59.123456789", tz="UTC")
        assert result.nanosecond == 789

    def test_six_digit_fraction_is_unchanged(self):
        result = parse_date(
            "2024-01-15 23:59:59.123456",
            format_to_convert="%Y-%m-%d %H:%M:%S.%f",
        )
        assert result == pd.Timestamp("2024-01-15 23:59:59.123456", tz="UTC")


class TestEnsureTimezoneNormalisesToUtc:
    def test_naive_is_assumed_utc(self):
        assert ensure_timezone(pd.Timestamp("2024-01-15 10:00")).tzname() == "UTC"

    def test_aware_keeps_the_instant_but_loses_the_offset(self):
        result = ensure_timezone(pd.Timestamp("2024-01-15T10:00:00+10:00"))
        assert result.tzname() == "UTC"
        # the offset must be gone, or a later strftime() would render 10:00
        assert result.strftime("%Y-%m-%dT%H:%M:%S") == "2024-01-15T00:00:00"


class TestApiOutputFormat:
    """%z writes "+0000" with no colon, which the ECMAScript date-time format
    does not define. Browsers accept it; the spec does not."""

    def test_utc_renders_with_z(self):
        assert to_utc_iso_z(pd.Timestamp("2024-01-15T00:00:00Z")) == (
            "2024-01-15T00:00:00Z"
        )

    def test_naive_is_treated_as_utc(self):
        assert to_utc_iso_z(pd.Timestamp("2024-01-15T00:00:00")) == (
            "2024-01-15T00:00:00Z"
        )

    def test_offset_is_converted_before_rendering(self):
        assert to_utc_iso_z(pd.Timestamp("2024-01-15T10:00:00+10:00")) == (
            "2024-01-15T00:00:00Z"
        )


class TestParquetFilterKeepsTheWholeRange:
    """The row-count filter used to format the range as "%Y-%m-%d". The library
    compares against pd.to_datetime(end_str), so the end became midnight: a
    one-day range collapsed to one instant, counted 0 rows, and was dropped."""

    def test_end_of_day_survives_formatting(self):
        _, end = resolve_date_range("2024-01-15", "2024-01-15")
        assert to_naive_utc_string(end) == "2024-01-15 23:59:59.999999999"
        # what the library will actually compare the TIME column against
        assert pd.to_datetime(to_naive_utc_string(end)) == pd.Timestamp(
            "2024-01-15 23:59:59.999999999"
        )

    def test_string_is_naive_because_the_time_column_is(self):
        _, end = resolve_date_range("2024-01-15", "2024-01-15")
        assert pd.to_datetime(to_naive_utc_string(end)).tz is None

    def test_offset_is_converted_to_utc_not_dropped(self):
        assert to_naive_utc_string(pd.Timestamp("2024-01-15T10:00:00+10:00")) == (
            "2024-01-15 00:00:00.000000000"
        )

    def test_two_halves_of_one_day_stay_distinct(self):
        # a binary split lands inside one day on large datasets; truncating to
        # the calendar day made both halves the same string
        left_end = pd.Timestamp("2024-01-15 11:59:59.999999999Z")
        right_start = pd.Timestamp("2024-01-15 12:00:00Z")
        assert to_naive_utc_string(left_end) != to_naive_utc_string(right_start)


class TestOpenEndDate:
    def test_open_end_date_uses_the_utc_date(self):
        start, end = resolve_non_specified_dates("non-specified", "non-specified")
        assert parse_date(start) == UNIX_EPOCH_UTC
        assert parse_date(end) == end_of_day_nano(pd.Timestamp.now(tz="UTC"))

    def test_open_end_date_is_already_the_end_of_the_day(self):
        """The default must not rely on end_of_specified_precision running
        downstream to become the end of the day - it says so itself."""
        _, end = resolve_non_specified_dates("non-specified", "non-specified")
        assert parse_date(end).strftime("%H:%M:%S") == "23:59:59"
        assert parse_date(end).nanosecond == 999


class TestTrimKeepsRequestedPrecision:
    """The extent trim must narrow a range, never widen it back to whole days.

    It used to end with start_of_day_nano()/end_of_day_nano(), so any request
    that named a time lost it the moment the dataset reported an extent - the
    exact precision this module exists to preserve.
    """

    @staticmethod
    def _api_with_extent(start: str, end: str):
        api = MagicMock()
        api.get_temporal_extent.return_value = (
            pd.Timestamp(start),
            pd.Timestamp(end),
        )
        return api

    def test_explicit_times_survive_the_trim(self):
        api = self._api_with_extent("2020-01-01T00:00:00Z", "2030-01-01T00:00:00Z")
        start, end = resolve_date_range(
            "2024-01-15T10:00:00Z",
            "2024-01-15T12:00:00Z",
            api=api,
            uuid="u",
            keys=["k"],
        )
        assert start == pd.Timestamp("2024-01-15T10:00:00Z")
        assert end == pd.Timestamp("2024-01-15T12:00:00.999999999Z")

    def test_trim_matches_the_untrimmed_parse(self):
        # A request wholly inside the extent must resolve to exactly what it
        # resolves to with no extent to trim against.
        api = self._api_with_extent("2020-01-01T00:00:00Z", "2030-01-01T00:00:00Z")
        trimmed = resolve_date_range(
            "2024-01-15T10:30:00Z",
            "2024-01-16T18:45:00Z",
            api=api,
            uuid="u",
            keys=["k"],
        )
        assert trimmed == resolve_date_range(
            "2024-01-15T10:30:00Z", "2024-01-16T18:45:00Z"
        )

    def test_date_only_request_still_covers_the_whole_day(self):
        # Dropping the widening must not regress the common case: a bare date
        # still means that entire day.
        api = self._api_with_extent("2020-01-01T00:00:00Z", "2030-01-01T00:00:00Z")
        start, end = resolve_date_range(
            "2024-01-15", "2024-01-15", api=api, uuid="u", keys=["k"]
        )
        assert start == pd.Timestamp("2024-01-15T00:00:00Z")
        assert end == pd.Timestamp("2024-01-15T23:59:59.999999999Z")

    def test_out_of_range_bound_is_still_clamped_to_the_extent(self):
        api = self._api_with_extent(
            "2024-01-10T00:00:00Z", "2024-01-20T23:59:59.999999999Z"
        )
        start, end = resolve_date_range(
            "2020-01-01", "2029-12-31", api=api, uuid="u", keys=["k"]
        )
        assert start == pd.Timestamp("2024-01-10T00:00:00Z")
        assert end == pd.Timestamp("2024-01-20T23:59:59.999999999Z")


class TestMonthOnlyBoundsHandledAtEntry:
    def test_month_only_expands_to_the_whole_month(self):
        start, end = to_utc_bounds("02-2024", "02-2024")
        assert start == pd.Timestamp("2024-02-01T00:00:00Z")
        assert end == pd.Timestamp("2024-02-29T23:59:59.999999999Z")  # leap year

    def test_mixed_month_only_and_iso_bounds(self):
        # The legacy check is per-bound now, so one side being ISO no longer
        # makes the other side fall through unexpanded.
        start, end = to_utc_bounds("02-2024", "2024-03-15T06:00:00Z")
        assert start == pd.Timestamp("2024-02-01T00:00:00Z")
        assert end == pd.Timestamp("2024-03-15T06:00:00.999999999Z")


class TestSubsetRequestDateValidation:
    def test_mixed_naive_and_aware_bounds_do_not_crash(self):
        # pd.Timestamp() left one bound naive and the other aware, and the
        # start > end check then raised TypeError before any resolving ran.
        request = SubsetRequest(
            uuid="u",
            keys=["k"],
            start_date="2024-01-15",
            end_date="2024-01-16T00:00:00Z",
            recipient="a@b.com",
            output_format="netcdf",
        )
        assert request.start_date == "2024-01-15"

    def test_start_after_end_is_still_rejected(self):
        with pytest.raises(ValueError, match="must be on or before"):
            SubsetRequest(
                uuid="u",
                keys=["k"],
                start_date="2024-01-17",
                end_date="2024-01-16T00:00:00Z",
                recipient="a@b.com",
                output_format="netcdf",
            )
