"""Date and time helpers. Everything here works in UTC.

In the examples a quoted value stands for the pd.Timestamp of that instant,
e.g. "2024-01-15 10:00" means pd.Timestamp("2024-01-15 10:00").
"""

import logging
import re
import time

import pandas as pd
import pytz

from pandas import Timestamp
from functools import wraps
from typing import Tuple
from datetime import tzinfo
from inspect import iscoroutinefunction

from pandas._libs import NaTType

from data_access_service.core.constants import NON_SPECIFIED

YEAR_MONTH_DAY = "%Y-%m-%d"

# %z do not produce Z for +0000, %z just add the offset value which is fine
# for client, however if you prefer to have Z the please replace the string
# output manually
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
MIN_DATE = "1970-01-01T00:00:00Z"


log = logging.getLogger(__name__)


def _to_timezone(
    ts: pd.Timestamp | NaTType, time_zone: str | tzinfo
) -> Timestamp | NaTType:
    """Attach a timezone to a naive timestamp, or convert an aware one to it.

    :param ts: naive or timezone-aware timestamp, or NaT
    :param time_zone: target timezone, e.g. pytz.UTC or "Asia/Tokyo"
    :return: the same instant expressed in `time_zone`; NaT passes through

    Example:
        _to_timezone("2024-01-15 10:00", pytz.UTC) -> "2024-01-15 10:00:00+00:00"
    """
    if ts.tz is None:
        return ts.tz_localize(time_zone)
    return ts.tz_convert(time_zone)


def parse_date(
    date_string: str,
    format_to_convert: str | None = None,
    time_zone: str | tzinfo = pytz.UTC,
) -> pd.Timestamp | NaTType:
    """Parse a date string into a timezone-aware timestamp.

    A string with no offset is read as `time_zone`; one that already carries
    `Z` or an offset is converted, never re-localized - tz_localize() raises on
    an aware value, which is what used to crash subsetting on the ISO-8601 a JS
    date picker produces.

    :param date_string: date to parse, e.g. "2024-01-15" or "2024-01-15T10:00:00Z"
    :param format_to_convert: strptime format, for a string that is not ISO-8601
    :param time_zone: timezone to read a string with no offset as (UTC by default)
    :return: timestamp in `time_zone`, or NaT when the value parses to NaT

    Example:
        parse_date("2024-01-15") -> "2024-01-15 00:00:00+00:00"
        parse_date("2024-01-15T10:00:00+10:00") -> "2024-01-15 00:00:00+00:00"
    """
    if format_to_convert is None:
        ts = pd.Timestamp(date_string)
    else:
        # pd.to_datetime keeps all 9 digits of a "%f" fraction. There used to be
        # a manual fix-up adding the last 3 digits back, written for
        # datetime.strptime, which stops at microseconds - on pandas it added
        # them a second time (.123456789 became .123457578).
        ts = pd.to_datetime(date_string, format=format_to_convert)
    return _to_timezone(ts, time_zone)


def end_of_day_nano(ts: pd.Timestamp) -> pd.Timestamp:
    """Move a timestamp to the last nanosecond of its day.

    :param ts: any timestamp
    :return: the same day at 23:59:59.999999999

    Example:
        end_of_day_nano("2024-01-15 10:00") -> "2024-01-15 23:59:59.999999999"
    """
    # a coarser-resolution timestamp (e.g. "us") silently drops the
    # nanosecond=999 in replace(), so force nanosecond resolution first
    return ts.as_unit("ns").replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=999999,
        # pyrefly: ignore [unexpected-keyword]
        nanosecond=999,
    )


def get_final_day_of_month_(date: pd.Timestamp) -> pd.Timestamp:
    """Move a timestamp to the last nanosecond of its month.

    :param date: any timestamp; a naive one is read as UTC
    :return: last day of that month at 23:59:59.999999999, UTC-aware

    Example:
        get_final_day_of_month_("2024-02-05") -> "2024-02-29 23:59:59.999999999+00:00"
    """
    if date.tz is None:
        date = date.tz_localize(pytz.UTC)
    return end_of_day_nano(date + pd.offsets.MonthEnd(0))


def get_first_day_of_month(date: pd.Timestamp) -> pd.Timestamp:
    """Roll a timestamp forward to a month's first day at midnight.

    Despite the name, only a date already on the 1st stays in its own month;
    any later day rolls forward to the NEXT month, because pandas' MonthBegin(0)
    rolls forward. The one caller always passes the 1st, so this is safe today -
    but do not reuse it on an arbitrary date without checking.

    :param date: any timestamp; its timezone is kept as-is
    :return: a month's first day at 00:00:00

    Example:
        get_first_day_of_month("2024-02-01 18:30") -> "2024-02-01 00:00:00"
        get_first_day_of_month("2024-02-15 18:30") -> "2024-03-01 00:00:00"  (!)
    """
    first_day = date + pd.offsets.MonthBegin(0)
    return first_day.normalize()


def ensure_timezone(dt: pd.Timestamp | NaTType) -> pd.Timestamp | NaTType:
    """Normalise a timestamp to UTC.

    A naive value is assumed to be UTC; an aware value is converted, so the
    instant never moves but the offset is always +00:00. Keeping the client's
    own offset would compare correctly yet be wrong to strftime() or strip the
    tz from, which several callers do.

    :param dt: naive or aware timestamp, or NaT
    :return: the same instant in UTC; NaT passes through

    Example:
        ensure_timezone("2024-01-15 10:00") -> "2024-01-15 10:00:00+00:00"
        ensure_timezone("2024-01-15T10:00:00+10:00") -> "2024-01-15 00:00:00+00:00"
    """
    if isinstance(dt, NaTType):
        return dt
    if dt.tz is None:
        return dt.tz_localize(pytz.UTC)
    return dt.tz_convert(pytz.UTC)


def to_utc_iso_z(ts: pd.Timestamp) -> str:
    """Format a timestamp for the API as ISO-8601 UTC with a `Z` suffix.

    strftime("%z") would write "+0000" with no colon, which is not one of the
    two forms the ECMAScript date-time format defines, so JS would parse it on
    browser goodwill rather than by spec.

    :param ts: any timestamp; a naive one is read as UTC
    :return: string like "2024-01-15T00:00:00Z" (seconds precision)

    Example:
        to_utc_iso_z("2024-01-15T10:00:00+10:00") -> "2024-01-15T00:00:00Z"
    """
    return ensure_timezone(pd.Timestamp(ts)).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_naive_utc(ts: pd.Timestamp | None) -> pd.Timestamp | None:
    """Drop the timezone from a timestamp, keeping the UTC instant.

    The zarr time coordinate is tz-naive and cannot be compared against a
    timezone-aware value.

    :param ts: any timestamp, or None
    :return: naive UTC timestamp; None passes through so an open slice stays open

    Example:
        to_naive_utc("2024-01-15T10:00:00+10:00") -> "2024-01-15 00:00:00"
    """
    if ts is None:
        return None
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def to_naive_utc_string(ts: pd.Timestamp) -> str:
    """Format a timestamp as a naive-UTC string, all 9 fraction digits kept.

    The parquet time columns are tz-naive TIMESTAMP_NS holding UTC, and the
    filter builders compare them against pd.to_datetime(<this string>), so the
    value has to be naive UTC. "%Y-%m-%d" instead would collapse the end of a
    range to midnight and lose the rest of the day.

    :param ts: any timestamp; a naive one is read as UTC
    :return: string like "2024-01-15 23:59:59.999999999"

    Example:
        to_naive_utc_string("2024-01-15 23:59:59.999999999+00:00")
        -> "2024-01-15 23:59:59.999999999"
    """
    ts = to_naive_utc(ensure_timezone(pd.Timestamp(ts)))
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S.%f')}{ts.nanosecond:03d}"


def split_date_range_binary(
    start_date: Timestamp, end_date: Timestamp
) -> tuple[Timestamp, Timestamp | NaTType, Timestamp | NaTType, Timestamp]:
    """Split a date range into two halves that touch but never overlap.

    Filters treat both ends as inclusive, so the halves are separated by a 1ns
    gap: left_end + 1ns == right_start. Together they still cover the whole
    range, without sharing any timestamp.

    :param start_date: inclusive start; a naive value is read as UTC and a
        string is coerced to a Timestamp
    :param end_date: inclusive end, same handling
    :return: (left_start, left_end, right_start, right_end), all UTC-aware
    :raises ValueError: if either bound is NaT, if end is before start, or if
        start == end - a zero-length range has no two halves. A range of one
        nanosecond is fine: it holds two ticks, one for each half.

    Example:
        split_date_range_binary("2024-01-01", "2024-01-03") -> left half ends
        "2024-01-01 23:59:59.999999999", right half starts "2024-01-02 00:00:00"
    """
    if not isinstance(start_date, pd.Timestamp):
        start_date = pd.Timestamp(start_date)
    if not isinstance(end_date, pd.Timestamp):
        end_date = pd.Timestamp(end_date)

    start_date = ensure_timezone(start_date)
    end_date = ensure_timezone(end_date)

    if isinstance(start_date, NaTType):
        raise ValueError(f"Invalid start_date of type NaTType")

    if isinstance(end_date, NaTType):
        raise ValueError(f"Invalid end_date of type NaTType")

    if end_date < start_date:
        raise ValueError(f"Invalid range: end {end_date} is before start {start_date}")

    # Need at least 2 distinct nanosecond ticks so each half is non-empty.
    # Work in integer nanoseconds so we never hit pandas' Timestamp|NaTType
    # arithmetic stubs (Timestamp ± Timedelta is typed as possibly NaT).
    start_ns = int(start_date.value)
    end_ns = int(end_date.value)
    duration_ns = end_ns - start_ns
    if duration_ns < 1:
        raise ValueError(
            f"Range too short to split without overlap: {start_date} to {end_date}"
        )

    # right_start is the first tick of the right half (ceiling of midpoint).
    mid_offset_ns = (duration_ns + 1) // 2
    right_start_ns = start_ns + mid_offset_ns
    left_end_ns = right_start_ns - 1
    tz = start_date.tz

    right_start = pd.Timestamp(right_start_ns, unit="ns", tz=tz)
    left_end = pd.Timestamp(left_end_ns, unit="ns", tz=tz)

    if left_end < start_date or right_start > end_date:
        raise ValueError(
            f"Range too short to split without overlap: {start_date} to {end_date}"
        )

    return start_date, left_end, right_start, end_date


def get_monthly_utc_date_range_array_from_(
    start_date: pd.Timestamp, end_date: pd.Timestamp
) -> list[dict]:
    """Split a date range into one entry per calendar month.

    The first entry keeps the exact start_date and the last keeps the exact
    end_date; every month in between runs midnight to end-of-day.

    One exception: when start_date is the last day of its month, that day is
    merged into the next month's entry instead of getting one of its own. The
    range is still covered in full, the caller just gets a wider window.

    :param start_date: start of the range; a naive one is read as UTC
    :param end_date: end of the range; a naive one is read as UTC
    :return: list of {"start_date": Timestamp, "end_date": Timestamp}, UTC-aware
    :raises ValueError: if start_date is after end_date

    Example:
        get_monthly_utc_date_range_array_from_("2024-01-15", "2024-03-10")
        -> 3 entries starting 2024-01-15, 2024-02-01 and 2024-03-01, the last
           one ending at the requested 2024-03-10
    """
    # Check if start_date > end_date
    if start_date > end_date:
        raise ValueError("start_date should not be greater than end_date")

    # Ensure naive timestamps for consistency
    start_date = (
        start_date.tz_convert(pytz.UTC)
        if start_date.tz is not None
        else start_date.tz_localize(pytz.UTC)
    )
    end_date = (
        end_date.tz_convert(pytz.UTC)
        if end_date.tz is not None
        else end_date.tz_localize(pytz.UTC)
    )

    # Handle the case where start_date == end_date
    if start_date == end_date:
        return [{"start_date": start_date, "end_date": end_date}]

    # Generate date range, excluding end_date
    date_range = pd.date_range(
        start=start_date, end=end_date, freq="D", inclusive="left"
    )
    if not date_range.is_monotonic_increasing:
        raise ValueError("Generated date range is not monotonic")

    # Create DataFrame and group by year and month
    df = pd.DataFrame({"date": date_range}).sort_values(by="date")

    # Initialize result
    result = []

    # Iterate over months from start_date to end_date
    start = None
    for d in df["date"]:
        if start is None:
            # This the first start day
            start = d
        elif d.is_month_end:
            v = pd.Timestamp(
                year=d.year,
                month=d.month,
                day=d.day,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
                nanosecond=999,
                tz=pytz.UTC,
            )
            result.append(
                {
                    "start_date": start,
                    # Must set to end of time of that day
                    "end_date": v,
                }
            )
            # The next start time will be 1 nanosecond more than the end_date
            start = (v + pd.offsets.MonthBegin(1)).normalize()

    # Edge case where you have start but no end
    if start < end_date:
        result.append(
            {
                "start_date": start,
                # This one needs to follow the one from the incoming request
                "end_date": end_date,
            }
        )
    return result


def resolve_non_specified_dates(start_date: str, end_date: str) -> Tuple[str, str]:
    """Replace the "non-specified" markers with the default open bounds.

    The end default carries full nanosecond precision. A day-only end would
    become the end of that day only if end_of_specified_precision happened to
    run downstream; spelling it out here means the value is already the instant
    it claims to be.

    :param start_date: raw start bound, or NON_SPECIFIED
    :param end_date: raw end bound, or NON_SPECIFIED
    :return: (start, end) strings - the epoch and the end of today (UTC) for an
        open bound. Any other value is returned untouched and still a string;
        to_utc_bounds is what parses it later.

    Example:
        resolve_non_specified_dates("non-specified", "non-specified")
        -> ("1970-01-01T00:00:00Z", "<today>T23:59:59.999999999+00:00")
        resolve_non_specified_dates("2024-01-15", "2024-01-16")
        -> ("2024-01-15", "2024-01-16")   (no marker, so nothing to replace)
    """
    if start_date == NON_SPECIFIED:
        start_date = MIN_DATE
    if end_date == NON_SPECIFIED:
        # now(tz="UTC") rather than today(), which reads the host clock and so
        # returns the wrong calendar day on any machine not pinned to UTC.
        end_date = end_of_day_nano(pd.Timestamp.now(tz="UTC")).isoformat()
    return start_date, end_date


def has_explicit_time(date_string: str) -> bool:
    """Tell whether a date string carries a clock time, not just a calendar date.

    :param date_string: the raw string to inspect
    :return: True when an "HH:MM" part is present

    Example:
        has_explicit_time("2024-01-15") -> False
        has_explicit_time("2024-01-15T10:00:00Z") -> True
    """
    return bool(re.search(r"[T ]\d{2}:\d{2}", date_string))


def end_of_specified_precision(date_string: str, ts: pd.Timestamp) -> pd.Timestamp:
    """Widen an inclusive end bound to the last nanosecond of the precision given.

    A bare date means that whole day, a whole second means that second. Only
    those two precisions are recognised: a minute-only bound like
    "2024-01-15T10:00" is read as second 00 and widens to 10:00:00.999999999,
    not to the end of the minute. The portal always sends seconds.

    The portal builds its end as 23:59:59.999 but formats it with
    "YYYY-MM-DDTHH:mm:ss[Z]", dropping the milliseconds; taking that at face
    value would leave the last second of every day outside both this range and
    the next, so nothing could ever download it.

    :param date_string: the raw string the bound was given as
    :param ts: that same string, already parsed
    :return: `ts` moved to the end of the unit `date_string` specified

    Example:
        end_of_specified_precision("2024-01-15", ts)
        -> "2024-01-15 23:59:59.999999999+00:00"   (a date means the whole day)
        end_of_specified_precision("2024-01-15T10:00:00Z", ts)
        -> "2024-01-15 10:00:00.999999999+00:00"   (a second means that second)
    """
    if not has_explicit_time(date_string):
        return end_of_day_nano(ts)

    fraction = re.search(r"[T ]\d{2}:\d{2}:\d{2}\.(\d+)", date_string)
    digits = len(fraction.group(1)) if fraction else 0
    if digits >= 9:
        return ts
    # fill the digits the caller left unspecified with 9s
    return ts + pd.Timedelta(nanoseconds=10 ** (9 - digits) - 1)


# The legacy bound format the portal used to send, e.g. "02-2024".
MONTH_ONLY_PATTERN = re.compile(r"^(0[1-9]|1[0-2])-\d{4}$")


def _month_to_iso(date_string: str, *, is_end: bool) -> str:
    """Rewrite a legacy "MM-YYYY" bound as the ISO date it means.

    The portal sends ISO now. Converting here keeps the one place that still
    understands the old format at the entry, so nothing downstream has to.

    :param date_string: raw bound; anything not "MM-YYYY" is returned untouched
    :param is_end: True picks the last day of the month, False the first
    :return: a "YYYY-MM-DD" string

    Example:
        _month_to_iso("02-2024", is_end=False) -> "2024-02-01"
        _month_to_iso("02-2024", is_end=True) -> "2024-02-29"
        _month_to_iso("2024-02-15", is_end=True) -> "2024-02-15"  (not MM-YYYY)
    """
    if not MONTH_ONLY_PATTERN.match(date_string):
        return date_string

    month = parse_date(date_string, format_to_convert="%m-%Y")
    day = get_final_day_of_month_(month) if is_end else get_first_day_of_month(month)
    return day.strftime(YEAR_MONTH_DAY)


def to_utc_bounds(
    start_date_str: str, end_date_str: str
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Turn the request's raw date strings into the UTC bounds the subset uses.

    This is the entry point: everything downstream works in UTC-aware
    pd.Timestamp, never in strings.

    :param start_date_str: raw start bound, ISO-8601 or legacy "MM-YYYY"
    :param end_date_str: raw end bound, same formats
    :return: (start, end) UTC timestamps; the end is widened to the last
        nanosecond of the precision it was given, so a plain date still means
        that whole day

    Example:
        to_utc_bounds("2024-01-15", "2024-01-15")
        -> ("2024-01-15 00:00:00+00:00", "2024-01-15 23:59:59.999999999+00:00")
    """
    start_date_str = _month_to_iso(start_date_str, is_end=False)
    end_date_str = _month_to_iso(end_date_str, is_end=True)

    return (
        parse_date(start_date_str),
        end_of_specified_precision(end_date_str, parse_date(end_date_str)),
    )


def split_date_range(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    month_count_per_job: int,
) -> dict:
    """Group a date range into the windows one Batch array job hands its children.

    The last window is kept even when it holds fewer months than asked for. If
    the whole range has fewer months than month_count_per_job, everything
    collapses into a single window at key 0.

    :param start_date: start of the range
    :param end_date: end of the range
    :param month_count_per_job: how many calendar months one child job covers
    :return: {child job index: [start string, end string]}, naive UTC strings

    Example:
        split_date_range("2024-01-01", "2024-02-29", month_count_per_job=1)
        -> {0: ["2024-01-01 00:00:00.000000000", "2024-01-31 23:59:59.999999999"],
            1: ["2024-02-01 00:00:00.000000000", "2024-02-29 00:00:00.000000000"]}
           The last window ends at the requested end, not at end of month.
    """
    date_ranges = {}
    index = 0

    months: list[dict] = get_monthly_utc_date_range_array_from_(start_date, end_date)

    def as_param(window: list[dict]) -> list[str]:
        # AWS Batch job parameters are strings, so the bounds cross to the child
        # job as text; naive UTC with nanoseconds is what parse_date reads back.
        return [
            to_naive_utc_string(window[0]["start_date"]),
            to_naive_utc_string(window[-1]["end_date"]),
        ]

    # Special case, if your split is too high and cannot be split we just return the start end date
    if len(months) < month_count_per_job:
        date_ranges[0] = as_param(months)
    else:
        for i in range(0, len(months), month_count_per_job):
            window = months[i : i + month_count_per_job]
            date_ranges[index] = as_param(window)
            if len(window) < month_count_per_job:
                # already stored above; a short window can only be the last one
                break
            index = index + 1

    return date_ranges


def time_it(func):
    """Log how long the wrapped function took to run.

    :param func: function to time; sync and async are both supported
    :return: the wrapped function, returning whatever `func` returns

    Example:
        @time_it
        def add(a, b): ...
        add(1, 2) -> returns 3 and logs "[add] took 0.000002 seconds."
    """

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        end = time.perf_counter()
        log.info(f"[{func.__name__}] took {end - start:.6f} seconds.")
        return result

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        log.info(f"[{func.__name__}] took {end - start:.6f} seconds.")
        return result

    return async_wrapper if iscoroutinefunction(func) else sync_wrapper
