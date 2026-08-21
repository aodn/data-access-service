"""The app must treat UTC as local time, whatever timezone the host runs in.

AWS hosts are not guaranteed to be UTC. Without a pinned process timezone,
naive clock calls such as datetime.now() and pd.Timestamp.today() resolve
"today" against the host clock, which is up to a day ahead in Australia.
"""

import os
import subprocess
import sys
import time

import pytest

from data_access_service import set_default_timezone_utc


@pytest.fixture(autouse=True)
def _restore_utc():
    """Put the process back in the state package import leaves it in."""
    yield
    os.environ["TZ"] = "UTC"
    time.tzset()


def _run_on_a_sydney_host(snippet: str) -> str:
    """Run snippet in a fresh interpreter whose host timezone is not UTC.

    A subprocess is the only honest check: this process already imported
    data_access_service, so its timezone is settled before any test runs.
    """
    env = {**os.environ, "TZ": "Australia/Sydney", "PROFILE": "testing"}
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        env=env,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_sets_utc_when_tz_is_not_configured(monkeypatch):
    monkeypatch.delenv("TZ", raising=False)
    time.tzset()

    set_default_timezone_utc()

    assert time.strftime("%Z") == "UTC"


def test_keeps_an_explicitly_configured_tz(monkeypatch):
    """setdefault, not overwrite - a deliberate TZ must still win."""
    monkeypatch.setenv("TZ", "Australia/Sydney")
    time.tzset()

    set_default_timezone_utc()

    assert time.strftime("%Z") in ("AEST", "AEDT")


def test_package_import_applies_utc_on_a_non_utc_host():
    """Importing data_access_service is enough - no entry point has to opt in."""
    assert (
        _run_on_a_sydney_host(
            "import data_access_service, time; print(time.strftime('%Z'))"
        )
        == "UTC"
    )


def test_naive_clock_calls_report_utc_on_a_non_utc_host():
    """The two calls that resolve an open-ended end date - issue 9010."""
    snippet = (
        "import data_access_service\n"
        "from datetime import datetime, timezone\n"
        "import pandas as pd\n"
        "print(datetime.now().strftime('%Y-%m-%d'))\n"
        "print(pd.Timestamp.today().strftime('%Y-%m-%d'))\n"
        "print(datetime.now(timezone.utc).strftime('%Y-%m-%d'))\n"
    )
    naive_now, pandas_today, utc_today = _run_on_a_sydney_host(snippet).splitlines()

    assert naive_now == utc_today
    assert pandas_today == utc_today
