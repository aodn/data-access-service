import json
import os
import subprocess
import logging
import logging.config
import re
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from data_access_service import init_log
from data_access_service.config.config import EnvType
from data_access_service.utils.log_context import ContextFilter, bind_log_context
from data_access_service.utils.log_formatter import (
    TEXT_LOG_DATE_FORMAT,
    SERVICE_NAME,
    JsonLogFormatter,
    build_formatter,
    use_json_logs,
)

LOG_CONFIG_PATH = Path(__file__).resolve().parents[2] / "log_config.yaml"
CONFIGURED_LOGGERS = [
    "uvicorn.error",
    "uvicorn.access",
    "botocore",
    "s3fs",
    "aiobotocore",
]


def _make_record(msg="hello", level=logging.INFO, exc_info=None, name="test.logger"):
    return logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=None,
        exc_info=exc_info,
    )


@pytest.fixture
def clean_logging():
    """Snapshot root + the loggers log_config.yaml touches, restore after the test."""
    root = logging.getLogger()
    root_snapshot = (list(root.handlers), root.level)
    logger_snapshots = {
        name: (
            list(logging.getLogger(name).handlers),
            logging.getLogger(name).level,
            logging.getLogger(name).propagate,
        )
        for name in CONFIGURED_LOGGERS
    }
    yield
    root.handlers, root.level = root_snapshot
    for name, (handlers, level, propagate) in logger_snapshots.items():
        logger = logging.getLogger(name)
        logger.handlers = handlers
        logger.level = level
        logger.propagate = propagate


# -- use_json_logs -----------------------------------------------------------


@pytest.mark.parametrize(
    "profile,expected",
    [
        (EnvType.EDGE, True),
        (EnvType.STAGING, True),
        (EnvType.PRODUCTION, True),
        (EnvType.DEV, False),
        (EnvType.TESTING, False),
    ],
)
def test_use_json_logs_explicit_profiles(profile, expected):
    assert use_json_logs(profile) is expected


def test_use_json_logs_reads_profile_env_var(monkeypatch):
    # PYTEST_CURRENT_TEST would otherwise force the "testing" profile.
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    monkeypatch.setenv("PROFILE", "edge")
    assert use_json_logs() is True

    monkeypatch.setenv("PROFILE", "dev")
    assert use_json_logs() is False


def test_use_json_logs_defaults_to_dev_when_unset(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("PROFILE", raising=False)

    assert use_json_logs() is False


# -- build_formatter -----------------------------------------------------------


@pytest.mark.parametrize("profile", [EnvType.EDGE, EnvType.STAGING, EnvType.PRODUCTION])
def test_build_formatter_json_profiles(profile, monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile.value)

    assert isinstance(build_formatter(), JsonLogFormatter)


@pytest.mark.parametrize("profile", [EnvType.DEV, EnvType.TESTING])
def test_build_formatter_text_profiles(profile, monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile.value)

    formatter = build_formatter()
    assert isinstance(formatter, logging.Formatter)
    assert not isinstance(formatter, JsonLogFormatter)


def test_build_formatter_text_matches_yaml_format_with_milliseconds(monkeypatch):
    """No explicit datefmt (as in log_config.yaml) -> asctime keeps milliseconds."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", "dev")

    formatter = build_formatter(fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    output = formatter.format(_make_record(msg="line1\nline2", level=logging.WARNING))

    assert re.match(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} \[WARNING\] test\.logger: line1\nline2$",
        output,
    )


def test_build_formatter_text_matches_batch_format_seconds_only(monkeypatch):
    """Batch's init_log passes TEXT_LOG_DATE_FORMAT explicitly -> no milliseconds."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", "testing")

    formatter = build_formatter(datefmt=TEXT_LOG_DATE_FORMAT)
    output = formatter.format(_make_record(msg="batch line", level=logging.INFO))

    assert re.match(
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - test\.logger - INFO - batch line$",
        output,
    )


# -- JsonLogFormatter -----------------------------------------------------------


def test_json_formatter_produces_valid_json_with_required_fields():
    output = JsonLogFormatter().format(_make_record(msg="hello", level=logging.INFO))
    payload = json.loads(output)

    assert payload["level"] == "INFO"
    assert payload["loggerName"] == "test.logger"
    assert payload["message"] == "hello"
    assert payload["service"] == SERVICE_NAME == "data-access-service"
    assert payload["threadId"] == threading.get_ident()
    assert "thrown" not in payload
    # Pre-alignment field names must be gone (CloudWatch queries match literally).
    assert not {"timestamp", "logger", "exception"} & payload.keys()


def test_json_formatter_instant_is_utc_millis_with_z_suffix():
    record = _make_record()
    record.created = 1_780_000_000.5249  # fixed point in time

    instant = json.loads(JsonLogFormatter().format(record))["instant"]

    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", instant)
    assert instant == "2026-05-28T20:26:40.524Z"


def test_json_formatter_interpolates_args():
    record = _make_record(msg="value=%s count=%d")
    record.args = ("x", 3)

    assert json.loads(JsonLogFormatter().format(record))["message"] == "value=x count=3"


def test_json_formatter_multiline_message_stays_one_json_line():
    output = JsonLogFormatter().format(_make_record(msg="line1\nline2"))

    assert "\n" not in output
    assert json.loads(output)["message"] == "line1\nline2"


def test_json_formatter_captures_exception_as_thrown():
    try:
        raise ValueError("boom")
    except ValueError:
        record = _make_record(
            msg="failed", level=logging.ERROR, exc_info=sys.exc_info()
        )

    output = JsonLogFormatter().format(record)
    payload = json.loads(output)

    assert "\n" not in output
    assert payload["thrown"]["name"] == "ValueError"
    assert payload["thrown"]["message"] == "boom"
    assert payload["thrown"]["extendedStackTrace"].startswith("Traceback")
    assert "ValueError: boom" in payload["thrown"]["extendedStackTrace"]
    assert "exc_info" not in payload


def test_json_formatter_uses_cached_exc_text_when_no_exc_info():
    record = _make_record(msg="already formatted")
    record.exc_text = "Traceback (most recent call last):\ncached"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["thrown"] == {
        "extendedStackTrace": "Traceback (most recent call last):\ncached"
    }


def test_json_formatter_flattens_extra_fields_to_top_level():
    logger = logging.getLogger("das.test.extra")
    records = []
    handler = logging.Handler()
    handler.emit = records.append
    logger.addHandler(handler)
    try:
        logger.warning("with extra", extra={"dataset": "abc", "rows": 42})
    finally:
        logger.removeHandler(handler)

    payload = json.loads(JsonLogFormatter().format(records[0]))

    assert payload["dataset"] == "abc"
    assert payload["rows"] == 42
    # No standard LogRecord internals leak through.
    assert not {"args", "msg", "pathname", "lineno", "levelno", "thread"} & (
        payload.keys()
    )


def test_json_formatter_extra_cannot_override_core_fields():
    record = _make_record(msg="core wins")
    record.service = "spoofed"
    record.instant = "spoofed"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["service"] == "data-access-service"
    assert payload["instant"] != "spoofed"


def test_json_formatter_drops_uvicorn_color_message():
    record = _make_record(msg="Started server process [%d]")
    record.args = (1,)
    record.color_message = "Started server process [\x1b[36m%d\x1b[0m]"

    payload = json.loads(JsonLogFormatter().format(record))

    assert "color_message" not in payload
    assert payload["message"] == "Started server process [1]"


def test_json_formatter_non_serializable_extra_does_not_raise():
    class Opaque:
        def __str__(self):
            return "<opaque>"

    record = _make_record(msg="odd value")
    record.thing = Opaque()
    record.raw = b"bytes"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["thing"] == "<opaque>"
    assert payload["raw"] == "b'bytes'"


# -- log_config.yaml through dictConfig -----------------------------------------------------------


@pytest.mark.parametrize("profile", ["edge", "staging", "prod"])
def test_yaml_config_emits_one_json_line_per_record_no_duplicates(
    profile, monkeypatch, capsys, clean_logging
):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile)

    with open(LOG_CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)

    logging.getLogger().warning("root warning")
    logging.getLogger("uvicorn.error").info("uvicorn error line")
    logging.getLogger("uvicorn.access").info("uvicorn access line")

    captured = capsys.readouterr()
    out_lines = [line for line in captured.out.splitlines() if line.strip()]
    err_lines = [line for line in captured.err.splitlines() if line.strip()]

    # uvicorn.access -> stdout; root + uvicorn.error (propagate: no) -> stderr, once each.
    assert len(out_lines) == 1
    assert len(err_lines) == 2

    for line in out_lines + err_lines:
        payload = json.loads(line)
        assert {"instant", "level", "loggerName", "message", "service"} <= (
            payload.keys()
        )


# -- init_log -----------------------------------------------------------


@pytest.mark.parametrize("profile", ["edge", "staging", "prod"])
def test_init_log_emits_json_with_no_existing_root_handlers(
    profile, monkeypatch, capsys, clean_logging
):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile)
    logging.getLogger().handlers = []

    init_log(SimpleNamespace(LOGLEVEL=logging.DEBUG))
    logging.getLogger("das.test").warning("no prior handler")

    lines = [line for line in capsys.readouterr().err.splitlines() if line.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["message"] == "no prior handler"


@pytest.mark.parametrize("profile", ["edge", "staging", "prod"])
def test_init_log_emits_json_with_existing_root_handler(
    profile, monkeypatch, capsys, clean_logging
):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile)

    existing_handler = logging.StreamHandler()
    existing_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().handlers = [existing_handler]

    init_log(SimpleNamespace(LOGLEVEL=logging.DEBUG))
    logging.getLogger("das.test").error("existing handler path")

    lines = [line for line in capsys.readouterr().err.splitlines() if line.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["message"] == "existing handler path"


@pytest.mark.parametrize("profile", ["dev", "testing"])
def test_init_log_keeps_text_output_on_text_profiles(
    profile, monkeypatch, capsys, clean_logging
):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile)
    logging.getLogger().handlers = []

    init_log(SimpleNamespace(LOGLEVEL=logging.DEBUG))
    logging.getLogger("das.test").warning("text profile line")

    lines = [line for line in capsys.readouterr().err.splitlines() if line.strip()]
    assert len(lines) == 1
    with pytest.raises(json.JSONDecodeError):
        json.loads(lines[0])
    assert "text profile line" in lines[0]


# -- request/job context wiring -----------------------------------------------------------


@pytest.mark.parametrize("profile", ["edge", "staging", "prod"])
def test_yaml_handlers_carry_bound_request_id(
    profile, monkeypatch, capsys, clean_logging
):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile)

    with open(LOG_CONFIG_PATH) as f:
        logging.config.dictConfig(yaml.safe_load(f))

    with bind_log_context(request_id="req-yaml"):
        logging.getLogger("uvicorn.access").info("GET /health 200")
        logging.getLogger("das.module").warning("from root")

    captured = capsys.readouterr()
    access = json.loads(captured.out.strip())
    root = json.loads(captured.err.strip())
    assert access["request_id"] == root["request_id"] == "req-yaml"


@pytest.mark.parametrize("profile", ["edge", "dev"])
def test_init_log_installs_context_filter_once(
    profile, monkeypatch, capsys, clean_logging
):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("PROFILE", profile)
    logging.getLogger().handlers = []

    init_log(SimpleNamespace(LOGLEVEL=logging.DEBUG))
    init_log(SimpleNamespace(LOGLEVEL=logging.DEBUG))

    (handler,) = logging.getLogger().handlers
    assert sum(isinstance(f, ContextFilter) for f in handler.filters) == 1


BATCH_SNIPPET = """
import logging, runpy, sys
from unittest.mock import MagicMock
import boto3
from data_access_service.batch.sites_parquet import refresher

def fake_refresh():
    logging.getLogger(refresher.__name__).warning("refreshing from batch submodule")

refresher.refresh_sites_parquet_snapshots = fake_refresh
batch = MagicMock()
batch.describe_jobs.return_value = {
    "jobs": [{"parameters": {"type": "refresh-sites-parquet"}}]
}
boto3.client = lambda *a, **k: batch
runpy.run_path("entry_point.py", run_name="__main__")
"""


@pytest.mark.parametrize("profile", ["edge", "staging", "prod"])
def test_batch_entry_point_logs_json_with_job_id(profile):
    """Runs the real entry_point.py (Batch describe_jobs stubbed) in a clean
    interpreter: every line is JSON and carries job_id, including the line
    logged from a batch/ submodule."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("PYTEST_CURRENT_TEST", "PROFILE")
    }
    env.update(PROFILE=profile, AWS_BATCH_JOB_ID="job-1234", AWS_DEFAULT_REGION="x")
    result = subprocess.run(
        [sys.executable, "-c", BATCH_SNIPPET],
        capture_output=True,
        text=True,
        env=env,
        cwd=LOG_CONFIG_PATH.parent,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr

    payloads = []
    for line in result.stderr.splitlines():
        if line.startswith("{"):
            payloads.append(json.loads(line))
    by_message = {p["message"]: p for p in payloads}

    submodule = by_message["refreshing from batch submodule"]
    assert submodule["loggerName"] == (
        "data_access_service.batch.sites_parquet.refresher"
    )
    assert submodule["job_id"] == "job-1234"
    assert by_message["Job started"]["job_id"] == "job-1234"
