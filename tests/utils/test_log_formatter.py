import json
import logging
import logging.config
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from data_access_service import init_log
from data_access_service.config.config import EnvType
from data_access_service.utils.log_formatter import (
    TEXT_LOG_DATE_FORMAT,
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
    assert payload["logger"] == "test.logger"
    assert payload["message"] == "hello"
    assert "timestamp" in payload
    assert "exception" not in payload


def test_json_formatter_multiline_message_stays_one_json_line():
    output = JsonLogFormatter().format(_make_record(msg="line1\nline2"))

    assert "\n" not in output
    assert json.loads(output)["message"] == "line1\nline2"


def test_json_formatter_captures_exception_from_exc_info():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = _make_record(
            msg="failed", level=logging.ERROR, exc_info=sys.exc_info()
        )

    output = JsonLogFormatter().format(record)
    payload = json.loads(output)

    assert "\n" not in output
    assert "ValueError: boom" in payload["exception"]


def test_json_formatter_uses_cached_exc_text_when_no_exc_info():
    record = _make_record(msg="already formatted")
    record.exc_text = "Traceback (most recent call last):\ncached"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["exception"] == "Traceback (most recent call last):\ncached"


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
        assert {"timestamp", "level", "logger", "message"} <= payload.keys()


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
