import json
import logging
from datetime import datetime, timezone

from data_access_service.config.config import Config, EnvType

JSON_LOG_PROFILES = (EnvType.EDGE, EnvType.STAGING, EnvType.PRODUCTION)

TEXT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
TEXT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exception"] = record.exc_text

        # default=str so a stray non-string value cannot kill the log line.
        return json.dumps(payload, default=str)


def use_json_logs(profile: EnvType = None) -> bool:
    """True when the active profile should log JSON rather than text."""
    return Config.resolve_profile(profile) in JSON_LOG_PROFILES


def build_formatter(
    fmt: str = None, datefmt: str = None, style: str = "%"
) -> logging.Formatter:
    """Formatter for the active profile. fmt/datefmt/style only apply to the
    text profiles; ignored for JSON."""
    if use_json_logs():
        return JsonLogFormatter()
    return logging.Formatter(fmt or TEXT_LOG_FORMAT, datefmt, style)
