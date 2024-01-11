import datetime
import logging
from pathlib import Path
from sys import stdout
import pytz

DEFAULT_LOG_LEVEL = logging.INFO


class SuppressFormatter:
    """
    - Suppresses formatter of the logger if called as below
    with SuppressFormatter():
        logger.info('message without formatter')
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.original_formatters = []

    def __enter__(self):
        for handler in self.logger.handlers:
            _original_formatter = handler.formatter
            self.original_formatters.append(_original_formatter)
            handler.setFormatter(logging.Formatter("%(message)s"))

    def __exit__(self, *args):
        for original_formatter, handler in zip(
            self.original_formatters, self.logger.handlers
        ):
            handler.setFormatter(original_formatter)


class CustomFormatter(logging.Formatter):
    def format(self, record):
        caller_filename = Path(record.pathname).resolve()
        record.relative_filename = caller_filename
        record.now = (
            datetime.datetime.now()
            .astimezone(pytz.timezone("Asia/Kolkata"))
            .isoformat()
        )
        log_format = (
            "{name}: {now} - {levelname} - [{relative_filename}] [{lineno}] {message}"
        )
        formatter = logging.Formatter(log_format, style="{")

        return formatter.format(record)


def init_logger(logger_name: str) -> logging.Logger:
    """
    - initializes a logger with the input logger name
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(DEFAULT_LOG_LEVEL)
    fmt = CustomFormatter()

    sh = logging.StreamHandler(stdout)
    sh.setFormatter(fmt)
    sh.setLevel(DEFAULT_LOG_LEVEL)
    logger.addHandler(sh)


def add_handler(
    logger: logging.Logger, handler: logging.Handler, level: int = DEFAULT_LOG_LEVEL
) -> None:
    """
    - adds the input handler to the input logger
    - expects the input logger to contain atleast one handler
    """
    formatter = logger.handlers[0].formatter
    handler.setLevel(level)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
