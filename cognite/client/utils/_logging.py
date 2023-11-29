from __future__ import annotations

import logging

_COGNITE_LOGGER_NAME = "cognite.client"


class DebugLogFormatter(logging.Formatter):
    RESERVED_ATTRS = (
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    )

    def __init__(self) -> None:
        fmt = "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(threadName)s. %(message)s (%(filename)s:%(lineno)s)"
        datefmt = "%Y-%m-%d %H:%M:%S"
        super().__init__(fmt, datefmt)

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        s_extra_objs = []
        for attr, value in record.__dict__.items():
            if attr not in self.RESERVED_ATTRS:
                s_extra_objs.append(f"\n    - {attr}: {value}")
        for s_extra in s_extra_objs:
            s += s_extra
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        return s


def _configure_logger_for_debug_mode() -> None:
    logger = logging.getLogger(_COGNITE_LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    formatter = DebugLogFormatter()
    log_handler.setFormatter(formatter)
    logger.handlers = []
    logger.propagate = False
    logger.addHandler(log_handler)


def _disable_debug_logging() -> None:
    logger = logging.getLogger(_COGNITE_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers = []


def _is_debug_logging_enabled() -> bool:
    logger = logging.getLogger(_COGNITE_LOGGER_NAME)
    return logger.isEnabledFor(logging.DEBUG) and logger.hasHandlers()
