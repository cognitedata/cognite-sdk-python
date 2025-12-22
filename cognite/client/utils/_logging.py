from __future__ import annotations

import logging

_COGNITE_LOGGER_NAME = "cognite.client"
_DEBUG_HANDLER_NAME = "cognite_client_debug_handler"


class DebugLogFormatter(logging.Formatter):
    # fmt: off
    RESERVED_ATTRS = frozenset((
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName"
    ))
    # fmt: on

    def __init__(self) -> None:
        fmt = "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(threadName)s. %(message)s (%(filename)s:%(lineno)s)"
        datefmt = "%Y-%m-%d %H:%M:%S"
        super().__init__(fmt, datefmt)

    def format(self, record: logging.LogRecord) -> str:
        # Let standard library handle the main message + exception + stack traces:
        log_str = super().format(record)

        # Append custom 'extra' fields
        extra_output = []
        for attr, value in record.__dict__.items():
            if attr not in self.RESERVED_ATTRS:
                extra_output.append(f"\n    - {attr}: {value}")

        if extra_output:
            log_str += "".join(extra_output)

        return log_str


def _configure_logger_for_debug_mode() -> None:
    logger = logging.getLogger(_COGNITE_LOGGER_NAME)
    logger.setLevel(logging.DEBUG)

    if _is_debug_logging_enabled():
        return

    log_handler = logging.StreamHandler()
    log_handler.setFormatter(DebugLogFormatter())

    # Tag the handler so we can find it later
    log_handler.set_name(_DEBUG_HANDLER_NAME)

    logger.propagate = False
    logger.addHandler(log_handler)


def _disable_debug_logging() -> None:
    logger = logging.getLogger(_COGNITE_LOGGER_NAME)
    logger.setLevel(logging.INFO)

    # Find and remove ONLY our specific debug handler
    handlers_to_remove = [h for h in logger.handlers if h.get_name() == _DEBUG_HANDLER_NAME]

    for handler in handlers_to_remove:
        logger.removeHandler(handler)

    # Restore propagation so logs flow to the root logger again
    logger.propagate = True


def _is_debug_logging_enabled() -> bool:
    logger = logging.getLogger(_COGNITE_LOGGER_NAME)
    # Check if level is DEBUG and if our specific handler is present
    has_debug_handler = any(h.get_name() == _DEBUG_HANDLER_NAME for h in logger.handlers)
    return logger.isEnabledFor(logging.DEBUG) and has_debug_handler
