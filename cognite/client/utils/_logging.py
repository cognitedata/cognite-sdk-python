import logging


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
                s_extra_objs.append("\n    - {}: {}".format(attr, value))
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
    logger = logging.getLogger("cognite-sdk")
    logger.setLevel("DEBUG")
    log_handler = logging.StreamHandler()
    formatter = DebugLogFormatter()
    log_handler.setFormatter(formatter)
    logger.handlers = []
    logger.propagate = False
    logger.addHandler(log_handler)
