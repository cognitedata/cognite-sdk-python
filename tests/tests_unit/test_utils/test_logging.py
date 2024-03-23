from cognite.client.utils._logging import (
    _configure_logger_for_debug_mode,
    _disable_debug_logging,
    _is_debug_logging_enabled,
)


class TestDebugLoggingState:
    def test_debug_logging_toggle_on_off(self):
        assert not _is_debug_logging_enabled(), "should be disabled by default"

        _configure_logger_for_debug_mode()
        assert _is_debug_logging_enabled(), "should be enabled"

        _disable_debug_logging()
        assert not _is_debug_logging_enabled(), "should be disabled"
