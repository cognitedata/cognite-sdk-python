from unittest.mock import MagicMock

import httpx # Replaced requests.exceptions
import pytest
import respx
from respx import MockRouter # Using MockRouter alias for HTTPXMock
# import urllib3.exceptions # No longer needed as httpx has its own hierarchy

from cognite.client._http_client import HTTPClient, HTTPClientConfig, _RetryTracker
from cognite.client.exceptions import CogniteConnectionError, CogniteConnectionRefused, CogniteReadTimeout

DEFAULT_CONFIG = HTTPClientConfig(
    status_codes_to_retry={429},
    backoff_factor=0.5,
    max_backoff_seconds=30,
    max_retries_total=10,
    max_retries_read=5,
    max_retries_connect=4,
    max_retries_status=10,
)


class TestRetryTracker:
    def test_total_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_total = 10
        rt.status = 4
        rt.connect = 4
        rt.read = 4

        assert rt.total == 12
        assert rt.should_retry(200) is False

    def test_status_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_status = 1
        assert rt.should_retry(None) is True
        rt.status = 1
        assert rt.should_retry(None) is False

    def test_read_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_read = 1
        assert rt.should_retry(None) is True
        rt.read = 1
        assert rt.should_retry(None) is False

    def test_connect_retries_exceeded(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_connect = 1
        assert rt.should_retry(None) is True
        rt.connect = 1
        assert rt.should_retry(None) is False

    def test_correct_retry_of_status(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        assert rt.should_retry(500) is False
        rt.config.status_codes_to_retry = {500, 429}
        assert rt.should_retry(500) is True

    def test_get_backoff_time(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        for i in range(1000):
            rt.read += 1
            assert 0 <= rt.get_backoff_time() <= DEFAULT_CONFIG.max_backoff_seconds

    def test_is_auto_retryable(self):
        rt = _RetryTracker(config=DEFAULT_CONFIG)
        rt.config.max_retries_status = 1

        # 409 is not in the list of status codes to retry, but we set is_auto_retryable=True, which should override it
        assert rt.should_retry(409, is_auto_retryable=True) is True
        assert rt.should_retry(409, is_auto_retryable=False) is False


# The function raise_exception_wrapped_as_in_requests_lib is no longer needed
# as we will mock httpx exceptions directly.

class TestHTTPClient:
    @respx.mock
    def test_read_timeout_errors(self, respx_mock: MockRouter):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        
        # Define the mock route using respx_mock
        respx_mock.get("https://example.com/bla").mock(side_effect=httpx.ReadTimeout("timeout"))

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            # No longer passing mock_session
        )

        with pytest.raises(CogniteReadTimeout):
            c.request("GET", "https://example.com/bla", headers={"accept": "application/json"})

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_read
        assert retry_tracker.read == DEFAULT_CONFIG.max_retries_read
        assert retry_tracker.connect == 0
        assert retry_tracker.status == 0

    @pytest.mark.parametrize("exc_type", [ConnectionAbortedError, ConnectionResetError, BrokenPipeError])
    @respx.mock
    def test_connect_errors(self, respx_mock: MockRouter, exc_type):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)

        # Define the mock route using respx_mock
        respx_mock.get("https://example.com/bla").mock(side_effect=httpx.ConnectError(f"{exc_type.__name__} error"))

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            # No longer passing mock_session
        )

        with pytest.raises(CogniteConnectionError):
            c.request("GET", "https://example.com/bla", headers={"accept": "application/json"})

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_connect
        assert retry_tracker.read == 0
        assert retry_tracker.connect == DEFAULT_CONFIG.max_retries_connect
        assert retry_tracker.status == 0

    @respx.mock
    def test_connection_refused_retried(self, respx_mock: MockRouter):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)

        connect_error_with_refused_cause = httpx.ConnectError("connection refused")
        # In Python 3.11+, __cause__ must be an exception instance.
        # For older versions, it might be None or an exception.
        # Let's ensure it's always an instance for consistency.
        connect_error_with_refused_cause.__cause__ = ConnectionRefusedError("connection refused error detail")
        
        respx_mock.get("https://example.com/bla").mock(side_effect=connect_error_with_refused_cause)

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            # No longer passing mock_session
        )

        with pytest.raises(CogniteConnectionRefused):
            c.request("GET", "https://example.com/bla", headers={"accept": "application/json"})

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_connect

    @respx.mock
    def test_status_errors(self, respx_mock: MockRouter):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)

        respx_mock.get("https://example.com/bla").respond(status_code=429)

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            # No longer passing mock_session
        )

        res = c.request("GET", "https://example.com/bla", headers={"accept": "application/json"})
        assert res.status_code == 429

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_status
        assert retry_tracker.read == 0
        assert retry_tracker.connect == 0
        assert retry_tracker.status == DEFAULT_CONFIG.max_retries_status

# The TestGetUnderlyingException class is removed as _any_exception_in_context_isinstance was removed.
