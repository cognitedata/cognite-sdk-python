from unittest.mock import MagicMock

import httpx # Replaced requests.exceptions
import pytest
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
    def test_read_timeout_errors(self):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        mock_session = MagicMock(spec=httpx.Client)
        mock_session.request.side_effect = httpx.ReadTimeout("timeout")

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            session=mock_session,
        )

        with pytest.raises(CogniteReadTimeout):
            c.request("GET", "bla", headers={"accept": "application/json"})

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_read
        assert retry_tracker.read == DEFAULT_CONFIG.max_retries_read
        assert retry_tracker.connect == 0
        assert retry_tracker.status == 0

    @pytest.mark.parametrize("exc_type", [ConnectionAbortedError, ConnectionResetError, BrokenPipeError])
    def test_connect_errors(self, exc_type):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        mock_session = MagicMock(spec=httpx.Client)
        # httpx.ConnectError is a general connection error. Specific ones like ConnectionAbortedError
        # might be wrapped by httpcore or httpx itself. For testing, directly raising ConnectError is sufficient
        # or using the specific errors if httpx guarantees they are raised directly.
        # Let's assume ConnectError for simplicity, or the specific one if appropriate.
        mock_session.request.side_effect = httpx.ConnectError(f"{exc_type.__name__} error")


        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            session=mock_session,
        )

        with pytest.raises(CogniteConnectionError):
            c.request("GET", "bla", headers={"accept": "application/json"})

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_connect
        assert retry_tracker.read == 0
        assert retry_tracker.connect == DEFAULT_CONFIG.max_retries_connect
        assert retry_tracker.status == 0

    def test_connection_refused_retried(self):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        mock_session = MagicMock(spec=httpx.Client)
        # _http_client.py specifically checks for e.__cause__ being ConnectionRefusedError
        # when handling httpx.ConnectError. So, we construct an httpx.ConnectError with ConnectionRefusedError as its cause.
        connect_error_with_refused_cause = httpx.ConnectError("connection refused")
        connect_error_with_refused_cause.__cause__ = ConnectionRefusedError()
        mock_session.request.side_effect = connect_error_with_refused_cause

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            session=mock_session,
        )

        with pytest.raises(CogniteConnectionRefused):
            c.request("GET", "bla", headers={"accept": "application/json"})

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_connect

    def test_status_errors(self):
        cnf = DEFAULT_CONFIG
        cnf.max_backoff_seconds = 0
        retry_tracker = _RetryTracker(cnf)
        mock_session = MagicMock(spec=httpx.Client)
        # httpx.Response needs a request object.
        dummy_request = httpx.Request("GET", "https://example.com/bla")
        mock_session.request.return_value = httpx.Response(429, request=dummy_request)

        c = HTTPClient(
            config=cnf,
            refresh_auth_header=lambda headers: None,
            retry_tracker_factory=lambda _: retry_tracker,
            session=mock_session,
        )

        res = c.request("GET", "bla", headers={"accept": "application/json"})
        assert res.status_code == 429

        assert retry_tracker.total == DEFAULT_CONFIG.max_retries_status
        assert retry_tracker.read == 0
        assert retry_tracker.connect == 0
        assert retry_tracker.status == DEFAULT_CONFIG.max_retries_status

# The TestGetUnderlyingException class is removed as _any_exception_in_context_isinstance was removed.
