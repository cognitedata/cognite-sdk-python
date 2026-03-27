from __future__ import annotations

import httpx
import pytest

from cognite.client._http_client import AsyncHTTPClientWithRetryConfig, RetryTracker


@pytest.fixture
def default_config() -> AsyncHTTPClientWithRetryConfig:
    return AsyncHTTPClientWithRetryConfig(
        status_codes_to_retry={429},
        backoff_factor=0.5,
        max_backoff_seconds=30,
        max_retries_total=10,
        max_retries_read=5,
        max_retries_connect=4,
        max_retries_status=10,
    )


URL = "https://example.com"


def make_http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", URL)
    response = httpx.Response(status_code=status_code, request=request)
    return httpx.HTTPStatusError(f"Error {status_code}", request=request, response=response)


@pytest.fixture
def timeout_error() -> httpx.TimeoutException:
    return httpx.ReadTimeout("read timeout")


@pytest.fixture
def connect_error() -> httpx.ConnectError:
    return httpx.ConnectError("connection error")


@pytest.fixture
def status_error_429() -> httpx.HTTPStatusError:
    return make_http_status_error(429)


class TestRetryTracker:
    def test_total_retries_exceeded(
        self, default_config: AsyncHTTPClientWithRetryConfig, status_error_429: httpx.HTTPStatusError
    ) -> None:
        default_config._max_retries_total = 10
        rt = RetryTracker(URL, default_config)
        rt.status = 4
        rt.connect = 4
        rt.read = 4

        assert rt.total == 12
        assert rt.should_retry_total is False
        assert rt.should_retry_status_code(status_error_429) is False

    def test_status_retries_exceeded(
        self, default_config: AsyncHTTPClientWithRetryConfig, status_error_429: httpx.HTTPStatusError
    ) -> None:
        default_config._max_retries_status = 1
        rt = RetryTracker(URL, default_config)
        assert rt.should_retry_total is True
        assert rt.should_retry_status_code(status_error_429) is True
        assert rt.should_retry_status_code(status_error_429) is False
        assert rt.last_failed_reason is not None
        assert "429" in rt.last_failed_reason

    def test_read_retries_exceeded(
        self, default_config: AsyncHTTPClientWithRetryConfig, timeout_error: httpx.TimeoutException
    ) -> None:
        default_config._max_retries_read = 1
        rt = RetryTracker(URL, default_config)
        assert rt.should_retry_timeout(timeout_error) is True
        assert rt.should_retry_timeout(timeout_error) is False
        assert rt.last_failed_reason is not None
        assert "ReadTimeout" in rt.last_failed_reason

    def test_connect_retries_exceeded(
        self, default_config: AsyncHTTPClientWithRetryConfig, connect_error: httpx.ConnectError
    ) -> None:
        default_config._max_retries_connect = 1
        rt = RetryTracker(URL, default_config)
        assert rt.should_retry_connect_error(connect_error) is True
        assert rt.should_retry_connect_error(connect_error) is False
        assert rt.last_failed_reason is not None
        assert "ConnectError" in rt.last_failed_reason

    def test_correct_retry_of_status(self, default_config: AsyncHTTPClientWithRetryConfig) -> None:
        rt = RetryTracker(URL, default_config)
        assert rt.should_retry_status_code(make_http_status_error(500)) is False
        rt.config.status_codes_to_retry.add(500)
        assert rt.should_retry_status_code(make_http_status_error(500)) is True

    def test_get_backoff_time(self, default_config: AsyncHTTPClientWithRetryConfig) -> None:
        rt = RetryTracker(URL, default_config)
        for i in range(10):
            rt.read += 1
            assert 0 <= rt.get_backoff_time() <= default_config.max_backoff_seconds

    def test_is_auto_retryable(self, default_config: AsyncHTTPClientWithRetryConfig) -> None:
        default_config._max_retries_status = 1
        rt = RetryTracker(URL, default_config)

        # 409 is not in the list of status codes to retry, but we set is_auto_retryable=True, which should override it
        assert rt.should_retry_status_code(make_http_status_error(409), is_auto_retryable=True) is True
        assert rt.should_retry_status_code(make_http_status_error(409), is_auto_retryable=False) is False
