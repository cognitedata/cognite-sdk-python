from __future__ import annotations

import ssl
from collections.abc import Iterator

import httpx
import pytest

from cognite.client._http_client import (
    AsyncHTTPClientWithRetryConfig,
    NoCookiesPlease,
    RetryTracker,
    _global_async_httpx_clients,
    get_global_async_httpx_client,
)
from cognite.client.config import global_config
from cognite.client.response import CogniteHTTPResponse


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


@pytest.fixture
def _clean_global_httpx_clients() -> Iterator[None]:
    _global_async_httpx_clients.clear()
    yield
    _global_async_httpx_clients.clear()


@pytest.mark.usefixtures("_clean_global_httpx_clients")
class TestGetGlobalAsyncHttpxClient:
    async def test_no_proxy_when_env_unset_and_config_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Ensure test env does not randomly have these set:
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.delenv("HTTP_PROXY", raising=False)

        client = get_global_async_httpx_client()

        assert not client._mounts

    async def test_env_proxy_is_respected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HTTPS_PROXY", "http://magicenvproxy:666")

        client = get_global_async_httpx_client()

        assert len(client._mounts) == 1

        # If the below asserts fail due to httpx/httpcore private API changes, just keep the assert above.
        (transport,) = client._mounts.values()
        assert transport._pool._proxy_url.host == b"magicenvproxy"  # type: ignore[union-attr]
        assert transport._pool._proxy_url.port == 666  # type: ignore[union-attr]

    async def test_explicit_proxy_config_still_works(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(global_config, "proxy", "http://explicit:1234")

        client = get_global_async_httpx_client()

        assert client._mounts

    async def test_client_settings_match_global_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify pool limits, SSL, redirect, and cookie settings are forwarded correctly."""
        monkeypatch.setattr(global_config, "max_connection_pool_size", 42)
        monkeypatch.setattr(global_config, "disable_ssl", True)
        monkeypatch.setattr(global_config, "follow_redirects", True)
        client = get_global_async_httpx_client()

        assert client.follow_redirects is True
        assert isinstance(client._cookies.jar, NoCookiesPlease)

        pool = client._transport._pool  # type: ignore[attr-defined]
        assert pool._max_connections == 42
        assert pool._keepalive_expiry == 5
        assert pool._ssl_context.verify_mode == ssl.CERT_NONE  # disable_ssl should cause this
        assert pool._ssl_context.check_hostname is False


def make_response(status_code: int) -> CogniteHTTPResponse:
    request = httpx.Request("GET", URL)
    return CogniteHTTPResponse(httpx.Response(status_code=status_code, request=request))


class TestCogniteHTTPResponseRepr:
    @pytest.mark.parametrize(
        "status_code, expected",
        [
            (200, "<CogniteHTTPResponse [200 OK]>"),
            (404, "<CogniteHTTPResponse [404 Not Found]>"),
            (500, "<CogniteHTTPResponse [500 Internal Server Error]>"),
        ],
    )
    def test_repr_format(self, status_code: int, expected: str) -> None:
        assert repr(make_response(status_code)) == expected
