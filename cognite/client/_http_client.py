from __future__ import annotations

import asyncio
import functools
import logging
import random
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    Mapping,
    MutableMapping,
)
from contextlib import asynccontextmanager, nullcontext
from http.cookiejar import Cookie, CookieJar
from typing import Any, Literal, TypeAlias

import httpx

from cognite.client.config import global_config
from cognite.client.exceptions import (
    CogniteConnectionError,
    CogniteConnectionRefused,
    CogniteHTTPStatusError,
    CogniteReadTimeout,
    CogniteRequestError,
)
from cognite.client.response import CogniteHTTPResponse

logger = logging.getLogger(__name__)


HTTPResponseCoro: TypeAlias = Coroutine[Any, Any, httpx.Response]


class NoCookiesPlease(CookieJar):
    def set_cookie(self, cookie: Cookie) -> None:
        pass


@functools.cache
def get_global_async_httpx_client() -> httpx.AsyncClient:
    async_transport = httpx.AsyncHTTPTransport(
        proxy=global_config.proxy,
        retries=0,  # 'retries': The maximum number of retries when trying to establish a connection.
        verify=not global_config.disable_ssl,
        limits=httpx.Limits(
            # max_connections: The maximum number of concurrent HTTP connections that
            #     the pool should allow. Any attempt to send a request on a pool that
            #     would exceed this amount will block until a connection is available.
            # max_keepalive_connections: The maximum number of idle HTTP connections
            #     that will be maintained in the pool.
            # keepalive_expiry: The duration in seconds that an idle HTTP connection
            #     may be maintained for before being expired from the pool.
            max_connections=global_config.max_connection_pool_size,
            max_keepalive_connections=None,  # defaults to match max_connections
            keepalive_expiry=5,  # copy httpx default
        ),
    )
    return httpx.AsyncClient(
        transport=async_transport,
        follow_redirects=global_config.follow_redirects,
        cookies=NoCookiesPlease(),
        # Below should not be needed when we pass transport, but... :)
        proxy=global_config.proxy,
        verify=not global_config.disable_ssl,
    )


class AsyncHTTPClientWithRetryConfig:
    def __init__(
        self,
        status_codes_to_retry: set[int] | None = None,
        backoff_factor: float = 0.5,
        max_backoff_seconds: int | None = None,
        max_retries_total: int | None = None,
        max_retries_status: int | None = None,
        max_retries_read: int | None = None,
        max_retries_connect: int | None = None,
    ) -> None:
        self._status_codes_to_retry = status_codes_to_retry
        self.backoff_factor = backoff_factor
        self._max_backoff_seconds = max_backoff_seconds
        self._max_retries_total = max_retries_total
        self._max_retries_status = max_retries_status
        self._max_retries_read = max_retries_read
        self._max_retries_connect = max_retries_connect

    @property
    def status_codes_to_retry(self) -> set[int]:
        if self._status_codes_to_retry is None:
            # Changes to the global config need to take effect immediately
            return global_config.status_forcelist
        return self._status_codes_to_retry

    @property
    def max_backoff_seconds(self) -> int:
        if self._max_backoff_seconds is None:
            return global_config.max_retry_backoff
        return self._max_backoff_seconds

    @property
    def max_retries_total(self) -> int:
        if self._max_retries_total is None:
            return global_config.max_retries
        return self._max_retries_total

    @property
    def max_retries_status(self) -> int:
        if self._max_retries_status is None:
            return global_config.max_retries
        return self._max_retries_status

    @property
    def max_retries_read(self) -> int:
        if self._max_retries_read is None:
            return global_config.max_retries
        return self._max_retries_read

    @property
    def max_retries_connect(self) -> int:
        if self._max_retries_connect is None:
            return global_config.max_retries_connect
        return self._max_retries_connect


class RetryTracker:
    def __init__(self, url: str, config: AsyncHTTPClientWithRetryConfig) -> None:
        self.url = url
        self.config = config
        self.status = self.read = self.connect = 0
        self.last_failed_reason = ""
        self.total_time_in_backoff = 0.0

    @property
    def total(self) -> int:
        return self.status + self.read + self.connect

    def get_backoff_time(self) -> float:
        """
        Computes the randomized delay (backoff time) before the next retry attempt.

        This method implements the **Exponential Backoff with Full Jitter** strategy,
        with one addition, a small "initial floor" to avoid immediate retries.
        """
        base = self.config.backoff_factor * 2**self.total
        cap = min(base, self.config.max_backoff_seconds)
        return 0.1 + random.uniform(0, cap)

    async def back_off(self) -> None:
        backoff_time = self.get_backoff_time()
        self.total_time_in_backoff += backoff_time
        # We put logging here, since this is always called before retrying
        logger.debug(
            f"Retrying failed request, attempt #{self.total}, backoff wait: {backoff_time:.4f} sec "
            f"(total: {self.total_time_in_backoff:.4f} sec), reason: {self.last_failed_reason!r}, url: {self.url}"
        )
        await asyncio.sleep(backoff_time)

    @property
    def should_retry_total(self) -> bool:
        # We use 'less than or equal' because should_retry_total is always checked after we have bumped
        # one of [status, read, connect] += 1. Said differently, do last retry when 'total = max':
        return self.total <= self.config.max_retries_total

    def should_retry_status_code(self, err: httpx.HTTPStatusError, is_auto_retryable: bool = False) -> bool:
        self.status += 1
        status_code = err.response.status_code
        error_type = CogniteHTTPStatusError.get_error_type(status_code)
        self.last_failed_reason = f"HTTPStatusError({status_code} - {error_type}: {err.response.reason_phrase})"
        return (
            self.should_retry_total
            and self.status <= self.config.max_retries_status
            and (is_auto_retryable or status_code in self.config.status_codes_to_retry)
        )

    def should_retry_connect_error(self, error: httpx.TransportError | httpx.DecodingError) -> bool:
        self.connect += 1
        self.last_failed_reason = f"{type(error).__name__}({error!s})"
        return self.should_retry_total and self.connect <= self.config.max_retries_connect

    def should_retry_timeout(self, error: httpx.TimeoutException) -> bool:
        self.read += 1
        self.last_failed_reason = f"{type(error).__name__}({error!s})"
        return self.should_retry_total and self.read <= self.config.max_retries_read


class AsyncHTTPClientWithRetry:
    def __init__(
        self,
        config: AsyncHTTPClientWithRetryConfig,
        refresh_auth_header: Callable[[MutableMapping[str, str]], None],
        httpx_async_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self.refresh_auth_header = refresh_auth_header
        self.httpx_async_client = httpx_async_client or get_global_async_httpx_client()

    async def request(
        self,
        method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        /,
        url: str,
        *,
        content: str | bytes | Iterable[bytes] | AsyncIterable[bytes] | None = None,
        data: Mapping[str, Any] | None = None,
        json: Any = None,
        params: Mapping[str, str] | None = None,
        headers: MutableMapping[str, str] | None = None,
        follow_redirects: bool = False,
        timeout: float | None = None,
        semaphore: asyncio.BoundedSemaphore | None,
    ) -> CogniteHTTPResponse:
        def coro_factory() -> HTTPResponseCoro:
            return self.httpx_async_client.request(
                method,
                url,
                content=content,
                data=data,
                json=json,
                params=params,
                headers=headers,
                follow_redirects=follow_redirects,
                timeout=timeout,
            )

        return await self._with_retry(coro_factory, url=url, headers=headers, semaphore=semaphore)

    @asynccontextmanager
    async def stream(
        self,
        method: Literal["GET", "POST"],
        /,
        url: str,
        *,
        json: Any = None,
        headers: MutableMapping[str, str] | None = None,
        timeout: float | None = None,
        semaphore: asyncio.BoundedSemaphore | None,
    ) -> AsyncIterator[CogniteHTTPResponse]:
        # This method is basically a clone of httpx.AsyncClient.stream() so that we may add our own retry logic.
        def coro_factory() -> HTTPResponseCoro:
            request = self.httpx_async_client.build_request(
                method=method, url=url, json=json, headers=headers, timeout=timeout
            )
            return self.httpx_async_client.send(request, stream=True)

        response: CogniteHTTPResponse | None = None
        try:
            yield (response := await self._with_retry(coro_factory, url=url, headers=headers, semaphore=semaphore))
        finally:
            if response:
                await response.aclose()

    async def _with_retry(
        self,
        coro_factory: Callable[[], HTTPResponseCoro],
        *,
        url: str,
        headers: MutableMapping[str, str] | None,
        semaphore: asyncio.BoundedSemaphore | None,
    ) -> CogniteHTTPResponse:
        # When no semaphore is passed, we use nullcontext to skip concurrency limiting. These come from
        # custom top-level POST or GET calls directly on the (Async)Cogniteclient. All normal API calls
        # will be assigned a semaphore in the APIClient layer.
        concurrency_control = semaphore or nullcontext()

        is_auto_retryable = False
        retry_tracker = RetryTracker(url, self.config)
        accepts_json = (headers or {}).get("accept") == "application/json"
        while True:
            try:
                async with concurrency_control:
                    # Ensure our credentials are not about to expire right before making the request:
                    if headers is not None:
                        self.refresh_auth_header(headers)
                    response = await coro_factory()

                if accepts_json:
                    # Cache .json() return value in order to avoid redecoding JSON if called multiple times
                    response.json = functools.cache(response.json)  # type: ignore [method-assign]
                return CogniteHTTPResponse(response.raise_for_status())

            except httpx.HTTPStatusError as err:
                response = err.response
                is_auto_retryable = response.headers.get("cdf-is-auto-retryable", False)
                if not retry_tracker.should_retry_status_code(err, is_auto_retryable):
                    raise CogniteHTTPStatusError(
                        response.status_code,
                        request=err.request,
                        response=CogniteHTTPResponse(response),
                    ) from None

            except httpx.ConnectError as err:
                if not retry_tracker.should_retry_connect_error(err):
                    raise CogniteConnectionRefused from err

            except (httpx.NetworkError, httpx.ConnectTimeout, httpx.DecodingError) as err:
                if not retry_tracker.should_retry_connect_error(err):
                    raise CogniteConnectionError from err

            except httpx.TimeoutException as err:
                if not retry_tracker.should_retry_timeout(err):
                    raise CogniteReadTimeout from err

            except httpx.RequestError as err:
                # We want to avoid raising a non-Cognite error (from the underlying library). httpx.RequestError is the
                # base class for all exceptions that can be raised during a request, so we use it here as a fallback.
                raise CogniteRequestError from err

            # If we got here, it means we have decided to retry the request!
            await retry_tracker.back_off()
