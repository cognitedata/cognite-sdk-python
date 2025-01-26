from __future__ import annotations

import functools
import random
import time
from collections.abc import AsyncIterable, Callable, Iterable, Mapping, MutableMapping
from contextlib import AbstractContextManager, suppress
from http.cookiejar import Cookie, CookieJar
from json import JSONDecodeError
from typing import Any, Literal, TypeVar
from venv import logger

import httpx

from cognite.client.config import global_config
from cognite.client.exceptions import (
    CogniteConnectionError,
    CogniteConnectionRefused,
    CogniteReadTimeout,
    CogniteRequestError,
)

_T = TypeVar("_T", httpx.Response, AbstractContextManager[httpx.Response])


class NoCookiesPlease(CookieJar):
    def set_cookie(self, cookie: Cookie) -> None:
        pass


@functools.cache
def get_global_httpx_client() -> httpx.Client:
    client = httpx.Client(
        transport=httpx.HTTPTransport(
            # 'retries': The maximum number of retries when trying to establish a connection.
            retries=0,
            verify=not global_config.disable_ssl,
            limits=httpx.Limits(
                # max_connections: The maximum number of concurrent HTTP connections that
                #     the pool should allow. Any attempt to send a request on a pool that
                #     would exceed this amount will block until a connection is available.
                max_connections=global_config.max_connection_pool_size,
                # max_keepalive_connections: The maximum number of idle HTTP connections
                #     that will be maintained in the pool.
                max_keepalive_connections=None,  # defaults to match max_connections
                # keepalive_expiry: The duration in seconds that an idle HTTP connection
                #     may be maintained for before being expired from the pool.
                keepalive_expiry=5,  # copy httpx default
            ),
        ),
        follow_redirects=global_config.allow_redirects,
        cookies=NoCookiesPlease(),
        verify=not global_config.disable_ssl,  # ...not be needed when we pass transport, but... :)
        # TODO: httpx has deprecated 'proxies'; pass single proxy or dict of proxies as 'mounts' instead
        # proxies=global_config.proxies,
    )
    return client


class HTTPClientWithRetryConfig:
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
    def __init__(self, config: HTTPClientWithRetryConfig) -> None:
        self.config = config
        self.status = self.read = self.connect = 0
        self.last_failed_reason = ""

    @property
    def total(self) -> int:
        return self.status + self.read + self.connect

    def get_backoff_time(self) -> float:
        backoff_time = self.config.backoff_factor * 2**self.total
        return random.random() * min(backoff_time, self.config.max_backoff_seconds)

    def back_off(self, url: str) -> None:
        backoff_time = self.get_backoff_time()
        logger.debug(
            f"Retrying failed request, attempt #{self.total}, backoff time: {backoff_time=:.4f} sec, "
            f"reason: {self.last_failed_reason!r}, url: {url}"
        )
        time.sleep(backoff_time)

    @property
    def should_retry_total(self) -> bool:
        return self.total < self.config.max_retries_total

    def should_retry_status_code(self, status_code: int, is_auto_retryable: bool = False) -> bool:
        self.status += 1
        self.last_failed_reason = f"{status_code=}"
        return (
            self.should_retry_total
            and self.status <= self.config.max_retries_status
            and (is_auto_retryable or status_code in self.config.status_codes_to_retry)
        )

    def should_retry_connect_error(self, error: httpx.RequestError) -> bool:
        self.connect += 1
        self.last_failed_reason = type(error).__name__
        return self.should_retry_total and self.connect <= self.config.max_retries_connect

    def should_retry_timeout(self, error: httpx.RequestError) -> bool:
        self.read += 1
        self.last_failed_reason = type(error).__name__
        return self.should_retry_total and self.read <= self.config.max_retries_read


class HTTPClientWithRetry:
    def __init__(
        self,
        config: HTTPClientWithRetryConfig,
        refresh_auth_header: Callable[[MutableMapping[str, str]], None],
        httpx_client: httpx.Client | None = None,
    ) -> None:
        self.config = config
        self.refresh_auth_header = refresh_auth_header
        self.httpx_client = httpx_client or get_global_httpx_client()

    def __call__(
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
    ) -> httpx.Response:
        fn: Callable[..., httpx.Response] = functools.partial(
            self.httpx_client.request,
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
        return self._with_retry(fn, url=url, headers=headers)

    def stream(
        self,
        method: Literal["GET", "POST"],
        /,
        url: str,
        *,
        json: Any = None,
        headers: MutableMapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> AbstractContextManager[httpx.Response]:
        fn: Callable[..., AbstractContextManager[httpx.Response]] = functools.partial(
            self.httpx_client.stream, method, url, json=json, headers=headers, timeout=timeout
        )
        return self._with_retry(fn, url=url, headers=headers, stream=True)

    def _with_retry(
        self,
        fn: Callable[..., _T],
        *,
        url: str,
        headers: MutableMapping[str, str] | None,
        is_auto_retryable: bool = False,
        stream: bool = False,
    ) -> _T:
        retry_tracker = RetryTracker(self.config)
        accepts_json = (headers or {}).get("accept") == "application/json"
        while True:
            try:
                if stream:
                    return fn()
                res: httpx.Response = fn()  # type: ignore
                if accepts_json:
                    # Cache .json() return value in order to avoid redecoding JSON if called multiple times
                    # TODO: Can this be removed now if we check the 'cdf-is-auto-retryable' header?
                    res.json = functools.cache(res.json)  # type: ignore [assignment]
                return res.raise_for_status()  # type: ignore [return-value]

            except httpx.HTTPStatusError:  # only raised from raise_for_status() -> status code is guaranteed
                if accepts_json:
                    with suppress(JSONDecodeError, AttributeError):
                        # If the response is not JSON or it doesn't conform to the api design guide,
                        # we assume it's not auto-retryable
                        # TODO: Can we just check the header now? 'cdf-is-auto-retryable'
                        is_auto_retryable = res.json().get("error", {}).get("isAutoRetryable", False)

                if not retry_tracker.should_retry_status_code(res.status_code, is_auto_retryable):
                    raise

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

            retry_tracker.back_off(url)
            # During a backoff loop, our credentials might expire, so we check and maybe refresh:
            if headers is not None:
                self.refresh_auth_header(headers)  # TODO: Refactoring needed to make this "prettier"
