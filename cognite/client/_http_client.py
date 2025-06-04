from __future__ import annotations

import functools
import random
import socket
import sys # Added for Pyodide check
import time
from collections.abc import Callable, Iterable, MutableMapping
from http import cookiejar
from typing import Any, Literal

import httpx

from cognite.client.config import global_config
from cognite.client.exceptions import CogniteConnectionError, CogniteConnectionRefused, CogniteReadTimeout
from cognite.client.utils.useful_types import SupportsRead


class BlockAll(cookiejar.CookiePolicy):
    def no(*args: Any, **kwargs: Any) -> Literal[False]:
        return False

    return_ok = set_ok = domain_return_ok = path_return_ok = no
    netscape = True
    rfc2965 = hide_cookie2 = False


@functools.lru_cache(1)
def get_global_httpx_client() -> httpx.Client:
    # httpx default behavior is to not use cookies, similar to BlockAll.
    # Retries are handled by the SDK's retry logic, not at the client level here.
    limits = httpx.Limits(max_connections=global_config.max_connection_pool_size)
    transport = None
    if "pyodide" in sys.modules:
        try:
            from pyodide_http import PyodideClientTransport
            transport = PyodideClientTransport()
        except ImportError:
            # Handle case where pyodide_http is not available, though it should be in Pyodide
            pass # Or log a warning

    client = httpx.Client(
        limits=limits,
        proxies=global_config.proxies,
        verify=not global_config.disable_ssl,
        transport=transport, # Set transport if in Pyodide
    )
    # No need to mount adapters for http/https like in requests.
    # httpx handles this by default.
    # urllib3.disable_warnings() is not needed as httpx manages its own SSL context
    # if verify=False (disable_ssl=True).
    return client


class HTTPClientConfig:
    def __init__(
        self,
        status_codes_to_retry: set[int],
        backoff_factor: float,
        max_backoff_seconds: int,
        max_retries_total: int,
        max_retries_status: int,
        max_retries_read: int,
        max_retries_connect: int,
    ) -> None:
        self.status_codes_to_retry = status_codes_to_retry
        self.backoff_factor = backoff_factor
        self.max_backoff_seconds = max_backoff_seconds
        self.max_retries_total = max_retries_total
        self.max_retries_status = max_retries_status
        self.max_retries_read = max_retries_read
        self.max_retries_connect = max_retries_connect


class _RetryTracker:
    def __init__(self, config: HTTPClientConfig) -> None:
        self.config = config
        self.status = 0
        self.read = 0
        self.connect = 0

    @property
    def total(self) -> int:
        return self.status + self.read + self.connect

    def _max_backoff_and_jitter(self, t: int) -> int:
        return int(min(t, self.config.max_backoff_seconds) * random.uniform(0, 1.0))

    def get_backoff_time(self) -> int:
        backoff_time = self.config.backoff_factor * (2**self.total)
        backoff_time_adjusted = self._max_backoff_and_jitter(backoff_time)
        return backoff_time_adjusted

    def should_retry(self, status_code: int | None, is_auto_retryable: bool = False) -> bool:
        if self.total >= self.config.max_retries_total:
            return False
        if self.status > 0 and self.status >= self.config.max_retries_status:
            return False
        if self.read > 0 and self.read >= self.config.max_retries_read:
            return False
        if self.connect > 0 and self.connect >= self.config.max_retries_connect:
            return False
        if status_code and status_code not in self.config.status_codes_to_retry and not is_auto_retryable:
            return False
        return True


class HTTPClient:
    def __init__(
        self,
        config: HTTPClientConfig,
        session: httpx.Client,
        refresh_auth_header: Callable[[MutableMapping[str, Any]], None],
        retry_tracker_factory: Callable[[HTTPClientConfig], _RetryTracker] = _RetryTracker,
    ) -> None:
        self.session = session
        self.config = config
        self.refresh_auth_header = refresh_auth_header
        self.retry_tracker_factory = retry_tracker_factory  # needed for tests

    def request(
        self,
        method: str,
        url: str,
        data: str | bytes | Iterable[bytes] | SupportsRead | None = None,
        headers: MutableMapping[str, Any] | None = None,
        timeout: float | None = None,
        params: dict[str, Any] | str | bytes | None = None,
        stream: bool | None = None,
        allow_redirects: bool = False,
    ) -> httpx.Response:
        retry_tracker = self.retry_tracker_factory(self.config)
        accepts_json = (headers or {}).get("accept") == "application/json"
        is_auto_retryable = False
        while True:
            try:
                res = self._do_request(
                    method=method,
                    url=url,
                    data=data,
                    headers=headers,
                    timeout=timeout,
                    params=params,
                    stream=stream,
                    allow_redirects=allow_redirects,
                )
                if accepts_json:
                    # httpx.Response.json() is a method. To cache its result, we can store it in a custom attribute.
                    # However, the SDK's retry logic might lead to multiple response objects for the "same" logical request.
                    # Caching here might be premature if the response object itself is recreated on retry.
                    # For now, let's assume that if `is_auto_retryable` needs to be checked, it's okay to call .json()
                    # and then call it again later if the consumer of the response needs the JSON body.
                    # A more robust caching would involve a wrapper class or careful handling within the retry loop if needed.
                    _cached_json = None
                    try:
                        _cached_json = res.json() # Call once
                        is_auto_retryable = _cached_json.get("error", {}).get("isAutoRetryable", False)
                    except Exception: # Covers JSONDecodeError or if 'error' key path doesn't exist
                        pass

                retry_tracker.status += 1
                if not retry_tracker.should_retry(status_code=res.status_code, is_auto_retryable=is_auto_retryable):
                    return res

            except CogniteReadTimeout as e:
                retry_tracker.read += 1
                if not retry_tracker.should_retry(status_code=None, is_auto_retryable=True):
                    raise e
            except CogniteConnectionError as e:
                retry_tracker.connect += 1
                if not retry_tracker.should_retry(status_code=None, is_auto_retryable=True):
                    raise e

            # During a backoff loop, our credentials might expire, so we check and maybe refresh:
            time.sleep(retry_tracker.get_backoff_time())
            if headers is not None:
                # TODO: Refactoring needed to make this "prettier"
                self.refresh_auth_header(headers)

    def _do_request(
        self,
        method: str,
        url: str,
        data: str | bytes | Iterable[bytes] | SupportsRead | None = None,
        headers: MutableMapping[str, Any] | None = None,
        timeout: float | None = None,
        params: dict[str, Any] | str | bytes | None = None,
        stream: bool | None = None,
        allow_redirects: bool = False,
    ) -> httpx.Response:
        """httpx raises more direct exceptions compared to requests/urllib3's layered approach."""
        try:
            # httpx uses 'content' for body, and 'follow_redirects' for 'allow_redirects'
            res = self.session.request(
                method=method,
                url=url,
                content=data, # Changed from 'data' to 'content'
                headers=headers,
                timeout=timeout,
                params=params,
                stream=stream,
                follow_redirects=allow_redirects, # Changed from 'allow_redirects'
            )
            return res
        except httpx.ReadTimeout as e:
            raise CogniteReadTimeout from e
        except httpx.ConnectTimeout as e: # More specific than ConnectError for timeout cases
            raise CogniteConnectionError(f"Connection timed out: {e}") from e
        except httpx.ConnectError as e:
            # Check for ConnectionRefusedError specifically, as httpx might wrap it
            if isinstance(e.__cause__, ConnectionRefusedError):
                raise CogniteConnectionRefused from e
            raise CogniteConnectionError from e
        except httpx.NetworkError as e: # A base class for network-related errors in httpx
            # This can catch a broader range of network issues not covered by ConnectError/ReadTimeout
            raise CogniteConnectionError from e
        # No need for _any_exception_in_context_isinstance with httpx's more direct exceptions for these cases.
        # Other httpx exceptions like httpcore.LocalProtocolError or httpcore.RemoteProtocolError
        # would typically indicate server-side or unexpected protocol issues and will be raised as is.
        except Exception as e: # Catch any other unexpected exceptions
            raise e
