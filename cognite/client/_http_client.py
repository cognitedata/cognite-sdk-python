from __future__ import annotations

import functools
import random
import socket
import time
from http import cookiejar
from typing import Any, Callable, Literal, MutableMapping

import requests
import requests.adapters
import urllib3

from cognite.client.config import global_config
from cognite.client.exceptions import CogniteConnectionError, CogniteConnectionRefused, CogniteReadTimeout


class BlockAll(cookiejar.CookiePolicy):
    def no(*args: Any, **kwargs: Any) -> Literal[False]:
        return False

    return_ok = set_ok = domain_return_ok = path_return_ok = no
    netscape = True
    rfc2965 = hide_cookie2 = False


@functools.lru_cache(1)
def get_global_requests_session() -> requests.Session:
    session = requests.Session()
    session.cookies.set_policy(BlockAll())
    adapter = requests.adapters.HTTPAdapter(
        pool_maxsize=global_config.max_connection_pool_size, max_retries=urllib3.Retry(False)
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if global_config.disable_ssl:
        urllib3.disable_warnings()
        session.verify = False
    if global_config.proxies is not None:
        session.proxies.update(global_config.proxies)
    return session


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

    def should_retry(self, status_code: int | None) -> bool:
        if self.total >= self.config.max_retries_total:
            return False
        if self.status > 0 and self.status >= self.config.max_retries_status:
            return False
        if self.read > 0 and self.read >= self.config.max_retries_read:
            return False
        if self.connect > 0 and self.connect >= self.config.max_retries_connect:
            return False
        if status_code and status_code not in self.config.status_codes_to_retry:
            return False
        return True


class HTTPClient:
    def __init__(
        self,
        config: HTTPClientConfig,
        session: requests.Session,
        refresh_auth_header: Callable[[MutableMapping[str, Any]], None],
        retry_tracker_factory: Callable[[HTTPClientConfig], _RetryTracker] = _RetryTracker,
    ) -> None:
        self.session = session
        self.config = config
        self.refresh_auth_header = refresh_auth_header
        self.retry_tracker_factory = retry_tracker_factory  # needed for tests

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        retry_tracker = self.retry_tracker_factory(self.config)
        headers = kwargs.get("headers")
        last_status = None
        while True:
            try:
                res = self._do_request(method=method, url=url, **kwargs)
                last_status = res.status_code
                retry_tracker.status += 1
                if not retry_tracker.should_retry(status_code=last_status):
                    # Cache .json() return value in order to avoid redecoding JSON if called multiple times
                    res.json = functools.lru_cache(maxsize=1)(res.json)  # type: ignore[assignment]
                    return res
            except CogniteReadTimeout as e:
                retry_tracker.read += 1
                if not retry_tracker.should_retry(status_code=last_status):
                    raise e
            except CogniteConnectionError as e:
                retry_tracker.connect += 1
                if not retry_tracker.should_retry(status_code=last_status):
                    raise e

            # During a backoff loop, our credentials might expire, so we check and maybe refresh:
            time.sleep(retry_tracker.get_backoff_time())
            if headers is not None:
                # TODO: Refactoring needed to make this "prettier"
                self.refresh_auth_header(headers)

    def _do_request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """requests/urllib3 adds 2 or 3 layers of exceptions on top of built-in networking exceptions.

        Sometimes the appropriate built-in networking exception is not in the context, sometimes the requests
        exception is not in the context, so we need to check for the appropriate built-in exceptions,
        urllib3 exceptions, and requests exceptions.
        """
        try:
            res = self.session.request(method=method, url=url, **kwargs)
            return res
        except Exception as e:
            if self._any_exception_in_context_isinstance(
                e, (socket.timeout, urllib3.exceptions.ReadTimeoutError, requests.exceptions.ReadTimeout)
            ):
                raise CogniteReadTimeout from e
            if self._any_exception_in_context_isinstance(
                e,
                (
                    ConnectionError,
                    urllib3.exceptions.ConnectionError,
                    urllib3.exceptions.ConnectTimeoutError,
                    requests.exceptions.ConnectionError,
                ),
            ):
                if self._any_exception_in_context_isinstance(e, ConnectionRefusedError):
                    raise CogniteConnectionRefused from e
                raise CogniteConnectionError from e
            raise e

    @classmethod
    def _any_exception_in_context_isinstance(
        cls, exc: BaseException, exc_types: tuple[type[BaseException], ...] | type[BaseException]
    ) -> bool:
        """requests does not use the "raise ... from ..." syntax, so we need to access the underlying exceptions using
        the __context__ attribute.
        """
        if isinstance(exc, exc_types):
            return True
        if exc.__context__ is None:
            return False
        return cls._any_exception_in_context_isinstance(exc.__context__, exc_types)
