import random
import socket
import time
from http import cookiejar
from typing import Optional, Set, Union

import requests.exceptions
import urllib3.exceptions
from requests import Response, Session
from requests.adapters import HTTPAdapter

from cognite.client import utils
from cognite.client.exceptions import CogniteConnectionError, CogniteConnectionRefused, CogniteReadTimeout


class BlockAll(cookiejar.CookiePolicy):
    return_ok = set_ok = domain_return_ok = path_return_ok = lambda self, *args, **kwargs: False
    netscape = True
    rfc2965 = hide_cookie2 = False


def _init_requests_session():
    session = Session()
    session.cookies.set_policy(BlockAll())
    cognite_config = utils._client_config._DefaultConfig()
    adapter = HTTPAdapter(pool_maxsize=cognite_config.max_connection_pool_size)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if cognite_config.disable_ssl:
        urllib3.disable_warnings()
        session.verify = False
    return session


GLOBAL_REQUEST_SESSION = _init_requests_session()


class HTTPClientConfig:
    def __init__(
        self,
        status_codes_to_retry: Set[int],
        backoff_factor: float,
        max_backoff_seconds: int,
        max_retries_total: int,
        max_retries_status: int,
        max_retries_read: int,
        max_retries_connect: int,
    ):
        self.status_codes_to_retry = status_codes_to_retry
        self.backoff_factor = backoff_factor
        self.max_backoff_seconds = max_backoff_seconds
        self.max_retries_total = max_retries_total
        self.max_retries_status = max_retries_status
        self.max_retries_read = max_retries_read
        self.max_retries_connect = max_retries_connect


class _RetryTracker:
    def __init__(self, config: HTTPClientConfig):
        self.config = config
        self.status = 0
        self.read = 0
        self.connect = 0

    @property
    def total(self):
        return self.status + self.read + self.connect

    def _max_backoff_and_jitter(self, t: int) -> int:
        return int(min(t, self.config.max_backoff_seconds) * random.uniform(0, 1.0))

    def get_backoff_time(self) -> int:
        backoff_time = self.config.backoff_factor * (2 ** self.total)
        backoff_time_adjusted = self._max_backoff_and_jitter(backoff_time)
        return backoff_time_adjusted

    def should_retry(self, status_code: Optional[int]) -> bool:
        if self.total >= self.config.max_retries_total:
            return False
        if self.status >= self.config.max_retries_status:
            return False
        if self.read >= self.config.max_retries_read:
            return False
        if self.connect >= self.config.max_retries_connect:
            return False
        if status_code and status_code not in self.config.status_codes_to_retry:
            return False
        return True


class HTTPClient:
    def __init__(self, session: Session, config: HTTPClientConfig):
        self.session = session
        self.config = config
        self.retry_tracker = None

    def request(self, method: str, url: str, **kwargs) -> Response:
        self.retry_tracker = _RetryTracker(config=self.config)
        last_status = None
        while True:
            try:
                res = self._do_request(method=method, url=url, **kwargs)
                last_status = res.status_code
                self.retry_tracker.status += 1
                if not self.retry_tracker.should_retry(status_code=last_status):
                    return res
            except CogniteReadTimeout as e:
                self.retry_tracker.read += 1
                if not self.retry_tracker.should_retry(status_code=last_status):
                    raise e
            except CogniteConnectionError as e:
                self.retry_tracker.connect += 1
                if isinstance(e, CogniteConnectionRefused) or not self.retry_tracker.should_retry(
                    status_code=last_status
                ):
                    raise e
            time.sleep(self.retry_tracker.get_backoff_time())

    def _do_request(self, method: str, url: str, **kwargs) -> Response:
        try:
            res = self.session.request(method=method, url=url, **kwargs)
            return res
        except requests.exceptions.RequestException as e:
            underlying_exc = self._get_underlying_exception(e)
            if isinstance(underlying_exc, socket.timeout):
                raise CogniteReadTimeout from e
            if isinstance(underlying_exc, ConnectionError):
                if isinstance(underlying_exc, ConnectionRefusedError):
                    raise CogniteConnectionRefused from e
                else:
                    raise CogniteConnectionError from e

    @staticmethod
    def _get_underlying_exception(exc: requests.exceptions.RequestException) -> Union[BaseException, None]:
        """ requests.exceptions.RequestException adds three layers on top of built-in networking exceptions:
        requests.exceptions.RequestException -> urllib3.exceptions.MaxRetryError -> urllib3.exceptions.HTTPError
        -> underlying exception

        requests does not use the "raise ... from ..." syntax, so we need to access the underlying exception using the
        __context__ attribute.
        """
        return exc.__context__.__context__.__context__
