import functools
import gzip
import json
import logging
import time
from copy import deepcopy
from typing import Any, Dict

import requests

from cognite.client.exceptions import APIError

log = logging.getLogger("cognite-sdk")


def _status_is_valid(status_code: int):
    return status_code < 400


def _should_retry(status_code):
    return status_code in [429, 500, 502, 503]


def _serialize(obj):
    """JSON serializer for objects not serializable by default json code"""
    return obj.__dict__


def _exponential_backoff_sleep_seconds(backoff_factor, num_of_tries):
    return backoff_factor * (2 ** (num_of_tries - 1)) if num_of_tries > 0 else 0


def _raise_API_error(res):
    x_request_id = res.headers.get("X-Request-Id")
    code = res.status_code
    try:
        error = res.json()["error"]
        if isinstance(error, str):
            msg = error
        else:
            msg = error["message"]
    except KeyError:
        msg = res.json()
    except:
        msg = res.content

    raise APIError(msg, code, x_request_id)


def _log_request(method, url, **kwargs):
    extra = deepcopy(kwargs)
    if "api-key" in extra.get("headers", {}):
        extra["headers"]["api-key"] = None
    log.info("HTTP/1.1 {} {}".format(method, url), extra=extra)


def request_method(method=None, do_retry: bool = True):
    if method is None:
        return functools.partial(request_method, do_retry=do_retry)

    @functools.wraps(method)
    def wrapper(client_instance, url, *args, **kwargs):
        if not url.startswith("/"):
            raise ValueError("URL must start with '/'")
        full_url = client_instance._base_url + url

        default_headers = deepcopy(client_instance._headers)
        default_headers.update(kwargs.get("headers") or {})
        kwargs["headers"] = default_headers

        total_number_of_tries = range(client_instance._num_of_retries + 1 if do_retry else 1)

        for try_num in total_number_of_tries:
            time.sleep(_exponential_backoff_sleep_seconds(backoff_factor=1, num_of_tries=try_num))
            res = method(client_instance, full_url, *args, **kwargs)
            if _status_is_valid(res.status_code):
                return res
            if not _should_retry(res.status_code):
                break
        _raise_API_error(res)

    return wrapper


class APIClient:
    _LIMIT = 100_000
    _LIMIT_AGG = 10_000

    def __init__(
        self,
        version: str = None,
        project: str = None,
        base_url: str = None,
        num_of_retries: int = None,
        num_of_workers: int = None,
        cookies: Dict = None,
        headers: Dict = None,
        timeout: int = None,
    ):
        self._project = project
        __base_path = f"/api/{version}/projects/{project}" if version else ""
        self._base_url = base_url + __base_path
        self._num_of_retries = num_of_retries
        self._num_of_workers = num_of_workers
        self._cookies = cookies
        self._headers = headers
        self._timeout = timeout

    @request_method
    def _delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a DELETE request with a predetermined number of retries."""
        _log_request("DELETE", url, params=params, headers=headers, cookies=self._cookies)
        return requests.delete(url, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout)

    @request_method
    def _get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a GET request with a predetermined number of retries."""
        _log_request("GET", url, params=params, headers=headers, cookies=self._cookies)
        return requests.get(url, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout)

    @request_method(do_retry=False)
    def _post(
        self,
        url: str,
        body: Dict[str, Any],
        params: Dict[str, Any] = None,
        use_gzip: bool = True,
        headers: Dict[str, Any] = None,
    ):
        """Perform a POST request."""
        _log_request("POST", url, body=body, params=params, headers=headers, cookies=self._cookies)

        data = json.dumps(body, default=_serialize)
        headers = headers or {}
        if use_gzip:
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(json.dumps(body, default=_serialize).encode("utf-8"))
        return requests.post(
            url, data=data, headers=headers, params=params, cookies=self._cookies, timeout=self._timeout
        )

    @request_method
    def _put(self, url: str, body: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a PUT request with a predetermined number of retries."""
        _log_request("PUT", url, body=body, headers=headers, cookies=self._cookies)
        return requests.put(url, data=json.dumps(body), headers=headers, cookies=self._cookies, timeout=self._timeout)


class CogniteResponse:
    """Cognite Response class

    All responses inherit from this class.

    Examples:
        All responses are pretty-printable::

            from cognite import CogniteClient

            client = CogniteClient()
            res = client.assets.get_assets(limit=1)

            print(res)

        All endpoints which support paging have an ``autopaging`` flag which may be set to true in order to sequentially
        fetch all resources. If for some reason, you want to do this manually, you may use the next_cursor() method on
        the response object. Here is an example of that::

            from cognite import CogniteClient

            client = CogniteClient()

            asset_list = []

            cursor = None
            while True:
                res = client.assets.get_assets(cursor=cursor)
                asset_list.extend(res.to_json())
                cursor = res.next_cursor()
                if cursor is None:
                    break

            print(asset_list)
    """

    def __init__(self, internal_representation):
        self.internal_representation = internal_representation

    def __str__(self):
        return json.dumps(self.to_json(), indent=4, sort_keys=True)

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def next_cursor(self):
        """Returns next cursor to use for paging through results. Returns ``None`` if there are no more results."""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("nextCursor")

    def previous_cursor(self):
        """Returns previous cursor to use for paging through results. Returns ``None`` if there are no more results."""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("previousCursor")
