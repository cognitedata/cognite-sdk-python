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
    return status_code in [401, 429, 500, 502, 503]


def _serialize(obj):
    """JSON serializer for objects not serializable by default json code"""
    return obj.__dict__


def _exponential_backoff_sleep_seconds(backoff_factor, num_of_tries):
    return backoff_factor * (2 ** (num_of_tries - 1))


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


def _log_request(log_level, method, url, **kwargs):
    log.log(logging.getLevelName(log_level), "HTTP/1.1 {} {}".format(method, url), extra=kwargs)


def request_method(method):
    def wrapper(client_instance, url, *args, **kwargs):
        if not url.startswith("/"):
            raise ValueError("URL must start with '/'")
        full_url = client_instance._base_url + url

        default_headers = deepcopy(client_instance._headers)
        default_headers.update(kwargs.get("headers") or {})
        kwargs["headers"] = default_headers

        for number_of_tries in range(client_instance._num_of_retries + 1):
            res = method(client_instance, full_url, *args, **kwargs)
            if _status_is_valid(res.status_code):
                return res
            if not _should_retry(res.status_code):
                break

            time.sleep(_exponential_backoff_sleep_seconds(backoff_factor=1, num_of_tries=number_of_tries))
        _raise_API_error(res)

    return wrapper


class APIClient:
    LIMIT = 100000
    LIMIT_AGG = 10000

    def __init__(
        self,
        project: str = None,
        base_url: str = None,
        num_of_retries: int = None,
        num_of_workers: int = None,
        cookies: Dict = None,
        headers: Dict = None,
        log_level: str = None,
    ):
        self._project = project
        self._base_url = base_url
        self._num_of_retries = num_of_retries
        self._num_of_workers = num_of_workers
        self._cookies = cookies
        self._headers = headers
        self._log_level = log_level

    @request_method
    def _delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a DELETE request with a predetermined number of retries."""
        _log_request(self._log_level, "DELETE", url, params=params, headers=headers, cookies=self._cookies)
        return requests.delete(url, params=params, headers=headers, cookies=self._cookies)

    @request_method
    def _get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a GET request with a predetermined number of retries."""
        _log_request(self._log_level, "GET", url, params=params, headers=headers, cookies=self._cookies)
        return requests.get(url, params=params, headers=headers, cookies=self._cookies)

    @request_method
    def _post(
        self,
        url: str,
        body: Dict[str, Any],
        params: Dict[str, Any] = None,
        use_gzip: bool = False,
        headers: Dict[str, Any] = None,
    ):
        """Perform a POST request with a predetermined number of retries."""
        _log_request(self._log_level, "POST", url, body=body, params=params, headers=headers, cookies=self._cookies)

        data = json.dumps(body, default=_serialize)
        headers = headers or {}
        if use_gzip:
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(json.dumps(body, default=_serialize).encode("utf-8"))
        return requests.post(url, data=data, headers=headers, params=params, cookies=self._cookies)

    @request_method
    def _put(self, url: str, body: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        """Perform a PUT request with a predetermined number of retries."""
        _log_request(self._log_level, "PUT", url, body=body, headers=headers, cookies=self._cookies)
        return requests.put(url, data=json.dumps(body), headers=headers, cookies=self._cookies)
