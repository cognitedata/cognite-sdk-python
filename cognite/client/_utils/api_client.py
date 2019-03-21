import gzip
import json as _json
import logging
import os
import re
from typing import Any, Dict, List, Union

import numpy
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cognite.client.exceptions import APIError

log = logging.getLogger("cognite-sdk")

DEFAULT_NUM_OF_RETRIES = 5
HTTP_METHODS_TO_RETRY = [429, 500, 502, 503]


def _init_requests_session():
    session = Session()
    num_of_retries = int(os.getenv("COGNITE_NUM_RETRIES", DEFAULT_NUM_OF_RETRIES))
    retry = Retry(
        total=num_of_retries,
        read=num_of_retries,
        connect=num_of_retries,
        backoff_factor=0.5,
        status_forcelist=HTTP_METHODS_TO_RETRY,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_REQUESTS_SESSION = _init_requests_session()


def _status_is_valid(status_code: int):
    return status_code < 400


def _raise_API_error(res: Response):
    x_request_id = res.headers.get("X-Request-Id")
    code = res.status_code
    extra = {}
    try:
        error = res.json()["error"]
        if isinstance(error, str):
            msg = error
        else:
            msg = error["message"]
            extra = error.get("extra")
    except:
        msg = res.content

    log.error("HTTP Error %s: %s", code, msg, extra={"X-Request-ID": x_request_id, "extra": extra})
    raise APIError(msg, code, x_request_id, extra=extra)


def _log_request(res: Response, **kwargs):
    method = res.request.method
    url = res.request.url
    status_code = res.status_code

    extra = kwargs.copy()
    extra["headers"] = res.request.headers
    if "api-key" in extra.get("headers", {}):
        extra["headers"]["api-key"] = None

    http_protocol_version = ".".join(list(str(res.raw.version)))

    log.info("HTTP/{} {} {} {}".format(http_protocol_version, method, url, status_code), extra=extra)


def _model_hosting_emulator_url_converter(url):
    pattern = "https://api.cognitedata.com/api/0.6/projects/(.*)/analytics/models(.*)"
    res = re.match(pattern, url)
    if res is not None:
        project = res.group(1)
        path = res.group(2)
        return "http://localhost:8000/api/0.1/projects/{}/models{}".format(project, path)
    return url


class APIClient:
    _LIMIT = 1000

    def __init__(
        self,
        version: str = None,
        project: str = None,
        base_url: str = None,
        num_of_workers: int = None,
        cookies: Dict = None,
        headers: Dict = None,
        timeout: int = None,
    ):
        self._request_session = _REQUESTS_SESSION
        self._project = project
        __base_path = "/api/{}/projects/{}".format(version, project) if version else ""
        self._base_url = base_url + __base_path
        self._num_of_workers = num_of_workers
        self._cookies = cookies
        self._headers = headers
        self._timeout = timeout

    def _delete(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._do_request(
            "DELETE", url_path, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res)
        return res

    def _get(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._do_request(
            "GET", url_path, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res)
        return res

    def _post(self, url_path: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        data = _json.dumps(json, default=self._json_dumps_default)
        res = self._do_request(
            "POST", url_path, data=data, headers=headers, params=params, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res, body=json)
        return res

    def _put(self, url_path: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        data = _json.dumps(json or {}, default=self._json_dumps_default)
        res = self._do_request(
            "PUT", url_path, data=data, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res, body=json)
        return res

    def _do_request(self, method: str, url_path: str, **kwargs):
        if not url_path.startswith("/"):
            raise ValueError("URL path must start with '/'")
        full_url = self._base_url + url_path
        # Hack to allow running model hosting requests against local emulator
        if os.getenv("USE_MODEL_HOSTING_EMULATOR") == "1":
            full_url = _model_hosting_emulator_url_converter(full_url)

        default_headers = self._headers.copy()
        if (
            not os.getenv("COGNITE_DISABLE_GZIP", False)
            and method in ["PUT", "POST"]
            and kwargs.get("data") is not None
        ):
            default_headers["Content-Encoding"] = "gzip"
            kwargs["data"] = gzip.compress(kwargs["data"].encode())
        default_headers.update(kwargs.get("headers") or {})
        kwargs["headers"] = default_headers

        res = self._request_session.request(method=method, url=full_url, **kwargs)
        if not _status_is_valid(res.status_code):
            _raise_API_error(res)
        return res

    def _retrieve(self, cls, resource_path: str, id: int, params: Dict = None, headers: Dict = None):
        return cls._load(
            self._get(url_path=resource_path + "/{}".format(id), params=params, headers=headers).json()["data"][
                "items"
            ][0]
        )

    def _retrieve_multiple(self, cls, resource_path: str, ids: Union[List[int], int], headers: Dict = None):
        if isinstance(ids, int):
            return cls._RESOURCE._load(
                self._post(url_path=resource_path + "/byids", json={"items": [ids]}, headers=headers).json()["data"][
                    "items"
                ][0]
            )
        elif isinstance(ids, list):
            return cls._load(
                self._post(url_path=resource_path + "/byids", json={"items": ids}, headers=headers).json()["data"][
                    "items"
                ]
            )
        raise TypeError("ids must be int or list of int")

    def _list_generator(
        self, cls, resource_path: str, limit: int = None, chunk: int = None, params: Dict = None, headers: Dict = None
    ):
        total_items_retrieved = 0
        current_limit = self._LIMIT
        if chunk:
            assert chunk <= self._LIMIT, "Chunk size can not exceed {}".format(self._LIMIT)
            current_limit = chunk
        next_cursor = None
        params = params or {}
        while True:
            if limit:
                num_of_remaining_items = limit - total_items_retrieved
                if num_of_remaining_items < self._LIMIT:
                    current_limit = num_of_remaining_items
            params["limit"] = current_limit
            params["cursor"] = next_cursor
            res = self._get(url_path=resource_path, params=params, headers=headers)
            current_items = res.json()["data"]["items"]
            if chunk:
                yield cls._load(current_items)
            else:
                for item in current_items:
                    yield cls._RESOURCE._load(item)
            total_items_retrieved += len(current_items)
            next_cursor = res.json()["data"].get("nextCursor")
            if total_items_retrieved == limit or next_cursor is None:
                break

    def _list(self, cls, resource_path: str, limit: int = None, params: Dict = None, headers: Dict = None):
        items = []
        for resource_list in self._list_generator(
            cls=cls, resource_path=resource_path, limit=limit, chunk=self._LIMIT, params=params, headers=headers
        ):
            items.extend(resource_list._resources)
        return cls(items)

    def _create(self, cls: Any, resource_path: str, items: List[Any], params: Dict = None, headers: Dict = None):
        items = {"items": [item.dump(camel_case=True) for item in items]}
        res = self._post(resource_path, json=items, headers=headers, params=params)
        return cls._load(res.json()["data"]["items"])

    def _delete_multiple(
        self, cls: Any, resource_path: str, ids: Union[List[int], int], params: Dict = None, headers: Dict = None
    ):
        if isinstance(ids, int):
            items = {"items": [ids]}
            res = self._post(resource_path + "/delete", json=items, headers=headers, params=params)
            return cls._RESOURCE._load(res.json()["data"]["items"][0])
        elif isinstance(ids, list):
            items = {"items": ids}
            res = self._post(resource_path + "/delete", json=items, headers=headers, params=params)
            return cls._load(res.json()["data"]["items"])
        raise TypeError("ids must be int or list of ints")

    @staticmethod
    def _json_dumps_default(x):
        if isinstance(x, numpy.int_):
            return int(x)
        if isinstance(x, numpy.float_):
            return float(x)
        if isinstance(x, numpy.bool_):
            return bool(x)
        return x.__dict__
