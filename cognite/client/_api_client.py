import functools
import gzip
import json
import logging
import os
import re
from typing import Any, Dict

import numpy
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry

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


def request_method(method=None):
    @functools.wraps(method)
    def wrapper(client_instance, url, *args, **kwargs):
        if not url.startswith("/"):
            raise ValueError("URL must start with '/'")
        full_url = client_instance._base_url + url

        # Hack to allow running model hosting requests against local emulator
        if os.getenv("USE_MODEL_HOSTING_EMULATOR") == "1":
            full_url = _model_hosting_emulator_url_converter(full_url)

        default_headers = client_instance._headers.copy()
        default_headers.update(kwargs.get("headers") or {})
        kwargs["headers"] = default_headers
        res = method(client_instance, full_url, *args, **kwargs)
        if _status_is_valid(res.status_code):
            return res
        _raise_API_error(res)

    return wrapper


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

    @request_method
    def _delete(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._request_session.delete(
            url, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res)
        return res

    def _autopaged_get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        params = params.copy()
        items = []
        while True:
            url = re.sub("{}(/.*)".format(self._base_url), r"\1", url)
            res = self._get(url, params=params, headers=headers)
            params["cursor"] = res.json()["data"].get("nextCursor")
            items.extend(res.json()["data"]["items"])
            next_cursor = res.json()["data"].get("nextCursor")
            if not next_cursor:
                break
        res._content = json.dumps({"data": {"items": items}}).encode()
        return res

    @request_method
    def _get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None, autopaging: bool = False):
        if autopaging:
            return self._autopaged_get(url, params, headers)
        res = self._request_session.get(
            url, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res)
        return res

    @request_method
    def _post(self, url: str, body: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        data = json.dumps(body, default=self._json_dumps_default)
        if not os.getenv("COGNITE_DISABLE_GZIP", False):
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(data.encode())
        res = self._request_session.post(
            url, data=data, headers=headers, params=params, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res, body=body)
        return res

    @request_method
    def _put(self, url: str, body: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        data = json.dumps(body or {}, default=self._json_dumps_default)
        if not os.getenv("COGNITE_DISABLE_GZIP", False):
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(data.encode())
        res = self._request_session.put(url, data=data, headers=headers, cookies=self._cookies, timeout=self._timeout)
        _log_request(res, body=body)
        return res

    @staticmethod
    def _json_dumps_default(x):
        if isinstance(x, numpy.int_):
            return int(x)
        if isinstance(x, numpy.float_):
            return float(x)
        if isinstance(x, numpy.bool_):
            return bool(x)
        return x.__dict__


class CogniteResponse:
    """Cognite Response class

    All responses inherit from this class.

    Examples:
        All responses are pretty-printable::

            from cognite.client import CogniteClient

            client = CogniteClient()
            res = client.assets.get_assets(limit=1)

            print(res)

        All endpoints which support paging have an ``autopaging`` flag which may be set to true in order to sequentially
        fetch all resources. If for some reason, you want to do this manually, you may use the next_cursor() method on
        the response object. Here is an example of that::

            from cognite.client import CogniteClient

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
        return self.internal_representation["data"]["items"][0]

    def next_cursor(self):
        """Returns next cursor to use for paging through results. Returns ``None`` if there are no more results."""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("nextCursor")

    def previous_cursor(self):
        """Returns previous cursor to use for paging through results. Returns ``None`` if there are no more results."""
        if self.internal_representation.get("data"):
            return self.internal_representation.get("data").get("previousCursor")


class CogniteCollectionResponse(CogniteResponse):
    """Cognite Collection Response class

    All collection responses inherit from this class. Collection responses are subscriptable and iterable.
    """

    _RESPONSE_CLASS = None

    def to_json(self):
        """Returns data as a json object"""
        return self.internal_representation["data"]["items"]

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.__class__({"data": {"items": self.to_json()[index]}})
        return self._RESPONSE_CLASS({"data": {"items": [self.to_json()[index]]}})

    def __len__(self):
        return len(self.to_json())

    def __iter__(self):
        self.counter = 0
        return self

    def __next__(self):
        if self.counter > len(self.to_json()) - 1:
            raise StopIteration
        else:
            self.counter += 1
            return self._RESPONSE_CLASS({"data": {"items": [self.to_json()[self.counter - 1]]}})


class CogniteResource:
    @staticmethod
    def _to_camel_case(snake_case_string: str):
        components = snake_case_string.split("_")
        return components[0] + "".join(x.title() for x in components[1:])

    def camel_case_dict(self):
        new_d = {}
        for key in self.__dict__:
            new_d[self._to_camel_case(key)] = self.__dict__[key]
        return new_d

    def to_json(self):
        return json.loads(json.dumps(self, default=lambda x: x.__dict__))

    def __eq__(self, other):
        return type(self) == type(other) and self.to_json() == other.to_json()

    def __str__(self):
        return json.dumps(self.__dict__, default=lambda x: x.__dict__, indent=4, sort_keys=True)
