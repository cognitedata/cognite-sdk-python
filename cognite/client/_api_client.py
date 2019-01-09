import functools
import gzip
import json
import logging
from typing import Any, Dict

from requests import Response, Session

from cognite.client.exceptions import APIError

log = logging.getLogger("cognite-sdk")


def _status_is_valid(status_code: int):
    return status_code < 400


def _raise_API_error(res: Response):
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

        default_headers = client_instance._headers.copy()
        default_headers.update(kwargs.get("headers") or {})
        kwargs["headers"] = default_headers
        res = method(client_instance, full_url, *args, **kwargs)
        if _status_is_valid(res.status_code):
            return res
        _raise_API_error(res)

    return wrapper


class APIClient:
    _LIMIT = 100000
    _LIMIT_AGG = 10000

    def __init__(
        self,
        request_session: Session,
        version: str = None,
        project: str = None,
        base_url: str = None,
        num_of_workers: int = None,
        cookies: Dict = None,
        headers: Dict = None,
        timeout: int = None,
    ):
        self._request_session = request_session
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

    @request_method
    def _get(self, url: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._request_session.get(
            url, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res)
        return res

    @request_method
    def _post(
        self,
        url: str,
        body: Dict[str, Any],
        params: Dict[str, Any] = None,
        use_gzip: bool = True,
        headers: Dict[str, Any] = None,
    ):
        data = json.dumps(body, default=lambda x: x.__dict__)
        headers = headers or {}
        if use_gzip:
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(json.dumps(body, default=lambda x: x.__dict__).encode("utf-8"))
        res = self._request_session.post(
            url, data=data, headers=headers, params=params, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res, body=body)
        return res

    @request_method
    def _put(self, url: str, body: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._request_session.put(
            url, data=json.dumps(body), headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        _log_request(res, body=body)
        return res


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
