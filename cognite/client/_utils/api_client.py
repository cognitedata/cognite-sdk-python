import gzip
import json as _json
import logging
import os
import re
from typing import Any, Dict, List, Tuple, Union

import numpy
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from cognite.client._utils import utils
from cognite.client._utils.base import CogniteResource, CogniteUpdate
from cognite.client.exceptions import APIError

log = logging.getLogger("cognite-sdk")

DEFAULT_MAX_RETRIES = 5
HTTP_METHODS_TO_RETRY = [429, 500, 502, 503]


def _init_requests_session():
    session = Session()
    num_of_retries = int(os.getenv("COGNITE_MAX_RETRIES", DEFAULT_MAX_RETRIES))
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


class APIClient:
    _LIMIT = 1000

    def __init__(
        self,
        version: str = None,
        project: str = None,
        base_url: str = None,
        max_workers: int = None,
        cookies: Dict = None,
        headers: Dict = None,
        timeout: int = None,
    ):
        self._request_session = _REQUESTS_SESSION
        self._project = project
        __base_path = "/api/{}/projects/{}".format(version, project) if version else ""
        self._base_url = base_url + __base_path
        self._max_workers = max_workers
        self._cookies = cookies
        self._headers = headers
        self._timeout = timeout

    def _delete(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._do_request(
            "DELETE", url_path, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        self._log_request(res)
        return res

    def _get(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        res = self._do_request(
            "GET", url_path, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        self._log_request(res)
        return res

    def _post(self, url_path: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        data = _json.dumps(json, default=self._json_dumps_default)
        res = self._do_request(
            "POST", url_path, data=data, headers=headers, params=params, cookies=self._cookies, timeout=self._timeout
        )
        self._log_request(res, body=json)
        return res

    def _put(self, url_path: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        data = _json.dumps(json or {}, default=self._json_dumps_default)
        res = self._do_request(
            "PUT", url_path, data=data, headers=headers, cookies=self._cookies, timeout=self._timeout
        )
        self._log_request(res, body=json)
        return res

    def _do_request(self, method: str, url_path: str, **kwargs):
        if not url_path.startswith("/"):
            raise ValueError("URL path must start with '/'")
        full_url = self._base_url + url_path
        # Hack to allow running model hosting requests against local emulator
        if os.getenv("USE_MODEL_HOSTING_EMULATOR") == "1":
            full_url = self._model_hosting_emulator_url_converter(full_url)

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
        if not self._status_is_valid(res.status_code):
            self._raise_API_error(res)
        return res

    def _retrieve(self, cls, resource_path: str, id: int, params: Dict = None, headers: Dict = None):
        return cls._load(
            self._get(url_path=resource_path + "/{}".format(id), params=params, headers=headers).json()["data"][
                "items"
            ][0]
        )

    def _retrieve_multiple(
        self,
        cls,
        resource_path: str,
        wrap_ids: bool,
        ids: Union[List[int], int] = None,
        external_ids: Union[List[str], str] = None,
        headers: Dict = None,
    ):
        all_ids, is_single_identifier = self._process_ids(ids, external_ids, wrap_ids=wrap_ids)
        res = self._post(url_path=resource_path + "/byids", json={"items": all_ids}, headers=headers).json()["data"][
            "items"
        ]
        if is_single_identifier:
            return cls._RESOURCE._load(res[0])
        return cls._load(res)

    def _list_generator(
        self,
        cls,
        resource_path: str,
        method: str,
        limit: int = None,
        chunk: int = None,
        filter: Dict = None,
        headers: Dict = None,
    ):
        total_items_retrieved = 0
        current_limit = self._LIMIT
        if chunk:
            assert chunk <= self._LIMIT, "Chunk size can not exceed {}".format(self._LIMIT)
            current_limit = chunk
        next_cursor = None
        filter = filter or {}
        while True:
            if limit:
                num_of_remaining_items = limit - total_items_retrieved
                if num_of_remaining_items < self._LIMIT:
                    current_limit = num_of_remaining_items

            if method == "GET":
                params = filter.copy()
                params["limit"] = current_limit
                params["cursor"] = next_cursor
                res = self._get(url_path=resource_path, params=params, headers=headers)
            elif method == "POST":
                body = {"filter": filter, "limit": current_limit, "cursor": next_cursor}
                res = self._post(url_path=resource_path + "/list", json=body, headers=headers)
            else:
                raise ValueError("_list parameter `method` must be GET or POST, not %s", method)
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

    def _list(self, cls, resource_path: str, method: str, limit: int = None, filter: Dict = None, headers: Dict = None):
        items = []
        for resource_list in self._list_generator(
            cls=cls,
            resource_path=resource_path,
            method=method,
            limit=limit,
            chunk=self._LIMIT,
            filter=filter,
            headers=headers,
        ):
            items.extend(resource_list._resources)
        return cls(items)

    def _create_multiple(
        self,
        cls: Any,
        resource_path: str,
        items: Union[List[Any], Any],
        params: Dict = None,
        headers: Dict = None,
        limit=None,
    ):
        limit = limit or self._LIMIT
        single_item = not isinstance(items, list)
        if single_item:
            items = [items]

        items_split = []
        for i in range(0, len(items), limit):
            items_split.append({"items": [item.dump(camel_case=True) for item in items[i : i + limit]]})
        tasks = [(resource_path, task_items, params, headers) for task_items in items_split]
        results = utils.execute_tasks_concurrently(self._post, tasks, max_workers=self._max_workers)

        created_resources = []
        for res in results:
            created_resources.extend(res.json()["data"]["items"])

        if single_item:
            return cls._RESOURCE._load(created_resources[0])
        return cls._load(created_resources)

    def _delete_multiple(
        self,
        resource_path: str,
        wrap_ids: bool,
        ids: Union[List[int], int] = None,
        external_ids: Union[List[str], str] = None,
        params: Dict = None,
        headers: Dict = None,
    ):
        all_ids, is_single_identifier = self._process_ids(ids, external_ids, wrap_ids)
        self._post(resource_path + "/delete", json={"items": all_ids}, params=params, headers=headers)

    def _update_multiple(
        self, cls: Any, resource_path: str, items: Union[List[Any], Any], params: Dict = None, headers: Dict = None
    ):
        patch_objects = []
        single_item = not isinstance(items, list)
        if single_item:
            items = [items]

        for item in items:
            if isinstance(item, CogniteResource):
                patch_objects.append(self._convert_resource_to_patch_object(item))
            elif isinstance(item, CogniteUpdate):
                patch_objects.append(item.dump())
            else:
                raise ValueError("update item must be of type CogniteResource or CogniteUpdate")
        res = self._post(
            resource_path + "/update", json={"items": patch_objects}, params=params, headers=headers
        ).json()["data"]["items"]

        if single_item:
            return cls._RESOURCE._load(res[0])
        return cls._load(res)

    def _search(self, cls: Any, resource_path: str, json: Dict, params: Dict = None, headers: Dict = None):
        res = self._post(url_path=resource_path + "/search", json=json, params=params, headers=headers)
        return cls._load(res.json()["data"]["items"])

    @staticmethod
    def _convert_resource_to_patch_object(resource):
        dumped_resource = resource.dump(camel_case=True)
        has_id = "id" in dumped_resource
        has_external_id = "externalId" in dumped_resource
        utils.assert_exactly_one_of_id_or_external_id(dumped_resource.get("id"), dumped_resource.get("externalId"))

        patch_object = {"update": {}}
        if has_id:
            patch_object["id"] = dumped_resource.pop("id")
        elif has_external_id:
            patch_object["externalId"] = dumped_resource.pop("externalId")

        for key, value in dumped_resource.items():
            patch_object["update"][key] = {"set": value}
        return patch_object

    @staticmethod
    def _process_ids(
        ids: Union[List[int], int, None], external_ids: Union[List[str], str, None], wrap_ids: bool
    ) -> Tuple[List, bool]:
        if not external_ids and not ids:
            raise ValueError("No ids specified")
        if external_ids and not wrap_ids:
            raise ValueError("externalIds must be wrapped")

        single_id = False
        single_external_id = False

        if isinstance(ids, int):
            ids = [ids]
            single_id = True
        elif isinstance(ids, List) or ids is None:
            ids = ids or []
        else:
            raise TypeError("ids must be int or list of int")

        if isinstance(external_ids, str):
            external_ids = [external_ids]
            single_external_id = True
        elif isinstance(external_ids, List) or external_ids is None:
            external_ids = external_ids or []
        else:
            raise TypeError("external_ids must be str or list of str")

        if wrap_ids:
            ids = [{"id": id} for id in ids]
            external_ids = [{"externalId": external_id} for external_id in external_ids]

        all_ids = ids + external_ids
        is_single_identifier = (single_id or single_external_id) and len(all_ids) == 1

        return all_ids, is_single_identifier

    @staticmethod
    def _json_dumps_default(x):
        if isinstance(x, numpy.int_):
            return int(x)
        if isinstance(x, numpy.float_):
            return float(x)
        if isinstance(x, numpy.bool_):
            return bool(x)
        return x.__dict__

    @staticmethod
    def _status_is_valid(status_code: int):
        return status_code < 400

    @staticmethod
    def _raise_API_error(res: Response):
        x_request_id = res.headers.get("X-Request-Id")
        code = res.status_code
        extra = {}
        try:
            error = res.json()["error"]
            if isinstance(error, str):
                msg = error
            elif isinstance(error, Dict):
                msg = error["message"]
                extra = error.get("extra", {})
                for key in set(error.keys()) - {"code", "message", "extra"}:
                    extra[key] = error[key]
            else:
                msg = res.content
        except:
            msg = res.content

        log.error("HTTP Error %s: %s", code, msg, extra={"X-Request-ID": x_request_id, "extra": extra})
        raise APIError(msg, code, x_request_id, extra=extra)

    @staticmethod
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

    @staticmethod
    def _model_hosting_emulator_url_converter(url):
        pattern = "https://api.cognitedata.com/api/0.6/projects/(.*)/analytics/models(.*)"
        res = re.match(pattern, url)
        if res is not None:
            project = res.group(1)
            path = res.group(2)
            return "http://localhost:8000/api/0.1/projects/{}/models{}".format(project, path)
        return url
