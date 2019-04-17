import gzip
import json as _json
import logging
import os
import re
from typing import Any, Dict, List, Union

import requests.utils
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from urllib3 import Retry

from cognite.client._utils import utils
from cognite.client._utils.base import CogniteResource, CogniteUpdate
from cognite.client.exceptions import CogniteAPIError

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
        api_key: str = None,
        base_url: str = None,
        max_workers: int = None,
        cookies: Dict = None,
        headers: Dict = None,
        timeout: int = None,
        cognite_client=None,
    ):
        self._request_session = _REQUESTS_SESSION
        self._project = project
        self._api_key = api_key
        __base_path = "/api/{}/projects/{}".format(version, project) if version else ""
        self._base_url = base_url + __base_path
        self._max_workers = max_workers
        self._cookies = cookies
        self._headers = self._configure_headers(headers)
        self._timeout = timeout
        self._cognite_client = cognite_client

    def _delete(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request(
            "DELETE", url_path, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )

    def _get(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request(
            "GET", url_path, params=params, headers=headers, cookies=self._cookies, timeout=self._timeout
        )

    def _post(self, url_path: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request(
            "POST", url_path, json=json, headers=headers, params=params, cookies=self._cookies, timeout=self._timeout
        )

    def _put(self, url_path: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request(
            "PUT", url_path, json=json, headers=headers, cookies=self._cookies, timeout=self._timeout
        )

    def _do_request(self, method: str, url_path: str, **kwargs):
        full_url = self._resolve_url(url_path)

        json_payload = kwargs.get("json")
        headers = self._headers.copy()
        headers.update(kwargs.get("headers") or {})

        if json_payload:
            data = _json.dumps(json_payload, default=utils.json_dump_default)
            kwargs["data"] = data
            if method in ["PUT", "POST"] and not os.getenv("COGNITE_DISABLE_GZIP", False):
                kwargs["data"] = gzip.compress(data.encode())
                headers["Content-Encoding"] = "gzip"

        kwargs["headers"] = headers

        res = self._request_session.request(method=method, url=full_url, **kwargs)

        if not self._status_is_valid(res.status_code):
            self._raise_API_error(res)
        self._log_request(res, payload=json_payload)
        return res

    def _configure_headers(self, additional_headers):
        headers = CaseInsensitiveDict()
        headers.update(requests.utils.default_headers())
        headers["api-key"] = self._api_key
        headers["content-type"] = "application/json"
        headers["accept"] = "application/json"
        if "User-Agent" in headers:
            headers["User-Agent"] += " " + utils.get_user_agent()
        else:
            headers["User-Agent"] = utils.get_user_agent()
        headers.update(additional_headers)
        return headers

    def _resolve_url(self, url_path: str):
        if not url_path.startswith("/"):
            raise ValueError("URL path must start with '/'")
        full_url = self._base_url + url_path
        # Hack to allow running model hosting requests against local emulator
        full_url = self._apply_model_hosting_emulator_url_filter(full_url)
        return full_url

    def _retrieve(self, cls, resource_path: str, id: Union[int, str], params: Dict = None, headers: Dict = None):
        return cls._load(
            self._get(
                url_path=utils.interpolate_and_url_encode(resource_path + "/{}", str(id)),
                params=params,
                headers=headers,
            ).json()["data"]["items"][0],
            cognite_client=self._cognite_client,
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
        all_ids = self._process_ids(ids, external_ids, wrap_ids=wrap_ids)
        res = self._post(url_path=resource_path + "/byids", json={"items": all_ids}, headers=headers).json()["data"][
            "items"
        ]
        if self._is_single_identifier(ids, external_ids):
            return cls._RESOURCE._load(res[0], cognite_client=self._cognite_client)
        return cls._load(res, cognite_client=self._cognite_client)

    def _list_generator(
        self,
        cls,
        resource_path: str,
        method: str,
        limit: int = None,
        chunk_size: int = None,
        filter: Dict = None,
        headers: Dict = None,
    ):
        total_items_retrieved = 0
        current_limit = self._LIMIT
        if chunk_size and chunk_size <= self._LIMIT:
            current_limit = chunk_size
        next_cursor = None
        filter = filter or {}
        current_items = []
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
                raise ValueError("_list_generator parameter `method` must be GET or POST, not %s", method)
            last_received_items = res.json()["data"]["items"]
            current_items.extend(last_received_items)

            if not chunk_size:
                for item in current_items:
                    yield cls._RESOURCE._load(item, cognite_client=self._cognite_client)
                total_items_retrieved += len(current_items)
                current_items = []
            elif len(current_items) >= chunk_size or len(last_received_items) < self._LIMIT:
                items_to_yield = current_items[:chunk_size]
                yield cls._load(items_to_yield, cognite_client=self._cognite_client)
                total_items_retrieved += len(items_to_yield)
                current_items = current_items[chunk_size:]

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
            chunk_size=self._LIMIT,
            filter=filter,
            headers=headers,
        ):
            items.extend(resource_list.data)
        return cls(items, cognite_client=self._cognite_client)

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
            if isinstance(items[i], CogniteResource):
                items_chunk = [item.dump(camel_case=True) for item in items[i : i + limit]]
            else:
                items_chunk = [item for item in items[i : i + limit]]
            items_split.append({"items": items_chunk})

        tasks = [(resource_path, task_items, params, headers) for task_items in items_split]
        results = utils.execute_tasks_concurrently(self._post, tasks, max_workers=self._max_workers)

        created_resources = []
        for res in results:
            created_resources.extend(res.json()["data"]["items"])

        if single_item:
            return cls._RESOURCE._load(created_resources[0], cognite_client=self._cognite_client)
        return cls._load(created_resources, cognite_client=self._cognite_client)

    def _delete_multiple(
        self,
        resource_path: str,
        wrap_ids: bool,
        ids: Union[List[int], int] = None,
        external_ids: Union[List[str], str] = None,
        params: Dict = None,
        headers: Dict = None,
    ):
        all_ids = self._process_ids(ids, external_ids, wrap_ids)
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
                patch_objects.append(self._convert_resource_to_patch_object(item, cls._UPDATE._get_update_properties()))
            elif isinstance(item, CogniteUpdate):
                patch_objects.append(item.dump())
            else:
                raise ValueError("update item must be of type CogniteResource or CogniteUpdate")
        res = self._post(
            resource_path + "/update", json={"items": patch_objects}, params=params, headers=headers
        ).json()["data"]["items"]

        if single_item:
            return cls._RESOURCE._load(res[0], cognite_client=self._cognite_client)
        return cls._load(res, cognite_client=self._cognite_client)

    def _search(self, cls: Any, resource_path: str, json: Dict, params: Dict = None, headers: Dict = None):
        res = self._post(url_path=resource_path + "/search", json=json, params=params, headers=headers)
        return cls._load(res.json()["data"]["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _convert_resource_to_patch_object(resource, update_attributes):
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
            if key in update_attributes:
                patch_object["update"][key] = {"set": value}
        return patch_object

    @staticmethod
    def _process_ids(
        ids: Union[List[int], int, None], external_ids: Union[List[str], str, None], wrap_ids: bool
    ) -> List:
        if external_ids is None and ids is None:
            raise ValueError("No ids specified")
        if external_ids and not wrap_ids:
            raise ValueError("externalIds must be wrapped")

        if isinstance(ids, int):
            ids = [ids]
        elif isinstance(ids, list) or ids is None:
            ids = ids or []
        else:
            raise TypeError("ids must be int or list of int")

        if isinstance(external_ids, str):
            external_ids = [external_ids]
        elif isinstance(external_ids, list) or external_ids is None:
            external_ids = external_ids or []
        else:
            raise TypeError("external_ids must be str or list of str")

        if wrap_ids:
            ids = [{"id": id} for id in ids]
            external_ids = [{"externalId": external_id} for external_id in external_ids]

        all_ids = ids + external_ids

        return all_ids

    @staticmethod
    def _is_single_identifier(ids, external_ids):
        single_id = isinstance(ids, int) and external_ids is None
        single_external_id = isinstance(external_ids, str) and ids is None
        return single_id or single_external_id

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
        raise CogniteAPIError(msg, code, x_request_id, extra=extra)

    @staticmethod
    def _log_request(res: Response, **kwargs):
        method = res.request.method
        url = res.request.url
        status_code = res.status_code

        extra = kwargs.copy()
        extra["headers"] = res.request.headers.copy()
        if "api-key" in extra.get("headers", {}):
            extra["headers"]["api-key"] = None
        if extra["payload"] is None:
            del extra["payload"]

        http_protocol_version = ".".join(list(str(res.raw.version)))

        log.info("HTTP/{} {} {} {}".format(http_protocol_version, method, url, status_code), extra=extra)

    def _apply_model_hosting_emulator_url_filter(self, full_url):
        mlh_emul_url = os.getenv("MODEL_HOSTING_EMULATOR_URL")
        if mlh_emul_url is not None:
            pattern = "{}/analytics/models(.*)".format(self._base_url)
            res = re.match(pattern, full_url)
            if res is not None:
                path = res.group(1)
                return "{}/projects/{}/models{}".format(mlh_emul_url, self._project, path)
        return full_url
