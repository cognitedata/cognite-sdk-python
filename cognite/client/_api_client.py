import gzip
import json as _json
import logging
import os
import re
from typing import Any, Dict, List, Union

import requests.utils
from requests import Response, Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry
from requests.structures import CaseInsensitiveDict

from cognite.client import utils
from cognite.client.data_classes._base import CogniteFilter, CogniteResource, CogniteUpdate
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

log = logging.getLogger("cognite-sdk")


class RetryWithMaxBackoff(Retry):
    def get_backoff_time(self):
        return min(utils._client_config._DefaultConfig().max_retry_backoff, super().get_backoff_time())


def _init_requests_session():
    session = Session()
    session_with_retry = Session()
    config = utils._client_config._DefaultConfig()
    adapter = HTTPAdapter(
        max_retries=RetryWithMaxBackoff(
            total=config.max_retries,
            connect=config.max_retries,
            read=0,
            status=0,
            backoff_factor=0.5,
            raise_on_status=False,
        ),
        pool_maxsize=config.max_connection_pool_size,
    )
    adapter_with_retry = HTTPAdapter(
        max_retries=RetryWithMaxBackoff(
            total=config.max_retries,
            backoff_factor=0.5,
            status_forcelist=config.status_forcelist,
            method_whitelist=False,
            raise_on_status=False,
        ),
        pool_maxsize=config.max_connection_pool_size,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session_with_retry.mount("http://", adapter_with_retry)
    session_with_retry.mount("https://", adapter_with_retry)

    if config.disable_ssl:
        import urllib3

        urllib3.disable_warnings()
        session.verify = False
        session_with_retry.verify = False

    return session, session_with_retry


_REQUESTS_SESSION, _REQUESTS_SESSION_WITH_RETRY = _init_requests_session()


class APIClient:
    _RESOURCE_PATH = None
    _LIST_CLASS = None

    def __init__(self, config: utils._client_config.ClientConfig, api_version: str = None, cognite_client=None):
        self._request_session = _REQUESTS_SESSION
        self._request_session_with_retry = _REQUESTS_SESSION_WITH_RETRY

        self._config = config
        self._api_version = api_version
        self._cognite_client = cognite_client

        self._CREATE_LIMIT = 1000
        self._LIST_LIMIT = 1000
        self._RETRIEVE_LIMIT = 1000
        self._DELETE_LIMIT = 1000
        self._UPDATE_LIMIT = 1000

    def _delete(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request("DELETE", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _get(self, url_path: str, params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request("GET", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _post(self, url_path: str, json: Dict[str, Any], params: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request(
            "POST", url_path, json=json, headers=headers, params=params, timeout=self._config.timeout
        )

    def _put(self, url_path: str, json: Dict[str, Any] = None, headers: Dict[str, Any] = None):
        return self._do_request("PUT", url_path, json=json, headers=headers, timeout=self._config.timeout)

    def _do_request(self, method: str, url_path: str, **kwargs):
        is_retryable, full_url = self._resolve_url(method, url_path)

        json_payload = kwargs.get("json")
        headers = self._configure_headers(self._config.headers.copy())
        headers.update(kwargs.get("headers") or {})

        if json_payload:
            data = _json.dumps(json_payload, default=utils._auxiliary.json_dump_default)
            kwargs["data"] = data
            if method in ["PUT", "POST"] and not os.getenv("COGNITE_DISABLE_GZIP", False):
                kwargs["data"] = gzip.compress(data.encode())
                headers["Content-Encoding"] = "gzip"

        kwargs["headers"] = headers

        if is_retryable:
            res = self._request_session_with_retry.request(method=method, url=full_url, **kwargs)
        else:
            res = self._request_session.request(method=method, url=full_url, **kwargs)

        if not self._status_is_valid(res.status_code):
            self._raise_API_error(res, payload=json_payload)
        self._log_request(res, payload=json_payload)
        return res

    def _configure_headers(self, additional_headers):
        headers = CaseInsensitiveDict()
        headers.update(requests.utils.default_headers())
        headers["api-key"] = self._config.api_key
        headers["content-type"] = "application/json"
        headers["accept"] = "application/json"
        headers["x-cdp-sdk"] = "CognitePythonSDK:{}".format(utils._auxiliary.get_current_sdk_version())
        headers["x-cdp-app"] = self._config.client_name
        if "User-Agent" in headers:
            headers["User-Agent"] += " " + utils._auxiliary.get_user_agent()
        else:
            headers["User-Agent"] = utils._auxiliary.get_user_agent()
        headers.update(additional_headers)
        return headers

    def _resolve_url(self, method: str, url_path: str):
        if not url_path.startswith("/"):
            raise ValueError("URL path must start with '/'")
        base_url = self._get_base_url_with_base_path()
        full_url = base_url + url_path
        is_retryable = self._is_retryable(method, full_url)
        # Hack to allow running model hosting requests against local emulator
        full_url = self._apply_model_hosting_emulator_url_filter(full_url)
        return is_retryable, full_url

    def _get_base_url_with_base_path(self):
        base_path = "/api/{}/projects/{}".format(self._api_version, self._config.project) if self._api_version else ""
        return self._config.base_url + base_path

    def _is_retryable(self, method, path):
        valid_methods = ["GET", "POST", "PUT", "DELETE"]
        match = re.match("(?:http|https)://[a-z\d.:]+(?:/api/v1/projects/[^/]+)?(/.+)", path)

        if not match:
            raise ValueError("Path {} is not valid. Cannot resolve whether or not it is retryable".format(path))
        if method not in valid_methods:
            raise ValueError("Method {} is not valid. Must be one of {}".format(method, valid_methods))
        path_end = match.group(1)
        # TODO: This following set should be generated from the openapi spec somehow.
        retryable_post_endpoints = {
            "/assets/list",
            "/assets/byids",
            "/assets/search",
            "/events/list",
            "/events/byids",
            "/events/search",
            "/files/list",
            "/files/byids",
            "/files/search",
            "/files/initupload",
            "/files/downloadlink",
            "/timeseries/byids",
            "/timeseries/search",
            "/timeseries/data",
            "/timeseries/data/list",
            "/timeseries/data/latest",
            "/timeseries/data/delete",
        }
        if method == "GET":
            return True
        if method == "POST" and path_end in retryable_post_endpoints:
            return True
        return False

    def _retrieve(
        self, id: Union[int, str], cls=None, resource_path: str = None, params: Dict = None, headers: Dict = None
    ):
        cls = cls or self._LIST_CLASS._RESOURCE
        resource_path = resource_path or self._RESOURCE_PATH
        try:
            res = self._get(
                url_path=utils._auxiliary.interpolate_and_url_encode(resource_path + "/{}", str(id)),
                params=params,
                headers=headers,
            )
            return cls._load(res.json(), cognite_client=self._cognite_client)
        except CogniteAPIError as e:
            if e.code != 404:
                raise

    def _retrieve_multiple(
        self,
        wrap_ids: bool,
        cls=None,
        resource_path: str = None,
        ids: Union[List[int], int] = None,
        external_ids: Union[List[str], str] = None,
        headers: Dict = None,
    ):
        cls = cls or self._LIST_CLASS
        resource_path = resource_path or self._RESOURCE_PATH
        all_ids = self._process_ids(ids, external_ids, wrap_ids=wrap_ids)
        id_chunks = utils._auxiliary.split_into_chunks(all_ids, self._RETRIEVE_LIMIT)

        tasks = [
            {"url_path": resource_path + "/byids", "json": {"items": id_chunk}, "headers": headers}
            for id_chunk in id_chunks
        ]
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self._post, tasks, max_workers=self._config.max_workers
        )

        if tasks_summary.exceptions:
            try:
                utils._concurrency.collect_exc_info_and_raise(tasks_summary.exceptions)
            except CogniteNotFoundError:
                if self._is_single_identifier(ids, external_ids):
                    return None
                raise

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        if self._is_single_identifier(ids, external_ids):
            return cls._RESOURCE._load(retrieved_items[0], cognite_client=self._cognite_client)
        return cls._load(retrieved_items, cognite_client=self._cognite_client)

    def _list_generator(
        self,
        method: str,
        cls=None,
        resource_path: str = None,
        limit: int = None,
        chunk_size: int = None,
        filter: Dict = None,
        headers: Dict = None,
    ):
        if limit == -1 or limit == float("inf"):
            limit = None
        cls = cls or self._LIST_CLASS
        resource_path = resource_path or self._RESOURCE_PATH
        total_items_retrieved = 0
        current_limit = self._LIST_LIMIT
        if chunk_size and chunk_size <= self._LIST_LIMIT:
            current_limit = chunk_size
        next_cursor = None
        filter = filter or {}
        current_items = []
        while True:
            if limit:
                num_of_remaining_items = limit - total_items_retrieved
                if num_of_remaining_items < self._LIST_LIMIT:
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
            last_received_items = res.json()["items"]
            current_items.extend(last_received_items)

            if not chunk_size:
                for item in current_items:
                    yield cls._RESOURCE._load(item, cognite_client=self._cognite_client)
                total_items_retrieved += len(current_items)
                current_items = []
            elif len(current_items) >= chunk_size or len(last_received_items) < self._LIST_LIMIT:
                items_to_yield = current_items[:chunk_size]
                yield cls._load(items_to_yield, cognite_client=self._cognite_client)
                total_items_retrieved += len(items_to_yield)
                current_items = current_items[chunk_size:]

            next_cursor = res.json().get("nextCursor")
            if total_items_retrieved == limit or next_cursor is None:
                break

    def _list(
        self,
        method: str,
        cls=None,
        resource_path: str = None,
        limit: int = None,
        filter: Dict = None,
        headers: Dict = None,
    ):
        cls = cls or self._LIST_CLASS
        resource_path = resource_path or self._RESOURCE_PATH
        items = []
        for resource_list in self._list_generator(
            cls=cls,
            resource_path=resource_path,
            method=method,
            limit=limit,
            chunk_size=self._LIST_LIMIT,
            filter=filter,
            headers=headers,
        ):
            items.extend(resource_list.data)
        return cls(items, cognite_client=self._cognite_client)

    def _create_multiple(
        self,
        items: Union[List[Any], Any],
        cls: Any = None,
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
        limit=None,
    ):
        cls = cls or self._LIST_CLASS
        resource_path = resource_path or self._RESOURCE_PATH
        limit = limit or self._CREATE_LIMIT
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
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)

        def unwrap_element(el):
            if isinstance(el, dict):
                return cls._RESOURCE._load(el)
            else:
                return el

        def str_format_element(el):
            if isinstance(el, CogniteResource):
                dumped = el.dump()
                if "external_id" in dumped:
                    return dumped["external_id"]
                return dumped
            return el

        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1]["items"],
            task_list_element_unwrap_fn=unwrap_element,
            str_format_element_fn=str_format_element,
        )
        created_resources = summary.joined_results(lambda res: res.json()["items"])

        if single_item:
            return cls._RESOURCE._load(created_resources[0], cognite_client=self._cognite_client)
        return cls._load(created_resources, cognite_client=self._cognite_client)

    def _delete_multiple(
        self,
        wrap_ids: bool,
        resource_path: str = None,
        ids: Union[List[int], int] = None,
        external_ids: Union[List[str], str] = None,
        params: Dict = None,
        headers: Dict = None,
    ):
        resource_path = resource_path or self._RESOURCE_PATH
        all_ids = self._process_ids(ids, external_ids, wrap_ids)
        id_chunks = utils._auxiliary.split_into_chunks(all_ids, self._DELETE_LIMIT)
        tasks = [
            {"url_path": resource_path + "/delete", "json": {"items": chunk}, "params": params, "headers": headers}
            for chunk in id_chunks
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"],
            task_list_element_unwrap_fn=utils._auxiliary.unwrap_identifer,
        )

    def _update_multiple(
        self,
        items: Union[List[Any], Any],
        cls: Any = None,
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
    ):
        cls = cls or self._LIST_CLASS
        resource_path = resource_path or self._RESOURCE_PATH
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
        patch_object_chunks = utils._auxiliary.split_into_chunks(patch_objects, self._UPDATE_LIMIT)

        tasks = [
            {"url_path": resource_path + "/update", "json": {"items": chunk}, "params": params, "headers": headers}
            for chunk in patch_object_chunks
        ]

        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self._post, tasks, max_workers=self._config.max_workers
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"],
            task_list_element_unwrap_fn=lambda el: utils._auxiliary.unwrap_identifer(el),
        )
        updated_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        if single_item:
            return cls._RESOURCE._load(updated_items[0], cognite_client=self._cognite_client)
        return cls._load(updated_items, cognite_client=self._cognite_client)

    def _search(
        self,
        search: Dict,
        filter: Union[Dict, CogniteFilter],
        limit: int,
        cls: Any = None,
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
    ):
        utils._auxiliary.assert_type(filter, "filter", [dict, CogniteFilter], allow_none=True)
        if isinstance(filter, CogniteFilter):
            filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            filter = utils._auxiliary.convert_all_keys_to_camel_case(filter)
        cls = cls or self._LIST_CLASS
        resource_path = resource_path or self._RESOURCE_PATH
        res = self._post(
            url_path=resource_path + "/search",
            json={"search": search, "filter": filter, "limit": limit},
            params=params,
            headers=headers,
        )
        return cls._load(res.json()["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _convert_resource_to_patch_object(resource, update_attributes):
        dumped_resource = resource.dump(camel_case=True)
        has_id = "id" in dumped_resource
        has_external_id = "externalId" in dumped_resource

        patch_object = {"update": {}}
        if has_id:
            patch_object["id"] = dumped_resource.pop("id")
        elif has_external_id:
            patch_object["externalId"] = dumped_resource.pop("externalId")

        for key, value in dumped_resource.items():
            if utils._auxiliary.to_snake_case(key) in update_attributes:
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
    def _raise_API_error(res: Response, payload: Dict):
        x_request_id = res.headers.get("X-Request-Id")
        code = res.status_code
        missing = None
        duplicated = None
        try:
            error = res.json()["error"]
            if isinstance(error, str):
                msg = error
            elif isinstance(error, Dict):
                msg = error["message"]
                missing = error.get("missing")
                duplicated = error.get("duplicated")
            else:
                msg = res.content
        except:
            msg = res.content

        error_details = {"X-Request-ID": x_request_id}
        if payload:
            error_details["payload"] = payload
        if missing:
            error_details["missing"] = missing
        if duplicated:
            error_details["duplicated"] = duplicated

        log.debug("HTTP Error %s %s %s: %s", code, res.request.method, res.request.url, msg, extra=error_details)
        raise CogniteAPIError(msg, code, x_request_id, missing=missing, duplicated=duplicated)

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
            pattern = "{}/analytics/models(.*)".format(self._get_base_url_with_base_path())
            res = re.match(pattern, full_url)
            if res is not None:
                path = res.group(1)
                return "{}/projects/{}/models{}".format(mlh_emul_url, self._config.project, path)
        return full_url
