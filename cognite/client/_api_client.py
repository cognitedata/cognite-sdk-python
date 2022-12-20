import functools
import gzip
import json as _json
import logging
import re
from collections import UserList
from json.decoder import JSONDecodeError
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Iterator,
    List,
    Literal,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)
from urllib.parse import urljoin

import requests.utils
from requests import Response
from requests.structures import CaseInsensitiveDict

from cognite.client import utils
from cognite.client._http_client import HTTPClient, HTTPClientConfig, get_global_requests_session
from cognite.client.config import ClientConfig, global_config
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteUpdate,
    T_CogniteResource,
    T_CogniteResourceList,
)
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._identifier import Identifier, IdentifierSequence, SingletonIdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient

log = logging.getLogger("cognite-sdk")

T = TypeVar("T")


class APIClient:
    _RESOURCE_PATH: str

    # TODO: This following set should be generated from the openapi spec somehow.
    _RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS = {
        rf"^{path}(\?.*)?$"
        for path in (
            "/(assets|events|files|timeseries|sequences|datasets|relationships)/(list|byids|search|aggregate)",
            "/files/downloadlink",
            "/timeseries/data",
            "/timeseries/data/(list|latest|delete)",
            "/sequences/data",
            "/sequences/data/(list|delete)",
            "/raw/dbs/[^/]+/tables/[^/]+",
            "/context/entitymatching/(byids|list|jobs)",
            "/sessions/revoke",
        )
    }

    def __init__(
        self, config: ClientConfig, api_version: Optional[str] = None, cognite_client: "CogniteClient" = None
    ) -> None:
        self._config = config
        self._api_version = api_version
        self._api_subversion = config.api_subversion
        self._cognite_client = cognite_client

        session = get_global_requests_session()

        self._http_client = HTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry={429},
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=0,
                max_retries_connect=global_config.max_retries,
                max_retries_status=global_config.max_retries,
            ),
            session=session,
        )

        self._http_client_with_retry = HTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry=global_config.status_forcelist,
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=global_config.max_retries,
                max_retries_connect=global_config.max_retries,
                max_retries_status=global_config.max_retries,
            ),
            session=session,
        )

        self._CREATE_LIMIT = 1000
        self._LIST_LIMIT = 1000
        self._RETRIEVE_LIMIT = 1000
        self._DELETE_LIMIT = 1000
        self._UPDATE_LIMIT = 1000

    def _delete(
        self, url_path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, Any]] = None
    ) -> Response:
        return self._do_request("DELETE", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _get(
        self, url_path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, Any]] = None
    ) -> Response:
        return self._do_request("GET", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _post(
        self,
        url_path: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Response:
        return self._do_request(
            "POST", url_path, json=json, headers=headers, params=params, timeout=self._config.timeout
        )

    def _put(
        self, url_path: str, json: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, Any]] = None
    ) -> Response:
        return self._do_request("PUT", url_path, json=json, headers=headers, timeout=self._config.timeout)

    def _do_request(self, method: str, url_path: str, accept: str = "application/json", **kwargs: Any) -> Response:
        is_retryable, full_url = self._resolve_url(method, url_path)

        json_payload = kwargs.get("json")
        headers = self._configure_headers(accept, additional_headers=self._config.headers.copy())
        headers.update(kwargs.get("headers") or {})

        if json_payload:
            try:
                data = _json.dumps(json_payload, default=utils._auxiliary.json_dump_default, allow_nan=False)
            except ValueError as e:
                # A lot of work to give a more human friendly error message when nans and infs are present:
                msg = "Out of range float values are not JSON compliant"
                if msg in str(e):  # exc. might e.g. contain an extra ": nan", depending on build (_json.make_encoder)
                    raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                        e.__traceback__
                    ) from None
                raise
            kwargs["data"] = data
            if method in ["PUT", "POST"] and not global_config.disable_gzip:
                kwargs["data"] = gzip.compress(data.encode())
                headers["Content-Encoding"] = "gzip"

        kwargs["headers"] = headers

        # requests will by default follow redirects. This can be an SSRF-hazard if
        # the client can be tricked to request something with an open redirect, in
        # addition to leaking the token, as requests will send the headers to the
        # redirected-to endpoint.
        # If redirects are to be followed in a call, this should be opted into instead.
        kwargs.setdefault("allow_redirects", False)

        if is_retryable:
            res = self._http_client_with_retry.request(method=method, url=full_url, **kwargs)
        else:
            res = self._http_client.request(method=method, url=full_url, **kwargs)

        if not self._status_ok(res.status_code):
            self._raise_API_error(res, payload=json_payload)
        stream = kwargs.get("stream")
        self._log_request(res, payload=json_payload, stream=stream)
        return res

    def _configure_headers(self, accept: str, additional_headers: Dict[str, str]) -> MutableMapping[str, Any]:
        headers: MutableMapping[str, Any] = CaseInsensitiveDict()
        headers.update(requests.utils.default_headers())
        auth_header_name, auth_header_value = self._config.credentials.authorization_header()
        headers[auth_header_name] = auth_header_value
        headers["content-type"] = "application/json"
        headers["accept"] = accept
        headers["x-cdp-sdk"] = "CognitePythonSDK:{}".format(utils._auxiliary.get_current_sdk_version())
        headers["x-cdp-app"] = self._config.client_name
        headers["cdf-version"] = self._api_subversion
        if "User-Agent" in headers:
            headers["User-Agent"] += " " + utils._auxiliary.get_user_agent()
        else:
            headers["User-Agent"] = utils._auxiliary.get_user_agent()
        headers.update(additional_headers)
        return headers

    def _resolve_url(self, method: str, url_path: str) -> Tuple[bool, str]:
        if not url_path.startswith("/"):
            raise ValueError("URL path must start with '/'")
        base_url = self._get_base_url_with_base_path()
        full_url = base_url + url_path
        is_retryable = self._is_retryable(method, full_url)
        return is_retryable, full_url

    def _get_base_url_with_base_path(self) -> str:
        base_path = "/api/{}/projects/{}".format(self._api_version, self._config.project) if self._api_version else ""
        return urljoin(self._config.base_url, base_path)

    def _is_retryable(self, method: str, path: str) -> bool:
        valid_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]

        if method not in valid_methods:
            raise ValueError("Method {} is not valid. Must be one of {}".format(method, valid_methods))

        if method in ["GET", "PUT", "PATCH"]:
            return True
        if method == "POST" and self._url_is_retryable(path):
            return True
        return False

    @classmethod
    @functools.lru_cache(64)
    def _url_is_retryable(cls, url: str) -> bool:
        valid_url_pattern = (
            r"^(?:http|https)://[a-z\d.:\-]+(?:/api/(?:v1|playground)/projects/[^/]+)?((/[^\?]+)?(\?.+)?)"
        )
        match = re.match(valid_url_pattern, url)
        if not match:
            raise ValueError("URL {} is not valid. Cannot resolve whether or not it is retryable".format(url))
        path = match.group(1)
        for pattern in cls._RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS:
            if re.match(pattern, path):
                return True
        return False

    def _retrieve(
        self,
        identifier: Identifier,
        cls: Type[T_CogniteResource],
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
    ) -> Optional[T_CogniteResource]:
        resource_path = resource_path or self._RESOURCE_PATH
        try:
            res = self._get(
                url_path=utils._auxiliary.interpolate_and_url_encode(
                    resource_path + "/{}", str(identifier.as_primitive())
                ),
                params=params,
                headers=headers,
            )
            return cls._load(res.json(), cognite_client=self._cognite_client)
        except CogniteAPIError as e:
            if e.code != 404:
                raise
        return None

    @overload
    def _retrieve_multiple(
        self,
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        identifiers: SingletonIdentifierSequence,
        resource_path: Optional[str] = None,
        ignore_unknown_ids: Optional[bool] = None,
        headers: Optional[Dict[str, Any]] = None,
        other_params: Optional[Dict[str, Any]] = None,
    ) -> Optional[T_CogniteResource]:
        ...

    @overload
    def _retrieve_multiple(
        self,
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        identifiers: IdentifierSequence,
        resource_path: Optional[str] = None,
        ignore_unknown_ids: Optional[bool] = None,
        headers: Optional[Dict[str, Any]] = None,
        other_params: Optional[Dict[str, Any]] = None,
    ) -> T_CogniteResourceList:
        ...

    def _retrieve_multiple(
        self,
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        identifiers: Union[SingletonIdentifierSequence, IdentifierSequence],
        resource_path: Optional[str] = None,
        ignore_unknown_ids: Optional[bool] = None,
        headers: Optional[Dict[str, Any]] = None,
        other_params: Optional[Dict[str, Any]] = None,
    ) -> Union[T_CogniteResourceList, Optional[T_CogniteResource]]:
        resource_path = resource_path or self._RESOURCE_PATH

        ignore_unknown_obj = {} if ignore_unknown_ids is None else {"ignoreUnknownIds": ignore_unknown_ids}
        tasks: List[Dict[str, Union[str, Dict[str, Any], None]]] = [
            {
                "url_path": resource_path + "/byids",
                "json": {
                    "items": id_chunk.as_dicts(),
                    **ignore_unknown_obj,
                    **(other_params or {}),
                },
                "headers": headers,
            }
            for id_chunk in identifiers.chunked(self._RETRIEVE_LIMIT)
        ]
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self._post, tasks, max_workers=self._config.max_workers
        )

        if tasks_summary.exceptions:
            try:
                utils._concurrency.collect_exc_info_and_raise(tasks_summary.exceptions)
            except CogniteNotFoundError:
                if identifiers.is_singleton():
                    return None
                raise

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        if identifiers.is_singleton():
            return resource_cls._load(retrieved_items[0], cognite_client=self._cognite_client)
        return list_cls._load(retrieved_items, cognite_client=self._cognite_client)

    def _list_generator(
        self,
        method: str,
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        resource_path: Optional[str] = None,
        url_path: Optional[str] = None,
        limit: Optional[int] = None,
        chunk_size: Optional[int] = None,
        filter: Optional[Dict[str, Any]] = None,
        sort: Optional[Sequence[str]] = None,
        other_params: Optional[Dict[str, Any]] = None,
        partitions: Optional[int] = None,
        headers: Optional[Dict[str, Any]] = None,
        initial_cursor: Optional[str] = None,
    ) -> Union[Iterator[T_CogniteResourceList], Iterator[T_CogniteResource]]:
        if limit == -1 or limit == float("inf"):
            limit = None
        resource_path = resource_path or self._RESOURCE_PATH

        if partitions:
            if limit is not None:
                raise ValueError("When using partitions, limit should be `None`, `-1` or `inf`.")
            if sort is not None:
                raise ValueError("When using sort, partitions is not supported.")

            yield from self._list_generator_partitioned(
                partitions=partitions,
                resource_cls=resource_cls,
                resource_path=resource_path,
                filter=filter,
                other_params=other_params,
                headers=headers,
            )

        else:
            total_items_retrieved = 0
            current_limit = self._LIST_LIMIT
            if chunk_size and chunk_size <= self._LIST_LIMIT:
                current_limit = chunk_size
            next_cursor = initial_cursor
            filter = filter or {}
            current_items = []
            while True:
                if limit:
                    num_of_remaining_items = limit - total_items_retrieved
                    if num_of_remaining_items < current_limit:
                        current_limit = num_of_remaining_items

                if method == "GET":
                    params = filter.copy()
                    params["limit"] = current_limit
                    params["cursor"] = next_cursor
                    if sort is not None:
                        params["sort"] = sort
                    res = self._get(url_path=url_path or resource_path, params=params, headers=headers)
                elif method == "POST":
                    body = {"filter": filter, "limit": current_limit, "cursor": next_cursor, **(other_params or {})}
                    if sort is not None:
                        body["sort"] = sort
                    res = self._post(url_path=url_path or resource_path + "/list", json=body, headers=headers)
                else:
                    raise ValueError("_list_generator parameter `method` must be GET or POST, not {}".format(method))
                last_received_items = res.json()["items"]
                total_items_retrieved += len(last_received_items)

                if not chunk_size:
                    for item in last_received_items:
                        yield resource_cls._load(item, cognite_client=self._cognite_client)
                else:
                    current_items.extend(last_received_items)
                    if len(current_items) >= chunk_size:
                        items_to_yield = current_items[:chunk_size]
                        current_items = current_items[chunk_size:]
                        yield list_cls._load(items_to_yield, cognite_client=self._cognite_client)

                next_cursor = res.json().get("nextCursor")
                if total_items_retrieved == limit or next_cursor is None:
                    if chunk_size and current_items:
                        yield list_cls._load(current_items, cognite_client=self._cognite_client)
                    break

    def _list_generator_partitioned(
        self,
        partitions: int,
        resource_cls: Type[T_CogniteResource],
        resource_path: str,
        filter: Optional[Dict] = None,
        other_params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> Iterator[T_CogniteResource]:
        next_cursors = {i + 1: None for i in range(partitions)}

        def get_partition(partition_num: int) -> List[Dict[str, Any]]:
            next_cursor = next_cursors[partition_num]

            body = {
                "filter": filter or {},
                "limit": self._LIST_LIMIT,
                "cursor": next_cursor,
                "partition": "{}/{}".format(partition_num, partitions),
                **(other_params or {}),
            }
            res = self._post(url_path=resource_path + "/list", json=body, headers=headers)
            next_cursor = res.json().get("nextCursor")
            next_cursors[partition_num] = next_cursor

            return res.json()["items"]

        while len(next_cursors) > 0:
            tasks_summary = utils._concurrency.execute_tasks_concurrently(
                get_partition, [(partition,) for partition in next_cursors], max_workers=partitions
            )
            if tasks_summary.exceptions:
                raise tasks_summary.exceptions[0]

            for item in tasks_summary.joined_results():
                yield resource_cls._load(item, cognite_client=self._cognite_client)

            # Remove None from cursor dict
            next_cursors = {partition: next_cursors[partition] for partition in next_cursors if next_cursors[partition]}

    def _list(
        self,
        method: Literal["POST", "GET"],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        resource_path: Optional[str] = None,
        url_path: Optional[str] = None,
        limit: Optional[int] = None,
        filter: Optional[Dict] = None,
        other_params: Optional[Dict] = None,
        partitions: Optional[int] = None,
        sort: Optional[Sequence[str]] = None,
        headers: Optional[Dict] = None,
        initial_cursor: Optional[str] = None,
    ) -> T_CogniteResourceList:
        if partitions:
            if limit not in [None, -1, float("inf")]:
                raise ValueError("When using partitions, limit should be `None`, `-1` or `inf`.")
            if sort is not None:
                raise ValueError("When using sort, partitions is not supported.")
            return self._list_partitioned(
                partitions=partitions,
                method=method,
                list_cls=list_cls,
                resource_path=resource_path,
                filter=filter,
                other_params=other_params,
                headers=headers,
            )

        resource_path = resource_path or self._RESOURCE_PATH
        items: List[T_CogniteResource] = []
        for resource_list in self._list_generator(
            list_cls=list_cls,
            resource_cls=resource_cls,
            resource_path=resource_path,
            url_path=url_path,
            method=method,
            limit=limit,
            chunk_size=self._LIST_LIMIT,
            filter=filter,
            sort=sort,
            other_params=other_params,
            headers=headers,
            initial_cursor=initial_cursor,
        ):
            items.extend(resource_list.data)
        return list_cls(items, cognite_client=self._cognite_client)

    def _list_partitioned(
        self,
        partitions: int,
        method: Literal["POST", "GET"],
        list_cls: Type[T_CogniteResourceList],
        resource_path: Optional[str] = None,
        filter: Optional[Dict] = None,
        other_params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> T_CogniteResourceList:
        def get_partition(partition: int) -> List[Dict[str, Any]]:
            next_cursor = None
            retrieved_items = []
            while True:
                if method == "POST":
                    body = {
                        "filter": filter or {},
                        "limit": self._LIST_LIMIT,
                        "cursor": next_cursor,
                        "partition": partition,
                        **(other_params or {}),
                    }
                    res = self._post(
                        url_path=(resource_path or self._RESOURCE_PATH) + "/list", json=body, headers=headers
                    )
                elif method == "GET":
                    params = {
                        **(filter or {}),
                        "limit": self._LIST_LIMIT,
                        "cursor": next_cursor,
                        "partition": partition,
                        **(other_params or {}),
                    }
                    res = self._get(url_path=(resource_path or self._RESOURCE_PATH), params=params, headers=headers)
                else:
                    raise ValueError(f"Unsupported method: {method}")
                retrieved_items.extend(res.json()["items"])
                next_cursor = res.json().get("nextCursor")
                if next_cursor is None:
                    break
            return retrieved_items

        tasks = [("{}/{}".format(i + 1, partitions),) for i in range(partitions)]
        tasks_summary = utils._concurrency.execute_tasks_concurrently(get_partition, tasks, max_workers=partitions)
        if tasks_summary.exceptions:
            raise tasks_summary.exceptions[0]
        return list_cls._load(tasks_summary.joined_results(), cognite_client=self._cognite_client)

    def _aggregate(
        self,
        cls: Type[T],
        resource_path: Optional[str] = None,
        filter: Optional[Union[CogniteFilter, Dict]] = None,
        aggregate: Optional[str] = None,
        fields: Optional[Sequence[str]] = None,
        headers: Optional[Dict] = None,
    ) -> List[T]:
        utils._auxiliary.assert_type(filter, "filter", [dict, CogniteFilter], allow_none=True)
        utils._auxiliary.assert_type(fields, "fields", [list], allow_none=True)
        if isinstance(filter, CogniteFilter):
            dumped_filter = filter.dump(camel_case=True)
        elif isinstance(filter, Dict):
            dumped_filter = utils._auxiliary.convert_all_keys_to_camel_case(filter)
        else:
            dumped_filter = {}
        resource_path = resource_path or self._RESOURCE_PATH
        body: Dict[str, Any] = {"filter": dumped_filter}
        if aggregate is not None:
            body["aggregate"] = aggregate
        if fields is not None:
            body["fields"] = fields
        res = self._post(url_path=resource_path + "/aggregate", json=body, headers=headers)
        return [cls(**agg) for agg in res.json()["items"]]

    @overload
    def _create_multiple(
        self,
        items: Union[Sequence[T_CogniteResource], Sequence[Dict[str, Any]]],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        resource_path: Optional[str] = None,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        extra_body_fields: Optional[Dict] = None,
        limit: Optional[int] = None,
    ) -> T_CogniteResourceList:
        ...

    @overload
    def _create_multiple(
        self,
        items: Union[T_CogniteResource, Dict[str, Any]],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        resource_path: Optional[str] = None,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        extra_body_fields: Optional[Dict] = None,
        limit: Optional[int] = None,
    ) -> T_CogniteResource:
        ...

    def _create_multiple(
        self,
        items: Union[Sequence[CogniteResource], Sequence[Dict[str, Any]], CogniteResource, Dict[str, Any]],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        resource_path: Optional[str] = None,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        extra_body_fields: Optional[Dict] = None,
        limit: Optional[int] = None,
    ) -> Union[T_CogniteResourceList, T_CogniteResource]:
        resource_path = resource_path or self._RESOURCE_PATH
        limit = limit or self._CREATE_LIMIT
        single_item = not isinstance(items, Sequence)
        if single_item:
            items = cast(Union[Sequence[CogniteResource], Sequence[Dict[str, Any]]], [items])
        else:
            items = cast(Union[Sequence[CogniteResource], Sequence[Dict[str, Any]]], items)

        items_split = []
        for i in range(0, len(items), limit):
            if isinstance(items[i], CogniteResource):
                items = cast(List[CogniteResource], items)
                items_chunk = [item.dump(camel_case=True) for item in items[i : i + limit]]
            else:
                items = cast(List[Dict[str, Any]], items)
                items_chunk = [item for item in items[i : i + limit]]
            items_split.append({"items": items_chunk, **(extra_body_fields or {})})

        tasks = [(resource_path, task_items, params, headers) for task_items in items_split]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)

        def unwrap_element(el: T) -> Union[CogniteResource, T]:
            if isinstance(el, dict):
                return resource_cls._load(el, cognite_client=self._cognite_client)
            else:
                return el

        def str_format_element(el: T) -> Union[str, Dict, T]:
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
            return resource_cls._load(created_resources[0], cognite_client=self._cognite_client)
        return list_cls._load(created_resources, cognite_client=self._cognite_client)

    def _delete_multiple(
        self,
        identifiers: IdentifierSequence,
        wrap_ids: bool,
        resource_path: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_body_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        resource_path = resource_path or self._RESOURCE_PATH
        tasks = [
            {
                "url_path": resource_path + "/delete",
                "json": {
                    "items": chunk.as_dicts() if wrap_ids else chunk.as_primitives(),
                    **(extra_body_fields or {}),
                },
                "params": params,
                "headers": headers,
            }
            for chunk in identifiers.chunked(self._DELETE_LIMIT)
        ]
        summary = utils._concurrency.execute_tasks_concurrently(self._post, tasks, max_workers=self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"],
            task_list_element_unwrap_fn=utils._auxiliary.unwrap_identifer,
        )

    @overload
    def _update_multiple(
        self,
        items: Union[CogniteResource, CogniteUpdate],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        update_cls: Type[CogniteUpdate],
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
    ) -> T_CogniteResource:
        ...

    @overload
    def _update_multiple(
        self,
        items: Sequence[Union[CogniteResource, CogniteUpdate]],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        update_cls: Type[CogniteUpdate],
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
    ) -> T_CogniteResourceList:
        ...

    def _update_multiple(
        self,
        items: Union[Sequence[Union[CogniteResource, CogniteUpdate]], CogniteResource, CogniteUpdate],
        list_cls: Type[T_CogniteResourceList],
        resource_cls: Type[T_CogniteResource],
        update_cls: Type[CogniteUpdate],
        resource_path: str = None,
        params: Dict = None,
        headers: Dict = None,
    ) -> Union[T_CogniteResourceList, T_CogniteResource]:
        resource_path = resource_path or self._RESOURCE_PATH
        patch_objects = []
        single_item = not isinstance(items, (Sequence, UserList))
        if single_item:
            item_list = cast(Union[Sequence[CogniteResource], Sequence[CogniteUpdate]], [items])
        else:
            item_list = cast(Union[Sequence[CogniteResource], Sequence[CogniteUpdate]], items)

        for index, item in enumerate(item_list):
            if isinstance(item, CogniteResource):
                patch_objects.append(self._convert_resource_to_patch_object(item, update_cls._get_update_properties()))
            elif isinstance(item, CogniteUpdate):
                patch_objects.append(item.dump())
                patch_object_update = patch_objects[index]["update"]
                if "metadata" in patch_object_update and patch_object_update["metadata"] == {"set": None}:
                    patch_object_update["metadata"] = {"set": {}}
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
            return resource_cls._load(updated_items[0], cognite_client=self._cognite_client)
        return list_cls._load(updated_items, cognite_client=self._cognite_client)

    def _search(
        self,
        list_cls: Type[T_CogniteResourceList],
        search: Dict,
        filter: Union[Dict, CogniteFilter],
        limit: int,
        resource_path: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> T_CogniteResourceList:
        utils._auxiliary.assert_type(filter, "filter", [dict, CogniteFilter], allow_none=True)
        if isinstance(filter, CogniteFilter):
            filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            filter = utils._auxiliary.convert_all_keys_to_camel_case(filter)
        resource_path = resource_path or self._RESOURCE_PATH
        res = self._post(
            url_path=resource_path + "/search",
            json={"search": search, "filter": filter, "limit": limit},
            params=params,
            headers=headers,
        )
        return list_cls._load(res.json()["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _convert_resource_to_patch_object(
        resource: CogniteResource, update_attributes: Collection[str]
    ) -> Dict[str, Dict[str, Dict]]:
        dumped_resource = resource.dump(camel_case=True)
        has_id = "id" in dumped_resource
        has_external_id = "externalId" in dumped_resource

        patch_object: Dict[str, Dict[str, Dict]] = {"update": {}}
        if has_id:
            patch_object["id"] = dumped_resource.pop("id")
        elif has_external_id:
            patch_object["externalId"] = dumped_resource.pop("externalId")

        for key, value in dumped_resource.items():
            if utils._auxiliary.to_snake_case(key) in update_attributes:
                patch_object["update"][key] = {"set": value}
        return patch_object

    @staticmethod
    def _status_ok(status_code: int) -> bool:
        return status_code in {200, 201, 202, 204}

    @classmethod
    def _raise_API_error(cls, res: Response, payload: Dict) -> NoReturn:
        x_request_id = res.headers.get("X-Request-Id")
        code = res.status_code
        missing = None
        duplicated = None
        extra = {}
        try:
            error = res.json()["error"]
            if isinstance(error, str):
                msg = error
            elif isinstance(error, Dict):
                msg = error["message"]
                missing = error.get("missing")
                duplicated = error.get("duplicated")
                for k, v in error.items():
                    if k not in ["message", "missing", "duplicated", "code"]:
                        extra[k] = v
            else:
                msg = res.content.decode()
        except Exception:
            msg = res.content.decode()

        error_details: Dict[str, Any] = {"X-Request-ID": x_request_id}
        if payload:
            error_details["payload"] = payload
        if missing:
            error_details["missing"] = missing
        if duplicated:
            error_details["duplicated"] = duplicated
        error_details["headers"] = res.request.headers.copy()
        cls._sanitize_headers(error_details["headers"])
        error_details["response_payload"] = cls._truncate(cls._get_response_content_safe(res))
        error_details["response_headers"] = res.headers

        if res.history:
            for res_hist in res.history:
                log.debug(
                    f"REDIRECT AFTER HTTP Error {res_hist.status_code} {res_hist.request.method} {res_hist.request.url}: {res_hist.content.decode()}"
                )
        log.debug(f"HTTP Error {code} {res.request.method} {res.request.url}: {msg}", extra=error_details)
        raise CogniteAPIError(msg, code, x_request_id, missing=missing, duplicated=duplicated, extra=extra)

    def _log_request(self, res: Response, **kwargs: Any) -> None:
        method = res.request.method
        url = res.request.url
        status_code = res.status_code

        extra = kwargs.copy()
        extra["headers"] = res.request.headers.copy()
        self._sanitize_headers(extra["headers"])
        if extra["payload"] is None:
            del extra["payload"]

        stream = kwargs.get("stream")
        if not stream and self._config.debug is True:
            extra["response_payload"] = self._truncate(self._get_response_content_safe(res))
        extra["response_headers"] = res.headers

        http_protocol_version = ".".join(list(str(res.raw.version)))

        log.debug("HTTP/{} {} {} {}".format(http_protocol_version, method, url, status_code), extra=extra)

    @staticmethod
    def _truncate(s: str, limit: int = 500) -> str:
        if len(s) > limit:
            return s[:limit] + "..."
        return s

    @classmethod
    def _get_response_content_safe(cls, res: Response) -> str:
        try:
            return _json.dumps(res.json())
        except JSONDecodeError:
            pass

        try:
            return res.content.decode()
        except UnicodeDecodeError:
            pass

        return "<binary>"

    @staticmethod
    def _sanitize_headers(headers: Optional[Dict]) -> None:
        if headers is None:
            return None
        if "api-key" in headers:
            headers["api-key"] = "***"
        if "Authorization" in headers:
            headers["Authorization"] = "***"
