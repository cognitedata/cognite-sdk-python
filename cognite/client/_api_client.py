from __future__ import annotations

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
    ClassVar,
    Dict,
    Iterator,
    Literal,
    MutableMapping,
    NoReturn,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)
from urllib.parse import urljoin

import requests.utils
from requests import Response
from requests.structures import CaseInsensitiveDict

from cognite.client._http_client import HTTPClient, HTTPClientConfig, get_global_requests_session
from cognite.client.config import global_config
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteResource,
    CogniteUpdate,
    EnumProperty,
    PropertySpec,
    T_CogniteResource,
    T_CogniteResourceList,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import (
    assert_type,
    get_current_sdk_version,
    get_user_agent,
    interpolate_and_url_encode,
    is_unlimited,
    json_dump_default,
    split_into_chunks,
)
from cognite.client.utils._concurrency import TaskExecutor, collect_exc_info_and_raise, execute_tasks
from cognite.client.utils._identifier import (
    Identifier,
    IdentifierCore,
    IdentifierSequence,
    IdentifierSequenceCore,
    SingletonIdentifierSequence,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case, shorten, to_camel_case, to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig

log = logging.getLogger("cognite-sdk")

T = TypeVar("T")


class APIClient:
    _RESOURCE_PATH: str
    _RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS: ClassVar[frozenset[str]] = frozenset(
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
            "/models/.*",
            "/units/.*",
        )
    )

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        self._config = config
        self._api_version = api_version
        self._api_subversion = config.api_subversion
        self._cognite_client = cognite_client
        self._init_http_clients()

        self._CREATE_LIMIT = 1000
        self._LIST_LIMIT = 1000
        self._RETRIEVE_LIMIT = 1000
        self._DELETE_LIMIT = 1000
        self._UPDATE_LIMIT = 1000

    def _init_http_clients(self) -> None:
        session = get_global_requests_session()
        self._http_client = HTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry={429},
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=0,
                max_retries_connect=global_config.max_retries_connect,
                max_retries_status=global_config.max_retries,
            ),
            session=session,
            refresh_auth_header=self._refresh_auth_header,
        )
        self._http_client_with_retry = HTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry=global_config.status_forcelist,
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=global_config.max_retries,
                max_retries_connect=global_config.max_retries_connect,
                max_retries_status=global_config.max_retries,
            ),
            session=session,
            refresh_auth_header=self._refresh_auth_header,
        )

    def _delete(
        self, url_path: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> Response:
        return self._do_request("DELETE", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _get(
        self, url_path: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> Response:
        return self._do_request("GET", url_path, params=params, headers=headers, timeout=self._config.timeout)

    def _post(
        self,
        url_path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        api_subversion: str | None = None,
    ) -> Response:
        return self._do_request(
            "POST",
            url_path,
            json=json,
            headers=headers,
            params=params,
            timeout=self._config.timeout,
            api_subversion=api_subversion,
        )

    def _put(
        self, url_path: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> Response:
        return self._do_request("PUT", url_path, json=json, headers=headers, timeout=self._config.timeout)

    def _do_request(
        self,
        method: str,
        url_path: str,
        accept: str = "application/json",
        api_subversion: str | None = None,
        **kwargs: Any,
    ) -> Response:
        is_retryable, full_url = self._resolve_url(method, url_path)
        json_payload = kwargs.get("json")
        headers = self._configure_headers(
            accept,
            additional_headers=self._config.headers.copy(),
            api_subversion=api_subversion,
        )
        headers.update(kwargs.get("headers") or {})

        if json_payload:
            try:
                data = _json.dumps(json_payload, default=json_dump_default, allow_nan=False)
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
            self._raise_api_error(res, payload=json_payload)
        stream = kwargs.get("stream")
        self._log_request(res, payload=json_payload, stream=stream)
        return res

    def _configure_headers(
        self, accept: str, additional_headers: dict[str, str], api_subversion: str | None
    ) -> MutableMapping[str, Any]:
        headers: MutableMapping[str, Any] = CaseInsensitiveDict()
        headers.update(requests.utils.default_headers())
        self._refresh_auth_header(headers)
        headers["content-type"] = "application/json"
        headers["accept"] = accept
        headers["x-cdp-sdk"] = f"CognitePythonSDK:{get_current_sdk_version()}"
        headers["x-cdp-app"] = self._config.client_name
        headers["cdf-version"] = api_subversion or self._api_subversion
        if "User-Agent" in headers:
            headers["User-Agent"] += " " + get_user_agent()
        else:
            headers["User-Agent"] = get_user_agent()
        headers.update(additional_headers)
        return headers

    def _refresh_auth_header(self, headers: MutableMapping[str, Any]) -> None:
        auth_header_name, auth_header_value = self._config.credentials.authorization_header()
        headers[auth_header_name] = auth_header_value

    def _resolve_url(self, method: str, url_path: str) -> tuple[bool, str]:
        if not url_path.startswith("/"):
            raise ValueError("URL path must start with '/'")
        base_url = self._get_base_url_with_base_path()
        full_url = base_url + url_path
        is_retryable = self._is_retryable(method, full_url)
        return is_retryable, full_url

    def _get_base_url_with_base_path(self) -> str:
        base_path = ""
        if self._api_version:
            base_path = f"/api/{self._api_version}/projects/{self._config.project}"
        return urljoin(self._config.base_url, base_path)

    def _is_retryable(self, method: str, path: str) -> bool:
        valid_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]

        if method not in valid_methods:
            raise ValueError(f"Method {method} is not valid. Must be one of {valid_methods}")

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
            raise ValueError(f"URL {url} is not valid. Cannot resolve whether or not it is retryable")
        path = match.group(1)
        for pattern in cls._RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS:
            if re.match(pattern, path):
                return True
        return False

    def _retrieve(
        self,
        identifier: IdentifierCore,
        cls: type[T_CogniteResource],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
    ) -> T_CogniteResource | None:
        resource_path = resource_path or self._RESOURCE_PATH
        try:
            res = self._get(
                url_path=interpolate_and_url_encode(resource_path + "/{}", str(identifier.as_primitive())),
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
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        identifiers: SingletonIdentifierSequence,
        resource_path: str | None = None,
        ignore_unknown_ids: bool | None = None,
        headers: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        executor: TaskExecutor | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResource | None:
        ...

    @overload
    def _retrieve_multiple(
        self,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        identifiers: IdentifierSequenceCore,
        resource_path: str | None = None,
        ignore_unknown_ids: bool | None = None,
        headers: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        executor: TaskExecutor | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList:
        ...

    def _retrieve_multiple(
        self,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        identifiers: SingletonIdentifierSequence | IdentifierSequenceCore,
        resource_path: str | None = None,
        ignore_unknown_ids: bool | None = None,
        headers: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        executor: TaskExecutor | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList | T_CogniteResource | None:
        resource_path = resource_path or self._RESOURCE_PATH

        ignore_unknown_obj = {} if ignore_unknown_ids is None else {"ignoreUnknownIds": ignore_unknown_ids}
        tasks: list[dict[str, str | dict[str, Any] | None]] = [
            {
                "url_path": resource_path + "/byids",
                "json": {
                    "items": id_chunk.as_dicts(),
                    **ignore_unknown_obj,
                    **(other_params or {}),
                },
                "headers": headers,
                "params": params,
            }
            for id_chunk in identifiers.chunked(self._RETRIEVE_LIMIT)
        ]
        tasks_summary = execute_tasks(
            functools.partial(self._post, api_subversion=api_subversion),
            tasks,
            max_workers=self._config.max_workers,
            executor=executor,
        )

        if tasks_summary.exceptions:
            try:
                collect_exc_info_and_raise(tasks_summary.exceptions)
            except CogniteNotFoundError:
                if identifiers.is_singleton():
                    return None
                raise

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        if identifiers.is_singleton():
            if retrieved_items:
                return resource_cls._load(retrieved_items[0], cognite_client=self._cognite_client)
            else:
                # Not all APIs (such as the Data Modeling API) return an error when unknown ids are provided,
                # so we need to handle the unknown singleton identifier case here as well.
                return None
        return list_cls._load(retrieved_items, cognite_client=self._cognite_client)

    def _list_generator(
        self,
        method: str,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        url_path: str | None = None,
        limit: int | None = None,
        chunk_size: int | None = None,
        filter: dict[str, Any] | None = None,
        sort: Sequence[str | dict[str, Any]] | None = None,
        other_params: dict[str, Any] | None = None,
        partitions: int | None = None,
        headers: dict[str, Any] | None = None,
        initial_cursor: str | None = None,
        advanced_filter: dict | Filter | None = None,
        api_subversion: str | None = None,
    ) -> Iterator[T_CogniteResourceList] | Iterator[T_CogniteResource]:
        if is_unlimited(limit):
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
                    params.update(other_params or {})
                    res = self._get(url_path=url_path or resource_path, params=params, headers=headers)

                elif method == "POST":
                    body: dict[str, Any] = {"limit": current_limit, "cursor": next_cursor, **(other_params or {})}
                    if filter:
                        body["filter"] = filter
                    if advanced_filter:
                        body["advancedFilter"] = (
                            advanced_filter.dump(camel_case=True)
                            if isinstance(advanced_filter, Filter)
                            else advanced_filter
                        )
                    if sort is not None:
                        body["sort"] = sort
                    res = self._post(
                        url_path=url_path or resource_path + "/list",
                        json=body,
                        headers=headers,
                        api_subversion=api_subversion,
                    )
                else:
                    raise ValueError(f"_list_generator parameter `method` must be GET or POST, not {method}")
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
        resource_cls: type[T_CogniteResource],
        resource_path: str,
        filter: dict | None = None,
        other_params: dict | None = None,
        headers: dict | None = None,
    ) -> Iterator[T_CogniteResource]:
        next_cursors = {i + 1: None for i in range(partitions)}

        def get_partition(partition_num: int) -> list[dict[str, Any]]:
            next_cursor = next_cursors[partition_num]

            body = {
                "filter": filter or {},
                "limit": self._LIST_LIMIT,
                "cursor": next_cursor,
                "partition": f"{partition_num}/{partitions}",
                **(other_params or {}),
            }
            res = self._post(url_path=resource_path + "/list", json=body, headers=headers)
            next_cursor = res.json().get("nextCursor")
            next_cursors[partition_num] = next_cursor

            return res.json()["items"]

        while len(next_cursors) > 0:
            tasks_summary = execute_tasks(
                get_partition, [(partition,) for partition in next_cursors], max_workers=partitions
            )
            tasks_summary.raise_first_encountered_exception()

            for item in tasks_summary.joined_results():
                yield resource_cls._load(item, cognite_client=self._cognite_client)

            # Remove None from cursor dict
            next_cursors = {partition: next_cursors[partition] for partition in next_cursors if next_cursors[partition]}

    def _list(
        self,
        method: Literal["POST", "GET"],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        url_path: str | None = None,
        limit: int | None = None,
        filter: dict | None = None,
        other_params: dict | None = None,
        partitions: int | None = None,
        sort: Sequence[str | dict[str, Any]] | None = None,
        headers: dict | None = None,
        initial_cursor: str | None = None,
        advanced_filter: dict | Filter | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList:
        if partitions:
            if not is_unlimited(limit):
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
        items: list[T_CogniteResource] = []
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
            advanced_filter=advanced_filter,
            api_subversion=api_subversion,
        ):
            items.extend(cast(T_CogniteResourceList, resource_list).data)
        return list_cls(items, cognite_client=self._cognite_client)

    def _list_partitioned(
        self,
        partitions: int,
        method: Literal["POST", "GET"],
        list_cls: type[T_CogniteResourceList],
        resource_path: str | None = None,
        filter: dict | None = None,
        other_params: dict | None = None,
        headers: dict | None = None,
        advanced_filter: dict | Filter | None = None,
    ) -> T_CogniteResourceList:
        def get_partition(partition: int) -> list[dict[str, Any]]:
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
                    if advanced_filter:
                        body["advancedFilter"] = (
                            advanced_filter.dump(camel_case=True)
                            if isinstance(advanced_filter, Filter)
                            else advanced_filter
                        )
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

        tasks = [(f"{i + 1}/{partitions}",) for i in range(partitions)]
        tasks_summary = execute_tasks(get_partition, tasks, max_workers=partitions)
        tasks_summary.raise_first_encountered_exception()

        return list_cls._load(tasks_summary.joined_results(), cognite_client=self._cognite_client)

    def _aggregate(
        self,
        cls: type[T],
        resource_path: str | None = None,
        filter: CogniteFilter | dict | None = None,
        aggregate: str | None = None,
        fields: Sequence[str] | None = None,
        keys: Sequence[str] | None = None,
        headers: dict | None = None,
    ) -> list[T]:
        assert_type(filter, "filter", [dict, CogniteFilter], allow_none=True)
        assert_type(fields, "fields", [list], allow_none=True)
        if isinstance(filter, CogniteFilter):
            dumped_filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            dumped_filter = convert_all_keys_to_camel_case(filter)
        else:
            dumped_filter = {}
        resource_path = resource_path or self._RESOURCE_PATH
        body: dict[str, Any] = {"filter": dumped_filter}
        if aggregate is not None:
            body["aggregate"] = aggregate
        if fields is not None:
            body["fields"] = fields
        if keys is not None:
            body["keys"] = keys
        res = self._post(url_path=resource_path + "/aggregate", json=body, headers=headers)
        return [cls(**agg) for agg in res.json()["items"]]

    @overload
    def _advanced_aggregate(
        self,
        aggregate: Literal["count", "cardinalityValues", "cardinalityProperties"],
        properties: EnumProperty
        | str
        | list[str]
        | tuple[EnumProperty | str | list[str], AggregationFilter]
        | None = None,
        path: EnumProperty | str | list[str] | None = None,
        query: str | None = None,
        filter: CogniteFilter | dict | None = None,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        limit: int | None = None,
        api_subversion: str | None = None,
    ) -> int:
        ...

    @overload
    def _advanced_aggregate(
        self,
        aggregate: Literal["uniqueValues", "uniqueProperties"],
        properties: EnumProperty
        | str
        | list[str]
        | tuple[EnumProperty | str | list[str], AggregationFilter]
        | None = None,
        path: EnumProperty | str | list[str] | None = None,
        query: str | None = None,
        filter: CogniteFilter | dict | None = None,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        limit: int | None = None,
        api_subversion: str | None = None,
    ) -> UniqueResultList:
        ...

    def _advanced_aggregate(
        self,
        aggregate: Literal["count", "cardinalityValues", "cardinalityProperties", "uniqueValues", "uniqueProperties"],
        properties: EnumProperty
        | str
        | list[str]
        | tuple[EnumProperty | str | list[str], AggregationFilter]
        | None = None,
        path: EnumProperty | str | list[str] | None = None,
        query: str | None = None,
        filter: CogniteFilter | dict | None = None,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        limit: int | None = None,
        api_subversion: str | None = None,
    ) -> int | UniqueResultList:
        if aggregate not in ["count", "cardinalityValues", "cardinalityProperties", "uniqueValues", "uniqueProperties"]:
            raise ValueError(
                f"Invalid aggregate '{aggregate}'. Valid aggregates are 'count', 'cardinalityValues', "
                f"'cardinalityProperties', 'uniqueValues', and 'uniqueProperties'."
            )

        body: dict[str, Any] = {"aggregate": aggregate}
        if properties is not None:
            if isinstance(properties, tuple):
                properties, property_aggregation_filter = properties
            else:
                property_aggregation_filter = None
            if isinstance(properties, EnumProperty):
                dumped_properties = properties.as_reference()
            elif isinstance(properties, str):
                dumped_properties = [to_camel_case(properties)]
            elif isinstance(properties, list):
                dumped_properties = [to_camel_case(p) for p in properties]
            else:
                raise ValueError(f"Unknown property format: {properties}")

            body["properties"] = [{"property": dumped_properties}]
            if property_aggregation_filter is not None:
                body["properties"][0]["filter"] = property_aggregation_filter.dump()

        if path is not None:
            if isinstance(path, EnumProperty):
                dumped_path = path.as_reference()
            elif isinstance(path, str):
                dumped_path = [path]
            elif isinstance(path, list):
                dumped_path = path
            else:
                raise ValueError(f"Unknown path format: {path}")
            body["path"] = dumped_path

        if query is not None:
            body["search"] = {"query": query}

        if filter is not None:
            assert_type(filter, "filter", [dict, CogniteFilter], allow_none=False)
            if isinstance(filter, CogniteFilter):
                dumped_filter = filter.dump(camel_case=True)
            elif isinstance(filter, dict):
                dumped_filter = convert_all_keys_to_camel_case(filter)
            else:
                raise ValueError(f"Unknown filter format: {filter}")
            body["filter"] = dumped_filter

        if advanced_filter is not None:
            body["advancedFilter"] = advanced_filter.dump() if isinstance(advanced_filter, Filter) else advanced_filter

        if aggregate_filter is not None:
            body["aggregateFilter"] = (
                aggregate_filter.dump() if isinstance(aggregate_filter, AggregationFilter) else aggregate_filter
            )
        if limit is not None:
            body["limit"] = limit

        res = self._post(url_path=f"{self._RESOURCE_PATH}/aggregate", json=body, api_subversion=api_subversion)
        json_items = res.json()["items"]
        if aggregate in {"count", "cardinalityValues", "cardinalityProperties"}:
            return json_items[0]["count"]
        elif aggregate in {"uniqueValues", "uniqueProperties"}:
            return UniqueResultList._load(json_items, cognite_client=self._cognite_client)
        else:
            raise ValueError(f"Unknown aggregate: {aggregate}")

    @overload
    def _create_multiple(
        self,
        items: Sequence[CogniteResource] | Sequence[dict[str, Any]],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        extra_body_fields: dict | None = None,
        limit: int | None = None,
        input_resource_cls: type[CogniteResource] | None = None,
        executor: TaskExecutor | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList:
        ...

    @overload
    def _create_multiple(
        self,
        items: CogniteResource | dict[str, Any],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        extra_body_fields: dict | None = None,
        limit: int | None = None,
        input_resource_cls: type[CogniteResource] | None = None,
        executor: TaskExecutor | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResource:
        ...

    def _create_multiple(
        self,
        items: Sequence[CogniteResource] | Sequence[dict[str, Any]] | CogniteResource | dict[str, Any],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        extra_body_fields: dict | None = None,
        limit: int | None = None,
        input_resource_cls: type[CogniteResource] | None = None,
        executor: TaskExecutor | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList | T_CogniteResource:
        resource_path = resource_path or self._RESOURCE_PATH
        input_resource_cls = input_resource_cls or resource_cls
        limit = limit or self._CREATE_LIMIT
        single_item = not isinstance(items, Sequence)
        if single_item:
            items = cast(Union[Sequence[T_CogniteResource], Sequence[Dict[str, Any]]], [items])
        else:
            items = cast(Union[Sequence[T_CogniteResource], Sequence[Dict[str, Any]]], items)

        tasks = [
            (resource_path, task_items, params, headers)
            for task_items in self._prepare_item_chunks(items, limit, extra_body_fields)
        ]
        summary = execute_tasks(
            functools.partial(self._post, api_subversion=api_subversion),
            tasks,
            max_workers=self._config.max_workers,
            executor=executor,
        )

        def unwrap_element(el: T) -> CogniteResource | T:
            if isinstance(el, dict):
                return input_resource_cls._load(el, cognite_client=self._cognite_client)
            else:
                return el

        def str_format_element(el: T) -> str | dict | T:
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
        identifiers: IdentifierSequenceCore,
        wrap_ids: bool,
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        extra_body_fields: dict[str, Any] | None = None,
        returns_items: bool = False,
        executor: TaskExecutor | None = None,
    ) -> list | None:
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
        summary = execute_tasks(self._post, tasks, max_workers=self._config.max_workers, executor=executor)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"],
            task_list_element_unwrap_fn=identifiers.unwrap_identifier,
        )
        if returns_items:
            return summary.joined_results(lambda res: res.json()["items"])
        else:
            return None

    @overload
    def _update_multiple(
        self,
        items: CogniteResource | CogniteUpdate,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        api_subversion: str | None = None,
    ) -> T_CogniteResource:
        ...

    @overload
    def _update_multiple(
        self,
        items: Sequence[CogniteResource | CogniteUpdate],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList:
        ...

    def _update_multiple(
        self,
        items: Sequence[CogniteResource | CogniteUpdate] | CogniteResource | CogniteUpdate,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        resource_path: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList | T_CogniteResource:
        resource_path = resource_path or self._RESOURCE_PATH
        patch_objects = []
        single_item = not isinstance(items, (Sequence, UserList))
        if single_item:
            item_list = cast(Union[Sequence[CogniteResource], Sequence[CogniteUpdate]], [items])
        else:
            item_list = cast(Union[Sequence[CogniteResource], Sequence[CogniteUpdate]], items)

        for index, item in enumerate(item_list):
            if isinstance(item, CogniteResource):
                patch_objects.append(
                    self._convert_resource_to_patch_object(item, update_cls._get_update_properties(), mode)
                )
            elif isinstance(item, CogniteUpdate):
                patch_objects.append(item.dump(camel_case=True))
                patch_object_update = patch_objects[index]["update"]
                if "metadata" in patch_object_update and patch_object_update["metadata"] == {"set": None}:
                    patch_object_update["metadata"] = {"set": {}}
            else:
                raise ValueError("update item must be of type CogniteResource or CogniteUpdate")
        patch_object_chunks = split_into_chunks(patch_objects, self._UPDATE_LIMIT)

        tasks = [
            {"url_path": resource_path + "/update", "json": {"items": chunk}, "params": params, "headers": headers}
            for chunk in patch_object_chunks
        ]

        tasks_summary = execute_tasks(
            functools.partial(self._post, api_subversion=api_subversion), tasks, max_workers=self._config.max_workers
        )
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"],
            task_list_element_unwrap_fn=lambda el: IdentifierSequenceCore.unwrap_identifier(el),
        )
        updated_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        if single_item:
            return resource_cls._load(updated_items[0], cognite_client=self._cognite_client)
        return list_cls._load(updated_items, cognite_client=self._cognite_client)

    def _upsert_multiple(
        self,
        items: CogniteResource | Sequence[CogniteResource],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        mode: Literal["patch", "replace"],
        input_resource_cls: type[CogniteResource] | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResource | T_CogniteResourceList:
        if mode not in ["patch", "replace"]:
            raise ValueError(f"mode must be either 'patch' or 'replace', got {mode!r}")
        is_single = isinstance(items, CogniteResource)
        items = cast(Sequence[T_CogniteResource], [items] if is_single else items)
        try:
            result = self._update_multiple(
                items, list_cls, resource_cls, update_cls, mode=mode, api_subversion=api_subversion
            )
        except CogniteNotFoundError as not_found_error:
            items_by_external_id = {item.external_id: item for item in items if item.external_id is not None}  # type: ignore [attr-defined]
            items_by_id = {item.id: item for item in items if hasattr(item, "id") and item.id is not None}
            # Not found must have an external id as they do not exist in CDF:
            try:
                missing_external_ids = {entry["externalId"] for entry in not_found_error.not_found}
            except KeyError:
                # There is a not found internal id, which means we cannot identify it.
                raise not_found_error
            to_create = [
                items_by_external_id[external_id]
                for external_id in not_found_error.failed
                if external_id in missing_external_ids
            ]

            # Updates can have either external id or id. If they have an id, they must exist in CDF.
            to_update = [
                items_by_external_id[identifier] if isinstance(identifier, str) else items_by_id[identifier]
                for identifier in not_found_error.failed
                if identifier not in missing_external_ids or isinstance(identifier, int)
            ]

            created: T_CogniteResourceList | None = None
            updated: T_CogniteResourceList | None = None
            try:
                if to_create:
                    created = self._create_multiple(
                        to_create,
                        list_cls=list_cls,
                        resource_cls=resource_cls,
                        input_resource_cls=input_resource_cls,
                        api_subversion=api_subversion,
                    )
                if to_update:
                    updated = self._update_multiple(
                        to_update,
                        list_cls=list_cls,
                        resource_cls=resource_cls,
                        update_cls=update_cls,
                        mode=mode,
                        api_subversion=api_subversion,
                    )
            except CogniteAPIError as api_error:
                successful = list(api_error.successful)
                unknown = list(api_error.unknown)
                failed = list(api_error.failed)

                successful.extend(not_found_error.successful)
                unknown.extend(not_found_error.unknown)
                if created is not None:
                    # The update call failed
                    successful.extend(item.external_id for item in created)
                if updated is None and created is not None:
                    # The created call failed
                    failed.extend(item.external_id if item.external_id is not None else item.id for item in to_update)  # type: ignore [attr-defined]
                raise CogniteAPIError(
                    api_error.message, code=api_error.code, successful=successful, failed=failed, unknown=unknown
                )
            # Need to retrieve the successful updated items from the first call.
            successful_resources: T_CogniteResourceList | None = None
            if not_found_error.successful:
                identifiers = IdentifierSequence.of(*not_found_error.successful)
                successful_resources = self._retrieve_multiple(
                    list_cls=list_cls, resource_cls=resource_cls, identifiers=identifiers, api_subversion=api_subversion
                )
                if isinstance(successful_resources, resource_cls):
                    successful_resources = list_cls([successful_resources], cognite_client=self._cognite_client)

            result = list_cls(
                (successful_resources or []) + (created or []) + (updated or []), cognite_client=self._cognite_client
            )

            # Reorder to match the order of the input items
            result.data = [
                result.get(
                    **Identifier.load(item.id if hasattr(item, "id") else None, item.external_id).as_dict(  # type: ignore [attr-defined]
                        camel_case=False
                    )
                )
                for item in items
            ]

        if is_single:
            return result[0]
        return result

    def _search(
        self,
        list_cls: type[T_CogniteResourceList],
        search: dict,
        filter: dict | CogniteFilter,
        limit: int,
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        api_subversion: str | None = None,
    ) -> T_CogniteResourceList:
        assert_type(filter, "filter", [dict, CogniteFilter], allow_none=True)
        if isinstance(filter, CogniteFilter):
            filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            filter = convert_all_keys_to_camel_case(filter)
        resource_path = resource_path or self._RESOURCE_PATH
        res = self._post(
            url_path=resource_path + "/search",
            json={"search": search, "filter": filter, "limit": limit},
            params=params,
            headers=headers,
            api_subversion=api_subversion,
        )
        return list_cls._load(res.json()["items"], cognite_client=self._cognite_client)

    @staticmethod
    def _prepare_item_chunks(
        items: Sequence[T_CogniteResource] | Sequence[dict[str, Any]],
        limit: int,
        extra_body_fields: dict | None,
    ) -> list[dict[str, Any]]:
        return [
            {"items": chunk, **(extra_body_fields or {})}
            for chunk in split_into_chunks(
                [it.dump(camel_case=True) if isinstance(it, CogniteResource) else it for it in items],
                chunk_size=limit,
            )
        ]

    @classmethod
    def _convert_resource_to_patch_object(
        cls,
        resource: CogniteResource,
        update_attributes: list[PropertySpec],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> dict[str, dict[str, dict]]:
        dumped_resource = resource.dump(camel_case=True)
        has_id = "id" in dumped_resource
        has_external_id = "externalId" in dumped_resource

        patch_object: dict[str, dict[str, dict]] = {"update": {}}
        if has_id:
            patch_object["id"] = dumped_resource.pop("id")
        elif has_external_id:
            patch_object["externalId"] = dumped_resource.pop("externalId")

        update: dict[str, dict] = cls._clear_all_attributes(update_attributes) if mode == "replace" else {}

        update_attribute_by_name = {prop.name: prop for prop in update_attributes}
        for key, value in dumped_resource.items():
            if (snake := to_snake_case(key)) not in update_attribute_by_name:
                continue
            prop = update_attribute_by_name[snake]
            if prop.is_container and mode == "patch":
                update[key] = {"add": value}
            else:
                update[key] = {"set": value}

        patch_object["update"] = update
        return patch_object

    @staticmethod
    def _clear_all_attributes(
        update_attributes: list[PropertySpec],
    ) -> dict[str, dict]:
        return {
            to_camel_case(prop.name): {"set": []} if prop.is_container else {"setNull": True}
            for prop in update_attributes
            if prop.is_nullable and not prop.is_beta
        }

    @staticmethod
    def _status_ok(status_code: int) -> bool:
        return status_code in {200, 201, 202, 204}

    @classmethod
    def _raise_api_error(cls, res: Response, payload: dict) -> NoReturn:
        x_request_id = res.headers.get("X-Request-Id")
        code = res.status_code
        missing = None
        duplicated = None
        extra = {}
        try:
            error = res.json()["error"]
            if isinstance(error, str):
                msg = error
            elif isinstance(error, dict):
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

        error_details: dict[str, Any] = {"X-Request-ID": x_request_id}
        if payload:
            error_details["payload"] = payload
        if missing:
            error_details["missing"] = missing
        if duplicated:
            error_details["duplicated"] = duplicated
        error_details["headers"] = res.request.headers.copy()
        cls._sanitize_headers(error_details["headers"])
        error_details["response_payload"] = shorten(cls._get_response_content_safe(res), 500)
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
            extra["response_payload"] = shorten(self._get_response_content_safe(res), 500)
        extra["response_headers"] = res.headers

        try:
            http_protocol = f"HTTP/{'.'.join(str(res.raw.version))}"
        except AttributeError:
            # If this fails, it means we are running in a browser (pyodide) with patched requests package:
            http_protocol = "XMLHTTP"

        log.debug(f"{http_protocol} {method} {url} {status_code}", extra=extra)

    @staticmethod
    def _get_response_content_safe(res: Response) -> str:
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
    def _sanitize_headers(headers: dict | None) -> None:
        if headers is None:
            return None
        if "api-key" in headers:
            headers["api-key"] = "***"
        if "Authorization" in headers:
            headers["Authorization"] = "***"
