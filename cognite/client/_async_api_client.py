from __future__ import annotations

import functools
import gzip
import itertools
import logging
import re
import warnings
from collections import UserList
from collections.abc import AsyncIterator, Iterator, Mapping, MutableMapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NoReturn,
    TypeVar,
    cast,
    overload,
)
from urllib.parse import urljoin

import httpx
from requests.structures import CaseInsensitiveDict

from cognite.client._async_http_client import AsyncHTTPClient, HTTPClientConfig, get_global_async_client
from cognite.client.config import global_config
from cognite.client.data_classes._base import (
    CogniteFilter,
    CogniteObject,
    CogniteResource,
    CogniteUpdate,
    EnumProperty,
    PropertySpec,
    T_CogniteResource,
    T_CogniteResourceList,
    T_WritableCogniteResource,
    WriteableCogniteResource,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError, CogniteProjectAccessError
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import (
    get_current_sdk_version,
    get_user_agent,
    interpolate_and_url_encode,
    is_unlimited,
    split_into_chunks,
    unpack_items_in_payload,
)
from cognite.client.utils._concurrency import TaskExecutor, execute_tasks_async
from cognite.client.utils._identifier import (
    Identifier,
    IdentifierCore,
    IdentifierSequence,
    IdentifierSequenceCore,
    SingletonIdentifierSequence,
)
from cognite.client.utils._json import JSONDecodeError
from cognite.client.utils._text import convert_all_keys_to_camel_case, shorten, to_camel_case, to_snake_case
from cognite.client.utils._validation import assert_type, verify_limit
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=CogniteObject)

VALID_AGGREGATIONS = {"count", "cardinalityValues", "cardinalityProperties", "uniqueValues", "uniqueProperties"}


class AsyncAPIClient:
    _RESOURCE_PATH: str
    # TODO: When Cognite Experimental SDK is deprecated, remove frozenset in favour of re.compile:
    _RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS: ClassVar[frozenset[str]] = frozenset(
        [
            r"|".join(
                rf"^/{path}(\?.*)?$"
                for path in (
                    "(assets|events|files|timeseries|sequences|datasets|relationships|labels)/(list|byids|search|aggregate)",
                    "files/downloadlink",
                    "timeseries/(data(/(list|latest|delete))?|synthetic/query)",
                    "sequences/data(/(list|delete))?",
                    "raw/dbs/[^/]+/tables/[^/]+/rows(/delete)?",
                    "context/entitymatching/(byids|list|jobs)",
                    "sessions/revoke",
                    "models/.*",
                    ".*/graphql",
                    "units/.*",
                    "annotations/(list|byids|reverselookup)",
                    r"functions/(list|byids|status|schedules/(list|byids)|\d+/calls/(list|byids))",
                    r"3d/models/\d+/revisions/\d+/(mappings/list|nodes/(list|byids))",
                    "documents/(aggregate|list|search|content|status|passages/search)",
                    "profiles/(byids|search)",
                    "geospatial/(compute|crs/byids|featuretypes/(byids|list))",
                    "geospatial/featuretypes/[A-Za-z][A-Za-z0-9_]{0,31}/features/(aggregate|list|byids|search|search-streaming|[A-Za-z][A-Za-z0-9_]{0,255}/rasters/[A-Za-z][A-Za-z0-9_]{0,31})",
                    "transformations/(filter|byids|jobs/byids|schedules/byids|query/run)",
                    "simulators/list",
                    "extpipes/(list|byids|runs/list)",
                    "workflows/.*",
                    "hostedextractors/.*",
                    "postgresgateway/.*",
                    "context/diagram/.*",
                    "ai/tools/documents/(summarize|ask)",
                    "ai/agents(/(byids|delete))?",
                )
            )
        ]
    )

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
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
        client = get_global_async_client()
        self._http_client = AsyncHTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry={429},
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=0,
                max_retries_connect=global_config.max_retries_connect,
                max_retries_status=global_config.max_retries,
            ),
            client=client,
            refresh_auth_header=self._refresh_auth_header,
        )
        self._http_client_with_retry = AsyncHTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry=global_config.status_forcelist,
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=global_config.max_retries,
                max_retries_connect=global_config.max_retries_connect,
                max_retries_status=global_config.max_retries,
            ),
            client=client,
            refresh_auth_header=self._refresh_auth_header,
        )

    async def _delete(
        self, url_path: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> httpx.Response:
        return await self._do_request("DELETE", url_path, params=params, headers=headers, timeout=self._config.timeout)

    async def _get(
        self, url_path: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> httpx.Response:
        return await self._do_request("GET", url_path, params=params, headers=headers, timeout=self._config.timeout)

    async def _post(
        self,
        url_path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        api_subversion: str | None = None,
    ) -> httpx.Response:
        return await self._do_request(
            "POST",
            url_path,
            json=json,
            headers=headers,
            params=params,
            timeout=self._config.timeout,
            api_subversion=api_subversion,
        )

    async def _put(
        self, url_path: str, json: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
    ) -> httpx.Response:
        return await self._do_request("PUT", url_path, json=json, headers=headers, timeout=self._config.timeout)

    async def _do_request(
        self,
        method: str,
        url_path: str,
        accept: str = "application/json",
        api_subversion: str | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        is_retryable, full_url = self._resolve_url(method, url_path)
        json_payload = kwargs.pop("json", None)
        headers = self._configure_headers(
            accept,
            additional_headers=self._config.headers.copy(),
            api_subversion=api_subversion,
        )
        headers.update(kwargs.get("headers") or {})

        if json_payload is not None:
            try:
                data = _json.dumps(json_payload, allow_nan=False)
            except ValueError as e:
                msg = "Out of range float values are not JSON compliant"
                if msg in str(e):
                    raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                        e.__traceback__
                    ) from None
                raise
            kwargs["content"] = data
            if method in ["PUT", "POST"] and not global_config.disable_gzip:
                kwargs["content"] = gzip.compress(data.encode())
                headers["Content-Encoding"] = "gzip"

        kwargs["headers"] = headers
        kwargs.setdefault("allow_redirects", False)

        if is_retryable:
            res = await self._http_client_with_retry.request(method=method, url=full_url, **kwargs)
        else:
            res = await self._http_client.request(method=method, url=full_url, **kwargs)

        match res.status_code:
            case 200 | 201 | 202 | 204:
                pass
            case 401:
                self._raise_no_project_access_error(res)
            case _:
                self._raise_api_error(res, payload=json_payload)

        stream = kwargs.get("stream")
        self._log_request(res, payload=json_payload, stream=stream)
        return res

    def _configure_headers(
        self, accept: str, additional_headers: dict[str, str], api_subversion: str | None = None
    ) -> MutableMapping[str, Any]:
        headers: MutableMapping[str, Any] = CaseInsensitiveDict()
        headers.update({
            'User-Agent': f'python-httpx/{httpx.__version__}',
            'Accept': accept,
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        self._refresh_auth_header(headers)
        headers["content-type"] = "application/json"
        headers["accept"] = accept
        headers["x-cdp-sdk"] = f"CognitePythonSDK:{get_current_sdk_version()}"
        headers["x-cdp-app"] = self._config.client_name
        headers["cdf-version"] = api_subversion or self._api_subversion
        if "User-Agent" in headers:
            headers["User-Agent"] += f" {get_user_agent()}"
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

        return method in ["GET", "PUT", "PATCH"] or (method == "POST" and self._url_is_retryable(path))

    @classmethod
    @functools.lru_cache(64)
    def _url_is_retryable(cls, url: str) -> bool:
        valid_url_pattern = r"^https?://[a-z\d.:\-]+(?:/api/(?:v1|playground)/projects/[^/]+)?((/[^\?]+)?(\?.+)?)"
        match = re.match(valid_url_pattern, url)
        if not match:
            raise ValueError(f"URL {url} is not valid. Cannot resolve whether or not it is retryable")
        path = match.group(1)
        return any(re.match(pattern, path) for pattern in cls._RETRYABLE_POST_ENDPOINT_REGEX_PATTERNS)

    async def _retrieve(
        self,
        identifier: IdentifierCore,
        cls: type[T_CogniteResource],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> T_CogniteResource | None:
        resource_path = resource_path or self._RESOURCE_PATH
        try:
            res = await self._get(
                url_path=interpolate_and_url_encode(resource_path + "/{}", str(identifier.as_primitive())),
                params=params,
                headers=headers,
            )
            return cls._load(res.json(), cognite_client=self._cognite_client)
        except CogniteAPIError as e:
            if e.code != 404:
                raise
        return None

    # I'll implement key methods here, focusing on the most commonly used ones
    # The full implementation would include all the overloaded methods from the original

    async def _retrieve_multiple(
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
        settings_forcing_raw_response_loading: list[str] | None = None,
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
        tasks_summary = await execute_tasks_async(
            functools.partial(self._post, api_subversion=api_subversion),
            tasks,
            max_workers=self._config.max_workers,
            fail_fast=True,
            executor=executor,
        )
        try:
            tasks_summary.raise_compound_exception_if_failed_tasks(
                task_unwrap_fn=unpack_items_in_payload,
                task_list_element_unwrap_fn=identifiers.extract_identifiers,
            )
        except CogniteNotFoundError:
            if identifiers.is_singleton():
                return None
            raise

        if settings_forcing_raw_response_loading:
            loaded = list_cls._load_raw_api_response(
                tasks_summary.raw_api_responses, cognite_client=self._cognite_client
            )
            return (loaded[0] if loaded else None) if identifiers.is_singleton() else loaded

        retrieved_items = tasks_summary.joined_results(lambda res: res.json()["items"])

        if identifiers.is_singleton():
            if retrieved_items:
                return resource_cls._load(retrieved_items[0], cognite_client=self._cognite_client)
            else:
                return None
        return list_cls._load(retrieved_items, cognite_client=self._cognite_client)

    # Async generator for listing resources
    async def _list_generator(
        self,
        method: Literal["GET", "POST"],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        url_path: str | None = None,
        limit: int | None = None,
        chunk_size: int | None = None,
        filter: dict[str, Any] | None = None,
        sort: SequenceNotStr[str | dict[str, Any]] | None = None,
        other_params: dict[str, Any] | None = None,
        partitions: int | None = None,
        headers: dict[str, Any] | None = None,
        initial_cursor: str | None = None,
        advanced_filter: dict | Filter | None = None,
        api_subversion: str | None = None,
    ) -> AsyncIterator[T_CogniteResourceList] | AsyncIterator[T_CogniteResource]:
        if partitions:
            warnings.warn("passing `partitions` to a generator method is not supported, so it's being ignored")
            chunk_size = None
        
        limit, url_path, params = self._prepare_params_for_list_generator(
            limit, method, filter, url_path, resource_path, sort, other_params, advanced_filter
        )
        unprocessed_items: list[dict[str, Any]] = []
        total_retrieved, current_limit, next_cursor = 0, self._LIST_LIMIT, initial_cursor
        
        while True:
            if limit and (n_remaining := limit - total_retrieved) < current_limit:
                current_limit = n_remaining

            params.update(limit=current_limit, cursor=next_cursor)
            if method == "GET":
                res = await self._get(url_path=url_path, params=params, headers=headers)
            else:
                res = await self._post(url_path=url_path, json=params, headers=headers, api_subversion=api_subversion)

            response = res.json()
            async for item in self._process_into_chunks(response, chunk_size, resource_cls, list_cls, unprocessed_items):
                yield item

            next_cursor = response.get("nextCursor")
            total_retrieved += len(response["items"])
            if total_retrieved == limit or next_cursor is None:
                if unprocessed_items:
                    yield list_cls._load(unprocessed_items, cognite_client=self._cognite_client)
                break

    async def _process_into_chunks(
        self,
        response: dict[str, Any],
        chunk_size: int | None,
        resource_cls: type[T_CogniteResource],
        list_cls: type[T_CogniteResourceList],
        unprocessed_items: list[dict[str, Any]],
    ) -> AsyncIterator[T_CogniteResourceList] | AsyncIterator[T_CogniteResource]:
        if not chunk_size:
            for item in response["items"]:
                yield resource_cls._load(item, cognite_client=self._cognite_client)
        else:
            unprocessed_items.extend(response["items"])
            if len(unprocessed_items) >= chunk_size:
                chunks = split_into_chunks(unprocessed_items, chunk_size)
                unprocessed_items.clear()
                if chunks and len(chunks[-1]) < chunk_size:
                    unprocessed_items.extend(chunks.pop(-1))
                for chunk in chunks:
                    yield list_cls._load(chunk, cognite_client=self._cognite_client)

    def _prepare_params_for_list_generator(
        self,
        limit: int | None,
        method: Literal["GET", "POST"],
        filter: dict[str, Any] | None,
        url_path: str | None,
        resource_path: str | None,
        sort: SequenceNotStr[str | dict[str, Any]] | None,
        other_params: dict[str, Any] | None,
        advanced_filter: dict | Filter | None,
    ) -> tuple[int | None, str, dict[str, Any]]:
        verify_limit(limit)
        if is_unlimited(limit):
            limit = None
        filter, other_params = (filter or {}).copy(), (other_params or {}).copy()
        if method == "GET":
            url_path = url_path or resource_path or self._RESOURCE_PATH
            if sort is not None:
                filter["sort"] = sort
            filter.update(other_params)
            return limit, url_path, filter

        if method == "POST":
            url_path = url_path or (resource_path or self._RESOURCE_PATH) + "/list"
            body: dict[str, Any] = {}
            if filter:
                body["filter"] = filter
            if advanced_filter:
                if isinstance(advanced_filter, Filter):
                    body["advancedFilter"] = advanced_filter.dump(camel_case_property=True)
                else:
                    body["advancedFilter"] = advanced_filter
            if sort is not None:
                body["sort"] = sort
            body.update(other_params)
            return limit, url_path, body
        raise ValueError(f"_list_generator parameter `method` must be GET or POST, not {method}")

    def _raise_no_project_access_error(self, res: httpx.Response) -> NoReturn:
        raise CogniteProjectAccessError(
            client=self._cognite_client,
            project=self._cognite_client._config.project,
            x_request_id=res.headers.get("X-Request-Id"),
            cluster=self._config.cdf_cluster,
        )

    def _raise_api_error(self, res: httpx.Response, payload: dict) -> NoReturn:
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
        error_details["headers"] = dict(res.request.headers)  # httpx headers don't have copy method
        self._sanitize_headers(error_details["headers"])
        error_details["response_payload"] = shorten(self._get_response_content_safe(res), 500)
        error_details["response_headers"] = dict(res.headers)

        logger.debug(f"HTTP Error {code} {res.request.method} {res.request.url}: {msg}", extra=error_details)
        raise CogniteAPIError(
            message=msg,
            code=code,
            x_request_id=x_request_id,
            missing=missing,
            duplicated=duplicated,
            extra=extra,
            cluster=self._config.cdf_cluster,
            project=self._config.project,
        )

    def _log_request(self, res: httpx.Response, **kwargs: Any) -> None:
        method = res.request.method
        url = res.request.url
        status_code = res.status_code

        extra = kwargs.copy()
        extra["headers"] = dict(res.request.headers)
        self._sanitize_headers(extra["headers"])
        if extra.get("payload") is None:
            extra.pop("payload", None)

        stream = kwargs.get("stream")
        if not stream and self._config.debug is True:
            extra["response_payload"] = shorten(self._get_response_content_safe(res), 500)
        extra["response_headers"] = dict(res.headers)

        logger.debug(f"HTTP/1.1 {method} {url} {status_code}", extra=extra)

    @staticmethod
    def _get_response_content_safe(res: httpx.Response) -> str:
        try:
            return _json.dumps(res.json())
        except Exception:
            pass

        try:
            return res.content.decode()
        except UnicodeDecodeError:
            pass

        return "<binary>"

    @staticmethod
    def _sanitize_headers(headers: dict[str, Any] | None) -> None:
        if headers is None:
            return None
        if "api-key" in headers:
            headers["api-key"] = "***"
        if "Authorization" in headers:
            headers["Authorization"] = "***"