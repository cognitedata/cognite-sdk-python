from __future__ import annotations

import asyncio
import itertools
import logging
import warnings
from collections import UserList
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import (
    Any,
    Literal,
    TypeVar,
    cast,
    overload,
)

from cognite.client._basic_api_client import BasicAsyncAPIClient
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
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import (
    is_unlimited,
    split_into_chunks,
    unpack_items,
    unpack_items_in_payload,
)
from cognite.client.utils._concurrency import AsyncSDKTask, execute_async_tasks
from cognite.client.utils._identifier import (
    Identifier,
    IdentifierCore,
    IdentifierSequence,
    IdentifierSequenceCore,
    SingletonIdentifierSequence,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_camel_case, to_snake_case
from cognite.client.utils._url import interpolate_and_url_encode
from cognite.client.utils._validation import assert_type, verify_limit
from cognite.client.utils.useful_types import SequenceNotStr

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=CogniteObject)

VALID_AGGREGATIONS = {"count", "cardinalityValues", "cardinalityProperties", "uniqueValues", "uniqueProperties"}


class APIClient(BasicAsyncAPIClient):
    _RESOURCE_PATH: str

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

    @overload
    async def _retrieve_multiple(
        self,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        identifiers: SingletonIdentifierSequence,
        resource_path: str | None = None,
        ignore_unknown_ids: bool | None = None,
        headers: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        api_subversion: str | None = None,
        settings_forcing_raw_response_loading: list[str] | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResource | None: ...

    @overload
    async def _retrieve_multiple(
        self,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        identifiers: IdentifierSequenceCore,
        resource_path: str | None = None,
        ignore_unknown_ids: bool | None = None,
        headers: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        api_subversion: str | None = None,
        settings_forcing_raw_response_loading: list[str] | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResourceList: ...

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
        api_subversion: str | None = None,
        settings_forcing_raw_response_loading: list[str] | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResourceList | T_CogniteResource | None:
        resource_path = resource_path or self._RESOURCE_PATH

        ignore_unknown_obj = {} if ignore_unknown_ids is None else {"ignoreUnknownIds": ignore_unknown_ids}
        tasks = [
            AsyncSDKTask(
                self._post,
                url_path=resource_path + "/byids",
                json={"items": id_chunk.as_dicts()} | ignore_unknown_obj | (other_params or {}),
                headers=headers,
                params=params,
                api_subversion=api_subversion,
                semaphore=semaphore,
            )
            for id_chunk in identifiers.chunked(self._RETRIEVE_LIMIT)
        ]
        tasks_summary = await execute_async_tasks(tasks, fail_fast=True)
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
            # The API response include one or more top-level keys than items we care about:
            loaded = list_cls._load_raw_api_response(
                tasks_summary.raw_api_responses, cognite_client=self._cognite_client
            )
            return (loaded[0] if loaded else None) if identifiers.is_singleton() else loaded

        retrieved_items = tasks_summary.joined_results(unpack_items)

        if identifiers.is_singleton():
            if retrieved_items:
                return resource_cls._load(retrieved_items[0], cognite_client=self._cognite_client)
            else:
                # Not all APIs (such as the Data Modeling API) return an error when unknown ids are provided,
                # so we need to handle the unknown singleton identifier case here as well.
                return None
        return list_cls._load(retrieved_items, cognite_client=self._cognite_client)

    @overload
    def _list_generator(
        self,
        method: Literal["GET", "POST"],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        url_path: str | None = None,
        limit: int | None = None,
        chunk_size: None = None,
        filter: dict[str, Any] | None = None,
        sort: SequenceNotStr | None = None,
        other_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        initial_cursor: str | None = None,
        advanced_filter: dict | Filter | None = None,
        api_subversion: str | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> AsyncIterator[T_CogniteResource]: ...

    @overload
    def _list_generator(
        self,
        method: Literal["GET", "POST"],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        url_path: str | None = None,
        limit: int | None = None,
        *,
        chunk_size: int,
        filter: dict[str, Any] | None = None,
        sort: SequenceNotStr | None = None,
        other_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        initial_cursor: str | None = None,
        advanced_filter: dict | Filter | None = None,
        api_subversion: str | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> AsyncIterator[T_CogniteResourceList]: ...

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
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> AsyncIterator[T_CogniteResourceList | T_CogniteResource]:
        if partitions:
            warnings.warn("passing `partitions` to a generator method is not supported, so it's being ignored")
            # set chunk_size to None in order to not break the existing API.
            # TODO: Remove this and support for partitions (in combo with generator) in the next major version
            chunk_size = None
        limit, url_path, params = self._prepare_params_for_list_generator(
            limit, method, filter, url_path, resource_path, sort, other_params, advanced_filter
        )
        unprocessed_items: list[dict[str, Any]] = []
        total_retrieved, current_limit, next_cursor = 0, self._LIST_LIMIT, initial_cursor
        while True:
            if limit and (n_remaining := limit - total_retrieved) < current_limit:
                current_limit = n_remaining

            params["limit"] = current_limit
            if next_cursor is not None:
                params["cursor"] = next_cursor

            if method == "GET":
                res = await self._get(url_path=url_path, params=params, headers=headers, semaphore=semaphore)
            else:
                res = await self._post(
                    url_path=url_path, json=params, headers=headers, api_subversion=api_subversion, semaphore=semaphore
                )

            response = res.json()
            for chunk in self._process_into_chunks(response, chunk_size, resource_cls, list_cls, unprocessed_items):
                yield chunk

            next_cursor = response.get("nextCursor")
            total_retrieved += len(response["items"])
            if total_retrieved == limit or next_cursor is None:
                if unprocessed_items:  # may only happen when -not- yielding one-by-one
                    yield list_cls._load(unprocessed_items, cognite_client=self._cognite_client)
                break

    async def _list_generator_raw_responses(
        self,
        method: Literal["GET", "POST"],
        settings_forcing_raw_response_loading: list[str],
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
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        if partitions:
            raise ValueError("When fetching additional data (besides items), using partitions is not supported")
        if not chunk_size:
            raise ValueError(
                f"When fetching additional data (besides items), {chunk_size=} must match the "
                f"API limit: {self._LIST_LIMIT}"
            )
        if chunk_size != self._LIST_LIMIT:
            warnings.warn(
                f"When fetching additional data (besides items), an arbitrary {chunk_size=} setting is "
                f"not supported, only {self._LIST_LIMIT} (the API limit). This is caused by the following "
                f"settings: {settings_forcing_raw_response_loading}.",
                UserWarning,
            )
        limit, url_path, params = self._prepare_params_for_list_generator(
            limit, method, filter, url_path, resource_path, sort, other_params, advanced_filter
        )
        total_retrieved, current_limit, next_cursor = 0, self._LIST_LIMIT, initial_cursor
        while True:
            if limit and (n_remaining := limit - total_retrieved) < current_limit:
                current_limit = n_remaining

            params.update(limit=current_limit, cursor=next_cursor)
            if method == "GET":
                res = await self._get(url_path=url_path, params=params, headers=headers, semaphore=semaphore)
            else:
                res = await self._post(
                    url_path=url_path, json=params, headers=headers, api_subversion=api_subversion, semaphore=semaphore
                )

            yield (response := res.json())
            next_cursor = response.get("nextCursor")
            total_retrieved += len(response["items"])
            if total_retrieved == limit or next_cursor is None:
                break

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
            if advanced_filter is not None:
                if isinstance(advanced_filter, Filter):
                    # TODO: Does our json.dumps now understand Filter?
                    body["advancedFilter"] = advanced_filter.dump(camel_case_property=True)
                else:
                    body["advancedFilter"] = advanced_filter
            if sort is not None:
                body["sort"] = sort
            body.update(other_params)
            return limit, url_path, body
        raise ValueError(f"_list_generator parameter `method` must be GET or POST, not {method}")

    @overload
    def _process_into_chunks(
        self,
        response: dict[str, Any],
        chunk_size: None,
        resource_cls: type[T_CogniteResource],
        list_cls: type[T_CogniteResourceList],
        unprocessed_items: list[dict[str, Any]],
    ) -> Iterator[T_CogniteResource]: ...

    @overload
    def _process_into_chunks(
        self,
        response: dict[str, Any],
        chunk_size: int,
        resource_cls: type[T_CogniteResource],
        list_cls: type[T_CogniteResourceList],
        unprocessed_items: list[dict[str, Any]],
    ) -> Iterator[T_CogniteResourceList]: ...

    def _process_into_chunks(
        self,
        response: dict[str, Any],
        chunk_size: int | None,
        resource_cls: type[T_CogniteResource],
        list_cls: type[T_CogniteResourceList],
        unprocessed_items: list[dict[str, Any]],
    ) -> Iterator[T_CogniteResourceList | T_CogniteResource]:
        if chunk_size is None:
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

    async def _list(
        self,
        method: Literal["POST", "GET"],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        resource_path: str | None = None,
        url_path: str | None = None,
        limit: int | None = None,
        filter: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        partitions: int | None = None,
        sort: SequenceNotStr[str | dict[str, Any]] | None = None,
        headers: dict[str, Any] | None = None,
        initial_cursor: str | None = None,
        advanced_filter: dict | Filter | None = None,
        api_subversion: str | None = None,
        settings_forcing_raw_response_loading: list[str] | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResourceList:
        verify_limit(limit)
        if partitions:
            if not is_unlimited(limit):
                raise ValueError(
                    "When using partitions, a finite limit can not be used. Pass one of `None`, `-1` or `inf`."
                )
            if sort is not None:
                raise ValueError("When using sort, partitions is not supported.")
            if settings_forcing_raw_response_loading:
                raise ValueError(
                    "When using partitions, the following settings are not "
                    f"supported (yet): {settings_forcing_raw_response_loading}"
                )
            assert initial_cursor is api_subversion is None
            return await self._list_partitioned(
                partitions=partitions,
                method=method,
                list_cls=list_cls,
                resource_path=resource_path,
                filter=filter,
                advanced_filter=advanced_filter,
                other_params=other_params,
                headers=headers,
                semaphore=semaphore,
            )
        fetch_kwargs = dict(
            resource_path=resource_path or self._RESOURCE_PATH,
            url_path=url_path,
            limit=limit,
            chunk_size=self._LIST_LIMIT,
            filter=filter,
            sort=sort,
            other_params=other_params,
            headers=headers,
            initial_cursor=initial_cursor,
            advanced_filter=advanced_filter,
            api_subversion=api_subversion,
            semaphore=semaphore,
        )
        if settings_forcing_raw_response_loading:
            raw_response_fetcher = self._list_generator_raw_responses(
                method,
                settings_forcing_raw_response_loading,
                **fetch_kwargs,  # type: ignore [arg-type]
            )
            return list_cls._load_raw_api_response(
                [r async for r in raw_response_fetcher],
                cognite_client=self._cognite_client,
            )
        # TODO: List generator loads each chunk into 'list_cls', so kind of weird for us to chain
        #       elements, then do it again. Perhaps a modified version of 'raw responses' should be used:
        resource_lists = [rl async for rl in self._list_generator(method, list_cls, resource_cls, **fetch_kwargs)]
        return list_cls(
            list(itertools.chain.from_iterable(resource_lists)),
            cognite_client=self._cognite_client,
        )

    async def _get_partition(
        self,
        method: Literal["POST", "GET"],
        partition: int,
        other_params: dict[str, Any],
        advanced_filter: dict | None,
        url_path: str,
        headers: dict[str, Any] | None,
        filter: dict[str, Any],
        semaphore: asyncio.BoundedSemaphore | None,
    ) -> list[dict[str, Any]]:
        next_cursor = None
        retrieved_items = []
        while True:
            if method == "POST":
                body = {
                    "filter": filter,
                    "limit": self._LIST_LIMIT,
                    "cursor": next_cursor,
                    "partition": partition,
                } | other_params
                if advanced_filter is not None:
                    body["advancedFilter"] = advanced_filter
                res = await self._post(url_path=url_path + "/list", json=body, headers=headers, semaphore=semaphore)

            elif method == "GET":
                params = (
                    filter | {"limit": self._LIST_LIMIT, "cursor": next_cursor, "partition": partition} | other_params
                )
                res = await self._get(url_path=url_path, params=params, headers=headers)
            else:
                raise ValueError(f"Unsupported method: {method}")

            retrieved_items.extend(unpack_items(res))
            next_cursor = res.json().get("nextCursor")
            if next_cursor is None:
                break
        return retrieved_items

    async def _list_partitioned(
        self,
        partitions: int,
        method: Literal["POST", "GET"],
        list_cls: type[T_CogniteResourceList],
        resource_path: str | None = None,
        filter: dict[str, Any] | None = None,
        other_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        advanced_filter: dict | Filter | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResourceList:
        if isinstance(advanced_filter, Filter):
            advanced_filter = advanced_filter.dump(camel_case_property=True)
        tasks = [
            AsyncSDKTask(
                self._get_partition,
                method,
                partition=f"{i}/{partitions}",
                other_params=other_params or {},
                advanced_filter=advanced_filter,
                url_path=resource_path or self._RESOURCE_PATH,
                headers=headers,
                filter=filter or {},
                semaphore=semaphore,
            )
            for i in range(1, partitions + 1)
        ]
        tasks_summary = await execute_async_tasks(tasks, fail_fast=True)
        tasks_summary.raise_compound_exception_if_failed_tasks()

        return list_cls._load(tasks_summary.joined_results(), cognite_client=self._cognite_client)

    async def _aggregate(
        self,
        cls: type[T],
        resource_path: str | None = None,
        filter: CogniteFilter | dict[str, Any] | None = None,
        aggregate: str | None = None,
        fields: SequenceNotStr[str] | None = None,
        keys: SequenceNotStr[str] | None = None,
        headers: dict[str, Any] | None = None,
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
        res = await self._post(url_path=resource_path + "/aggregate", json=body, headers=headers)
        return [cls._load(agg) for agg in unpack_items(res)]

    @overload
    async def _advanced_aggregate(
        self,
        aggregate: Literal["count", "cardinalityValues", "cardinalityProperties"],
        properties: EnumProperty
        | str
        | list[str]
        | tuple[EnumProperty | str | list[str], AggregationFilter]
        | None = None,
        path: EnumProperty | str | list[str] | None = None,
        query: str | None = None,
        filter: CogniteFilter | dict[str, Any] | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = None,
        api_subversion: str | None = None,
    ) -> int: ...

    @overload
    async def _advanced_aggregate(
        self,
        aggregate: Literal["uniqueValues", "uniqueProperties"],
        properties: EnumProperty
        | str
        | list[str]
        | tuple[EnumProperty | str | list[str], AggregationFilter]
        | None = None,
        path: EnumProperty | str | list[str] | None = None,
        query: str | None = None,
        filter: CogniteFilter | dict[str, Any] | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = None,
        api_subversion: str | None = None,
    ) -> UniqueResultList: ...

    async def _advanced_aggregate(
        self,
        aggregate: Literal["count", "cardinalityValues", "cardinalityProperties", "uniqueValues", "uniqueProperties"],
        properties: EnumProperty
        | str
        | list[str]
        | tuple[EnumProperty | str | list[str], AggregationFilter]
        | None = None,
        path: EnumProperty | str | list[str] | None = None,
        query: str | None = None,
        filter: CogniteFilter | dict[str, Any] | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = None,
        api_subversion: str | None = None,
    ) -> int | UniqueResultList:
        verify_limit(limit)
        if aggregate not in VALID_AGGREGATIONS:
            raise ValueError(f"Invalid aggregate {aggregate!r}. Valid aggregates are {sorted(VALID_AGGREGATIONS)}.")

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
                dumped_properties = [to_camel_case(properties[0])] if len(properties) == 1 else properties
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
            body["filter"] = dumped_filter

        if advanced_filter is not None:
            body["advancedFilter"] = advanced_filter.dump() if isinstance(advanced_filter, Filter) else advanced_filter

        if aggregate_filter is not None:
            body["aggregateFilter"] = (
                aggregate_filter.dump() if isinstance(aggregate_filter, AggregationFilter) else aggregate_filter
            )
        if limit is not None:
            body["limit"] = limit

        res = await self._post(url_path=f"{self._RESOURCE_PATH}/aggregate", json=body, api_subversion=api_subversion)
        json_items = unpack_items(res)
        if aggregate in {"count", "cardinalityValues", "cardinalityProperties"}:
            return json_items[0]["count"]
        elif aggregate in {"uniqueValues", "uniqueProperties"}:
            return UniqueResultList._load(json_items, cognite_client=self._cognite_client)
        else:
            raise ValueError(f"Unknown aggregate: {aggregate}")

    @overload
    async def _create_multiple(
        self,
        items: Sequence[WriteableCogniteResource] | Sequence[dict[str, Any]],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_WritableCogniteResource],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        extra_body_fields: dict[str, Any] | None = None,
        limit: int | None = None,
        input_resource_cls: type[CogniteResource] | None = None,
        api_subversion: str | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResourceList: ...

    @overload
    async def _create_multiple(
        self,
        items: WriteableCogniteResource | dict[str, Any],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_WritableCogniteResource],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        extra_body_fields: dict[str, Any] | None = None,
        limit: int | None = None,
        input_resource_cls: type[CogniteResource] | None = None,
        api_subversion: str | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_WritableCogniteResource: ...

    async def _create_multiple(
        self,
        items: Sequence[WriteableCogniteResource]
        | Sequence[dict[str, Any]]
        | WriteableCogniteResource
        | dict[str, Any],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_WritableCogniteResource],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        extra_body_fields: dict[str, Any] | None = None,
        limit: int | None = None,
        input_resource_cls: type[CogniteResource] | None = None,
        api_subversion: str | None = None,
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> T_CogniteResourceList | T_WritableCogniteResource:
        resource_path = resource_path or self._RESOURCE_PATH
        input_resource_cls = input_resource_cls or resource_cls
        limit = limit or self._CREATE_LIMIT
        single_item = not isinstance(items, Sequence)
        if single_item:
            items = cast(Sequence[T_WritableCogniteResource] | Sequence[dict[str, Any]], [items])
        else:
            items = cast(Sequence[T_WritableCogniteResource] | Sequence[dict[str, Any]], items)

        items = [item.as_write() if isinstance(item, WriteableCogniteResource) else item for item in items]

        tasks = [
            AsyncSDKTask(
                self._post,
                resource_path,
                task_items,
                params,
                headers,
                api_subversion=api_subversion,
                semaphore=semaphore,
            )
            for task_items in self._prepare_item_chunks(items, limit, extra_body_fields)
        ]
        summary = await execute_async_tasks(tasks)

        def task_unwrap_fn(task: AsyncSDKTask) -> Any:
            return task[1]["items"]

        def task_list_element_unwrap_fn(el: T) -> CogniteResource | T:
            if isinstance(el, dict):
                return input_resource_cls._load(el, cognite_client=self._cognite_client)
            return el

        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=task_unwrap_fn,
            task_list_element_unwrap_fn=task_list_element_unwrap_fn,
        )
        created_resources = summary.joined_results(unpack_items)

        if single_item:
            return resource_cls._load(created_resources[0], cognite_client=self._cognite_client)
        return list_cls._load(created_resources, cognite_client=self._cognite_client)

    async def _delete_multiple(
        self,
        identifiers: IdentifierSequenceCore,
        wrap_ids: bool,
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        extra_body_fields: dict[str, Any] | None = None,
        returns_items: bool = False,
        delete_endpoint: str = "/delete",
        semaphore: asyncio.BoundedSemaphore | None = None,
    ) -> list | None:
        resource_path = (resource_path or self._RESOURCE_PATH) + delete_endpoint
        extra_body_fields = extra_body_fields or {}
        tasks = [
            AsyncSDKTask(
                self._post,
                url_path=resource_path,
                json={"items": chunk.as_dicts() if wrap_ids else chunk.as_primitives()} | extra_body_fields,
                params=params,
                headers=headers,
                semaphore=semaphore,
            )
            for chunk in identifiers.chunked(self._DELETE_LIMIT)
        ]
        summary = await execute_async_tasks(tasks)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
            task_list_element_unwrap_fn=identifiers.unwrap_identifier,
        )
        if returns_items:
            return summary.joined_results(unpack_items)
        return None

    @overload
    async def _update_multiple(
        self,
        items: CogniteResource | CogniteUpdate | WriteableCogniteResource,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        api_subversion: str | None = None,
        cdf_item_by_id: Mapping[Any, T_CogniteResource] | None = None,
    ) -> T_CogniteResource: ...

    @overload
    async def _update_multiple(
        self,
        items: Sequence[CogniteResource | CogniteUpdate | WriteableCogniteResource],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        api_subversion: str | None = None,
        cdf_item_by_id: Mapping[Any, T_CogniteResource] | None = None,
    ) -> T_CogniteResourceList: ...

    async def _update_multiple(
        self,
        items: Sequence[CogniteResource | CogniteUpdate | WriteableCogniteResource]
        | CogniteResource
        | CogniteUpdate
        | WriteableCogniteResource,
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_CogniteResource],
        update_cls: type[CogniteUpdate],
        resource_path: str | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
        api_subversion: str | None = None,
        cdf_item_by_id: Mapping[Any, T_CogniteResource] | None = None,
    ) -> T_CogniteResourceList | T_CogniteResource:
        resource_path = resource_path or self._RESOURCE_PATH
        patch_objects = []
        single_item = not isinstance(items, (Sequence, UserList))
        if single_item:
            item_list = cast(Sequence[CogniteResource] | Sequence[CogniteUpdate], [items])
        else:
            item_list = cast(Sequence[CogniteResource] | Sequence[CogniteUpdate], items)

        for index, item in enumerate(item_list):
            if isinstance(item, CogniteResource):
                patch_objects.append(
                    self._convert_resource_to_patch_object(
                        item, update_cls._get_update_properties(item), mode, cdf_item_by_id
                    )
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
            AsyncSDKTask(
                self._post,
                url_path=resource_path + "/update",
                json={"items": chunk},
                params=params,
                headers=headers,
                api_subversion=api_subversion,
            )
            for chunk in patch_object_chunks
        ]
        tasks_summary = await execute_async_tasks(tasks)
        tasks_summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
            task_list_element_unwrap_fn=IdentifierSequenceCore.unwrap_identifier,
        )
        updated_items = tasks_summary.joined_results(unpack_items)

        if single_item:
            return resource_cls._load(updated_items[0], cognite_client=self._cognite_client)
        return list_cls._load(updated_items, cognite_client=self._cognite_client)

    async def _upsert_multiple(
        self,
        items: WriteableCogniteResource | Sequence[WriteableCogniteResource],
        list_cls: type[T_CogniteResourceList],
        resource_cls: type[T_WritableCogniteResource],
        update_cls: type[CogniteUpdate],
        mode: Literal["patch", "replace"],
        input_resource_cls: type[CogniteResource] | None = None,
        api_subversion: str | None = None,
        cdf_item_by_id: Mapping[Any, T_CogniteResource] | None = None,
    ) -> T_WritableCogniteResource | T_CogniteResourceList:
        if mode not in ["patch", "replace"]:
            raise ValueError(f"mode must be either 'patch' or 'replace', got {mode!r}")
        is_single = isinstance(items, WriteableCogniteResource)
        items = cast(Sequence[T_WritableCogniteResource], [items] if is_single else items)
        try:
            result = await self._update_multiple(
                items,
                list_cls,
                resource_cls,
                update_cls,
                mode=mode,
                api_subversion=api_subversion,
                cdf_item_by_id=cast(Mapping | None, cdf_item_by_id),
            )
        except CogniteNotFoundError as not_found_error:
            items_by_external_id = {item.external_id: item for item in items if item.external_id is not None}  # type: ignore [attr-defined]
            items_by_id = {item.id: item for item in items if hasattr(item, "id") and item.id is not None}
            # Not found must have an external id as they do not exist in CDF:
            try:
                missing_external_ids = {entry["externalId"] for entry in not_found_error.missing}
            except KeyError:
                # There is a not found internal id, which means we cannot identify it.
                raise not_found_error
            to_create = [items_by_external_id[xid] for xid in not_found_error.failed if xid in missing_external_ids]

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
                    created = await self._create_multiple(
                        to_create,
                        list_cls=list_cls,
                        resource_cls=resource_cls,
                        input_resource_cls=input_resource_cls,
                        api_subversion=api_subversion,
                    )
                if to_update:
                    updated = await self._update_multiple(
                        to_update,
                        list_cls=list_cls,
                        resource_cls=resource_cls,
                        update_cls=update_cls,
                        mode=mode,
                        api_subversion=api_subversion,
                        cdf_item_by_id=cast(Mapping | None, cdf_item_by_id),
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
                    api_error.message,
                    code=api_error.code,
                    successful=successful,
                    failed=failed,
                    unknown=unknown,
                    cluster=self._config.cdf_cluster,
                    project=self._config.project,
                )
            # Need to retrieve the successful updated items from the first call.
            successful_resources: T_CogniteResourceList | None = None
            if not_found_error.successful:
                identifiers = IdentifierSequence.of(*not_found_error.successful)
                successful_resources = await self._retrieve_multiple(
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
                    **Identifier.load(getattr(item, "id", None), item.external_id).as_dict(  # type: ignore [attr-defined]
                        camel_case=False
                    )
                )
                for item in items
            ]
        if is_single:
            return result[0]
        return result

    async def _search(
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
        verify_limit(limit)
        assert_type(filter, "filter", [dict, CogniteFilter], allow_none=True)
        if isinstance(filter, CogniteFilter):
            filter = filter.dump(camel_case=True)
        elif isinstance(filter, dict):
            filter = convert_all_keys_to_camel_case(filter)
        resource_path = resource_path or self._RESOURCE_PATH
        res = await self._post(
            url_path=resource_path + "/search",
            json={"search": search, "filter": filter, "limit": limit},
            params=params,
            headers=headers,
            api_subversion=api_subversion,
        )
        return list_cls._load(unpack_items(res), cognite_client=self._cognite_client)

    @staticmethod
    def _prepare_item_chunks(
        items: Sequence[T_CogniteResource] | Sequence[dict[str, Any]],
        limit: int,
        extra_body_fields: dict[str, Any] | None,
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
        cdf_item_by_id: Mapping[Any, T_CogniteResource] | None = None,
    ) -> dict[str, dict[str, dict]]:
        dumped = resource.dump(camel_case=True)

        patch_object: dict[str, dict[str, dict]] = {"update": {}}
        if "instanceId" in dumped:
            patch_object["instanceId"] = dumped.pop("instanceId")
            dumped.pop("id", None)
        elif "id" in dumped:
            patch_object["id"] = dumped.pop("id")
        elif "externalId" in dumped:
            patch_object["externalId"] = dumped.pop("externalId")

        update: dict[str, dict] = cls._clear_all_attributes(update_attributes) if mode == "replace" else {}

        update_attribute_by_name = {prop.name: prop for prop in update_attributes}
        for key, value in dumped.items():
            if (snake := to_snake_case(key)) not in update_attribute_by_name:
                continue
            prop = update_attribute_by_name[snake]
            if (prop.is_list or prop.is_object) and mode == "patch":
                update[key] = {"add": value}
            else:
                update[key] = {"set": value}

        patch_object["update"] = update
        return patch_object

    @staticmethod
    def _clear_all_attributes(update_attributes: list[PropertySpec]) -> dict[str, dict]:
        cleared = {}
        for prop in update_attributes:
            if prop.is_beta:
                continue
            elif prop.is_explicit_nullable_object:
                clear_with: dict = {"setNull": True}
            elif prop.is_object:
                clear_with = {"set": {}}
            elif prop.is_list:
                clear_with = {"set": []}
            elif prop.is_nullable:
                clear_with = {"setNull": True}
            else:
                continue
            cleared[to_camel_case(prop.name)] = clear_with
        return cleared
