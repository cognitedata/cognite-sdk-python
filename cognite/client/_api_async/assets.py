from __future__ import annotations

import functools
import heapq
import itertools
import math
import threading
import warnings
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Sequence
from functools import cached_property
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    NoReturn,
    TypeAlias,
    cast,
    overload,
)

from cognite.client._async_api_client import AsyncAPIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    AssetHierarchy,
    AssetList,
    AssetUpdate,
    CountAggregate,
    GeoLocationFilter,
    LabelFilter,
    TimestampRange,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.assets import (
    AssetCore,
    AssetPropertyLike,
    AssetSort,
    AssetWrite,
    SortableAssetProperty,
)
from cognite.client.data_classes.filters import _BASIC_FILTERS, Filter, _validate_filter
from cognite.client.exceptions import CogniteAPIError, CogniteMultiException
from cognite.client.utils._auxiliary import split_into_chunks, split_into_n_parts
from cognite.client.utils._concurrency import ConcurrencySettings, classify_error, execute_tasks_async
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._importing import import_as_completed
from cognite.client.utils._text import to_camel_case
from cognite.client.utils._validation import (
    assert_type,
    prepare_filter_sort,
    process_asset_subtree_ids,
    process_data_set_ids,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from concurrent.futures import Future, ThreadPoolExecutor

as_completed = import_as_completed()

AggregateAssetProperty: TypeAlias = Literal["child_count", "path", "depth"]

SortSpec: TypeAlias = (
    AssetSort
    | str
    | SortableAssetProperty
    | tuple[str, Literal["asc", "desc"]]
    | tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]]
)

_FILTERS_SUPPORTED: frozenset[type[Filter]] = _BASIC_FILTERS | {filters.Search}


class AsyncAssetsAPI(AsyncAPIClient):
    _RESOURCE_PATH = "/assets"

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: TimestampRange | dict[str, Any] | None = None,
        last_updated_time: TimestampRange | dict[str, Any] | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        aggregated_properties: Sequence[AggregateAssetProperty] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> AsyncIterator[Asset]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: TimestampRange | dict[str, Any] | None = None,
        last_updated_time: TimestampRange | dict[str, Any] | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        aggregated_properties: Sequence[AggregateAssetProperty] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> AsyncIterator[AssetList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        metadata: dict[str, str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: TimestampRange | dict[str, Any] | None = None,
        last_updated_time: TimestampRange | dict[str, Any] | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        aggregated_properties: Sequence[AggregateAssetProperty] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> AsyncIterator[Asset] | AsyncIterator[AssetList]:
        """Async iterator over assets"""
        agg_props = self._process_aggregated_props(aggregated_properties)
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = AssetFilter(
            name=name,
            parent_ids=parent_ids,
            parent_external_ids=parent_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            labels=labels,
            geo_location=geo_location,
            metadata=metadata,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            root=root,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, AssetSort)
        self._validate_filter(advanced_filter)

        return self._list_generator(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            other_params=agg_props,
            partitions=partitions,
        )

    def __aiter__(self) -> AsyncIterator[Asset]:
        """Async iterate over all assets."""
        return self.__call__()

    async def retrieve(self, id: int | None = None, external_id: str | None = None) -> Asset | None:
        """`Retrieve a single asset by id. <https://developer.cognite.com/api#tag/Assets/operation/byIdsAssets>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Asset | None: Requested asset or None if it does not exist.

        Examples:

            Get asset by id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.assets.retrieve(id=1)

            Get asset by external id::

                >>> res = await client.assets.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return await self._retrieve_multiple(
            list_cls=AssetList,
            resource_cls=Asset,
            identifiers=identifiers,
        )

    async def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> AssetList:
        """`Retrieve multiple assets by id. <https://developer.cognite.com/api#tag/Assets/operation/byIdsAssets>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            AssetList: The retrieved assets.

        Examples:

            Get assets by id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.assets.retrieve_multiple(ids=[1, 2, 3])

            Get assets by external id::

                >>> res = await client.assets.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids, external_ids)
        return await self._retrieve_multiple(
            list_cls=AssetList,
            resource_cls=Asset,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def aggregate(self, filter: AssetFilter | dict[str, Any] | None = None) -> list[CountAggregate]:
        """`Aggregate assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            filter (AssetFilter | dict[str, Any] | None): Filter on assets with strict matching.

        Returns:
            list[CountAggregate]: List of asset aggregates

        Examples:

            Aggregate assets::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> aggregate_root = await client.assets.aggregate(filter={"root": True})
        """

        return await self._aggregate(
            cls=CountAggregate,
            resource_path=self._RESOURCE_PATH,
            filter=filter,
        )

    async def aggregate_count(
        self,
        filter: AssetFilter | dict[str, Any] | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
    ) -> int:
        """`Count of assets matching the specified filters and search. <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            filter (AssetFilter | dict[str, Any] | None): Filter on assets with strict matching.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL (Domain Specific Language).

        Returns:
            int: Count of assets matching the specified filters and search.

        Examples:

            Count assets::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> count = await client.assets.aggregate_count(filter={"root": True})
        """
        return await self._advanced_aggregate(
            aggregate="count",
            filter=filter,
            advanced_filter=advanced_filter,
        )

    async def list(
        self,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: SequenceNotStr[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | SequenceNotStr[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | SequenceNotStr[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        aggregated_properties: Sequence[AggregateAssetProperty] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        advanced_filter: Filter | dict[str, Any] | None = None,
        sort: SortSpec | list[SortSpec] | None = None,
    ) -> AssetList:
        """`List assets <https://developer.cognite.com/api#tag/Assets/operation/listAssets>`_

        Args:
            name (str | None): Name of asset. Often referred to as tag.
            parent_ids (Sequence[int] | None): Return only the direct descendants of the specified assets.
            parent_external_ids (SequenceNotStr[str] | None): Return only the direct descendants of the specified assets.
            asset_subtree_ids (int | Sequence[int] | None): Only include assets in subtrees rooted at any of the specified assetIds.
            asset_subtree_external_ids (str | SequenceNotStr[str] | None): Only include assets in subtrees rooted at any of the specified assetExternalIds.
            data_set_ids (int | Sequence[int] | None): Return only assets in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | SequenceNotStr[str] | None): Return only assets in the specified data set(s) with this external id / these external ids.
            labels (LabelFilter | None): Return only the assets matching the specified label filter.
            geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
            metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value.
            source (str | None): The source of this asset.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps.
            root (bool | None): filtered assets are root assets or not.
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            aggregated_properties (Sequence[AggregateAssetProperty] | None): Set of aggregated properties to include.
            partitions (int | None): Retrieve resources in parallel using this number of workers.
            limit (int | None): Maximum number of assets to return. Defaults to 25.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by.

        Returns:
            AssetList: List of requested assets

        Examples:

            List assets::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> asset_list = await client.assets.list(limit=5)

            Filter assets based on labels::

                >>> from cognite.client.data_classes import LabelFilter
                >>> my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])
                >>> asset_list = await client.assets.list(labels=my_label_filter)
        """
        agg_props = self._process_aggregated_props(aggregated_properties)
        asset_subtree_ids_processed = process_asset_subtree_ids(asset_subtree_ids, asset_subtree_external_ids)
        data_set_ids_processed = process_data_set_ids(data_set_ids, data_set_external_ids)

        filter = AssetFilter(
            name=name,
            parent_ids=parent_ids,
            parent_external_ids=parent_external_ids,
            asset_subtree_ids=asset_subtree_ids_processed,
            data_set_ids=data_set_ids_processed,
            labels=labels,
            geo_location=geo_location,
            metadata=metadata,
            source=source,
            created_time=created_time,
            last_updated_time=last_updated_time,
            root=root,
            external_id_prefix=external_id_prefix,
        ).dump(camel_case=True)

        prep_sort = prepare_filter_sort(sort, AssetSort)
        self._validate_filter(advanced_filter)

        return await self._list(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            limit=limit,
            filter=filter,
            advanced_filter=advanced_filter,
            sort=prep_sort,
            other_params=agg_props,
            partitions=partitions,
        )

    @overload
    async def create(self, asset: Sequence[Asset] | Sequence[AssetWrite]) -> AssetList: ...

    @overload
    async def create(self, asset: Asset | AssetWrite) -> Asset: ...

    async def create(self, asset: Asset | AssetWrite | Sequence[Asset] | Sequence[AssetWrite]) -> Asset | AssetList:
        """`Create one or more assets. <https://developer.cognite.com/api#tag/Assets/operation/createAssets>`_

        Args:
            asset (Asset | AssetWrite | Sequence[Asset] | Sequence[AssetWrite]): Asset or list of assets to create.

        Returns:
            Asset | AssetList: Created asset(s)

        Examples:

            Create new asset::

                >>> from cognite.client import AsyncCogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> client = AsyncCogniteClient()
                >>> assets = [Asset(name="asset1"), Asset(name="asset2")]
                >>> res = await client.assets.create(assets)
        """
        return await self._create_multiple(
            list_cls=AssetList,
            resource_cls=Asset,
            items=asset,
        )

    async def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | SequenceNotStr[str] | None = None,
        recursive: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more assets <https://developer.cognite.com/api#tag/Assets/operation/deleteAssets>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | SequenceNotStr[str] | None): External ID or list of external ids
            recursive (bool): Recursively delete whole asset subtrees under given asset(s). Defaults to False.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None

        Examples:

            Delete assets by id or external id::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> await client.assets.delete(id=[1,2,3], external_id="3")
        """
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(id, external_id),
            wrap_ids=True,
            extra_body_fields={"recursive": recursive, "ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    async def update(self, item: Sequence[Asset | AssetUpdate]) -> AssetList: ...

    @overload
    async def update(self, item: Asset | AssetUpdate) -> Asset: ...

    async def update(self, item: Asset | AssetUpdate | Sequence[Asset | AssetUpdate]) -> Asset | AssetList:
        """`Update one or more assets <https://developer.cognite.com/api#tag/Assets/operation/updateAssets>`_

        Args:
            item (Asset | AssetUpdate | Sequence[Asset | AssetUpdate]): Asset(s) to update

        Returns:
            Asset | AssetList: Updated asset(s)

        Examples:

            Update an asset that you have fetched. This will perform a full update of the asset::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> asset = await client.assets.retrieve(id=1)
                >>> asset.description = "New description"
                >>> res = await client.assets.update(asset)

            Perform a partial update on an asset, updating the description and adding a new field to metadata::

                >>> from cognite.client.data_classes import AssetUpdate
                >>> my_update = AssetUpdate(id=1).description.set("New description").metadata.set({"key": "value"})
                >>> res = await client.assets.update(my_update)
        """
        return await self._update_multiple(
            list_cls=AssetList,
            resource_cls=Asset,
            update_cls=AssetUpdate,
            items=item,
        )

    @overload
    async def upsert(self, item: Sequence[Asset | AssetWrite], mode: Literal["patch", "replace"] = "patch") -> AssetList: ...

    @overload 
    async def upsert(self, item: Asset | AssetWrite, mode: Literal["patch", "replace"] = "patch") -> Asset: ...

    async def upsert(
        self,
        item: Asset | AssetWrite | Sequence[Asset | AssetWrite],
        mode: Literal["patch", "replace"] = "patch",
    ) -> Asset | AssetList:
        """`Upsert assets <https://developer.cognite.com/api#tag/Assets/operation/createAssets>`_

        Args:
            item (Asset | AssetWrite | Sequence[Asset | AssetWrite]): Asset or list of assets to upsert.
            mode (Literal["patch", "replace"]): Whether to patch or replace in the case the assets are existing.

        Returns:
            Asset | AssetList: The upserted asset(s).

        Examples:

            Upsert for assets::

                >>> from cognite.client import AsyncCogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> client = AsyncCogniteClient()
                >>> existing_asset = await client.assets.retrieve(id=1)
                >>> existing_asset.description = "New description"
                >>> new_asset = Asset(external_id="new_asset", name="new_asset")
                >>> res = await client.assets.upsert([existing_asset, new_asset], mode="replace")
        """
        return await self._upsert_multiple(
            items=item,
            list_cls=AssetList,
            resource_cls=Asset,
            update_cls=AssetUpdate,
            mode=mode,
        )

    async def filter(
        self,
        filter: Filter | dict,
        sort: SortSpec | list[SortSpec] | None = None,
        aggregated_properties: Sequence[AggregateAssetProperty] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> AssetList:
        """`Advanced filter assets <https://developer.cognite.com/api#tag/Assets/operation/listAssets>`_

        Advanced filter lets you create complex filtering expressions that combine simple operations,
        such as equals, prefix, exists, etc., using boolean operators and, or, and not.
        It applies to basic fields as well as metadata.

        Args:
            filter (Filter | dict): Filter to apply.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by.
            aggregated_properties (Sequence[AggregateAssetProperty] | None): Set of aggregated properties to include.
            limit (int | None): Maximum number of results to return.

        Returns:
            AssetList: List of assets that match the filter criteria.
        """
        warnings.warn(
            f"{self.__class__.__name__}.filter() method is deprecated and will be removed in the next major version of the SDK. Please use the {self.__class__.__name__}.list() method with advanced_filter parameter instead.",
            DeprecationWarning,
        )
        self._validate_filter(filter)
        agg_props = self._process_aggregated_props(aggregated_properties)
        return await self._list(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case_property=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, AssetSort),
            other_params=agg_props,
        )

    async def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: AssetFilter | dict[str, Any] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> AssetList:
        """`Search for assets <https://developer.cognite.com/api#tag/Assets/operation/searchAssets>`_

        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and
        ordering may change over time. Use the `list` or `aggregate` method instead if you want to stable
        and performant iteration over all assets.

        Args:
            name (str | None): Prefix and fuzzy search on name.
            description (str | None): Prefix and fuzzy search on description.
            query (str | None): Search on name and description using wildcard search on each of the words (separated by spaces).
            filter (AssetFilter | dict[str, Any] | None): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            AssetList: Search results

        Examples:

            Search for assets by fuzzy search on name::

                >>> from cognite.client import AsyncCogniteClient
                >>> client = AsyncCogniteClient()
                >>> res = await client.assets.search(name="some name")

            Search for assets by query::

                >>> res = await client.assets.search(query="TAG_30_X*")

            Search for assets by name and filter on external_id_prefix::

                >>> res = await client.assets.search(name="some name", filter=AssetFilter(external_id_prefix="big"))
        """
        return await self._search(
            list_cls=AssetList,
            search={
                "name": name,
                "description": description,
                "query": query,
            },
            filter=filter or {},
            limit=limit,
        )

    async def retrieve_subtree(
        self, id: int | None = None, external_id: str | None = None, depth: int | None = None
    ) -> AssetList:
        """Retrieve the subtree for this asset up to a specified depth.

        Args:
            id (int | None): Id of the root asset in the subtree.
            external_id (str | None): External id of the root asset in the subtree.
            depth (int | None): Retrieve assets up to this depth below the root asset in the subtree.

        Returns:
            AssetList: The requested assets or empty AssetList if asset does not exist.
        """
        asset = await self.retrieve(id=id, external_id=external_id)
        if asset is None:
            return AssetList([], self._cognite_client)
        subtree = await self._get_asset_subtree([asset], current_depth=0, depth=depth)
        return AssetList(subtree, self._cognite_client)

    async def _get_asset_subtree(self, assets: list, current_depth: int, depth: int | None) -> list:
        subtree = assets
        if depth is None or current_depth < depth:
            if children := await self._get_children(subtree):
                children_subtree = await self._get_asset_subtree(children, current_depth + 1, depth)
                subtree.extend(children_subtree)
        return subtree

    async def _get_children(self, assets: list) -> list:
        ids = [a.id for a in assets]
        tasks = [{"parent_ids": chunk, "limit": -1} for chunk in split_into_chunks(ids, 100)]
        tasks_summary = await execute_tasks_async(self.list, tasks=tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        res_list = tasks_summary.results
        children = []
        for res in res_list:
            children.extend(res)
        return children

    async def aggregate_cardinality_values(
        self,
        property: AssetPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate property cardinality for assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            property (AssetPropertyLike): The property to count the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.

        Returns:
            int: Approximate cardinality of property.
        """
        return await self._advanced_aggregate(
            aggregate="cardinalityValues",
            properties=property,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    async def aggregate_cardinality_properties(
        self,
        path: AssetPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
    ) -> int:
        """`Find approximate paths cardinality for assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            path (AssetPropertyLike | None): The path to find the cardinality of.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.

        Returns:
            int: Approximate cardinality of path.
        """
        return await self._advanced_aggregate(
            aggregate="cardinalityProperties",
            path=path,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    async def aggregate_unique_values(
        self,
        property: AssetPropertyLike,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique properties with counts for assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            property (AssetPropertyLike): The property to get unique values for.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.
            limit (int | None): Maximum number of unique values to return.

        Returns:
            UniqueResultList: List of unique values with counts.
        """
        return await self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    async def aggregate_unique_properties(
        self,
        path: AssetPropertyLike | None = None,
        advanced_filter: Filter | dict[str, Any] | None = None,
        aggregate_filter: AggregationFilter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> UniqueResultList:
        """`Get unique paths with counts for assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            path (AssetPropertyLike | None): The path to get unique values for.
            advanced_filter (Filter | dict[str, Any] | None): Advanced filter query using the filter DSL.
            aggregate_filter (AggregationFilter | dict[str, Any] | None): Aggregated filter applied to the result.
            limit (int | None): Maximum number of unique values to return.

        Returns:
            UniqueResultList: List of unique paths with counts.
        """
        return await self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
            limit=limit,
        )

    async def create_hierarchy(
        self,
        assets: Sequence[Asset | AssetWrite],
    ) -> AssetList:
        """`Create asset hierarchy <https://developer.cognite.com/api#tag/Assets/operation/createAssets>`_

        You can create an asset hierarchy using this function. This is for convenience,
        but you can achieve the same thing using the .create() method.

        Args:
            assets (Sequence[Asset | AssetWrite]): List of assets to be created in a hierarchical structure.

        Returns:
            AssetList: The created assets.

        Examples:

            Create asset hierarchy::

                >>> from cognite.client import AsyncCogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> client = AsyncCogniteClient()
                >>> root = Asset(external_id="root", name="root")
                >>> child = Asset(external_id="child", name="child", parent_external_id="root")
                >>> res = await client.assets.create_hierarchy([root, child])
        """
        return await self.create(assets)

    # Helper methods
    @staticmethod
    def _process_aggregated_props(agg_props: Sequence[AggregateAssetProperty] | None) -> dict[str, list[str]]:
        if not agg_props:
            return {}
        return {"aggregatedProperties": [to_camel_case(prop) for prop in agg_props]}

    def _validate_filter(self, filter: Filter | dict[str, Any] | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)


class _TaskResult(NamedTuple):
    successful: list[Asset]
    failed: list[Asset]
    unknown: list[Asset]