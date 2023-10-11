from __future__ import annotations

import functools
import heapq
import itertools
import math
import operator as op
import threading
import warnings
from functools import cached_property
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    NamedTuple,
    NoReturn,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeAlias

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    Asset,
    AssetAggregate,
    AssetFilter,
    AssetHierarchy,
    AssetList,
    AssetUpdate,
    GeoLocationFilter,
    LabelFilter,
    TimestampRange,
    filters,
)
from cognite.client.data_classes.aggregations import AggregationFilter, UniqueResultList
from cognite.client.data_classes.assets import AssetPropertyLike, AssetSort, SortableAssetProperty
from cognite.client.data_classes.filters import Filter, _validate_filter
from cognite.client.data_classes.shared import AggregateBucketResult
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import assert_type, split_into_chunks, split_into_n_parts
from cognite.client.utils._concurrency import classify_error, execute_tasks, get_priority_executor
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils._text import to_camel_case
from cognite.client.utils._validation import prepare_filter_sort, process_asset_subtree_ids, process_data_set_ids

if TYPE_CHECKING:
    from concurrent.futures import Future

    from cognite.client.utils._priority_tpe import PriorityThreadPoolExecutor


SortSpec: TypeAlias = Union[
    AssetSort,
    str,
    SortableAssetProperty,
    Tuple[str, Literal["asc", "desc"]],
    Tuple[str, Literal["asc", "desc"], Literal["auto", "first", "last"]],
]

_FILTERS_SUPPORTED: frozenset[type[Filter]] = frozenset(
    {
        filters.And,
        filters.Or,
        filters.Not,
        filters.In,
        filters.Equals,
        filters.Exists,
        filters.Range,
        filters.Prefix,
        filters.ContainsAny,
        filters.ContainsAll,
        filters.Search,
    }
)


class AssetsAPI(APIClient):
    _RESOURCE_PATH = "/assets"

    def __call__(
        self,
        chunk_size: int | None = None,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        metadata: dict[str, str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        aggregated_properties: Sequence[str] | None = None,
        limit: int | None = None,
        partitions: int | None = None,
    ) -> Iterator[Asset] | Iterator[AssetList]:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Args:
            chunk_size (int | None): Number of assets to return in each chunk. Defaults to yielding one asset a time.
            name (str | None): Name of asset. Often referred to as tag.
            parent_ids (Sequence[int] | None): Return only the direct descendants of the specified assets.
            parent_external_ids (Sequence[str] | None): Return only the direct descendants of the specified assets.
            asset_subtree_ids (int | Sequence[int] | None): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (str | Sequence[str] | None): Asset subtree external id or list of asset subtree external ids to filter on.
            metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value
            data_set_ids (int | Sequence[int] | None): Return only assets in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): Return only assets in the specified data set(s) with this external id / these external ids.
            labels (LabelFilter | None): Return only the assets matching the specified label.
            geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
            source (str | None): The source of this asset
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            root (bool | None): filtered assets are root assets or not
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            aggregated_properties (Sequence[str] | None): Set of aggregated properties to include.
            limit (int | None): Maximum number of assets to return. Defaults to return all items.
            partitions (int | None): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed. To prevent unexpected problems and maximize read throughput, API documentation recommends at most use 10 partitions. When using more than 10 partitions, actual throughout decreases. In future releases of the APIs, CDF may reject requests with more than 10 partitions.

        Returns:
            Iterator[Asset] | Iterator[AssetList]: yields Asset one by one if chunk_size is not specified, else AssetList objects.
        """
        if aggregated_properties:
            aggregated_properties = [to_camel_case(s) for s in aggregated_properties]

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

        return self._list_generator(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            chunk_size=chunk_size,
            filter=filter,
            limit=limit,
            partitions=partitions,
            other_params={"aggregatedProperties": aggregated_properties} if aggregated_properties else {},
        )

    def __iter__(self) -> Iterator[Asset]:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Returns:
            Iterator[Asset]: yields Assets one by one.
        """
        return cast(Iterator[Asset], self())

    def retrieve(self, id: int | None = None, external_id: str | None = None) -> Asset | None:
        """`Retrieve a single asset by id. <https://developer.cognite.com/api#tag/Assets/operation/getAsset>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID

        Returns:
            Asset | None: Requested asset or None if it does not exist.

        Examples:

            Get asset by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.retrieve(id=1)

            Get asset by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.retrieve(external_id="1")
        """
        identifier = IdentifierSequence.load(ids=id, external_ids=external_id).as_singleton()
        return self._retrieve_multiple(list_cls=AssetList, resource_cls=Asset, identifiers=identifier)

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: Sequence[str] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> AssetList:
        """`Retrieve multiple assets by id. <https://developer.cognite.com/api#tag/Assets/operation/byIdsAssets>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (Sequence[str] | None): External IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            AssetList: The requested assets.

        Examples:

            Get assets by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.retrieve_multiple(ids=[1, 2, 3])

            Get assets by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.retrieve_multiple(external_ids=["abc", "def"], ignore_unknown_ids=True)
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids)
        return self._retrieve_multiple(
            list_cls=AssetList, resource_cls=Asset, identifiers=identifiers, ignore_unknown_ids=ignore_unknown_ids
        )

    def aggregate(self, filter: AssetFilter | dict | None = None) -> list[AssetAggregate]:
        """`Aggregate assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            filter (AssetFilter | dict | None): Filter on assets with strict matching.

        Returns:
            list[AssetAggregate]: List of asset aggregates

        Examples:

            Aggregate assets:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_by_prefix = c.assets.aggregate(filter={"external_id_prefix": "prefix"})
        """
        return self._aggregate(filter=filter, cls=AssetAggregate)

    def aggregate_metadata_keys(self, filter: AssetFilter | dict | None = None) -> Sequence[AggregateBucketResult]:
        """`Aggregate assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Note:
            In the case of text fields, the values are aggregated in a case-insensitive manner

        Args:
            filter (AssetFilter | dict | None): Filter on assets with strict matching.

        Returns:
            Sequence[AggregateBucketResult]: List of asset aggregates

        Examples:

            Aggregate assets:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_by_prefix = c.assets.aggregate_metadata_keys(filter={"external_id_prefix": "prefix"})
        """
        warnings.warn(
            "This method is deprecated and will be removed in future versions of the SDK.", DeprecationWarning
        )
        return self._aggregate(filter=filter, aggregate="metadataKeys", cls=AggregateBucketResult)

    def aggregate_metadata_values(
        self, keys: Sequence[str], filter: AssetFilter | dict | None = None
    ) -> Sequence[AggregateBucketResult]:
        """`Aggregate assets <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Note:
            In the case of text fields, the values are aggregated in a case-insensitive manner

        Args:
            keys (Sequence[str]): Metadata key(s) to apply the aggregation on. Currently supports exactly one key per request.
            filter (AssetFilter | dict | None): Filter on assets with strict matching.

        Returns:
            Sequence[AggregateBucketResult]: List of asset aggregates

        Examples:

            Aggregate assets:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_by_prefix = c.assets.aggregate_metadata_values(
                ...     keys=["someKey"],
                ...     filter={"external_id_prefix": "prefix"}
                ... )
        """
        warnings.warn(
            "This method is deprecated and will be removed in future versions of the SDK.", DeprecationWarning
        )
        return self._aggregate(filter=filter, aggregate="metadataValues", keys=keys, cls=AggregateBucketResult)

    def aggregate_count(
        self,
        property: AssetPropertyLike | None = None,
        advanced_filter: Filter | dict | None = None,
        filter: AssetFilter | dict | None = None,
    ) -> int:
        """`Count of assets matching the specified filters. <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            property (AssetPropertyLike | None): If specified, get an approximate number of asset with a specific property (property is not null) and matching the filters.
            advanced_filter (Filter | dict | None): The advanced filter to narrow down the assets to count.
            filter (AssetFilter | dict | None): The filter to narrow down the assets to count (strict matching).

        Returns:
            int: The number of assets matching the specified filters.

        Examples:

        Count the number of assets in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> c = CogniteClient()
            >>> count = c.assets.aggregate_count()

        Count the number of assets with the metadata key "timezone" in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.assets import AssetProperty
            >>> c = CogniteClient()
            >>> has_timezone = filters.ContainsAny(AssetProperty.metadata, "timezone")
            >>> asset_count = c.assets.aggregate_count(advanced_filter=has_timezone)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "count",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
        )

    def aggregate_cardinality_values(
        self,
        property: AssetPropertyLike,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: AssetFilter | dict | None = None,
    ) -> int:
        """`Find approximate property count for assets. <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            property (AssetPropertyLike): The property to count the cardinality of.
            advanced_filter (Filter | dict | None): The advanced filter to narrow down assets.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (AssetFilter | dict | None): The filter to narrow down assets (strict matching).
        Returns:
            int: The number of properties matching the specified filters and search.

        Examples:

            Count the number of labels used by assets in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.assets import AssetProperty
                >>> c = CogniteClient()
                >>> label_count = c.assets.aggregate_cardinality_values(AssetProperty.labels)

            Count the number of timezones (metadata key) for assets with the word "critical" in the description in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.assets import AssetProperty
                >>> c = CogniteClient()
                >>> is_critical = filters.Search(AssetProperty.description, "critical")
                >>> critical_assets = c.assets.aggregate_cardinality_values(AssetProperty.metadata_key("timezone"), advanced_filter=is_critical)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_cardinality_properties(
        self,
        path: AssetPropertyLike,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: AssetFilter | dict | None = None,
    ) -> int:
        """`Find approximate paths count for assets.  <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            path (AssetPropertyLike): The scope in every document to aggregate properties. The only value allowed now is ["metadata"].
                It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict | None): The advanced filter to narrow down assets.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (AssetFilter | dict | None): The filter to narrow down assets (strict matching).
        Returns:
            int: The number of properties matching the specified filters.

        Examples:

            Count the number of unique metadata keys used by assets in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.assets import AssetProperty
                >>> c = CogniteClient()
                >>> key_count = c.assets.aggregate_cardinality_properties(AssetProperty.metadata)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            "cardinalityProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_unique_values(
        self,
        property: AssetPropertyLike,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: AssetFilter | dict | None = None,
    ) -> UniqueResultList:
        """`Get unique properties with counts for assets. <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            property (AssetPropertyLike): The property to group by.
            advanced_filter (Filter | dict | None): The advanced filter to narrow down assets.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (AssetFilter | dict | None): The filter to narrow down assets (strict matching).

        Returns:
            UniqueResultList: List of unique values of assets matching the specified filters and search.

        Examples:

        Get the timezones (metadata key) with count for your assets in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.assets import AssetProperty
            >>> c = CogniteClient()
            >>> result = c.assets.aggregate_unique_values(AssetProperty.metadata_key("timezone"))
            >>> print(result.unique)

        Get the different labels with count used for assets created after 2020-01-01 in your CDF project:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import filters
            >>> from cognite.client.data_classes.assets import AssetProperty
            >>> from cognite.client.utils import timestamp_to_ms
            >>> from datetime import datetime
            >>> c = CogniteClient()
            >>> created_after_2020 = filters.Range(AssetProperty.created_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
            >>> result = c.assets.aggregate_unique_values(AssetProperty.labels, advanced_filter=created_after_2020)
            >>> print(result.unique)

        Get the different labels with count for assets updated after 2020-01-01 in your CDF project, but exclude all labels that
        starts with "test":

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes.assets import AssetProperty
            >>> from cognite.client.data_classes import aggregations as aggs, filters
            >>> c = CogniteClient()
            >>> not_test = aggs.Not(aggs.Prefix("test"))
            >>> created_after_2020 = filters.Range(AssetProperty.last_updated_time, gte=timestamp_to_ms(datetime(2020, 1, 1)))
            >>> result = c.assets.aggregate_unique_values(AssetProperty.labels, advanced_filter=created_after_2020, aggregate_filter=not_test)
            >>> print(result.unique)

        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueValues",
            properties=property,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    def aggregate_unique_properties(
        self,
        path: AssetPropertyLike,
        advanced_filter: Filter | dict | None = None,
        aggregate_filter: AggregationFilter | dict | None = None,
        filter: AssetFilter | dict | None = None,
    ) -> UniqueResultList:
        """`Get unique paths with counts for assets. <https://developer.cognite.com/api#tag/Assets/operation/aggregateAssets>`_

        Args:
            path (AssetPropertyLike): The scope in every document to aggregate properties.  The only value allowed now is ["metadata"].
                It means to aggregate only metadata properties (aka keys).
            advanced_filter (Filter | dict | None): The advanced filter to narrow down assets.
            aggregate_filter (AggregationFilter | dict | None): The filter to apply to the resulting buckets.
            filter (AssetFilter | dict | None): The filter to narrow down assets (strict matching).

        Returns:
            UniqueResultList: List of unique values of assets matching the specified filters and search.

        Examples:

            Get the metadata keys with counts for your assets in your CDF project:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.assets import AssetProperty
                >>> c = CogniteClient()
                >>> result = c.assets.aggregate_unique_properties(AssetProperty.metadata)
        """
        self._validate_filter(advanced_filter)
        return self._advanced_aggregate(
            aggregate="uniqueProperties",
            path=path,
            filter=filter,
            advanced_filter=advanced_filter,
            aggregate_filter=aggregate_filter,
        )

    @overload
    def create(self, asset: Sequence[Asset]) -> AssetList:
        ...

    @overload
    def create(self, asset: Asset) -> Asset:
        ...

    def create(self, asset: Asset | Sequence[Asset]) -> Asset | AssetList:
        """`Create one or more assets. <https://developer.cognite.com/api#tag/Assets/operation/createAssets>`_

        You can create an arbitrary number of assets, and the SDK will split the request into multiple requests.
        When specifying parent-child relation between assets using `parentExternalId` the link will be resvoled into an internal ID and stored as `parentId`.

        Args:
            asset (Asset | Sequence[Asset]): Asset or list of assets to create.

        Returns:
            Asset | AssetList: Created asset(s)

        Examples:

            Create new assets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> c = CogniteClient()
                >>> assets = [Asset(name="asset1"), Asset(name="asset2")]
                >>> res = c.assets.create(assets)

            Create asset with label::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset, Label
                >>> c = CogniteClient()
                >>> asset = Asset(name="my_pump", labels=[Label(external_id="PUMP")])
                >>> res = c.assets.create(asset)
        """
        assert_type(asset, "asset", [Asset, Sequence])
        return self._create_multiple(list_cls=AssetList, resource_cls=Asset, items=asset)

    def create_hierarchy(
        self,
        assets: Sequence[Asset] | AssetHierarchy,
        *,
        upsert: bool = False,
        upsert_mode: Literal["patch", "replace"] = "patch",
    ) -> AssetList:
        """Create an asset hierarchy with validation.

        This helper function makes it easy to insert large asset hierarchies. It solves the problem of topological
        insertion order, i.e. a parent asset must exist before it can be referenced by any 'children' assets.
        You may pass any number of partial- or full hierarchies: there are no requirements on the number of root
        assets, so you may pass zero, one or many (same goes for the non-root assets).

        Args:
            assets (Sequence[Asset] | AssetHierarchy): List of assets to create or an instance of AssetHierarchy.
            upsert (bool): If used, already existing assets will be updated instead of an exception being raised. You may control how updates are applied with the 'upsert_mode' argument.
            upsert_mode (Literal["patch", "replace"]): Only applicable with upsert. Pass 'patch' to only update fields with non-null values (default), or 'replace' to do full updates (unset fields become null or empty).

        Returns:
            AssetList: Created (and possibly updated) asset hierarchy

        Prior to insertion, this function will run validation on the given assets and raise an error if any of
        the following issues are found:

            1. Any assets are invalid (category: ``invalid``):

                - Missing external ID.
                - Missing a valid name.
                - Has an ID set.
            2. Any asset duplicates exist (category: ``duplicates``)
            3. Any assets have an ambiguous parent link (category: ``unsure_parents``)
            4. Any group of assets form a cycle, e.g. A->B->A (category: ``cycles``)

        As part of validation there is a fifth category that is ignored when using this method (for backwards compatibility) and that
        is orphan assets. These are assets linking a parent by an identifier that is not present among the given assets, and as such,
        might contain links we are unable to vet ahead of insertion. These are thus assumed to be valid, but may fail.

        Tip:
            The different categories specified above corresponds to the name of the attribute you might access on the raised error to
            get the collection of 'bad' assets falling in that group, e.g. ``error.duplicates``.

        Note:
            Updating ``external_id`` via upsert is not supported (and will not be supported). Use ``AssetsAPI.update`` instead.

        Warning:
            The API does not natively support upsert, so the SDK has to simulate the behaviour at the cost of some insertion speed.

            Be careful when moving assets to new parents via upsert: Please do so only by specifying ``parent_external_id``
            (instead of ``parent_id``) to avoid race conditions in insertion order (temporary cycles might form since we
            can only make changes to 1000 assets at the time).

        Examples:

            Create an asset hierarchy:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> c = CogniteClient()
                >>> assets = [
                ...     Asset(external_id="root", name="root"),
                ...     Asset(external_id="child1", parent_external_id="root", name="child1"),
                ...     Asset(external_id="child2", parent_external_id="root", name="child2")]
                >>> res = c.assets.create_hierarchy(assets)

            Create an asset hierarchy, but run update for existing assets:

                >>> res = c.assets.create_hierarchy(assets, upsert=True, upsert_mode="patch")

            Patch will only update the parameters you have defined on your assets. Note that specifically setting
            something to ``None`` is the same as not setting it. For ``metadata``, this will extend your existing
            data, only overwriting when keys overlap. For ``labels`` the behaviour is mostly the same, existing are
            left untouched, and your new ones are simply added.

            You may also pass ``upsert_mode="replace"`` to make sure the updated assets look identical to the ones
            you passed to the method. For both ``metadata`` and ``labels`` this will clear out all existing,
            before (potentially) adding the new ones.

            If the hierarchy validation for some reason fail, you may inspect all the issues that were found by
            catching :class:`~cognite.client.exceptions.CogniteAssetHierarchyError`:

                >>> from cognite.client.exceptions import CogniteAssetHierarchyError
                >>> try:
                ...     res = c.assets.create_hierarchy(assets)
                ... except CogniteAssetHierarchyError as err:
                ...     if err.invalid:
                ...         ...  # do something

            In addition to ``invalid``, you may inspect ``duplicates``, ``unsure_parents``, ``orphans`` and ``cycles``.
            Note that cycles are not available if any of the other basic issues exist, as the search for cyclical
            references requires a clean asset hierarchy to begin with.

            You may also wrap the ``create_hierarchy()`` call in a try-except to get information if any of the assets
            fails to be created (assuming a valid hierarchy):

                >>> from cognite.client.exceptions import CogniteAPIError
                >>> try:
                ...     c.assets.create_hierarchy(assets)
                ... except CogniteAPIError as err:
                ...     created = err.successful
                ...     maybe_created = err.unknown
                ...     not_created = err.failed

            Here's a slightly longer explanation of the different groups:

                - ``err.successful``: Which assets were created (request yielded a 201)
                - ``err.unknown``: Which assets *may* have been created (request yielded 5xx)
                - ``err.failed``: Which assets were *not* created (request yielded 4xx, or was a descendant of an asset with unknown status)

            The preferred way to create an asset hierarchy, is to run validation *prior to insertion*. You may do this by
            using the :class:`~cognite.client.data_classes.assets.AssetHierarchy` class. It will by default consider orphan
            assets to be problematic (but accepts the boolean parameter ``ignore_orphans``), contrary to how ``create_hierarchy``
            works (which accepts them in order to be backwards-compatible). It also provides helpful methods to create reports
            of any issues found, check out ``validate_and_report``:

                >>> from cognite.client.data_classes import AssetHierarchy
                >>> from pathlib import Path
                >>> hierarchy = AssetHierarchy(assets)
                >>> if hierarchy.is_valid():
                ...     res = c.assets.create_hierarchy(hierarchy)
                ... else:
                ...     hierarchy.validate_and_report(output_file=Path("report.txt"))
        """
        if upsert and upsert_mode not in ("patch", "replace"):
            raise ValueError(f"'upsert_mode' must be either 'patch' or 'replace', not {upsert_mode!r}")

        if not isinstance(assets, AssetHierarchy):
            assert_type(assets, "assets", [Sequence])
            assets = AssetHierarchy(assets, ignore_orphans=True)

        return _AssetHierarchyCreator(assets, assets_api=self).create(upsert, upsert_mode)

    def delete(
        self,
        id: int | Sequence[int] | None = None,
        external_id: str | Sequence[str] | None = None,
        recursive: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more assets <https://developer.cognite.com/api#tag/Assets/operation/deleteAssets>`_

        Args:
            id (int | Sequence[int] | None): Id or list of ids
            external_id (str | Sequence[str] | None): External ID or list of external ids
            recursive (bool): Recursively delete whole asset subtrees under given ids. Defaults to False.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Examples:

            Delete assets by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.assets.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=id, external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"recursive": recursive, "ignoreUnknownIds": ignore_unknown_ids},
        )

    @overload
    def update(self, item: Sequence[Asset | AssetUpdate]) -> AssetList:
        ...

    @overload
    def update(self, item: Asset | AssetUpdate) -> Asset:
        ...

    def update(self, item: Asset | AssetUpdate | Sequence[Asset | AssetUpdate]) -> Asset | AssetList:
        """`Update one or more assets <https://developer.cognite.com/api#tag/Assets/operation/updateAssets>`_
        Labels can be added, removed or replaced (set). Note that set operation deletes all the existing labels and adds the new specified labels.

        Args:
            item (Asset | AssetUpdate | Sequence[Asset | AssetUpdate]): Asset(s) to update

        Returns:
            Asset | AssetList: Updated asset(s)

        Examples:
            Perform a partial update on an asset, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res1 = c.assets.update(my_update)
                >>> # Remove an already set field like so
                >>> another_update = AssetUpdate(id=1).description.set(None)
                >>> res2 = c.assets.update(another_update)

            Remove the metadata on an asset::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).metadata.add({"key": "value"})
                >>> res1 = c.assets.update(my_update)
                >>> another_update = AssetUpdate(id=1).metadata.set(None)
                >>> # The same result can be achieved with:
                >>> another_update2 = AssetUpdate(id=1).metadata.set({})
                >>> res2 = c.assets.update(another_update)

            Attach labels to an asset::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).labels.add(["PUMP", "VERIFIED"])
                >>> res = c.assets.update(my_update)

            Detach a single label from an asset::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).labels.remove("PUMP")
                >>> res = c.assets.update(my_update)

            Replace all labels for an asset::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).labels.set("PUMP")
                >>> res = c.assets.update(my_update)
        """
        return self._update_multiple(list_cls=AssetList, resource_cls=Asset, update_cls=AssetUpdate, items=item)

    @overload
    def upsert(self, item: Sequence[Asset], mode: Literal["patch", "replace"] = "patch") -> AssetList:
        ...

    @overload
    def upsert(self, item: Asset, mode: Literal["patch", "replace"] = "patch") -> Asset:
        ...

    def upsert(self, item: Asset | Sequence[Asset], mode: Literal["patch", "replace"] = "patch") -> Asset | AssetList:
        """Upsert assets, i.e., update if it exists, and create if it does not exist.
            Note this is a convenience method that handles the upserting for you by first calling update on all items,
            and if any of them fail because they do not exist, it will create them instead.

            For more details, see :ref:`appendix-upsert`.

        Args:
            item (Asset | Sequence[Asset]): Asset or list of assets to upsert.
            mode (Literal["patch", "replace"]): Whether to patch or replace in the case the assets are existing. If
                you set 'patch', the call will only update fields with non-null values (default).
                Setting 'replace' will unset any fields that are not specified.

        Returns:
            Asset | AssetList: The upserted asset(s).

        Examples:

            Upsert for assets:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> c = CogniteClient()
                >>> existing_asset = c.assets.retrieve(id=1)
                >>> existing_asset.description = "New description"
                >>> new_asset = Asset(external_id="new_asset", description="New asset")
                >>> res = c.assets.upsert([existing_asset, new_asset], mode="replace")
        """
        return self._upsert_multiple(
            item,
            list_cls=AssetList,
            resource_cls=Asset,
            update_cls=AssetUpdate,
            input_resource_cls=Asset,
            mode=mode,
        )

    def filter(
        self,
        filter: Filter | dict,
        sort: SortSpec | list[SortSpec] | None = None,
        aggregated_properties: Sequence[Literal["child_count", "path", "depth"]] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> AssetList:
        """`Advanced filter assets <https://developer.cognite.com/api#tag/Assets/operation/listAssets>`_

        Advanced filter lets you create complex filtering expressions that combine simple operations,
        such as equals, prefix, exists, etc., using boolean operators and, or, and not.
        It applies to basic fields as well as metadata.

        Args:
            filter (Filter | dict): Filter to apply.
            sort (SortSpec | list[SortSpec] | None): The criteria to sort by. Can be up to two properties to sort by default to ascending order.
            aggregated_properties (Sequence[Literal["child_count", "path", "depth"]] | None): Set of aggregated properties to include. Options are childCount, path, depth.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            AssetList: List of assets that match the filter criteria.

        Examples:

            Find all assets that have a metadata key 'timezone' starting with 'Europe',
            and sort by external id ascending:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> c = CogniteClient()
                >>> f = filters
                >>> in_timezone = f.Prefix(["metadata", "timezone"], "Europe")
                >>> res = c.assets.filter(filter=in_timezone,
                ...                       sort=("external_id", "asc"))

            Note that you can check the API documentation above to see which properties you can filter on
            with which filters.

            To make it easier to avoid spelling mistakes and easier to look up available properties
            for filtering and sorting, you can also use the `AssetProperty` and `SortableAssetProperty` Enums.

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import filters
                >>> from cognite.client.data_classes.assets import AssetProperty, SortableAssetProperty
                >>> c = CogniteClient()
                >>> f = filters
                >>> in_timezone = f.Prefix(AssetProperty.metadata_key("timezone"), "Europe")
                >>> res = c.assets.filter(filter=in_timezone,
                ...                       sort=(SortableAssetProperty.external_id, "asc"))

        """
        self._validate_filter(filter)

        if aggregated_properties:
            aggregated_properties_camel = [to_camel_case(prop) for prop in aggregated_properties]
        else:
            aggregated_properties_camel = None

        return self._list(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            limit=limit,
            advanced_filter=filter.dump(camel_case=True) if isinstance(filter, Filter) else filter,
            sort=prepare_filter_sort(sort, AssetSort),
            other_params={"aggregatedProperties": aggregated_properties_camel} if aggregated_properties_camel else {},
        )

    def _validate_filter(self, filter: Filter | dict | None) -> None:
        _validate_filter(filter, _FILTERS_SUPPORTED, type(self).__name__)

    def search(
        self,
        name: str | None = None,
        description: str | None = None,
        query: str | None = None,
        filter: AssetFilter | dict | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> AssetList:
        """`Search for assets <https://developer.cognite.com/api#tag/Assets/operation/searchAssets>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str | None): Fuzzy match on name.
            description (str | None): Fuzzy match on description.
            query (str | None): Whitespace-separated terms to search for in assets. Does a best-effort fuzzy search in relevant fields (currently name and description) for variations of any of the search terms, and orders results by relevance.
            filter (AssetFilter | dict | None): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            AssetList: List of requested assets

        Examples:

            Search for assets by fuzzy search on name::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.search(name="some name")

            Search for assets by exact search on name::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.search(filter={"name": "some name"})

            Search for assets by improved multi-field fuzzy search::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.search(query="TAG 30 XV")

            Search for assets using multiple filters, finding all assets with name similar to `xyz` with parent asset `123` or `456` with source `some source`::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.search(name="xyz",filter={"parent_ids": [123,456],"source": "some source"})

            Search for an asset with an attached label:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> my_label_filter = LabelFilter(contains_all=["PUMP"])
                >>> res = c.assets.search(name="xyz",filter=AssetFilter(labels=my_label_filter))
        """
        return self._search(
            list_cls=AssetList,
            search={"name": name, "description": description, "query": query},
            filter=filter or {},
            limit=limit,
        )

    def retrieve_subtree(
        self, id: int | None = None, external_id: str | None = None, depth: int | None = None
    ) -> AssetList:
        """Retrieve the subtree for this asset up to a specified depth.

        Args:
            id (int | None): Id of the root asset in the subtree.
            external_id (str | None): External id of the root asset in the subtree.
            depth (int | None): Retrieve assets up to this depth below the root asset in the subtree. Omit to get the entire subtree.

        Returns:
            AssetList: The requested assets or empty AssetList if asset does not exist.
        """
        asset = self.retrieve(id=id, external_id=external_id)
        if asset is None:
            return AssetList([], self._cognite_client)
        subtree = self._get_asset_subtree([asset], current_depth=0, depth=depth)
        return AssetList(subtree, self._cognite_client)

    def _get_asset_subtree(self, assets: list, current_depth: int, depth: int | None) -> list:
        subtree = assets
        if depth is None or current_depth < depth:
            if children := self._get_children(subtree):
                subtree.extend(self._get_asset_subtree(children, current_depth + 1, depth))
        return subtree

    def _get_children(self, assets: list) -> list:
        ids = [a.id for a in assets]
        tasks = [{"parent_ids": chunk, "limit": -1} for chunk in split_into_chunks(ids, 100)]
        tasks_summary = execute_tasks(self.list, tasks=tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        res_list = tasks_summary.results
        children = []
        for res in res_list:
            children.extend(res)
        return children

    def list(
        self,
        name: str | None = None,
        parent_ids: Sequence[int] | None = None,
        parent_external_ids: Sequence[str] | None = None,
        asset_subtree_ids: int | Sequence[int] | None = None,
        asset_subtree_external_ids: str | Sequence[str] | None = None,
        data_set_ids: int | Sequence[int] | None = None,
        data_set_external_ids: str | Sequence[str] | None = None,
        labels: LabelFilter | None = None,
        geo_location: GeoLocationFilter | None = None,
        metadata: dict[str, str] | None = None,
        source: str | None = None,
        created_time: dict[str, Any] | TimestampRange | None = None,
        last_updated_time: dict[str, Any] | TimestampRange | None = None,
        root: bool | None = None,
        external_id_prefix: str | None = None,
        aggregated_properties: Sequence[str] | None = None,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> AssetList:
        """`List assets <https://developer.cognite.com/api#tag/Assets/operation/listAssets>`_

        Args:
            name (str | None): Name of asset. Often referred to as tag.
            parent_ids (Sequence[int] | None): Return only the direct descendants of the specified assets.
            parent_external_ids (Sequence[str] | None): Return only the direct descendants of the specified assets.
            asset_subtree_ids (int | Sequence[int] | None): Asset subtree id or list of asset subtree ids to filter on.
            asset_subtree_external_ids (str | Sequence[str] | None): Asset subtree external id or list of asset subtree external ids to filter on.
            data_set_ids (int | Sequence[int] | None): Return only assets in the specified data set(s) with this id / these ids.
            data_set_external_ids (str | Sequence[str] | None): Return only assets in the specified data set(s) with this external id / these external ids.
            labels (LabelFilter | None): Return only the assets matching the specified label filter.
            geo_location (GeoLocationFilter | None): Only include files matching the specified geographic relation.
            metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value.
            source (str | None): The source of this asset.
            created_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (dict[str, Any] | TimestampRange | None):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            root (bool | None): filtered assets are root assets or not.
            external_id_prefix (str | None): Filter by this (case-sensitive) prefix for the external ID.
            aggregated_properties (Sequence[str] | None): Set of aggregated properties to include.
            partitions (int | None): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed. To prevent unexpected problems and maximize read throughput, API documentation recommends at most use 10 partitions. When using more than 10 partitions, actual throughout decreases. In future releases of the APIs, CDF may reject requests with more than 10 partitions.
            limit (int | None): Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            AssetList: List of requested assets

        Examples:

            List assets::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> asset_list = c.assets.list(limit=5)

            Iterate over assets::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for asset in c.assets:
                ...     asset # do something with the asset

            Iterate over chunks of assets to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> for asset_list in c.assets(chunk_size=2500):
                ...     asset_list # do something with the assets

            Filter assets based on labels::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LabelFilter
                >>> c = CogniteClient()
                >>> my_label_filter = LabelFilter(contains_all=["PUMP", "VERIFIED"])
                >>> asset_list = c.assets.list(labels=my_label_filter)
        """
        if aggregated_properties:
            aggregated_properties = [to_camel_case(s) for s in aggregated_properties]

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
        return self._list(
            list_cls=AssetList,
            resource_cls=Asset,
            method="POST",
            limit=limit,
            filter=filter,
            other_params={"aggregatedProperties": aggregated_properties} if aggregated_properties else {},
            partitions=partitions,
        )


class _CreateTask(NamedTuple):
    items: set[Asset]
    priority: int


class _TaskResult(NamedTuple):
    successful: list[Asset]
    failed: list[Asset]
    unknown: list[Asset]


class _AssetHierarchyCreator:
    def __init__(self, hierarchy: AssetHierarchy, assets_api: AssetsAPI) -> None:
        hierarchy.is_valid(on_error="raise")
        self.hierarchy = hierarchy
        self.n_assets = len(hierarchy)
        self.assets_api = assets_api
        self.create_limit = assets_api._CREATE_LIMIT
        self.resource_path = assets_api._RESOURCE_PATH
        self.max_workers = assets_api._config.max_workers
        self.failed: list[Asset] = []
        self.unknown: list[Asset] = []
        # Each thread needs to store its latest exception:
        self.latest_exception: dict[int, Exception | None] = {}

        self.__counter = itertools.count().__next__

    def create(self, upsert: bool, upsert_mode: Literal["patch", "replace"]) -> AssetList:
        insert_fn = functools.partial(self._insert, upsert=upsert, upsert_mode=upsert_mode)
        insert_dct = self.hierarchy.groupby_parent_xid()
        subtree_count = self.hierarchy.count_subtree(insert_dct)

        with get_priority_executor(max_workers=self.max_workers) as pool:
            created_assets = self._create(pool, insert_fn, insert_dct, subtree_count)

        if all_exceptions := [exc for exc in self.latest_exception.values() if exc is not None]:
            self._raise_latest_exception(all_exceptions, created_assets)
        return AssetList(created_assets, cognite_client=self.assets_api._cognite_client)

    def _create(
        self,
        pool: PriorityThreadPoolExecutor,
        insert_fn: Callable[[list[Asset]], _TaskResult],
        insert_dct: dict[str | None, list[Asset]],
        subtree_count: dict[str, int],
    ) -> list[Asset]:
        queue_fn = functools.partial(
            self._queue_tasks,
            pool=pool,
            insert_fn=insert_fn,
            insert_dct=insert_dct,
            subtree_count=subtree_count,
        )
        # Kick things off with all...:
        # 1. Root assets
        # 2. Assets linking parent by ID
        # 3. Orphans assets (if `hierarchy.ignore_orphans` is True)
        created_assets = []
        futures = queue_fn(insert_dct.pop(None))

        while futures:
            futures.remove(fut := next(pool.as_completed(futures)))
            new_assets, failed, unknown = fut.result()
            created_assets.extend(new_assets)
            if unknown or failed:
                self.failed.extend(failed)
                self.unknown.extend(unknown)
                self._skip_all_descendants(unknown, failed, insert_dct)

            # Newly created assets are now unblocked as parents for the next iteration:
            to_create = list(self._pop_child_assets(new_assets, insert_dct))
            futures |= queue_fn(to_create)
        return created_assets

    def _queue_tasks(
        self,
        assets: list[Asset],
        *,
        pool: PriorityThreadPoolExecutor,
        insert_fn: Callable,
        insert_dct: dict[str | None, list[Asset]],
        subtree_count: dict[str, int],
    ) -> set[Future]:
        if not assets:
            return set()
        return {
            pool.submit(insert_fn, task.items, priority=self.n_assets - task.priority)
            for task in self._split_and_prioritise_assets(assets, insert_dct, subtree_count)
        }

    def _insert(
        self,
        assets: list[Asset],
        *,
        upsert: bool,
        upsert_mode: Literal["patch", "replace"],
        no_recursion: bool = False,
    ) -> _TaskResult:
        try:
            resp = self.assets_api._post(self.resource_path, self._dump_assets(assets))
            successful = list(map(Asset._load, resp.json()["items"]))
            return _TaskResult(successful, failed=[], unknown=[])
        except Exception as err:
            self._set_latest_exception(err)
            successful = []
            failed: list[Asset] = []
            unknown: list[Asset] = []
            # Store to 'failed' or 'unknown':
            err_status = classify_error(err)
            bad_assets = {"failed": failed, "unknown": unknown}[err_status]
            bad_assets.extend(assets)

            # Note the last cond.: we got CogniteAPIError and are running with upsert, but no duplicates gotten:
            if no_recursion or not isinstance(err, CogniteAPIError) or err.duplicated is None:
                return _TaskResult(successful, failed, unknown)

            # Split assets based on their is-duplicated status:
            non_dupes, dupe_assets = self._split_out_duplicated(cast(List[Dict], err.duplicated), assets)
            # We should try to create the non-duplicated assets before running update (as these might be dependent):
            if non_dupes:
                result = self._insert(non_dupes, no_recursion=True, upsert=False, upsert_mode=upsert_mode)
                if result.successful:
                    successful.extend(result.successful)
                    # The assets that were not duplicated should be removed from "bad":
                    bad_assets.clear()
                    bad_assets.extend(dupe_assets)

            # If upsert=True, run update on any existing assets:
            if upsert and dupe_assets:
                updated = self._update(dupe_assets, upsert_mode)
                # If update went well: Add to list of successful assets and remove from "bad":
                if updated is not None:
                    successful.extend(updated)
                    updated_xids = set(upd.external_id for upd in updated)
                    still_bad = [bad for bad in bad_assets if bad.external_id not in updated_xids]
                    bad_assets.clear()
                    bad_assets.extend(still_bad)

            return _TaskResult(successful, failed, unknown)

    def _update(self, to_update: list[Asset], upsert_mode: Literal["patch", "replace"]) -> list[Asset] | None:
        is_patch = upsert_mode == "patch"
        updates = [self._make_asset_updates(asset, patch=is_patch) for asset in to_update]
        return self._update_post(updates)

    def _update_post(self, items: list[AssetUpdate]) -> list[Asset] | None:
        try:
            resp = self.assets_api._post(self.resource_path + "/update", json=self._dump_assets(items))
            updated = [Asset._load(item) for item in resp.json()["items"]]
            self._set_latest_exception(None)  # Update worked, so we hide exception
            return updated
        except Exception as err:
            # At this point, we don't care what caused the failure (well, we store error to show the user):
            # All assets that failed the update are already marked as either failed or unknown.
            self._set_latest_exception(err)
            return None

    def _make_asset_updates(self, asset: Asset, patch: bool) -> AssetUpdate:
        # Note: The SDK makes it very hard to do full updates... we also rely on the update-object to
        # have an updated list of all "updateable" parameters...
        dumped = asset.dump(camel_case=True)
        dct_update = {} if patch else self.clear_all_update.copy()
        dct_update.update({k: {"set": v} for k, v in dumped.items()})
        # Since we enforce XID given and 'no ID', there's no point in "renaming xid to itself":
        dct_update.pop("externalId")
        if patch:
            if "metadata" in dumped:
                dct_update["metadata"]["add"] = dct_update["metadata"].pop("set")
            if "labels" in dumped:
                dct_update["labels"]["add"] = dct_update["labels"].pop("set")

        upd = AssetUpdate(external_id=dumped["externalId"])
        upd._update_object = dct_update
        return upd

    def _set_latest_exception(self, err: Exception | None) -> None:
        thread_id = threading.get_ident()
        self.latest_exception[thread_id] = err

    @cached_property
    def clear_all_update(self) -> MappingProxyType[str, dict[str, Any]]:
        props = {to_camel_case(prop.name) for prop in AssetUpdate._get_update_properties()}

        # Does not support setNull:
        props -= {"name", "parentExternalId", "parentId"}
        dct: dict[str, dict[str, Any]] = {k: {"setNull": True} for k in props}
        # Handle labels and metadata separately...
        dct.update(labels={"set": []}, metadata={"set": {}})
        return MappingProxyType(dct)

    def _split_and_prioritise_assets(
        self,
        to_create: list[Asset],
        insert_dct: dict[str | None, list[Asset]],
        subtree_count: dict[str, int],
    ) -> Iterator[_CreateTask]:
        # We want to dive as deep down the hierarchy as possible while prioritising assets with the biggest
        # subtree, that way we more quickly get into a state with enough unblocked parents to always keep
        # our worker threads fed with create-requests.
        n = len(to_create)
        n_parts = min(n, max(self.max_workers, math.ceil(n / self.create_limit)))
        tasks = [
            self._extend_with_unblocked_from_subtree(set(chunk), insert_dct, subtree_count)
            for chunk in split_into_n_parts(to_create, n=n_parts)
        ]
        # Also, to not waste worker threads on tiny requests, we might recombine:
        tasks.sort(key=lambda task: len(task.items))
        yield from self._recombine_chunks(tasks, limit=self.create_limit)

    @staticmethod
    def _dump_assets(assets: Sequence[Asset] | Sequence[AssetUpdate]) -> dict[str, list[dict]]:
        return {"items": [asset.dump(camel_case=True) for asset in assets]}

    @staticmethod
    def _recombine_chunks(lst: list[_CreateTask], limit: int) -> Iterator[_CreateTask]:
        task = lst[0]
        for next_task in lst[1:]:
            if len(task.items) + len(next_task.items) > limit:
                yield task
                task = next_task
            else:
                task = _CreateTask(task.items | next_task.items, max(task.priority, next_task.priority))
        yield task

    def _extend_with_unblocked_from_subtree(
        self,
        to_create: set[Asset],
        insert_dct: dict[str | None, list[Asset]],
        subtree_count: dict[str, int],
    ) -> _CreateTask:
        pri_q = [(-subtree_count[cast(str, asset.external_id)], self.__counter(), asset) for asset in to_create]
        heapq.heapify(pri_q)
        priority = -pri_q[0][0]  # No child asset can have a larger subtree than its parent

        while pri_q:  # Queue should seriously be spelled q
            *_, asset = heapq.heappop(pri_q)
            to_create.add(asset)
            if children := insert_dct.get(asset.parent_external_id):
                children.remove(asset)  # Counter-intuitive: Using a set is not faster with small avg. list sizes
            if len(to_create) == self.create_limit:
                break
            for child in insert_dct.get(asset.external_id, []):
                heapq.heappush(pri_q, (-subtree_count[cast(str, child.external_id)], self.__counter(), child))

        return _CreateTask(to_create, priority)

    @staticmethod
    def _pop_child_assets(assets: Iterable[Asset], insert_dct: dict[str | None, list[Asset]]) -> Iterator[Asset]:
        return itertools.chain.from_iterable(insert_dct.pop(asset.external_id, []) for asset in assets)

    @staticmethod
    def _split_out_duplicated(subset: list[dict[str, str]], assets: list[Asset]) -> tuple[list[Asset], list[Asset]]:
        # Avoids repeated list-lookups (O(N^2))
        duplicated = {asset["externalId"] for asset in subset}
        split_assets: tuple[list[Asset], list[Asset]] = [], []
        for a in assets:
            split_assets[a.external_id in duplicated].append(a)
        return split_assets

    def _skip_all_descendants(
        self,
        unknown: list[Asset],
        failed: list[Asset],
        insert_dct: dict[str | None, list[Asset]],
    ) -> None:
        skip_assets = [*unknown, *failed]
        while skip_assets:
            skip_assets = list(self._pop_child_assets(skip_assets, insert_dct))
            self.failed.extend(skip_assets)

    def _raise_latest_exception(self, exceptions: list[Exception], successful: list[Asset]) -> NoReturn:
        *_, latest_exception = exceptions
        common = dict(
            successful=AssetList(successful),
            unknown=AssetList(self.unknown),
            failed=AssetList(self.failed),
            unwrap_fn=op.attrgetter("external_id"),
        )
        err_message = "One or more errors happened during asset creation. Latest error:"
        if isinstance(latest_exception, CogniteAPIError):
            raise CogniteAPIError(
                message=f"{err_message} {latest_exception.message}",
                x_request_id=latest_exception.x_request_id,
                code=latest_exception.code,
                extra=latest_exception.extra,
                **common,  # type: ignore [arg-type]
            )
        # If a non-Cognite-exception was raised, we still raise CogniteAPIError, but use 'from' to not hide
        # the underlying reason from the user. We also do this because we promise that 'successful', 'unknown'
        # and 'failed' can be inspected:
        raise CogniteAPIError(
            message=f"{err_message} {type(latest_exception).__name__}('{latest_exception}')",
            code=None,  # type: ignore [arg-type]
            **common,  # type: ignore [arg-type]
        ) from latest_exception
