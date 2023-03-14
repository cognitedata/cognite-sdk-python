from __future__ import annotations

import functools
import heapq
import itertools
import math
import operator as op
from collections import namedtuple
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
    NoReturn,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
    overload,
)

from cognite.client import utils
from cognite.client._api_client import APIClient
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
)
from cognite.client.data_classes.shared import AggregateBucketResult
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError
from cognite.client.utils._auxiliary import split_into_n_parts, to_camel_case, unwrap_identifer
from cognite.client.utils._concurrency import get_as_completed_fn, get_priority_executor
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from concurrent.futures import Future

    from cognite.client.utils._priority_tpe import PriorityThreadPoolExecutor

UpsertOptions = Literal["patch", "replace"]


class AssetsAPI(APIClient):
    _RESOURCE_PATH = "/assets"

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        parent_ids: Sequence[int] = None,
        parent_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[int] = None,
        asset_subtree_external_ids: Sequence[str] = None,
        metadata: Dict[str, str] = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        labels: LabelFilter = None,
        geo_location: GeoLocationFilter = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        root: bool = None,
        external_id_prefix: str = None,
        aggregated_properties: Sequence[str] = None,
        limit: int = None,
        partitions: int = None,
    ) -> Union[Iterator[Asset], Iterator[AssetList]]:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Args:
            chunk_size (int, optional): Number of assets to return in each chunk. Defaults to yielding one asset a time.
            name (str): Name of asset. Often referred to as tag.
            parent_ids (Sequence[int]): Return only the direct descendants of the specified assets.
            parent_external_ids (Sequence[str]): Return only the direct descendants of the specified assets.
            asset_subtree_ids (Sequence[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (Sequence[str]): List of asset subtrees external ids to filter on.
            metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value
            data_set_ids (Sequence[int]): Return only assets in the specified data sets with these ids.
            data_set_external_ids (Sequence[str]): Return only assets in the specified data sets with these external ids.
            labels (LabelFilter): Return only the assets matching the specified label.
            geo_location (GeoLocationFilter): Only include files matching the specified geographic relation.
            source (str): The source of this asset
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            root (bool): filtered assets are root assets or not
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            aggregated_properties (Sequence[str]): Set of aggregated properties to include.
            limit (int, optional): Maximum number of assets to return. Defaults to return all items.
            partitions (int): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed.

        Yields:
            Union[Asset, AssetList]: yields Asset one by one if chunk is not specified, else AssetList objects.
        """
        if aggregated_properties:
            aggregated_properties = [to_camel_case(s) for s in aggregated_properties]

        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

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

        Yields:
            Asset: yields Assets one by one.
        """
        return cast(Iterator[Asset], self())

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Asset]:
        """`Retrieve a single asset by id. <https://docs.cognite.com/api/v1/#operation/getAsset>`_

        Args:
            id (int, optional): ID
            external_id (str, optional): External ID

        Returns:
            Optional[Asset]: Requested asset or None if it does not exist.

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
        ids: Optional[Sequence[int]] = None,
        external_ids: Optional[Sequence[str]] = None,
        ignore_unknown_ids: bool = False,
    ) -> AssetList:
        """`Retrieve multiple assets by id. <https://docs.cognite.com/api/v1/#operation/byIdsAssets>`_

        Args:
            ids (Sequence[int], optional): IDs
            external_ids (Sequence[str], optional): External IDs
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

    def list(
        self,
        name: str = None,
        parent_ids: Sequence[int] = None,
        parent_external_ids: Sequence[str] = None,
        asset_subtree_ids: Sequence[int] = None,
        asset_subtree_external_ids: Sequence[str] = None,
        data_set_ids: Sequence[int] = None,
        data_set_external_ids: Sequence[str] = None,
        labels: LabelFilter = None,
        geo_location: GeoLocationFilter = None,
        metadata: Dict[str, str] = None,
        source: str = None,
        created_time: Union[Dict[str, Any], TimestampRange] = None,
        last_updated_time: Union[Dict[str, Any], TimestampRange] = None,
        root: bool = None,
        external_id_prefix: str = None,
        aggregated_properties: Sequence[str] = None,
        partitions: int = None,
        limit: int = 25,
    ) -> AssetList:
        """`List assets <https://docs.cognite.com/api/v1/#operation/listAssets>`_

        Args:
            name (str): Name of asset. Often referred to as tag.
            parent_ids (Sequence[int]): Return only the direct descendants of the specified assets.
            parent_external_ids (Sequence[str]): Return only the direct descendants of the specified assets.
            asset_subtree_ids (Sequence[int]): List of asset subtrees ids to filter on.
            asset_subtree_external_ids (Sequence[str]): List of asset subtrees external ids to filter on.
            data_set_ids (Sequence[int]): Return only assets in the specified data sets with these ids.
            data_set_external_ids (Sequence[str]): Return only assets in the specified data sets with these external ids.
            labels (LabelFilter): Return only the assets matching the specified label filter.
            geo_location (GeoLocationFilter): Only include files matching the specified geographic relation.
            metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value.
            source (str): The source of this asset.
            created_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            last_updated_time (Union[Dict[str, int], TimestampRange]):  Range between two timestamps. Possible keys are `min` and `max`, with values given as time stamps in ms.
            root (bool): filtered assets are root assets or not.
            external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
            aggregated_properties (Sequence[str]): Set of aggregated properties to include.
            partitions (int): Retrieve assets in parallel using this number of workers. Also requires `limit=None` to be passed.
            limit (int, optional): Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

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

        asset_subtree_ids_processed = None
        if asset_subtree_ids or asset_subtree_external_ids:
            asset_subtree_ids_processed = IdentifierSequence.load(
                asset_subtree_ids, asset_subtree_external_ids
            ).as_dicts()

        data_set_ids_processed = None
        if data_set_ids or data_set_external_ids:
            data_set_ids_processed = IdentifierSequence.load(data_set_ids, data_set_external_ids).as_dicts()

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

    def aggregate(self, filter: Union[AssetFilter, dict] = None) -> List[AssetAggregate]:
        """`Aggregate assets <https://docs.cognite.com/api/v1/#operation/aggregateAssets>`_

        Args:
            filter (Union[AssetFilter, Dict]): Filter on assets filter with exact match

        Returns:
            List[AssetAggregate]: List of asset aggregates

        Examples:

            Aggregate assets:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_by_prefix = c.assets.aggregate(filter={"external_id_prefix": "prefix"})
        """
        return self._aggregate(filter=filter, cls=AssetAggregate)

    def aggregate_metadata_keys(self, filter: Union[AssetFilter, dict] = None) -> Sequence[AggregateBucketResult]:
        """`Aggregate assets <https://docs.cognite.com/api/v1/#operation/aggregateAssets>`_

        Note:
            In the case of text fields, the values are aggregated in a case-insensitive manner

        Args:
            filter (Union[AssetFilter, Dict]): Filter on assets filter with exact match

        Returns:
            Sequence[AggregateBucketResult]: List of asset aggregates

        Examples:

            Aggregate assets:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> aggregate_by_prefix = c.assets.aggregate_metadata_keys(filter={"external_id_prefix": "prefix"})
        """
        return self._aggregate(filter=filter, aggregate="metadataKeys", cls=AggregateBucketResult)

    def aggregate_metadata_values(
        self, keys: Sequence[str], filter: Union[AssetFilter, dict] = None
    ) -> Sequence[AggregateBucketResult]:
        """`Aggregate assets <https://docs.cognite.com/api/v1/#operation/aggregateAssets>`_

        Note:
            In the case of text fields, the values are aggregated in a case-insensitive manner

        Args:
            filter (Union[AssetFilter, Dict]): Filter on assets filter with exact match
            keys (Sequence[str]): Metadata key(s) to apply the aggregation on. Currently supports exactly one key per request.

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
        return self._aggregate(filter=filter, aggregate="metadataValues", keys=keys, cls=AggregateBucketResult)

    @overload
    def create(self, asset: Sequence[Asset]) -> AssetList:
        ...

    @overload
    def create(self, asset: Asset) -> Asset:
        ...

    def create(self, asset: Union[Asset, Sequence[Asset]]) -> Union[Asset, AssetList]:
        """`Create one or more assets. <https://docs.cognite.com/api/v1/#operation/createAssets>`_

        You can create an arbitrary number of assets, and the SDK will split the request into multiple requests.
        When specifying parent-child relation between assets using `parentExternalId` the link will be resvoled into an internal ID and stored as `parentId`.

        Args:
            asset (Union[Asset, Sequence[Asset]]): Asset or list of assets to create.

        Returns:
            Union[Asset, AssetList]: Created asset(s)

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
        utils._auxiliary.assert_type(asset, "asset", [Asset, Sequence])
        return self._create_multiple(list_cls=AssetList, resource_cls=Asset, items=asset)

    def create_hierarchy(
        self,
        assets: Union[Sequence[Asset], AssetHierarchy],
        upsert: bool = False,
        upsert_mode: UpsertOptions = "patch",
    ) -> AssetList:
        """Create an asset hierarchy with validation.

        This helper function makes it easy to insert large asset hierarchies. It solves the problem of topological insertion
        order, i.e. a parent asset must exist before it can be referenced by any 'children' assets.

        Additionally, prior to insertion, this function will run validation on the given assets, raising an error if ('category'
        below is the name of the attribute you might access on the raised error to get the collection of 'bad' assets):

            1. Any assets are invalid (category "invalid"):
                1. Missing external ID
                2. Missing a valid name
                3. Has an ID set
            2. Any asset duplicates exist (category "duplicates")
            3. Any assets have an ambiguous parent link (category "unsure_parents")
            4. Any group of assets form a cycle, e.g. A->B->A (category "cycles")

        It is worth noting that validation is done "offline", i.e. existing assets in CDF are not inspected. This means:

            1. All assets linking a parent by ID are assumed valid
            2. All orphan assets are assumed valid. "Orphan" means the parent is not part of the given assets (category "orphans")

        Args:
            assets (Sequence[Asset] | AssetHierarchy): List of assets to create or an instance of AssetHierarchy.
            upsert (bool): Already existing assets will be updated instead of an exception being raised. You may control how updates
                are applied with the 'upsert_mode' argument.
            upsert_mode ("patch" | "replace"): Only applicable with upsert. Pass 'patch' to only update fields with non-null values
                (default), or 'replace' to do full updates.

        Returns:
            AssetList: Created (and possibly updated) asset hierarchy

        Examples:

            Create asset hierarchy::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> c = CogniteClient()
                >>> assets = [
                ...     Asset(external_id="root", name="root"),
                ...     Asset(external_id="child1", parent_external_id="root", name="child1"),
                ...     Asset(external_id="child2", parent_external_id="root", name="child2")]
                >>> res = c.assets.create_hierarchy(assets)

            If the validation for some reason fail, you may inspect the issues by catching CogniteAssetHierarchyError::

                >>> from cognite.client.exceptions import CogniteAssetHierarchyError
                >>> try:
                ...     res = c.assets.create_hierarchy(assets)
                ... except CogniteAssetHierarchyError as err:
                ...     if err.duplicates:
                ...         ...  # do something

            In addition to 'duplicates', you may inspect 'invalid', 'unsure_parents', 'orphans' and 'cycles'. Note that
            cycles are not available if any of the other basic issues exist, as the search for cyclical references
            requires a clean asset hierarchy to begin with.

            You may also run this validation prior to calling `AssetsAPI.create_hierarchy()`.
            See :class:`~cognite.client.data_classes.assets.AssetHierarchy`.
        """
        if not isinstance(assets, AssetHierarchy):
            utils._auxiliary.assert_type(assets, "assets", [Sequence])
            assets = AssetHierarchy(assets, ignore_orphans=True)

        return _AssetHierarchyCreator(assets, assets_api=self).create(upsert, upsert_mode)

    def delete(
        self,
        id: Union[int, Sequence[int]] = None,
        external_id: Union[str, Sequence[str]] = None,
        recursive: bool = False,
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete one or more assets <https://doc.cognitedata.com/api/v1/#operation/deleteAssets>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of ids
            external_id (Union[str, Sequence[str]]): External ID or list of external ids
            recursive (bool): Recursively delete whole asset subtrees under given ids. Defaults to False.
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            None

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
    def update(self, item: Sequence[Union[Asset, AssetUpdate]]) -> AssetList:
        ...

    @overload
    def update(self, item: Union[Asset, AssetUpdate]) -> Asset:
        ...

    def update(self, item: Union[Asset, AssetUpdate, Sequence[Union[Asset, AssetUpdate]]]) -> Union[Asset, AssetList]:
        """`Update one or more assets <https://docs.cognite.com/api/v1/#operation/updateAssets>`_
        Labels can be added, removed or replaced (set). Note that set operation deletes all the existing labels and adds the new specified labels.

        Args:
            item (Union[Asset, AssetUpdate, Sequence[Union[Asset, AssetUpdate]]]): Asset(s) to update

        Returns:
            Union[Asset, AssetList]: Updated asset(s)

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

            Rewrite all labels for an asset::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).labels.set("PUMP")
                >>> res = c.assets.update(my_update)
        """
        return self._update_multiple(list_cls=AssetList, resource_cls=Asset, update_cls=AssetUpdate, items=item)

    def search(
        self,
        name: str = None,
        description: str = None,
        query: str = None,
        filter: Union[AssetFilter, Dict] = None,
        limit: int = 100,
    ) -> AssetList:
        """`Search for assets <https://docs.cognite.com/api/v1/#operation/searchAssets>`_
        Primarily meant for human-centric use-cases and data exploration, not for programs, since matching and ordering may change over time. Use the `list` function if stable or exact matches are required.

        Args:
            name (str): Fuzzy match on name.
            description (str): Fuzzy match on description.
            query (str): Whitespace-separated terms to search for in assets. Does a best-effort fuzzy search in relevant fields (currently name and description) for variations of any of the search terms, and orders results by relevance.
            filter (Union[AssetFilter, Dict]): Filter to apply. Performs exact match on these fields.
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

    def retrieve_subtree(self, id: int = None, external_id: str = None, depth: int = None) -> AssetList:
        """Retrieve the subtree for this asset up to a specified depth.

        Args:
            id (int): Id of the root asset in the subtree.
            external_id (str): External id of the root asset in the subtree.
            depth (int): Retrieve assets up to this depth below the root asset in the subtree. Omit to get the entire
                subtree.

        Returns:
            AssetList: The requested assets or empty AssetList if asset does not exist.
        """
        asset = self.retrieve(id=id, external_id=external_id)
        if asset is None:
            return AssetList([], self._cognite_client)
        subtree = self._get_asset_subtree([asset], current_depth=0, depth=depth)
        return AssetList(subtree, self._cognite_client)

    def _get_asset_subtree(self, assets: List, current_depth: int, depth: Optional[int]) -> List:
        subtree = assets
        if depth is None or current_depth < depth:
            children = self._get_children(assets)
            if children:
                subtree.extend(self._get_asset_subtree(children, current_depth + 1, depth))
        return subtree

    def _get_children(self, assets: List) -> List:
        ids = [a.id for a in assets]
        tasks = []
        chunk_size = 100
        for i in range(0, len(ids), chunk_size):
            tasks.append({"parent_ids": ids[i : i + chunk_size], "limit": -1})
        tasks_summary = utils._concurrency.execute_tasks(self.list, tasks=tasks, max_workers=self._config.max_workers)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        res_list = tasks_summary.results
        children = []
        for res in res_list:
            children.extend(res)
        return children


_CreateTask = namedtuple("_CreateTask", ("items", "priority"))


class _AssetHierarchyCreator:
    def __init__(self, hierarchy: AssetHierarchy, assets_api: AssetsAPI) -> None:
        hierarchy.is_valid(on_error="raise")
        self.hierarchy = hierarchy
        self.n_assets = len(hierarchy._assets)
        self.assets_api = assets_api
        self.create_limit = assets_api._CREATE_LIMIT
        self.resource_path = assets_api._RESOURCE_PATH
        self.max_workers = assets_api._config.max_workers
        self.failed: List[Asset] = []
        self.unknown: List[Asset] = []
        self.exception: Union[CogniteAPIError, CogniteDuplicatedError, None] = None

        self.__counter = itertools.count().__next__

    def create(self, upsert: bool, upsert_mode: UpsertOptions) -> AssetList:
        insert_fn = functools.partial(self._insert, upsert=upsert, upsert_mode=upsert_mode)
        insert_dct = self.hierarchy.groupby_parent_xid()
        subtree_count = self.hierarchy.count_subtree(insert_dct)

        with get_priority_executor(max_workers=self.max_workers) as pool:
            return self._create(pool, insert_fn, insert_dct, subtree_count)

    def _create(
        self,
        pool: PriorityThreadPoolExecutor,
        insert_fn: Callable,
        insert_dct: Dict[Optional[str], List[Asset]],
        subtree_count: Dict[str, int],
    ) -> AssetList:
        queue_fn = functools.partial(
            self._queue_tasks,
            pool=pool,
            insert_fn=insert_fn,
            insert_dct=insert_dct,
            subtree_count=subtree_count,
        )
        created_assets = AssetList([], cognite_client=self.assets_api._cognite_client)

        # Kick things off with all...:
        # 1. Root assets
        # 2. Assets linking parent by ID
        # 3. Orphans assets (if `hierarchy.ignore_orphans` is True)
        futures = queue_fn(insert_dct.pop(None))

        as_completed = get_as_completed_fn(pool)
        while futures:
            futures.remove(fut := next(as_completed(futures)))
            new_assets, failed, unknown = fut.result()
            created_assets.extend(new_assets)
            # if unknown or failed:
            #     self.failed.extend(failed)
            #     self.unknown.extend(unknown)
            #     self._skip_all_descendants(unknown, failed, insert_dct)

            # Newly created assets are now unblocked as parents for the next iteration:
            to_create = list(self._get_child_assets(new_assets, insert_dct))
            futures |= queue_fn(to_create)

        # if self.unknown or self.failed:
        #     self._raise_latest_exception(created_assets)
        return created_assets

    def _queue_tasks(
        self,
        assets: List[Asset],
        *,
        pool: PriorityThreadPoolExecutor,
        insert_fn: Callable,
        insert_dct: Dict[Optional[str], List[Asset]],
        subtree_count: Dict[str, int],
    ) -> Set[Future]:
        if not assets:
            return set()
        return {
            pool.submit(insert_fn, self._dump_assets(task.items), priority=self.n_assets - task.priority)
            for task in self._split_and_prioritise_assets(assets, insert_dct, subtree_count)
        }

    def _insert(
        self,
        assets: Dict[str, List[Dict]],
        *,
        upsert: bool,
        upsert_mode: UpsertOptions,
    ) -> Tuple[AssetList, List[Asset], List[Asset]]:
        try:
            resp = self.assets_api._post(self.resource_path, assets)
            success = AssetList._load(resp.json()["items"])
            return success, [], []

        except (CogniteAPIError, CogniteDuplicatedError) as err:
            raise NotImplementedError("upsert not rdy")
            if err.duplicated:
                if not upsert:
                    raise
                # TODO: How to deal with exceptions here?
                updated = self._update(assets, cast(List, err.duplicated), cast(List, err.failed), upsert_mode)
                err.successful.extend(updated)  # type: ignore [attr-defined]

            self._update_latest_exception(err)
            return cast(AssetList, err.successful), cast(List[Asset], err.failed), cast(List[Asset], err.unknown)

    def _update(
        self, assets: List[Asset], duplicated: List[Dict], failed: List[Asset], upsert_mode: UpsertOptions
    ) -> AssetList:
        subset = self._extract_subset(duplicated, assets)
        if upsert_mode == "patch":
            updated = self.assets_api.update(subset)

        elif upsert_mode == "replace":
            updates = list(map(self.make_replace_update, subset))
            updated = self.assets_api.update(updates)
        else:
            raise ValueError(f"'upsert_mode' must be one of {list(UpsertOptions.__args__)}, not {upsert_mode!r}")

        # Duplicated are also passed as failed, we need to clear up which are not failures:
        # TODO: Verify implementation works when we use inserts using 'parent_id'
        still_failed = [f for f in failed if updated.get(external_id=f.external_id) is None]
        failed.clear()
        if still_failed:
            failed.extend(still_failed)
        return updated

    def make_replace_update(self, asset: Asset) -> AssetUpdate:
        # TODO: The SDK makes it very hard to do full updates... this feels hacky:
        patch = asset.dump(camel_case=True)
        full_update = {**self.clear_all_update, **{k: {"set": v} for k, v in patch.items()}}
        for key in ("ExternalId", "parentId", "parentExternalId"):
            full_update.pop(key, None)
        (upd := AssetUpdate(external_id=asset.external_id))._update_object = full_update
        return upd

    @cached_property
    def clear_all_update(self) -> MappingProxyType[str, Dict[str, Any]]:
        props = set(map(to_camel_case, AssetUpdate._get_update_properties()))
        dct: Dict[str, Dict[str, Any]] = {k: {"setNull": True} for k in props}
        # Handle metadata separately... lol:
        # https://github.com/cognitedata/cognite-sdk-python/pull/757#issuecomment-723973727
        dct["metadata"] = {"set": {}}
        return MappingProxyType(dct)

    def _split_and_prioritise_assets(
        self,
        to_create: List[Asset],
        insert_dct: Dict[Optional[str], List[Asset]],
        subtree_count: Dict[str, int],
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
    def _dump_assets(assets: Set[Asset]) -> Dict[str, List[Dict[str, Any]]]:
        return {"items": [asset.dump(camel_case=True) for asset in assets]}

    @staticmethod
    def _recombine_chunks(lst: List[_CreateTask], limit: int) -> Iterator[_CreateTask]:
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
        to_create: Set[Asset],
        insert_dct: Dict[Optional[str], List[Asset]],
        subtree_count: Dict[str, int],
    ) -> _CreateTask:
        pri_q = [(-subtree_count[cast(str, asset.external_id)], self.__counter(), asset) for asset in to_create]
        heapq.heapify(pri_q)
        priority = -pri_q[0][0]  # No child asset can have a larger subtree than its parent

        while pri_q:  # Queue should seriously be spelled q
            *_, asset = heapq.heappop(pri_q)
            to_create.add(asset)
            if children := insert_dct.get(asset.parent_external_id):
                children.remove(asset)  # TODO: Change to set (at least benchmark on real data)
            if len(to_create) == self.create_limit:
                break
            for child in insert_dct.get(asset.external_id, []):
                heapq.heappush(pri_q, (-subtree_count[cast(str, child.external_id)], self.__counter(), child))

        return _CreateTask(to_create, priority)

    @staticmethod
    def _get_child_assets(assets: Iterable[Asset], insert_dct: Dict[Optional[str], List[Asset]]) -> Iterator[Asset]:
        return itertools.chain.from_iterable(insert_dct.pop(asset.external_id, []) for asset in assets)

    @staticmethod
    def _extract_subset(xids_subset: List[Dict[str, str]], assets: List[Asset]) -> List[Asset]:
        # Avoids repeated list-lookups (O(N^2))
        lookup = {asset.external_id: asset for asset in assets}
        return [lookup[cast(str, xid)] for xid in map(unwrap_identifer, xids_subset)]

    def _skip_all_descendants(
        self,
        unknown: List[Asset],
        failed: List[Asset],
        insert_dct: Dict[Optional[str], List[Asset]],
    ) -> None:
        skip_assets = [*unknown, *failed]
        while skip_assets:
            skip_assets = list(self._get_child_assets(skip_assets, insert_dct))
            self.failed.extend(skip_assets)

    def _update_latest_exception(self, exception: Union[CogniteAPIError, CogniteDuplicatedError]) -> None:
        if exception.failed or exception.unknown:
            self.exception = exception

    def _raise_latest_exception(self, successful: AssetList) -> NoReturn:
        assert self.exception is not None
        err_details: Dict[str, Any] = dict(
            message="One or more errors happened during asset creation",
            code=None,
            successful=successful,
            unknown=AssetList(self.unknown),
            failed=AssetList(self.failed),
            unwrap_fn=op.attrgetter("external_id"),
        )
        if isinstance(self.exception, CogniteAPIError):
            err_details.update(
                message=f"{err_details['message']}. Latest error: {self.exception.message}",
                code=self.exception.code,
                x_request_id=self.exception.x_request_id,
            )
        raise CogniteAPIError(**err_details)  # type: ignore [arg-type]
