from __future__ import annotations

import queue
import threading
from collections import OrderedDict
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Union, cast, overload

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    Asset,
    AssetAggregate,
    AssetFilter,
    AssetList,
    AssetUpdate,
    GeoLocationFilter,
    LabelFilter,
    TimestampRange,
)
from cognite.client.data_classes.shared import AggregateBucketResult
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._identifier import IdentifierSequence


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
            aggregated_properties = [utils._auxiliary.to_camel_case(s) for s in aggregated_properties]

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
            aggregated_properties = [utils._auxiliary.to_camel_case(s) for s in aggregated_properties]

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

    def create_hierarchy(self, assets: Sequence[Asset]) -> AssetList:
        """Create asset hierarchy. Like the create() method, when posting a large number of assets, the IDE will split the request into smaller requests.
        However, create_hierarchy() will additionally make sure that the assets are posted in correct order. The ordering is determined from the
        external_id and parent_external_id properties of the assets, and the external_id is therefore required for all assets. Before posting, it is
        checked that all assets have a unique external_id and that there are no circular dependencies.

        Args:
            assets (Sequence[Asset]]): List of assets to create. Requires each asset to have a unique external id.

        Returns:
            AssetList: Created asset hierarchy

        Examples:

            Create asset hierarchy::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> c = CogniteClient()
                >>> assets = [Asset(external_id="root", name="root"), Asset(external_id="child1", parent_external_id="root", name="child1"), Asset(external_id="child2", parent_external_id="root", name="child2")]
                >>> res = c.assets.create_hierarchy(assets)
        """
        utils._auxiliary.assert_type(assets, "assets", [Sequence])
        return _AssetPoster(assets, client=self).post()

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


class _AssetsFailedToPost:
    def __init__(self, exc: Exception, assets: Sequence[Asset]):
        self.exc = exc
        self.assets = assets


class _AssetPosterWorker(threading.Thread):
    def __init__(
        self,
        client: AssetsAPI,
        request_queue: queue.Queue,
        response_queue: queue.Queue,
    ):
        self.client = client
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.stop = False
        super().__init__(daemon=True)

    def run(self) -> None:
        request: Sequence[Asset] = []
        while not self.stop:
            try:
                try:
                    request = cast(Sequence[Asset], self.request_queue.get(timeout=0.1))
                except queue.Empty:
                    continue
                assets = cast(AssetList, self.client.create(request))
                self.response_queue.put(assets)
            except Exception as e:
                self.response_queue.put(_AssetsFailedToPost(e, request))


class _AssetPoster:
    def __init__(self, assets: Sequence[Asset], client: AssetsAPI):
        self._validate_asset_hierarchy(assets)
        self.remaining_external_ids: OrderedDict[str, Optional[str]] = OrderedDict()
        self.remaining_external_ids_set = set()
        self.external_id_to_asset = {}

        for asset in assets:
            self.remaining_external_ids[cast(str, asset.external_id)] = None
            self.remaining_external_ids_set.add(asset.external_id)
            self.external_id_to_asset[asset.external_id] = asset

        self.client = client

        self.num_of_assets = len(self.remaining_external_ids)
        self.external_ids_without_circular_deps: Set[str] = set()
        self.external_id_to_children: Dict[str, Set[Asset]] = {
            external_id: set() for external_id in self.remaining_external_ids
        }
        self.external_id_to_descendent_count = {external_id: 0 for external_id in self.remaining_external_ids}
        self.successfully_posted_external_ids: Set[str] = set()
        self.posted_assets: Set[Asset] = set()
        self.may_have_been_posted_assets: Set[Asset] = set()
        self.not_posted_assets: Set[Asset] = set()
        self.exception: Optional[Exception] = None

        self.request_queue: queue.Queue[Sequence[Asset]] = queue.Queue()
        self.response_queue: queue.Queue[Union[AssetList, _AssetsFailedToPost]] = queue.Queue()

        self._initialize()

    def assets_remaining(self) -> bool:
        return (
            len(self.posted_assets) + len(self.may_have_been_posted_assets) + len(self.not_posted_assets)
            < self.num_of_assets
        )

    @staticmethod
    def _validate_asset_hierarchy(assets: Sequence[Asset]) -> None:
        external_ids_seen = set()
        for asset in assets:
            if asset.external_id is None:
                raise AssertionError("An asset does not have external_id.")
            if asset.external_id in external_ids_seen:
                raise AssertionError(f"Duplicate external_id '{asset.external_id}' found")
            external_ids_seen.add(asset.external_id)

            parent_ref = asset.parent_external_id
            if parent_ref:
                if asset.parent_id is not None:
                    raise AssertionError(
                        f"An asset has both parent_id '{asset.parent_id}' and parent_external_id "
                        f"'{asset.parent_external_id}' set."
                    )

    def _initialize(self) -> None:
        root_assets = set()
        for external_id in self.remaining_external_ids:
            asset = self.external_id_to_asset[external_id]
            if asset.parent_external_id not in self.external_id_to_asset or asset.parent_id is not None:
                root_assets.add(asset)
            elif asset.parent_external_id in self.external_id_to_children:
                self.external_id_to_children[asset.parent_external_id].add(asset)
            self._verify_asset_is_not_part_of_tree_with_circular_deps(asset)

        for root_asset in root_assets:
            self._initialize_asset_to_descendant_count(root_asset)

        self.remaining_external_ids = self._sort_external_ids_by_descendant_count(self.remaining_external_ids)

    def _initialize_asset_to_descendant_count(self, asset: Asset) -> int:
        for child in self.external_id_to_children[cast(str, asset.external_id)]:
            self.external_id_to_descendent_count[
                cast(str, asset.external_id)
            ] += 1 + self._initialize_asset_to_descendant_count(child)
        return self.external_id_to_descendent_count[cast(str, asset.external_id)]

    def _get_descendants(self, asset: Asset) -> Sequence[Asset]:
        descendants = []
        for child in self.external_id_to_children[cast(str, asset.external_id)]:
            descendants.append(child)
            descendants.extend(self._get_descendants(child))
        return descendants

    def _verify_asset_is_not_part_of_tree_with_circular_deps(self, asset: Asset) -> None:
        next_asset: Optional[Asset] = asset
        seen = {cast(str, asset.external_id)}
        while next_asset is not None and next_asset.parent_external_id is not None:
            next_asset = self.external_id_to_asset.get(next_asset.parent_external_id)
            if next_asset is None:
                break
            if next_asset.external_id in self.external_ids_without_circular_deps:
                break
            if next_asset.external_id not in seen:
                seen.add(cast(str, next_asset.external_id))
            else:
                raise AssertionError("The asset hierarchy has circular dependencies")
        self.external_ids_without_circular_deps.update(seen)

    def _sort_external_ids_by_descendant_count(self, external_ids: OrderedDict) -> OrderedDict:
        sorted_external_ids = sorted(external_ids, key=lambda x: self.external_id_to_descendent_count[x], reverse=True)
        return OrderedDict({external_id: None for external_id in sorted_external_ids})

    def _get_assets_unblocked_locally(self, asset: Asset, limit: int) -> Set[Asset]:
        pq = utils._auxiliary.PriorityQueue()
        pq.add(asset, self.external_id_to_descendent_count[cast(str, asset.external_id)])
        unblocked_descendents: Set[Asset] = set()
        while pq:
            if len(unblocked_descendents) == limit:
                break
            asset = pq.get()
            unblocked_descendents.add(asset)
            self.remaining_external_ids_set.remove(asset.external_id)
            for child in self.external_id_to_children[cast(str, asset.external_id)]:
                pq.add(child, self.external_id_to_descendent_count[cast(str, child.external_id)])
        return unblocked_descendents

    def _get_unblocked_assets(self) -> Sequence[Set[Asset]]:
        limit = self.client._CREATE_LIMIT
        unblocked_assets_lists = []
        unblocked_assets_chunk: Set[Asset] = set()
        for external_id in self.remaining_external_ids:
            asset = self.external_id_to_asset[external_id]
            parent_external_id = asset.parent_external_id

            if external_id in self.remaining_external_ids_set:
                has_parent_id = asset.parent_id is not None
                is_root = (not has_parent_id) and parent_external_id not in self.external_id_to_asset
                is_unblocked = parent_external_id in self.successfully_posted_external_ids
                if is_root or has_parent_id or is_unblocked:
                    unblocked_assets_chunk.update(
                        self._get_assets_unblocked_locally(asset, limit - len(unblocked_assets_chunk))
                    )
                    if len(unblocked_assets_chunk) == limit:
                        unblocked_assets_lists.append(unblocked_assets_chunk)
                        unblocked_assets_chunk = set()
        if len(unblocked_assets_chunk) > 0:
            unblocked_assets_lists.append(unblocked_assets_chunk)

        for unblocked_assets_chunk in unblocked_assets_lists:
            for unblocked_asset in unblocked_assets_chunk:
                del self.remaining_external_ids[cast(str, unblocked_asset.external_id)]

        return unblocked_assets_lists

    def run(self) -> None:
        unblocked_assets_lists = self._get_unblocked_assets()
        for unblocked_assets in unblocked_assets_lists:
            self.request_queue.put(list(unblocked_assets))
        while self.assets_remaining():
            res = self.response_queue.get()
            if isinstance(res, _AssetsFailedToPost):
                if isinstance(res.exc, CogniteAPIError):
                    self.exception = res.exc
                    for asset in res.assets:
                        if res.exc.code >= 500:
                            self.may_have_been_posted_assets.add(asset)
                        elif res.exc.code >= 400:
                            self.not_posted_assets.add(asset)
                        for descendant in self._get_descendants(asset):
                            self.not_posted_assets.add(descendant)
                else:
                    raise res.exc
            else:
                for asset in res:
                    self.posted_assets.add(asset)
                    self.successfully_posted_external_ids.add(cast(str, asset.external_id))
                unblocked_assets_lists = self._get_unblocked_assets()
                for unblocked_assets in unblocked_assets_lists:
                    self.request_queue.put(list(unblocked_assets))
        if len(self.may_have_been_posted_assets) > 0 or len(self.not_posted_assets) > 0:
            if isinstance(self.exception, CogniteAPIError):
                raise CogniteAPIError(
                    message=self.exception.message,
                    code=self.exception.code,
                    x_request_id=self.exception.x_request_id,
                    missing=self.exception.missing,
                    duplicated=self.exception.duplicated,
                    successful=AssetList(list(self.posted_assets)),
                    unknown=AssetList(list(self.may_have_been_posted_assets)),
                    failed=AssetList(list(self.not_posted_assets)),
                    unwrap_fn=lambda a: a.external_id,
                )
            raise cast(Exception, self.exception)

    def post(self) -> AssetList:
        workers = []
        for _ in range(self.client._config.max_workers):
            worker = _AssetPosterWorker(self.client, self.request_queue, self.response_queue)
            workers.append(worker)
            worker.start()

        self.run()

        for worker in workers:
            worker.stop = True

        return AssetList(self.posted_assets)
