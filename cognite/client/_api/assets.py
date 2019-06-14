import queue
import threading
from collections import OrderedDict
from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import Asset, AssetFilter, AssetList, AssetUpdate
from cognite.client.exceptions import CogniteAPIError


class AssetsAPI(APIClient):
    _RESOURCE_PATH = "/assets"
    _LIST_CLASS = AssetList

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        root: bool = None,
        external_id_prefix: str = None,
        limit: int = None,
    ) -> Generator[Union[Asset, AssetList], None, None]:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Args:
            chunk_size (int, optional): Number of assets to return in each chunk. Defaults to yielding one asset a time.
            name (str): Name of asset. Often referred to as tag.
            parent_ids (List[int]): No description.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            source (str): The source of this asset
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            root (bool): filtered assets are root assets or not
            external_id_prefix (str): External Id provided by client. Should be unique within the project
            limit (int, optional): Maximum number of assets to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Yields:
            Union[Asset, AssetList]: yields Asset one by one if chunk is not specified, else AssetList objects.
        """

        filter = AssetFilter(
            name, parent_ids, metadata, source, created_time, last_updated_time, root, external_id_prefix
        ).dump(camel_case=True)
        return self._list_generator(method="POST", chunk_size=chunk_size, filter=filter, limit=limit)

    def __iter__(self) -> Generator[Asset, None, None]:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Yields:
            Asset: yields Assets one by one.
        """
        return self.__call__()

    def retrieve(self, id: Optional[int] = None, external_id: Optional[str] = None) -> Optional[Asset]:
        """Retrieve a single asset by id.

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
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        return self._retrieve_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def retrieve_multiple(self, ids: Optional[List[int]] = None, external_ids: Optional[List[str]] = None) -> AssetList:
        """Retrieve multiple assets by id.

        Args:
            ids (List[int], optional): IDs
            external_ids (List[str], optional): External IDs

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
                >>> res = c.assets.retrieve_multiple(external_ids=["abc", "def"])
        """
        utils._auxiliary.assert_type(ids, "id", [List], allow_none=True)
        utils._auxiliary.assert_type(external_ids, "external_id", [List], allow_none=True)
        return self._retrieve_multiple(ids=ids, external_ids=external_ids, wrap_ids=True)

    def list(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        root: bool = None,
        external_id_prefix: str = None,
        limit: int = 25,
    ) -> AssetList:
        """List assets

        Args:
            name (str): Name of asset. Often referred to as tag.
            parent_ids (List[int]): No description.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            source (str): The source of this asset
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            root (bool): filtered assets are root assets or not
            external_id_prefix (str): External Id provided by client. Should be unique within the project
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
        """
        filter = AssetFilter(
            name, parent_ids, metadata, source, created_time, last_updated_time, root, external_id_prefix
        ).dump(camel_case=True)
        return self._list(method="POST", limit=limit, filter=filter)

    def create(self, asset: Union[Asset, List[Asset]]) -> Union[Asset, AssetList]:
        """Create one or more assets.

        Args:
            asset (Union[Asset, List[Asset]]): Asset or list of assets to create.

        Returns:
            Union[Asset, AssetList]: Created asset(s)

        Examples:

            Create new assets::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Asset
                >>> c = CogniteClient()
                >>> assets = [Asset(name="asset1"), Asset(name="asset2")]
                >>> res = c.assets.create(assets)
        """
        utils._auxiliary.assert_type(asset, "asset", [Asset, list])
        if isinstance(asset, Asset) or len(asset) <= self._CREATE_LIMIT:
            return self._create_multiple(asset)
        return _AssetPoster(asset, client=self).post()

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete one or more assets

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of exgernal ids

        Returns:
            None

        Examples:

            Delete assets by id or external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.delete(id=[1,2,3], external_id="3")
        """
        self._delete_multiple(ids=id, external_ids=external_id, wrap_ids=True)

    def update(self, item: Union[Asset, AssetUpdate, List[Union[Asset, AssetUpdate]]]) -> Union[Asset, AssetList]:
        """Update one or more assets

        Args:
            item (Union[Asset, AssetUpdate, List[Union[Asset, AssetUpdate]]]): Asset(s) to update

        Returns:
            Union[Asset, AssetList]: Updated asset(s)

        Examples:

            Update an asset that you have fetched. This will perform a full update of the asset::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> asset = c.assets.retrieve(id=1)
                >>> asset.description = "New description"
                >>> res = c.assets.update(asset)

            Perform a partial update on a asset, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.assets.update(my_update)
        """
        return self._update_multiple(items=item)

    def search(
        self, name: str = None, description: str = None, filter: Union[AssetFilter, Dict] = None, limit: int = None
    ) -> AssetList:
        """Search for assets

        Args:
            name (str): Fuzzy match on name.
            description (str): Fuzzy match on description.
            filter (Union[AssetFilter, Dict]): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            AssetList: List of requested assets

        Examples:

            Search for assets::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.search(name="some name")
        """
        return self._search(search={"name": name, "description": description}, filter=filter, limit=limit)

    def retrieve_subtree(self, id: int = None, external_id: str = None, depth: int = None) -> AssetList:
        """Retrieve the subtree for this asset up to a specified depth.

        Args:
            id (int): Id of the root asset in the subtree.
            external_id (str): External id of the root asset in the subtree.
            depth (int): Retrieve assets up to this depth below the root asset in the subtree. Omit to get the entire
                subtree.

        Returns:
            AssetList: The requested assets sorted topologically.
        """
        utils._auxiliary.assert_exactly_one_of_id_or_external_id(id, external_id)
        asset = self.retrieve(id=id, external_id=external_id)
        subtree = self._get_asset_subtree(AssetList([asset]), current_depth=0, depth=depth)
        return AssetList(sorted(subtree, key=lambda a: a.depth), cognite_client=self._cognite_client)

    def _get_asset_subtree(self, assets: AssetList, current_depth: int, depth: Optional[int]) -> AssetList:
        subtree = assets
        if depth is None or current_depth < depth:
            children = self._get_children(assets)
            if children:
                subtree.extend(self._get_asset_subtree(children, current_depth + 1, depth))
        return subtree

    def _get_children(self, assets: AssetList) -> AssetList:
        ids = [a.id for a in assets]
        tasks = []
        chunk_size = 100
        for i in range(0, len(ids), chunk_size):
            tasks.append({"parent_ids": ids[i : i + chunk_size], "limit": -1})
        tasks_summary = utils._concurrency.execute_tasks_concurrently(
            self.list, tasks=tasks, max_workers=self._config.max_workers
        )
        tasks_summary.raise_compound_exception_if_failed_tasks()
        res_list = tasks_summary.results
        children = AssetList([])
        for res in res_list:
            children.extend(res)
        return children


class _AssetsFailedToPost:
    def __init__(self, exc: Exception, assets: List[Asset]):
        self.exc = exc
        self.assets = assets


class _AssetPosterWorker(threading.Thread):
    def __init__(self, client: AssetsAPI, request_queue: queue.Queue, response_queue: queue.Queue):
        self.client = client
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.stop = False
        super().__init__(daemon=True)

    def run(self):
        request = None
        try:
            while not self.stop:
                try:
                    request = self.request_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                assets = self.client.create(request)
                self.response_queue.put(assets)
        except Exception as e:
            self.response_queue.put(_AssetsFailedToPost(e, request))


class _AssetPoster:
    def __init__(self, assets: List[Asset], client: AssetsAPI):
        self._validate_asset_hierarchy(assets)
        self.remaining_external_ids = OrderedDict()
        self.remaining_external_ids_set = set()
        self.external_id_to_asset = {}

        for asset in assets:
            asset_copy = Asset(**asset.dump())
            external_id = asset.external_id
            if external_id is None:
                external_id = utils._auxiliary.random_string()
                asset_copy.external_id = external_id
            self.remaining_external_ids[external_id] = None
            self.remaining_external_ids_set.add(external_id)
            self.external_id_to_asset[external_id] = asset_copy

        self.client = client

        self.num_of_assets = len(self.remaining_external_ids)
        self.external_ids_without_circular_deps = set()
        self.external_id_to_children = {external_id: set() for external_id in self.remaining_external_ids}
        self.external_id_to_descendent_count = {external_id: 0 for external_id in self.remaining_external_ids}
        self.successfully_posted_external_ids = set()
        self.posted_assets = set()
        self.may_have_been_posted_assets = set()
        self.not_posted_assets = set()
        self.exception = None

        self.assets_remaining = (
            lambda: len(self.posted_assets) + len(self.may_have_been_posted_assets) + len(self.not_posted_assets)
            < self.num_of_assets
        )

        self.request_queue = queue.Queue()
        self.response_queue = queue.Queue()

        self._initialize()

    @staticmethod
    def _validate_asset_hierarchy(assets) -> None:
        external_ids = set([asset.external_id for asset in assets])
        external_ids_seen = set()
        for asset in assets:
            if asset.external_id:
                if asset.external_id in external_ids_seen:
                    raise AssertionError("Duplicate external_id '{}' found".format(asset.external_id))
                external_ids_seen.add(asset.external_id)

            parent_ref = asset.parent_external_id
            if parent_ref:
                if parent_ref not in external_ids:
                    raise AssertionError("parent_external_id '{}' does not point to any asset".format(parent_ref))
                if asset.parent_id is not None:
                    raise AssertionError(
                        "An asset has both parent_id '{}' and parent_external_id '{}' set.".format(
                            asset.parent_id, asset.parent_external_id
                        )
                    )

    def _initialize(self):
        root_assets = set()
        for external_id in self.remaining_external_ids:
            asset = self.external_id_to_asset[external_id]
            if asset.parent_external_id is None or asset.parent_id is not None:
                root_assets.add(asset)
            elif asset.parent_external_id in self.external_id_to_children:
                self.external_id_to_children[asset.parent_external_id].add(asset)
            self._verify_asset_is_not_part_of_tree_with_circular_deps(asset)

        for root_asset in root_assets:
            self._initialize_asset_to_descendant_count(root_asset)

        self.remaining_external_ids = self._sort_external_ids_by_descendant_count(self.remaining_external_ids)

    def _initialize_asset_to_descendant_count(self, asset):
        for child in self.external_id_to_children[asset.external_id]:
            self.external_id_to_descendent_count[asset.external_id] += 1 + self._initialize_asset_to_descendant_count(
                child
            )
        return self.external_id_to_descendent_count[asset.external_id]

    def _get_descendants(self, asset):
        descendants = []
        for child in self.external_id_to_children[asset.external_id]:
            descendants.append(child)
            descendants.extend(self._get_descendants(child))
        return descendants

    def _verify_asset_is_not_part_of_tree_with_circular_deps(self, asset: Asset):
        next_asset = asset
        seen = {asset.external_id}
        while next_asset.parent_external_id is not None:
            next_asset = self.external_id_to_asset[next_asset.parent_external_id]
            if next_asset.external_id in self.external_ids_without_circular_deps:
                break
            if next_asset.external_id not in seen:
                seen.add(next_asset.external_id)
            else:
                raise AssertionError("The asset hierarchy has circular dependencies")
        self.external_ids_without_circular_deps.update(seen)

    def _sort_external_ids_by_descendant_count(self, external_ids: OrderedDict) -> OrderedDict:
        sorted_external_ids = sorted(external_ids, key=lambda x: self.external_id_to_descendent_count[x], reverse=True)
        return OrderedDict({external_id: None for external_id in sorted_external_ids})

    def _get_assets_unblocked_locally(self, asset: Asset, limit):
        pq = utils._auxiliary.PriorityQueue()
        pq.add(asset, self.external_id_to_descendent_count[asset.external_id])
        unblocked_descendents = set()
        while pq:
            if len(unblocked_descendents) == limit:
                break
            asset = pq.get()
            unblocked_descendents.add(asset)
            self.remaining_external_ids_set.remove(asset.external_id)
            for child in self.external_id_to_children[asset.external_id]:
                pq.add(child, self.external_id_to_descendent_count[child.external_id])
        return unblocked_descendents

    def _get_unblocked_assets(self) -> List[Set[Asset]]:
        limit = self.client._CREATE_LIMIT
        unblocked_assets_lists = []
        unblocked_assets_chunk = set()
        for external_id in self.remaining_external_ids:
            asset = self.external_id_to_asset[external_id]
            parent_external_id = asset.parent_external_id

            if external_id in self.remaining_external_ids_set:
                has_parent_id = asset.parent_id is not None
                is_root = (not has_parent_id) and parent_external_id is None
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
                del self.remaining_external_ids[unblocked_asset.external_id]

        return unblocked_assets_lists

    def run(self):
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
                    self.successfully_posted_external_ids.add(asset.external_id)
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
            raise self.exception

    def post(self):
        workers = []
        for _ in range(self.client._config.max_workers):
            worker = _AssetPosterWorker(self.client, self.request_queue, self.response_queue)
            workers.append(worker)
            worker.start()

        self.run()

        for worker in workers:
            worker.stop = True

        return AssetList(sorted(self.posted_assets, key=lambda x: x.path))
