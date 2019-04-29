import queue
import threading
from collections import OrderedDict
from typing import *

from cognite.client._api_client import APIClient
from cognite.client._base import *
from cognite.client._utils import PriorityQueue
from cognite.client.exceptions import CogniteAPIError, CogniteAssetPostingError


# GenClass: Asset, ExternalAssetItem
class Asset(CogniteResource):
    """Representation of a physical asset, e.g plant or piece of equipment

    Args:
        external_id (str): External Id provided by client. Should be unique within the project.
        name (str): Name of asset. Often referred to as tag.
        parent_id (int): Javascript friendly internal ID given to the object.
        description (str): Description of asset.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        source (str): The source of this asset
        id (int): Javascript friendly internal ID given to the object.
        last_updated_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        path (List[int]): IDs of assets on the path to the asset.
        depth (int): Asset path depth (number of levels below root node).
        ref_id (str): Reference ID used only in post request to disambiguate references to duplicate names.
        parent_ref_id (str): Reference ID of parent, to disambiguate if multiple nodes have the same name.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        parent_id: int = None,
        description: str = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        id: int = None,
        last_updated_time: int = None,
        path: List[int] = None,
        depth: int = None,
        ref_id: str = None,
        parent_ref_id: str = None,
        **kwargs
    ):
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.metadata = metadata
        self.source = source
        self.id = id
        self.last_updated_time = last_updated_time
        self.path = path
        self.depth = depth
        self.ref_id = ref_id
        self.parent_ref_id = parent_ref_id

    # GenStop

    def __hash__(self):
        return hash(self.ref_id)

    def parent(self) -> "Asset":
        """Returns this assets parent.

        Returns:
            Asset: The parent asset.
        """
        if self.parent_id is None:
            raise ValueError("parent_id is None")
        return self._client.assets.get(id=self.parent_id)

    def children(self) -> "AssetList":
        """Returns the children of this asset.

        Returns:
            AssetList: The requested assets
        """
        return self._client.assets.list(parent_ids=[self.id])

    def subtree(self) -> "AssetList":
        """Returns the subtree of this asset.

        Returns:
            AssetList: The requested subtree
        """
        return self._client.assets.list(asset_subtrees=[self.id])


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def external_id(self):
        return _PrimitiveAssetUpdate(self, "externalId")

    @property
    def name(self):
        return _PrimitiveAssetUpdate(self, "name")

    @property
    def description(self):
        return _PrimitiveAssetUpdate(self, "description")

    @property
    def metadata(self):
        return _ObjectAssetUpdate(self, "metadata")

    @property
    def source(self):
        return _PrimitiveAssetUpdate(self, "source")


class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> AssetUpdate:
        return self._set(value)


class _ObjectAssetUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> AssetUpdate:
        return self._set(value)

    def add(self, value: Dict) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)


class _ListAssetUpdate(CogniteListUpdate):
    def set(self, value: List) -> AssetUpdate:
        return self._set(value)

    def add(self, value: List) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)

    # GenStop


class AssetList(CogniteResourceList):
    _RESOURCE = Asset
    _UPDATE = AssetUpdate

    def _indented_asset_str(self, asset: Asset):
        single_indent = " " * 8
        marked_indent = "|______ "
        indent = len(asset.path) - 1

        s = single_indent * (indent - 1)
        if indent > 0:
            s += marked_indent
        s += str(asset.id) + "\n"
        dumped = asset.dump()
        for key, value in sorted(dumped.items()):
            if isinstance(value, dict):
                s += single_indent * indent + "{}:\n".format(key)
                for mkey, mvalue in sorted(value.items()):
                    s += single_indent * indent + " - {}: {}\n".format(mkey, mvalue)
            elif key != "id":
                s += single_indent * indent + key + ": " + str(value) + "\n"

        return s

    def __str__(self):
        try:
            sorted_assets = sorted(self.data, key=lambda x: x.path)
        except:
            return super().__str__()

        ids = set([asset.id for asset in sorted_assets])

        s = "\n"
        root = sorted_assets[0].path[0]
        for asset in sorted_assets:
            this_root = asset.path[0]
            if this_root != root:
                s += "\n" + "*" * 80 + "\n\n"
                root = this_root
            elif len(asset.path) > 1 and asset.path[-2] not in ids:
                s += "\n" + "-" * 80 + "\n\n"
            s += self._indented_asset_str(asset)
        return s


# GenClass: AssetFilter.filter
class AssetFilter(CogniteFilter):
    """No description.

    Args:
        name (str): Name of asset. Often referred to as tag.
        parent_ids (List[int]): No description.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        source (str): The source of this asset
        created_time (Dict[str, Any]): Range between two timestamps
        last_updated_time (Dict[str, Any]): Range between two timestamps
        asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
        depth (Dict[str, Any]): Range between two integers
        external_id_prefix (str): External Id provided by client. Should be unique within the project.
    """

    def __init__(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        asset_subtrees: List[int] = None,
        depth: Dict[str, Any] = None,
        external_id_prefix: str = None,
        **kwargs
    ):
        self.name = name
        self.parent_ids = parent_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.asset_subtrees = asset_subtrees
        self.depth = depth
        self.external_id_prefix = external_id_prefix

    # GenStop


class AssetsAPI(APIClient):
    _RESOURCE_PATH = "/assets"

    def __call__(
        self,
        chunk_size: int = None,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        asset_subtrees: List[int] = None,
        depth: Dict[str, Any] = None,
        external_id_prefix: str = None,
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
            asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
            depth (Dict[str, Any]): Range between two integers
            external_id_prefix (str): External Id provided by client. Should be unique within the project

        Yields:
            Union[Asset, AssetList]: yields Asset one by one if chunk is not specified, else AssetList objects.
        """

        filter = AssetFilter(
            name,
            parent_ids,
            metadata,
            source,
            created_time,
            last_updated_time,
            asset_subtrees,
            depth,
            external_id_prefix,
        ).dump(camel_case=True)
        return self._list_generator(
            AssetList, resource_path=self._RESOURCE_PATH, method="POST", chunk_size=chunk_size, filter=filter
        )

    def __iter__(self) -> Generator[Asset, None, None]:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Yields:
            Asset: yields Assets one by one.
        """
        return self.__call__()

    def get(
        self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> Union[Asset, AssetList]:
        """Get assets

        Args:
            id (Union[int, List[int], optional): Id or list of ids
            external_id (Union[str, List[str]], optional): External ID or list of external ids

        Returns:
            Union[Asset, AssetList]: Requested asset(s)

        Examples:

            Get assets by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.get(id=1)

            Get assets by external id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.get(external_id=["1", "abc"])
        """
        return self._retrieve_multiple(AssetList, self._RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True)

    def list(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        asset_subtrees: List[int] = None,
        depth: Dict[str, Any] = None,
        external_id_prefix: str = None,
        limit: int = None,
    ) -> AssetList:
        """List assets

        Args:
            name (str): Name of asset. Often referred to as tag.
            parent_ids (List[int]): No description.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            source (str): The source of this asset
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
            depth (Dict[str, Any]): Range between two integers
            limit (int, optional): Maximum number of assets to return. If not specified, all assets will be returned.

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
            name,
            parent_ids,
            metadata,
            source,
            created_time,
            last_updated_time,
            asset_subtrees,
            depth,
            external_id_prefix,
        ).dump(camel_case=True)
        return self._list(AssetList, resource_path=self._RESOURCE_PATH, method="POST", limit=limit, filter=filter)

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
        utils.assert_type(asset, "asset", [Asset, list])
        if isinstance(asset, Asset) or len(asset) <= self._LIMIT:
            return self._create_multiple(AssetList, self._RESOURCE_PATH, asset)
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
        self._delete_multiple(resource_path=self._RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True)

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
                >>> asset = c.assets.get(id=1)
                >>> asset.description = "New description"
                >>> res = c.assets.update(asset)

            Perform a partial update on a asset, updating the description and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import AssetUpdate
                >>> c = CogniteClient()
                >>> my_update = AssetUpdate(id=1).description.set("New description").metadata.add({"key": "value"})
                >>> res = c.assets.update(my_update)
        """
        return self._update_multiple(cls=AssetList, resource_path=self._RESOURCE_PATH, items=item)

    def search(
        self, name: str = None, description: str = None, filter: AssetFilter = None, limit: int = None
    ) -> AssetList:
        """Search for assets

        Args:
            name (str): Fuzzy match on name.
            description (str): Fuzzy match on description.
            filter (AssetFilter): Filter to apply. Performs exact match on these fields.
            limit (int): Maximum number of results to return.

        Returns:
            AssetList: List of requested assets

        Examples:

            Search for assets::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.assets.search(name="some name")
        """
        filter = filter.dump(camel_case=True) if filter else None
        return self._search(
            cls=AssetList,
            resource_path=self._RESOURCE_PATH,
            json={"search": {"name": name, "description": description}, "filter": filter, "limit": limit},
        )


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

    @staticmethod
    def set_ref_id_on_assets_in_response(assets_in_request, assets_in_response):
        for i, asset in enumerate(assets_in_request):
            assets_in_response[i].ref_id = asset.ref_id
            assets_in_response[i].parent_ref_id = asset.parent_ref_id
        return assets_in_response

    def run(self):
        try:
            request = None
            while not self.stop:
                try:
                    request = self.request_queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                response = self.client.create(request)
                assets = self.set_ref_id_on_assets_in_response(request, response)
                self.response_queue.put(assets)
        except Exception as e:
            self.response_queue.put(_AssetsFailedToPost(e, request))


class _AssetPoster:
    def __init__(self, assets: List[Asset], client: AssetsAPI):
        self._validate_asset_hierarchy(assets)
        self.remaining_ref_ids = OrderedDict()
        self.remaining_ref_ids_set = set()
        self.ref_id_to_asset = {}

        for asset in assets:
            asset_copy = Asset(**asset.dump())
            ref_id = asset.ref_id
            if ref_id is None:
                ref_id = utils.random_string()
                asset_copy.ref_id = ref_id
            self.remaining_ref_ids[ref_id] = None
            self.remaining_ref_ids_set.add(ref_id)
            self.ref_id_to_asset[ref_id] = asset_copy

        self.client = client

        self.num_of_assets = len(self.remaining_ref_ids)
        self.ref_id_to_id = {}
        self.ref_ids_without_circular_deps = set()
        self.ref_id_to_children = {ref_id: set() for ref_id in self.remaining_ref_ids}
        self.ref_id_to_descendent_count = {ref_id: 0 for ref_id in self.remaining_ref_ids}
        self.posted_assets = AssetList([])
        self.may_have_been_posted_assets = AssetList([])
        self.not_posted_assets = AssetList([])
        self.assets_remaining = (
            lambda: len(self.posted_assets) + len(self.may_have_been_posted_assets) + len(self.not_posted_assets)
            < self.num_of_assets
        )

        self.request_queue = queue.Queue()
        self.response_queue = queue.Queue()

        self._initialize()

    @staticmethod
    def _validate_asset_hierarchy(assets) -> None:
        ref_ids = set([asset.ref_id for asset in assets])
        ref_ids_seen = set()
        for asset in assets:
            if asset.ref_id:
                if asset.ref_id in ref_ids_seen:
                    raise AssertionError("Duplicate ref_id '{}' found".format(asset.ref_id))
                ref_ids_seen.add(asset.ref_id)

            parent_ref = asset.parent_ref_id
            if parent_ref:
                if parent_ref not in ref_ids:
                    raise AssertionError("parent_ref_id '{}' does not point to any asset".format(parent_ref))
                if asset.parent_id is not None:
                    raise AssertionError(
                        "An asset has both parent_id '{}' and parent_ref_id '{}' set.".format(
                            asset.parent_id, asset.parent_ref_id
                        )
                    )

    def _initialize(self):
        root_assets = set()
        for ref_id in self.remaining_ref_ids:
            asset = self.ref_id_to_asset[ref_id]
            if asset.parent_ref_id is None or asset.parent_id is not None:
                root_assets.add(asset)
            elif asset.parent_ref_id in self.ref_id_to_children:
                self.ref_id_to_children[asset.parent_ref_id].add(asset)
            self._verify_asset_is_not_part_of_tree_with_circular_deps(asset)

        for root_asset in root_assets:
            self._initialize_asset_to_descendant_count(root_asset)

        self.remaining_ref_ids = self._sort_ref_ids_by_descendant_count(self.remaining_ref_ids)

    def _initialize_asset_to_descendant_count(self, asset):
        for child in self.ref_id_to_children[asset.ref_id]:
            self.ref_id_to_descendent_count[asset.ref_id] += 1 + self._initialize_asset_to_descendant_count(child)
        return self.ref_id_to_descendent_count[asset.ref_id]

    def _get_descendants(self, asset):
        descendants = []
        for child in self.ref_id_to_children[asset.ref_id]:
            descendants.append(child)
            descendants.extend(self._get_descendants(child))
        return descendants

    def _verify_asset_is_not_part_of_tree_with_circular_deps(self, asset: Asset):
        next_asset = asset
        seen = {asset.ref_id}
        while next_asset.parent_ref_id is not None:
            next_asset = self.ref_id_to_asset[next_asset.parent_ref_id]
            if next_asset.ref_id in self.ref_ids_without_circular_deps:
                break
            if next_asset.ref_id not in seen:
                seen.add(next_asset.ref_id)
            else:
                raise AssertionError("The asset hierarchy has circular dependencies")
        self.ref_ids_without_circular_deps.update(seen)

    def _sort_ref_ids_by_descendant_count(self, ref_ids: OrderedDict) -> OrderedDict:
        sorted_ref_ids = sorted(ref_ids, key=lambda x: self.ref_id_to_descendent_count[x], reverse=True)
        return OrderedDict({ref_id: None for ref_id in sorted_ref_ids})

    def _get_assets_unblocked_by_ref_id(self, asset: Asset, limit):
        pq = PriorityQueue()
        pq.add(asset, self.ref_id_to_descendent_count[asset.ref_id])
        unblocked_descendents = []
        while pq:
            if len(unblocked_descendents) == limit:
                break
            asset = pq.get()
            unblocked_descendents.append(asset)
            self.remaining_ref_ids_set.remove(asset.ref_id)
            for child in self.ref_id_to_children[asset.ref_id]:
                pq.add(child, self.ref_id_to_descendent_count[child.ref_id])
        return unblocked_descendents

    def _get_unblocked_assets(self) -> List[List[Asset]]:
        limit = self.client._LIMIT
        unblocked_assets_lists = []
        unblocked_assets = []
        for ref_id in self.remaining_ref_ids:
            asset = self.ref_id_to_asset[ref_id]
            if ref_id in self.remaining_ref_ids_set and (
                asset.parent_ref_id is None or asset.parent_id is not None or asset.parent_ref_id in self.ref_id_to_id
            ):
                if asset.parent_ref_id in self.ref_id_to_id:
                    asset.parent_id = self.ref_id_to_id[asset.parent_ref_id]
                    asset.parent_ref_id = None
                unblocked_assets.extend(self._get_assets_unblocked_by_ref_id(asset, limit - len(unblocked_assets)))
                if len(unblocked_assets) == limit:
                    unblocked_assets_lists.append(unblocked_assets)
                    unblocked_assets = []

        if len(unblocked_assets) > 0:
            unblocked_assets_lists.append(unblocked_assets)

        for unblocked_assets in unblocked_assets_lists:
            for unblocked_asset in unblocked_assets:
                del self.remaining_ref_ids[unblocked_asset.ref_id]

        return unblocked_assets_lists

    def _update_ref_id_to_id_map(self, assets):
        for asset in assets:
            if asset.ref_id is not None:
                self.ref_id_to_id[asset.ref_id] = asset.id

    def run(self):
        unblocked_assets_lists = self._get_unblocked_assets()
        for unblocked_assets in unblocked_assets_lists:
            self.request_queue.put(unblocked_assets)
        while self.assets_remaining():
            res = self.response_queue.get()
            if isinstance(res, _AssetsFailedToPost):
                if isinstance(res.exc, CogniteAPIError):
                    if res.exc.code >= 500:
                        self.may_have_been_posted_assets.extend(AssetList(res.assets))
                    elif res.exc.code >= 400:
                        self.not_posted_assets.extend(AssetList(res.assets))
                    for asset in res.assets:
                        self.not_posted_assets.extend(self._get_descendants(asset))
                else:
                    raise res.exc
            else:
                self._update_ref_id_to_id_map(res)
                self.posted_assets.extend(res)
                unblocked_assets_lists = self._get_unblocked_assets()
                for unblocked_assets in unblocked_assets_lists:
                    self.request_queue.put(unblocked_assets)
        if len(self.may_have_been_posted_assets) > 0 or len(self.not_posted_assets) > 0:
            raise CogniteAssetPostingError(
                posted=self.posted_assets,
                may_have_been_posted=self.may_have_been_posted_assets,
                not_posted=self.not_posted_assets,
            )

    def post(self):
        workers = []
        for _ in range(self.client._max_workers):
            worker = _AssetPosterWorker(self.client, self.request_queue, self.response_queue)
            workers.append(worker)
            worker.start()

        self.run()

        for worker in workers:
            worker.stop = True

        return AssetList(sorted(self.posted_assets, key=lambda x: x.path))
