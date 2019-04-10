import queue
import threading

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import *


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


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def external_id(self):
        return PrimitiveUpdate(self, "externalId")

    @property
    def name(self):
        return PrimitiveUpdate(self, "name")

    @property
    def description(self):
        return PrimitiveUpdate(self, "description")

    @property
    def metadata(self):
        return ObjectUpdate(self, "metadata")

    @property
    def source(self):
        return PrimitiveUpdate(self, "source")


class PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> AssetUpdate:
        return self._set(value)


class ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> AssetUpdate:
        return self._set(value)

    def add(self, value: Dict) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)


class ListUpdate(CogniteListUpdate):
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
            chunk_size (int, optional): Number of assets to return in each chunk. Defaults to yielding one event a time.
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

                >>> from cognite.client import CogniteClient, Asset
                >>> c = CogniteClient()
                >>> assets = [Asset(name="asset1"), Asset(name="asset2")]
                >>> res = c.assets.create(assets)
        """
        utils.assert_type(asset, "asset", [Asset, list])
        if isinstance(asset, Asset) or len(asset) <= self._LIMIT:
            return self._create_multiple(AssetList, self._RESOURCE_PATH, asset)
        return AssetPoster(asset, client=self).post()

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

                >>> from cognite.client import CogniteClient, AssetUpdate
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


class AssetPosterWorker(threading.Thread):
    def __init__(
        self, client: AssetsAPI, request_queue: queue.Queue, response_queue: queue.Queue, assets_remaining: Callable
    ):
        self.client = client
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.assets_remaining = assets_remaining
        super().__init__(daemon=True)

    @staticmethod
    def set_ref_id_on_assets_in_response(assets_in_request, assets_in_response):
        for i, asset in enumerate(assets_in_request):
            assets_in_response[i].ref_id = asset.ref_id
            assets_in_response[i].parent_ref_id = asset.parent_ref_id
        return assets_in_response

    def run(self):
        while self.assets_remaining():
            request = self.request_queue.get()
            response = self.client.create(request)
            assets = self.set_ref_id_on_assets_in_response(request, response)
            self.response_queue.put(assets)


class AssetPoster:
    def __init__(self, assets: List[Asset], client: AssetsAPI):
        self.validate_asset_hierarchy(assets)

        self.remaining_assets = [Asset._load(a.dump(camel_case=True)) for a in assets]
        self.client = client

        self.number_of_assets = len(assets)

        self.ref_id_to_remaining_children = self.initialize_ref_id_to_remaining_children_map(self.remaining_assets)
        self.ref_id_to_id = {}

        self.request_queue = queue.Queue()
        self.response_queue = queue.Queue()

        self.created_assets = AssetList([])

    @staticmethod
    def validate_asset_hierarchy(assets: List[Asset]) -> None:
        ref_ids = [asset.ref_id for asset in assets if asset.ref_id is not None]
        ref_ids_set = set(ref_ids)
        assert sorted(ref_ids) == sorted(list(ref_ids_set)), "Duplicate ref_ids found"
        for asset in assets:
            parent_ref = asset.parent_ref_id
            if parent_ref:
                assert parent_ref in ref_ids_set, "parent_ref_id '{}' does not point to anything".format(parent_ref)
                assert asset.parent_id is None, "An asset has both parent_id and parent_ref_id set."

    @staticmethod
    def initialize_ref_id_to_remaining_children_map(assets: List[Asset]) -> Dict[str, List[Asset]]:
        ref_id_to_children = {asset.ref_id: [] for asset in assets if asset.ref_id is not None}
        for asset in assets:
            if asset.parent_ref_id in ref_id_to_children:
                ref_id_to_children[asset.parent_ref_id].append(asset)
        return ref_id_to_children

    def get_descendants_unblocked_by_ref_id(self, assets: List[Asset], limit):
        unblocked_children = []
        for asset in assets:
            if asset.ref_id in self.ref_id_to_remaining_children:
                asset_children = self.ref_id_to_remaining_children[asset.ref_id].copy()
                for child_asset in asset_children:
                    unblocked_children.append(child_asset)
                    self.ref_id_to_remaining_children[asset.ref_id].remove(child_asset)
                    if len(unblocked_children) == limit:
                        return unblocked_children

                    max_descendents = limit - len(unblocked_children)
                    unblocked_children.extend(self.get_descendants_unblocked_by_ref_id([child_asset], max_descendents))
                    if len(unblocked_children) == limit:
                        return unblocked_children
        return unblocked_children

    def get_unblocked_assets(self, limit) -> List[Asset]:
        unblocked_assets = []

        for asset in self.remaining_assets:
            if asset.parent_ref_id is None or asset.parent_id is not None:
                unblocked_assets.append(asset)
            elif self.ref_id_to_id.get(asset.parent_ref_id) is not None:
                asset.parent_id = self.ref_id_to_id[asset.parent_ref_id]
                unblocked_assets.append(asset)
            if len(unblocked_assets) == limit:
                break

        if len(unblocked_assets) < limit:
            max_descendents = limit - len(unblocked_assets)
            unblocked_assets.extend(self.get_descendants_unblocked_by_ref_id(unblocked_assets, max_descendents))

        for unblocked_asset in unblocked_assets:
            self.remaining_assets.remove(unblocked_asset)

        return unblocked_assets

    def update_ref_id_to_id_map(self, assets):
        for asset in assets:
            if asset.ref_id is not None:
                self.ref_id_to_id[asset.ref_id] = asset.id

    def assets_remaining(self):
        return len(self.created_assets) < self.number_of_assets

    def run(self):
        unblocked_assets = self.get_unblocked_assets(self.client._LIMIT)
        self.request_queue.put(unblocked_assets)
        while self.assets_remaining():
            posted_assets = self.response_queue.get()
            self.update_ref_id_to_id_map(posted_assets)
            self.created_assets.extend(posted_assets)
            unblocked_assets = self.get_unblocked_assets(self.client._LIMIT)
            self.request_queue.put(unblocked_assets)

    def post(self):
        workers = []
        for _ in range(self.client._max_workers):
            worker = AssetPosterWorker(self.client, self.request_queue, self.response_queue, self.assets_remaining)
            workers.append(worker)
            worker.start()

        self.run()

        for worker in workers:
            worker.join()

        for asset in self.created_assets:
            print(asset.path)
        return sorted(self.created_assets, key=lambda x: x.path)
