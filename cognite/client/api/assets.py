from typing import *

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
            external_id_prefix (str): External Id provided by client. Should be unique within the project

        Yields:
            Union[Asset, AssetList]: yields Asset one by one if chunk is not specified, else AssetList objects.
        """

        filter = AssetFilter(
            name, parent_ids, metadata, source, created_time, last_updated_time, asset_subtrees, external_id_prefix
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
        external_id_prefix: str = None,
        limit: int = None,
    ) -> AssetList:
        """List assets

        Args:
            chunk_size (int, optional): Number of assets to return in each chunk. Defaults to yielding one event a time.
            name (str): Name of asset. Often referred to as tag.
            parent_ids (List[int]): No description.
            metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
            source (str): The source of this asset
            created_time (Dict[str, Any]): Range between two timestamps
            last_updated_time (Dict[str, Any]): Range between two timestamps
            asset_subtrees (List[int]): Filter out events that are not linked to assets in the subtree rooted at these assets.
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
            name, parent_ids, metadata, source, created_time, last_updated_time, asset_subtrees, external_id_prefix
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
        return self._create_multiple(AssetList, resource_path=self._RESOURCE_PATH, items=asset)

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
