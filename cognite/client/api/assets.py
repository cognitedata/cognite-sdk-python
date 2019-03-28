# -*- coding: utf-8 -*-
from typing import *

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.resource_base import CogniteFilter, CogniteResource, CogniteResourceList, CogniteUpdate


# GenClass: Asset, AssetReferences
class Asset(CogniteResource):
    """Representation of a physical asset, e.g plant or piece of equipment

    Args:
        external_id (str): External Id provided by client. Should be unique within the project
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
        parent_ref_id (str): Reference ID of parent, to disambiguate if multiple nodes have the same name
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


class AssetList(CogniteResourceList):
    _RESOURCE = Asset


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project
    """

    def __init__(self, id: int = None, external_id: str = None):
        self.id = id
        self.external_id = external_id
        self._update_object = {}

    def external_id_set(self, value: str):
        if value is None:
            self._update_object["externalId"] = {"setNull": True}
            return self
        self._update_object["externalId"] = {"set": value}
        return self

    def name_set(self, value: str):
        if value is None:
            self._update_object["name"] = {"setNull": True}
            return self
        self._update_object["name"] = {"set": value}
        return self

    def description_set(self, value: str):
        if value is None:
            self._update_object["description"] = {"setNull": True}
            return self
        self._update_object["description"] = {"set": value}
        return self

    def metadata_set(self, value: Dict[str, Any]):
        if value is None:
            self._update_object["metadata"] = {"setNull": True}
            return self
        self._update_object["metadata"] = {"set": value}
        return self

    def source_set(self, value: str):
        if value is None:
            self._update_object["source"] = {"setNull": True}
            return self
        self._update_object["source"] = {"set": value}
        return self

    # GenStop


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
        external_id_prefix (str): External Id provided by client. Should be unique within the project
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
    RESOURCE_PATH = "/assets"

    def __call__(self, filter: AssetFilter = None, chunk_size: int = None) -> Generator:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Args:
            filter (AssetFilter, optional): Filter to apply.
            chunk_size (int, optional): Number of assets to return in each chunk. Defaults to yielding one event a time.

        Yields:
            Union[Asset, AssetList]: yields Asset one by one if chunk is not specified, else AssetList objects.
        """
        params = filter.dump(camel_case=True) if filter else None
        return self._list_generator(AssetList, resource_path=self.RESOURCE_PATH, chunk=chunk_size, params=params)

    def __iter__(self) -> Generator:
        """Iterate over assets

        Fetches assets as they are iterated over, so you keep a limited number of assets in memory.

        Yields:
            Asset: yields Assets one by one.
        """
        return self.__call__()

    def get(
        self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None
    ) -> Union[Asset, AssetList]:
        """Get assets by id

        Args:
            id (Union[int, List[int], optional): Id or list of ids
            external_id (Union[str, List[str]], optional): External ID or list of external ids

        Returns:
            Union[Asset, AssetList]: Requested asset(s)
        """
        return self._retrieve_multiple(AssetList, self.RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True)

    def list(self, filter: AssetFilter = None, limit: int = None) -> AssetList:
        """List assets

        Args:
            filter (AssetFilter, optional): Filter to apply.
            limit (int, optional): Maximum number of assets to return. If not specified, all assets will be returned.

        Returns:
            AssetList: List of requested assets
        """
        params = filter.dump(camel_case=True) if filter else None
        return self._list(AssetList, resource_path=self.RESOURCE_PATH, limit=limit, params=params)

    def create(self, asset: Union[Asset, List[Asset]]) -> Union[Asset, AssetList]:
        """Create one or more assets.

        Args:
            asset (Union[Asset, List[Asset]]): Asset or list of assets to create.

        Returns:
            Union[Asset, AssetList]: Created asset(s)
        """
        return self._create_multiple(AssetList, resource_path=self.RESOURCE_PATH, items=asset)

    def delete(self, id: Union[int, List[int]] = None, external_id: Union[str, List[str]] = None) -> None:
        """Delete one or more assets

        Args:
            id (Union[int, List[int]): Id or list of ids
            external_id (Union[str, List[str]]): External ID or list of exgernal ids

        Returns:
            None
        """
        self._delete_multiple(resource_path=self.RESOURCE_PATH, ids=id, external_ids=external_id, wrap_ids=True)

    def update(self, item: Union[Asset, AssetUpdate, List[Union[Asset, AssetUpdate]]]) -> Union[Asset, AssetList]:
        """Update one or more assets

        Args:
            item (Union[Asset, AssetUpdate, List[Union[Asset, AssetUpdate]]]): Asset(s) to update

        Returns:
            Union[Asset, AssetList]: Updated asset(s)
        """
        return self._update_multiple(cls=AssetList, resource_path=self.RESOURCE_PATH, items=item)

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
        """
        filter = filter.dump(camel_case=True) if filter else None
        return self._search(
            cls=AssetList,
            resource_path=self.RESOURCE_PATH,
            json={"search": {"name": name, "description": description}, "filter": filter, "limit": limit},
        )
