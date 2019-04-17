from typing import *


class CogniteResource:
    pass


class CogniteUpdate:
    pass


class CogniteFilter:
    pass


class CognitePrimitiveUpdate:
    pass


class CogniteObjectUpdate:
    pass


class CogniteListUpdate:
    pass


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
    def to_pandas(self):
        pass


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes will be applied to event.

    Args:
        id (int): Javascript friendly internal ID given to the object.
        external_id (str): External Id provided by client. Should be unique within the project.
    """

    @property
    def external_id(self):
        return _PrimitiveUpdate(self, "externalId")

    @property
    def name(self):
        return _PrimitiveUpdate(self, "name")

    @property
    def description(self):
        return _PrimitiveUpdate(self, "description")

    @property
    def metadata(self):
        return _ObjectUpdate(self, "metadata")

    @property
    def source(self):
        return _PrimitiveUpdate(self, "source")


class _PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> AssetUpdate:
        return self._set(value)


class _ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> AssetUpdate:
        return self._set(value)

    def add(self, value: Dict) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)


class _ListUpdate(CogniteListUpdate):
    def set(self, value: List) -> AssetUpdate:
        return self._set(value)

    def add(self, value: List) -> AssetUpdate:
        return self._add(value)

    def remove(self, value: List) -> AssetUpdate:
        return self._remove(value)

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
