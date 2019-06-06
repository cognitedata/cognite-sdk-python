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


# GenClass: Asset
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
        created_time (int): It is the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): It is the number of milliseconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        path (List[int]): IDs of assets on the path to the asset.
        depth (int): Asset path depth (number of levels below root node).
        cognite_client (CogniteClient): The client to associate with this object.
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
        created_time: int = None,
        last_updated_time: int = None,
        path: List[int] = None,
        depth: int = None,
        cognite_client=None,
    ):
        self.external_id = external_id
        self.name = name
        self.parent_id = parent_id
        self.description = description
        self.metadata = metadata
        self.source = source
        self.id = id
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.path = path
        self.depth = depth
        self._cognite_client = cognite_client

    # GenStop
    def to_pandas(self):
        pass


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes applied to asset

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
        root (bool): filtered assets are root assets or not
        external_id_prefix (str): External Id provided by client. Should be unique within the project.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        metadata: Dict[str, Any] = None,
        source: str = None,
        created_time: Dict[str, Any] = None,
        last_updated_time: Dict[str, Any] = None,
        root: bool = None,
        external_id_prefix: str = None,
        cognite_client=None,
    ):
        self.name = name
        self.parent_ids = parent_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root = root
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    # GenStop
