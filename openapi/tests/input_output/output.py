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


class AggregateResultItem:
    pass


class EpochTimestampRange:
    pass


# GenClass: Asset
class Asset(CogniteResource):
    """A representation of a physical asset, for example a factory or a piece of equipment.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        name (str): The name of the asset.
        parent_id (int): A server-generated ID for the object.
        description (str): The description of the asset.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        source (str): The source of the asset.
        id (int): A server-generated ID for the object.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
        root_id (int): A server-generated ID for the object.
        aggregates (AggregateResultItem): Aggregated metrics of the asset
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        external_id: str = None,
        name: str = None,
        parent_id: int = None,
        description: str = None,
        metadata: Dict[str, str] = None,
        source: str = None,
        id: int = None,
        created_time: int = None,
        last_updated_time: int = None,
        root_id: int = None,
        aggregates: AggregateResultItem = None,
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
        self.root_id = root_id
        self.aggregates = aggregates
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(Asset, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if "aggregates" in resource:
                setattr(instance, "aggregates", AggregateResultItem(**resource["aggregates"]))
        return instance

    # GenStop
    def to_pandas(self):
        pass


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes applied to asset

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
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

    @property
    def parent_id(self):
        return _PrimitiveAssetUpdate(self, "parentId")

    @property
    def parent_external_id(self):
        return _PrimitiveAssetUpdate(self, "parentExternalId")


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
    """Filter on assets with strict matching.

    Args:
        name (str): The name of the asset.
        parent_ids (List[int]): Return only the direct descendants of the specified assets.
        root_ids (List[Union[Dict[str, Any], Dict[str, Any]]]): Return all descendants of the specified root assets.
        metadata (Dict[str, str]): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        source (str): The source of the asset.
        created_time (EpochTimestampRange): Range between two timestamps.
        last_updated_time (EpochTimestampRange): Range between two timestamps.
        root (bool): Whether the filtered assets are root assets, or not. Set to True to only list root assets.
        external_id_prefix (str): Filter by this (case-sensitive) prefix for the external ID.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        parent_ids: List[int] = None,
        root_ids: List[Union[Dict[str, Any], Dict[str, Any]]] = None,
        metadata: Dict[str, str] = None,
        source: str = None,
        created_time: EpochTimestampRange = None,
        last_updated_time: EpochTimestampRange = None,
        root: bool = None,
        external_id_prefix: str = None,
        cognite_client=None,
    ):
        self.name = name
        self.parent_ids = parent_ids
        self.root_ids = root_ids
        self.metadata = metadata
        self.source = source
        self.created_time = created_time
        self.last_updated_time = last_updated_time
        self.root = root
        self.external_id_prefix = external_id_prefix
        self._cognite_client = cognite_client

    @classmethod
    def _load(cls, resource: Union[Dict, str], cognite_client=None):
        instance = super(AssetFilter, cls)._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if "created_time" in resource:
                setattr(instance, "created_time", EpochTimestampRange(**resource["created_time"]))
            if "last_updated_time" in resource:
                setattr(instance, "last_updated_time", EpochTimestampRange(**resource["last_updated_time"]))
        return instance

    # GenStop
