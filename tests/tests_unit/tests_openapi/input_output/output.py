from typing import *


class CogniteResource:
    pass


class CogniteUpdate:
    pass


class CogniteFilter:
    pass


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
        created_time (int): It is the number of seconds that have elapsed since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time (UTC), minus leap seconds.
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
        created_time: int = None,
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
        self.created_time = created_time
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

    def created_time_set(self, value: int):
        if value is None:
            self._update_object["createdTime"] = {"setNull": True}
            return self
        self._update_object["createdTime"] = {"set": value}
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
