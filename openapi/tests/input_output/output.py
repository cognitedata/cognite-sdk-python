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


class CogniteLabelUpdate:
    pass


class CognitePropertyClassUtil:
    @staticmethod
    def declare_property(tmp):
        return None


class TimestampRange:
    pass


class LabelFilter:
    pass


# GenPropertyClass: AggregateResultItem
class AggregateResultItem(dict):
    """Aggregated metrics of the asset

    Args:
        child_count (int): Number of direct descendants for the asset
        depth (int): Asset path depth (number of levels below root node).
        path (List[Dict[str, Any]]): IDs of assets on the path to the asset.
    """

    def __init__(self, child_count: int = None, depth: int = None, path: List[Dict[str, Any]] = None, **kwargs):
        self.child_count = child_count
        self.depth = depth
        self.path = path
        self.update(kwargs)

    child_count = CognitePropertyClassUtil.declare_property("childCount")
    depth = CognitePropertyClassUtil.declare_property("depth")
    path = CognitePropertyClassUtil.declare_property("path")

    # GenStop


# GenUpdateClass: AssetChange
class AssetUpdate(CogniteUpdate):
    """Changes applied to asset

    Args:
        id (int): A server-generated ID for the object.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
    """

    class _PrimitiveAssetUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> "AssetUpdate":
            return self._set(value)

    class _ObjectAssetUpdate(CogniteObjectUpdate):
        def set(self, value: Dict) -> "AssetUpdate":
            return self._set(value)

        def add(self, value: Dict) -> "AssetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "AssetUpdate":
            return self._remove(value)

    class _ListAssetUpdate(CogniteListUpdate):
        def set(self, value: List) -> "AssetUpdate":
            return self._set(value)

        def add(self, value: List) -> "AssetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "AssetUpdate":
            return self._remove(value)

    class _LabelAssetUpdate(CogniteLabelUpdate):
        def add(self, value: List) -> "AssetUpdate":
            return self._add(value)

        def remove(self, value: List) -> "AssetUpdate":
            return self._remove(value)

    @property
    def external_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "externalId")

    @property
    def name(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "name")

    @property
    def description(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "description")

    @property
    def data_set_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "dataSetId")

    @property
    def metadata(self):
        return AssetUpdate._ObjectAssetUpdate(self, "metadata")

    @property
    def source(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "source")

    @property
    def parent_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "parentId")

    @property
    def parent_external_id(self):
        return AssetUpdate._PrimitiveAssetUpdate(self, "parentExternalId")

    @property
    def labels(self):
        return AssetUpdate._LabelAssetUpdate(self, "labels")

    # GenStop
