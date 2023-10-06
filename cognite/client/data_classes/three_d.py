from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from cognite.client.data_classes._base import (
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CognitePropertyClassUtil,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    PropertySpec,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class RevisionCameraProperties(dict):
    """Initial camera position and target.

    Args:
        target (list[float] | None): Initial camera target.
        position (list[float] | None): Initial camera position.
        **kwargs (Any): No description.
    """

    def __init__(self, target: list[float] | None = None, position: list[float] | None = None, **kwargs: Any) -> None:
        self.target = target
        self.position = position
        self.update(kwargs)

    target = CognitePropertyClassUtil.declare_property("target")
    position = CognitePropertyClassUtil.declare_property("position")


class BoundingBox3D(dict):
    """The bounding box of the subtree with this sector as the root sector. Is null if there are no geometries in the subtree.

    Args:
        max (list[float] | None): No description.
        min (list[float] | None): No description.
        **kwargs (Any): No description.
    """

    def __init__(self, max: list[float] | None = None, min: list[float] | None = None, **kwargs: Any) -> None:
        self.max = max
        self.min = min
        self.update(kwargs)

    max = CognitePropertyClassUtil.declare_property("max")
    min = CognitePropertyClassUtil.declare_property("min")


class ThreeDModel(CogniteResource):
    """No description.

    Args:
        name (str | None): The name of the model.
        id (int | None): The ID of the model.
        created_time (int | None): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
        data_set_id (int | None): The id of the dataset this 3D model belongs to.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        name: str | None = None,
        id: int | None = None,
        created_time: int | None = None,
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.name = name
        self.id = id
        self.created_time = created_time
        self.data_set_id = data_set_id
        self.metadata = metadata
        self._cognite_client = cast("CogniteClient", cognite_client)


class ThreeDModelUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): A server-generated ID for the object.
    """

    class _PrimitiveThreeDModelUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> ThreeDModelUpdate:
            return self._set(value)

    class _ObjectThreeDModelUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> ThreeDModelUpdate:
            return self._set(value)

        def add(self, value: dict) -> ThreeDModelUpdate:
            return self._add(value)

        def remove(self, value: list) -> ThreeDModelUpdate:
            return self._remove(value)

    class _ListThreeDModelUpdate(CogniteListUpdate):
        def set(self, value: list) -> ThreeDModelUpdate:
            return self._set(value)

        def add(self, value: list) -> ThreeDModelUpdate:
            return self._add(value)

        def remove(self, value: list) -> ThreeDModelUpdate:
            return self._remove(value)

    class _LabelThreeDModelUpdate(CogniteLabelUpdate):
        def add(self, value: list) -> ThreeDModelUpdate:
            return self._add(value)

        def remove(self, value: list) -> ThreeDModelUpdate:
            return self._remove(value)

    @property
    def name(self) -> _PrimitiveThreeDModelUpdate:
        return ThreeDModelUpdate._PrimitiveThreeDModelUpdate(self, "name")

    @property
    def metadata(self) -> _ObjectThreeDModelUpdate:
        return ThreeDModelUpdate._ObjectThreeDModelUpdate(self, "metadata")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("metadata", is_container=True),
        ]


class ThreeDModelList(CogniteResourceList[ThreeDModel]):
    _RESOURCE = ThreeDModel


class ThreeDModelRevision(CogniteResource):
    """No description.

    Args:
        id (int | None): The ID of the revision.
        file_id (int | None): The file id.
        published (bool | None): True if the revision is marked as published.
        rotation (list[float] | None): No description.
        scale (list[float] | None): Scale of 3D model in directions X,Y and Z. Should be uniform.
        translation (list[float] | None): 3D offset of the model.
        camera (dict[str, Any] | RevisionCameraProperties | None): Initial camera position and target.
        status (str | None): The status of the revision.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        thumbnail_threed_file_id (int | None): The threed file ID of a thumbnail for the revision. Use /3d/files/{id} to retrieve the file.
        thumbnail_url (str | None): The URL of a thumbnail for the revision.
        asset_mapping_count (int | None): The number of asset mappings for this revision.
        created_time (int | None): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        file_id: int | None = None,
        published: bool | None = None,
        rotation: list[float] | None = None,
        scale: list[float] | None = None,
        translation: list[float] | None = None,
        camera: dict[str, Any] | RevisionCameraProperties | None = None,
        status: str | None = None,
        metadata: dict[str, str] | None = None,
        thumbnail_threed_file_id: int | None = None,
        thumbnail_url: str | None = None,
        asset_mapping_count: int | None = None,
        created_time: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.id = id
        self.file_id = file_id
        self.published = published
        self.rotation = rotation
        self.scale = scale
        self.translation = translation
        self.camera = camera
        self.status = status
        self.metadata = metadata
        self.thumbnail_threed_file_id = thumbnail_threed_file_id
        self.thumbnail_url = thumbnail_url
        self.asset_mapping_count = asset_mapping_count
        self.created_time = created_time
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> ThreeDModelRevision:
        instance = super()._load(resource, cognite_client)
        if isinstance(resource, dict) and instance.camera is not None:
            instance.camera = RevisionCameraProperties(**instance.camera)
        return instance


class ThreeDModelRevisionUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): A server-generated ID for the object.
    """

    class _PrimitiveThreeDModelRevisionUpdate(CognitePrimitiveUpdate):
        def set(self, value: Any) -> ThreeDModelRevisionUpdate:
            return self._set(value)

    class _ObjectThreeDModelRevisionUpdate(CogniteObjectUpdate):
        def set(self, value: dict) -> ThreeDModelRevisionUpdate:
            return self._set(value)

        def add(self, value: dict) -> ThreeDModelRevisionUpdate:
            return self._add(value)

        def remove(self, value: list) -> ThreeDModelRevisionUpdate:
            return self._remove(value)

    class _ListThreeDModelRevisionUpdate(CogniteListUpdate):
        def set(self, value: list) -> ThreeDModelRevisionUpdate:
            return self._set(value)

        def add(self, value: list) -> ThreeDModelRevisionUpdate:
            return self._add(value)

        def remove(self, value: list) -> ThreeDModelRevisionUpdate:
            return self._remove(value)

    class _LabelThreeDModelRevisionUpdate(CogniteLabelUpdate):
        def add(self, value: list) -> ThreeDModelRevisionUpdate:
            return self._add(value)

        def remove(self, value: list) -> ThreeDModelRevisionUpdate:
            return self._remove(value)

    @property
    def published(self) -> _PrimitiveThreeDModelRevisionUpdate:
        return ThreeDModelRevisionUpdate._PrimitiveThreeDModelRevisionUpdate(self, "published")

    @property
    def rotation(self) -> _ListThreeDModelRevisionUpdate:
        return ThreeDModelRevisionUpdate._ListThreeDModelRevisionUpdate(self, "rotation")

    @property
    def scale(self) -> _ListThreeDModelRevisionUpdate:
        return ThreeDModelRevisionUpdate._ListThreeDModelRevisionUpdate(self, "scale")

    @property
    def translation(self) -> _ListThreeDModelRevisionUpdate:
        return ThreeDModelRevisionUpdate._ListThreeDModelRevisionUpdate(self, "translation")

    @property
    def camera(self) -> _ObjectThreeDModelRevisionUpdate:
        return ThreeDModelRevisionUpdate._ObjectThreeDModelRevisionUpdate(self, "camera")

    @property
    def metadata(self) -> _ObjectThreeDModelRevisionUpdate:
        return ThreeDModelRevisionUpdate._ObjectThreeDModelRevisionUpdate(self, "metadata")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            PropertySpec("published", is_nullable=False),
            PropertySpec("rotation", is_nullable=False),
            PropertySpec("camera", is_nullable=False),
            PropertySpec("scale", is_nullable=False),
            PropertySpec("translation", is_nullable=False),
            PropertySpec("metadata", is_container=True),
        ]


class ThreeDModelRevisionList(CogniteResourceList[ThreeDModelRevision]):
    _RESOURCE = ThreeDModelRevision


class ThreeDNode(CogniteResource):
    """No description.

    Args:
        id (int | None): The ID of the node.
        tree_index (int | None): The index of the node in the 3D model hierarchy, starting from 0. The tree is traversed in a depth-first order.
        parent_id (int | None): The parent of the node, null if it is the root node.
        depth (int | None): The depth of the node in the tree, starting from 0 at the root node.
        name (str | None): The name of the node.
        subtree_size (int | None): The number of descendants of the node, plus one (counting itself).
        properties (dict[str, dict[str, str]] | None): Properties extracted from 3D model, with property categories containing key/value string pairs.
        bounding_box (dict[str, Any] | BoundingBox3D | None): The bounding box of the subtree with this sector as the root sector. Is null if there are no geometries in the subtree.
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        id: int | None = None,
        tree_index: int | None = None,
        parent_id: int | None = None,
        depth: int | None = None,
        name: str | None = None,
        subtree_size: int | None = None,
        properties: dict[str, dict[str, str]] | None = None,
        bounding_box: dict[str, Any] | BoundingBox3D | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.id = id
        self.tree_index = tree_index
        self.parent_id = parent_id
        self.depth = depth
        self.name = name
        self.subtree_size = subtree_size
        self.properties = properties
        self.bounding_box = bounding_box
        self._cognite_client = cast("CogniteClient", cognite_client)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> ThreeDNode:
        instance = super()._load(resource, cognite_client)
        if isinstance(resource, dict) and instance.bounding_box is not None:
            instance.bounding_box = BoundingBox3D(**instance.bounding_box)
        return instance


class ThreeDNodeList(CogniteResourceList[ThreeDNode]):
    _RESOURCE = ThreeDNode


class ThreeDAssetMapping(CogniteResource):
    """No description.

    Args:
        node_id (int | None): The ID of the node.
        asset_id (int | None): The ID of the associated asset (Cognite's Assets API).
        tree_index (int | None): A number describing the position of this node in the 3D hierarchy, starting from 0. The tree is traversed in a depth-first order.
        subtree_size (int | None): The number of nodes in the subtree of this node (this number included the node itself).
        cognite_client (CogniteClient | None): The client to associate with this object.
    """

    def __init__(
        self,
        node_id: int | None = None,
        asset_id: int | None = None,
        tree_index: int | None = None,
        subtree_size: int | None = None,
        cognite_client: CogniteClient | None = None,
    ) -> None:
        self.node_id = node_id
        self.asset_id = asset_id
        self.tree_index = tree_index
        self.subtree_size = subtree_size
        self._cognite_client = cast("CogniteClient", cognite_client)


class ThreeDAssetMappingList(CogniteResourceList[ThreeDAssetMapping]):
    _RESOURCE = ThreeDAssetMapping
