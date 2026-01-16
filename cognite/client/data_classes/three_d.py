from __future__ import annotations

from abc import ABC
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteLabelUpdate,
    CogniteListUpdate,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteResourceList,
    CogniteUpdate,
    InternalIdTransformerMixin,
    NameTransformerMixin,
    PropertySpec,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)


class RevisionCameraProperties(CogniteResource):
    """Initial camera position and target.

    Args:
        target (list[float]): Initial camera target.
        position (list[float]): Initial camera position.
    """

    def __init__(self, target: list[float], position: list[float]) -> None:
        self.target = target
        self.position = position

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            target=resource["target"],
            position=resource["position"],
        )


class BoundingBox3D(CogniteResource):
    """The bounding box of the subtree with this sector as the root sector. Is null if there are no geometries in the subtree.

    Args:
        max (list[float]): No description.
        min (list[float]): No description.
    """

    def __init__(self, max: list[float], min: list[float]) -> None:
        self.max = max
        self.min = min

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            max=resource["max"],
            min=resource["min"],
        )


class ThreeDModelCore(WriteableCogniteResource["ThreeDModelWrite"], ABC):
    """This class represents a 3D model in Cognite Data Fusion.


    Args:
        name (str): The name of the model.
        data_set_id (int | None): The id of the dataset this 3D model belongs to.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        name: str,
        data_set_id: int | None,
        metadata: dict[str, str] | None,
    ) -> None:
        self.name = name
        self.data_set_id = data_set_id
        self.metadata = metadata


class ThreeDModel(ThreeDModelCore):
    """This class represents a 3D model in Cognite Data Fusion.
    This is the read version of ThreeDModel, which is used when retrieving 3D models.

    Args:
        name (str): The name of the model.
        id (int): The ID of the model.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
        data_set_id (int | None): The id of the dataset this 3D model belongs to.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        name: str,
        id: int,
        created_time: int,
        data_set_id: int | None,
        metadata: dict[str, str] | None,
    ) -> None:
        super().__init__(
            name=name,
            data_set_id=data_set_id,
            metadata=metadata,
        )
        self.id = id
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            name=resource["name"],
            id=resource["id"],
            created_time=resource["createdTime"],
            data_set_id=resource.get("dataSetId"),
            metadata=resource.get("metadata"),
        )

    def as_write(self) -> ThreeDModelWrite:
        """Returns this ThreedModel in a write version."""
        if self.name is None:
            raise ValueError("ThreeDModel must have a name to be writable")
        return ThreeDModelWrite(
            name=self.name,
            data_set_id=self.data_set_id,
            metadata=self.metadata,
        )


class ThreeDModelWrite(ThreeDModelCore):
    """This class represents a 3D model in Cognite Data Fusion.
    This is the write version of ThreeDModel, which is used when creating 3D models.


    Args:
        name (str): The name of the model.
        data_set_id (int | None): The id of the dataset this 3D model belongs to.
        metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        name: str,
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            data_set_id=data_set_id,
            metadata=metadata,
        )

    @classmethod
    def _load(cls, resource: dict) -> ThreeDModelWrite:
        return cls(
            name=resource["name"],
            data_set_id=resource.get("dataSetId"),
            metadata=resource.get("metadata"),
        )

    def as_write(self) -> ThreeDModelWrite:
        """Returns this ThreedModelWrite instance."""
        return self


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

    @property
    def data_set_id(self) -> _PrimitiveThreeDModelUpdate:
        return ThreeDModelUpdate._PrimitiveThreeDModelUpdate(self, "dataSetId")

    @classmethod
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("name", is_nullable=False),
            PropertySpec("metadata", is_object=True),
            PropertySpec("data_set_id", is_nullable=True),
        ]


class ThreeDModelWriteList(CogniteResourceList[ThreeDModelWrite], NameTransformerMixin):
    _RESOURCE = ThreeDModelWrite


class ThreeDModelList(
    WriteableCogniteResourceList[ThreeDModelWrite, ThreeDModel], NameTransformerMixin, InternalIdTransformerMixin
):
    _RESOURCE = ThreeDModel

    def as_write(self) -> ThreeDModelWriteList:
        """Returns this ThreedModelList in a write version."""
        return ThreeDModelWriteList([item.as_write() for item in self.data])


class ThreeDModelRevisionCore(WriteableCogniteResource["ThreeDModelRevisionWrite"], ABC):
    """No description.

    Args:
        file_id (int | None): The file id.
        published (bool | None): True if the revision is marked as published.
        rotation (list[float] | None): No description.
        scale (list[float] | None): Scale of 3D model in directions X,Y and Z. Should be uniform.
        translation (list[float] | None): 3D offset of the model.
        camera (RevisionCameraProperties | dict[str, Any] | None): Initial camera position and target.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        file_id: int | None = None,
        published: bool | None = None,
        rotation: list[float] | None = None,
        scale: list[float] | None = None,
        translation: list[float] | None = None,
        camera: RevisionCameraProperties | dict[str, Any] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        self.file_id = file_id
        self.published = published
        self.rotation = rotation
        self.scale = scale
        self.translation = translation
        self.camera = camera
        self.metadata = metadata

    @classmethod
    def _load(cls, resource: dict) -> Self:
        return cls(
            file_id=resource.get("fileId"),
            published=resource.get("published"),
            rotation=resource.get("rotation"),
            scale=resource.get("scale"),
            translation=resource.get("translation"),
            camera=RevisionCameraProperties._load_if(resource.get("camera")),
            metadata=resource.get("metadata"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if isinstance(self.camera, RevisionCameraProperties):
            result["camera"] = self.camera.dump(camel_case=camel_case)
        return result


class ThreeDModelRevision(ThreeDModelRevisionCore):
    """This class represents a 3D model revision in Cognite Data Fusion.
    This is the read version of ThreeDModelRevision, which is used when retrieving 3D model revisions.

    Args:
        id (int): The ID of the revision.
        file_id (int): The file id.
        published (bool): True if the revision is marked as published.
        rotation (list[float] | None): No description.
        scale (list[float] | None): Scale of 3D model in directions X,Y and Z. Should be uniform.
        translation (list[float] | None): 3D offset of the model.
        camera (RevisionCameraProperties | None): Initial camera position and target.
        status (str): The status of the revision.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
        thumbnail_threed_file_id (int | None): The threed file ID of a thumbnail for the revision. Use /3d/files/{id} to retrieve the file.
        thumbnail_url (str | None): The URL of a thumbnail for the revision.
        asset_mapping_count (int): The number of asset mappings for this revision.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
    """

    def __init__(
        self,
        id: int,
        file_id: int,
        published: bool,
        rotation: list[float] | None,
        scale: list[float] | None,
        translation: list[float] | None,
        camera: RevisionCameraProperties | None,
        status: str,
        metadata: dict[str, str] | None,
        thumbnail_threed_file_id: int | None,
        thumbnail_url: str | None,
        asset_mapping_count: int,
        created_time: int,
    ) -> None:
        super().__init__(
            file_id=file_id,
            published=published,
            rotation=rotation,
            scale=scale,
            translation=translation,
            camera=camera,
            metadata=metadata,
        )
        self.id = id
        self.created_time = created_time
        self.status = status
        self.thumbnail_threed_file_id = thumbnail_threed_file_id
        self.thumbnail_url = thumbnail_url
        self.asset_mapping_count = asset_mapping_count

    @classmethod
    def _load(cls, resource: dict) -> Self:
        return cls(
            id=resource["id"],
            file_id=resource["fileId"],
            published=resource["published"],
            rotation=resource.get("rotation"),
            scale=resource.get("scale"),
            translation=resource.get("translation"),
            camera=RevisionCameraProperties._load_if(resource.get("camera")),
            status=resource["status"],
            metadata=resource.get("metadata"),
            thumbnail_threed_file_id=resource.get("thumbnailThreedFileId"),
            thumbnail_url=resource.get("thumbnailUrl"),
            asset_mapping_count=resource["assetMappingCount"],
            created_time=resource["createdTime"],
        )

    def as_write(self) -> ThreeDModelRevisionWrite:
        """Returns this ThreedModelRevision in a write version."""
        if self.file_id is None:
            raise ValueError("ThreeDModelRevision must have a file_id to be writable")
        return ThreeDModelRevisionWrite(
            file_id=self.file_id,
            published=self.published or False,
            rotation=self.rotation,
            scale=self.scale,
            translation=self.translation,
            camera=self.camera,
            metadata=self.metadata,
        )


class ThreeDModelRevisionWrite(ThreeDModelRevisionCore):
    """This class represents a 3D model revision in Cognite Data Fusion.
    This is the write version of ThreeDModelRevision, which is used when creating 3D model revisions.

    Args:
        file_id (int): The file id to a file uploaded to Cognite's Files API. Can only be set on revision creation, and can never be updated.
        published (bool): True if the revision is marked as published.
        rotation (list[float] | None): No description.
        scale (list[float] | None): Scale of 3D model in directions X,Y and Z. Should be uniform.
        translation (list[float] | None): 3D offset of the model.
        camera (RevisionCameraProperties | dict[str, Any] | None): Initial camera position and target.
        metadata (dict[str, str] | None): Custom, application specific metadata. String key -> String value. Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.
    """

    def __init__(
        self,
        file_id: int,
        published: bool = False,
        rotation: list[float] | None = None,
        scale: list[float] | None = None,
        translation: list[float] | None = None,
        camera: RevisionCameraProperties | dict[str, Any] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        super().__init__(
            file_id=file_id,
            published=published,
            rotation=rotation,
            scale=scale,
            translation=translation,
            camera=camera,
            metadata=metadata,
        )

    @classmethod
    def _load(cls, resource: dict) -> ThreeDModelRevisionWrite:
        return cls(
            file_id=resource["fileId"],
            published=resource.get("published", False),
            rotation=resource.get("rotation"),
            scale=resource.get("scale"),
            translation=resource.get("translation"),
            camera=RevisionCameraProperties._load_if(resource.get("camera")),
            metadata=resource.get("metadata"),
        )

    def as_write(self) -> ThreeDModelRevisionWrite:
        """Returns this ThreedModelRevisionWrite instance."""
        return self


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
    def _get_update_properties(cls, item: CogniteResource | None = None) -> list[PropertySpec]:
        return [
            PropertySpec("published", is_nullable=False),
            PropertySpec("rotation", is_nullable=False),
            PropertySpec("camera", is_nullable=False),
            PropertySpec("scale", is_nullable=False),
            PropertySpec("translation", is_nullable=False),
            PropertySpec("metadata", is_object=True),
        ]


class ThreeDModelRevisionWriteList(CogniteResourceList[ThreeDModelRevisionWrite]):
    _RESOURCE = ThreeDModelRevisionWrite


class ThreeDModelRevisionList(
    WriteableCogniteResourceList[ThreeDModelRevisionWrite, ThreeDModelRevision], InternalIdTransformerMixin
):
    _RESOURCE = ThreeDModelRevision

    def as_write(self) -> ThreeDModelRevisionWriteList:
        """Returns this ThreedModelRevisionList in a write version."""
        return ThreeDModelRevisionWriteList([item.as_write() for item in self.data])


class ThreeDNode(CogniteResource):
    """No description.

    Args:
        id (int): The ID of the node.
        tree_index (int): The index of the node in the 3D model hierarchy, starting from 0. The tree is traversed in a depth-first order.
        parent_id (int | None): The parent of the node, null if it is the root node.
        depth (int): The depth of the node in the tree, starting from 0 at the root node.
        name (str): The name of the node.
        subtree_size (int): The number of descendants of the node, plus one (counting itself).
        properties (dict[str, dict[str, str]] | None): Properties extracted from 3D model, with property categories containing key/value string pairs.
        bounding_box (BoundingBox3D | None): The bounding box of the subtree with this sector as the root sector. Is null if there are no geometries in the subtree.
    """

    def __init__(
        self,
        id: int,
        tree_index: int,
        parent_id: int | None,
        depth: int,
        name: str,
        subtree_size: int,
        properties: dict[str, dict[str, str]] | None,
        bounding_box: BoundingBox3D | None,
    ) -> None:
        self.id = id
        self.tree_index = tree_index
        self.parent_id = parent_id
        self.depth = depth
        self.name = name
        self.subtree_size = subtree_size
        self.properties = properties
        self.bounding_box = bounding_box

    @classmethod
    def _load(cls, resource: dict) -> ThreeDNode:
        return cls(
            id=resource["id"],
            tree_index=resource["treeIndex"],
            parent_id=resource.get("parentId"),
            depth=resource["depth"],
            name=resource["name"],
            subtree_size=resource["subtreeSize"],
            properties=resource.get("properties"),
            bounding_box=BoundingBox3D._load_if(resource.get("boundingBox")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if isinstance(self.bounding_box, BoundingBox3D):
            result["boundingBox" if camel_case else "bounding_box"] = self.bounding_box.dump(camel_case=camel_case)
        return result


class ThreeDNodeList(CogniteResourceList[ThreeDNode], InternalIdTransformerMixin):
    _RESOURCE = ThreeDNode


class ThreeDAssetMappingCore(WriteableCogniteResource["ThreeDAssetMappingWrite"], ABC):
    """No description.

    Args:
        node_id (int): The ID of the node.
        asset_id (int | None): The ID of the associated asset (Cognite's Assets API).
    """

    def __init__(
        self,
        node_id: int,
        asset_id: int | None,
    ) -> None:
        self.node_id = node_id
        self.asset_id = asset_id


class ThreeDAssetMapping(ThreeDAssetMappingCore):
    """3D Asset mappings.
    This is the read version of ThreeDAssetMapping, which is used when retrieving 3D asset mappings.

    Args:
        node_id (int): The ID of the node.
        asset_id (int | None): The ID of the associated asset (Cognite's Assets API).
        tree_index (int | None): A number describing the position of this node in the 3D hierarchy, starting from 0. The tree is traversed in a depth-first order.
        subtree_size (int | None): The number of nodes in the subtree of this node (this number included the node itself).
    """

    def __init__(
        self,
        node_id: int,
        asset_id: int | None,
        tree_index: int | None,
        subtree_size: int | None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            asset_id=asset_id,
        )
        self.tree_index = tree_index
        self.subtree_size = subtree_size

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            node_id=resource["nodeId"],
            asset_id=resource.get("assetId"),
            tree_index=resource.get("treeIndex"),
            subtree_size=resource.get("subtreeSize"),
        )

    def as_write(self) -> ThreeDAssetMappingWrite:
        """Returns this ThreedAssetMapping in a write version."""
        if self.node_id is None or self.asset_id is None:
            raise ValueError("ThreeDAssetMapping must have a node_id and asset_id to be writable")
        return ThreeDAssetMappingWrite(
            node_id=self.node_id,
            asset_id=self.asset_id,
        )


class ThreeDAssetMappingWrite(ThreeDAssetMappingCore):
    """3D Asset mappings.
    This is the write version of ThreeDAssetMapping, which is used when creating 3D asset mappings.

    Args:
        node_id (int): The ID of the node.
        asset_id (int | None): The ID of the associated asset (Cognite's Assets API).
    """

    def __init__(self, node_id: int, asset_id: int | None = None) -> None:
        super().__init__(node_id=node_id, asset_id=asset_id)

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> ThreeDAssetMappingWrite:
        return cls(
            node_id=resource["nodeId"],
            asset_id=resource.get("assetId"),
        )

    def as_write(self) -> ThreeDAssetMappingWrite:
        """Returns this ThreedAssetMappingWrite instance."""
        return self


class ThreeDAssetMappingWriteList(CogniteResourceList[ThreeDAssetMappingWrite]):
    _RESOURCE = ThreeDAssetMappingWrite


class ThreeDAssetMappingList(WriteableCogniteResourceList[ThreeDAssetMappingWrite, ThreeDAssetMapping]):
    _RESOURCE = ThreeDAssetMapping

    def as_write(self) -> ThreeDAssetMappingWriteList:
        """Returns this ThreedAssetMappingList in a write version."""
        return ThreeDAssetMappingWriteList([item.as_write() for item in self.data])
