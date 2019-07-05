from typing import *

from cognite.client.data_classes._base import *


# GenClass: Model3D
class ThreeDModel(CogniteResource):
    """No description.

    Args:
        name (str): The name of the model.
        id (int): The ID of the model.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        id: int = None,
        created_time: int = None,
        metadata: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.name = name
        self.id = id
        self.created_time = created_time
        self.metadata = metadata
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: UpdateModel3D
class ThreeDModelUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): A JavaScript-friendly internal ID for the object.
    """

    @property
    def name(self):
        return _PrimitiveThreeDModelUpdate(self, "name")

    @property
    def metadata(self):
        return _ObjectThreeDModelUpdate(self, "metadata")


class _PrimitiveThreeDModelUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> ThreeDModelUpdate:
        return self._set(value)


class _ObjectThreeDModelUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> ThreeDModelUpdate:
        return self._set(value)

    def add(self, value: Dict) -> ThreeDModelUpdate:
        return self._add(value)

    def remove(self, value: List) -> ThreeDModelUpdate:
        return self._remove(value)


class _ListThreeDModelUpdate(CogniteListUpdate):
    def set(self, value: List) -> ThreeDModelUpdate:
        return self._set(value)

    def add(self, value: List) -> ThreeDModelUpdate:
        return self._add(value)

    def remove(self, value: List) -> ThreeDModelUpdate:
        return self._remove(value)

    # GenStop


class ThreeDModelList(CogniteResourceList):
    _RESOURCE = ThreeDModel
    _UPDATE = ThreeDModelUpdate


# GenClass: Revision3D
class ThreeDModelRevision(CogniteResource):
    """No description.

    Args:
        id (int): The ID of the revision.
        file_id (int): The file id.
        published (bool): True if the revision is marked as published.
        rotation (List[float]): No description.
        camera (Dict[str, Any]): Initial camera position and target.
        status (str): The status of the revision.
        metadata (Dict[str, Any]): Custom, application specific metadata. String key -> String value
        thumbnail_threed_file_id (int): The threed file ID of a thumbnail for the revision. Use /3d/files/{id} to retrieve the file.
        thumbnail_url (str): The URL of a thumbnail for the revision.
        asset_mapping_count (int): The number of asset mappings for this revision.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        file_id: int = None,
        published: bool = None,
        rotation: List[float] = None,
        camera: Dict[str, Any] = None,
        status: str = None,
        metadata: Dict[str, Any] = None,
        thumbnail_threed_file_id: int = None,
        thumbnail_url: str = None,
        asset_mapping_count: int = None,
        created_time: int = None,
        cognite_client=None,
    ):
        self.id = id
        self.file_id = file_id
        self.published = published
        self.rotation = rotation
        self.camera = camera
        self.status = status
        self.metadata = metadata
        self.thumbnail_threed_file_id = thumbnail_threed_file_id
        self.thumbnail_url = thumbnail_url
        self.asset_mapping_count = asset_mapping_count
        self.created_time = created_time
        self._cognite_client = cognite_client

    # GenStop


# GenUpdateClass: UpdateRevision3D
class ThreeDModelRevisionUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): A JavaScript-friendly internal ID for the object.
    """

    @property
    def published(self):
        return _PrimitiveThreeDModelRevisionUpdate(self, "published")

    @property
    def rotation(self):
        return _ListThreeDModelRevisionUpdate(self, "rotation")

    @property
    def camera(self):
        return _ObjectThreeDModelRevisionUpdate(self, "camera")

    @property
    def metadata(self):
        return _ObjectThreeDModelRevisionUpdate(self, "metadata")


class _PrimitiveThreeDModelRevisionUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> ThreeDModelRevisionUpdate:
        return self._set(value)


class _ObjectThreeDModelRevisionUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> ThreeDModelRevisionUpdate:
        return self._set(value)

    def add(self, value: Dict) -> ThreeDModelRevisionUpdate:
        return self._add(value)

    def remove(self, value: List) -> ThreeDModelRevisionUpdate:
        return self._remove(value)


class _ListThreeDModelRevisionUpdate(CogniteListUpdate):
    def set(self, value: List) -> ThreeDModelRevisionUpdate:
        return self._set(value)

    def add(self, value: List) -> ThreeDModelRevisionUpdate:
        return self._add(value)

    def remove(self, value: List) -> ThreeDModelRevisionUpdate:
        return self._remove(value)

    # GenStop


class ThreeDModelRevisionList(CogniteResourceList):
    _RESOURCE = ThreeDModelRevision
    _UPDATE = ThreeDModelRevisionUpdate


# GenClass: Node3D
class ThreeDNode(CogniteResource):
    """No description.

    Args:
        id (int): The ID of the node.
        tree_index (int): The index of the node in the 3D model hierarchy, starting from 0. The tree is traversed in a depth-first order.
        parent_id (int): The parent of the node, null if it is the root node.
        depth (int): The depth of the node in the tree, starting from 0 at the root node.
        name (str): The name of the node.
        subtree_size (int): The number of descendants of the node, plus one (counting itself).
        bounding_box (Dict[str, Any]): The bounding box of the subtree with this sector as the root sector. Is null if there are no geometries in the subtree.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        tree_index: int = None,
        parent_id: int = None,
        depth: int = None,
        name: str = None,
        subtree_size: int = None,
        bounding_box: Dict[str, Any] = None,
        cognite_client=None,
    ):
        self.id = id
        self.tree_index = tree_index
        self.parent_id = parent_id
        self.depth = depth
        self.name = name
        self.subtree_size = subtree_size
        self.bounding_box = bounding_box
        self._cognite_client = cognite_client

    # GenStop


class ThreeDNodeList(CogniteResourceList):
    _RESOURCE = ThreeDNode
    _ASSERT_CLASSES = False


# GenClass: AssetMapping3D
class ThreeDAssetMapping(CogniteResource):
    """No description.

    Args:
        node_id (int): The ID of the node.
        asset_id (int): The ID of the associated asset (Cognite's Assets API).
        tree_index (int): A number describing the position of this node in the 3D hierarchy, starting from 0. The tree is traversed in a depth-first order.
        subtree_size (int): The number of nodes in the subtree of this node (this number included the node itself).
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        node_id: int = None,
        asset_id: int = None,
        tree_index: int = None,
        subtree_size: int = None,
        cognite_client=None,
    ):
        self.node_id = node_id
        self.asset_id = asset_id
        self.tree_index = tree_index
        self.subtree_size = subtree_size
        self._cognite_client = cognite_client

    # GenStop


class ThreeDAssetMappingList(CogniteResourceList):
    _RESOURCE = ThreeDAssetMapping
    _ASSERT_CLASSES = False
