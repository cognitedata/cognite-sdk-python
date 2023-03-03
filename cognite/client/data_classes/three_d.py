
from cognite.client.data_classes._base import CogniteLabelUpdate, CogniteListUpdate, CogniteObjectUpdate, CognitePrimitiveUpdate, CognitePropertyClassUtil, CogniteResource, CogniteResourceList, CogniteUpdate
if TYPE_CHECKING:
    from cognite.client import CogniteClient

class RevisionCameraProperties(dict):

    def __init__(self, target=None, position=None, **kwargs: Any):
        self.target = target
        self.position = position
        self.update(kwargs)
    target = CognitePropertyClassUtil.declare_property('target')
    position = CognitePropertyClassUtil.declare_property('position')

class BoundingBox3D(dict):

    def __init__(self, max=None, min=None, **kwargs: Any):
        self.max = max
        self.min = min
        self.update(kwargs)
    max = CognitePropertyClassUtil.declare_property('max')
    min = CognitePropertyClassUtil.declare_property('min')

class ThreeDModel(CogniteResource):

    def __init__(self, name=None, id=None, created_time=None, metadata=None, cognite_client=None):
        self.name = name
        self.id = id
        self.created_time = created_time
        self.metadata = metadata
        self._cognite_client = cast('CogniteClient', cognite_client)

class ThreeDModelUpdate(CogniteUpdate):

    class _PrimitiveThreeDModelUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectThreeDModelUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListThreeDModelUpdate(CogniteListUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelThreeDModelUpdate(CogniteLabelUpdate):

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def name(self):
        return ThreeDModelUpdate._PrimitiveThreeDModelUpdate(self, 'name')

    @property
    def metadata(self):
        return ThreeDModelUpdate._ObjectThreeDModelUpdate(self, 'metadata')

class ThreeDModelList(CogniteResourceList):
    _RESOURCE = ThreeDModel

class ThreeDModelRevision(CogniteResource):

    def __init__(self, id=None, file_id=None, published=None, rotation=None, camera=None, status=None, metadata=None, thumbnail_threed_file_id=None, thumbnail_url=None, asset_mapping_count=None, created_time=None, cognite_client=None):
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
        self._cognite_client = cast('CogniteClient', cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if (instance.camera is not None):
                instance.camera = RevisionCameraProperties(**instance.camera)
        return instance

class ThreeDModelRevisionUpdate(CogniteUpdate):

    class _PrimitiveThreeDModelRevisionUpdate(CognitePrimitiveUpdate):

        def set(self, value):
            return self._set(value)

    class _ObjectThreeDModelRevisionUpdate(CogniteObjectUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _ListThreeDModelRevisionUpdate(CogniteListUpdate):

        def set(self, value):
            return self._set(value)

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    class _LabelThreeDModelRevisionUpdate(CogniteLabelUpdate):

        def add(self, value):
            return self._add(value)

        def remove(self, value):
            return self._remove(value)

    @property
    def published(self):
        return ThreeDModelRevisionUpdate._PrimitiveThreeDModelRevisionUpdate(self, 'published')

    @property
    def rotation(self):
        return ThreeDModelRevisionUpdate._ListThreeDModelRevisionUpdate(self, 'rotation')

    @property
    def camera(self):
        return ThreeDModelRevisionUpdate._ObjectThreeDModelRevisionUpdate(self, 'camera')

    @property
    def metadata(self):
        return ThreeDModelRevisionUpdate._ObjectThreeDModelRevisionUpdate(self, 'metadata')

class ThreeDModelRevisionList(CogniteResourceList):
    _RESOURCE = ThreeDModelRevision

class ThreeDNode(CogniteResource):

    def __init__(self, id=None, tree_index=None, parent_id=None, depth=None, name=None, subtree_size=None, properties=None, bounding_box=None, cognite_client=None):
        self.id = id
        self.tree_index = tree_index
        self.parent_id = parent_id
        self.depth = depth
        self.name = name
        self.subtree_size = subtree_size
        self.properties = properties
        self.bounding_box = bounding_box
        self._cognite_client = cast('CogniteClient', cognite_client)

    @classmethod
    def _load(cls, resource, cognite_client=None):
        instance = super()._load(resource, cognite_client)
        if isinstance(resource, Dict):
            if (instance.bounding_box is not None):
                instance.bounding_box = BoundingBox3D(**instance.bounding_box)
        return instance

class ThreeDNodeList(CogniteResourceList):
    _RESOURCE = ThreeDNode

class ThreeDAssetMapping(CogniteResource):

    def __init__(self, node_id=None, asset_id=None, tree_index=None, subtree_size=None, cognite_client=None):
        self.node_id = node_id
        self.asset_id = asset_id
        self.tree_index = tree_index
        self.subtree_size = subtree_size
        self._cognite_client = cast('CogniteClient', cognite_client)

class ThreeDAssetMappingList(CogniteResourceList):
    _RESOURCE = ThreeDAssetMapping
