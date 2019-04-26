from typing import *

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import *


class ThreeDAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = ThreeDModelsAPI(*args, **kwargs)
        self.revisions = ThreeDRevisionsAPI(*args, **kwargs)
        self.files = ThreeDFilesAPI(*args, **kwargs)
        self.asset_mapping = ThreeDAssetMappingAPI(*args, **kwargs)
        self.reveal = ThreeDRevealAPI(*args, **kwargs)


# GenClass: 3DModel
class ThreeDModel(CogniteResource):
    """No description.

    Args:
        name (str): The name of the model.
        id (int): The ID of the model.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
    """

    def __init__(self, name: str = None, id: int = None, created_time: int = None, **kwargs):
        self.name = name
        self.id = id
        self.created_time = created_time

    # GenStop


# GenUpdateClass: Update3DModel
class ThreeDModelUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): Javascript friendly internal ID given to the object.
    """

    @property
    def name(self):
        return _PrimitiveUpdate(self, "name")


class _PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> ThreeDModelUpdate:
        return self._set(value)


class _ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> ThreeDModelUpdate:
        return self._set(value)

    def add(self, value: Dict) -> ThreeDModelUpdate:
        return self._add(value)

    def remove(self, value: List) -> ThreeDModelUpdate:
        return self._remove(value)


class _ListUpdate(CogniteListUpdate):
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


class ThreeDModelsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models"

    def __call__(
        self, chunk_size: int = None, published: bool = False
    ) -> Generator[Union[ThreeDModel, ThreeDModelList], None, None]:
        """Iterate over 3d models

        Fetches 3d models as they are iterated over, so you keep a limited number of 3d models in memory.

        Args:
            chunk_size (int, optional): Number of 3d models to return in each chunk. Defaults to yielding one model a time.
            published (bool): Filter based on whether or not the model has published revisions.

        Yields:
            Union[ThreeDModel, ThreeDModelList]: yields ThreeDModel one by one if chunk is not specified, else ThreeDModelList objects.
        """
        return self._list_generator(
            ThreeDModelList,
            resource_path=self._RESOURCE_PATH,
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
        )

    def __iter__(self) -> Generator[ThreeDModel, None, None]:
        """Iterate over 3d models

        Fetches models as they are iterated over, so you keep a limited number of models in memory.

        Yields:
            ThreeDModel: yields models one by one.
        """
        return self.__call__()

    def get(self, id: int) -> ThreeDModel:
        """Retrieve a 3d model by id

        Args:
            id (int): Get the model with this id.

        Returns:
            ThreeDModel: The requested 3d model.
        """
        return self._retrieve(ThreeDModel, self._RESOURCE_PATH, id)

    def list(self, published: bool = False, limit: int = None) -> ThreeDModelList:
        """List 3d models.

        Args:
            published (bool): Filter based on whether or not the model has published revisions.
            limit (int): Maximum number of models to retrieve.

        Returns:
            ThreeDModelList: The list of 3d models.
        """
        return self._list(
            ThreeDModelList, self._RESOURCE_PATH, method="GET", filter={"published": published}, limit=limit
        )

    def update(
        self, item: Union[ThreeDModel, ThreeDModelUpdate, List[Union[ThreeDModel, ThreeDModelList]]]
    ) -> Union[ThreeDModel, ThreeDModelList]:
        """Update 3d models.
        
        Args:
            item (Union[ThreeDModel, ThreeDModelUpdate, List[Union[ThreeDModel, ThreeDModelUpdate]]]): ThreeDModel(s) to update

        Returns:
            Union[ThreeDModel, ThreeDModelList]: Updated ThreeDModel(s)
        """
        return self._update_multiple(cls=ThreeDModelList, resource_path=self._RESOURCE_PATH, items=item)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete 3d models.

        Args:
            id (Union[int, List[int]]): ID or list of IDs to delete.

        Returns:
            None
        """
        self._delete_multiple(resource_path=self._RESOURCE_PATH, ids=id, wrap_ids=True)


# GenClass: 3DRevision
class ThreeDModelRevision(CogniteResource):
    """No description.

    Args:
        id (int): The ID of the revision.
        file_id (int): The file id.
        published (bool): True if the revision is marked as published.
        rotation (List[float]): No description.
        camera (Dict[str, Any]): Initial camera position and target.
        status (str): The status of the revision.
        thumbnail_threed_file_id (int): The threed file ID of a thumbnail for the revision. Use /3d/files/{id} to retrieve the file.
        thumbnail_url (str): The URL of a thumbnail for the revision.
        asset_mapping_count (int): The number of asset mappings for this revision.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
    """

    def __init__(
        self,
        id: int = None,
        file_id: int = None,
        published: bool = None,
        rotation: List[float] = None,
        camera: Dict[str, Any] = None,
        status: str = None,
        thumbnail_threed_file_id: int = None,
        thumbnail_url: str = None,
        asset_mapping_count: int = None,
        created_time: int = None,
        **kwargs
    ):
        self.id = id
        self.file_id = file_id
        self.published = published
        self.rotation = rotation
        self.camera = camera
        self.status = status
        self.thumbnail_threed_file_id = thumbnail_threed_file_id
        self.thumbnail_url = thumbnail_url
        self.asset_mapping_count = asset_mapping_count
        self.created_time = created_time

    # GenStop


class ThreeDRevisionsAPI(APIClient):
    pass


class ThreeDFilesAPI(APIClient):
    pass


# GenClass: 3DAssetMapping
class ThreeDAssetMapping(CogniteResource):
    """No description.

    Args:
        node_id (int): The ID of the node.
        asset_id (int): The ID of the associated asset (Cognite's Assets API).
        tree_index (int): A number describing the position of this node in the 3D hierarchy, starting from 0. The tree is traversed in a depth-first order.
        subtree_size (int): The number of nodes in the subtree of this node (this number included the node itself).
    """

    def __init__(
        self, node_id: int = None, asset_id: int = None, tree_index: int = None, subtree_size: int = None, **kwargs
    ):
        self.node_id = node_id
        self.asset_id = asset_id
        self.tree_index = tree_index
        self.subtree_size = subtree_size

    # GenStop


class ThreeDAssetMappingList(CogniteResourceList):
    _RESOURCE = ThreeDAssetMapping
    _ASSERT_CLASSES = False


class ThreeDAssetMappingAPI(APIClient):
    pass


# GenClass: Reveal3DRevision
class ThreeDRevealRevision(CogniteResource):
    """No description.

    Args:
        id (int): The ID of the revision.
        file_id (int): The file id.
        published (bool): True if the revision is marked as published.
        rotation (List[float]): No description.
        camera (Dict[str, Any]): Initial camera position and target.
        status (str): The status of the revision.
        thumbnail_threed_file_id (int): The threed file ID of a thumbnail for the revision. Use /3d/files/{id} to retrieve the file.
        thumbnail_url (str): The URL of a thumbnail for the revision.
        asset_mapping_count (int): The number of asset mappings for this revision.
        created_time (int): The creation time of the resource, in milliseconds since January 1, 1970 at 00:00 UTC.
        scene_threed_files (List[Dict[str, Any]]): No description.
    """

    def __init__(
        self,
        id: int = None,
        file_id: int = None,
        published: bool = None,
        rotation: List[float] = None,
        camera: Dict[str, Any] = None,
        status: str = None,
        thumbnail_threed_file_id: int = None,
        thumbnail_url: str = None,
        asset_mapping_count: int = None,
        created_time: int = None,
        scene_threed_files: List[Dict[str, Any]] = None,
        **kwargs
    ):
        self.id = id
        self.file_id = file_id
        self.published = published
        self.rotation = rotation
        self.camera = camera
        self.status = status
        self.thumbnail_threed_file_id = thumbnail_threed_file_id
        self.thumbnail_url = thumbnail_url
        self.asset_mapping_count = asset_mapping_count
        self.created_time = created_time
        self.scene_threed_files = scene_threed_files

    # GenStop


class ThreeDRevealAPI(APIClient):
    pass
