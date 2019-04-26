from typing import *

from cognite.client._utils.api_client import APIClient
from cognite.client._utils.base import *


class ThreeDAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = ThreeDModelsAPI(*args, **kwargs)
        self.revisions = ThreeDRevisionsAPI(*args, **kwargs)
        self.files = ThreeDFilesAPI(*args, **kwargs)
        self.asset_mappings = ThreeDAssetMappingAPI(*args, **kwargs)
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


# GenUpdateClass: Update3DRevision
class ThreeDModelRevisionUpdate(CogniteUpdate):
    """No description.

    Args:
        id (int): Javascript friendly internal ID given to the object.
    """

    @property
    def published(self):
        return _PrimitiveUpdate(self, "published")

    @property
    def rotation(self):
        return _ListUpdate(self, "rotation")

    @property
    def camera(self):
        return _ObjectUpdate(self, "camera")


class _PrimitiveUpdate(CognitePrimitiveUpdate):
    def set(self, value: Any) -> ThreeDModelRevisionUpdate:
        return self._set(value)


class _ObjectUpdate(CogniteObjectUpdate):
    def set(self, value: Dict) -> ThreeDModelRevisionUpdate:
        return self._set(value)

    def add(self, value: Dict) -> ThreeDModelRevisionUpdate:
        return self._add(value)

    def remove(self, value: List) -> ThreeDModelRevisionUpdate:
        return self._remove(value)


class _ListUpdate(CogniteListUpdate):
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


# GenClass: 3DNode
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
        **kwargs
    ):
        self.id = id
        self.tree_index = tree_index
        self.parent_id = parent_id
        self.depth = depth
        self.name = name
        self.subtree_size = subtree_size
        self.bounding_box = bounding_box

    # GenStop


class ThreeDNodeList(CogniteResourceList):
    _RESOURCE = ThreeDNode
    _ASSERT_CLASSES = False


class ThreeDRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions"

    def __call__(
        self, model_id: int, chunk_size: int = None, published: bool = False
    ) -> Generator[Union[ThreeDModelRevision, ThreeDModelRevisionList], None, None]:
        """Iterate over 3d model revisions

        Fetches 3d model revisions as they are iterated over, so you keep a limited number of 3d model revisions in memory.

        Args:
            model_id (int): Iterate over revisions for the model with this id.
            chunk_size (int, optional): Number of 3d model revisions to return in each chunk. Defaults to yielding one model a time.
            published (bool): Filter based on whether or not the revision has been published.

        Yields:
            Union[ThreeDModelRevision, ThreeDModelRevisionList]: yields ThreeDModelRevision one by one if chunk is not
                specified, else ThreeDModelRevisionList objects.
        """
        return self._list_generator(
            ThreeDModelRevisionList,
            resource_path=self._RESOURCE_PATH,
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
        )

    def get(self, model_id: int, id: int) -> ThreeDModelRevision:
        """Retrieve a 3d model revision by id

        Args:
            model_id (int): Get the revision under the model with this id.
            id (int): Get the model revision with this id.

        Returns:
            ThreeDModelRevision: The requested 3d model revision.
        """
        return self._retrieve(ThreeDModelRevision, self._RESOURCE_PATH.format(model_id), id)

    def create(
        self, model_id: int, revision: Union[ThreeDModelRevision, List[ThreeDModelRevision]]
    ) -> Union[ThreeDModelRevision, ThreeDModelRevisionList]:
        """Create a revisions for a specified 3d model.

        Args:
            model_id (int): Create revisions for this model.
            revision (Union[ThreeDModelRevision, List[ThreeDModelRevision]]): The revision(s) to create.

        Returns:
            Union[ThreeDModelRevision, ThreeDModelRevisionList]: The created revision(s)
        """
        return self._create_multiple(ThreeDModelRevisionList, self._RESOURCE_PATH.format(model_id), items=revision)

    def list(self, model_id: int, published: bool = False, limit: int = None) -> ThreeDModelRevisionList:
        """List 3d model revisions.

        Args:
            model_id (int): List revisions under the model with this id.
            published (bool): Filter based on whether or not the revision is published.
            limit (int): Maximum number of models to retrieve.

        Returns:
            ThreeDModelRevisionList: The list of 3d model revisions.
        """
        return self._list(
            ThreeDModelRevisionList,
            self._RESOURCE_PATH.format(model_id),
            method="GET",
            filter={"published": published},
            limit=limit,
        )

    def update(
        self,
        model_id: int,
        item: Union[
            ThreeDModelRevision, ThreeDModelRevisionUpdate, List[Union[ThreeDModelRevision, ThreeDModelRevisionList]]
        ],
    ) -> Union[ThreeDModelRevision, ThreeDModelRevisionList]:
        """Update 3d model revisions.

        Args:
            model_id (int): Update the revision under the model with this id.
            item (Union[ThreeDModelRevision, ThreeDModelRevisionUpdate, List[Union[ThreeDModelRevision, ThreeDModelRevisionUpdate]]]):
                ThreeDModelRevision(s) to update

        Returns:
            Union[ThreeDModelRevision, ThreeDModelRevisionList]: Updated ThreeDModelRevision(s)
        """
        return self._update_multiple(
            cls=ThreeDModelRevisionList, resource_path=self._RESOURCE_PATH.format(model_id), items=item
        )

    def delete(self, model_id: int, id: Union[int, List[int]]) -> None:
        """Delete 3d model revisions.

        Args:
            model_id (int): Delete the revision under the model with this id.
            id (Union[int, List[int]]): ID or list of IDs to delete.

        Returns:
            None
        """
        self._delete_multiple(resource_path=self._RESOURCE_PATH.format(model_id), ids=id, wrap_ids=True)

    def update_thumbnail(self, model_id: int, revision_id: int, file_id: int) -> None:
        """Update a revision thumbnail.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            file_id (int): Id of the thumbnail file in the Files API.

        Returns:
            None
        """
        resource_path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/thumbnail", model_id, revision_id)
        body = {"fileId": file_id}
        self._post(resource_path, json=body)

    def list_nodes(
        self, model_id: int, revision_id: int, node_id: int = None, depth: int = None, limit: int = None
    ) -> ThreeDNodeList:
        """Retrieves a list of nodes from the hierarchy in the 3D Model.

        You can also request a specific subtree with the 'nodeId' query parameter and limit the depth of
        the resulting subtree with the 'depth' query parameter.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): ID of the root node of the subtree you request (default is the root node).
            depth (int): Get sub nodes up to this many levels below the specified node. Depth 0 is the root node.
            limit (int): Maximun number of nodes to return.

        Returns:
            ThreeDNodeList: The list of 3d nodes.
        """
        resource_path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            ThreeDNodeList, resource_path, method="GET", limit=limit, filter={"depth": depth, "nodeId": node_id}
        )

    def list_ancestor_nodes(
        self, model_id: int, revision_id: int, node_id: int = None, limit: int = None
    ) -> ThreeDNodeList:
        """Retrieves a list of ancestor nodes of a given node, including itself, in the hierarchy of the 3D model

        You can also request a specific subtree with the 'nodeId' query parameter and limit the depth of
        the resulting subtree with the 'depth' query parameter.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): ID of the node to get the ancestors of.
            depth (int): Get sub nodes up to this many levels below the specified node. Depth 0 is the root node.
            limit (int): Maximun number of nodes to return.

        Returns:
            ThreeDNodeList: The list of 3d nodes.
        """
        resource_path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(ThreeDNodeList, resource_path, method="GET", limit=limit, filter={"nodeId": node_id})


class ThreeDFilesAPI(APIClient):
    _RESOURCE_PATH = "/3d/files"

    def get(self, id: int) -> bytes:
        """Retrieve the contents of a 3d file by id.

        Args:
            id (int): The id of the file to retrieve.

        Returns:
            bytes: The contents of the file.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", id)
        return self._get(path).content


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
    _RESOURCE_PATH = "/3d/models/{}/revisions/{}/mappings"

    def list(
        self, model_id: int, revision_id: int, node_id: int = None, asset_id: int = None, limit: int = None
    ) -> ThreeDAssetMappingList:
        """List 3D node asset mappings.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): List only asset mappings associated with this node.
            asset_id (int): List only asset mappings associated with this asset.
            limit (int): Maximum number of asset mappings to return.

        Returns:
            ThreeDAssetMappingList: The list of asset mappings.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._list(
            ThreeDAssetMappingList, path, method="GET", filter={"nodeId": node_id, "assetId": asset_id}, limit=limit
        )

    def create(
        self, model_id: int, revision_id: int, asset_mapping: Union[ThreeDAssetMapping, List[ThreeDAssetMapping]]
    ) -> Union[ThreeDAssetMapping, ThreeDAssetMappingList]:
        """Create 3d node asset mappings.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            asset_mapping (Union[ThreeDAssetMapping, List[ThreeDAssetMapping]]): The asset mapping(s) to create.

        Returns:
            Union[ThreeDAssetMapping, ThreeDAssetMappingList]: The created asset mapping(s).
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._create_multiple(ThreeDAssetMappingList, path, items=asset_mapping)

    def delete(
        self, model_id: int, revision_id: int, asset_mapping: Union[ThreeDAssetMapping, List[ThreeDAssetMapping]]
    ) -> None:
        """Delete 3d node asset mappings.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            asset_mapping (Union[ThreeDAssetMapping, List[ThreeDAssetMapping]]): The asset mapping(s) to delete.

        Returns:
            None
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        utils.assert_type(asset_mapping, "asset_mapping", [list, ThreeDAssetMapping])
        if isinstance(asset_mapping, ThreeDAssetMapping):
            asset_mapping = [asset_mapping]
        items = {"items": [a.dump(camel_case=True) for a in asset_mapping]}
        self._post(path + "/delete", json=items)


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
