import json
from typing import *

from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelUpdate,
    ThreeDNodeList,
    ThreeDRevealNodeList,
    ThreeDRevealRevision,
    ThreeDRevealSectorList,
)
from cognite.client.utils import _utils as utils


class ThreeDAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = ThreeDModelsAPI(*args, **kwargs)
        self.revisions = ThreeDRevisionsAPI(*args, **kwargs)
        self.files = ThreeDFilesAPI(*args, **kwargs)
        self.asset_mappings = ThreeDAssetMappingAPI(*args, **kwargs)
        # self.reveal = ThreeDRevealAPI(*args, **kwargs)


class ThreeDModelsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models"
    _LIST_CLASS = ThreeDModelList

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
        return self._list_generator(method="GET", chunk_size=chunk_size, filter={"published": published})

    def __iter__(self) -> Generator[ThreeDModel, None, None]:
        """Iterate over 3d models

        Fetches models as they are iterated over, so you keep a limited number of models in memory.

        Yields:
            ThreeDModel: yields models one by one.
        """
        return self.__call__()

    def retrieve(self, id: int) -> ThreeDModel:
        """Retrieve a 3d model by id

        Args:
            id (int): Get the model with this id.

        Returns:
            ThreeDModel: The requested 3d model.
        """
        return self._retrieve(id)

    def list(self, published: bool = False, limit: int = 25) -> ThreeDModelList:
        """List 3d models.

        Args:
            published (bool): Filter based on whether or not the model has published revisions.
            limit (int): Maximum number of models to retrieve. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ThreeDModelList: The list of 3d models.
        """
        return self._list(method="GET", filter={"published": published}, limit=limit)

    def create(self, name: Union[str, List[str]]) -> Union[ThreeDModel, ThreeDModelList]:
        """Create new 3d models.

        Args:
            name (Union[str, List[str]): The name of the 3d model(s) to create.

        Returns:
            Union[ThreeDModel, ThreeDModelList]: The created 3d model(s).
        """
        return self._create_multiple(items=name)

    def update(
        self, item: Union[ThreeDModel, ThreeDModelUpdate, List[Union[ThreeDModel, ThreeDModelList]]]
    ) -> Union[ThreeDModel, ThreeDModelList]:
        """Update 3d models.
        
        Args:
            item (Union[ThreeDModel, ThreeDModelUpdate, List[Union[ThreeDModel, ThreeDModelUpdate]]]): ThreeDModel(s) to update

        Returns:
            Union[ThreeDModel, ThreeDModelList]: Updated ThreeDModel(s)
        """
        return self._update_multiple(items=item)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete 3d models.

        Args:
            id (Union[int, List[int]]): ID or list of IDs to delete.

        Returns:
            None
        """
        self._delete_multiple(ids=id, wrap_ids=True)


class ThreeDRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions"
    _LIST_CLASS = ThreeDModelRevisionList

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
            resource_path=self._RESOURCE_PATH.format(model_id),
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
        )

    def retrieve(self, model_id: int, id: int) -> ThreeDModelRevision:
        """Retrieve a 3d model revision by id

        Args:
            model_id (int): Get the revision under the model with this id.
            id (int): Get the model revision with this id.

        Returns:
            ThreeDModelRevision: The requested 3d model revision.
        """
        return self._retrieve(resource_path=self._RESOURCE_PATH.format(model_id), id=id)

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
        return self._create_multiple(resource_path=self._RESOURCE_PATH.format(model_id), items=revision)

    def list(self, model_id: int, published: bool = False, limit: int = 25) -> ThreeDModelRevisionList:
        """List 3d model revisions.

        Args:
            model_id (int): List revisions under the model with this id.
            published (bool): Filter based on whether or not the revision is published.
            limit (int): Maximum number of models to retrieve. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ThreeDModelRevisionList: The list of 3d model revisions.
        """
        return self._list(
            resource_path=self._RESOURCE_PATH.format(model_id),
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
        return self._update_multiple(resource_path=self._RESOURCE_PATH.format(model_id), items=item)

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
        self, model_id: int, revision_id: int, node_id: int = None, depth: int = None, limit: int = 25
    ) -> ThreeDNodeList:
        """Retrieves a list of nodes from the hierarchy in the 3D Model.

        You can also request a specific subtree with the 'nodeId' query parameter and limit the depth of
        the resulting subtree with the 'depth' query parameter.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): ID of the root node of the subtree you request (default is the root node).
            depth (int): Get sub nodes up to this many levels below the specified node. Depth 0 is the root node.
            limit (int): Maximun number of nodes to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ThreeDNodeList: The list of 3d nodes.
        """
        resource_path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            cls=ThreeDNodeList,
            resource_path=resource_path,
            method="GET",
            limit=limit,
            filter={"depth": depth, "nodeId": node_id},
        )

    def list_ancestor_nodes(
        self, model_id: int, revision_id: int, node_id: int = None, limit: int = 25
    ) -> ThreeDNodeList:
        """Retrieves a list of ancestor nodes of a given node, including itself, in the hierarchy of the 3D model

        You can also request a specific subtree with the 'nodeId' query parameter and limit the depth of
        the resulting subtree with the 'depth' query parameter.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): ID of the node to get the ancestors of.
            depth (int): Get sub nodes up to this many levels below the specified node. Depth 0 is the root node.
            limit (int): Maximun number of nodes to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ThreeDNodeList: The list of 3d nodes.
        """
        resource_path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            cls=ThreeDNodeList, resource_path=resource_path, method="GET", limit=limit, filter={"nodeId": node_id}
        )


class ThreeDFilesAPI(APIClient):
    _RESOURCE_PATH = "/3d/files"

    def retrieve(self, id: int) -> bytes:
        """Retrieve the contents of a 3d file by id.

        Args:
            id (int): The id of the file to retrieve.

        Returns:
            bytes: The contents of the file.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", id)
        return self._get(path).content


class ThreeDAssetMappingAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions/{}/mappings"
    _LIST_CLASS = ThreeDAssetMappingList

    def list(
        self, model_id: int, revision_id: int, node_id: int = None, asset_id: int = None, limit: int = 25
    ) -> ThreeDAssetMappingList:
        """List 3D node asset mappings.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): List only asset mappings associated with this node.
            asset_id (int): List only asset mappings associated with this asset.
            limit (int): Maximum number of asset mappings to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.

        Returns:
            ThreeDAssetMappingList: The list of asset mappings.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._list(
            resource_path=path, method="GET", filter={"nodeId": node_id, "assetId": asset_id}, limit=limit
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
        return self._create_multiple(resource_path=path, items=asset_mapping)

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
        chunks = utils.split_into_chunks([a.dump(camel_case=True) for a in asset_mapping], self._DELETE_LIMIT)
        tasks = [{"url_path": path + "/delete", "json": {"items": chunk}} for chunk in chunks]
        summary = utils.execute_tasks_concurrently(self._post, tasks, self._max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task["json"]["items"],
            task_list_element_unwrap_fn=lambda el: ThreeDAssetMapping._load(el),
            str_format_element_fn=lambda el: (el.asset_id, el.node_id),
        )


class ThreeDRevealAPI(APIClient):
    _RESOURCE_PATH = "/3d/reveal/models/{}/revisions"

    def retrieve_revision(self, model_id: int, revision_id: int):
        """Retrieve a revision.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH, model_id)
        return self._retrieve(cls=ThreeDRevealRevision, resource_path=path, id=revision_id)

    def list_nodes(self, model_id: int, revision_id: int, depth: int = None, node_id: int = None, limit: int = 25):
        """List 3D nodes.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            depth (int, optional): Get sub nodes up to this many levels below the specified node.
            node_id (int, optional): ID of the root note of the subtree you request.
            limit (int, optional): Maximun number of nodes to retrieve. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            cls=ThreeDRevealNodeList,
            resource_path=path,
            method="GET",
            filter={"depth": depth, "nodeId": node_id},
            limit=limit,
        )

    def list_ancestor_nodes(self, model_id: int, revision_id: int, node_id: int, limit: int = 25):
        """Retrieve a revision.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int): ID of the node to get the ancestors of.
            limit (int, optional): Maximun number of nodes to retrieve. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
        """
        path = utils.interpolate_and_url_encode(
            self._RESOURCE_PATH + "/{}/nodes/{}/ancestors", model_id, revision_id, node_id
        )
        return self._list(cls=ThreeDRevealNodeList, resource_path=path, method="GET", limit=limit)

    def list_sectors(self, model_id: int, revision_id: int, bounding_box: Dict[str, List] = None, limit: int = 25):
        """Retrieve a revision.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            bounding_box (Dict[str, List], optional): Bounding box to restrict search to. If given, only return sectors that intersect the given bounding box.
            limit (int, optional): Maximum number of items to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
        """
        path = utils.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/sectors", model_id, revision_id)
        return self._list(
            cls=ThreeDRevealSectorList,
            resource_path=path,
            method="GET",
            filter={"boundingBox": json.dumps(bounding_box)},
            limit=limit,
        )
