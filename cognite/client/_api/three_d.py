from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    BoundingBox3D,
    ThreeDAssetMapping,
    ThreeDAssetMappingList,
    ThreeDAssetMappingWrite,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelRevisionWrite,
    ThreeDModelUpdate,
    ThreeDModelWrite,
    ThreeDNode,
    ThreeDNodeList,
)
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import interpolate_and_url_encode, split_into_chunks, unpack_items_in_payload
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import IdentifierSequence, InternalId
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class ThreeDAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.models = ThreeDModelsAPI(config, api_version, cognite_client)
        self.revisions = ThreeDRevisionsAPI(config, api_version, cognite_client)
        self.files = ThreeDFilesAPI(config, api_version, cognite_client)
        self.asset_mappings = ThreeDAssetMappingAPI(config, api_version, cognite_client)


class ThreeDModelsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models"

    @overload
    def __call__(
        self, chunk_size: None = None, published: bool | None = None, limit: int | None = None
    ) -> Iterator[ThreeDModel]: ...

    @overload
    def __call__(
        self, chunk_size: int, published: bool | None = None, limit: int | None = None
    ) -> Iterator[ThreeDModelList]: ...

    def __call__(
        self, chunk_size: int | None = None, published: bool | None = None, limit: int | None = None
    ) -> Iterator[ThreeDModel] | Iterator[ThreeDModelList]:
        """Iterate over 3d models

        Fetches 3d models as they are iterated over, so you keep a limited number of 3d models in memory.

        Args:
            chunk_size (int | None): Number of 3d models to return in each chunk. Defaults to yielding one model a time.
            published (bool | None): Filter based on whether or not the model has published revisions.
            limit (int | None): Maximum number of 3d models to return. Defaults to return all items.

        Returns:
            Iterator[ThreeDModel] | Iterator[ThreeDModelList]: yields ThreeDModel one by one if chunk is not specified, else ThreeDModelList objects.
        """
        return self._list_generator(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
            limit=limit,
        )

    def __iter__(self) -> Iterator[ThreeDModel]:
        """Iterate over 3d models

        Fetches models as they are iterated over, so you keep a limited number of models in memory.

        Returns:
            Iterator[ThreeDModel]: yields models one by one.
        """
        return self()

    def retrieve(self, id: int) -> ThreeDModel | None:
        """`Retrieve a 3d model by id <https://developer.cognite.com/api#tag/3D-Models/operation/get3DModel>`_

        Args:
            id (int): Get the model with this id.

        Returns:
            ThreeDModel | None: The requested 3d model.

        Example:

            Get 3d model by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.models.retrieve(id=1)
        """
        return self._retrieve(cls=ThreeDModel, identifier=InternalId(id))

    def list(self, published: bool | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> ThreeDModelList:
        """`List 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/get3DModels>`_

        Args:
            published (bool | None): Filter based on whether or not the model has published revisions.
            limit (int | None): Maximum number of models to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDModelList: The list of 3d models.

        Examples:

            List 3d models::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> three_d_model_list = client.three_d.models.list()

            Iterate over 3d models::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for three_d_model in client.three_d.models:
                ...     three_d_model # do something with the 3d model

            Iterate over chunks of 3d models to reduce memory load::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> for three_d_model in client.three_d.models(chunk_size=50):
                ...     three_d_model # do something with the 3d model
        """
        return self._list(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            method="GET",
            filter={"published": published},
            limit=limit,
        )

    def create(
        self,
        name: str | ThreeDModelWrite | SequenceNotStr[str | ThreeDModelWrite],
        data_set_id: int | None = None,
        metadata: dict[str, str] | None = None,
    ) -> ThreeDModel | ThreeDModelList:
        """`Create new 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/create3DModels>`_

        Args:
            name (str | ThreeDModelWrite | SequenceNotStr[str | ThreeDModelWrite]): The name of the 3d model(s) or 3D
                model object to create. If a 3D model object is provided, the other arguments are ignored.
            data_set_id (int | None): The id of the dataset this 3D model belongs to.
            metadata (dict[str, str] | None): Custom, application-specific metadata. String key -> String value.
                Limits: Maximum length of key is 32 bytes, value 512 bytes, up to 16 key-value pairs.

        Returns:
            ThreeDModel | ThreeDModelList: The created 3d model(s).

        Example:

            Create new 3d models::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.models.create(name="My Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})

            Create multiple new 3D Models::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDModelWrite
                >>> client = CogniteClient()
                >>> my_model = ThreeDModelWrite(name="My Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})
                >>> my_other_model = ThreeDModelWrite(name="My Other Model", data_set_id=1, metadata={"key1": "value1", "key2": "value2"})
                >>> res = client.three_d.models.create([my_model, my_other_model])

        """
        items: ThreeDModelWrite | list[ThreeDModelWrite]
        if isinstance(name, str):
            items = ThreeDModelWrite(name, data_set_id, metadata)
        elif isinstance(name, ThreeDModelWrite):
            items = name
        else:
            items = [ThreeDModelWrite(n, data_set_id, metadata) if isinstance(n, str) else n for n in name]
        return self._create_multiple(list_cls=ThreeDModelList, resource_cls=ThreeDModel, items=items)

    @overload
    def update(
        self,
        item: ThreeDModel | ThreeDModelUpdate,
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModel: ...

    @overload
    def update(
        self,
        item: Sequence[ThreeDModel | ThreeDModelUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModelList: ...

    def update(
        self,
        item: ThreeDModel | ThreeDModelUpdate | Sequence[ThreeDModel | ThreeDModelUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModel | ThreeDModelList:
        """`Update 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/update3DModels>`_

        Args:
            item (ThreeDModel | ThreeDModelUpdate | Sequence[ThreeDModel | ThreeDModelUpdate]): ThreeDModel(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (ThreeDModel or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            ThreeDModel | ThreeDModelList: Updated ThreeDModel(s)

        Examples:

            Update 3d model that you have fetched. This will perform a full update of the model::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> three_d_model = client.three_d.models.retrieve(id=1)
                >>> three_d_model.name = "New Name"
                >>> res = client.three_d.models.update(three_d_model)

            Perform a partial update on a 3d model::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDModelUpdate
                >>> client = CogniteClient()
                >>> my_update = ThreeDModelUpdate(id=1).name.set("New Name")
                >>> res = client.three_d.models.update(my_update)

        """
        # Note that we cannot use the ThreeDModelWrite to update as the write format of a 3D model
        # does not have ID or External ID, thus no identifier to know which model to update.
        return self._update_multiple(
            list_cls=ThreeDModelList,
            resource_cls=ThreeDModel,
            update_cls=ThreeDModelUpdate,
            items=item,
            mode=mode,
        )

    def delete(self, id: int | Sequence[int]) -> None:
        """`Delete 3d models. <https://developer.cognite.com/api#tag/3D-Models/operation/delete3DModels>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs to delete.

        Example:

            Delete 3d model by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.models.delete(id=1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=True)


class ThreeDRevisionsAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions"

    @overload
    def __call__(
        self, model_id: int, chunk_size: None = None, published: bool = False, limit: int | None = None
    ) -> Iterator[ThreeDModelRevision]: ...
    @overload
    def __call__(
        self, model_id: int, chunk_size: int, published: bool = False, limit: int | None = None
    ) -> Iterator[ThreeDModelRevisionList]: ...

    def __call__(
        self, model_id: int, chunk_size: int | None = None, published: bool = False, limit: int | None = None
    ) -> Iterator[ThreeDModelRevision] | Iterator[ThreeDModelRevisionList]:
        """Iterate over 3d model revisions

        Fetches 3d model revisions as they are iterated over, so you keep a limited number of 3d model revisions in memory.

        Args:
            model_id (int): Iterate over revisions for the model with this id.
            chunk_size (int | None): Number of 3d model revisions to return in each chunk. Defaults to yielding one model a time.
            published (bool): Filter based on whether or not the revision has been published.
            limit (int | None): Maximum number of 3d model revisions to return. Defaults to return all items.

        Returns:
            Iterator[ThreeDModelRevision] | Iterator[ThreeDModelRevisionList]: yields ThreeDModelRevision one by one if chunk is not specified, else ThreeDModelRevisionList objects.
        """
        return self._list_generator(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            method="GET",
            chunk_size=chunk_size,
            filter={"published": published},
            limit=limit,
        )

    def retrieve(self, model_id: int, id: int) -> ThreeDModelRevision | None:
        """`Retrieve a 3d model revision by id <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/get3DRevision>`_

        Args:
            model_id (int): Get the revision under the model with this id.
            id (int): Get the model revision with this id.

        Returns:
            ThreeDModelRevision | None: The requested 3d model revision.

        Example:

            Retrieve 3d model revision by model id and revision id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.retrieve(model_id=1, id=1)
        """
        return self._retrieve(
            cls=ThreeDModelRevision,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            identifier=InternalId(id),
        )

    @overload
    def create(
        self, model_id: int, revision: ThreeDModelRevision | ThreeDModelRevisionWrite
    ) -> ThreeDModelRevision: ...

    @overload
    def create(
        self, model_id: int, revision: Sequence[ThreeDModelRevision] | Sequence[ThreeDModelRevisionWrite]
    ) -> ThreeDModelRevisionList: ...

    def create(
        self,
        model_id: int,
        revision: ThreeDModelRevision
        | ThreeDModelRevisionWrite
        | Sequence[ThreeDModelRevision]
        | Sequence[ThreeDModelRevisionWrite],
    ) -> ThreeDModelRevision | ThreeDModelRevisionList:
        """`Create a revisions for a specified 3d model. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/create3DRevisions>`_

        Args:
            model_id (int): Create revisions for this model.
            revision (ThreeDModelRevision | ThreeDModelRevisionWrite | Sequence[ThreeDModelRevision] | Sequence[ThreeDModelRevisionWrite]): The revision(s) to create.

        Returns:
            ThreeDModelRevision | ThreeDModelRevisionList: The created revision(s)

        Example:

            Create 3d model revision::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDModelRevisionWrite
                >>> client = CogniteClient()
                >>> my_revision = ThreeDModelRevisionWrite(file_id=1)
                >>> res = client.three_d.revisions.create(model_id=1, revision=my_revision)
        """
        return self._create_multiple(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            items=revision,
            input_resource_cls=ThreeDModelRevisionWrite,
        )

    def list(
        self, model_id: int, published: bool = False, limit: int | None = DEFAULT_LIMIT_READ
    ) -> ThreeDModelRevisionList:
        """`List 3d model revisions. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/get3DRevisions>`_

        Args:
            model_id (int): List revisions under the model with this id.
            published (bool): Filter based on whether or not the revision is published.
            limit (int | None): Maximum number of models to retrieve. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDModelRevisionList: The list of 3d model revisions.

        Example:

            List 3d model revisions::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.list(model_id=1, published=True, limit=100)
        """
        return self._list(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            method="GET",
            filter={"published": published},
            limit=limit,
        )

    def update(
        self,
        model_id: int,
        item: ThreeDModelRevision
        | ThreeDModelRevisionUpdate
        | Sequence[ThreeDModelRevision | ThreeDModelRevisionUpdate],
        mode: Literal["replace_ignore_null", "patch", "replace"] = "replace_ignore_null",
    ) -> ThreeDModelRevision | ThreeDModelRevisionList:
        """`Update 3d model revisions. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/update3DRevisions>`_

        Args:
            model_id (int): Update the revision under the model with this id.
            item (ThreeDModelRevision | ThreeDModelRevisionUpdate | Sequence[ThreeDModelRevision | ThreeDModelRevisionUpdate]): ThreeDModelRevision(s) to update
            mode (Literal['replace_ignore_null', 'patch', 'replace']): How to update data when a non-update object is given (ThreeDModelRevision or -Write). If you use 'replace_ignore_null', only the fields you have set will be used to replace existing (default). Using 'replace' will additionally clear all the fields that are not specified by you. Last option, 'patch', will update only the fields you have set and for container-like fields such as metadata or labels, add the values to the existing. For more details, see :ref:`appendix-update`.

        Returns:
            ThreeDModelRevision | ThreeDModelRevisionList: Updated ThreeDModelRevision(s)

        Examples:

            Update a revision that you have fetched. This will perform a full update of the revision::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> revision = client.three_d.revisions.retrieve(model_id=1, id=1)
                >>> revision.status = "New Status"
                >>> res = client.three_d.revisions.update(model_id=1, item=revision)

            Perform a partial update on a revision, updating the published property and adding a new field to metadata::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDModelRevisionUpdate
                >>> client = CogniteClient()
                >>> my_update = ThreeDModelRevisionUpdate(id=1).published.set(False).metadata.add({"key": "value"})
                >>> res = client.three_d.revisions.update(model_id=1, item=my_update)
        """
        return self._update_multiple(
            list_cls=ThreeDModelRevisionList,
            resource_cls=ThreeDModelRevision,
            update_cls=ThreeDModelRevisionUpdate,
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            items=item,
            mode=mode,
        )

    def delete(self, model_id: int, id: int | Sequence[int]) -> None:
        """`Delete 3d model revisions. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/delete3DRevisions>`_

        Args:
            model_id (int): Delete the revision under the model with this id.
            id (int | Sequence[int]): ID or list of IDs to delete.

        Example:

            Delete 3d model revision by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.delete(model_id=1, id=1)
        """
        self._delete_multiple(
            resource_path=interpolate_and_url_encode(self._RESOURCE_PATH, model_id),
            identifiers=IdentifierSequence.load(ids=id),
            wrap_ids=True,
        )

    def update_thumbnail(self, model_id: int, revision_id: int, file_id: int) -> None:
        """`Update a revision thumbnail. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/updateThumbnail>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            file_id (int): Id of the thumbnail file in the Files API.

        Example:

            Update revision thumbnail::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.update_thumbnail(model_id=1, revision_id=1, file_id=1)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/thumbnail", model_id, revision_id)
        body = {"fileId": file_id}
        self._post(resource_path, json=body)

    def list_nodes(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        depth: int | None = None,
        sort_by_node_id: bool = False,
        partitions: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ThreeDNodeList:
        """`Retrieves a list of nodes from the hierarchy in the 3D Model. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/get3DNodes>`_

        You can also request a specific subtree with the 'nodeId' query parameter and limit the depth of
        the resulting subtree with the 'depth' query parameter.

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int | None): ID of the root node of the subtree you request (default is the root node).
            depth (int | None): Get sub nodes up to this many levels below the specified node. Depth 0 is the root node.
            sort_by_node_id (bool): Returns the nodes in `nodeId` order.
            partitions (int | None): The result is retrieved in this many parts in parallel. Requires `sort_by_node_id` to be set to `true`.
            limit (int | None): Maximum number of nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDNodeList: The list of 3d nodes.

        Example:

            List nodes from the hierarchy in the 3d model::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.list_nodes(model_id=1, revision_id=1, limit=10)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            list_cls=ThreeDNodeList,
            resource_cls=ThreeDNode,
            resource_path=resource_path,
            method="GET",
            limit=limit,
            filter={"depth": depth, "nodeId": node_id},
            partitions=partitions,
            other_params={"sortByNodeId": sort_by_node_id},
        )

    def filter_nodes(
        self,
        model_id: int,
        revision_id: int,
        properties: dict[str, dict[str, SequenceNotStr[str]]] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        partitions: int | None = None,
    ) -> ThreeDNodeList:
        """`List nodes in a revision, filtered by node property values. <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/filter3DNodes>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            properties (dict[str, dict[str, SequenceNotStr[str]]] | None): Properties for filtering. The object contains one or more category. Each category references one or more properties. Each property is associated with a list of values. For a node to satisfy the filter, it must, for each category/property in the filter, contain the category+property combination with a value that is contained within the corresponding list in the filter.
            limit (int | None): Maximum number of nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            partitions (int | None): The result is retrieved in this many parts in parallel. Requires `sort_by_node_id` to be set to `true`.

        Returns:
            ThreeDNodeList: The list of 3d nodes.

        Example:

            Filter nodes from the hierarchy in the 3d model that have one of the values "AB76", "AB77" or "AB78" for property PDMS/Area AND that also have one of the values "PIPE", "BEND" or "PIPESUP" for the property PDMS/Type.

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.filter_nodes(model_id=1, revision_id=1, properties={ "PDMS": { "Area": ["AB76", "AB77", "AB78"], "Type": ["PIPE", "BEND", "PIPESUP"] } }, limit=10)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            list_cls=ThreeDNodeList,
            resource_cls=ThreeDNode,
            resource_path=resource_path,
            method="POST",
            limit=limit,
            filter={"properties": properties},
            partitions=partitions,
        )

    def list_ancestor_nodes(
        self, model_id: int, revision_id: int, node_id: int | None = None, limit: int | None = DEFAULT_LIMIT_READ
    ) -> ThreeDNodeList:
        """`Retrieves a list of ancestor nodes of a given node, including itself, in the hierarchy of the 3D model <https://developer.cognite.com/api#tag/3D-Model-Revisions/operation/get3DNodeAncestors>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int | None): ID of the node to get the ancestors of.
            limit (int | None): Maximum number of nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDNodeList: The list of 3d nodes.

        Example:

            Get a list of ancestor nodes of a given node::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.revisions.list_ancestor_nodes(model_id=1, revision_id=1, node_id=5, limit=10)
        """
        resource_path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/nodes", model_id, revision_id)
        return self._list(
            list_cls=ThreeDNodeList,
            resource_cls=ThreeDNode,
            resource_path=resource_path,
            method="GET",
            limit=limit,
            filter={"nodeId": node_id},
        )


class ThreeDFilesAPI(APIClient):
    _RESOURCE_PATH = "/3d/files"

    def retrieve(self, id: int) -> bytes:
        """`Retrieve the contents of a 3d file by id. <https://developer.cognite.com/api#tag/3D-Files/operation/get3DFile>`_

        Args:
            id (int): The id of the file to retrieve.

        Returns:
            bytes: The contents of the file.

        Example:

            Retrieve the contents of a 3d file by id::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.files.retrieve(1)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH + "/{}", id)
        return self._get(path).content


class ThreeDAssetMappingAPI(APIClient):
    _RESOURCE_PATH = "/3d/models/{}/revisions/{}/mappings"

    def list(
        self,
        model_id: int,
        revision_id: int,
        node_id: int | None = None,
        asset_id: int | None = None,
        intersects_bounding_box: BoundingBox3D | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ThreeDAssetMappingList:
        """`List 3D node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/get3DMappings>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            node_id (int | None): List only asset mappings associated with this node.
            asset_id (int | None): List only asset mappings associated with this asset.
            intersects_bounding_box (BoundingBox3D | None): If given, only return asset mappings for assets whose bounding box intersects with the given bounding box.
            limit (int | None): Maximum number of asset mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ThreeDAssetMappingList: The list of asset mappings.

        Example:

            List 3d node asset mappings:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.three_d.asset_mappings.list(model_id=1, revision_id=1)

            List 3d node asset mappings for assets whose bounding box intersects with a given bounding box:

                >>> from cognite.client.data_classes import BoundingBox3D
                >>> bbox = BoundingBox3D(min=[0.0, 0.0, 0.0], max=[1.0, 1.0, 1.0])
                >>> res = client.three_d.asset_mappings.list(
                ...     model_id=1, revision_id=1, intersects_bounding_box=bbox)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        flt: dict[str, str | int | None] = {"nodeId": node_id, "assetId": asset_id}
        if intersects_bounding_box:
            flt["intersectsBoundingBox"] = _json.dumps(intersects_bounding_box)
        return self._list(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            resource_path=path,
            method="GET",
            filter=flt,
            limit=limit,
        )

    @overload
    def create(
        self, model_id: int, revision_id: int, asset_mapping: ThreeDAssetMapping | ThreeDAssetMappingWrite
    ) -> ThreeDAssetMapping: ...

    @overload
    def create(
        self,
        model_id: int,
        revision_id: int,
        asset_mapping: Sequence[ThreeDAssetMapping] | Sequence[ThreeDAssetMappingWrite],
    ) -> ThreeDAssetMappingList: ...

    def create(
        self,
        model_id: int,
        revision_id: int,
        asset_mapping: ThreeDAssetMapping
        | ThreeDAssetMappingWrite
        | Sequence[ThreeDAssetMapping]
        | Sequence[ThreeDAssetMappingWrite],
    ) -> ThreeDAssetMapping | ThreeDAssetMappingList:
        """`Create 3d node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/create3DMappings>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            asset_mapping (ThreeDAssetMapping | ThreeDAssetMappingWrite | Sequence[ThreeDAssetMapping] | Sequence[ThreeDAssetMappingWrite]): The asset mapping(s) to create.

        Returns:
            ThreeDAssetMapping | ThreeDAssetMappingList: The created asset mapping(s).

        Example:

            Create new 3d node asset mapping::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ThreeDAssetMappingWrite
                >>> my_mapping = ThreeDAssetMappingWrite(node_id=1, asset_id=1)
                >>> client = CogniteClient()
                >>> res = client.three_d.asset_mappings.create(model_id=1, revision_id=1, asset_mapping=my_mapping)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        return self._create_multiple(
            list_cls=ThreeDAssetMappingList,
            resource_cls=ThreeDAssetMapping,
            resource_path=path,
            items=asset_mapping,
            input_resource_cls=ThreeDAssetMappingWrite,
        )

    def delete(
        self, model_id: int, revision_id: int, asset_mapping: ThreeDAssetMapping | Sequence[ThreeDAssetMapping]
    ) -> None:
        """`Delete 3d node asset mappings. <https://developer.cognite.com/api#tag/3D-Asset-Mapping/operation/delete3DMappings>`_

        Args:
            model_id (int): Id of the model.
            revision_id (int): Id of the revision.
            asset_mapping (ThreeDAssetMapping | Sequence[ThreeDAssetMapping]): The asset mapping(s) to delete.

        Example:

            Delete 3d node asset mapping::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> mapping_to_delete = client.three_d.asset_mappings.list(model_id=1, revision_id=1)[0]
                >>> res = client.three_d.asset_mappings.delete(model_id=1, revision_id=1, asset_mapping=mapping_to_delete)
        """
        path = interpolate_and_url_encode(self._RESOURCE_PATH, model_id, revision_id)
        assert_type(asset_mapping, "asset_mapping", [Sequence, ThreeDAssetMapping])
        if isinstance(asset_mapping, ThreeDAssetMapping):
            asset_mapping = [asset_mapping]
        chunks = split_into_chunks(
            [ThreeDAssetMapping(a.node_id, a.asset_id).dump(camel_case=True) for a in asset_mapping], self._DELETE_LIMIT
        )
        tasks = [{"url_path": path + "/delete", "json": {"items": chunk}} for chunk in chunks]
        summary = execute_tasks(self._post, tasks, self._config.max_workers)
        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=unpack_items_in_payload,
            task_list_element_unwrap_fn=lambda el: ThreeDAssetMapping._load(el),
            str_format_element_fn=lambda el: (el.asset_id, el.node_id),
        )
