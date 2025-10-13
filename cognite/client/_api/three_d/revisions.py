from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import (
    ThreeDModelRevision,
    ThreeDModelRevisionList,
    ThreeDModelRevisionUpdate,
    ThreeDModelRevisionWrite,
    ThreeDNode,
    ThreeDNodeList,
)
from cognite.client.utils._auxiliary import interpolate_and_url_encode
from cognite.client.utils._identifier import IdentifierSequence, InternalId
from cognite.client.utils.useful_types import SequenceNotStr


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

            Update a revision that you have fetched. This will perform a full update of the revision:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> revision = client.three_d.revisions.retrieve(model_id=1, id=1)
                >>> revision.status = "New Status"
                >>> res = client.three_d.revisions.update(model_id=1, item=revision)

            Perform a partial update on a revision, updating the published property and adding a new field to metadata:

                >>> from cognite.client.data_classes import ThreeDModelRevisionUpdate
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
