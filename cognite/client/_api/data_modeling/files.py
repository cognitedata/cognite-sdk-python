from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn, overload


from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile
from cognite.client.data_classes.data_modeling.ids import NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, Node, NodeList
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.data_classes.filters import Filter

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig

COGNITE_FILE_VIEW_ID = CogniteFile.get_source()


def _resolve_source(source: View | ViewId | tuple[str, str, str]) -> tuple[list[ViewId], bool]:
    match source:
        case ViewId():
            source_as_id = source
        case View():
            source_as_id = source.as_id()
        case [str(), str(), str()]:
            source_as_id = ViewId(*source)
        case _:
            raise TypeError(f"Expected View, ViewId, or a (space, external_id, version) tuple, got {type(source)}")

    if source_as_id == COGNITE_FILE_VIEW_ID:
        return [source_as_id], False

    # User has passed a custom source, we include CogniteFile source to guarantee only file nodes
    # are returned. We will later strip them (hence the 'True' flag) to avoid returning nodes with
    # properties from multiple sources as they are very annoying to work with in the SDK.
    return [source_as_id, COGNITE_FILE_VIEW_ID], True


class DataModelingFilesAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._files_api = cognite_client.files

    async def retrieve_download_urls(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def download(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def download_to_path(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def download_bytes(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload_bytes(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload_content(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def upload_content_bytes(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    async def __call__(self) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    @overload
    async def retrieve(
        self,
        nodes: NodeId | tuple[str, str],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
    ) -> Node | None: ...

    @overload
    async def retrieve(
        self,
        nodes: Sequence[NodeId | tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
    ) -> NodeList[Node]: ...

    async def retrieve(
        self,
        nodes: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
    ) -> Node | NodeList[Node] | None:
        """`Retrieve one or more files by instance ID. <https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances>`_

        Only nodes that are files (i.e. have data in the CogniteFile view) will be returned.
        If a single instance ID is requested and it is not found, ``None`` is returned.

        Args:
            nodes (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Single instance ID or a list of instance IDs.
            source (View | ViewId | tuple[str, str, str]): The view to fetch properties from. Defaults to CogniteFile.

        Returns:
            Node | NodeList[Node] | None: A single ``Node`` (or ``None`` if not found) when given a single identifier, or a ``NodeList`` when given a sequence.

        Examples:

            Retrieve a single file by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.files.retrieve(NodeId("my-space", "my-file"))

            Using a tuple shorthand:

                >>> res = client.data_modeling.files.retrieve(("my-space", "my-file"))

            Retrieve multiple files nodes:

                >>> res = client.data_modeling.files.retrieve(
                ...     [("my-space", "file-1"), ("my-space", "file-2")]
                ... )

            Fetch properties from a custom view (note, only files will be returned):

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> res = client.data_modeling.files.retrieve(
                ...     NodeId("my-space", "my-file"),
                ...     source=ViewId("my-space", "MyFileExtension", "v1"),
                ... )
        """
        sources, strip = _resolve_source(source)
        result = await self._cognite_client.data_modeling.instances.retrieve_nodes(nodes=nodes, sources=sources)  # type: ignore[arg-type]
        if strip and result:
            for node in [result] if isinstance(result, Node) else result:
                node.drop_source(COGNITE_FILE_VIEW_ID)
        return result

    async def list(
        self,
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
        space: str | Sequence[str] | None = None,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> NodeList[Node]:
        """`List files nodes. <https://api-docs.cognite.com/20230101/tag/Instances/operation/listInstances>`_

        Only file nodes will be returned, regardless of the source passed.

        Args:
            source (View | ViewId | tuple[str, str, str]): The view to fetch properties from. Defaults to CogniteFile.
            space (str | Sequence[str] | None): Restrict results to this space (or list of spaces).
            sort (Sequence[InstanceSort | dict] | InstanceSort | dict | None): Sort order for the results.
            filter (Filter | dict[str, Any] | None): Advanced filter to apply. See :class:`~cognite.client.data_classes.filters`.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            NodeList[Node]: The matching files.

        Examples:

            List a few files:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.files.list(limit=5)

            List all files in a specific space:

                >>> res = client.data_modeling.files.list(space="my-space", limit=None)

            Fetch properties from a custom view (note, only files will be returned), and
            apply a custom filter on the file name:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes import filters
                >>> view_id = ViewId("my-space", "MyFileExtension", "v1")
                >>> res = client.data_modeling.files.list(
                ...     source=view_id,
                ...     filter=filters.Prefix(view_id.as_property_ref("name"), "report"),
                ...     limit=None,
                ... )
        """
        sources, strip = _resolve_source(source)
        results = await self._cognite_client.data_modeling.instances.list(  # type: ignore[call-overload]
            instance_type="node",
            sources=sources,
            space=space,
            sort=sort,
            filter=filter,
            limit=limit,
        )
        if strip:
            for node in results:
                node.drop_source(COGNITE_FILE_VIEW_ID)
        return results
