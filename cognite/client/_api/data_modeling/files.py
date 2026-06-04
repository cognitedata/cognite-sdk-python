from __future__ import annotations

import asyncio
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.config import global_config
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile
from cognite.client.data_classes.data_modeling.ids import NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, Node, NodeApply, NodeList
from cognite.client.data_classes.data_modeling.views import View
from cognite.client.data_classes.files import FileMetadata
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteFileUploadError, CogniteNotFoundError
from cognite.client.utils._retry import Backoff
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import BinaryIO

    from cognite.client import AsyncCogniteClient
    from cognite.client._api.data_modeling.instances import InstancesAPI
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

    @cached_property
    def _instances_api(self) -> InstancesAPI:
        return self._cognite_client.data_modeling.instances

    async def retrieve_download_urls(
        self,
        node_ids: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        extended_expiration: bool = False,
    ) -> dict[NodeId, str]:
        """`Get download URLs for one or more files by instance ID <https://api-docs.cognite.com/20230101/tag/Files/operation/downloadLinks>`_.

        Note:
            If you pass instance IDs as tuple(s), the returned mapping will always use NodeIds as keys.

        Args:
            node_ids (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Instance ID or list of instance IDs.
            extended_expiration (bool): Extend expiration time of download URL to 1 hour. Defaults to False.

        Returns:
            dict[NodeId, str]: Dictionary mapping each instance ID to its download URL.

        Examples:

            Get a download URL for a single file:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> urls = client.data_modeling.files.retrieve_download_urls(
                ...     NodeId("my-space", "my-file")
                ... )

            Get download URLs for multiple files:

                >>> urls = client.data_modeling.files.retrieve_download_urls(
                ...     [("my-space", "file-1"), ("my-space", "file-2")]
                ... )
        """
        return await self._files_api.retrieve_download_urls(
            instance_id=node_ids,
            extended_expiration=extended_expiration,
        )

    async def download(
        self,
        directory: Path,
        node_ids: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        keep_directory_structure: bool = False,
        resolve_duplicate_file_names: bool = False,
    ) -> None:
        """`Download files by instance ID <https://api-docs.cognite.com/20230101/tag/Files/operation/downloadLinks>`_.

        Streams all files to disk one chunk at a time. By default, chunk size is dynamic to maximize
        throughput; set ``global_config.file_download_chunk_size`` (bytes) to enforce a fixed size.

        Args:
            directory (Path): Directory to download the file(s) to.
            node_ids (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Instance ID or list of instance IDs.
            keep_directory_structure (bool): Whether to keep the directory hierarchy from CDF, creating subdirectories as needed.
            resolve_duplicate_file_names (bool): Whether to resolve duplicate file names by appending a number.

        Examples:

            Download files by instance ID into directory 'my_directory':

                >>> from pathlib import Path
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.files.download(
                ...     directory=Path("my_directory"),
                ...     node_ids=[NodeId("my-space", "file-1"), NodeId("my-space", "file-2")],
                ... )
        """
        await self._files_api.download(
            directory=directory,
            instance_id=node_ids,
            keep_directory_structure=keep_directory_structure,
            resolve_duplicate_file_names=resolve_duplicate_file_names,
        )

    async def download_to_path(self, path: Path, node_id: NodeId | tuple[str, str]) -> None:
        """Download a file to a specific path by instance ID.

        Args:
            path (Path): Download to this path.
            node_id (NodeId | tuple[str, str]): Instance ID of the file to download.

        Examples:

            Download a file by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.files.download_to_path(
                ...     Path("~/mydir/my_file.txt"), NodeId("my-space", "my-file")
                ... )
        """
        await self._files_api.download_to_path(path=path, instance_id=node_id)

    async def download_bytes(self, node_id: NodeId | tuple[str, str]) -> bytes:
        """Download a file as bytes by instance ID.

        Args:
            node_id (NodeId | tuple[str, str]): Instance ID of the file.

        Returns:
            bytes: The file content.

        Examples:

            Download a file's content into memory by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> content = client.data_modeling.files.download_bytes(NodeId("my-space", "my-file"))
        """
        return await self._files_api.download_bytes(instance_id=node_id)

    async def upload(self, path: Path, node: NodeApply) -> None:
        """`Create a file node and upload content in one step <https://api-docs.cognite.com/20230101/tag/Files/operation/getUploadLink>`_.

        The node is created (or updated) via ``instances.apply``, then the file content is uploaded.

        Args:
            path (Path): Path to the file to upload.
            node (NodeApply): The file node to apply before uploading.

        Examples:

            Create a file node and upload content:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
                >>> client = CogniteClient()
                >>> file_name = "Quarterly-Report.pdf"
                >>> client.data_modeling.files.upload(
                ...     Path(file_name),
                ...     CogniteFileApply(
                ...         space="my-space",
                ...         external_id="my-file",
                ...         name=file_name,
                ...         mime_type="application/pdf",
                ...     ),
                ... )
        """
        await self._instances_api.apply(nodes=node)
        node_id = node.as_id()
        await self._upload_to_newly_created_file_node(
            node_id, upload_fn=lambda: self._files_api.upload_content(path=path, instance_id=node_id)
        )

    async def upload_bytes(self, content: str | bytes | BinaryIO, node: NodeApply) -> None:
        """Create a file node and upload in-memory content in one step.

        The node is created (or updated) via ``instances.apply``, then the content is uploaded.

        Args:
            content (str | bytes | BinaryIO): The content to upload.
            node (NodeApply): The file node to apply before uploading.

        Examples:

            Create a file node and upload bytes:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply
                >>> client = CogniteClient()
                >>> client.data_modeling.files.upload_bytes(
                ...     b"some important notes",
                ...     CogniteFileApply(
                ...         space="my-space",
                ...         external_id="my-file",
                ...         name="notes.txt",
                ...         mime_type="text/plain",
                ...     ),
                ... )
        """
        await self._instances_api.apply(nodes=node)
        node_id = node.as_id()
        await self._upload_to_newly_created_file_node(
            node_id, upload_fn=lambda: self._files_api.upload_content_bytes(content=content, instance_id=node_id)
        )

    async def _upload_to_newly_created_file_node(
        self, node_id: NodeId, upload_fn: Callable[[], Awaitable[FileMetadata]]
    ) -> None:
        try:
            await upload_fn()
            return  # we do not want to return legacy FileMetadata
        except CogniteNotFoundError as err:
            # If a newly created node is not found, we first need to verify that the node is actually a file node.
            # The retrieve endpoint is immediately consistent, so we check:
            if await self.retrieve(node_id, source=COGNITE_FILE_VIEW_ID) is None:
                raise CogniteFileUploadError(
                    f"The file upload failed because the target {node_id=} is not a file node. "
                    "Make sure to write through CogniteFile or an extension of it.",
                    code=err.code,
                ) from err

            # We now know that the newly created node -is- a file node, we are just experiencing propagation delays to the
            # backend file service. We should retry with backoff settings (set by the user):
            await self._upload_with_retry(node_id, upload_fn, err)

    async def _upload_with_retry(
        self,
        node_id: NodeId,
        upload_fn: Callable[[], Awaitable[FileMetadata]],
        latest_error: CogniteNotFoundError,
    ) -> None:
        backoff = Backoff(max_wait=global_config.max_retry_backoff)
        for _ in range(global_config.max_retries):
            await asyncio.sleep(next(backoff))  # we sleep immediately because we have already tried uploading
            try:
                await upload_fn()
                return
            except CogniteNotFoundError as err:
                latest_error = err

        total_attempts = global_config.max_retries + 1
        raise CogniteFileUploadError(
            f"The file upload failed to {node_id=} after {total_attempts} attempt(s): "
            "backend file propagation is taking longer than expected. "
            "Ensure no one has deleted the file node in the meantime, then try again shortly.",
            code=latest_error.code,
        ) from latest_error

    async def upload_content(self, path: Path, node_id: NodeId | tuple[str, str]) -> None:
        """`Upload content to an existing file node by instance ID <https://api-docs.cognite.com/20230101/tag/Files/operation/getUploadLink>`_.

        Args:
            path (Path): Path to the file to upload.
            node_id (NodeId | tuple[str, str]): Instance ID of the file node.

        Examples:

            Upload file content by instance ID:

                >>> from pathlib import Path
                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.files.upload_content(
                ...     Path("/path/to/file.txt"), NodeId("my-space", "my-file")
                ... )
        """
        node_id = NodeId.load(node_id)
        await self._upload_to_existing_node(
            node_id, upload_fn=lambda: self._files_api.upload_content(path=path, instance_id=node_id)
        )

    async def _upload_to_existing_node(self, node_id: NodeId, upload_fn: Callable[[], Awaitable[FileMetadata]]) -> None:
        try:
            await upload_fn()
            return  # we do not want to return legacy FileMetadata
        except CogniteNotFoundError as err:
            # We did not create the node before upload, so we don't know if the node even exists. We first
            # need to verify that the node is actually a file node, so we use the retrieve endpoint which
            # is immediately consistent to check:
            if await self.retrieve(node_id, source=COGNITE_FILE_VIEW_ID) is None:
                if await self._instances_api.retrieve_nodes(nodes=node_id, sources=None):
                    err_msg = (
                        f"The file upload failed because the target {node_id=} exists but is not a file node. "
                        "Make sure to write through CogniteFile or an extension of it."
                    )
                else:
                    err_msg = f"The file upload failed because the target {node_id=} does not exist."
                raise CogniteFileUploadError(err_msg, code=err.code) from err

            # We now know that the existing node -is- a file node, we are just experiencing propagation delays to the
            # backend file service. We should retry with backoff settings (set by the user):
            await self._upload_with_retry(node_id, upload_fn, err)

    async def upload_content_bytes(
        self,
        content: str | bytes | BinaryIO,
        node_id: NodeId | tuple[str, str],
    ) -> None:
        """Upload bytes or string content to an existing file node by instance ID.

        Args:
            content (str | bytes | BinaryIO): The content to upload.
            node_id (NodeId | tuple[str, str]): Instance ID of the file node.

        Examples:

            Upload bytes to an existing file node by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.files.upload_content_bytes(
                ...     b"some content", NodeId("my-space", "my-file")
                ... )
        """
        node_id = NodeId.load(node_id)
        await self._upload_to_existing_node(
            node_id, upload_fn=lambda: self._files_api.upload_content_bytes(content=content, instance_id=node_id)
        )

    async def __call__(self) -> NoReturn:
        raise NotImplementedError("This method is not implemented yet!")

    @overload
    async def retrieve(
        self,
        node_ids: NodeId | tuple[str, str],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
    ) -> Node | None: ...

    @overload
    async def retrieve(
        self,
        node_ids: Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
    ) -> NodeList[Node]: ...

    async def retrieve(
        self,
        node_ids: NodeId | tuple[str, str] | Sequence[NodeId] | Sequence[tuple[str, str]],
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
    ) -> Node | NodeList[Node] | None:
        """`Retrieve one or more files by instance ID <https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances>`_.

        Only nodes that are files (i.e. have data in the CogniteFile view) will be returned.
        If a single instance ID is requested and it is not found, ``None`` is returned.

        Args:
            node_ids (NodeId | tuple[str, str] | Sequence[NodeId] | Sequence[tuple[str, str]]): Single instance ID or a list of instance IDs.
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

            Retrieve multiple file nodes:

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
        result = await self._instances_api.retrieve_nodes(nodes=node_ids, sources=sources)
        if strip and result:
            for node in [result] if isinstance(result, Node) else result:
                node.drop_source(COGNITE_FILE_VIEW_ID)
        return result

    async def list(
        self,
        *,
        source: View | ViewId | tuple[str, str, str] = COGNITE_FILE_VIEW_ID,
        space: str | SequenceNotStr[str] | None = None,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> NodeList[Node]:
        """`List file nodes <https://api-docs.cognite.com/20230101/tag/Instances/operation/listInstances>`_.

        Only file nodes will be returned, regardless of the source passed.

        Args:
            source (View | ViewId | tuple[str, str, str]): The view to fetch properties from. Defaults to CogniteFile.
            space (str | SequenceNotStr[str] | None): Restrict results to this space (or list of spaces).
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
        results = await self._instances_api.list(
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
