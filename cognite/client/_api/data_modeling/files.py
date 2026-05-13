from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import FileMetadata, FileMetadataList
from cognite.client.data_classes.data_modeling import NodeId

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client._api.files import FilesAPI
    from cognite.client.config import ClientConfig


class DataModelingFilesAPI(APIClient):
    """Access files via Data Modeling instance IDs.

    This API mirrors a subset of client.files but restricts identifiers to
    instance_id (NodeId) only, making the DM-native workflow explicit.
    """

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._files_api: FilesAPI = cognite_client.files

    @overload
    async def retrieve(self, instance_id: NodeId | tuple[str, str]) -> FileMetadata | None: ...

    @overload
    async def retrieve(
        self, instance_id: Sequence[NodeId | tuple[str, str]], *, ignore_unknown_ids: bool = ...
    ) -> FileMetadataList: ...

    async def retrieve(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        ignore_unknown_ids: bool = False,
    ) -> FileMetadata | FileMetadataList | None:
        """`Retrieve one or more file metadatas by instance ID. <https://api-docs.cognite.com/20230101/tag/Files/operation/getFileByInternalId>`_

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Single instance ID or a list of instance IDs. Each identifier can be a :class:`~cognite.client.data_classes.data_modeling.NodeId` or a ``(space, external_id)`` tuple.
            ignore_unknown_ids (bool): Ignore IDs that are not found rather than throw an exception. Only used when a sequence is passed.

        Returns:
            FileMetadata | FileMetadataList | None: A single ``FileMetadata`` (or ``None`` if not found) when given a single identifier, or a ``FileMetadataList`` when given a sequence.

        Examples:

            Get a single file by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.files.retrieve(NodeId("my-space", "my-file"))

            Using a tuple shorthand:

                >>> res = client.data_modeling.files.retrieve(("my-space", "my-file"))

            Get multiple files:

                >>> res = client.data_modeling.files.retrieve(
                ...     [NodeId("my-space", "file-1"), ("my-space", "file-2")]
                ... )
        """
        if isinstance(instance_id, (NodeId, tuple)):
            return await self._files_api.retrieve(instance_id=as_node_id(instance_id))
        return await self._files_api.retrieve_multiple(
            instance_ids=as_node_ids(instance_id),
            ignore_unknown_ids=ignore_unknown_ids,
        )

    async def download(
        self,
        directory: str | Path,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        keep_directory_structure: bool = False,
        resolve_duplicate_file_names: bool = False,
    ) -> None:
        """`Download files by instance ID. <https://api-docs.cognite.com/20230101/tag/Files/operation/downloadLinks>`_

        Streams all files to disk, never keeping more than 2MB in memory per worker.

        Args:
            directory (str | Path): Directory to download the file(s) to.
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Instance ID or list of instance IDs.
            keep_directory_structure (bool): Whether to keep the directory hierarchy from CDF, creating subdirectories as needed.
            resolve_duplicate_file_names (bool): Whether to resolve duplicate file names by appending a number.

        Examples:

            Download files by instance ID into directory 'my_directory':

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.files.download(
                ...     directory="my_directory",
                ...     instance_id=[NodeId("my-space", "file-1"), ("my-space", "file-2")],
                ... )
        """
        await self._files_api.download(
            directory=directory,
            instance_id=normalize_to_node_ids(instance_id),
            keep_directory_structure=keep_directory_structure,
            resolve_duplicate_file_names=resolve_duplicate_file_names,
        )

    async def download_to_path(self, path: Path | str, instance_id: NodeId | tuple[str, str]) -> None:
        """Download a file to a specific path by instance ID.

        Args:
            path (Path | str): Download to this path.
            instance_id (NodeId | tuple[str, str]): Instance ID of the file to download.

        Examples:

            Download a file by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> client.data_modeling.files.download_to_path(
                ...     "~/mydir/my_file.txt", NodeId("my-space", "my-file")
                ... )
        """
        await self._files_api.download_to_path(path=path, instance_id=as_node_id(instance_id))

    async def download_bytes(self, instance_id: NodeId | tuple[str, str]) -> bytes:
        """Download a file as bytes by instance ID.

        Args:
            instance_id (NodeId | tuple[str, str]): Instance ID of the file.

        Returns:
            bytes: The file content.

        Examples:

            Download a file's content into memory by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> content = client.data_modeling.files.download_bytes(NodeId("my-space", "my-file"))
        """
        return await self._files_api.download_bytes(instance_id=as_node_id(instance_id))

    async def upload_content(self, path: Path | str, instance_id: NodeId | tuple[str, str]) -> FileMetadata:
        """`Upload content to an existing file node by instance ID. <https://api-docs.cognite.com/20230101/tag/Files/operation/getUploadLink>`_

        Args:
            path (Path | str): Path to the file to upload.
            instance_id (NodeId | tuple[str, str]): Instance ID of the file node.

        Returns:
            FileMetadata: The updated file metadata.

        Examples:

            Upload file content by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.files.upload_content(
                ...     "/path/to/file.txt", NodeId("my-space", "my-file")
                ... )
        """
        return await self._files_api.upload_content(path=path, instance_id=as_node_id(instance_id))

    async def upload_content_bytes(self, content: str | bytes | BinaryIO, instance_id: NodeId | tuple[str, str]) -> FileMetadata:
        """Upload bytes or string content to an existing file node by instance ID.

        Note that the maximum file size is 5GiB.

        Args:
            content (str | bytes | BinaryIO): The content to upload.
            instance_id (NodeId | tuple[str, str]): Instance ID of the file node.

        Returns:
            FileMetadata: The updated file metadata.

        Examples:

            Upload bytes to a file by instance ID:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.data_modeling import NodeId
                >>> client = CogniteClient()
                >>> res = client.data_modeling.files.upload_content_bytes(
                ...     b"some content", NodeId("my-space", "my-file")
                ... )
        """
        return await self._files_api.upload_content_bytes(content=content, instance_id=as_node_id(instance_id))
