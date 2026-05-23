"""
===============================================================================
f9df59e18a97e4f2f253e8e4f1170187
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.data_modeling.instances import InstanceSort, Node, NodeList
from cognite.client.data_classes.filters import Filter
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from typing import BinaryIO
from cognite.client._api.data_modeling.instances import Source


class SyncDataModelingFilesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def retrieve(self, instance_id: NodeId | tuple[str, str], *, source: Source | None = None) -> Node | None: ...

    @overload
    def retrieve(
        self, instance_id: Sequence[NodeId | tuple[str, str]], *, source: Source | None = None
    ) -> NodeList[Node]: ...

    def retrieve(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        source: Source | None = None,
    ) -> Node | NodeList[Node] | None:
        """
        `Retrieve one or more file nodes by instance ID. <https://api-docs.cognite.com/20230101/tag/Instances/operation/byExternalIdsInstances>`_

        Properties are fetched from the given source view. The source must implement CogniteFile
        (``ViewId("cdf_cdm", "CogniteFile", "v1")``). Defaults to CogniteFile itself.

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Single instance ID or a list of instance IDs.
            source (Source | None): View to retrieve properties from. Must implement CogniteFile. Defaults to ``ViewId("cdf_cdm", "CogniteFile", "v1")``.

        Returns:
            Node | NodeList[Node] | None: A single ``Node`` (or ``None`` if not found) when given a single identifier, or a ``NodeList`` when given a sequence.

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

            Get with a custom source that implements CogniteFile:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> res = client.data_modeling.files.retrieve(
                ...     NodeId("my-space", "my-file"),
                ...     source=ViewId("my-space", "MyFile", "v1"),
                ... )
        """
        return run_sync(self.__async_client.data_modeling.files.retrieve(instance_id=instance_id, source=source))

    def list(
        self,
        *,
        source: Source | None = None,
        space: str | SequenceNotStr[str] | None = None,
        sort: Sequence[InstanceSort | dict] | InstanceSort | dict | None = None,
        filter: Filter | dict[str, Any] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> NodeList[Node]:
        """
        `List file nodes from Data Modeling. <https://api-docs.cognite.com/20230101/tag/Instances/operation/listInstances>`_

        Args:
            source (Source | None): View to retrieve properties from. Must implement CogniteFile. Defaults to ``ViewId("cdf_cdm", "CogniteFile", "v1")``.
            space (str | SequenceNotStr[str] | None): Restrict results to this space (or list of spaces).
            sort (Sequence[InstanceSort | dict] | InstanceSort | dict | None): Sort order for the results.
            filter (Filter | dict[str, Any] | None): Advanced filter to apply. See :class:`~cognite.client.data_classes.filters`.
            limit (int | None): Maximum number of nodes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            NodeList[Node]: The matching file nodes.

        Examples:

            List all CogniteFile nodes:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.files.list()

            List nodes in a specific space:

                >>> res = client.data_modeling.files.list(space="my-space")

            List with a custom source and filter:

                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes import filters
                >>> res = client.data_modeling.files.list(
                ...     source=ViewId("my-space", "MyFile", "v1"),
                ...     filter=filters.Prefix(["cdf_cdm", "CogniteFile", "v1", "name"], "report"),
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.files.list(
                source=source, space=space, sort=sort, filter=filter, limit=limit
            )
        )

    def retrieve_download_urls(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        *,
        extended_expiration: bool = False,
    ) -> dict[NodeId, str]:
        """
        `Get download URLs for one or more files by instance ID. <https://api-docs.cognite.com/20230101/tag/Files/operation/downloadLinks>`_

        Args:
            instance_id (NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]]): Instance ID or list of instance IDs.
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
                ...     [NodeId("my-space", "file-1"), ("my-space", "file-2")]
                ... )
        """
        return run_sync(
            self.__async_client.data_modeling.files.retrieve_download_urls(
                instance_id=instance_id, extended_expiration=extended_expiration
            )
        )

    def download(
        self,
        directory: str | Path,
        instance_id: NodeId | tuple[str, str] | Sequence[NodeId | tuple[str, str]],
        keep_directory_structure: bool = False,
        resolve_duplicate_file_names: bool = False,
    ) -> None:
        """
        `Download files by instance ID. <https://api-docs.cognite.com/20230101/tag/Files/operation/downloadLinks>`_

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
        return run_sync(
            self.__async_client.data_modeling.files.download(
                directory=directory,
                instance_id=instance_id,
                keep_directory_structure=keep_directory_structure,
                resolve_duplicate_file_names=resolve_duplicate_file_names,
            )
        )

    def download_to_path(self, path: Path | str, instance_id: NodeId | tuple[str, str]) -> None:
        """
        Download a file to a specific path by instance ID.

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
        return run_sync(self.__async_client.data_modeling.files.download_to_path(path=path, instance_id=instance_id))

    def download_bytes(self, instance_id: NodeId | tuple[str, str]) -> bytes:
        """
        Download a file as bytes by instance ID.

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
        return run_sync(self.__async_client.data_modeling.files.download_bytes(instance_id=instance_id))

    def upload_content(self, path: Path | str, instance_id: NodeId | tuple[str, str]) -> FileMetadata:
        """
        `Upload content to an existing file node by instance ID. <https://api-docs.cognite.com/20230101/tag/Files/operation/getUploadLink>`_

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
        return run_sync(self.__async_client.data_modeling.files.upload_content(path=path, instance_id=instance_id))

    def upload_content_bytes(
        self, content: str | bytes | BinaryIO, instance_id: NodeId | tuple[str, str]
    ) -> FileMetadata:
        """
        Upload bytes or string content to an existing file node by instance ID.

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
        return run_sync(
            self.__async_client.data_modeling.files.upload_content_bytes(content=content, instance_id=instance_id)
        )
