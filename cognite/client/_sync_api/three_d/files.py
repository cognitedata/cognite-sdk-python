"""
===============================================================================
c23bdbc81be7b11616bd3dae81e7ef44
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.utils._async_helpers import run_sync


class Sync3DFilesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve(self, id: int) -> bytes:
        """
        `Retrieve the contents of a 3d file by id. <https://developer.cognite.com/api#tag/3D-Files/operation/get3DFile>`_

        Args:
            id (int): The id of the file to retrieve.

        Returns:
            bytes: The contents of the file.

        Example:

            Retrieve the contents of a 3d file by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.three_d.files.retrieve(1)
        """
        return run_sync(self.__async_client.three_d.files.retrieve(id=id))
