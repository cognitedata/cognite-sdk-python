"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.streams import StreamTemplateInfoList
from cognite.client.utils._async_helpers import run_sync


class SyncStreamTemplatesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(self) -> StreamTemplateInfoList:
        """
        List stream templates.

        Lists the stream templates streams can be created from in the project, together with their limits
        and lifecycle settings. Templates with no streams provisioned are not included.

        Note:
            This endpoint is in alpha, and is called with the alpha ``cdf-version`` header.

        Returns:
            StreamTemplateInfoList: The available stream templates, sorted by name.

        Examples:

            List the stream templates available in the project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.data_modeling.streams.templates.list()
        """
        return run_sync(self.__async_client.data_modeling.streams.templates.list())
