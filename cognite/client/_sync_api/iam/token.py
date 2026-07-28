"""
===============================================================================
d7f8ea05edb54a84534b0b464e2e5645
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.utils._async_helpers import run_sync


class SyncTokenAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def inspect(self) -> TokenInspection:
        """
        Inspect a token.

        Get details about which projects it belongs to and which capabilities are granted to it.

        Returns:
            TokenInspection: The object with token inspection details.

        Example:

            Inspect token::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.token.inspect()
        """
        return run_sync(self.__async_client.iam.token.inspect())
