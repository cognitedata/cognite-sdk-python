"""
===============================================================================
88cee191bc62e71aa597f61192fdab2f
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.limits import Limit
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncLimitsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve(self, id: str) -> Limit | None:
        """
        `Retrieve a limit value by its id. <https://api-docs.cognite.com/20230101-alpha/tag/Limits/operation/fetchLimitById/>`_

        Retrieves a limit value by its `limitId`.

        Args:
            id (str): Limit ID to retrieve.
                Limits are identified by an id containing the service name and a service-scoped limit name.
                For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
                Service and limit names are always in `lower_snake_case`.

        Returns:
            Limit | None: The requested limit, or `None` if not found.

        Examples:

            Retrieve a single limit by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.limits.retrieve(id="atlas.monthly_ai_tokens")
        """
        return run_sync(self.__async_client.limits.retrieve(id=id))
