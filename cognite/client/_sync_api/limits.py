"""
===============================================================================
6b64355411f69544440ffc5607c18fb1
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.limits import Limit, LimitList
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
from cognite.client.data_classes.filters import Prefix


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

    def list(self, filter: Prefix | None = None, limit: int | None = DEFAULT_LIMIT_READ) -> LimitList:
        """
        `List all limit values <https://api-docs.cognite.com/20230101-alpha/tag/Limits/operation/listLimits/>`_

        Retrieves all limit values for a specific project. Optionally filter by limit ID prefix using a `Prefix` filter.

        Args:
            filter (Prefix | None): Optional `Prefix` filter to apply on the `limitId` property (only `Prefix` filters are supported).
            limit (int | None): Maximum number of limits to return. Defaults to 25. Set to None or -1 to return all limits

        Returns:
            LimitList: List of all limit values in the project.

        Examples:

            List all limits:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> limits = client.limits.list(limit=None)

            List limits filtered by prefix (e.g., all limits for the 'atlas' service):

                >>> from cognite.client.data_classes.filters import Prefix
                >>> prefix_filter = Prefix("limitId", "atlas.")
                >>> limits = client.limits.list(filter=prefix_filter)
        """
        return run_sync(self.__async_client.limits.list(filter=filter, limit=limit))
