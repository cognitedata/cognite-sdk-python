"""
===============================================================================
c88ea700f6ec990f24800e9596b097d4
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.limits import LimitValue
from cognite.client.utils._async_helpers import run_sync


class SyncLimitsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[True]) -> LimitValue | None: ...

    @overload
    def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[False] = False) -> LimitValue: ...

    def retrieve(self, limit_id: str, ignore_unknown_ids: bool = False) -> LimitValue | None:
        """
        Retrieve a limit value by its id.

        Retrieves a limit value by its `limitId`.

        Args:
            limit_id (str): Limit ID.
                Limits are identified by an id containing the service name and a service-scoped limit name.
                For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
                Service and limit names are always in `lower_snake_case`.
            ignore_unknown_ids (bool): If True, ignore IDs that are not found rather than throw an exception.

        Returns:
            LimitValue | None: The requested limit value, or None if not found and ignore_unknown_ids is True.

        Examples:

            Get limit by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.limits.retrieve(limit_id="atlas.monthly_ai_tokens", ignore_unknown_ids=True)
        """
        return run_sync(self.__async_client.limits.retrieve(limit_id=limit_id, ignore_unknown_ids=ignore_unknown_ids))
