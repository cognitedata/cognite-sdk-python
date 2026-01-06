"""
===============================================================================
21e1e307f3b19c61d3f7188ccc5d9f5e
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.limits import LimitList
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncLimitsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve_multiple(self, ids: SequenceNotStr[str]) -> LimitList:
        """
        `Retrieve multiple limit values by their ids. <https://api-docs.cognite.com/20230101-alpha/tag/Limits/operation/listLimitsAdvanced/>`_

        Retrieves multiple limit values by their `limitId`s.

        Args:
            ids (SequenceNotStr[str]): List of limit IDs to retrieve.
                Limits are identified by an id containing the service name and a service-scoped limit name.
                For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
                Service and limit names are always in `lower_snake_case`.

        Returns:
            LimitList: List of requested limit values. Only limits that exist will be returned.

        Examples:

            Retrieve multiple limits by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.limits.retrieve_multiple(ids=["atlas.monthly_ai_tokens", "files.storage_bytes"])
        """
        return run_sync(self.__async_client.limits.retrieve_multiple(ids=ids))
