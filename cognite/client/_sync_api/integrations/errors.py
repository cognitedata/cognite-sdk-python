"""
===============================================================================
bde27dc1b43cfa3eeb5745a5669fe8cc
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.integrations.errors import IntegrationErrorList
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncIntegrationErrorsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(
        self,
        external_id: str | None = None,
        task: str | None = None,
        min_start_time: int | None = None,
        max_end_time: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> IntegrationErrorList:
        """
        `List errors <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Errors/operation/get_integration_errors>`_

        Args:
            external_id (str | None): Only return errors for the integration with this external id.
            task (str | None): Only return errors for the task with this name. Requires `external_id` to also be set.
            min_start_time (int | None): Only return errors that started at or after this time, in milliseconds since epoch.
            max_end_time (int | None): Only return errors that ended at or before this time, in milliseconds since epoch.
            limit (int | None): Maximum number of errors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Raises:
            ValueError: If `task` is given without `external_id`.

        Returns:
            IntegrationErrorList: List of errors

        Examples:

            List errors for a single integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.errors.list(external_id="my-integration")
        """
        return run_sync(
            self.__async_client.integrations.errors.list(
                external_id=external_id,
                task=task,
                min_start_time=min_start_time,
                max_end_time=max_end_time,
                limit=limit,
            )
        )
