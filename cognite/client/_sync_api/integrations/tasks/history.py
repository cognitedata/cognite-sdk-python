"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.integrations.tasks import TaskHistoryList
from cognite.client.utils._async_helpers import run_sync


class SyncIntegrationTaskHistoryAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(
        self,
        external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TaskHistoryList:
        """
        `List task history <https://api-docs.cognite.com/20230101-beta/tag/Integration-Tasks/operation/get_integration_task_history>`_

        Args:
            external_id (str | None): Only return history for the integration with this external id.
            task_name (str | None): Only return history for the task with this name. Requires `external_id` to also be set.
            last_per_task (bool): Only return the latest history entry per task.
            limit (int | None): Maximum number of history entries to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Raises:
            ValueError: If `task_name` is given without `external_id`.

        Returns:
            TaskHistoryList: List of task history entries

        Examples:

            List task history for a single integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.tasks.history.list(external_id="my-integration")
        """
        return run_sync(
            self.__async_client.integrations.tasks.history.list(
                external_id=external_id, task_name=task_name, last_per_task=last_per_task, limit=limit
            )
        )
