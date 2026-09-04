"""
===============================================================================
c13367430cb1ccec84ff53d416749174
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.integrations.tasks import SyncResult, TaskHistoryList
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncIntegrationTasksAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list_history(
        self,
        external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TaskHistoryList:
        """
        `List task history <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Tasks/operation/get_integration_task_history>`_

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
                >>> res = client.integrations.tasks.list_history(external_id="my-integration")
        """
        return run_sync(
            self.__async_client.integrations.tasks.list_history(
                external_id=external_id, task_name=task_name, last_per_task=last_per_task, limit=limit
            )
        )

    def sync(
        self,
        external_id: str,
        task_name: str | None = None,
        include_errors: bool = False,
        include_task_updates: bool = False,
        start_time: int | None = None,
        cursor: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> SyncResult:
        """
        `Sync integration history <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Tasks/operation/sync_integration_history>`_

        Incrementally fetch task history and/or errors for an integration since a previous sync call. This is more
        efficient than repeatedly listing task history/errors when polling for updates, e.g. for dashboards or
        alerting. At least one of `include_errors` and `include_task_updates` must be True.

        Args:
            external_id (str): Only return history for the integration with this external id.
            task_name (str | None): Only return history for the task with this name.
            include_errors (bool): Include errors reported since the last sync.
            include_task_updates (bool): Include task history entries reported since the last sync.
            start_time (int | None): Only return items reported at or after this time, in milliseconds since epoch. Only used on the first call, pass the returned cursor on subsequent calls instead.
            cursor (str | None): Cursor returned from a previous call to this method, to continue syncing from where you left off.
            limit (int | None): Maximum number of items to return in this page. Defaults to 25.

        Raises:
            ValueError: If both `include_errors` and `include_task_updates` are False.

        Returns:
            SyncResult: A single page of results. Inspect `more_data` to see whether you should immediately call this method again with the returned `next_cursor`, or back off before doing so.

        Examples:

            Sync task history and errors for an integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.tasks.sync(
                ...     external_id="my-integration", include_errors=True, include_task_updates=True
                ... )
                >>> while res.more_data:
                ...     res = client.integrations.tasks.sync(
                ...         external_id="my-integration",
                ...         include_errors=True,
                ...         include_task_updates=True,
                ...         cursor=res.next_cursor,
                ...     )
        """
        return run_sync(
            self.__async_client.integrations.tasks.sync(
                external_id=external_id,
                task_name=task_name,
                include_errors=include_errors,
                include_task_updates=include_task_updates,
                start_time=start_time,
                cursor=cursor,
                limit=limit,
            )
        )
