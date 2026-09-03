from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.integrations.tasks import SyncResult, TaskHistory, TaskHistoryList
from cognite.client.utils._auxiliary import drop_none_values
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class IntegrationTasksAPI(APIClient):
    _RESOURCE_PATH = "/integrations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Integrations")

    async def list_history(
        self,
        external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TaskHistoryList:
        """`List task history <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Tasks/operation/get_integration_task_history>`_

        Args:
            external_id (str | None): Only return history for the integration with this external id.
            task_name (str | None): Only return history for the task with this name. Requires `external_id` to also be set.
            last_per_task (bool): Only return the latest history entry per task.
            limit (int | None): Maximum number of history entries to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            TaskHistoryList: List of task history entries

        Examples:

            List task history for a single integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.tasks.list_history(external_id="my-integration")
        """
        self._warning.warn()
        return await self._list(
            method="GET",
            url_path=f"{self._RESOURCE_PATH}/history",
            list_cls=TaskHistoryList,
            resource_cls=TaskHistory,
            limit=limit,
            filter=drop_none_values(
                {
                    "externalId": external_id,
                    "taskName": task_name,
                    "lastPerTask": last_per_task,
                }
            ),
            headers=self._alpha_version_header(),
        )

    async def sync(
        self,
        external_id: str,
        task_name: str | None = None,
        include_errors: bool = False,
        include_task_updates: bool = False,
        start_time: int | None = None,
        cursor: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> SyncResult:
        """`Sync integration history <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Tasks/operation/sync_integration_history>`_

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
        self._warning.warn()
        response = await self._get(
            url_path=f"{self._RESOURCE_PATH}/sync",
            params=drop_none_values(
                {
                    "externalId": external_id,
                    "taskName": task_name,
                    "includeErrors": include_errors,
                    "includeTaskUpdates": include_task_updates,
                    "startTime": start_time,
                    "cursor": cursor,
                    "limit": limit,
                }
            ),
            headers=self._alpha_version_header(),
            semaphore=self._get_semaphore("read"),
        )
        return SyncResult._load(response.json())
