from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.integrations.tasks import TaskHistory, TaskHistoryList
from cognite.client.utils._auxiliary import drop_none_values
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class IntegrationHistoryAPI(APIClient):
    _RESOURCE_PATH = "/integrations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Integrations")

    async def list(
        self,
        external_id: str | None = None,
        task_name: str | None = None,
        last_per_task: bool = False,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> TaskHistoryList:
        """`List task history <https://api-docs.cognite.com/20230101-beta/tag/Integration-Tasks/operation/get_integration_task_history>`_

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
                >>> res = client.integrations.history.list(external_id="my-integration")
        """
        if task_name is not None and external_id is None:
            raise ValueError("Specifying 'task_name' requires 'external_id' to also be set.")

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
            headers=self._beta_version_header(),
        )
