from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.integrations.errors import IntegrationError, IntegrationErrorList
from cognite.client.utils._auxiliary import drop_none_values
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class IntegrationErrorsAPI(APIClient):
    _RESOURCE_PATH = "/integrations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Integrations")

    async def list(
        self,
        external_id: str | None = None,
        task: str | None = None,
        min_start_time: int | None = None,
        max_end_time: int | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> IntegrationErrorList:
        """`List errors <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Errors/operation/get_integration_errors>`_

        Args:
            external_id (str | None): Only return errors for the integration with this external id.
            task (str | None): Only return errors for the task with this name. Requires `external_id` to also be set.
            min_start_time (int | None): Only return errors that started at or after this time, in milliseconds since epoch.
            max_end_time (int | None): Only return errors that ended at or before this time, in milliseconds since epoch.
            limit (int | None): Maximum number of errors to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            IntegrationErrorList: List of errors

        Examples:

            List errors for a single integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.errors.list(external_id="my-integration")
        """
        self._warning.warn()
        return await self._list(
            method="GET",
            url_path=f"{self._RESOURCE_PATH}/errors",
            list_cls=IntegrationErrorList,
            resource_cls=IntegrationError,
            limit=limit,
            filter=drop_none_values(
                {
                    "externalId": external_id,
                    "task": task,
                    "minStartTime": min_start_time,
                    "maxEndTime": max_end_time,
                }
            ),
            headers=self._alpha_version_header(),
        )
