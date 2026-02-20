from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

from cognite.client._api.data_modeling.space_statistics import SpaceStatisticsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.statistics import ProjectStatistics

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class StatisticsAPI(APIClient):
    _RESOURCE_PATH = "/models/statistics"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.spaces = SpaceStatisticsAPI(config, api_version, cognite_client)

    def _get_semaphore(self, operation: Literal["read_schema"]) -> asyncio.BoundedSemaphore:
        from cognite.client import global_config

        assert operation == "read_schema", "Statistics API should only use read_schema semaphore"
        return global_config.concurrency_settings.data_modeling._semaphore_factory(
            operation, project=self._cognite_client.config.project
        )

    async def project(self) -> ProjectStatistics:
        """`Retrieve project-wide usage data and limits <https://developer.cognite.com/api#tag/Statistics/operation/getStatistics>`_

        Returns the usage data and limits for a project's data modelling usage, including data model schemas and graph instances

        Returns:
            The requested statistics and limits

        Examples:

            Fetch project statistics (and limits) and check the current number of data models vs.
            and how many more can be created:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> stats = client.data_modeling.statistics.project()
                >>> data_model_count = stats.data_models.count
                >>> available_count = stats.data_models.limit - data_model_count
        """
        response = await self._get(self._RESOURCE_PATH, semaphore=self._get_semaphore("read_schema"))
        return ProjectStatistics._load_with_project(response.json(), self._cognite_client.config.project)
