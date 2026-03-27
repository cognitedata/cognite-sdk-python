"""
===============================================================================
c321b1967b69a3a77cfa4c3979d4220d
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api.data_modeling.space_statistics import SyncSpaceStatisticsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.statistics import ProjectStatistics
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncStatisticsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.spaces = SyncSpaceStatisticsAPI(async_client)

    def project(self) -> ProjectStatistics:
        """
        `Retrieve project-wide usage data and limits <https://api-docs.cognite.com/20230101/tag/Statistics/operation/getStatistics>`_

        Returns the usage data and limits for a project's data modelling usage, including data model schemas and graph instances

        Returns:
            ProjectStatistics: The requested statistics and limits

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
        return run_sync(self.__async_client.data_modeling.statistics.project())
