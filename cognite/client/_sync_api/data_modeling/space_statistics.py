"""
===============================================================================
4fc58be818b3c0f31dea52d2572478db
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.data_modeling.statistics import SpaceStatistics, SpaceStatisticsList
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSpaceStatisticsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def retrieve(self, space: str) -> SpaceStatistics | None: ...

    @overload
    def retrieve(self, space: SequenceNotStr[str]) -> SpaceStatisticsList: ...

    def retrieve(self, space: str | SequenceNotStr[str]) -> SpaceStatistics | SpaceStatisticsList | None:
        """
        `Retrieve usage data and limits per space <https://developer.cognite.com/api#tag/Statistics/operation/getSpaceStatisticsByIds>`_

        Args:
            space (str | SequenceNotStr[str]): The space or spaces to retrieve statistics for.

        Returns:
            SpaceStatistics | SpaceStatisticsList | None: The requested statistics and limits for the specified space(s).

        Examples:

            Fetch statistics for a single space:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> result = client.data_modeling.statistics.spaces.retrieve("my-space")

            Fetch statistics for multiple spaces:
                >>> res = client.data_modeling.statistics.spaces.retrieve(
                ...     ["my-space1", "my-space2"]
                ... )
        """
        return run_sync(self.__async_client.data_modeling.statistics.spaces.retrieve(space=space))

    def list(self) -> SpaceStatisticsList:
        """
        `Retrieve usage for all spaces <https://developer.cognite.com/api#tag/Statistics/operation/getSpaceStatistics>`_

        Returns statistics for data modeling resources grouped by each space in the project.

        Returns:
            SpaceStatisticsList: The requested statistics and limits for all spaces in the project.

        Examples:

            Fetch statistics for all spaces in the project:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> stats = client.data_modeling.statistics.spaces.list()
                >>> for space_stats in stats:
                ...     print(f"Space: {space_stats.space}, Nodes: {space_stats.nodes}")
        """
        return run_sync(self.__async_client.data_modeling.statistics.spaces.list())
