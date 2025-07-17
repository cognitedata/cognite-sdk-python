from __future__ import annotations

from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.ids import _load_space_identifier
from cognite.client.data_classes.data_modeling.statistics import (
    ProjectStatistics,
    SpaceStatistics,
    SpaceStatisticsList,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


class SpaceStatisticsAPI(APIClient):
    _RESOURCE_PATH = "/models/statistics/spaces"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 100

    @overload
    def retrieve(self, space: str) -> SpaceStatistics | None: ...

    @overload
    def retrieve(self, space: SequenceNotStr[str]) -> SpaceStatisticsList: ...

    def retrieve(
        self,
        space: str | SequenceNotStr[str],
    ) -> SpaceStatistics | SpaceStatisticsList | None:
        """`Retrieve usage data and limits per space <https://developer.cognite.com/api#tag/Statistics/operation/getSpaceStatisticsByIds>`_

        Args:
            space (str | SequenceNotStr[str]): The space or spaces to retrieve statistics for.

        Returns:
            SpaceStatistics | SpaceStatisticsList | None: The requested statistics and limits for the specified space(s).

        Examples:

            Fetch statistics for a single space:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> result = client.data_modeling.statistics.spaces.retrieve("my-space")

            Fetch statistics for multiple spaces:
                >>> res = client.data_modeling.statistics.spaces.retrieve(
                ...     ["my-space1", "my-space2"]
                ... )

        """
        return self._retrieve_multiple(
            SpaceStatisticsList,
            SpaceStatistics,
            identifiers=_load_space_identifier(space),
            resource_path=self._RESOURCE_PATH,
        )

    def list(self) -> SpaceStatisticsList:
        """`Retrieve usage for all spaces <https://developer.cognite.com/api#tag/Statistics/operation/getSpaceStatistics>`_

        Returns statistics for data modeling resources grouped by each space in the project.

        Returns:
            SpaceStatisticsList: The requested statistics and limits for all spaces in the project.

        Examples:

            Fetch statistics for all spaces in the project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> stats = client.data_modeling.statistics.spaces.list()
                >>> for space_stats in stats:
                ...     print(f"Space: {space_stats.space}, Nodes: {space_stats.nodes}")

        """
        # None 2xx responses are handled by the _get method.
        response = self._get(self._RESOURCE_PATH)
        return SpaceStatisticsList._load(response.json()["items"], self._cognite_client)


class StatisticsAPI(APIClient):
    _RESOURCE_PATH = "/models/statistics"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.spaces = SpaceStatisticsAPI(config, api_version, cognite_client)

    def project(self) -> ProjectStatistics:
        """`Retrieve project-wide usage data and limits <https://developer.cognite.com/api#tag/Statistics/operation/getStatistics>`_

        Returns the usage data and limits for a project's data modelling usage, including data model schemas and graph instances

        Returns:
            ProjectStatistics: The requested statistics and limits

        Examples:

            Fetch project statistics (and limits) and check the current number of data models vs.
            and how many more can be created:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> stats = client.data_modeling.statistics.project()
                >>> data_model_count = stats.data_models.count
                >>> available_count = stats.data_models.limit - data_model_count
        """
        response = self._get(self._RESOURCE_PATH)

        return ProjectStatistics._load(response.json(), self._cognite_client)
