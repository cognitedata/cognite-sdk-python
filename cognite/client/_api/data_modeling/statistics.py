from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.ids import _load_space_identifier
from cognite.client.data_classes.data_modeling.statistics import (
    InstanceStatsList,
    InstanceStatsPerSpace,
    ProjectStatistics,
)
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


class StatisticsAPI(APIClient):
    _RESOURCE_PATH = "/models/statistics"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 100  # may need to be renamed, but fine for now

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
                >>> num_dm = stats.data_models.current
                >>> num_dm_left = stats.data_models.limit - num_dm
        """
        return ProjectStatistics._load(
            self._get(self._RESOURCE_PATH).json(), project=self._cognite_client._config.project
        )

    @overload
    def per_space(self, space: str, return_all: Literal[False]) -> InstanceStatsPerSpace: ...

    @overload
    def per_space(self, space: Any, return_all: Literal[True]) -> InstanceStatsList: ...

    @overload
    def per_space(self, space: SequenceNotStr[str], return_all: bool) -> InstanceStatsList: ...

    def per_space(
        self, space: str | SequenceNotStr[str] | None = None, return_all: bool = False
    ) -> InstanceStatsPerSpace | InstanceStatsList:
        """`Retrieve usage data and limits per space <https://developer.cognite.com/api#tag/Statistics/operation/getSpaceStatisticsByIds>`_

        See also: `Retrieve statistics and limits for all spaces <https://developer.cognite.com/api#tag/Statistics/operation/getSpaceStatistics>`_

        Args:
            space (str | SequenceNotStr[str] | None): The space or spaces to retrieve statistics for.
            return_all (bool): If True, fetch statistics for all spaces. If False, fetch statistics for the specified space(s).

        Returns:
            InstanceStatsPerSpace | InstanceStatsList: InstanceStatsPerSpace if a single space is given, else InstanceStatsList (which is a list of InstanceStatsPerSpace)

        Examples:

            Fetch statistics for a single space:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.data_modeling.statistics.per_space("my-space")

            Fetch statistics for multiple spaces:
                >>> res = client.data_modeling.statistics.per_space(
                ...     ["my-space1", "my-space2"]
                ... )

            Fetch statistics for all spaces (ignores the 'space' argument):
                >>> res = client.data_modeling.statistics.per_space(return_all=True)
        """
        if return_all:
            return InstanceStatsList._load(self._get(self._RESOURCE_PATH + "/spaces").json()["items"])

        elif space is None:
            raise ValueError("Either 'space' or 'return_all' must be specified")

        ids = _load_space_identifier(space)
        return InstanceStatsList._load(
            itertools.chain.from_iterable(
                self._post(self._RESOURCE_PATH + "/spaces/byids", json={"items": chunk.as_dicts()}).json()["items"]
                for chunk in ids.chunked(self._RETRIEVE_LIMIT)
            )
        )
