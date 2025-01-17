from __future__ import annotations

import itertools
from collections.abc import Sequence
from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.data_modeling.ids import _load_space_identifier
from cognite.client.data_classes.data_modeling.statistics import InstanceStatsList, ProjectStatsAndLimits

if TYPE_CHECKING:
    from cognite.client._cognite_client import CogniteClient
    from cognite.client.config import ClientConfig


class StatisticsAPI(APIClient):
    _RESOURCE_PATH = "/models/statistics"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 100  # may need to be renamed, but fine for now

    def project(self) -> ProjectStatsAndLimits:
        """Usage data and limits for a project's data modelling usage, including data model schemas and graph instances"""
        return ProjectStatsAndLimits._load(
            self._get(self._RESOURCE_PATH).json(), project=self._cognite_client._config.project
        )

    def per_space(self, space: str | Sequence[str] | None = None, return_all: bool = False) -> InstanceStatsList:
        """Statistics and limits for all spaces in the project, or for a select subset"""
        if return_all:
            return InstanceStatsList._load(self._get(self._RESOURCE_PATH + "/spaces").json()["items"])

        elif space is not None:
            # Statistics and limits for spaces by their space identifiers
            ids = _load_space_identifier(space)
            return InstanceStatsList._load(
                itertools.chain.from_iterable(
                    self._post(self._RESOURCE_PATH + "/spaces/byids", json={"items": chunk.as_dicts()}).json()["items"]
                    for chunk in ids.chunked(self._RETRIEVE_LIMIT)
                )
            )
        raise ValueError("Either 'space' or 'return_all' must be specified")
