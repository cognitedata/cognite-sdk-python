from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, overload

from cognite.client._api.simulators.integrations import SimulatorIntegrationsAPI
from cognite.client._api.simulators.models import SimulatorModelsAPI
from cognite.client._api.simulators.routines import SimulatorRoutinesAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.simulators import Simulator, SimulatorList
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SimulatorsAPI(APIClient):
    _RESOURCE_PATH = "/simulators"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.integrations = SimulatorIntegrationsAPI(config, api_version, cognite_client)
        self.models = SimulatorModelsAPI(config, api_version, cognite_client)
        self.routines = SimulatorRoutinesAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def __iter__(self) -> Iterator[Simulator]:
        """Iterate over simulators

        Fetches simulators as they are iterated over, so you keep a limited number of simulators in memory.

        Returns:
            Iterator[Simulator]: yields Simulators one by one.
        """
        return self()

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Simulator]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[SimulatorList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Simulator] | Iterator[SimulatorList]:
        """Iterate over simulators

        Fetches simulators as they are iterated over, so you keep a limited number of simulators in memory.

        Args:
            chunk_size (int | None): Number of simulators to return in each chunk. Defaults to yielding one simulator a time.
            limit (int | None): Maximum number of simulators to return. Defaults to return all items.

        Returns:
            Iterator[Simulator] | Iterator[SimulatorList]: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        return self._list_generator(
            list_cls=SimulatorList,
            resource_cls=Simulator,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> SimulatorList:
        """`List simulators <https://developer.cognite.com/api#tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List simulators

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Returns:
            SimulatorList: List of simulators

        Examples:

            List simulators:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.list()

        """
        self._warning.warn()
        return self._list(method="POST", limit=limit, resource_cls=Simulator, list_cls=SimulatorList)
