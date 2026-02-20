from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, overload

from cognite.client._api.simulators.integrations import SimulatorIntegrationsAPI
from cognite.client._api.simulators.logs import SimulatorLogsAPI
from cognite.client._api.simulators.models import SimulatorModelsAPI
from cognite.client._api.simulators.routines import SimulatorRoutinesAPI
from cognite.client._api.simulators.runs import SimulatorRunsAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.simulators import Simulator, SimulatorList
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class SimulatorsAPI(APIClient):
    _RESOURCE_PATH = "/simulators"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.integrations = SimulatorIntegrationsAPI(config, api_version, cognite_client)
        self.models = SimulatorModelsAPI(config, api_version, cognite_client)
        self.runs = SimulatorRunsAPI(config, api_version, cognite_client)
        self.routines = SimulatorRoutinesAPI(config, api_version, cognite_client)
        self.logs = SimulatorLogsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[Simulator]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[SimulatorList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[Simulator] | AsyncIterator[SimulatorList]:
        """Iterate over simulators

        Fetches simulators as they are iterated over, so you keep a limited number of simulators in memory.

        Args:
            chunk_size: Number of simulators to return in each chunk. Defaults to yielding one simulator a time.
            limit: Maximum number of simulators to return. Defaults to return all items.

        Yields:
            yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """  # noqa: DOC404
        async for item in self._list_generator(
            list_cls=SimulatorList,
            resource_cls=Simulator,
            method="POST",
            chunk_size=chunk_size,
            limit=limit,
        ):
            yield item

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> SimulatorList:
        """`List all simulators <https://developer.cognite.com/api#tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        Args:
            limit: Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Returns:
            List of simulators

        Examples:
            List simulators:
                    >>> from cognite.client import CogniteClient, AsyncCogniteClient
                    >>> client = CogniteClient()
                    >>> # async_client = AsyncCogniteClient()  # another option
                    >>> res = client.simulators.list(limit=10)

            Iterate over simulators, one-by-one:
                    >>> for simulator in client.simulators():
                    ...     simulator  # do something with the simulator

        """
        self._warning.warn()
        return await self._list(method="POST", limit=limit, resource_cls=Simulator, list_cls=SimulatorList)
