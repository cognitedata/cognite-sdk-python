"""
===============================================================================
a7c293e0cee8a18b88bd0f3649a337a3
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.simulators.integrations import SyncSimulatorIntegrationsAPI
from cognite.client._sync_api.simulators.logs import SyncSimulatorLogsAPI
from cognite.client._sync_api.simulators.models import SyncSimulatorModelsAPI
from cognite.client._sync_api.simulators.routines import SyncSimulatorRoutinesAPI
from cognite.client._sync_api.simulators.runs import SyncSimulatorRunsAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.simulators.simulators import Simulator, SimulatorList
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.integrations = SyncSimulatorIntegrationsAPI(async_client)
        self.models = SyncSimulatorModelsAPI(async_client)
        self.runs = SyncSimulatorRunsAPI(async_client)
        self.routines = SyncSimulatorRoutinesAPI(async_client)
        self.logs = SyncSimulatorLogsAPI(async_client)

    @overload
    def __call__(self, chunk_size: None = None) -> Iterator[Simulator]: ...

    @overload
    def __call__(self, chunk_size: int) -> Iterator[SimulatorList]: ...

    def __call__(self, chunk_size: int | None = None, limit: int | None = None) -> Iterator[Simulator | SimulatorList]:
        """
        Iterate over simulators

        Fetches simulators as they are iterated over, so you keep a limited number of simulators in memory.

        Args:
            chunk_size (int | None): Number of simulators to return in each chunk. Defaults to yielding one simulator a time.
            limit (int | None): Maximum number of simulators to return. Defaults to return all items.

        Yields:
            Simulator | SimulatorList: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        yield from SyncIterator(self.__async_client.simulators(chunk_size=chunk_size, limit=limit))

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> SimulatorList:
        """
        `List all simulators <https://developer.cognite.com/api#tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float(“inf”) or None to return all items.

        Returns:
            SimulatorList: List of simulators

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
        return run_sync(self.__async_client.simulators.list(limit=limit))
