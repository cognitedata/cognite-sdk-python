"""
===============================================================================
215983ac4df951f01a198f0eca4a529a
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.simulators.simulators import SimulatorIntegration, SimulatorIntegrationList
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorIntegrationsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: int,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        active: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorIntegrationList]: ...

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        active: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorIntegration]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        active: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[SimulatorIntegration] | Iterator[SimulatorIntegrationList]:
        """
        Iterate over simulator integrations

        Fetches simulator integrations as they are iterated over, so you keep a limited number of simulator integrations in memory.

        Args:
            chunk_size (int | None): Number of simulator integrations to return in each chunk. Defaults to yielding one simulator integration a time.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter on simulator external ids.
            active (bool | None): Filter on active status of the simulator integration.
            limit (int | None): The maximum number of simulator integrations to return, pass None to return all.

        Yields:
            SimulatorIntegration | SimulatorIntegrationList: yields SimulatorIntegration one by one if chunk_size is not specified, else SimulatorIntegrationList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.simulators.integrations(
                chunk_size=chunk_size, simulator_external_ids=simulator_external_ids, active=active, limit=limit
            )
        )  # type: ignore [misc]

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        active: bool | None = None,
    ) -> SimulatorIntegrationList:
        """
        `Filter simulator integrations <https://developer.cognite.com/api#tag/Simulator-Integrations/operation/filter_simulator_integrations_simulators_integrations_list_post>`_

        Retrieves a list of simulator integrations that match the given criteria.

        Args:
            limit (int | None): The maximum number of simulator integrations to return, pass None to return all.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter on simulator external ids.
            active (bool | None): Filter on active status of the simulator integration.

        Returns:
            SimulatorIntegrationList: List of simulator integrations

        Examples:
            List a few simulator integrations:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.simulators.integrations.list(limit=10)

            Iterate over simulator integrations, one-by-one:
                >>> for integration in client.simulators.integrations():
                ...     integration  # do something with the simulator integration

            Filter simulator integrations by simulator external ids and active status:
                >>> res = client.simulators.integrations.list(
                ...     simulator_external_ids=["sim1", "sim2"],
                ...     active=True,
                ... )
        """
        return run_sync(
            self.__async_client.simulators.integrations.list(
                limit=limit, simulator_external_ids=simulator_external_ids, active=active
            )
        )

    def delete(
        self, ids: int | Sequence[int] | None = None, external_ids: str | SequenceNotStr[str] | None = None
    ) -> None:
        """
        `Delete simulator integrations <https://developer.cognite.com/api#tag/Simulator-Integrations/operation/delete_simulator_integrations_simulators_integrations_delete_post>`_

        Args:
            ids (int | Sequence[int] | None): Id(s) of simulator integrations to delete
            external_ids (str | SequenceNotStr[str] | None): External_id(s) of simulator integrations to delete

        Examples:
            Delete simulator integrations by id or external id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.simulators.integrations.delete(ids=[1,2,3], external_ids="foo")
        """
        return run_sync(self.__async_client.simulators.integrations.delete(ids=ids, external_ids=external_ids))
