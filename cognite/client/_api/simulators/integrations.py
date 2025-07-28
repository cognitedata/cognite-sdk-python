from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes._base import CogniteFilter
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.data_classes.simulators.simulators import (
    SimulatorIntegration,
    SimulatorIntegrationList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import CogniteClient
    from cognite.client.config import ClientConfig


class SimulatorIntegrationsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/integrations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 1
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def __iter__(self) -> Iterator[SimulatorIntegration]:
        """Iterate over simulator integrations

        Fetches simulator integrations as they are iterated over, so you keep a limited number of simulator integrations in memory.

        Returns:
            Iterator[SimulatorIntegration]: yields Simulator integrations one by one.
        """
        return self()

    @overload
    def __call__(
        self, chunk_size: int, filter: SimulatorIntegrationFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorIntegration]: ...

    @overload
    def __call__(
        self, chunk_size: None = None, filter: SimulatorIntegrationFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorIntegration]: ...

    def __call__(
        self, chunk_size: int | None = None, filter: SimulatorIntegrationFilter | None = None, limit: int | None = None
    ) -> Iterator[SimulatorIntegration] | Iterator[SimulatorIntegrationList]:
        """Iterate over simulator integrations

        Fetches simulator integrations as they are iterated over, so you keep a limited number of simulator integrations in memory.

        Args:
            chunk_size (int | None): Number of simulator integrations to return in each chunk. Defaults to yielding one simulator integration a time.
            filter (SimulatorIntegrationFilter | None): Filter to apply on the integrations list.
            limit (int | None): Maximum number of simulator integrations to return. Defaults to return all items.

        Returns:
            Iterator[SimulatorIntegration] | Iterator[SimulatorIntegrationList]: yields Simulator one by one if chunk is not specified, else SimulatorList objects.
        """
        return self._list_generator(
            list_cls=SimulatorIntegrationList,
            resource_cls=SimulatorIntegration,
            method="POST",
            filter=filter.dump() if isinstance(filter, CogniteFilter) else filter,
            chunk_size=chunk_size,
            limit=limit,
        )

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        simulator_external_ids: str | SequenceNotStr[str] | None = None,
        active: bool | None = None,
    ) -> SimulatorIntegrationList:
        """`Filter simulator integrations <https://developer.cognite.com/api#tag/Simulator-Integrations/operation/filter_simulator_integrations_simulators_integrations_list_post>`_

        Retrieves a list of simulator integrations that match the given criteria.

        Args:
            limit (int | None): The maximum number of simulator integrations to return, pass None to return all.
            simulator_external_ids (str | SequenceNotStr[str] | None): Filter on simulator external ids.
            active (bool | None): Filter on active status of the simulator integration.

        Returns:
            SimulatorIntegrationList: List of simulator integrations

        Examples:
            List a few simulator integrations:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.simulators.integrations.list(limit=10)

            Filter simulator integrations by simulator external ids and active status:
                >>> res = client.simulators.integrations.list(
                ...     simulator_external_ids=["sim1", "sim2"],
                ...     active=True,
                ... )
        """
        integrations_filter = SimulatorIntegrationFilter(simulator_external_ids=simulator_external_ids, active=active)
        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            resource_cls=SimulatorIntegration,
            list_cls=SimulatorIntegrationList,
            filter=integrations_filter.dump(),
        )

    def delete(
        self,
        ids: int | Sequence[int] | None = None,
        external_ids: str | SequenceNotStr[str] | None = None,
    ) -> None:
        """`Delete simulator integrations <https://developer.cognite.com/api#tag/Simulator-Integrations/operation/delete_simulator_integrations_simulators_integrations_delete_post>`_

        Args:
            ids (int | Sequence[int] | None): Id(s) of simulator integrations to delete
            external_ids (str | SequenceNotStr[str] | None): External_id(s) of simulator integrations to delete

        Examples:
            Delete simulator integrations by id or external id:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.integrations.delete(ids=[1,2,3], external_ids="foo")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(ids=ids, external_ids=external_ids),
            wrap_ids=True,
        )
