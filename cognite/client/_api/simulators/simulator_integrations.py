from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.filters import SimulatorIntegrationFilter
from cognite.client.data_classes.simulators.simulators import (
    SimulatorIntegration,
    SimulatorIntegrationList,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorIntegrationsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/integrations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter: SimulatorIntegrationFilter | dict[str, Any] | None = None,
    ) -> SimulatorIntegrationList:
        """`Filter simulator integrations <https://developer.cognite.com/api#tag/Simulator-Integrations/operation/filter_simulator_integrations_simulators_integrations_list_post>`_

        List simulator integrations

        Args:
            limit (int): The maximum number of simulator integrations to return. Defaults to 100.
            filter (SimulatorIntegrationFilter | dict[str, Any] | None): The filter to narrow down simulator integrations.

        Returns:
            SimulatorIntegrationList: List of simulator integrations

        Examples:

            List simulator integrations:

                    >>> from cognite.client import CogniteClient
                    >>> client = CogniteClient()
                    >>> res = client.simulators.integrations.list()

        """

        self._warning.warn()
        return self._list(
            method="POST",
            limit=limit,
            url_path="/simulators/integrations/list",
            resource_cls=SimulatorIntegration,
            list_cls=SimulatorIntegrationList,
            filter=filter.dump()
            if isinstance(filter, SimulatorIntegrationFilter)
            else filter
            if isinstance(filter, dict)
            else None,  # fix this
        )
