from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api.simulators.integrations import SimulatorIntegrationsAPI
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
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> SimulatorList:
        """`List simulators <https://developer.cognite.com/api#tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List simulators

        Args:
            limit (int): Maximum number of results to return. Defaults to 1000. Set to -1, float(“inf”) or None to return all items.

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
