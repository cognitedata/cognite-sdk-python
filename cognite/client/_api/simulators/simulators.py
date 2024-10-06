from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.simulators.simulators import Simulator, SimulatorList
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorsAPI(APIClient):
    _RESOURCE_PATH = "/simulators"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Simulators")

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> SimulatorList:
        """`Filter Simulators <https://api-docs.cognite.com/20230101-alpha/tag/Simulators/operation/filter_simulators_simulators_list_post>`_

        List simulators

        Args:
            limit (int): The maximum number of simulators to return. Defaults to 25. Set to -1, float("inf") or None

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
