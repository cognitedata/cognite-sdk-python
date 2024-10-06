from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite_gen.client.data_classes.simulators.simulators import Simulator, SimulatorList, SimulatorUpdate, SimulatorWrite
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorsAPI(APIClient):
    _RESOURCE_PATH = "/simulators"
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Simulators"
        )

    def create(self, simulator_create_command: SimulatorWrite | Sequence[SimulatorWrite]) -> Simulator:
        """`Create Simulators <MISSING>`_

        Create a simulator entry in the Simulator Integration framework.

        Args: 
            simulator_create_command (SimulatorWrite | Sequence[SimulatorWrite]): None
        
        Returns:
            Simulator: None

        Examples:

            Create simulator:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorWrite
                >>> client = CogniteClient()
                >>> simulator = SimulatorWrite(<MISSING>)
                >>> res = client.hosted_extractors.destinations.create(simulator)

        """
        self._warning.warn()
        return self._create_multiple(
            list_cls=SimulatorList,
            resource_cls=Simulator,
            items=simulator_create_command,
            input_resource_cls=SimulatorWrite,
            headers={"cdf-version": "beta"},
        )

    def update(self, simulator_update_command: SimulatorUpdate | Sequence[SimulatorUpdate]) -> Simulator:
        """`Update Simulators <MISSING>`_

        Update simulators

        Args: 
            simulator_update_command (SimulatorUpdate | Sequence[SimulatorUpdate]): None
        
        Returns:
            Simulator: None

        Examples:

            Update simulator:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.simulators import SimulatorUpdate
                >>> client = CogniteClient()
                >>> update = SimulatorUpdate('mySimulator').<MISSING>
                >>> res = client.simulators.update(update)

        """
        self._warning.warn()
        return self._update_multiple(
            items=simulator_update_command,
            list_cls=SimulatorList,
            resource_cls=Simulator,
            update_cls=SimulatorUpdate,
            headers={"cdf-version": "beta"},
        )

    def delete(self, unknown: Unknown) -> EmptyResponse | None:
        """`Delete Simulators <MISSING>`_

        Delete simulators

        Args: 
            unknown (Unknown): None
        
        Returns:
            EmptyResponse | None: None

        Examples:

            Delete simulator:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.simulators.delete(["mySimulator", "mySimulator2"])


        """
        self._warning.warn()
        return self._delete_multiple(
            identifiers=IdentifierSequence.load(unknowns=unknown),
            wrap_ids=False,
            returns_items=False,
            headers={"cdf-version": "beta"},
        )

    def filter(self, limit: int, filter: ListSimulatorsFilters | None = None) -> Simulator:
        """`Filter Simulators <MISSING>`_

        List simulators

        Args: 
            limit (int): None
            filter (ListSimulatorsFilters | None): None
        
        Returns:
            Simulator: None

        Examples:

            <MISSING>

        """
        "<MISSING>"
