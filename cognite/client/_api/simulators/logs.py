from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, NoReturn, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.simulators.logs import SimulatorLog, SimulatorLogList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class SimulatorLogsAPI(APIClient):
    _RESOURCE_PATH = "/simulators/logs"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="General Availability", sdk_maturity="alpha", feature_name="Simulators"
        )

    @overload
    def retrieve(self, id: None = None) -> NoReturn: ...

    @overload
    def retrieve(self, id: int) -> SimulatorLog | None: ...

    @overload
    def retrieve(
        self,
        id: int | Sequence[int] | None = None,
    ) -> SimulatorLogList | SimulatorLog | None: ...

    def retrieve(
        self,
        id: int | Sequence[int] | None = None,
    ) -> SimulatorLogList | SimulatorLog | None:
        """`Retrieve simulator log(s) <https://developer.cognite.com/api#tag/Simulator-Models/operation/retrieve_simulator_model_revisions_simulators_models_revisions_byids_post>`_
        Retrieve one simulator log by ID(s)

        Simulator logs track what happens during simulation runs, model parsing, and generic connector logic.
        They provide valuable information for monitoring, debugging, and auditing.

        Simulator logs capture important events, messages, and exceptions that occur during the execution of simulations, model parsing, and connector operations.
        They help users identify issues, diagnose problems, and gain insights into the behavior of the simulator integrations.

        Args:
            id (int | Sequence[int] | None): The ids of the simulator log.
        Returns:
            SimulatorLogList | SimulatorLog | None: Requested simulator log(s)
        Examples:
            Get simulator model logs:
                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> model = client.simulators.models.retrieve(id=1)
                >>> logs = client.simulators.logs.retrieve(id=model.log_id)
        """
        self._warning.warn()

        return self._retrieve_multiple(
            list_cls=SimulatorLogList,
            resource_cls=SimulatorLog,
            identifiers=IdentifierSequence.load(ids=id),
        )
