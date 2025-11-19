"""
===============================================================================
a0924757f3aa2b1e9014f5bc1247ee5f
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.simulators.logs import SimulatorLog, SimulatorLogList
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncSimulatorLogsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def retrieve(self, ids: int) -> SimulatorLog | None: ...

    @overload
    def retrieve(self, ids: Sequence[int]) -> SimulatorLogList | None: ...

    def retrieve(self, ids: int | Sequence[int]) -> SimulatorLogList | SimulatorLog | None:
        """
        `Retrieve simulator logs <https://developer.cognite.com/api#tag/Simulator-Logs/operation/simulator_logs_by_ids_simulators_logs_byids_post>`_

        Simulator logs track what happens during simulation runs, model parsing, and generic connector logic.
        They provide valuable information for monitoring, debugging, and auditing.

        Simulator logs capture important events, messages, and exceptions that occur during the execution of simulations, model parsing, and connector operations.
        They help users identify issues, diagnose problems, and gain insights into the behavior of the simulator integrations.

        Args:
            ids (int | Sequence[int]): The ids of the simulator log.

        Returns:
            SimulatorLogList | SimulatorLog | None: Requested simulator log(s)

        Examples:
            Get simulator logs by simulator model id:
                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> model = client.simulators.models.retrieve(ids=1)
                >>> logs = client.simulators.logs.retrieve(ids=model.log_id)

            Get simulator logs by simulator integration id:
                >>> integrations = client.simulators.integrations.list()
                >>> logs = client.simulators.logs.retrieve(ids=integrations[0].log_id)

            Get simulator logs by simulation run id:
                >>> run = client.simulators.runs.retrieve(ids=1)
                >>> logs = client.simulators.logs.retrieve(ids=run.log_id)

            Get simulator logs directly on a simulation run object:
                >>> run = client.simulators.runs.retrieve(ids=2)
                >>> res = run.get_logs()
        """
        return run_sync(self.__async_client.simulators.logs.retrieve(ids=ids))
