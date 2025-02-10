# import time
# import pytest

# from typing import TYPE_CHECKING
# from cognite.client._cognite_client import CogniteClient
# from cognite.client.data_classes.simulators.filters import SimulationRunsFilter
# from cognite.client.data_classes.simulators.simulators import SimulationRunCall

# if TYPE_CHECKING:
#     from cognite.client import CogniteClient

# class TestSimulationRuns:
#     def test_list_runs(self, cognite_client: CogniteClient) -> None:
#         routines = cognite_client.simulators.runs.list(limit=5)

#     @pytest.mark.usefixtures("seed_simulator_routine_revisions", "delete_simulator", "seed_resource_names")
#     def test_create_runs(self, cognite_client: CogniteClient, seed_resource_names) -> None:
#         integrations = cognite_client.simulators.integrations.list()
#         assert len(integrations) > 0
#         # Check if the seeded simulator integration is alive
#         integration_to_check = seed_resource_names["simulator_integration_external_id"]
#         integration = next((x for x in integrations if x.external_id == integration_to_check), None)
#         assert integration is not None
#         assert integration.heartbeat >= time.time() - 60

#         run_to_create = SimulationRunCall(
#             routine_external_id=seed_resource_names["simulator_routine_external_id"],
#         )

#         run = cognite_client.simulators.runs.run(run_to_create)
#         assert run is not None
#         assert run.status == "ready"

#     @pytest.mark.usefixtures("seed_resource_names")
#     def test_list_runs(self, cognite_client: CogniteClient, seed_resource_names) -> None:
#         model_external_id = seed_resource_names["simulator_model_external_id"]
#         routines = cognite_client.simulators.runs.list(
#             limit=5, filter=SimulationRunsFilter(model_external_ids=[model_external_id])
#         )
#         assert len(routines) > 0

#     @pytest.mark.usefixtures("seed_resource_names")
#     async def test_run_async(self, cognite_client: CogniteClient, seed_resource_names) -> None:
#         run_to_create = SimulationRunCall(
#             routine_external_id=seed_resource_names["simulator_routine_external_id"],
#         )

#         run = cognite_client.simulators.runs.run(run_to_create)

#         assert run is not None
