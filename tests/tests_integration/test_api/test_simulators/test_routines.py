import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import PropertySort
from cognite.client.data_classes.simulators.routines import SimulatorRoutine, SimulatorRoutineWrite
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed.data import ResourceNames


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_routines",
)
class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        model_unique_external_id = seed_resource_names.simulator_model_external_id

        routines = cognite_client.simulators.routines.list(model_external_ids=[model_unique_external_id])

        assert len(routines) > 0

    def test_create_routine(self, cognite_client: CogniteClient, seed_resource_names: ResourceNames) -> None:
        routine_external_id_new = random_string(10)

        routine_to_create = SimulatorRoutineWrite(
            name="sdk-test-routine",
            model_external_id=seed_resource_names.simulator_model_external_id,
            simulator_integration_external_id=seed_resource_names.simulator_integration_external_id,
            external_id=routine_external_id_new,
        )

        try:
            routine_created = cognite_client.simulators.routines.create(routine_to_create)
            assert routine_created is not None
            assert routine_created.as_write() == routine_to_create
        finally:
            cognite_client.simulators.routines.delete(external_ids=[routine_to_create.external_id])

    def test_sort(
        self,
        cognite_client: CogniteClient,
        seed_resource_names: ResourceNames,
        seed_simulator_routines: list[SimulatorRoutine],
    ) -> None:
        simulator_integration_unique_external_id = seed_resource_names.simulator_integration_external_id

        routines_asc = cognite_client.simulators.routines.list(
            simulator_integration_external_ids=[simulator_integration_unique_external_id],
            sort=PropertySort(order="asc", property="createdTime"),
        )

        assert len(routines_asc) > 1
        for i in range(1, len(routines_asc)):
            assert routines_asc[i].created_time >= routines_asc[i - 1].created_time

        # Test iterator with sort
        routines_iter = []
        for routine in cognite_client.simulators.routines(
            simulator_integration_external_ids=[simulator_integration_unique_external_id],
            sort=PropertySort(order="asc", property="createdTime"),
            limit=3,
        ):
            routines_iter.append(routine)

        assert len(routines_iter) > 0
        for i in range(1, len(routines_iter)):
            assert routines_iter[i].created_time >= routines_iter[i - 1].created_time
