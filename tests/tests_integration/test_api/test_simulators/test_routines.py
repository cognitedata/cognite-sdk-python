import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import PropertySort
from cognite.client.data_classes.simulators.routines import SimulatorRoutineWrite
from cognite.client.utils._text import random_string


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_routines",
)
class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_unique_external_id = seed_resource_names["simulator_model_external_id"]

        routines = cognite_client.simulators.routines.list(model_external_ids=[model_unique_external_id])

        assert len(routines) > 0

    def test_create_routine(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id_new = random_string(10)

        routine_to_create = SimulatorRoutineWrite(
            name="sdk-test-routine",
            model_external_id=seed_resource_names["simulator_model_external_id"],
            simulator_integration_external_id=seed_resource_names["simulator_integration_external_id"],
            external_id=routine_external_id_new,
        )

        try:
            routine_created = cognite_client.simulators.routines.create(routine_to_create)
            assert routine_created is not None
            assert routine_created.external_id == routine_external_id_new
        finally:
            rtns = cognite_client.simulators.routines.list(model_external_ids=[routine_to_create.model_external_id])
            for routine in rtns:
                cognite_client.simulators.routines.delete(external_ids=[routine.external_id])

    def test_sort(self, cognite_client: CogniteClient, seed_resource_names, seed_simulator_routines) -> None:
        simulator_integration_unique_external_id = seed_resource_names["simulator_integration_external_id"]

        routines_asc = cognite_client.simulators.routines.list(
            simulator_integration_external_ids=[simulator_integration_unique_external_id],
            sort=PropertySort(order="asc", property="createdTime"),
        )

        routines_desc = cognite_client.simulators.routines.list(
            simulator_integration_external_ids=[simulator_integration_unique_external_id],
            sort=PropertySort(order="desc", property="createdTime"),
        )

        assert len(routines_asc) > 0
        assert len(routines_desc) > 0
        assert len(routines_asc) == len(routines_desc)

        assert routines_asc[0].external_id == routines_desc[-1].external_id
