import datetime

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators.filters import SimulatorRoutinesFilter
from cognite.client.data_classes.simulators.routines import CreatedTimeSort, SimulatorRoutineWrite


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_routines",
)
class TestSimulatorRoutines:
    def test_list_routines(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        model_unique_external_id = seed_resource_names["simulator_model_external_id"]

        routines = cognite_client.simulators.routines.list(
            filter=SimulatorRoutinesFilter(model_external_ids=[model_unique_external_id])
        )

        assert len(routines) > 0

    def test_create_routine(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        routine_external_id_new = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        routine_to_create = SimulatorRoutineWrite(
            name="sdk-test-routine",
            model_external_id=seed_resource_names["simulator_model_external_id"],
            simulator_integration_external_id=seed_resource_names["simulator_integration_external_id"],
            external_id=routine_external_id_new,
        )

        routine_created = cognite_client.simulators.routines.create(routine_to_create)
        assert routine_created is not None
        assert routine_created.external_id == routine_external_id_new
        cognite_client.simulators.routines.delete(external_ids=[routine_external_id_new])

    def test_sort(self, cognite_client: CogniteClient, seed_resource_names) -> None:
        simulator_integration_unique_external_id = seed_resource_names["simulator_integration_external_id"]
        routine_external_ids = [str(datetime.datetime.now().microsecond) for _ in range(3)]

        for routine_external_id in routine_external_ids:
            routine_to_create = SimulatorRoutineWrite(
                name="sdk-test-routine",
                model_external_id=seed_resource_names["simulator_model_external_id"],
                simulator_integration_external_id=simulator_integration_unique_external_id,
                external_id=routine_external_id,
            )

            cognite_client.simulators.routines.create(routine_to_create)

        routines_asc = cognite_client.simulators.routines.list(
            filter=SimulatorRoutinesFilter(
                simulator_integration_external_ids=[simulator_integration_unique_external_id]
            ),
            sort=CreatedTimeSort(order="asc", property="createdTime"),
        )

        routines_desc = cognite_client.simulators.routines.list(
            filter=SimulatorRoutinesFilter(
                simulator_integration_external_ids=[simulator_integration_unique_external_id]
            ),
            sort=CreatedTimeSort(order="desc", property="createdTime"),
        )

        assert len(routines_asc) > 0
        assert len(routines_desc) > 0
        assert len(routines_asc) == len(routines_desc)

        assert routines_asc[0].external_id == routines_desc[-1].external_id

        for routine_external_id in routine_external_ids:
            cognite_client.simulators.routines.delete(external_ids=[routine_external_id])
