from itertools import pairwise

import pytest

from cognite.client._cognite_client import CogniteClient
from cognite.client.data_classes.simulators import (
    PropertySort,
    SimulatorRoutine,
    SimulatorRoutineRevision,
    SimulatorRoutineWrite,
)
from cognite.client.utils._text import random_string
from tests.tests_integration.test_api.test_simulators.seed.data import (
    ResourceNames,
)


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
            sort=PropertySort(order="asc", property="created_time"),
        )

        assert len(routines_asc) > 1
        for i in range(1, len(routines_asc)):
            assert routines_asc[i].created_time >= routines_asc[i - 1].created_time


@pytest.mark.usefixtures(
    "seed_resource_names",
    "seed_simulator_routine_revisions",
)
class TestSimulatorRoutinesRunWithRevisions:
    def test_run_with_revisions(
        self,
        cognite_client: CogniteClient,
        seed_resource_names: ResourceNames,
        seed_simulator_routine_revisions: tuple[SimulatorRoutineRevision, SimulatorRoutineRevision],
    ) -> None:
        """Test running a simulation using routine and model revision external IDs."""
        routine_revision_external_id = seed_simulator_routine_revisions[0].external_id
        model_revision_external_id = seed_resource_names.simulator_model_revision_external_id
        simulator_integration_unique_external_id = seed_resource_names.simulator_integration_external_id

        # Run simulation using revision external IDs
        run = cognite_client.simulators.routines.run(
            routine_revision_external_id=routine_revision_external_id,
            model_revision_external_id=model_revision_external_id,
            wait=False,  # Don't wait to avoid timeout in tests
        )

        assert run is not None
        assert run.id is not None
        assert run.routine_revision_external_id == routine_revision_external_id
        assert run.model_revision_external_id == model_revision_external_id
        # Test iterator with sort
        routines_iter = list(
            cognite_client.simulators.routines(
                simulator_integration_external_ids=[simulator_integration_unique_external_id],
                sort=PropertySort(order="asc", property="created_time"),
                limit=3,
            )
        )

        assert len(routines_iter) == 3
        for prev, curr in pairwise(routines_iter):
            assert curr.created_time >= prev.created_time
