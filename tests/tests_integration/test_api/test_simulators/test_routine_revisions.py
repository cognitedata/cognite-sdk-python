from cognite.client import CogniteClient
from cognite.client.data_classes.simulators.filters import (
    SimulatorRoutineRevisionsFilter,
)
from cognite.client.data_classes.simulators import PropertySort


class TestSimulatorRoutineRevisions:
    def test_list_and_filtering_routine_revisions(self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names) -> None:
        simulator_routine_external_id = seed_resource_names["simulator_routine_external_id"]
        revisions_all = cognite_client.simulators.routine_revisions.list(
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[simulator_routine_external_id], all_versions=True
            ),
        )
        assert len(revisions_all) == 2
        model_unique_external_id = seed_resource_names["simulator_model_external_id"]
        revisions_filter = cognite_client.simulators.routine_revisions.list(
            sort=PropertySort(order="asc", property="createdTime"),
            filter=SimulatorRoutineRevisionsFilter(model_external_ids=[model_unique_external_id], all_versions=True),
        )
        assert len(revisions_filter) == 2

    def test_retrieve_routine_revision(self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names) -> None:
        simulator_routine_external_id = seed_resource_names["simulator_routine_external_id"]
        revisions_all = cognite_client.simulators.routine_revisions.list(
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[simulator_routine_external_id], all_versions=True
            ),
        )

        assert len(revisions_all) == 2

        rev1 = revisions_all[0]
        rev2 = revisions_all[1]

        rev1_retrieve = cognite_client.simulators.routine_revisions.retrieve(external_id=rev1.external_id)
        assert rev1_retrieve is not None
        assert rev1_retrieve.external_id == rev1.external_id
        rev2_retrieve = cognite_client.simulators.routine_revisions.retrieve(external_id=rev2.external_id)
        assert rev2_retrieve is not None
        assert rev2_retrieve.external_id == rev2.external_id
