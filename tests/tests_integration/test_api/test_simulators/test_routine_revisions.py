import uuid

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.simulators.filters import (
    SimulatorRoutineRevisionsFilter,
)
from cognite.client.data_classes.simulators.models import CreatedTimeSort
from cognite.client.utils._text import random_string

def truncated_uuid4():
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
@pytest.mark.usefixtures("seed_resource_names")
def seed_simulator_routine_unique_external_id(seed_resource_names) -> str:
    return f"{seed_resource_names['simulator_routine_external_id']}_{random_string(8)}"

@pytest.mark.usefixtures("seed_simulator_routine_unique_external_id", "seed_simulator_routine_revisions")
class TestSimulatorRoutineRevisions:
    def test_list_and_filtering_routine_revisions(self, seed_simulator_routine_unique_external_id, seed_resource_names, cognite_client: CogniteClient) -> None:
        revisions_all = cognite_client.simulators.routine_revisions.list(
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[seed_simulator_routine_unique_external_id], all_versions=True
            ),
        )
        assert len(revisions_all) > 1
        model_unique_external_id = seed_resource_names["simulator_model_external_id"]
        revisions_filter = cognite_client.simulators.routine_revisions.list(
            filter=SimulatorRoutineRevisionsFilter(model_external_ids=[model_unique_external_id], all_versions=True),
        )
        assert len(revisions_filter) == 10
        revisions_sort_asc = cognite_client.simulators.routine_revisions.list(
            sort=CreatedTimeSort(order="asc", property="createdTime"),
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[seed_simulator_routine_unique_external_id], all_versions=True
            ),
        )
        revisions_sort_desc = cognite_client.simulators.routine_revisions.list(
            sort=CreatedTimeSort(order="desc", property="createdTime"),
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[seed_simulator_routine_unique_external_id], all_versions=True
            ),
        )
        assert revisions_sort_asc[0].external_id == revisions_sort_desc[-1].external_id

    def test_retrieve_routine_revision(self, cognite_client: CogniteClient) -> None:
        revisions_all = cognite_client.simulators.routine_revisions.list(
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[seed_simulator_routine_unique_external_id], all_versions=True
            ),
        )

        assert len(revisions_all) > 5

        rev1 = revisions_all[0]
        rev2 = revisions_all[1]

        rev1_retrieve = cognite_client.simulators.routine_revisions.retrieve(external_id=rev1.external_id)
        assert rev1_retrieve is not None
        assert rev1_retrieve.external_id == rev1.external_id
        rev2_retrieve = cognite_client.simulators.routine_revisions.retrieve(external_id=rev2.external_id)
        assert rev2_retrieve is not None
        assert rev2_retrieve.external_id == rev2.external_id
