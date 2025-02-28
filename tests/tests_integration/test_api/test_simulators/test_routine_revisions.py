import time
from typing import Any
from cognite.client import CogniteClient
from cognite.client.data_classes.simulators import PropertySort
from cognite.client.data_classes.simulators.filters import (
    SimulatorRoutineRevisionsFilter,
)
from cognite.client.data_classes.simulators.routine_revisions import SimulatorRoutineRevisionWrite
from tests.tests_integration.test_api.test_simulators.conftest import simulator_routine_revision


class TestSimulatorRoutineRevisions:
    def test_list_and_filtering_routine_revisions(
        self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names
    ) -> None:
        simulator_routine_external_id = seed_resource_names["simulator_routine_external_id"]
        revisions_all = cognite_client.simulators.routine_revisions.list(
            filter=SimulatorRoutineRevisionsFilter(
                routine_external_ids=[simulator_routine_external_id], all_versions=True
            ),
        )
        assert len(revisions_all) == 2
        model_external_id = seed_resource_names["simulator_model_external_id"]
        revisions_filter_res = cognite_client.simulators.routine_revisions.list(
            sort=PropertySort(order="asc", property="createdTime"),
            filter=SimulatorRoutineRevisionsFilter(model_external_ids=[model_external_id], all_versions=True),
            include_all_fields=True,
        )
        assert len(revisions_filter_res) == 2
        revisions_filter_res_json = [item.dump() for item in revisions_filter_res]
        # Inputs, outputs and script are not included in the response by default
        for revision_json in revisions_filter_res_json:
            revision_json["configuration"]["inputs"] = None
            revision_json["configuration"]["outputs"] = None
            revision_json["script"] = None
        revisions_all_json = [item.dump() for item in revisions_all]
        assert revisions_filter_res_json == revisions_all_json

        seed_rev2 = seed_simulator_routine_revisions[0]

        last_revision = revisions_filter_res[1]
        assert last_revision.external_id == seed_resource_names["simulator_routine_external_id"] + "_v2"

        last_revision_script_json = [item.dump() for item in last_revision.script]
        assert last_revision_script_json == seed_rev2["script"]
        # TODO: Fix this
        assert last_revision.script[0].steps[0].arguments["referenceId"] == seed_rev2["script"][0]["steps"][0]["arguments"]["referenceId"]

        last_revision_config_json = last_revision.configuration.dump()
        assert last_revision_config_json == seed_rev2["configuration"]

    def test_retrieve_routine_revision(
        self, cognite_client: CogniteClient, seed_simulator_routine_revisions, seed_resource_names
    ) -> None:
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

    def test_create_routine_revision(
        self, cognite_client: CogniteClient, seed_simulator_models: dict[str, Any], seed_resource_names
    ):
        routine_external_id = seed_resource_names["simulator_routine_external_id"]
        revision = cognite_client.simulators.routine_revisions.create(
            SimulatorRoutineRevisionWrite.load({
                **simulator_routine_revision,
                "externalId": f"{routine_external_id}_v3",
            })
        )
        assert revision is not None
        assert revision.external_id == f"{routine_external_id}_v3"
        assert revision.configuration.dump() == simulator_routine_revision["configuration"]
        assert [item.dump() for item in revision.script] == simulator_routine_revision["script"]
        assert revision.created_time
        assert revision.created_time > int(time.time() - 60) * 1000
