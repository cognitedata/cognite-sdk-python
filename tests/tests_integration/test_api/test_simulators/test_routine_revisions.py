import time

from cognite.client import CogniteClient
from cognite.client.data_classes.shared import TimestampRange
from cognite.client.data_classes.simulators import (
    PropertySort,
    SimulatorRoutine,
    SimulatorRoutineInputConstant,
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionWrite,
)
from cognite.client.utils._time import timestamp_to_ms
from tests.tests_integration.test_api.test_simulators.seed.data import (
    SIMULATOR_ROUTINE_REVISION,
    SIMULATOR_ROUTINE_REVISION_CONFIG,
    SIMULATOR_ROUTINE_REVISION_SCRIPT,
    ResourceNames,
    create_simulator_routine_revision,
)


class TestSimulatorRoutineRevisions:
    def test_list_and_filtering_routine_revisions(
        self,
        cognite_client: CogniteClient,
        seed_simulator_routine_revisions: tuple[SimulatorRoutineRevision, SimulatorRoutineRevision],
        seed_resource_names: ResourceNames,
    ) -> None:
        simulator_routine_external_id = seed_resource_names.simulator_routine_external_id
        one_min_ahead = timestamp_to_ms("1m-ahead")
        revisions_by_routine = cognite_client.simulators.routines.revisions.list(
            created_time=TimestampRange(min=0, max=one_min_ahead),
            routine_external_ids=[simulator_routine_external_id],
            all_versions=True,
        )
        assert len(revisions_by_routine) == 2
        model_external_id = seed_resource_names.simulator_model_external_id
        revisions_by_model: list[SimulatorRoutineRevision] = []

        for revision in cognite_client.simulators.routines.revisions(
            sort=PropertySort(order="asc", property="created_time"),
            model_external_ids=[model_external_id],
            all_versions=True,
            include_all_fields=True,
        ):
            revisions_by_model.append(revision)

        assert len(revisions_by_model) == 2
        revisions_by_model_json = [item.dump() for item in revisions_by_model]
        # Inputs, outputs and script are not included in the response by default
        for revision_json in revisions_by_model_json:
            revision_json["configuration"]["inputs"] = None
            revision_json["configuration"]["outputs"] = None
            revision_json["script"] = None

        revisions_by_routine_json = [item.dump() for item in revisions_by_routine]
        assert revisions_by_model_json == revisions_by_routine_json

        seed_rev2 = seed_simulator_routine_revisions[0]

        last_revision = revisions_by_model[1]
        assert last_revision.external_id == seed_resource_names.simulator_routine_external_id + "_v2"

        assert last_revision.script is not None
        assert last_revision.script == seed_rev2.script

        assert last_revision.configuration is not None
        assert last_revision.configuration == seed_rev2.configuration

    def test_retrieve_routine_revision(
        self,
        cognite_client: CogniteClient,
        seed_simulator_routine_revisions: tuple[SimulatorRoutineRevision, SimulatorRoutineRevision],
        seed_resource_names: ResourceNames,
    ) -> None:
        simulator_routine_external_id = seed_resource_names.simulator_routine_external_id
        revisions_all = cognite_client.simulators.routines.revisions.list(
            routine_external_ids=[simulator_routine_external_id], all_versions=True
        )

        assert len(revisions_all) == 2

        rev1 = revisions_all[0]
        rev2 = revisions_all[1]

        rev1_retrieve = cognite_client.simulators.routines.revisions.retrieve(external_ids=rev1.external_id)
        assert rev1_retrieve is not None
        assert rev1_retrieve.external_id == rev1.external_id
        rev2_retrieve = cognite_client.simulators.routines.revisions.retrieve(external_ids=rev2.external_id)
        assert rev2_retrieve is not None
        assert rev2_retrieve.external_id == rev2.external_id

    def test_create_routine_revision(
        self,
        cognite_client: CogniteClient,
        seed_simulator_routines: list[SimulatorRoutine],
        seed_resource_names: ResourceNames,
    ) -> None:
        routine_external_id = seed_resource_names.simulator_routine_external_id

        revisions = cognite_client.simulators.routines.revisions.create(
            [
                create_simulator_routine_revision(
                    external_id=f"{routine_external_id}_v3",
                    routine_external_id=routine_external_id,
                ),
                SimulatorRoutineRevisionWrite.load(
                    {
                        **SIMULATOR_ROUTINE_REVISION.dump(),
                        "externalId": f"{routine_external_id}_1_v1",
                        "routineExternalId": f"{routine_external_id}_1",
                    }
                ),
            ]
        )
        assert len(revisions) == 2

        revision_1 = revisions[0]
        assert revision_1 is not None
        assert revision_1.external_id == f"{routine_external_id}_v3"
        assert revision_1.configuration is not None
        assert revision_1.configuration == SIMULATOR_ROUTINE_REVISION_CONFIG
        assert revision_1.script == SIMULATOR_ROUTINE_REVISION_SCRIPT
        assert revision_1.created_time
        assert revision_1.created_time > int(time.time() - 60) * 1000

        revision_2 = revisions[1]
        assert revision_2 is not None
        assert revision_2.external_id == f"{routine_external_id}_1_v1"
        assert revision_2.configuration is not None
        assert revision_2.configuration.inputs is not None
        assert revision_2.configuration.inputs[0].reference_id == "CWT"
        assert type(revision_2.configuration.inputs[0]) is SimulatorRoutineInputConstant
        assert revision_2.configuration.outputs is not None
        assert revision_2.configuration.outputs[0].reference_id == "ST"
        assert revision_2.script is not None
        assert revision_2.script[0].steps[0].arguments["referenceId"] == "CWT"
