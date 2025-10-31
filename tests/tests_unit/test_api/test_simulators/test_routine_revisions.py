import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes.simulators import (
    SimulatorRoutineConfiguration,
    SimulatorRoutineInputConstant,
    SimulatorRoutineKind,
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionWrite,
    SimulatorRoutineStage,
)
from tests.tests_unit.test_api.test_simulators.conftest import add_mocked_request
from tests.utils import jsgz_load

TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS = {
    "id": 1,
    "externalId": "sdk-test-routine_v1",
    "simulatorExternalId": "TestSim",
    "simulatorIntegrationExternalId": "sdk-test-integration",
    "modelExternalId": "sdk-test-model",
    "dataSetId": 1,
    "createdByUserId": "1",
    "versionNumber": 1,
    "createdTime": 1,
    "lastUpdatedTime": 1,
}


class TestRoutineRevisions:
    @pytest.mark.parametrize(
        "list_params,mock_response_fields,expected_revision,expected_request_body",
        [
            pytest.param(
                {
                    "routine_external_ids": ["sdk-test-routine"],
                    "kind": SimulatorRoutineKind.LONG,
                },
                {
                    "externalId": "sdk-test-routine_v1",
                    "routineExternalId": "sdk-test-routine",
                    "script": [],
                    "kind": "long",
                    "configuration": {
                        "logicalCheck": [],
                        "steadyStateDetection": [],
                        "inputs": [
                            {
                                "name": "param1",
                                "referenceId": "ref1",
                                "value": 10,
                                "valueType": "DOUBLE",
                            }
                        ],
                        "outputs": [],
                    },
                },
                SimulatorRoutineRevision(
                    id=1,
                    external_id="sdk-test-routine_v1",
                    routine_external_id="sdk-test-routine",
                    simulator_external_id="TestSim",
                    simulator_integration_external_id="sdk-test-integration",
                    model_external_id="sdk-test-model",
                    version_number=1,
                    data_set_id=1,
                    created_by_user_id="1",
                    script=[],
                    kind=SimulatorRoutineKind.LONG,
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[
                            SimulatorRoutineInputConstant(
                                name="param1", reference_id="ref1", value=10, value_type="DOUBLE"
                            )
                        ],
                        outputs=[],
                    ),
                    created_time=1,
                ),
                {
                    "filter": {"allVersions": False, "kind": "long", "routineExternalIds": ["sdk-test-routine"]},
                    "includeAllFields": False,
                    "limit": 20,
                    "cursor": None,
                },
                id="list by routine_external_ids and kind",
            ),
        ],
    )
    def test_list_routine_revisions(
        self,
        cognite_client: CogniteClient,
        rsps: RequestsMock,
        list_params: dict,
        mock_response_fields: dict,
        expected_revision: SimulatorRoutineRevision,
        expected_request_body: dict,
    ) -> None:
        # Arrange
        def request_callback(_: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS,
                **mock_response_fields,
            }
            return (200, response_item)

        add_mocked_request(
            rsps, cognite_client.simulators.routines.revisions._get_base_url_with_base_path(), request_callback
        )

        # Act
        listed_revisions = cognite_client.simulators.routines.revisions.list(**list_params)

        # Assert
        assert len(listed_revisions) == 1
        assert listed_revisions[0].dump() == expected_revision.dump()
        assert expected_request_body == jsgz_load(rsps.calls[0].request.body)

    @pytest.mark.parametrize(
        "retrieve_params,mock_response_fields,expected_revision,expected_request_body",
        [
            pytest.param(
                {
                    "external_ids": ["sdk-test-routine_v1"],
                },
                {
                    "externalId": "sdk-test-routine_v1",
                    "routineExternalId": "sdk-test-routine",
                    "script": [],
                    "kind": "long",
                    "configuration": {
                        "logicalCheck": [],
                        "steadyStateDetection": [],
                        "inputs": [
                            {
                                "name": "param1",
                                "referenceId": "ref1",
                                "value": 10,
                                "valueType": "DOUBLE",
                            }
                        ],
                        "outputs": [],
                    },
                },
                SimulatorRoutineRevision(
                    id=1,
                    external_id="sdk-test-routine_v1",
                    routine_external_id="sdk-test-routine",
                    simulator_external_id="TestSim",
                    simulator_integration_external_id="sdk-test-integration",
                    model_external_id="sdk-test-model",
                    version_number=1,
                    data_set_id=1,
                    created_by_user_id="1",
                    script=[],
                    kind=SimulatorRoutineKind.LONG,
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[
                            SimulatorRoutineInputConstant(
                                name="param1", reference_id="ref1", value=10, value_type="DOUBLE"
                            )
                        ],
                        outputs=[],
                    ),
                    created_time=1,
                ),
                {"items": [{"externalId": "sdk-test-routine_v1"}]},
                id="retrieve by external_ids (long)",
            ),
        ],
    )
    def test_retrieve_routine_revisions(
        self,
        cognite_client: CogniteClient,
        rsps: RequestsMock,
        retrieve_params: dict,
        mock_response_fields: dict,
        expected_revision: SimulatorRoutineRevision,
        expected_request_body: dict,
    ) -> None:
        # Arrange
        def request_callback(_: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS,
                **mock_response_fields,
            }
            return (200, response_item)

        add_mocked_request(
            rsps, cognite_client.simulators.routines.revisions._get_base_url_with_base_path(), request_callback
        )

        # Act
        retrieved_revisions = cognite_client.simulators.routines.revisions.retrieve(**retrieve_params)

        # Assert
        assert len(retrieved_revisions) == 1
        assert retrieved_revisions[0].dump() == expected_revision.dump()
        assert expected_request_body == jsgz_load(rsps.calls[0].request.body)

    @pytest.mark.parametrize(
        "write_input,mock_response_fields,expected_revision,expected_request_body",
        [
            pytest.param(
                SimulatorRoutineRevisionWrite(
                    external_id="sdk-test-routine_v2",
                    routine_external_id="sdk-test-routine",
                    script=[SimulatorRoutineStage(description="", order=0, steps=[])],
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[
                            SimulatorRoutineInputConstant(
                                name="param1", reference_id="ref1", value=10, value_type="DOUBLE"
                            )
                        ],
                        outputs=[],
                    ),
                ),
                {
                    **TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS,
                    "kind": "long",
                },
                SimulatorRoutineRevision(
                    id=1,
                    external_id="sdk-test-routine_v2",
                    routine_external_id="sdk-test-routine",
                    simulator_external_id="TestSim",
                    simulator_integration_external_id="sdk-test-integration",
                    model_external_id="sdk-test-model",
                    version_number=1,
                    data_set_id=1,
                    created_by_user_id="1",
                    script=[
                        SimulatorRoutineStage(
                            description="",
                            order=0,
                            steps=[],
                        )
                    ],
                    kind=SimulatorRoutineKind.LONG,
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[
                            SimulatorRoutineInputConstant(
                                name="param1", reference_id="ref1", value=10, value_type="DOUBLE"
                            )
                        ],
                        outputs=[],
                    ),
                    created_time=1,
                ),
                {
                    "items": [
                        {
                            "externalId": "sdk-test-routine_v2",
                            "routineExternalId": "sdk-test-routine",
                            "script": [{"description": "", "order": 0, "steps": []}],
                            "configuration": {
                                "logicalCheck": [],
                                "dataSampling": {"enabled": False},
                                "schedule": {"enabled": False},
                                "steadyStateDetection": [],
                                "inputs": [
                                    {
                                        "name": "param1",
                                        "referenceId": "ref1",
                                        "value": 10,
                                        "valueType": "DOUBLE",
                                    }
                                ],
                                "outputs": None,
                            },
                        }
                    ]
                },
                id="create routine revision (long)",
            ),
        ],
    )
    def test_create_routine_revision(
        self,
        cognite_client: CogniteClient,
        rsps: RequestsMock,
        write_input: SimulatorRoutineRevisionWrite,
        mock_response_fields: dict,
        expected_revision: SimulatorRoutineRevision,
        expected_request_body: dict,
    ) -> None:
        # Arrange
        def request_callback(request_payload: dict) -> tuple[int, dict]:
            response_item = {
                **mock_response_fields,
                **request_payload["items"][0],
            }
            return (201, response_item)

        add_mocked_request(
            rsps, cognite_client.simulators.routines.revisions._get_base_url_with_base_path(), request_callback
        )

        # Act
        created_revision = cognite_client.simulators.routines.revisions.create(write_input)

        # Assert
        assert created_revision.dump() == expected_revision.dump()
        assert expected_request_body == jsgz_load(rsps.calls[0].request.body)
