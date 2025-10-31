from collections.abc import Generator

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes.simulators import SimulatorRoutineInputConstant, SimulatorRoutineRevision, SimulatorRoutineConfiguration, SimulatorRoutineRevisionWrite, SimulatorRoutineStage, SimulatorRoutineKind

from tests.tests_unit.test_api.test_simulators.conftest import add_mocked_request
from tests.utils import jsgz_load

TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS = {
    "id": 1,
    "externalId": "sdk-test-routine_v1",
    "simulatorExternalId": "TestSim",
    "simulatorIntegrationExternalId": "sdk-test-integration",
    "modelExternalId": "sdk-test-model",
    "kind": "long",
    "dataSetId": 1,
    "createdByUserId": "1",
    "versionNumber": 1,
    "createdTime": 1,
    "lastUpdatedTime": 1,
}

class TestRoutineRevisions:
    @pytest.fixture
    def mock_routines_revisions_read_endpoint(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Generator[RequestsMock, None, None]:
        def request_callback(_: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS,
                "externalId": "sdk-test-routine_v1",
                "routineExternalId": "sdk-test-routine",
                "script": [],
                "configuration": {
                    "logicalCheck": [],
                    "steadyStateDetection": [],
                    "inputs": [{
                        "name": "param1",
                        "referenceId": "ref1",
                        "value": 10,
                        "valueType": "DOUBLE",
                    }],
                    "outputs": [],
                }
            }
            return (200, response_item)
        
        add_mocked_request(rsps, cognite_client.simulators.routines.revisions._get_base_url_with_base_path(), request_callback)

        yield rsps

    @pytest.fixture
    def mock_routine_revisions_create_endpoint(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Generator[RequestsMock, None, None]:
        def request_callback(request_payload: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS,
                **request_payload["items"][0],
            }
            return (201, response_item)
        
        add_mocked_request(rsps, cognite_client.simulators.routines.revisions._get_base_url_with_base_path(), request_callback)

        yield rsps


    def test_list_routine_revisions(self, cognite_client: CogniteClient, mock_routines_revisions_read_endpoint: RequestsMock) -> None:
        listed_revisions = cognite_client.simulators.routines.revisions.list(
            routine_external_ids=["sdk-test-routine"],
            kind=SimulatorRoutineKind.LONG
        )
        assert len(listed_revisions) == 1
        first_revision = listed_revisions[0]
        expected_revision = SimulatorRoutineRevision(
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
                inputs=[SimulatorRoutineInputConstant(
                    name="param1",
                    reference_id="ref1",
                    value=10,
                    value_type="DOUBLE"
                )],
                outputs=[],
            ),
            created_time=1,
        )
        assert first_revision.dump() == expected_revision.dump()
        assert {
            "filter": {
                "allVersions": False,
                "kind": "long",
                "routineExternalIds": ["sdk-test-routine"]
            },
            "includeAllFields": False,
            "limit": 20,
            "cursor": None
        } == jsgz_load(mock_routines_revisions_read_endpoint.calls[0].request.body)

    def test_retrieve_routine_revisions(self, cognite_client: CogniteClient, mock_routines_revisions_read_endpoint: RequestsMock) -> None:
        retrieved_revisions = cognite_client.simulators.routines.revisions.retrieve(
            external_ids=["sdk-test-routine_v1"],
        )
        assert len(retrieved_revisions) == 1
        first_revision = retrieved_revisions[0]
        expected_revision = SimulatorRoutineRevision(
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
                inputs=[SimulatorRoutineInputConstant(
                    name="param1",
                    reference_id="ref1",
                    value=10,
                    value_type="DOUBLE"
                )],
                outputs=[],
            ),
            created_time=1,
        )
        assert first_revision.dump() == expected_revision.dump()
        assert {
            "items": [{
                "externalId": "sdk-test-routine_v1"
            }]
        } == jsgz_load(mock_routines_revisions_read_endpoint.calls[0].request.body)


    def test_create_routine_revision(
        self, cognite_client: CogniteClient, mock_routine_revisions_create_endpoint: RequestsMock
    ) -> None:
        created_revision = cognite_client.simulators.routines.revisions.create(
            SimulatorRoutineRevisionWrite(
                external_id="sdk-test-routine_v2",
                routine_external_id="sdk-test-routine",
                script=[SimulatorRoutineStage(
                    description="",
                    order=0,
                    steps=[]
                )],
                configuration=SimulatorRoutineConfiguration(
                    logical_check=[],
                    inputs=[SimulatorRoutineInputConstant(
                        name="param1",
                        reference_id="ref1",
                        value=10,
                        value_type="DOUBLE"
                    )],
                    outputs=[],
                ),
            )
        )
        expected_revision = SimulatorRoutineRevision(
            id=1,
            external_id="sdk-test-routine_v2",
            routine_external_id="sdk-test-routine",
            simulator_external_id="TestSim",
            simulator_integration_external_id="sdk-test-integration",
            model_external_id="sdk-test-model",
            version_number=1,
            data_set_id=1,
            created_by_user_id="1",
            script=[SimulatorRoutineStage(
                description="",
                order=0,
                steps=[],
            )],
            kind=SimulatorRoutineKind.LONG,
            configuration=SimulatorRoutineConfiguration(
                logical_check=[],
                inputs=[SimulatorRoutineInputConstant(
                    name="param1",
                    reference_id="ref1",
                    value=10,
                    value_type="DOUBLE"
                )],
                outputs=[],
            ),
            created_time=1,
        )
        assert created_revision.dump() == expected_revision.dump()
        assert {
            "items": [{
                "externalId": "sdk-test-routine_v2",
                "routineExternalId": "sdk-test-routine",
                "script": [{
                    "description": "",
                    "order": 0,
                    "steps": []
                }],
                "configuration": {
                    "logicalCheck": [],
                    "dataSampling": {"enabled": False},
                    "schedule": {"enabled": False},
                    "steadyStateDetection": [],
                    "inputs": [{
                        "name": "param1",
                        "referenceId": "ref1",
                        "value": 10,
                        "valueType": "DOUBLE",
                    }],
                    "outputs": None,
                },
            }]
        } == jsgz_load(mock_routine_revisions_create_endpoint.calls[0].request.body)
