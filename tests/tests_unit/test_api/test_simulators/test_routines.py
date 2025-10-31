from collections.abc import Generator

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes.simulators import SimulatorRoutine, SimulatorRoutineKind, SimulatorRoutineWrite

from tests.tests_unit.test_api.test_simulators.conftest import add_mocked_request
from tests.utils import jsgz_load

TEST_ROUTINE_ITEM_RESPONSE_FIELDS = {
    "id": 1,
    "dataSetId": 1,
    "simulatorExternalId": "TestSim",
    "createdTime": 1,
    "lastUpdatedTime": 1,
}

class TestRoutines:
    @pytest.fixture
    def mock_routines_create_endpoint(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Generator[RequestsMock, None, None]:
        def request_callback(request_payload: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_ITEM_RESPONSE_FIELDS,
                **request_payload["items"][0],
            }
            return (201, response_item)
        
        add_mocked_request(rsps, cognite_client.simulators.routines._get_base_url_with_base_path(), request_callback)

        yield rsps

    @pytest.fixture
    def mock_routines_list_endpoint(self, rsps: RequestsMock, cognite_client: CogniteClient) -> Generator[RequestsMock, None, None]:
        def request_callback(_: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_ITEM_RESPONSE_FIELDS,
                **{
                    "externalId": "sdk-test-routine",
                    "name": "sdk-test-routine",
                    "modelExternalId": "sdk-test-model",
                    "simulatorIntegrationExternalId": "sdk-test-integration",
                    "kind": "long",
                },
            }
            return (200, response_item)
        
        add_mocked_request(rsps, cognite_client.simulators.routines._get_base_url_with_base_path(), request_callback)

        yield rsps

    def test_create_long_routine(
        self, cognite_client: CogniteClient, mock_routines_create_endpoint: RequestsMock
    ) -> None:
        created_routine = cognite_client.simulators.routines.create(
            SimulatorRoutineWrite(
                name="sdk-test-routine",
                model_external_id="sdk-test-model",
                simulator_integration_external_id="sdk-test-integration",
                external_id="sdk-test-routine",
                kind=SimulatorRoutineKind.LONG,
            )
        )
        assert created_routine == SimulatorRoutine(
            id=1,
            external_id="sdk-test-routine",
            name="sdk-test-routine",
            simulator_external_id="TestSim",
            data_set_id=1,
            model_external_id="sdk-test-model",
            simulator_integration_external_id="sdk-test-integration",
            kind=SimulatorRoutineKind.LONG,
            created_time=1,
            last_updated_time=1,
        )
        assert {
            "items": [{
                "externalId": "sdk-test-routine",        
                "kind": "long",                          
                "modelExternalId": "sdk-test-model",     
                "name": "sdk-test-routine",              
                "simulatorIntegrationExternalId": "sdk-test-integration"
            }]
        } == jsgz_load(mock_routines_create_endpoint.calls[0].request.body)


    def test_list_long_routines(self, cognite_client: CogniteClient, mock_routines_list_endpoint: RequestsMock) -> None:
        listed_routines = cognite_client.simulators.routines.list(
            model_external_ids=["sdk-test-model"],
            kind=SimulatorRoutineKind.LONG
        )
        assert len(listed_routines) == 1
        assert listed_routines[0] == SimulatorRoutine(
            id=1,
            external_id="sdk-test-routine",
            name="sdk-test-routine",
            simulator_external_id="TestSim",
            data_set_id=1,
            model_external_id="sdk-test-model",
            simulator_integration_external_id="sdk-test-integration",
            kind=SimulatorRoutineKind.LONG,
            created_time=1,
            last_updated_time=1,
        )
        assert {
            "filter": {
                "modelExternalIds": ["sdk-test-model"],
                "kind": "long"
            },
            "limit": 25,
            "cursor": None
        } == jsgz_load(mock_routines_list_endpoint.calls[0].request.body)
