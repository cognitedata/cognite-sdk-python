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
    @pytest.mark.parametrize(
        "write_input,expected_routine,expected_request_body",
        [
            pytest.param(
                SimulatorRoutineWrite(
                    name="sdk-test-routine",
                    model_external_id="sdk-test-model",
                    simulator_integration_external_id="sdk-test-integration",
                    external_id="sdk-test-routine",
                    kind=SimulatorRoutineKind.LONG,
                ),
                SimulatorRoutine(
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
                ),
                {
                    "items": [
                        {
                            "externalId": "sdk-test-routine",
                            "kind": "long",
                            "modelExternalId": "sdk-test-model",
                            "name": "sdk-test-routine",
                            "simulatorIntegrationExternalId": "sdk-test-integration",
                        }
                    ]
                },
                id="create routine (long)",
            ),
        ],
    )
    def test_create_routine(
        self,
        cognite_client: CogniteClient,
        rsps: RequestsMock,
        write_input: SimulatorRoutineWrite,
        expected_routine: SimulatorRoutine,
        expected_request_body: dict,
    ) -> None:
        # Arrange
        def request_callback(request_payload: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_ITEM_RESPONSE_FIELDS,
                **request_payload["items"][0],
            }
            return (201, response_item)

        add_mocked_request(rsps, cognite_client.simulators.routines._get_base_url_with_base_path(), request_callback)

        # Act
        created_routine = cognite_client.simulators.routines.create(write_input)

        # Assert
        assert created_routine == expected_routine
        assert expected_request_body == jsgz_load(rsps.calls[0].request.body)

    @pytest.mark.parametrize(
        "list_params,mock_response_fields,expected_routine,expected_request_body",
        [
            pytest.param(
                {
                    "model_external_ids": ["sdk-test-model"],
                    "kind": SimulatorRoutineKind.LONG,
                },
                {
                    "externalId": "sdk-test-routine",
                    "name": "sdk-test-routine",
                    "modelExternalId": "sdk-test-model",
                    "simulatorIntegrationExternalId": "sdk-test-integration",
                    "kind": "long",
                },
                SimulatorRoutine(
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
                ),
                {"filter": {"modelExternalIds": ["sdk-test-model"], "kind": "long"}, "limit": 25, "cursor": None},
                id="list routines (long)",
            ),
        ],
    )
    def test_list_routines(
        self,
        cognite_client: CogniteClient,
        rsps: RequestsMock,
        list_params: dict,
        mock_response_fields: dict,
        expected_routine: SimulatorRoutine,
        expected_request_body: dict,
    ) -> None:
        # Arrange
        def request_callback(_: dict) -> tuple[int, dict]:
            response_item = {
                **TEST_ROUTINE_ITEM_RESPONSE_FIELDS,
                **mock_response_fields,
            }
            return (200, response_item)

        add_mocked_request(rsps, cognite_client.simulators.routines._get_base_url_with_base_path(), request_callback)

        # Act
        listed_routines = cognite_client.simulators.routines.list(**list_params)

        # Assert
        assert len(listed_routines) == 1
        assert listed_routines[0] == expected_routine
        assert expected_request_body == jsgz_load(rsps.calls[0].request.body)
