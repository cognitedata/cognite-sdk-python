import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.simulators import SimulatorRoutine, SimulatorRoutineWrite
from tests.utils import get_url, jsgz_load

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
                    kind="long",
                ),
                SimulatorRoutine(
                    id=1,
                    external_id="sdk-test-routine",
                    name="sdk-test-routine",
                    simulator_external_id="TestSim",
                    data_set_id=1,
                    model_external_id="sdk-test-model",
                    simulator_integration_external_id="sdk-test-integration",
                    kind="long",
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
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        write_input: SimulatorRoutineWrite,
        expected_routine: SimulatorRoutine,
        expected_request_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators.routines, "/simulators/routines"),
            json={
                "items": [
                    {
                        **TEST_ROUTINE_ITEM_RESPONSE_FIELDS,
                        **write_input.dump(),
                    }
                ]
            },
            status_code=201,
        )

        created_routine = cognite_client.simulators.routines.create(write_input)

        assert created_routine == expected_routine
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)

    @pytest.mark.parametrize(
        "list_params,mock_response_fields,expected_routine,expected_request_body",
        [
            pytest.param(
                {
                    "model_external_ids": ["sdk-test-model"],
                    "kind": "long",
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
                    kind="long",
                    created_time=1,
                    last_updated_time=1,
                ),
                {"filter": {"modelExternalIds": ["sdk-test-model"], "kind": "long"}, "limit": 25},
                id="list routines (long)",
            ),
        ],
    )
    def test_list_routines(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        list_params: dict,
        mock_response_fields: dict,
        expected_routine: SimulatorRoutine,
        expected_request_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators.routines, "/simulators/routines/list"),
            json={
                "items": [
                    {
                        **TEST_ROUTINE_ITEM_RESPONSE_FIELDS,
                        **mock_response_fields,
                    }
                ]
            },
            status_code=200,
        )

        listed_routines = cognite_client.simulators.routines.list(**list_params)

        assert len(listed_routines) == 1
        assert listed_routines[0] == expected_routine
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)
