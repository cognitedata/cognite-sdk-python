import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.simulators import (
    SimulatorRoutineConfiguration,
    SimulatorRoutineRevision,
    SimulatorRoutineRevisionWrite,
)
from tests.utils import get_url, jsgz_load

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
                    "kind": "long",
                },
                {
                    "externalId": "sdk-test-routine_v1",
                    "routineExternalId": "sdk-test-routine",
                    "script": [],
                    "kind": "long",
                    "configuration": {
                        "logicalCheck": [],
                        "steadyStateDetection": [],
                        "inputs": [],
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
                    kind="long",
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[],
                        outputs=[],
                    ),
                    created_time=1,
                ),
                {
                    "filter": {"allVersions": False, "kind": "long", "routineExternalIds": ["sdk-test-routine"]},
                    "includeAllFields": False,
                    "limit": 20,
                },
                id="list by routine_external_ids and kind",
            ),
        ],
    )
    def test_list_routine_revisions(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        list_params: dict,
        mock_response_fields: dict,
        expected_revision: SimulatorRoutineRevision,
        expected_request_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators.routines.revisions, "/simulators/routines/revisions/list"),
            json={"items": [TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS | mock_response_fields]},
            status_code=200,
        )
        listed_revisions = cognite_client.simulators.routines.revisions.list(**list_params)

        assert len(listed_revisions) == 1
        assert listed_revisions[0].dump() == expected_revision.dump()
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)

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
                        "inputs": [],
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
                    kind="long",
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[],
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
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        retrieve_params: dict,
        mock_response_fields: dict,
        expected_revision: SimulatorRoutineRevision,
        expected_request_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators.routines.revisions, "/simulators/routines/revisions/byids"),
            json={"items": [TEST_ROUTINE_REV_ITEM_RESPONSE_FIELDS | mock_response_fields]},
            status_code=200,
        )
        retrieved_revisions = cognite_client.simulators.routines.revisions.retrieve(**retrieve_params)

        assert len(retrieved_revisions) == 1
        assert retrieved_revisions[0].dump() == expected_revision.dump()
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)

    @pytest.mark.parametrize(
        "write_input,mock_response_fields,expected_revision,expected_request_body",
        [
            pytest.param(
                SimulatorRoutineRevisionWrite(
                    external_id="sdk-test-routine_v2",
                    routine_external_id="sdk-test-routine",
                    script=[],
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[],
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
                    script=[],
                    kind="long",
                    configuration=SimulatorRoutineConfiguration(
                        logical_check=[],
                        inputs=[],
                        outputs=[],
                    ),
                    created_time=1,
                ),
                {
                    "items": [
                        {
                            "externalId": "sdk-test-routine_v2",
                            "routineExternalId": "sdk-test-routine",
                            "script": None,
                            "configuration": {
                                "logicalCheck": [],
                                "dataSampling": {"enabled": False},
                                "schedule": {"enabled": False},
                                "steadyStateDetection": [],
                                "inputs": None,
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
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        write_input: SimulatorRoutineRevisionWrite,
        mock_response_fields: dict,
        expected_revision: SimulatorRoutineRevision,
        expected_request_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators.routines.revisions, "/simulators/routines/revisions"),
            json={"items": [mock_response_fields | write_input.dump()]},
            status_code=201,
        )
        created_revision = cognite_client.simulators.routines.revisions.create(write_input)

        assert created_revision.dump() == expected_revision.dump()
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)
