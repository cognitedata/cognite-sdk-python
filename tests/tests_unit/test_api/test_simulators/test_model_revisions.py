import pytest
from pytest_httpx import HTTPXMock

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client.data_classes.simulators import SimulatorModelRevision, SimulatorModelRevisionWrite
from tests.utils import get_url, jsgz_load

TEST_MODEL_REVISION_ITEM_RESPONSE_FIELDS = {
    "id": 1,
    "dataSetId": 1,
    "simulatorExternalId": "TestSim",
    "createdByUserId": "test-user",
    "status": "unknown",
    "versionNumber": 1,
    "logId": 1,
    "createdTime": 1,
    "lastUpdatedTime": 1,
}


class TestModelRevisions:
    @pytest.mark.parametrize(
        "write_input,delete_oldest,expected_request_body",
        [
            pytest.param(
                SimulatorModelRevisionWrite(
                    external_id="sdk-test-revision",
                    model_external_id="sdk-test-model",
                    file_id=1,
                ),
                False,
                {
                    "deleteOldest": False,
                    "items": [
                        {
                            "externalId": "sdk-test-revision",
                            "modelExternalId": "sdk-test-model",
                            "fileId": 1,
                        }
                    ],
                },
                id="create_model_revision_default_delete_oldest",
            ),
            pytest.param(
                SimulatorModelRevisionWrite(
                    external_id="sdk-test-revision",
                    model_external_id="sdk-test-model",
                    file_id=1,
                ),
                True,
                {
                    "deleteOldest": True,
                    "items": [
                        {
                            "externalId": "sdk-test-revision",
                            "modelExternalId": "sdk-test-model",
                            "fileId": 1,
                        }
                    ],
                },
                id="create_model_revision_with_delete_oldest",
            ),
            pytest.param(
                SimulatorModelRevisionWrite(
                    external_id="sdk-test-revision",
                    model_external_id="sdk-test-model",
                    file_id=1,
                ),
                None,
                {
                    "deleteOldest": False,
                    "items": [
                        {
                            "externalId": "sdk-test-revision",
                            "modelExternalId": "sdk-test-model",
                            "fileId": 1,
                        }
                    ],
                },
                id="create_model_revision_omits_delete_oldest",
            ),
        ],
    )
    def test_create_model_revision(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        write_input: SimulatorModelRevisionWrite,
        delete_oldest: bool | None,
        expected_request_body: dict,
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=get_url(async_client.simulators.models.revisions, "/simulators/models/revisions"),
            json={
                "items": [
                    {
                        **TEST_MODEL_REVISION_ITEM_RESPONSE_FIELDS,
                        **write_input.dump(),
                    }
                ]
            },
            status_code=201,
        )

        if delete_oldest is None:
            created_revision = cognite_client.simulators.models.revisions.create(write_input)
        else:
            created_revision = cognite_client.simulators.models.revisions.create(
                write_input, delete_oldest=delete_oldest
            )

        assert isinstance(created_revision, SimulatorModelRevision)
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)
