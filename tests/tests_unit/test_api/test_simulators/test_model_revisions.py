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
        "delete_kw",
        [
            pytest.param({"delete_oldest": True}, id="create_model_revision_with_delete_oldest"),
            pytest.param({"delete_oldest": False}, id="create_model_revision_without_delete_oldest"),
            pytest.param({}, id="create_model_revision_omits_delete_oldest"),
        ],
    )
    def test_create_model_revision(
        self,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        httpx_mock: HTTPXMock,
        delete_kw: dict,
    ) -> None:
        write_input = SimulatorModelRevisionWrite(
            external_id="sdk-test-revision",
            model_external_id="sdk-test-model",
            file_id=1,
        )
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

        created_revision = cognite_client.simulators.models.revisions.create(write_input, **delete_kw)

        assert isinstance(created_revision, SimulatorModelRevision)
        expected_request_body = {
            "deleteOldest": delete_kw.get("delete_oldest", False),
            "items": [write_input.dump()],
        }
        assert expected_request_body == jsgz_load(httpx_mock.get_requests()[0].content)
