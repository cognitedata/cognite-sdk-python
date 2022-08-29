import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.contextualization import JobStatus

COGNITE_CLIENT = CogniteClient()
VAPI = COGNITE_CLIENT.vision


@pytest.fixture(scope="class")
def cognite_client() -> CogniteClient:
    return CogniteClient()


@pytest.fixture(scope="class")
def file_id(cognite_client: CogniteClient) -> int:
    # Create a test file
    name = "vision_asset_detection_test_file"
    file = cognite_client.files.create(FileMetadata(external_id=name, name=name), overwrite=True)[0]
    yield file.id

    cognite_client.files.delete(id=file.id)


class TestAssetDetection:
    def test_asset_detection(self, file_id):
        response = VAPI.detect_assets_in_files(files=[{"file_id": file_id}])
        assert response is not None
        assert response.job_id > 0
        assert response.job_id == response.job_id
        assert response.status == JobStatus.QUEUED.value
        assert len(response.items) == 1
        assert response.items[0].file_id == file_id
        assert response.status_time > 0
        assert response.created_time > 0

        response = VAPI.retrieve_detected_assets_in_files_job(job_id=response.job_id)
        assert response is not None
        assert response.job_id > 0
        assert response.status_time > 0
        assert response.created_time > 0

        if response.status == JobStatus.COMPLETED.value:
            assert response.items is not None, response
