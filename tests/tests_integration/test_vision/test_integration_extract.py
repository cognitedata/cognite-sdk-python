import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import JobStatus
from cognite.client.data_classes.vision import (
    Feature,
    FeatureParameters,
    PeopleDetectionParameters,
    VisionExtractJob,
)


@pytest.fixture(scope="class")
def file_id(cognite_client: CogniteClient) -> int:
    # Create a test file
    file = cognite_client.files.retrieve(external_id="vision_extract_test_file")
    yield file.id

class TestExtract_integration:
    def test_extract_integration(self, cognite_client: CogniteClient, file_id: int) -> None:
        VAPI = cognite_client.vision
        job = VAPI.extract(
            features=Feature.PEOPLE_DETECTION,
            file_ids=[file_id],
            parameters=FeatureParameters(people_detection_parameters=PeopleDetectionParameters(threshold=0.1)),
        )
        assert isinstance(job, VisionExtractJob)
        assert job.job_id > 0
        assert JobStatus(job.status) == JobStatus.QUEUED
        assert len(job.items) == 1
        assert job.items[0]["fileId"] == file_id
        assert job.status_time > 0
        assert job.created_time > 0
