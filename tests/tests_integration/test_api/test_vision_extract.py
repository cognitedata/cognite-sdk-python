from collections.abc import Iterator

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.contextualization import (
    FeatureParameters,
    JobStatus,
    PeopleDetectionParameters,
    VisionExtractJob,
    VisionFeature,
)


# TODO(VIS-986): replace this file generator with a hard-coded ID of an actual image
@pytest.fixture(scope="class")
def file_id(cognite_client: CogniteClient, os_and_py_version: str) -> Iterator[int]:
    # Create a test file
    name = "vision_extract_test_file" + os_and_py_version
    file = cognite_client.files.create(FileMetadata(external_id=name, name=name), overwrite=True)[0]
    yield file.id

    cognite_client.files.delete(id=file.id)


class TestVisionExtractAPI:
    def test_extract_integration(self, cognite_client: CogniteClient, file_id: int) -> None:
        VAPI = cognite_client.vision
        job = VAPI.extract(
            features=VisionFeature.PEOPLE_DETECTION,
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
