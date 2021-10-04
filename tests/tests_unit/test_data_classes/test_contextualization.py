from unittest.mock import patch

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import ContextualizationJob
from tests.utils import set_env_var


@pytest.fixture()
def job():
    with set_env_var("COGNITE_API_KEY", "BLA"):
        return ContextualizationJob(job_id=123, status="Queued", cognite_client=CogniteClient())


def mock_update_status_running(self):
    self.status = "Running"
    return self.status


def mock_update_status_completed(self):
    self.status = "Completed"
    return self.status


class TestContextualizationJob:
    @patch("cognite.client.data_classes.ContextualizationJob.update_status", new=mock_update_status_running)
    def test_wait_for_completion_running(self, job):
        job.wait_for_completion(timeout=1)
        assert job.status == "Running"

    @patch("cognite.client.data_classes.ContextualizationJob.update_status", new=mock_update_status_completed)
    def test_wait_for_completion_completed(self, job):
        job.wait_for_completion(timeout=1)
        assert job.status == "Completed"
