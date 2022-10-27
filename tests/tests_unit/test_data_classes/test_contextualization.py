from typing import Any, Dict
from unittest.mock import Mock, patch

import pytest
from cognite.client.data_classes import ContextualizationJob
from cognite.client.data_classes.contextualization import JobBundle
from cognite.client.testing import monkeypatch_cognite_client


@pytest.fixture
def mock_base_job_response() -> Dict[str, Any]:
    return {
        "status": "Completed",
        "createdTime": 1666601590000,
        "startTime": 1666601590000,
        "statusTime": 1666601590000,
        "jobId": 1,
    }

@pytest.fixture()
def job(cognite_client):
    return ContextualizationJob(job_id=123, status="Queued", cognite_client=cognite_client)


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



class TestJobBundle:
    def test_JobBundle_completed(self, mock_base_job_response) -> None:
        _job_ids = [1,2]
        completed_job_mock = mock_base_job_response
        with monkeypatch_cognite_client() as mock_client:
            requestMock = Mock(json=lambda: {"items": [
                        completed_job_mock,
                        ]
                })
            mock_client.diagrams._get.return_value = Mock(json=lambda: {'createdTime': 1337, 'items': [{'annotations': [], 'fileId': 101}], 'jobId': 1,'status': 'Completed', 'statusCount': {'completed': 1}, 'statusTime': 1337})
            mock_client.diagrams._post.return_value = requestMock
            
            a = JobBundle(cognite_client=mock_client, job_ids= _job_ids)
            assert a.job_ids == _job_ids
            assert a.result[0]["status"] == "Completed"

    @patch("cognite.client.data_classes.contextualization.JobBundle._WAIT_TIME", 0)
    def test_JobBundle_one_running(self, mock_base_job_response) -> None:
        completed_job_mock = mock_base_job_response
        running_job_mock = {**mock_base_job_response, **{"status":"Running","jobId": 2}}
        running_job_completed_mock = {**mock_base_job_response, **{"status":"Completed","jobId": 2}}
        with monkeypatch_cognite_client() as mock_client:
            requestMock = Mock(json=lambda: {
                    "items": [completed_job_mock, running_job_mock]
                })
            requestMock_second_round = Mock(json=lambda: {
                    "items": [completed_job_mock, running_job_completed_mock]
                })

            mock_client.diagrams._get.return_value = Mock(json=lambda: {'createdTime': 1337, 'items': [{'annotations': [], 'fileId': 101}], 'jobId': 1,'status': 'Completed', 'statusCount': {'completed': 1}, 'statusTime': 1337})
            mock_client.diagrams._post.side_effect = [requestMock,requestMock_second_round]
            a = JobBundle(cognite_client=mock_client, job_ids= [1,2])
            assert a.result[0]["status"] == "Completed"
            assert a.result[1]["status"] == "Completed"
            assert a._WAIT_TIME == 4

            assert mock_client.diagrams._post.call_count == 2
            assert JobBundle.back_off.call_count == 1
            
    def test_JobBundle_faling(self, mock_base_job_response) -> None:
        failed_job_mock = {**mock_base_job_response, **{"status":"Failed","jobId": 1, "errorMessage": "JobFailedException: Oops",}}
        running_job_mock = {**mock_base_job_response, **{"status":"Running","jobId": 2}}
        with monkeypatch_cognite_client() as mock_client:
            requestMock = Mock(json=lambda: {
                    "items": [failed_job_mock,running_job_mock]
                })

            mock_client.diagrams._get.side_effect = [Mock(json=lambda: {'createdTime': 1337, 'items': [{'annotations': [], 'fileId': 101}], 'jobId': 1,'status': 'Completed', 'statusCount': {'completed': 1}, 'statusTime': 1337}), Mock(json=lambda: {'createdTime': 1337, 'items': [{'annotations': [], 'fileId': 101}], 'jobId': 1,'status': 'Failed', 'statusCount': {'failed': 1}, 'statusTime': 1337})]
            mock_client.diagrams._post.return_value = requestMock
            a = JobBundle(cognite_client=mock_client, job_ids= [1,2])
            