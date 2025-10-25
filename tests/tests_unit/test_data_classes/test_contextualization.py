from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

import pytest

from cognite.client._cognite_client import AsyncCogniteClient
from cognite.client.data_classes.contextualization import (
    ConnectionFlags,
    DetectJobBundle,
    DiagramDetectConfig,
    EntityMatchingPredictionResult,
)
from cognite.client.testing import monkeypatch_async_cognite_client


@pytest.fixture
def mock_base_job_response() -> dict[str, Any]:
    return {
        "status": "Completed",
        "createdTime": 1666601590000,
        "startTime": 1666601590000,
        "statusTime": 1666601590000,
        "jobId": 1,
    }


@pytest.fixture()
def job(async_client: AsyncCogniteClient) -> EntityMatchingPredictionResult:
    return EntityMatchingPredictionResult(
        job_id=123,
        status="Queued",
        cognite_client=async_client,
        status_time=1,
        created_time=1,
        start_time=123,
        error_message=None,
    )


async def mock_update_status_running(self: EntityMatchingPredictionResult) -> str:
    self.status = "Running"
    return self.status


async def mock_update_status_completed(self: EntityMatchingPredictionResult) -> str:
    self.status = "Completed"
    return self.status


class TestEntityMatchingPredictionResult:
    @patch(
        "cognite.client.data_classes.EntityMatchingPredictionResult.update_status_async",
        new=mock_update_status_running,
    )
    async def test_wait_for_completion_running(self, job: EntityMatchingPredictionResult) -> None:
        assert job.status == "Queued"
        await job.wait_for_completion_async(timeout=0.01, interval=0.01)
        assert job.status == "Running"

    @patch(
        "cognite.client.data_classes.EntityMatchingPredictionResult.update_status_async",
        new=mock_update_status_completed,
    )
    async def test_wait_for_completion_completed(self, job: EntityMatchingPredictionResult) -> None:
        assert job.status == "Queued"
        await job.wait_for_completion_async()
        assert job.status == "Completed"


class TestJobBundle:
    async def test_DetectJobBundle_completed(self, mock_base_job_response: dict[str, Any]) -> None:
        _job_ids = [1, 2]
        completed_job_mock = mock_base_job_response
        with monkeypatch_async_cognite_client() as mock_client:
            requestMock = Mock(json=lambda: {"items": [completed_job_mock]})
            mock_client.diagrams._get.return_value = Mock(
                json=lambda: {
                    "createdTime": 1337,
                    "items": [{"annotations": [], "fileId": 101}],
                    "jobId": 1,
                    "status": "Completed",
                    "statusCount": {"completed": 1},
                    "statusTime": 1337,
                }
            )
            mock_client.diagrams._post.return_value = requestMock

            job_bundle = DetectJobBundle(cognite_client=mock_client, job_ids=_job_ids)
            assert job_bundle.job_ids == _job_ids
            s, f = await job_bundle.get_result()
            assert len(s) == 2
            assert len(f) == 0
            assert s[0]["status"] == "Completed"
            assert s[1]["status"] == "Completed"

    @patch("cognite.client.data_classes.contextualization.DetectJobBundle._WAIT_TIME", 0)
    async def test_DetectJobBundle_one_running(self, mock_base_job_response: dict[str, Any]) -> None:
        completed_job_mock = mock_base_job_response
        running_job_mock = {**mock_base_job_response, **{"status": "Running", "jobId": 2}}
        running_job_completed_mock = {**mock_base_job_response, **{"status": "Completed", "jobId": 2}}
        with monkeypatch_async_cognite_client() as mock_client:
            requestMock = Mock(json=lambda: {"items": [completed_job_mock, running_job_mock]})
            requestMock_second_round = Mock(json=lambda: {"items": [completed_job_mock, running_job_completed_mock]})

            mock_client.diagrams._get.return_value = Mock(
                json=lambda: {
                    "createdTime": 1337,
                    "items": [{"annotations": [], "fileId": 101}],
                    "jobId": 1,
                    "status": "Completed",
                    "statusCount": {"completed": 1},
                    "statusTime": 1337,
                }
            )
            mock_client.diagrams._post.side_effect = [requestMock, requestMock_second_round]
            job_bundle = DetectJobBundle(cognite_client=mock_client, job_ids=[1, 2])
            s, f = await job_bundle.get_result()
            assert len(f) == 0
            assert len(s) == 2
            assert s[0]["status"] == "Completed"
            assert s[1]["status"] == "Completed"
            assert job_bundle._WAIT_TIME == 2  # NOTE: Overwritten to 0, then added 2
            assert mock_client.diagrams._post.call_count == 2

    async def test_DetectJobBundle_failing(self, mock_base_job_response: dict[str, Any]) -> None:
        completed_job_mock = mock_base_job_response
        failed_job_mock = {
            **mock_base_job_response,
            **{
                "status": "Failed",
                "jobId": 1,
                "errorMessage": "JobFailedException: Oops",
            },
        }
        with monkeypatch_async_cognite_client() as mock_client:
            requestMock = Mock(json=lambda: {"items": [failed_job_mock, completed_job_mock]})

            mock_client.diagrams._get.side_effect = [
                Mock(
                    json=lambda: {
                        "createdTime": 1337,
                        "items": [{"annotations": [], "fileId": 101}],
                        "jobId": 1,
                        "status": "Completed",
                        "statusCount": {"completed": 1},
                        "statusTime": 1337,
                    }
                ),
                Mock(
                    json=lambda: {
                        "createdTime": 1337,
                        "items": [{"errorMessage": "somethingWentWrong"}],
                        "jobId": 1,
                        "status": "Failed",
                        "statusCount": {"failed": 1},
                        "statusTime": 1337,
                    }
                ),
            ]
            mock_client.diagrams._post.return_value = requestMock
            a = DetectJobBundle(cognite_client=mock_client, job_ids=[1, 2])
            s, f = await a.get_result()
            assert len(s) == 1
            assert len(f) == 1
            assert s[0]["status"] == "Completed"
            assert f[0]["errorMessage"] == "somethingWentWrong"

    def test_DetectJobBundle_div(self) -> None:
        with monkeypatch_async_cognite_client() as mock_client:
            res = DetectJobBundle(cognite_client=mock_client, job_ids=[1, 2])
            assert isinstance(res, DetectJobBundle)

            # With no job_ids
            with pytest.raises(ValueError):
                res = DetectJobBundle(cognite_client=mock_client, job_ids=[])


class TestDiagramDetectConfig:
    def test_connection_flags(self) -> None:
        cf = ConnectionFlags(
            natural_reading_order=True,
            no_text_inbetween=True,
            new_parameter=True,
            excluded_parameter=False,
            newCamelCaseParameter=True,
        )
        expected = [
            "natural_reading_order",
            "no_text_inbetween",
            "new_parameter",
            "newCamelCaseParameter",
        ]
        assert cf.dump() == expected

    @pytest.mark.parametrize(
        "param_name",
        [
            "annotationExtract",
            "annotation_Extract",
            "minFuzzyScore",
        ],
    )
    def test_overlapping_parameter_name(self, param_name: str) -> None:
        kwargs: dict = {param_name: True}
        with pytest.raises(ValueError):
            DiagramDetectConfig(**kwargs)
