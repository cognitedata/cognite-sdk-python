from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from cognite.client import CogniteClient
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client.data_classes.contextualization import ContextualizationJob, DetectJobBundle


@pytest.fixture
def base_job_response() -> dict[str, Any]:
    return {
        "status": "Completed",
        "createdTime": 1666601590000,
        "startTime": 1666601590000,
        "statusTime": 1666601590000,
        "jobId": 1,
    }


class TestPNIDParsingUnit:
    @patch.object(DiagramsAPI, "_post")
    @patch.object(
        DiagramsAPI, "_run_job", return_value=ContextualizationJob(job_id=1, model_id=101, status="Completed")
    )
    @patch.object(DiagramsAPI, "_get")
    def test_run_diagram_detect(
        self,
        mocked_diagrams_get: MagicMock,
        mocked_diagrams_run_job: MagicMock,
        mocked_diagrams_post: MagicMock,
        base_job_response: dict[str, Any],
        cognite_client: CogniteClient,
        monkeypatch,
    ):
        entities = [{"name": "YT-96122"}, {"name": "XE-96125", "ee": 123}, {"name": "XWDW-9615"}]
        file_ids = [1, 2, 3]

        diagram_get_result = Mock(
            json=lambda: {
                "createdTime": 1337,
                "items": [{"annotations": [], "fileId": 101}],
                "jobId": 1,
                "status": "Completed",
                "statusCount": {"completed": 1},
                "statusTime": 1337,
            }
        )
        mocked_diagrams_get.return_value = diagram_get_result

        mocked_diagrams_post.return_value = Mock(
            json=lambda: {
                "items": [
                    base_job_response,
                ]
            }
        )
        job = cognite_client.diagrams.detect(file_ids=file_ids, entities=entities)
        assert isinstance(job, ContextualizationJob)

        # Enable multiple jobs
        job_bundle, unposted_jobs = cognite_client.diagrams.detect(
            file_ids=file_ids, entities=entities, multiple_jobs=True
        )
        assert isinstance(job_bundle, DetectJobBundle)
        successes, _failures = job_bundle.result
        successes == [diagram_get_result.json()]

        # Test empty file_ids
        with pytest.raises(ValueError):
            job, _unposted_jobs = cognite_client.diagrams.detect(file_ids=[], entities=entities, multiple_jobs=True)
        with pytest.raises(ValueError):
            job, _unposted_jobs = cognite_client.diagrams.detect(file_ids=None, entities=entities, multiple_jobs=True)

        # Provoking failing because num_posts > limit
        monkeypatch.setattr(cognite_client.diagrams, "_DETECT_API_FILE_LIMIT", 1)
        job_bundle, _unposted_jobs = cognite_client.diagrams.detect(
            file_ids=file_ids, entities=entities, multiple_jobs=True
        )
        successes, _failures = job_bundle.result
        assert len(successes) == 3

        monkeypatch.setattr(cognite_client.diagrams, "_DETECT_API_STATUS_JOB_LIMIT", 1)
        with pytest.raises(ValueError):
            job_bundle, _unposted_jobs = cognite_client.diagrams.detect(
                file_ids=file_ids, entities=entities, multiple_jobs=True
            )


class TestDiagramDownloadConvertedFile:
    @patch.object(DiagramsAPI, "_do_request")
    def test_download_converted_file_with_file_id(
        self, mocked_do_request: MagicMock, cognite_client: CogniteClient
    ) -> None:
        mock_response = Mock()
        mock_response.content = b"fake-png-bytes"
        mocked_do_request.return_value = mock_response

        result = cognite_client.diagrams.download_converted_file(
            job_id=123, file_id=456, page=2, mime_type="image/png"
        )

        assert result == b"fake-png-bytes"
        mocked_do_request.assert_called_once()
        call_kwargs = mocked_do_request.call_args[1]
        assert call_kwargs["accept"] == "image/png"
        assert call_kwargs["params"] == {"fileId": 456, "page": 2}

    @patch.object(DiagramsAPI, "_do_request")
    def test_download_converted_file_with_file_external_id(
        self, mocked_do_request: MagicMock, cognite_client: CogniteClient
    ) -> None:
        mock_response = Mock()
        mock_response.content = b"fake-svg-bytes"
        mocked_do_request.return_value = mock_response

        result = cognite_client.diagrams.download_converted_file(
            job_id=789, file_external_id="my-diagram.pdf", mime_type="image/svg+xml"
        )

        assert result == b"fake-svg-bytes"
        mocked_do_request.assert_called_once()
        call_kwargs = mocked_do_request.call_args[1]
        assert call_kwargs["accept"] == "image/svg+xml"
        assert call_kwargs["params"] == {"fileExternalId": "my-diagram.pdf", "page": 1}

    def test_download_converted_file_requires_file_identifier(self, cognite_client: CogniteClient) -> None:
        with pytest.raises(ValueError, match="Exactly one of file_id or file_external_id must be provided"):
            cognite_client.diagrams.download_converted_file(job_id=123)

        with pytest.raises(ValueError, match="Exactly one of file_id or file_external_id must be provided"):
            cognite_client.diagrams.download_converted_file(
                job_id=123, file_id=456, file_external_id="both-provided"
            )
