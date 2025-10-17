from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite.client import AsyncCogniteClient, CogniteClient
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client.data_classes.contextualization import DetectJobBundle, DiagramDetectResults

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class TestPNIDParsingUnit:
    @patch.object(DiagramsAPI, "_post")
    @patch.object(DiagramsAPI, "_get")
    async def test_run_diagram_detect(
        self,
        mocked_diagrams_get: MagicMock,
        mocked_diagrams_post: MagicMock,
        cognite_client: CogniteClient,
        async_client: AsyncCogniteClient,
        monkeypatch: MonkeyPatch,
    ) -> None:
        entities: list[dict] = [{"name": "YT-96122"}, {"name": "XE-96125", "ee": 123}, {"name": "XWDW-9615"}]
        file_ids = [1, 2, 3]

        mock_response = {
            "createdTime": 1337,
            "items": [{"annotations": [], "fileId": 101, "status": "Completed"}],
            "jobId": 1,
            "status": "Completed",
            "statusCount": {"completed": 1},
            "statusTime": 1337,
            "startTime": 1337,
        }

        mocked_diagrams_post.return_value = Mock(json=lambda: mock_response)
        diagram_get_result = Mock(json=lambda: mock_response)

        # Single job
        job = cognite_client.diagrams.detect(file_ids=file_ids, entities=entities)
        assert isinstance(job, DiagramDetectResults)

        # Multiple jobs
        mocked_diagrams_get.return_value = diagram_get_result
        job_bundle, unposted_jobs = cognite_client.diagrams.detect(
            file_ids=file_ids, entities=entities, multiple_jobs=True
        )
        assert isinstance(job_bundle, DetectJobBundle)

        # Test empty file_ids
        with pytest.raises(ValueError):
            cognite_client.diagrams.detect(file_ids=[], entities=entities, multiple_jobs=True)
        with pytest.raises(ValueError):
            cognite_client.diagrams.detect(file_ids=None, entities=entities, multiple_jobs=True)

        # Provoking failing because num_posts > limit
        monkeypatch.setattr(async_client.diagrams, "_DETECT_API_FILE_LIMIT", 1)
        job_bundle, _unposted_jobs = cognite_client.diagrams.detect(
            file_ids=file_ids, entities=entities, multiple_jobs=True
        )
        assert job_bundle
        successes, failures = await job_bundle.get_result()
        assert len(successes) == 3

        monkeypatch.setattr(async_client.diagrams, "_DETECT_API_STATUS_JOB_LIMIT", 1)
        with pytest.raises(ValueError):
            _, _unposted_jobs = cognite_client.diagrams.detect(file_ids=file_ids, entities=entities, multiple_jobs=True)
