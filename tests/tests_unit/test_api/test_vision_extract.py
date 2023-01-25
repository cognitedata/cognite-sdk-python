from __future__ import annotations

import re
from contextlib import nullcontext as does_not_raise
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

import pytest
from responses import RequestsMock

from cognite.client import CogniteClient
from cognite.client.data_classes.contextualization import (
    FeatureParameters,
    JobStatus,
    PeopleDetectionParameters,
    TextDetectionParameters,
    VisionExtractJob,
    VisionFeature,
)
from cognite.client.exceptions import CogniteException
from tests.utils import jsgz_load


@pytest.fixture
def mock_post_response_body() -> Dict[str, Any]:
    return {
        "status": JobStatus.QUEUED.value,
        "createdTime": 934875934785,
        "startTime": 934875934785,
        "statusTime": 934875934785,
        "jobId": 1,
        "items": [
            {"fileId": 1},
            {"fileId": 2, "fileExternalId": "some_external_id"},
            {"fileId": 3},
            {"fileId": 4},
            {"fileId": 5, "fileExternalId": "another"},
        ],
    }


@pytest.fixture
def mock_get_response_body_ok() -> Dict[str, Any]:
    return {
        "status": JobStatus.COMPLETED.value,
        "createdTime": 934875934785,
        "startTime": 934875934785,
        "statusTime": 934875934785,
        "jobId": 1,
        "items": [
            {
                "fileId": 1,
                "predictions": {
                    "assetTagPredictions": [
                        {
                            "assetRef": {"id": 1},
                            "confidence": 0.9,
                            "text": "testing",
                            "textRegion": {
                                "xMin": 0.1,
                                "yMin": 0.2,
                                "xMax": 0.3,
                                "yMax": 0.4,
                            },
                        }
                    ]
                },
            }
        ],
        "parameters": {"assetTagDetectionParameters": {"assetSubtreeIds": [], "partialMatch": False, "threshold": 0.4}},
    }


@pytest.fixture
def mock_post_extract(rsps: RequestsMock, mock_post_response_body: Dict[str, Any]) -> RequestsMock:
    rsps.add(
        rsps.POST,
        re.compile(".*?/context/vision/extract"),
        status=200,
        json=mock_post_response_body,
    )
    yield rsps


@pytest.fixture
def mock_get_extract(rsps: RequestsMock, mock_get_response_body_ok: Dict[str, Any]) -> RequestsMock:
    rsps.add(
        rsps.GET,
        re.compile(".*?/context/vision/extract/\\d+"),
        status=200,
        json=mock_get_response_body_ok,
    )
    yield rsps


@pytest.fixture
def mock_get_extract_empty_predictions(rsps: RequestsMock, mock_get_response_body_ok: Dict[str, Any]) -> RequestsMock:
    response_copy = deepcopy(mock_get_response_body_ok)
    response_copy["items"][0]["predictions"]["assetTagPredictions"] = []
    rsps.add(
        rsps.GET,
        re.compile(".*?/context/vision/extract/\\d+"),
        status=200,
        json=response_copy,
    )
    yield rsps


class TestJobStatusEnum:
    @pytest.mark.parametrize("job_status", list(JobStatus))
    def test_job_status_methods(self, job_status):
        v1 = job_status.is_finished()
        v2 = job_status.is_not_finished()
        assert sorted([v1, v2]) == [False, True]


class TestVisionExtract:
    @pytest.mark.parametrize(
        "features, parameters, error_message",
        [
            ("foo", None, "features must be one of types \\[<enum 'VisionFeature'>, <class 'list'>\\]"),
            (
                [VisionFeature.TEXT_DETECTION, "foo"],
                None,
                "feature 'foo' must be one of types \\[<enum 'VisionFeature'>]",
            ),
            (None, None, "features cannot be None"),
            (VisionFeature.TEXT_DETECTION, None, None),
            (
                VisionFeature.TEXT_DETECTION,
                FeatureParameters(text_detection_parameters=TextDetectionParameters(threshold=0.5)),
                None,
            ),
            (
                [VisionFeature.TEXT_DETECTION, VisionFeature.PEOPLE_DETECTION],
                FeatureParameters(
                    text_detection_parameters=TextDetectionParameters(threshold=0.1),
                    people_detection_parameters=PeopleDetectionParameters(threshold=0.2),
                ),
                None,
            ),
            (list(VisionFeature.beta_features())[0], None, None),
        ],
        ids=[
            "invalid_feature",
            "invalid_non_supported_type_in_list",
            "invalid_feature_None_value",
            "one_feature",
            "one_feature with parameter",
            "multiple_features with parameters",
            "beta_feature",
        ],
    )
    def test_extract_unit(
        self,
        mock_post_extract: RequestsMock,
        mock_get_extract: RequestsMock,
        features: Union[VisionFeature, List[VisionFeature]],
        parameters: Optional[FeatureParameters],
        error_message: Optional[str],
        cognite_client: CogniteClient,
    ) -> None:
        VAPI = cognite_client.vision
        file_ids = [1, 2, 3]
        file_external_ids = []
        if error_message is not None:
            with pytest.raises(TypeError, match=error_message):
                # GET request will not be executed due to invalid parameters in POST
                # thus relax the assertion requirements
                mock_post_extract.assert_all_requests_are_fired = False
                VAPI.extract(features=features, file_ids=file_ids, file_external_ids=file_external_ids)
        else:
            is_beta_feature: bool = len([f for f in features if f in VisionFeature.beta_features()]) > 0
            error_handling = UserWarning if is_beta_feature else does_not_raise()
            # Job should be queued immediately after a successfully POST
            with error_handling:
                job = VAPI.extract(
                    features=features, file_ids=file_ids, file_external_ids=file_external_ids, parameters=parameters
                )
            assert isinstance(job, VisionExtractJob)
            assert "Queued" == job.status

            # Cannot save prediction of an incomplete job
            with pytest.raises(
                CogniteException,
                match="Extract job is not completed. If the job is queued or running, wait for completion and try again",
            ):
                job.save_predictions()

            # Wait for job to complete and check its content
            expected_job_id = 1
            job.wait_for_completion(interval=0)
            assert "items" in job.result
            assert JobStatus.COMPLETED is JobStatus(job.status)
            assert expected_job_id == job.job_id

            num_post_requests, num_get_requests = 0, 0
            for call in mock_post_extract.calls:
                if "extract" in call.request.url and call.request.method == "POST":
                    num_post_requests += 1

                    expected_features_and_items = {
                        "features": [f.value for f in features] if isinstance(features, list) else [features.value],
                        "items": [{"fileId": fid} for fid in file_ids],
                    }
                    expected_request_body = (
                        expected_features_and_items
                        if parameters is None
                        else {**expected_features_and_items, "parameters": parameters.dump(camel_case=True)}
                    )
                    if is_beta_feature:
                        assert call.request.headers.get("cdf-version") == "beta"
                    assert expected_request_body == jsgz_load(call.request.body)
                else:
                    num_get_requests += 1
                    assert f"/{expected_job_id}" in call.request.url
            assert 1 == num_post_requests
            assert 1 == num_get_requests

    def test_get_extract(
        self, mock_post_extract: RequestsMock, mock_get_extract: RequestsMock, cognite_client: CogniteClient
    ) -> None:
        VAPI = cognite_client.vision
        file_ids = [1, 2, 3]
        file_external_ids = []

        job = VAPI.extract(
            features=VisionFeature.TEXT_DETECTION, file_ids=file_ids, file_external_ids=file_external_ids
        )

        # retrieved job should correspond to the started job:
        retrieved_job = VAPI.get_extract_job(job_id=job.job_id)

        assert isinstance(retrieved_job, VisionExtractJob)
        assert retrieved_job.job_id == job.job_id

        num_get_requests = 0
        for call in mock_get_extract.calls:
            if "extract" in call.request.url and call.request.method == "GET":
                num_get_requests += 1
                assert f"/{job.job_id}" in call.request.url
        assert 1 == num_get_requests

    def test_save_empty_predictions(
        self,
        mock_post_extract: RequestsMock,
        mock_get_extract_empty_predictions: RequestsMock,
        cognite_client: CogniteClient,
    ) -> None:
        VAPI = cognite_client.vision
        file_ids = [1]
        file_external_ids = []

        job = VAPI.extract(
            features=VisionFeature.ASSET_TAG_DETECTION, file_ids=file_ids, file_external_ids=file_external_ids
        )

        # retrieved job should correspond to the started job:
        retrieved_job = VAPI.get_extract_job(job_id=job.job_id)

        assert isinstance(retrieved_job, VisionExtractJob)
        assert retrieved_job.job_id == job.job_id

        with pytest.raises(CogniteException, match="Extract job does not contain any predictions."):
            retrieved_job.save_predictions()
