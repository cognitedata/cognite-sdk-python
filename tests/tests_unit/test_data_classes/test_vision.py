from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cognite.client import CogniteClient, utils
from cognite.client.data_classes import Annotation
from cognite.client.data_classes.annotation_types.images import ObjectDetection, Polygon, TextRegion
from cognite.client.data_classes.annotation_types.primitives import (
    BoundingBox,
    CdfResourceRef,
    Point,
    PolyLine,
    VisionResource,
)
from cognite.client.data_classes.contextualization import (
    AssetTagDetectionParameters,
    FeatureParameters,
    JobStatus,
    TextDetectionParameters,
    VisionExtractItem,
    VisionExtractJob,
    VisionExtractPredictions,
)

mock_vision_predictions_dict: Dict[str, List[Dict[str, Any]]] = {
    "textPredictions": [
        {"text": "a", "textRegion": {"xMin": 0.1, "xMax": 0.2, "yMin": 0.3, "yMax": 0.4}, "confidence": 0.1}
    ]
}
mock_vision_extract_predictions = VisionExtractPredictions(
    text_predictions=[
        TextRegion(
            text="a",
            text_region=BoundingBox(x_min=0.1, x_max=0.2, y_min=0.3, y_max=0.4),
            confidence=0.1,
        )
    ]
)


class TestVisionResource:
    @pytest.mark.parametrize(
        "item, expected_dump, camel_case",
        [
            (
                CdfResourceRef(id=1, external_id="a"),
                {"id": 1, "external_id": "a"},
                False,
            ),
            (
                CdfResourceRef(id=1, external_id="a"),
                {"id": 1, "externalId": "a"},
                True,
            ),
            (
                TextRegion(
                    text="foo", text_region={"x_min": 0.1, "x_max": 0.1, "y_min": 0.1, "y_max": 0.1}, confidence=None
                ),
                {"text": "foo", "text_region": {"x_min": 0.1, "x_max": 0.1, "y_min": 0.1, "y_max": 0.1}},
                False,
            ),
            (
                # Ensure nested objects with list of VisionResource subclasses are recursively dumped:
                Polygon([Point(1, 2), Point(3, 4), Point(-1, 0)]),
                {"vertices": [{"x": 1, "y": 2}, {"x": 3, "y": 4}, {"x": -1, "y": 0}]},
                False,
            ),
            (
                ObjectDetection(label="foo", confidence=None, polyline=PolyLine([Point(1, 2), Point(3, 4)])),
                {"label": "foo", "polyline": {"vertices": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]}},
                False,
            ),
        ],
        ids=["valid_dump", "valid_dump_camel_case", "valid_dump_mix", "valid_nested_dump1", "valid_nested_dump2"],
    )
    def test_dump(self, item: VisionResource, expected_dump: Dict[str, Any], camel_case: bool) -> None:
        assert item.dump(camel_case) == expected_dump

    @pytest.mark.dsl
    @pytest.mark.parametrize(
        "item, exp_df_data, exp_df_index, camel_case",
        [
            (
                CdfResourceRef(id=1, external_id="a"),
                {"value": [1, "a"]},
                ["id", "external_id"],
                False,
            ),
            (
                CdfResourceRef(id=1, external_id="a"),
                {"value": [1, "a"]},
                ["id", "externalId"],
                True,
            ),
            (
                TextRegion(
                    text="foo", text_region={"x_min": 0.1, "x_max": 0.1, "y_min": 0.1, "y_max": 0.1}, confidence=None
                ),
                {"value": ["foo", {"x_min": 0.1, "x_max": 0.1, "y_min": 0.1, "y_max": 0.1}]},
                ["text", "text_region"],
                False,
            ),
            (
                VisionExtractPredictions(
                    text_predictions=[
                        TextRegion(
                            text="foo",
                            text_region={"x_min": 0.1, "x_max": 0.1, "y_min": 0.1, "y_max": 0.1},
                            confidence=None,
                        )
                    ]
                ),
                {"value": [[{"text": "foo", "textRegion": {"xMin": 0.1, "xMax": 0.1, "yMin": 0.1, "yMax": 0.1}}]]},
                ["textPredictions"],
                True,
            ),
        ],
        ids=["valid_dump", "valid_dump_camel_case", "valid_dump_mix", "valid_dump_list"],
    )
    def test_to_pandas(self, item: VisionResource, exp_df_data, exp_df_index, camel_case: bool) -> None:
        pd = utils._auxiliary.local_import("pandas")

        res = item.to_pandas(camel_case)
        exp = pd.DataFrame(exp_df_data, index=exp_df_index)
        pd.testing.assert_frame_equal(res, exp)


class TestVisionExtractItem:
    def test_process_predictions_dict(self) -> None:
        useless_kwargs = {"foo": 1, "bar": "baz"}  # these should be ignored
        assert (
            VisionExtractItem._process_predictions_dict({**mock_vision_predictions_dict, **useless_kwargs})
            == mock_vision_extract_predictions
        )

    @pytest.mark.parametrize(
        "resource, expected_item",
        [
            (
                {"fileId": 1, "fileExternalId": "a", "predictions": None},
                VisionExtractItem(file_id=1, file_external_id="a", predictions=None),
            ),
            (
                {"fileId": 1, "predictions": mock_vision_predictions_dict},
                VisionExtractItem(file_id=1, predictions=mock_vision_predictions_dict),
            ),
        ],
        ids=["valid_vision_extract_item_no_predictions", "valid_vision_extract_item"],
    )
    def test_load(self, resource: Dict[str, Any], expected_item: VisionExtractItem) -> None:
        vision_extract_item = VisionExtractItem.load(resource)
        assert vision_extract_item == expected_item

    @pytest.mark.parametrize(
        "item, expected_dump, camel_case",
        [
            (
                VisionExtractItem(file_id=1, file_external_id="a", predictions=None),
                {"file_id": 1, "file_external_id": "a"},
                False,
            ),
            (
                VisionExtractItem(file_id=1, file_external_id="a", predictions=mock_vision_predictions_dict),
                {
                    "fileId": 1,
                    "fileExternalId": "a",
                    "predictions": mock_vision_predictions_dict,
                },
                True,
            ),
        ],
        ids=["valid_dump_no_predictions", "valid_dump_with_predictions_camel_case"],
    )
    def test_dump(self, item: VisionExtractItem, expected_dump: Dict[str, Any], camel_case: bool) -> None:
        assert item.dump(camel_case) == expected_dump


class TestVisionExtractJob:
    @patch("cognite.client.data_classes.contextualization.ContextualizationJob.result", new_callable=PropertyMock)
    @pytest.mark.parametrize(
        "status, result, expected_items",
        [
            (JobStatus.QUEUED, None, None),
            (
                JobStatus.COMPLETED,
                {"items": [{"fileId": 1, "predictions": mock_vision_predictions_dict}]},
                [VisionExtractItem(file_id=1, predictions=mock_vision_predictions_dict)],
            ),
        ],
        ids=["non_completed_job", "completed_job"],
    )
    def test_items_property(
        self, mock_result: MagicMock, status: JobStatus, result: Optional[Dict], expected_items: Optional[List]
    ) -> None:
        cognite_client = MagicMock(spec=CogniteClient)
        mock_result.return_value = result
        job = VisionExtractJob(status=status.value, cognite_client=cognite_client)
        assert job.items == expected_items

    @patch("cognite.client.data_classes.contextualization.ContextualizationJob.result", new_callable=PropertyMock)
    @pytest.mark.parametrize(
        "file_id, expected_item, error_message",
        [
            (1, VisionExtractItem(file_id=1, file_external_id="foo", predictions=mock_vision_predictions_dict), None),
            (1337, None, "File with id 1337 not found in results"),
        ],
        ids=["valid_unique_id", "non_existing_id"],
    )
    def test_get_item(
        self,
        mock_result: MagicMock,
        file_id: int,
        expected_item: Optional[VisionExtractItem],
        error_message: Optional[str],
    ) -> None:
        cognite_client = MagicMock(spec=CogniteClient)
        mock_result.return_value = {
            "items": [
                {
                    "fileId": i + 1,
                    "fileExternalId": "foo",
                    "predictions": mock_vision_predictions_dict,
                }
                for i in range(2)
            ]
        }
        job = VisionExtractJob(cognite_client=cognite_client)
        if error_message is not None:
            with pytest.raises(IndexError, match=error_message):
                job[file_id]
        else:
            assert job[file_id] == expected_item

    @patch("cognite.client.data_classes.contextualization.ContextualizationJob.result", new_callable=PropertyMock)
    @pytest.mark.parametrize(
        "result, params, expected_items",
        [
            (
                {"items": []},
                None,
                [],
            ),
            (
                {"items": [{"fileId": 1, "predictions": {}}]},
                None,
                [],
            ),
            (
                {"items": [{"fileId": 1, "predictions": {"text_predictions": []}}]},
                None,
                [],
            ),
            (
                {"items": [{"fileId": 1, "predictions": mock_vision_predictions_dict}]},
                {"creating_user": None, "creating_app": None, "creating_app_version": None},
                [
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.TextRegion",
                        data={
                            "text": "a",
                            "text_region": {"x_min": 0.1, "x_max": 0.2, "y_min": 0.3, "y_max": 0.4},
                            "confidence": 0.1,
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="cognite-sdk-python",
                        creating_app_version=1,
                        creating_user=None,
                    )
                ],
            ),
            (
                {"items": [{"fileId": 1, "predictions": mock_vision_predictions_dict}]},
                {"creating_user": "foo", "creating_app": "bar", "creating_app_version": "1.0.0"},
                [
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.TextRegion",
                        data={
                            "text": "a",
                            "text_region": {"x_min": 0.1, "x_max": 0.2, "y_min": 0.3, "y_max": 0.4},
                            "confidence": 0.1,
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="bar",
                        creating_app_version="1.0.0",
                        creating_user="foo",
                    )
                ],
            ),
        ],
        ids=[
            "empty_items",
            "empty_predictions",
            "empty_text_predictions",
            "completed_job_default_params",
            "completed_job_with_user_params",
        ],
    )
    def test_predictions_to_annotations(
        self,
        mock_result: MagicMock,
        result: Optional[Dict],
        params: Optional[Dict],
        expected_items: Optional[List],
    ) -> None:
        cognite_client = MagicMock(spec=CogniteClient)
        cognite_client.version = (params or {}).get("creating_app_version") or 1
        mock_result.return_value = result
        job = VisionExtractJob(status=JobStatus.COMPLETED.value, cognite_client=cognite_client)
        assert job._predictions_to_annotations(**(params or {})) == expected_items


class TestFeatureParameters:
    @pytest.mark.parametrize(
        "item, expected_dump, camel_case",
        [
            (
                FeatureParameters(text_detection_parameters=TextDetectionParameters(threshold=0.3)),
                {"text_detection_parameters": {"threshold": 0.3}},
                False,
            ),
            (
                FeatureParameters(text_detection_parameters=TextDetectionParameters(threshold=0.3)),
                {"textDetectionParameters": {"threshold": 0.3}},
                True,
            ),
            (
                FeatureParameters(
                    asset_tag_detection_parameters=AssetTagDetectionParameters(asset_subtree_ids=[1, 2, 3])
                ),
                {"assetTagDetectionParameters": {"assetSubtreeIds": [1, 2, 3]}},
                True,
            ),
        ],
        ids=["dump", "dump w/camelcase", "only non-null values are dumped"],
    )
    def test_dump(self, item: VisionResource, expected_dump: Dict[str, Any], camel_case: bool) -> None:
        assert item.dump(camel_case) == expected_dump
