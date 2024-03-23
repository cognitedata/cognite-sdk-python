from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Annotation
from cognite.client.data_classes.annotation_types.images import (
    AssetLink,
    KeypointCollection,
    KeypointCollectionWithObjectDetection,
    ObjectDetection,
    Polygon,
    TextRegion,
)
from cognite.client.data_classes.annotation_types.primitives import (
    Attribute,
    BoundingBox,
    CdfResourceRef,
    Keypoint,
    Point,
    Polyline,
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
from cognite.client.utils._importing import local_import
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_snake_case

object_detection_sample = ObjectDetection(
    label="foo",
    confidence=None,
    bounding_box=BoundingBox(x_min=0.1, y_min=0.2, x_max=0.3, y_max=0.4),
    attributes={
        "foo": Attribute(type="boolean", value=True),
        "bar": {"type": "numerical", "value": 0.1},
    },
)

object_detection_dict_sample = {
    "boundingBox": {"xMin": 0.1, "yMin": 0.2, "xMax": 0.3, "yMax": 0.4},
    "label": "foo",
    "attributes": {
        "foo": {"type": "boolean", "value": True},
        "bar": {"type": "numerical", "value": 0.1},
    },
}

keypoint_collection_with_object_detection_sample = KeypointCollectionWithObjectDetection(
    keypoint_collection=KeypointCollection(
        label="foo",
        confidence=0.2,
        attributes={"attr": Attribute(type="numerical", value=0.1)},
        keypoints={
            "bottom": Keypoint(Point(x=0.1, y=0.1)),
            "top": Keypoint(Point(x=0.2, y=0.2)),
            "value": Keypoint(Point(x=0.3, y=0.3)),
        },
    ),
    object_detection=object_detection_sample,
)

keypoint_collection_with_object_detection_dict_sample = {
    "keypointCollection": {
        "label": "foo",
        "confidence": 0.2,
        "attributes": {"attr": {"type": "numerical", "value": 0.1}},
        "keypoints": {
            "bottom": {"point": {"x": 0.1, "y": 0.1}},
            "top": {"point": {"x": 0.2, "y": 0.2}},
            "value": {"point": {"x": 0.3, "y": 0.3}},
        },
    },
    "objectDetection": object_detection_dict_sample,
}

mock_vision_predictions_dict: dict[str, list[dict[str, Any]]] = {
    "textPredictions": [
        {"text": "a", "textRegion": {"xMin": 0.1, "yMin": 0.2, "xMax": 0.3, "yMax": 0.4}, "confidence": 0.1}
    ],
    "assetTagPredictions": [
        {
            "text": "foo",
            "textRegion": {"xMin": 0.1, "yMin": 0.2, "xMax": 0.3, "yMax": 0.4},
            "assetRef": {"id": 1},
        }
    ],
    "industrialObjectPredictions": [object_detection_dict_sample],
    "peoplePredictions": [object_detection_dict_sample],
    "personalProtectiveEquipmentPredictions": [object_detection_dict_sample],
    "digitalGaugePredictions": [object_detection_dict_sample],
    "dialGaugePredictions": [keypoint_collection_with_object_detection_dict_sample],
    "levelGaugePredictions": [keypoint_collection_with_object_detection_dict_sample],
    "valvePredictions": [keypoint_collection_with_object_detection_dict_sample],
}

mock_vision_extract_predictions = VisionExtractPredictions(
    text_predictions=[
        TextRegion(
            text="a",
            text_region=BoundingBox(x_min=0.1, y_min=0.2, x_max=0.3, y_max=0.4),
            confidence=0.1,
        )
    ],
    asset_tag_predictions=[
        AssetLink(
            text="foo",
            text_region={"x_min": 0.1, "y_min": 0.2, "x_max": 0.3, "y_max": 0.4},
            asset_ref={"id": 1},
        )
    ],
    industrial_object_predictions=[object_detection_sample],
    people_predictions=[object_detection_sample],
    personal_protective_equipment_predictions=[object_detection_sample],
    digital_gauge_predictions=[object_detection_sample],
    dial_gauge_predictions=[keypoint_collection_with_object_detection_sample],
    level_gauge_predictions=[keypoint_collection_with_object_detection_sample],
    valve_predictions=[keypoint_collection_with_object_detection_sample],
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
                Polygon([Point(1, 2), Point(3, 4), {"x": -1, "y": 0}]),
                {"vertices": [{"x": 1, "y": 2}, {"x": 3, "y": 4}, {"x": -1, "y": 0}]},
                False,
            ),
            (
                Polyline([Point(1, 2), Point(3, 4), {"x": -1, "y": 0}]),
                {"vertices": [{"x": 1, "y": 2}, {"x": 3, "y": 4}, {"x": -1, "y": 0}]},
                False,
            ),
            (
                Keypoint(point=Point(1, 2), confidence=None),
                {"point": {"x": 1, "y": 2}},
                False,
            ),
            (
                Attribute(type="numerical", value=0.1),
                {"type": "numerical", "value": 0.1},
                False,
            ),
            (
                object_detection_sample,
                object_detection_dict_sample,
                True,
            ),
            (
                TextRegion(
                    text="foo", text_region={"x_min": 0.1, "x_max": 0.2, "y_min": 0.3, "y_max": 0.4}, confidence=None
                ),
                {"text": "foo", "text_region": {"x_min": 0.1, "x_max": 0.2, "y_min": 0.3, "y_max": 0.4}},
                False,
            ),
            (
                AssetLink(
                    text="foo",
                    text_region={"x_min": 0.1, "x_max": 0.2, "y_min": 0.3, "y_max": 0.4},
                    asset_ref={"id": 1},
                ),
                {
                    "text": "foo",
                    "text_region": {"x_min": 0.1, "x_max": 0.2, "y_min": 0.3, "y_max": 0.4},
                    "asset_ref": {"id": 1},
                },
                False,
            ),
            (
                KeypointCollection(
                    label="bar",
                    keypoints={"a": Keypoint(point=Point(1, 2))},
                    confidence=0.1,
                    attributes={
                        "foo": Attribute(type="boolean", value=True),
                        "bar": {"type": "numerical", "value": 0.1},
                    },
                ),
                {
                    "label": "bar",
                    "keypoints": {"a": {"point": {"x": 1, "y": 2}}},
                    "confidence": 0.1,
                    "attributes": {
                        "foo": {"type": "boolean", "value": True},
                        "bar": {"type": "numerical", "value": 0.1},
                    },
                },
                False,
            ),
            (
                keypoint_collection_with_object_detection_sample,
                keypoint_collection_with_object_detection_dict_sample,
                True,
            ),
        ],
        ids=[
            "cdf_resource_ref",
            "cdf_resource_ref_camel_case",
            "polygon",
            "polyline",
            "keypoint",
            "attribute",
            "object_detection",
            "text_region",
            "asset_link",
            "keypoint_collection",
            "keypoint_collection_with_object_detection",
        ],
    )
    def test_dump(self, item: VisionResource, expected_dump: dict[str, Any], camel_case: bool) -> None:
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
                    text="foo", text_region={"x_min": 0.1, "y_min": 0.2, "x_max": 0.3, "y_max": 0.4}, confidence=None
                ),
                {"value": ["foo", {"x_min": 0.1, "y_min": 0.2, "x_max": 0.3, "y_max": 0.4}]},
                ["text", "text_region"],
                False,
            ),
            (
                VisionExtractPredictions(
                    text_predictions=[
                        TextRegion(
                            text="foo",
                            text_region={"x_min": 0.1, "y_min": 0.2, "x_max": 0.3, "y_max": 0.4},
                            confidence=None,
                        )
                    ],
                    level_gauge_predictions=[
                        KeypointCollectionWithObjectDetection(
                            keypoint_collection=KeypointCollection(
                                label="levelGauge",
                                attributes={"level_gauge_value": Attribute(type="numerical", value=0.1)},
                                keypoints={
                                    "bottom": Keypoint(Point(x=0.1, y=0.1)),
                                },
                            ),
                            object_detection=ObjectDetection(
                                label="foo",
                                bounding_box=BoundingBox(x_min=0.1, y_min=0.2, x_max=0.3, y_max=0.4),
                            ),
                        )
                    ],
                ),
                {
                    "value": [
                        [{"text": "foo", "textRegion": {"xMin": 0.1, "yMin": 0.2, "xMax": 0.3, "yMax": 0.4}}],
                        [
                            {
                                "keypointCollection": {
                                    "label": "levelGauge",
                                    "attributes": {"level_gauge_value": {"type": "numerical", "value": 0.1}},
                                    "keypoints": {"bottom": {"point": {"x": 0.1, "y": 0.1}}},
                                },
                                "objectDetection": {
                                    "label": "foo",
                                    "boundingBox": {"xMin": 0.1, "yMin": 0.2, "xMax": 0.3, "yMax": 0.4},
                                },
                            },
                        ],
                    ]
                },
                ["textPredictions", "levelGaugePredictions"],
                True,
            ),
        ],
        ids=["valid_dump", "valid_dump_camel_case", "valid_dump_mix", "valid_dump_list"],
    )
    def test_to_pandas(self, item: VisionResource, exp_df_data, exp_df_index, camel_case: bool) -> None:
        pd = local_import("pandas")

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
    def test_load(self, resource: dict[str, Any], expected_item: VisionExtractItem) -> None:
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
    def test_dump(self, item: VisionExtractItem, expected_dump: dict[str, Any], camel_case: bool) -> None:
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
        self, mock_result: MagicMock, status: JobStatus, result: dict | None, expected_items: list | None
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
        expected_item: VisionExtractItem | None,
        error_message: str | None,
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
                {
                    "items": [
                        {
                            "fileId": 1,
                            "predictions": {
                                k: v
                                for k, v in mock_vision_predictions_dict.items()
                                if k
                                in [
                                    "textPredictions",
                                    "assetTagPredictions",
                                    "peoplePredictions",
                                    "dialGaugePredictions",
                                ]
                            },
                        }
                    ]
                },
                {"creating_user": None, "creating_app": None, "creating_app_version": None},
                [
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.TextRegion",
                        data={
                            # convert all keys to snake case since data is dumped as snake case in _predictions_to_annotations()
                            to_snake_case(k): (convert_all_keys_to_snake_case(v) if isinstance(v, dict) else v)
                            for k, v in mock_vision_predictions_dict["textPredictions"][0].items()
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="cognite-sdk-python",
                        creating_app_version=1,
                        creating_user=None,
                    ),
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.AssetLink",
                        data={
                            to_snake_case(k): (convert_all_keys_to_snake_case(v) if isinstance(v, dict) else v)
                            for k, v in mock_vision_predictions_dict["assetTagPredictions"][0].items()
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="cognite-sdk-python",
                        creating_app_version=1,
                        creating_user=None,
                    ),
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.ObjectDetection",
                        data={
                            to_snake_case(k): (convert_all_keys_to_snake_case(v) if isinstance(v, dict) else v)
                            for k, v in object_detection_dict_sample.items()
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="cognite-sdk-python",
                        creating_app_version=1,
                        creating_user=None,
                    ),
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.KeypointCollection",
                        data={
                            to_snake_case(k): (convert_all_keys_to_snake_case(v) if isinstance(v, dict) else v)
                            for k, v in keypoint_collection_with_object_detection_dict_sample[
                                "keypointCollection"
                            ].items()
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="cognite-sdk-python",
                        creating_app_version=1,
                        creating_user=None,
                    ),
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.ObjectDetection",
                        data={
                            to_snake_case(k): (convert_all_keys_to_snake_case(v) if isinstance(v, dict) else v)
                            for k, v in keypoint_collection_with_object_detection_dict_sample["objectDetection"].items()
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="cognite-sdk-python",
                        creating_app_version=1,
                        creating_user=None,
                    ),
                ],
            ),
            (
                {
                    "items": [
                        {
                            "fileId": 1,
                            "predictions": {
                                k: v for k, v in mock_vision_predictions_dict.items() if k == "textPredictions"
                            },
                        }
                    ]
                },
                {"creating_user": "foo", "creating_app": "bar", "creating_app_version": "1.0.0"},
                [
                    Annotation(
                        annotated_resource_id=1,
                        annotation_type="images.TextRegion",
                        data={
                            to_snake_case(k): (convert_all_keys_to_snake_case(v) if isinstance(v, dict) else v)
                            for k, v in mock_vision_predictions_dict["textPredictions"][0].items()
                        },
                        annotated_resource_type="file",
                        status="suggested",
                        creating_app="bar",
                        creating_app_version="1.0.0",
                        creating_user="foo",
                    ),
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
        result: dict | None,
        params: dict | None,
        expected_items: list | None,
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
    def test_dump(self, item: VisionResource, expected_dump: dict[str, Any], camel_case: bool) -> None:
        assert item.dump(camel_case) == expected_dump
