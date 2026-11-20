from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from typing_extensions import Self

from cognite.client.data_classes.annotation_types.primitives import (
    Attribute,
    BoundingBox,
    CdfResourceRef,
    Keypoint,
    Polygon,
    Polyline,
    VisionResource,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case_recursive


@dataclass
class ObjectDetection(VisionResource):
    label: str
    confidence: float | None = None
    attributes: dict[str, Attribute] | None = None

    # A valid object detection instance needs to have *exactly one* of these
    bounding_box: BoundingBox | None = None
    polygon: Polygon | None = None
    polyline: Polyline | None = None

    def __post_init__(self) -> None:
        if isinstance(self.bounding_box, dict):
            self.bounding_box = BoundingBox._load(convert_all_keys_to_camel_case_recursive(self.bounding_box))
        if isinstance(self.polygon, dict):
            self.polygon = Polygon._load(convert_all_keys_to_camel_case_recursive(self.polygon))
        if isinstance(self.polyline, dict):
            self.polyline = Polyline._load(convert_all_keys_to_camel_case_recursive(self.polyline))
        if isinstance(self.attributes, dict):
            self.attributes = {
                k: Attribute._load(convert_all_keys_to_camel_case_recursive(v)) if isinstance(v, dict) else v
                for k, v in self.attributes.items()
            }

    @classmethod
    def _load(cls, resource: dict) -> ObjectDetection:
        return cls(
            label=resource["label"],
            confidence=resource.get("confidence"),
            attributes={key: Attribute._load(attribute) for key, attribute in resource.get("attributes", {}).items()}
            or None,
            bounding_box=BoundingBox._load_if(resource.get("boundingBox")),
            polygon=Polygon._load_if(resource.get("polygon")),
            polyline=Polyline._load_if(resource.get("polyline")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        if self.attributes is not None:
            dumped["attributes"] = {k: v.dump(camel_case=camel_case) for k, v in self.attributes.items()}
        return dumped


@dataclass
class TextRegion(VisionResource):
    text: str
    text_region: BoundingBox
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, dict):
            self.text_region = BoundingBox._load(convert_all_keys_to_camel_case_recursive(self.text_region))

    @classmethod
    def _load(cls, resource: dict) -> TextRegion:
        return cls(
            text=resource["text"],
            text_region=BoundingBox._load(resource["textRegion"]),
            confidence=resource.get("confidence"),
        )


@dataclass
class AssetLink(VisionResource):
    text: str
    text_region: BoundingBox
    asset_ref: CdfResourceRef
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, dict):
            self.text_region = BoundingBox._load(convert_all_keys_to_camel_case_recursive(self.text_region))
        if isinstance(self.asset_ref, dict):
            self.asset_ref = CdfResourceRef._load(convert_all_keys_to_camel_case_recursive(self.asset_ref))

    @classmethod
    def _load(cls, resource: dict) -> AssetLink:
        return cls(
            text=resource["text"],
            text_region=BoundingBox._load(resource["textRegion"]),
            asset_ref=CdfResourceRef._load(resource["assetRef"]),
            confidence=resource.get("confidence"),
        )


@dataclass
class KeypointCollection(VisionResource):
    label: str
    keypoints: dict[str, Keypoint]
    attributes: dict[str, Attribute] | None = None
    confidence: float | None = None

    @classmethod
    def _load(cls, resource: dict) -> Self:
        return cls(
            label=resource["label"],
            keypoints={k: Keypoint._load(v) for k, v in resource["keypoints"].items()},
            attributes=(attrs := resource.get("attributes")) and {k: Attribute._load(v) for k, v in attrs.items()},
            confidence=resource.get("confidence"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        dumped["keypoints"] = {k: v.dump(camel_case=camel_case) for k, v in self.keypoints.items()}
        if self.attributes is not None:
            dumped["attributes"] = {k: v.dump(camel_case=camel_case) for k, v in self.attributes.items()}
        return dumped


@dataclass
class KeypointCollectionWithObjectDetection(VisionResource):
    object_detection: ObjectDetection
    keypoint_collection: KeypointCollection

    def __post_init__(self) -> None:
        if isinstance(self.object_detection, dict):
            self.object_detection = ObjectDetection._load(
                convert_all_keys_to_camel_case_recursive(self.object_detection)
            )
        if isinstance(self.keypoint_collection, dict):
            self.keypoint_collection = KeypointCollection._load(
                convert_all_keys_to_camel_case_recursive(self.keypoint_collection)
            )

    @classmethod
    def _load(cls, resource: dict) -> Self:
        return cls(
            object_detection=ObjectDetection._load(resource["objectDetection"]),
            keypoint_collection=KeypointCollection._load(resource["keypointCollection"]),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        dumped = super().dump(camel_case=camel_case)
        if self.object_detection is not None:
            dumped["objectDetection"] = self.object_detection.dump(camel_case=camel_case)
        if self.keypoint_collection is not None:
            dumped["keypointCollection"] = self.keypoint_collection.dump(camel_case=camel_case)
        return dumped
