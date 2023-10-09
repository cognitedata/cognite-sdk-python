from __future__ import annotations

from dataclasses import dataclass

from cognite.client.data_classes.annotation_types.primitives import (
    Attribute,
    BoundingBox,
    CdfResourceRef,
    Keypoint,
    Polygon,
    Polyline,
    VisionResource,
)


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
            self.bounding_box = BoundingBox(**self.bounding_box)
        if isinstance(self.polygon, dict):
            self.polygon = Polygon(**self.polygon)
        if isinstance(self.polyline, dict):
            self.polyline = Polyline(**self.polyline)
        if isinstance(self.attributes, dict):
            self.attributes = {k: Attribute(**v) if isinstance(v, dict) else v for k, v in self.attributes.items()}


@dataclass
class TextRegion(VisionResource):
    text: str
    text_region: BoundingBox
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, dict):
            self.text_region = BoundingBox(**self.text_region)


@dataclass
class AssetLink(VisionResource):
    text: str
    text_region: BoundingBox
    asset_ref: CdfResourceRef
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, dict):
            self.text_region = BoundingBox(**self.text_region)
        if isinstance(self.asset_ref, dict):
            self.asset_ref = CdfResourceRef(**self.asset_ref)


@dataclass
class KeypointCollection(VisionResource):
    label: str
    keypoints: dict[str, Keypoint]
    attributes: dict[str, Attribute] | None = None
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.attributes, dict):
            self.attributes = {k: Attribute(**v) if isinstance(v, dict) else v for k, v in self.attributes.items()}
        if isinstance(self.keypoints, dict):
            self.keypoints = {k: Keypoint(**v) if isinstance(v, dict) else v for k, v in self.keypoints.items()}


@dataclass
class KeypointCollectionWithObjectDetection(VisionResource):
    object_detection: ObjectDetection
    keypoint_collection: KeypointCollection

    def __post_init__(self) -> None:
        if isinstance(self.object_detection, dict):
            self.object_detection = ObjectDetection(**self.object_detection)
        if isinstance(self.keypoint_collection, dict):
            self.keypoint_collection = KeypointCollection(**self.keypoint_collection)
