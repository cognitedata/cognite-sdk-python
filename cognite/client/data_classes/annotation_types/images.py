from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from typing_extensions import Self

from cognite.client.data_classes._base import load_resource
from cognite.client.data_classes.annotation_types.primitives import (
    Attribute,
    BoundingBox,
    CdfResourceRef,
    Keypoint,
    Polygon,
    Polyline,
    VisionResource,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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

    @classmethod
    def load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> ObjectDetection:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            label=resource["label"],
            confidence=resource.get("confidence"),
            bounding_box=load_resource(resource, BoundingBox, "boundingBox"),
            polygon=load_resource(resource, Polygon, "polygon"),
            polyline=load_resource(resource, Polyline, "polyline"),
        )


@dataclass
class TextRegion(VisionResource):
    text: str
    text_region: BoundingBox
    confidence: float | None = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, dict):
            self.text_region = BoundingBox(**self.text_region)

    @classmethod
    def load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> TextRegion:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            text=resource["text"],
            text_region=BoundingBox.load(resource["textRegion"]),
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
            self.text_region = BoundingBox(**self.text_region)
        if isinstance(self.asset_ref, dict):
            self.asset_ref = CdfResourceRef(**self.asset_ref)

    @classmethod
    def load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> AssetLink:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            text=resource["text"],
            text_region=BoundingBox.load(resource["textRegion"]),
            asset_ref=CdfResourceRef.load(resource["assetRef"]),
            confidence=resource.get("confidence"),
        )


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

    @classmethod
    def load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Self:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            label=resource["label"],
            keypoints={k: Keypoint.load(v) for k, v in resource["keypoints"].items()},
            attributes=resource.get("attributes"),
            confidence=resource.get("confidence"),
        )


@dataclass
class KeypointCollectionWithObjectDetection(VisionResource):
    object_detection: ObjectDetection
    keypoint_collection: KeypointCollection

    def __post_init__(self) -> None:
        if isinstance(self.object_detection, dict):
            self.object_detection = ObjectDetection(**self.object_detection)
        if isinstance(self.keypoint_collection, dict):
            self.keypoint_collection = KeypointCollection(**self.keypoint_collection)

    @classmethod
    def load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Self:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            object_detection=ObjectDetection.load(resource["objectDetection"], cognite_client),
            keypoint_collection=KeypointCollection.load(resource["keypointCollection"], cognite_client),
        )
