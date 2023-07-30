from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional

from cognite.client.data_classes.annotation_types.primitives import (
    BoundingBox,
    CdfResourceRef,
    Polygon,
    PolyLine,
    VisionResource,
)

if TYPE_CHECKING:
    from cognite.client import CogniteClient


@dataclass
class ObjectDetection(VisionResource):
    label: str
    confidence: Optional[float]
    # A valid object detection instance needs to have *exactly one* of these
    bounding_box: Optional[BoundingBox] = None
    polygon: Optional[Polygon] = None
    polyline: Optional[PolyLine] = None

    def __post_init__(self) -> None:
        if isinstance(self.bounding_box, Dict):
            self.bounding_box = BoundingBox(**self.bounding_box)
        if isinstance(self.polygon, Dict):
            self.polygon = Polygon(**self.polygon)
        if isinstance(self.polyline, Dict):
            self.polyline = PolyLine(**self.polyline)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> ObjectDetection:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            label=resource["label"],
            confidence=resource.get("confidence"),
            bounding_box=BoundingBox._load(bounding_box) if (bounding_box := resource.get("boundingBox")) else None,
            polygon=Polygon._load(polygon) if (polygon := resource.get("polygon")) else None,
            polyline=PolyLine._load(polyline) if (polyline := resource.get("polyline")) else None,
        )


@dataclass
class TextRegion(VisionResource):
    text: str
    text_region: BoundingBox
    confidence: Optional[float] = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, Dict):
            self.text_region = BoundingBox(**self.text_region)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> TextRegion:
        resource = json.loads(resource) if isinstance(resource, str) else resource
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
    confidence: Optional[float] = None

    def __post_init__(self) -> None:
        if isinstance(self.text_region, Dict):
            self.text_region = BoundingBox(**self.text_region)
        if isinstance(self.asset_ref, Dict):
            self.asset_ref = CdfResourceRef(**self.asset_ref)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: Optional[CogniteClient] = None) -> AssetLink:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            text=resource["text"],
            text_region=BoundingBox._load(resource["textRegion"]),
            asset_ref=CdfResourceRef._load(resource["assetRef"]),
            confidence=resource.get("confidence"),
        )
