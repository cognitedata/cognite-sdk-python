from __future__ import annotations

from cognite.client.data_classes.cdm.v1 import (
    Activity,
    ActivityApply,
    Asset,
    AssetApply,
    AssetType,
    AssetTypeApply,
    Connection3D,
    Connection3DApply,
    Describable,
    DescribableApply,
    Equipment,
    EquipmentApply,
    Model3D,
    Model3DApply,
    Object3D,
    Object3DApply,
    Schedulable,
    SchedulableApply,
    Sourceable,
    SourceableApply,
    TimeSeriesBase,
    TimesSeriesBaseApply,
)
from cognite.client.utils._experimental import FeaturePreviewWarning

__all__ = [
    "Activity",
    "ActivityApply",
    "Asset",
    "AssetType",
    "AssetTypeApply",
    "AssetApply",
    "Connection3D",
    "Connection3DApply",
    "Describable",
    "DescribableApply",
    "Equipment",
    "EquipmentApply",
    "Model3D",
    "Model3DApply",
    "Object3D",
    "Object3DApply",
    "Schedulable",
    "SchedulableApply",
    "Sourceable",
    "SourceableApply",
    "TimeSeriesBase",
    "TimesSeriesBaseApply",
]

FeaturePreviewWarning("alpha", "alpha", "Core Data Model").warn()
