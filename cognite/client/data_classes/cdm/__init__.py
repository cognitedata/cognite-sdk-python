from cognite.client.config import global_config
from cognite.client.utils._experimental import FeaturePreviewWarning

FeaturePreviewWarning("alpha", "alpha", "Core Data Model").warn()

if global_config.cdm_model_version == "v1":
    from cognite.client.data_classes.cdm.v1 import (
        Activity,
        ActivityApply,
        Asset,
        AssetApply,
        AssetType,
        AssetTypeApply,
        Connection3D,
        Connection3DWrite,
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

    __all__ = [
        "Activity",
        "ActivityApply",
        "Asset",
        "AssetType",
        "AssetTypeApply",
        "AssetApply",
        "Connection3D",
        "Connection3DWrite",
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
else:
    raise ImportError(f"Unknown CDM model version: {global_config.cdm_model_version}")
