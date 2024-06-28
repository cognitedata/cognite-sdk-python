from cognite.client.config import global_config
from cognite.client.utils._experimental import FeaturePreviewWarning

FeaturePreviewWarning("alpha", "alpha", "Core Data Model").warn()

if global_config.cdm_model_version == "v1":
    from cognite.client.data_classes.cdm.v1 import (
        Activity,
        ActivityWrite,
        Asset,
        AssetType,
        AssetTypeWrite,
        AssetWrite,
        Connection3D,
        Connection3DWrite,
        Describable,
        DescribableWrite,
        Equipment,
        EquipmentWrite,
        Model3D,
        Model3DWrite,
        Object3D,
        Object3DWrite,
        Schedulable,
        SchedulableWrite,
        Sourceable,
        SourceableWrite,
        TimeSeriesBase,
        TimesSeriesBaseWrite,
    )

    __all__ = [
        "Activity",
        "ActivityWrite",
        "Asset",
        "AssetType",
        "AssetTypeWrite",
        "AssetWrite",
        "Connection3D",
        "Connection3DWrite",
        "Describable",
        "DescribableWrite",
        "Equipment",
        "EquipmentWrite",
        "Model3D",
        "Model3DWrite",
        "Object3D",
        "Object3DWrite",
        "Schedulable",
        "SchedulableWrite",
        "Sourceable",
        "SourceableWrite",
        "TimeSeriesBase",
        "TimesSeriesBaseWrite",
    ]
else:
    raise ImportError(f"Unknown CDM model version: {global_config.cdm_model_version}")
