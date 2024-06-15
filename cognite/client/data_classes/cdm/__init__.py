from cognite.client.config import global_config

if global_config.cdm_model_version == "v1":
    from cognite.client.data_classes.cdm.v1 import (
        Activity,
        Asset,
        AssetType,
        Connection3D,
        Describable,
        Equipment,
        Model3D,
        Object3D,
        Schedulable,
        Sourceable,
        TimeSeriesBase,
    )

    __all__ = [
        "Sourceable",
        "Describable",
        "Schedulable",
        "Connection3D",
        "Model3D",
        "Object3D",
        "Asset",
        "AssetType",
        "Activity",
        "Equipment",
        "TimeSeriesBase",
    ]
else:
    raise ImportError(f"Unknown CDM model version: {global_config.cdm_model_version}")
