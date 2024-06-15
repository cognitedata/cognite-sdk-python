from cognite.client.config import global_config

if global_config.cdm_model_version == "v1":
    from cognite.client.data_classes.cdm.v1 import Sourceable

    __all__ = ["Sourceable"]
else:
    raise ImportError(f"Unknown CDM model version: {global_config.cdm_model_version}")
