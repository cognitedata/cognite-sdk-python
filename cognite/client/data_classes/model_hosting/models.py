from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse


class Model(CogniteResource):
    def __init__(
        self,
        id: int = None,
        name: str = None,
        description: str = None,
        created_time: int = None,
        metadata: Dict = None,
        is_deprecated: bool = None,
        active_version_id: int = None,
        input_fields: List = None,
        output_fields: List = None,
        webhook_url: str = None,
        cognite_client=None,
    ):
        self.id = id
        self.name = name
        self.project = description
        self.description = description
        self.created_time = created_time
        self.metadata = metadata
        self.is_deprecated = is_deprecated
        self.active_version_id = active_version_id
        self.input_fields = input_fields
        self.output_fields = output_fields
        self.webhook_url = webhook_url
        self._cognite_client = cognite_client


class ModelList(CogniteResourceList):
    _RESOURCE = Model
    _ASSERT_CLASSES = False


class ModelVersion(CogniteResource):
    def __init__(
        self,
        id: int = None,
        is_deprecated: bool = None,
        training_details: Dict = None,
        name: str = None,
        error_msg: str = None,
        model_id: int = None,
        created_time: int = None,
        metadata: Dict = None,
        source_package_id: int = None,
        status: str = None,
        description: str = None,
        cognite_client=None,
    ):
        self.id = id
        self.is_deprecated = is_deprecated
        self.training_details = training_details
        self.name = name
        self.error_msg = error_msg
        self.model_id = model_id
        self.created_time = created_time
        self.metadata = metadata
        self.source_package_id = source_package_id
        self.status = status
        self.description = description
        self._cognite_client = cognite_client


class ModelVersionList(CogniteResourceList):
    _RESOURCE = ModelVersion
    _ASSERT_CLASSES = False


class ModelArtifact(CogniteResource):
    def __init__(self, name: str = None, size: int = None, cognite_client=None):
        self.name = name
        self.size = size
        self._cognite_client = cognite_client


class ModelArtifactList(CogniteResourceList):
    _RESOURCE = ModelArtifact
    _ASSERT_CLASSES = False


class ModelLog(CogniteResponse):
    def __init__(self, prediction_logs: List = None, training_logs: List = None):
        self.prediction_logs = prediction_logs
        self.training_logs = training_logs

    @classmethod
    def _load(cls, api_response):
        return cls(prediction_logs=api_response["data"]["predict"], training_logs=api_response["data"]["train"])
