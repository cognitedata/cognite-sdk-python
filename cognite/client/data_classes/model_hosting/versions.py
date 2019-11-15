from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse


class ModelVersion(CogniteResource):
    """A representation of a Model version in the model hosting environment.

    Args:
        name (str): Name of the model version.
        is_deprecated (bool): Whether or not the model version is deprecated.
        training_details (Dict): The training details for this model version. None if the associated source package
            does not define a .train() method.
        error_msg (str): The error message produced when trying to deploy the model version.
        model_name (str): The name of the model associated with this version.
        created_time (int): Created time in UNIX.
        metadata (Dict): User-defined metadata about the model.
        source_package_id (int): The id of the source package associated with this version.
        status (str): The current status of the model version deployment.
        description (str): Description of the model.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        name: str = None,
        is_deprecated: bool = None,
        training_details: Dict = None,
        error_msg: str = None,
        model_name: str = None,
        created_time: int = None,
        metadata: Dict = None,
        source_package_id: int = None,
        status: str = None,
        description: str = None,
        cognite_client=None,
    ):
        self.name = name
        self.is_deprecated = is_deprecated
        self.training_details = training_details
        self.error_msg = error_msg
        self.model_name = model_name
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


class ModelVersionLog(CogniteResponse):
    """An object containing the logs for a model version.

    Args:
        prediction_logs (List): A list of log entries for the prediction routine
        training_logs (List): A list of log entries for the training routine
    """

    def __init__(self, prediction_logs: List = None, training_logs: List = None):
        self.prediction_logs = prediction_logs
        self.training_logs = training_logs

    @classmethod
    def _load(cls, api_response):
        return cls(prediction_logs=api_response["predict"], training_logs=api_response["train"])
