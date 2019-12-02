from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse


class CreateSourcePackageResponse(CogniteResponse):
    """The response returned from the API when creating a new source package.

    Args:
        id (int): The id of the source package
        upload_url (str): The url to upload the source package distribution to.
    """

    def __init__(self, id: int = None, upload_url: str = None):
        self.id = id
        self.upload_url = upload_url

    @classmethod
    def _load(cls, api_response):
        return cls(id=api_response["id"], upload_url=api_response.get("uploadUrl"))


class SourcePackage(CogniteResource):
    """A representation of a source package in the model hosting environment.

    Args:
        id (int): Id of the source package.
        name (str): Name of the source package.
        description (str): Description of the schedule.
        is_deprecated (bool): Whether or not the source package is deprecated.
        package_name (str): The name of the package containing the model.py file.
        is_uploaded (bool): Whether or not the source package has been uploaded
        available_operations (List[str]): The available operations on this source package. Can be any of [PREDICT, TRAIN].
        created_time (int): Created time in UNIX.
        runtime_version (str): The runtime version this source package should be deployed with. Can be any of ["0.1"]
        metadata (Dict): User-defined metadata about the source package.
        cognite_client (CogniteClient): An optional CogniteClient to associate with this data class.
    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        description: str = None,
        is_deprecated: bool = None,
        package_name: str = None,
        is_uploaded: bool = None,
        available_operations: List = None,
        created_time: int = None,
        runtime_version: str = None,
        metadata: Dict = None,
        cognite_client=None,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.is_deprecated = is_deprecated
        self.package_name = package_name
        self.is_uploaded = is_uploaded
        self.available_operations = available_operations
        self.created_time = created_time
        self.runtime_version = runtime_version
        self.metadata = metadata
        self._cognite_client = cognite_client


class SourcePackageList(CogniteResourceList):
    _RESOURCE = SourcePackage
    _ASSERT_CLASSES = False
