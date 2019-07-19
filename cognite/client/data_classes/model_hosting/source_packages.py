from typing import *

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, CogniteResponse


class CreateSourcePackageResponse(CogniteResponse):
    def __init__(self, id: int = None, upload_url: str = None):
        self.id = id
        self.upload_url = upload_url

    @classmethod
    def _load(cls, api_response):
        return cls(id=api_response["data"]["id"], upload_url=api_response["data"].get("uploadUrl"))


class SourcePackage(CogniteResource):
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
