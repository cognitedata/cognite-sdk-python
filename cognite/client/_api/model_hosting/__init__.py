from cognite.client._api.model_hosting.models import ModelsAPI
from cognite.client._api.model_hosting.schedules import SchedulesAPI
from cognite.client._api.model_hosting.source_packages import SourcePackagesAPI
from cognite.client._api.model_hosting.versions import ModelVersionsAPI
from cognite.client._api_client import APIClient


class ModelHostingAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = ModelsAPI(*args, **kwargs)
        self.versions = ModelVersionsAPI(*args, **kwargs)
        self.source_packages = SourcePackagesAPI(*args, **kwargs)
        self.schedules = SchedulesAPI(*args, **kwargs)
