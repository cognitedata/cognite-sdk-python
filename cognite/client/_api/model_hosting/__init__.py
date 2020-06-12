from cognite.client._api.model_hosting.models import ModelsAPI
from cognite.client._api.model_hosting.schedules import SchedulesAPI
from cognite.client._api.model_hosting.source_packages import SourcePackagesAPI
from cognite.client._api.model_hosting.versions import ModelVersionsAPI
from cognite.client._api_client import APIClient
from cognite.client.utils._experimental_warning import experimental_api


@experimental_api(api_name="Model Hosting", deprecated=True)
class ModelHostingAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models = ModelsAPI(*args, **kwargs)
        self.versions = ModelVersionsAPI(*args, **kwargs)
        self.source_packages = SourcePackagesAPI(*args, **kwargs)
        self.schedules = SchedulesAPI(*args, **kwargs)
