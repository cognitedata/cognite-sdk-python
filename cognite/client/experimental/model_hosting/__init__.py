from cognite.client.experimental.model_hosting.models import ModelsClient
from cognite.client.experimental.model_hosting.schedules import SchedulesClient
from cognite.client.experimental.model_hosting.source_packages import SourcePackageClient


class ModelHostingClient:
    def __init__(self, client_factory):
        self._client_factory = client_factory
        self._models_client = self._client_factory(ModelsClient)
        self._source_package_client = self._client_factory(SourcePackageClient)
        self._schedules_client = self._client_factory(SchedulesClient)

    @property
    def models(self) -> ModelsClient:
        return self._models_client

    @property
    def source_packages(self) -> SourcePackageClient:
        return self._source_package_client

    @property
    def schedules(self) -> SchedulesClient:
        return self._schedules_client
