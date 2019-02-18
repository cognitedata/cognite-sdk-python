from cognite.client.experimental.model_hosting.models import ModelsClient
from cognite.client.experimental.model_hosting.schedules import SchedulesClient
from cognite.client.experimental.model_hosting.source_packages import SourcePackageClient


class ModelHostingClient:
    def __init__(self, client_factory):
        self._client_factory = client_factory

    @property
    def models(self) -> ModelsClient:
        return self._client_factory(ModelsClient)

    @property
    def source_packages(self) -> SourcePackageClient:
        return self._client_factory(SourcePackageClient)

    @property
    def schedules(self) -> SchedulesClient:
        return self._client_factory(SchedulesClient)
