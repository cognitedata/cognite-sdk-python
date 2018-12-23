from cognite.client.experimental.analytics.models import ModelsClient


class AnalyticsClient:
    def __init__(self, client_factory, version=None):
        self._client_factory = client_factory
        self._version = version

    @property
    def models(self) -> ModelsClient:
        return self._client_factory(ModelsClient, self._version)
