from cognite.client.v0_6.analytics.models import ModelsClientV0_6


class AnalyticsClient:
    def __init__(self, cognite_client):
        self._cognite_client = cognite_client

    @property
    def models(self) -> ModelsClientV0_6:
        return self._cognite_client.client_factory(ModelsClientV0_6)
