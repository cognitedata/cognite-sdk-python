from cognite.client._api.templates import TemplatesAPI
from cognite.client.beta import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.templates = TemplatesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
