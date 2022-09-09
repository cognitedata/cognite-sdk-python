from cognite.client import ClientConfig
from cognite.client._api.diagrams import DiagramsAPI
from cognite.client._api.entity_matching import EntityMatchingAPI
from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, config: ClientConfig) -> None:
        config.api_subversion = "beta"
        super().__init__(config)

        self.diagrams = DiagramsAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.entity_matching = EntityMatchingAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
