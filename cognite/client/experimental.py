from cognite.client._api.model_hosting import ModelHostingAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model_hosting = ModelHostingAPI(self._config, api_version="0.6", cognite_client=self)
        self.relationships = RelationshipsAPI(self._config, api_version="playground", cognite_client=self)
