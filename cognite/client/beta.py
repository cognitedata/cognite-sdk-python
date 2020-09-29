from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.relationships = RelationshipsAPI(self._config, api_version="beta", cognite_client=self)
