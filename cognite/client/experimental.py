from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.model_hosting import ModelHostingAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._cognite_client import CogniteClient as Client


class ExperimentalDatapointsApi(DatapointsAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model_hosting = ModelHostingAPI(self._config, api_version="playground", cognite_client=self)
        self.relationships = RelationshipsAPI(self._config, api_version="playground", cognite_client=self)
