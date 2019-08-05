from cognite.client._api.model_hosting import ModelHostingAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sequences = SequencesAPI(self._config, api_version=self._API_VERSION, cognite_client=self)
        self.model_hosting = ModelHostingAPI(self._config, api_version="0.6", cognite_client=self)
