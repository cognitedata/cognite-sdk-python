from cognite.client._cognite_client import CogniteClient as Client
from cognite.client._api.sequences import SequencesAPI

class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sequences = SequencesAPI(self._config, api_version=self.API_VERSION, cognite_client=self)
