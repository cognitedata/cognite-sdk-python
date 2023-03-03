from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, config):
        config.api_subversion = "beta"
        super().__init__(config)
