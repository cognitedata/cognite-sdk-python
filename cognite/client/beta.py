from cognite.client._cognite_client import CogniteClient as Client


class CogniteClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(api_subversion="beta", *args, **kwargs)
