from cognite.client.data_classes._base import CogniteResource


class Project(CogniteResource):
    def __init__(self, name: str, url_name: str):
        ...
