from cognite.client._api_client import APIClient


class DocumentsAPI(APIClient):
    _RESOURCE_PATH = "/documents"

    def aggregate(self):
        ...

    def retrieve(self, id: int):
        ...

    def search(self):
        ...

    def list(self):
        ...
