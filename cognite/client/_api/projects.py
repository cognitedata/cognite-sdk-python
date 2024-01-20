from __future__ import annotations

from cognite.client._api_client import APIClient


class ProjectsAPI(APIClient):
    _RESOURCE_PATH = "/projects"

    def create(self) -> None:
        raise NotImplementedError

    def retrieve(self) -> None:
        raise NotImplementedError

    def update(self) -> None:
        raise NotImplementedError

    def list(self) -> None:
        raise NotImplementedError
