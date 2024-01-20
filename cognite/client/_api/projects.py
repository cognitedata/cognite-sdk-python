from __future__ import annotations

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Project, ProjectUpdate, ProjectWrite


class ProjectsAPI(APIClient):
    _RESOURCE_PATH = "/projects"

    def create(self, item: ProjectWrite) -> Project:
        """`Create a project <https://developer.cognite.com/api#tag/Projects/operation/createProject>`_"""
        raise NotImplementedError

    def retrieve(self, project: str) -> Project:
        """`Retrieve a project <https://developer.cognite.com/api#tag/Projects/operation/getProject>`_"""
        raise NotImplementedError

    def update(self, item: ProjectWrite | ProjectUpdate) -> Project:
        """`Update a project <https://developer.cognite.com/api#tag/Projects/operation/updateProject>`_"""
        raise NotImplementedError

    def list(self) -> list[str]:
        """`List all projects <https://developer.cognite.com/api#tag/Projects/operation/listProjects>`_"""
        raise NotImplementedError
