from __future__ import annotations

from typing import Sequence, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Project, ProjectList, ProjectUpdate, ProjectURLNameList, ProjectWrite


class ProjectsAPI(APIClient):
    _RESOURCE_PATH = "/projects"

    @overload
    def create(self, item: ProjectWrite) -> Project:
        ...

    @overload
    def create(self, item: Sequence[ProjectWrite]) -> ProjectList:
        ...

    def create(self, item: ProjectWrite | Sequence[ProjectWrite]) -> Project | ProjectList:
        """`Create a project <https://developer.cognite.com/api#tag/Projects/operation/createProject>`_"""
        return self._create_multiple(item, list_cls=ProjectList, resource_cls=Project, input_resource_cls=ProjectWrite)

    def retrieve(self, project: str) -> Project:
        """`Retrieve a project <https://developer.cognite.com/api#tag/Projects/operation/getProject>`_"""
        item = self._get(f"{self._RESOURCE_PATH}/{project}")
        return Project._load(item.json(), cognite_client=self._cognite_client)

    def update(self, item: ProjectWrite | ProjectUpdate) -> Project:
        """`Update a project <https://developer.cognite.com/api#tag/Projects/operation/updateProject>`_"""
        return self._update_multiple(item, list_cls=ProjectList, resource_cls=Project, update_cls=ProjectUpdate)

    def list(self) -> ProjectURLNameList:
        """`List all projects <https://developer.cognite.com/api#tag/Projects/operation/listProjects>`_"""
        items = self._get(self._RESOURCE_PATH)
        return ProjectURLNameList.load(items.json(), cognite_client=self._cognite_client)
