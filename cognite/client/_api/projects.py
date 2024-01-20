from __future__ import annotations

from typing import Sequence, overload
from urllib.parse import quote

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
        """`Create a project <https://developer.cognite.com/api#tag/Projects/operation/createProject>`_

        Args:
            item (ProjectWrite | Sequence[ProjectWrite]): Project(s) to create

        Returns:
            Project | ProjectList: Created project(s)

        Examples:
            Create a new project with the name "my project"

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import ProjectWrite
            >>> c = CogniteClient()
            >>> project = ProjectWrite(name="my project", url_name="my-project", parent_project_url_name="root")
            >>> res = c.iam.projects.create(project)
        """
        return self._create_multiple(item, list_cls=ProjectList, resource_cls=Project, input_resource_cls=ProjectWrite)

    def retrieve(self, project: str) -> Project:
        """`Retrieve a project <https://developer.cognite.com/api#tag/Projects/operation/getProject>`_

        Args:
            project (str): Project to retrieve

        Returns:
            Project: The requested project

        Examples:

            Retrieve the project with the name "publicdata"

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.projects.retrieve("publicdata")
        """
        item = self._get(f"{self._RESOURCE_PATH}/{quote(project, '')}")
        return Project._load(item.json(), cognite_client=self._cognite_client)

    def update(self, item: ProjectUpdate) -> Project:
        """`Update a project <https://developer.cognite.com/api#tag/Projects/operation/updateProject>`_

        Args:
            item (ProjectUpdate): Project to update

        Returns:
            Project: Updated project

        Examples:

            >>> from cognite.client import CogniteClient
            >>> from cognite.client.data_classes import ProjectUpdate
            >>> c = CogniteClient()
            >>> my_update = ProjectUpdate("my_project").name.set("new name").oidc_configuration.modify.skew_ms.set(100)
            >>> res = c.iam.projects.update(my_update)
        """
        project = item._project
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{quote(project, '')}/update", json=item.dump(camel_case=True)
        )
        return Project._load(response.json(), cognite_client=self._cognite_client)

    def list(self) -> ProjectURLNameList:
        """`List all projects <https://developer.cognite.com/api#tag/Projects/operation/listProjects>`_

        Returns:
            ProjectURLNameList: List of project URL names

        Examples:

            List all projects

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.projects.list()
        """
        items = self._get(self._RESOURCE_PATH)
        return ProjectURLNameList.load(items.json(), cognite_client=self._cognite_client)
