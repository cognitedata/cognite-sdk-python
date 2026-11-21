from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.projects import Project, ProjectList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._url import interpolate_and_url_encode

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class ProjectsAPI(APIClient):
    # DEV NOTE: These endpoints are served by the CDF cluster (not CogIdP) and are not part of the public API docs.
    # They are kept in alpha in the SDK so we may change or remove them as we see fit.

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="internal", sdk_maturity="alpha", feature_name="Projects API"
        )

    async def current(self) -> Project:
        """Retrieve the project the client is configured for.

        Requires the ``projectsAcl:READ`` capability.

        Returns:
            Project: The current project, including the organization it belongs to.

        Examples:

            Look up the organization of the current project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.projects.current()
                >>> res.organization
        """
        self._warning.warn()
        path = interpolate_and_url_encode("/api/v1/projects/{}", self._config.project)
        response = await self._get(path, semaphore=self._get_semaphore("read"))
        return Project._load(response.json())

    async def list(self) -> ProjectList:
        """List the projects you have access to.

        Lists all projects the caller has the 'list projects' capability in. The caller may not have access to any
        resources in the listed projects.

        Returns:
            ProjectList: The projects, including the organization each belongs to.

        Examples:

            List projects and their organizations:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.projects.list()
                >>> {p.url_name: p.organization for p in res}
        """
        self._warning.warn()
        response = await self._get(
            "/api/v1/projects", params={"withOrganization": True}, semaphore=self._get_semaphore("read")
        )
        return ProjectList._load(response.json()["items"])
