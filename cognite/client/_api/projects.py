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
    # DEV NOTE: These endpoints are served by each CDF cluster (not the global CogIdP) and are, as of writing, not part
    # of the public API docs. The SDK has depended on these endpoints internally (e.g. PrincipalsAPI) for quite some time
    # already.

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="internal", sdk_maturity="alpha", feature_name="Projects API"
        )

    async def current(self) -> Project:
        """Retrieve the configuration of the project *this* client is using.

        Returns:
            Project: The current project configuration.

        Examples:

            Look up the organization of the current project:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> proj = client.projects.current()
                >>> proj.organization
        """
        self._warning.warn()
        return await self._current()

    async def _current(self) -> Project:
        # Essentially a current() without the warning meant for internal use by the SDK (look up organization in OrgAPIClient)
        path = interpolate_and_url_encode("/api/v1/projects/{}", self._config.project)
        response = await self._get(path, semaphore=self._get_semaphore("read"))
        return Project._load(response.json())

    async def list(self) -> ProjectList:
        """List the projects you have access to.

        Note:
            "Have access to" means all projects where you at least have capability the "projectsAcl:READ".

        Returns:
            ProjectList: The list of projects.

        Examples:

            List projects:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> projects = client.projects.list()
        """
        self._warning.warn()
        response = await self._get(
            "/api/v1/projects",
            # We always include the organization & DM status:
            params={"withOrganization": True, "withDataModelingStatus": True},
            semaphore=self._get_semaphore("read"),
        )
        return ProjectList._load(response.json()["items"])
