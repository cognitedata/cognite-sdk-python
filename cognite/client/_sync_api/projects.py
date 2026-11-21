"""
===============================================================================
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.projects import Project, ProjectList
from cognite.client.utils._async_helpers import run_sync


class SyncProjectsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def current(self) -> Project:
        """
        Retrieve the project the client is configured for.

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
        return run_sync(self.__async_client.projects.current())

    def list(self) -> ProjectList:
        """
        List the projects you have access to.

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
        return run_sync(self.__async_client.projects.list())
