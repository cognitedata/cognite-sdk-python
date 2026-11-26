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
        Retrieve the configuration of the project *this* client is using.

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
        return run_sync(self.__async_client.projects.current())

    def list(self) -> ProjectList:
        """
        List the projects you have access to.

        Only projects the caller has the capability "projectsAcl:READ" for will be included in the list.

        Returns:
            ProjectList: The list of projects.

        Examples:

            List projects:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> projects = client.projects.list()
        """
        return run_sync(self.__async_client.projects.list())
