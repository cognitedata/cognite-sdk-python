"""
===============================================================================
cce5eacd4949dd8e85347c3906460b9a
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.integrations.config import (
    ConfigRevision,
    ConfigRevisionMetadataList,
    ConfigRevisionWrite,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncIntegrationConfigAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def create(self, config: ConfigRevision | ConfigRevisionWrite) -> ConfigRevision:
        """
        `Create a new configuration revision <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration/operation/new_integration_config>`_

        Args:
            config (ConfigRevision | ConfigRevisionWrite): Configuration revision to create.

        Returns:
            ConfigRevision: Created configuration revision

        Examples:

            Create a config revision:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.integrations import ConfigRevisionWrite
                >>> client = CogniteClient()
                >>> res = client.integrations.config.create(
                ...     ConfigRevisionWrite(external_id="my-integration", config="my config contents")
                ... )
        """
        return run_sync(self.__async_client.integrations.config.create(config=config))

    def retrieve(self, external_id: str, revision: int | None = None) -> ConfigRevision:
        """
        `Retrieve a specific configuration revision, or the latest by default <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration/operation/get_integration_config>`_

        Args:
            external_id (str): External id of the integration to retrieve config from.
            revision (int | None): Optionally specify a revision number to retrieve. Defaults to the latest revision.

        Returns:
            ConfigRevision: Retrieved configuration revision

        Examples:

            Retrieve latest config revision:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.config.retrieve("my-integration")
        """
        return run_sync(self.__async_client.integrations.config.retrieve(external_id=external_id, revision=revision))

    def list(
        self, external_id: str | None = None, limit: int | None = DEFAULT_LIMIT_READ
    ) -> ConfigRevisionMetadataList:
        """
        `List configuration revisions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration/operation/list_integration_configs>`_

        Lists metadata about configuration revisions (without the config contents itself), ordered by revision
        number descending.

        Note:
            This endpoint does not support cursor-based pagination: it always returns up to `limit` of the most
            recent revisions in a single page (server-side capped at 100).

        Args:
            external_id (str | None): Only return config revisions for the integration with this external id.
            limit (int | None): Maximum number of config revisions to return. Defaults to 25.

        Returns:
            ConfigRevisionMetadataList: List of configuration revision metadata

        Examples:

            List config revisions for an integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.config.list(external_id="my-integration")
        """
        return run_sync(self.__async_client.integrations.config.list(external_id=external_id, limit=limit))
