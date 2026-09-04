from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.integrations.config import (
    ConfigRevision,
    ConfigRevisionMetadataList,
    ConfigRevisionWrite,
)
from cognite.client.utils._auxiliary import drop_none_values
from cognite.client.utils._experimental import FeaturePreviewWarning

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class IntegrationConfigAPI(APIClient):
    _RESOURCE_PATH = "/integrations/config"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Integrations")

    async def create(self, config: ConfigRevision | ConfigRevisionWrite) -> ConfigRevision:
        """`Create a new configuration revision <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration/operation/new_integration_config>`_

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
        self._warning.warn()
        if isinstance(config, ConfigRevision):
            config = config.as_write()
        response = await self._post(
            self._RESOURCE_PATH,
            json=config.dump(camel_case=True),
            headers=self._alpha_version_header(),
            semaphore=self._get_semaphore("write"),
        )
        return ConfigRevision._load(response.json())

    async def retrieve(self, external_id: str, revision: int | None = None) -> ConfigRevision:
        """`Retrieve a specific configuration revision, or the latest by default <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration/operation/get_integration_config>`_

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
        self._warning.warn()
        response = await self._get(
            self._RESOURCE_PATH,
            params=drop_none_values({"externalId": external_id, "revision": revision}),
            headers=self._alpha_version_header(),
            semaphore=self._get_semaphore("read"),
        )
        return ConfigRevision._load(response.json())

    async def list(
        self, external_id: str | None = None, limit: int | None = DEFAULT_LIMIT_READ
    ) -> ConfigRevisionMetadataList:
        """`List configuration revisions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Configuration/operation/list_integration_configs>`_

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
        self._warning.warn()
        response = await self._get(
            f"{self._RESOURCE_PATH}/revisions",
            params=drop_none_values({"externalId": external_id, "limit": limit}),
            headers=self._alpha_version_header(),
            semaphore=self._get_semaphore("read"),
        )
        return ConfigRevisionMetadataList._load(response.json()["items"])
