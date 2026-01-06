from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.limits import Limit
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import Identifier

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class LimitsAPI(APIClient):
    _RESOURCE_PATH = "/limits/values"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(
            api_maturity="alpha",
            sdk_maturity="alpha",
            feature_name="Limits API",
        )

    async def retrieve(self, id: str) -> Limit | None:
        """`Retrieve a limit value by its id. <https://api-docs.cognite.com/20230101-alpha/tag/Limits/operation/fetchLimitById/>`_

        Retrieves a limit value by its `limitId`.

        Args:
            id (str): Limit ID to retrieve.
                Limits are identified by an id containing the service name and a service-scoped limit name.
                For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
                Service and limit names are always in `lower_snake_case`.

        Returns:
            Limit | None: The requested limit, or `None` if not found.

        Examples:

            Retrieve a single limit by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.limits.retrieve(id="atlas.monthly_ai_tokens")
        """
        self._warning.warn()

        headers = {"cdf-version": f"{self._config.api_subversion}-alpha"}

        return await self._retrieve(
            identifier=Identifier(id),
            cls=Limit,
            headers=headers,
        )
