from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.limits import Limit, LimitList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import Identifier

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig
    from cognite.client.data_classes.filters import Filter


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

    async def list(self, filter: Filter | None = None, limit: int | None = 1000) -> LimitList:
        """`List all limit values <https://api-docs.cognite.com/20230101-alpha/tag/Limits/operation/listLimits/>`_

        Retrieves all limit values for a specific project. Optionally filter by limit ID prefix using a `Prefix` filter.

        Args:
            filter (Filter | None): Optional `Prefix` filter to apply on the `limitId` property.
                When a filter is provided, the method uses POST to `/limits/values/list` endpoint.
                Only `Prefix` filters are supported for filtering limits by `limitId`.
            limit (int | None): Maximum number of limits to return. Defaults to 1000. Set to None or -1 to return all limits.

        Returns:
            LimitList: List of all limit values in the project.

        Examples:

            List all limits:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> limits = client.limits.list()

            List all limits with a specific limit:

                >>> limits = client.limits.list(limit=100)

            List limits filtered by prefix (e.g., all limits for the 'atlas' service):

                >>> from cognite.client.data_classes import filters
                >>> prefix_filter = filters.Prefix(["limitId"], "atlas.")
                >>> limits = client.limits.list(filter=prefix_filter)
        """
        self._warning.warn()

        headers = {"cdf-version": f"{self._config.api_subversion}-alpha"}

        if filter is not None:
            return await self._list(
                method="POST",
                list_cls=LimitList,
                resource_cls=Limit,
                limit=limit,
                filter=filter.dump(camel_case_property=True),
                headers=headers,
            )

        return await self._list(
            method="GET",
            list_cls=LimitList,
            resource_cls=Limit,
            limit=limit,
            headers=headers,
        )
