from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client._api_client import APIClient
from cognite.client.data_classes.filters import In
from cognite.client.data_classes.limits import Limit, LimitList
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils.useful_types import SequenceNotStr

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

    async def retrieve_multiple(
        self,
        ids: SequenceNotStr[str],
    ) -> LimitList:
        """`Retrieve multiple limit values by their ids. <https://api-docs.cognite.com/20230101-alpha/tag/Limits/operation/listLimitsAdvanced/>`_

        Retrieves multiple limit values by their `limitId`s.

        Args:
            ids (SequenceNotStr[str]): List of limit IDs to retrieve.
                Limits are identified by an id containing the service name and a service-scoped limit name.
                For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
                Service and limit names are always in `lower_snake_case`.

        Returns:
            LimitList: List of requested limit values. Only limits that exist will be returned.

        Examples:

            Retrieve multiple limits by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.limits.retrieve_multiple(ids=["atlas.monthly_ai_tokens", "files.storage_bytes"])
        """
        self._warning.warn()

        advanced_filter = In(property=["limitId"], values=list(ids))

        return await self._list(
            method="POST",
            list_cls=LimitList,
            resource_cls=Limit,
            advanced_filter=advanced_filter,
            headers={"cdf-version": f"{self._config.api_subversion}-alpha"},
        )
