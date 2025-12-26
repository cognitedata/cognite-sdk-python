from __future__ import annotations

from typing import Literal, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes.limits import LimitValue
from cognite.client.exceptions import CogniteAPIError

# API version header for limits endpoints
_LIMITS_API_VERSION = "20230101-alpha"


class LimitsAPI(APIClient):
    _RESOURCE_PATH = "/limits/values"

    @overload
    async def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[True]) -> LimitValue | None: ...

    @overload
    async def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[False] = False) -> LimitValue: ...

    async def retrieve(self, limit_id: str, ignore_unknown_ids: bool = False) -> LimitValue | None:
        """Retrieve a limit value by its id.

        Retrieves a limit value by its `limitId`.

        Args:
            limit_id (str): Limit ID.
                Limits are identified by an id containing the service name and a service-scoped limit name.
                For instance `atlas.monthly_ai_tokens` is the id of the `atlas` service limit `monthly_ai_tokens`.
                Service and limit names are always in `lower_snake_case`.
            ignore_unknown_ids (bool): If True, ignore IDs that are not found rather than throw an exception.

        Returns:
            LimitValue | None: The requested limit value, or None if not found and ignore_unknown_ids is True.

        Examples:

            Get limit by id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.limits.retrieve(limit_id="atlas.monthly_ai_tokens", ignore_unknown_ids=True)
        """
        url_path = f"{self._RESOURCE_PATH}/{limit_id}"
        try:
            res = await self._get(url_path=url_path, headers={"cdf-version": _LIMITS_API_VERSION})
            return LimitValue._load(res.json(), cognite_client=self._cognite_client)
        except CogniteAPIError as e:
            if e.code == 404 and ignore_unknown_ids:
                return None
            raise
