from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.limits import LimitValue, LimitValueFilter, LimitValueList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import is_unlimited
from cognite.client.utils._validation import verify_limit

# API version header for limits endpoints
_LIMITS_API_VERSION = "20230101-alpha"


class LimitsAPI(APIClient):
    _RESOURCE_PATH = "/limits/values"

    def __iter__(self) -> Iterator[LimitValue]:
        """Iterate over limit values.

        Fetches limit values as they are iterated over, so you keep a limited number of limit values in memory.

        Returns:
            Iterator[LimitValue]: yields LimitValue one by one.
        """
        return self()

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[LimitValue]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[LimitValueList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> Iterator[LimitValue] | Iterator[LimitValueList]:
        """Iterate over limit values.

        Fetches limit values as they are iterated over, so you keep a limited number of limit values in memory.

        Args:
            chunk_size (int | None): Number of limit values to return in each chunk.
                Defaults to yielding one limit value a time.
            limit (int | None): Maximum number of limit values to return. Defaults to return all items.
            cursor (str | None): Cursor to use for paging through results.

        Returns:
            Iterator[LimitValue] | Iterator[LimitValueList]: yields LimitValue one by one or in chunks.
        """
        return self._list_generator(
            list_cls=LimitValueList,
            resource_cls=LimitValue,
            method="GET",
            limit=limit,
            chunk_size=chunk_size,
            initial_cursor=cursor,
            url_path=self._RESOURCE_PATH,
            headers={"cdf-version": _LIMITS_API_VERSION},
        )

    def list(
        self,
        limit: int | None = DEFAULT_LIMIT_READ,
        cursor: str | None = None,
    ) -> LimitValueList:
        """List all limit values.

        Retrieves all limit values for a specific project.

        Args:
            limit (int | None): Maximum number of limit values to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            cursor (str | None): Cursor to use for paging through results.

        Returns:
            LimitValueList: List of requested limit values.

        Examples:

            List limit values:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> limit_list = client.limits.list(limit=10)

            Iterate over limit values:

                >>> for limit_value in client.limits:
                ...     limit_value # do something with the limit value

            Iterate over chunks of limit values to reduce memory load:

                >>> for limit_list in client.limits(chunk_size=2500):
                ...     limit_list # do something with the list
        """
        return self._list(
            list_cls=LimitValueList,
            resource_cls=LimitValue,
            method="GET",
            limit=limit,
            initial_cursor=cursor,
            url_path=self._RESOURCE_PATH,
            headers={"cdf-version": _LIMITS_API_VERSION},
        )

    def list_advanced(
        self,
        filter: LimitValueFilter | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        cursor: str | None = None,
    ) -> LimitValueList:
        """Advanced list of limit values.

        Retrieves limit values using a filter. Only the `prefix` operator is supported.

        Args:
            filter (LimitValueFilter | None): Filter to apply to the list operation.
                To retrieve all limits for a specific service, use the "prefix" operator where the property
                is the limit's key.
            limit (int | None): Maximum number of limit values to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            cursor (str | None): Cursor to use for paging through results.

        Returns:
            LimitValueList: Filtered limit values.

        Examples:

            List limit values for a specific service:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import LimitValueFilter, LimitValuePrefixFilter
                >>> client = CogniteClient()
                >>> prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
                >>> filter_obj = LimitValueFilter(prefix=prefix_filter)
                >>> limit_list = client.limits.list_advanced(filter=filter_obj, limit=100)
        """
        verify_limit(limit)
        # Convert unlimited limits (-1, float("inf")) to None for API
        if is_unlimited(limit):
            limit = None

        filter_dict = filter.dump(camel_case=True) if filter is not None else {}

        body: dict[str, Any] = {}
        if filter_dict:
            body["filter"] = filter_dict
        if limit is not None:
            body["limit"] = limit
        if cursor is not None:
            body["cursor"] = cursor

        url_path = f"{self._RESOURCE_PATH}/list"
        res = self._post(url_path=url_path, json=body, api_subversion=_LIMITS_API_VERSION)
        response = res.json()
        return LimitValueList._load(response, cognite_client=self._cognite_client)

    @overload
    def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[True]) -> LimitValue | None: ...

    @overload
    def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[False] = False) -> LimitValue: ...

    def retrieve(self, limit_id: str, ignore_unknown_ids: bool = False) -> LimitValue | None:
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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.limits.retrieve(limit_id="atlas.monthly_ai_tokens", ignore_unknown_ids=True)
        """
        url_path = f"{self._RESOURCE_PATH}/{limit_id}"
        try:
            res = self._get(url_path=url_path, headers={"cdf-version": _LIMITS_API_VERSION})
            return LimitValue._load(res.json(), cognite_client=self._cognite_client)
        except CogniteAPIError as e:
            if e.code == 404 and ignore_unknown_ids:
                return None
            raise
