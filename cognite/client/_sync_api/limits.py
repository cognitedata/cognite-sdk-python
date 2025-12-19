"""
===============================================================================
8822ef7d66a6cd5cbc9d17cd3fb81fd0
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.limits import LimitValue, LimitValueFilter, LimitValueList
from cognite.client.utils._async_helpers import SyncIterator, run_sync


class SyncLimitsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self, chunk_size: None = None, limit: int | None = None, cursor: str | None = None
    ) -> Iterator[LimitValue]: ...

    @overload
    def __call__(
        self, chunk_size: int, limit: int | None = None, cursor: str | None = None
    ) -> Iterator[LimitValueList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None, cursor: str | None = None
    ) -> Iterator[LimitValue] | Iterator[LimitValueList]:
        """
        Iterate over limit values.

        Fetches limit values as they are iterated over, so you keep a limited number of limit values in memory.

        Args:
            chunk_size (int | None): Number of limit values to return in each chunk.
                Defaults to yielding one limit value a time.
            limit (int | None): Maximum number of limit values to return. Defaults to return all items.
            cursor (str | None): Cursor to use for paging through results.

        Yields:
            LimitValue | LimitValueList: yields LimitValue one by one or in chunks.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.limits(chunk_size=chunk_size, limit=limit, cursor=cursor))  # type: ignore [misc]

    def list(self, limit: int | None = DEFAULT_LIMIT_READ, cursor: str | None = None) -> LimitValueList:
        """
        List all limit values.

        Retrieves all limit values for a specific project.

        Args:
            limit (int | None): Maximum number of limit values to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            cursor (str | None): Cursor to use for paging through results.

        Returns:
            LimitValueList: List of requested limit values.

        Examples:

            List limit values:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> limit_list = client.limits.list(limit=10)

            Iterate over limit values, one-by-one:

                >>> for limit_value in client.limits():
                ...     limit_value  # do something with the limit value

            Iterate over chunks of limit values to reduce memory load:

                >>> for limit_list in client.limits(chunk_size=2500):
                ...     limit_list  # do something with the list
        """
        return run_sync(self.__async_client.limits.list(limit=limit, cursor=cursor))

    def list_advanced(
        self, filter: LimitValueFilter | None = None, limit: int | None = DEFAULT_LIMIT_READ, cursor: str | None = None
    ) -> LimitValueList:
        """
        Advanced list of limit values.

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

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> from cognite.client.data_classes import LimitValueFilter, LimitValuePrefixFilter
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> prefix_filter = LimitValuePrefixFilter(property=["limitId"], value="atlas.")
                >>> filter_obj = LimitValueFilter(prefix=prefix_filter)
                >>> limit_list = client.limits.list_advanced(filter=filter_obj, limit=100)
        """
        return run_sync(self.__async_client.limits.list_advanced(filter=filter, limit=limit, cursor=cursor))

    @overload
    def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[True]) -> LimitValue | None: ...

    @overload
    def retrieve(self, limit_id: str, ignore_unknown_ids: Literal[False] = False) -> LimitValue: ...

    def retrieve(self, limit_id: str, ignore_unknown_ids: bool = False) -> LimitValue | None:
        """
        Retrieve a limit value by its id.

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
        if ignore_unknown_ids:
            return run_sync(self.__async_client.limits.retrieve(limit_id=limit_id, ignore_unknown_ids=True))
        return run_sync(self.__async_client.limits.retrieve(limit_id=limit_id, ignore_unknown_ids=False))
