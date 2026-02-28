"""
===============================================================================
262ee1d44f5324c5b287f896f43f7ee5
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import SecurityCategory, SecurityCategoryList
from cognite.client.data_classes.iam import SecurityCategoryWrite
from cognite.client.utils._async_helpers import run_sync


class SyncSecurityCategoriesAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> SecurityCategoryList:
        """
        `List security categories. <https://api-docs.cognite.com/20230101/tag/Security-categories/operation/getSecurityCategories>`_

        Args:
            limit (int | None): Max number of security categories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            SecurityCategoryList: List of security categories

        Example:

            List security categories::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.iam.security_categories.list()
        """
        return run_sync(self.__async_client.iam.security_categories.list(limit=limit))

    @overload
    def create(self, security_category: SecurityCategory | SecurityCategoryWrite) -> SecurityCategory: ...

    @overload
    def create(
        self, security_category: Sequence[SecurityCategory] | Sequence[SecurityCategoryWrite]
    ) -> SecurityCategoryList: ...

    def create(
        self,
        security_category: SecurityCategory
        | SecurityCategoryWrite
        | Sequence[SecurityCategory]
        | Sequence[SecurityCategoryWrite],
    ) -> SecurityCategory | SecurityCategoryList:
        """
        `Create one or more security categories. <https://api-docs.cognite.com/20230101/tag/Security-categories/operation/createSecurityCategories>`_

        Args:
            security_category (SecurityCategory | SecurityCategoryWrite | Sequence[SecurityCategory] | Sequence[SecurityCategoryWrite]): Security category or list of categories to create.

        Returns:
            SecurityCategory | SecurityCategoryList: The created security category or categories.

        Example:

            Create security category::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SecurityCategoryWrite
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> my_category = SecurityCategoryWrite(name="My Category")
                >>> res = client.iam.security_categories.create(my_category)
        """
        return run_sync(self.__async_client.iam.security_categories.create(security_category=security_category))

    def delete(self, id: int | Sequence[int]) -> None:
        """
        `Delete one or more security categories. <https://api-docs.cognite.com/20230101/tag/Security-categories/operation/deleteSecurityCategories>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs of security categories to delete.

        Example:

            Delete security category::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.iam.security_categories.delete(1)
        """
        return run_sync(self.__async_client.iam.security_categories.delete(id=id))
