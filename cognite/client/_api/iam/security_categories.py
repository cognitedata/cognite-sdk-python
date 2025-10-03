from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes import SecurityCategory, SecurityCategoryList
from cognite.client.data_classes.iam import SecurityCategoryWrite
from cognite.client.utils._identifier import IdentifierSequence


class SecurityCategoriesAPI(APIClient):
    _RESOURCE_PATH = "/securitycategories"

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> SecurityCategoryList:
        """`List security categories. <https://developer.cognite.com/api#tag/Security-categories/operation/getSecurityCategories>`_

        Args:
            limit (int | None): Max number of security categories to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            SecurityCategoryList: List of security categories

        Example:

            List security categories::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.security_categories.list()
        """
        return self._list(list_cls=SecurityCategoryList, resource_cls=SecurityCategory, method="GET", limit=limit)

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
        """`Create one or more security categories. <https://developer.cognite.com/api#tag/Security-categories/operation/createSecurityCategories>`_

        Args:
            security_category (SecurityCategory | SecurityCategoryWrite | Sequence[SecurityCategory] | Sequence[SecurityCategoryWrite]): Security category or list of categories to create.

        Returns:
            SecurityCategory | SecurityCategoryList: The created security category or categories.

        Example:

            Create security category::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SecurityCategoryWrite
                >>> client = CogniteClient()
                >>> my_category = SecurityCategoryWrite(name="My Category")
                >>> res = client.iam.security_categories.create(my_category)
        """
        return self._create_multiple(
            list_cls=SecurityCategoryList,
            resource_cls=SecurityCategory,
            items=security_category,
            input_resource_cls=SecurityCategoryWrite,
        )

    def delete(self, id: int | Sequence[int]) -> None:
        """`Delete one or more security categories. <https://developer.cognite.com/api#tag/Security-categories/operation/deleteSecurityCategories>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs of security categories to delete.

        Example:

            Delete security category::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.iam.security_categories.delete(1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)
