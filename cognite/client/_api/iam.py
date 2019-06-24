import numbers
from typing import *

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    APIKey,
    APIKeyList,
    Group,
    GroupList,
    SecurityCategory,
    SecurityCategoryList,
    ServiceAccount,
    ServiceAccountList,
)


class IAMAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.service_accounts = ServiceAccountsAPI(*args, **kwargs)
        self.api_keys = APIKeysAPI(*args, **kwargs)
        self.groups = GroupsAPI(*args, **kwargs)
        self.security_categories = SecurityCategoriesAPI(*args, **kwargs)


class ServiceAccountsAPI(APIClient):
    _RESOURCE_PATH = "/serviceaccounts"
    _LIST_CLASS = ServiceAccountList

    def list(self) -> ServiceAccountList:
        """List service accounts.

        Returns:
            ServiceAccountList: List of service accounts.

        Example:

            List service accounts::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.service_accounts.list()
        """
        return ServiceAccountList._load(self._get(url_path=self._RESOURCE_PATH).json()["items"])

    def create(
        self, service_account: Union[ServiceAccount, List[ServiceAccount]]
    ) -> Union[ServiceAccount, ServiceAccountList]:
        """Create one or more new service accounts.

        Args:
            service_account (Union[ServiceAccount, List[ServiceAccount]]): The service account(s) to create.

        Returns:
            Union[ServiceAccount, ServiceAccountList]: The created service account(s).

        Example:

            Create service account::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import ServiceAccount
                >>> c = CogniteClient()
                >>> my_account = ServiceAccount(name="my@service.com", groups=[1, 2, 3])
                >>> res = c.iam.service_accounts.create(my_account)
        """
        return self._create_multiple(items=service_account)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more service accounts.

        Args:
            id (Union[int, List[int]]): ID or list of IDs to delete.

        Returns:
            None

        Example:

            Delete service account by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.service_accounts.delete(1)
        """
        self._delete_multiple(ids=id, wrap_ids=False)


class APIKeysAPI(APIClient):
    _RESOURCE_PATH = "/apikeys"
    _LIST_CLASS = APIKeyList

    def list(self, include_deleted: bool = False, all: bool = False, service_account_id: bool = None) -> APIKeyList:
        """List api keys.

        Args:
            include_deleted (bool): Whether or not to include deleted api keys. Defaults to False.
            all (bool): Whether or not to return all api keys for this project. Requires users:list acl. Defaults to False.
            service_account_id (int): Get api keys for this service account only. Only available to admin users.

        Returns:
            APIKeyList: List of api keys.

        Example:

            List api keys::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.api_keys.list()
        """
        res = self._get(
            self._RESOURCE_PATH,
            params={"all": all, "serviceAccountId": service_account_id, "includeDeleted": include_deleted},
        )
        return APIKeyList._load(res.json()["items"])

    def create(self, service_account_id: Union[int, List[int]]) -> Union[APIKey, APIKeyList]:
        """Create a new api key for one or more service accounts.

        Args:
            service_account_id (Union[int, List[int]]): ID or list of IDs of service accounts to create an api key for.

        Returns:
            Union[APIKey, APIKeyList]: API key or list of api keys.

        Example:

            Create new api key for a given service account::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.api_keys.create(1)
        """
        utils._auxiliary.assert_type(service_account_id, "service_account_id", [numbers.Integral, list])
        if isinstance(service_account_id, numbers.Integral):
            items = {"serviceAccountId": service_account_id}
        else:
            items = [{"serviceAccountId": sa_id} for sa_id in service_account_id]
        return self._create_multiple(items=items)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more api keys.

        Args:
            id (Union[int, List[int]]): ID or list of IDs of api keys to delete.

        Returns:
            None

        Example:

            Delete api key for a given service account::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.api_keys.delete(1)
        """
        self._delete_multiple(ids=id, wrap_ids=False)


class GroupsAPI(APIClient):
    _RESOURCE_PATH = "/groups"
    _LIST_CLASS = GroupList

    def list(self, all: bool = False) -> GroupList:
        """List groups.

        Args:
            all (bool): Whether to get all groups, only available with the groups:list acl.

        Returns:
            GroupList: List of groups.

        Example:

            List groups::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.groups.list()
        """
        res = self._get(self._RESOURCE_PATH, params={"all": all})
        return GroupList._load(res.json()["items"])

    def create(self, group: Union[Group, List[Group]]) -> Union[Group, GroupList]:
        """Create one or more groups.

        Args:
            group (Union[Group, List[Group]]): Group or list of groups to create.
        Returns:
            Union[Group, GroupList]: The created group(s).

        Example:

            Create group::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import Group
                >>> c = CogniteClient()
                >>> my_capabilities = [{"groupsAcl": {"actions": ["LIST"],"scope": {"all": { }}}}]
                >>> my_group = Group(name="My Group", capabilities=my_capabilities)
                >>> res = c.iam.groups.create(my_group)
        """
        return self._create_multiple(group)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more groups.

        Args:
            id (Union[int, List[int]]): ID or list of IDs of groups to delete.

        Returns:
            None

        Example:

            Delete group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.groups.delete(1)
        """
        self._delete_multiple(ids=id, wrap_ids=False)

    def list_service_accounts(self, id: int) -> ServiceAccountList:
        """List service accounts in a group.

        Args:
            id (int): List service accounts which are a member of this group.

        Returns:
            ServiceAccountList: List of service accounts.

        Example:

            List service accounts in a group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.groups.list_service_accounts(1)
        """
        resource_path = self._RESOURCE_PATH + "/{}/serviceaccounts".format(id)
        return ServiceAccountList._load(self._get(resource_path).json()["items"])

    def add_service_account(self, id: int, service_account_id: Union[int, List[int]]) -> None:
        """Add one or more service accounts to a group.

        Args:
            id (int): Add service accounts to the group with this id.
            service_account_id (Union[int, List[int]]): Add these service accounts to the specified group.

        Returns:
            None

        Example:

            Add service account to group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.groups.add_service_account(id=1, service_account_id=1)
        """
        resource_path = self._RESOURCE_PATH + "/{}/serviceaccounts".format(id)
        self._create_multiple(cls=ServiceAccountList, resource_path=resource_path, items=service_account_id)

    def remove_service_account(self, id: int, service_account_id: Union[int, List[int]]) -> None:
        """Remove one or more service accounts from a group.

        Args:
            id (int): Remove service accounts from the group with this id.
            service_account_id: Remove these service accounts from the specified group.

        Returns:
            None

        Example:

            Remove service account from group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.groups.remove_service_account(id=1, service_account_id=1)
        """
        url_path = self._RESOURCE_PATH + "/{}/serviceaccounts/remove".format(id)
        all_ids = self._process_ids(service_account_id, None, False)
        self._post(url_path, json={"items": all_ids})


class SecurityCategoriesAPI(APIClient):
    _RESOURCE_PATH = "/securitycategories"
    _LIST_CLASS = SecurityCategoryList

    def list(self, limit: int = 25) -> SecurityCategoryList:
        """List security categories.

        Args:
            limit (int): Max number of security categories to return. Defaults to 25.

        Returns:
            SecurityCategoryList: List of security categories

        Example:

            List security categories::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.security_categories.list()
        """
        return self._list(method="GET", limit=limit)

    def create(
        self, security_category: Union[SecurityCategory, List[SecurityCategory]]
    ) -> Union[SecurityCategory, SecurityCategoryList]:
        """Create one or more security categories.

        Args:
            security_category (Union[SecurityCategory, List[SecurityCategory]]): Security category or list of categories to create.

        Returns:
            Union[SecurityCategory, SecurityCategoryList]: The created security category or categories.

        Example:

            Create security category::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import SecurityCategory
                >>> c = CogniteClient()
                >>> my_category = SecurityCategory(name="My Category")
                >>> res = c.iam.security_categories.create(my_category)
        """
        return self._create_multiple(security_category)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more security categories.

        Args:
            id (Union[int, List[int]]): ID or list of IDs of security categories to delete.

        Returns:
            None

        Example:

            Delete security category::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.security_categories.delete(1)
        """
        self._delete_multiple(ids=id, wrap_ids=False)
