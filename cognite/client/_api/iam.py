from typing import *

from cognite.client._api_client import APIClient
from cognite.client._base import *


class IAMAPI(APIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.service_accounts = ServiceAccountsAPI(*args, **kwargs)
        self.api_keys = APIKeysAPI(*args, **kwargs)
        self.groups = GroupsAPI(*args, **kwargs)
        self.security_categories = SecurityCategoriesAPI(*args, **kwargs)


# GenClass: ServiceAccount
class ServiceAccount(CogniteResource):
    """No description.

    Args:
        unique_name (str): Unique name of the service account
        groups (List[int]): List of group ids
        id (int): No description.
        is_deleted (bool): If this service account has been logically deleted
        deleted_time (int): Time of deletion
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        unique_name: str = None,
        groups: List[int] = None,
        id: int = None,
        is_deleted: bool = None,
        deleted_time: int = None,
        cognite_client=None,
    ):
        self.unique_name = unique_name
        self.groups = groups
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cognite_client

    # GenStop


class ServiceAccountList(CogniteResourceList):
    _RESOURCE = ServiceAccount
    _ASSERT_CLASSES = False


class ServiceAccountsAPI(APIClient):
    _RESOURCE_PATH = "/serviceaccounts"

    def list(self) -> ServiceAccountList:
        """List service accounts.

        Returns:
            ServiceAccountList: List of service accounts.
        """
        return self._list(ServiceAccountList, self._RESOURCE_PATH, method="GET")

    def create(
        self, service_account: Union[ServiceAccount, List[ServiceAccount]]
    ) -> Union[ServiceAccount, ServiceAccountList]:
        """Create one or more new service accounts.

        Args:
            service_account (Union[ServiceAccount, List[ServiceAccount]]): The service account(s) to create.

        Returns:
            Union[ServiceAccount, List[ServiceAccount]]: The created service account(s).
        """
        return self._create_multiple(ServiceAccountList, self._RESOURCE_PATH, items=service_account)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more service accounts.

        Args:
            id (Union[int, List[int]]): ID or list of IDs to delete.

        Returns:
            None
        """
        self._delete_multiple(self._RESOURCE_PATH, ids=id, wrap_ids=False)


# GenClass: NewApiKeyResponseDTO
class APIKey(CogniteResource):
    """No description.

    Args:
        id (int): Internal id for the api key
        service_account_id (int): id of the service account
        created_time (int): Time of creating in unix ms
        status (str): The status of the api key.
        value (str): The api key to be used against the API
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        id: int = None,
        service_account_id: int = None,
        created_time: int = None,
        status: str = None,
        value: str = None,
        cognite_client=None,
    ):
        self.id = id
        self.service_account_id = service_account_id
        self.created_time = created_time
        self.status = status
        self.value = value
        self._cognite_client = cognite_client

    # GenStop


class APIKeyList(CogniteResourceList):
    _RESOURCE = APIKey
    _ASSERT_CLASSES = False


class APIKeysAPI(APIClient):
    _RESOURCE_PATH = "/apikeys"

    def list(self, include_deleted: bool = False, all: bool = False, service_account_id: bool = None) -> APIKeyList:
        """List api keys.

        Args:
            include_deleted (bool): Whether or not to include deleted api keys. Defaults to False.
            all (bool): Whether or not to return all api keys for this project. Requires users:list acl. Defaults to False.
            service_account_id (int): Get api keys for this service account only. Only available to admin users.

        Returns:
            APIKeyList: List of api keys.
        """
        res = self._get(
            self._RESOURCE_PATH,
            params={"all": all, "serviceAccountId": service_account_id, "includeDeleted": include_deleted},
        )
        return APIKeyList._load(res.json()["data"]["items"])

    def create(self, service_account_id: Union[int, List[int]]) -> Union[APIKey, APIKeyList]:
        """Create a new api key for one or more service accounts.

        Args:
            service_account_id (Union[int, List[int]]): ID or list of IDs of service accounts to create an api key for.

        Returns:
            Union[APIKey, APIKeyList]: API key or list of api keys.
        """
        return self._create_multiple(APIKeyList, self._RESOURCE_PATH, items=service_account_id)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more api keys.

        Args:
            id (Union[int, List[int]]): ID or list of IDs of api keys to delete.

        Returns:
            None
        """
        self._delete_multiple(self._RESOURCE_PATH, ids=id, wrap_ids=False)


# GenClass: Group
class Group(CogniteResource):
    """No description.

    Args:
        name (str): Name of the group
        source_id (str): ID of the group in the source. If this is the same ID as a group in the IDP, a user in that group will implicitly be a part of this group as well.
        capabilities (List[Dict[str, Any]]): No description.
        id (int): No description.
        is_deleted (bool): No description.
        deleted_time (int): No description.
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(
        self,
        name: str = None,
        source_id: str = None,
        capabilities: List[Dict[str, Any]] = None,
        id: int = None,
        is_deleted: bool = None,
        deleted_time: int = None,
        cognite_client=None,
    ):
        self.name = name
        self.source_id = source_id
        self.capabilities = capabilities
        self.id = id
        self.is_deleted = is_deleted
        self.deleted_time = deleted_time
        self._cognite_client = cognite_client

    # GenStop


class GroupList(CogniteResourceList):
    _RESOURCE = Group
    _ASSERT_CLASSES = False


class GroupsAPI(APIClient):
    _RESOURCE_PATH = "/groups"

    def list(self, all: bool = False) -> GroupList:
        """List groups.

        Args:
            all (bool): Whether to get all groups, only available with the groups:list acl.

        Returns:
            GroupList: List of groups.
        """
        res = self._get(self._RESOURCE_PATH, params={"all": all})
        return GroupList._load(res.json()["data"]["items"])

    def create(self, group: Union[Group, List[Group]]) -> Union[Group, GroupList]:
        """Create one or more groups.

        Args:
            group (Union[Group, List[Group]]): Group or list of groups to create.
        Returns:
            Union[Group, GroupList]: The created group(s).
        """
        return self._create_multiple(GroupList, self._RESOURCE_PATH, group)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more groups.

        Args:
            id (Union[int, List[int]]): ID or list of IDs of groups to delete.

        Returns:
            None
        """
        self._delete_multiple(self._RESOURCE_PATH, ids=id, wrap_ids=False)

    def list_service_accounts(self, id: int) -> ServiceAccountList:
        """List service accounts in a group.

        Args:
            id (int): List service accounts which are a member of this group.

        Returns:
            ServiceAccountList: List of service accounts.
        """
        resource_path = self._RESOURCE_PATH + "/{}/serviceaccounts".format(id)
        return ServiceAccountList._load(self._get(resource_path).json()["data"]["items"])

    def add_service_account(self, id: int, service_account_id: Union[int, List[int]]) -> None:
        """Add one or more service accounts to a group.

        Args:
            id (int): Add service accounts to the group with this id.
            service_account_id (Union[int, List[int]]): Add these service accounts to the specified group.

        Returns:
            None
        """
        resource_path = self._RESOURCE_PATH + "/{}/serviceaccounts".format(id)
        self._create_multiple(ServiceAccountList, resource_path, items=service_account_id)

    def remove_service_account(self, id: int, service_account_id: Union[int, List[int]]) -> None:
        """Remove one or more service accounts from a group.

        Args:
            id (int): Remove service accounts from the group with this id.
            service_account_id: Remove these service accounts from the specified group.

        Returns:
            None
        """
        url_path = self._RESOURCE_PATH + "/{}/serviceaccounts/remove".format(id)
        all_ids = self._process_ids(service_account_id, None, False)
        self._post(url_path, json={"items": all_ids})


# GenClass: SecurityCategoryDTO
class SecurityCategory(CogniteResource):
    """No description.

    Args:
        name (str): Name of the security category
        id (int): Id of the security category
        cognite_client (CogniteClient): The client to associate with this object.
    """

    def __init__(self, name: str = None, id: int = None, cognite_client=None):
        self.name = name
        self.id = id
        self._cognite_client = cognite_client

    # GenStop


class SecurityCategoryList(CogniteResourceList):
    _RESOURCE = SecurityCategory
    _ASSERT_CLASSES = False


class SecurityCategoriesAPI(APIClient):
    _RESOURCE_PATH = "/securitycategories"

    def list(self, limit: int = None) -> SecurityCategoryList:
        """List security categories.

        Args:
            limit (int): Max number of security categories to return.

        Returns:
            SecurityCategoryList: List of security categories
        """
        return self._list(SecurityCategoryList, self._RESOURCE_PATH, method="GET", limit=limit)

    def create(
        self, security_category: Union[SecurityCategory, List[SecurityCategory]]
    ) -> Union[SecurityCategory, SecurityCategoryList]:
        """Create one or more security categories.

        Args:
            group (Union[SecurityCategory, List[SecurityCategory]]): Security category or list of categories to create.

        Returns:
            Union[SecurityCategory, SecurityCategoryList]: The created security category or categories.
        """
        return self._create_multiple(SecurityCategoryList, self._RESOURCE_PATH, security_category)

    def delete(self, id: Union[int, List[int]]) -> None:
        """Delete one or more security categories.

        Args:
            id (Union[int, List[int]]): ID or list of IDs of security categories to delete.

        Returns:
            None
        """
        self._delete_multiple(self._RESOURCE_PATH, ids=id, wrap_ids=False)
