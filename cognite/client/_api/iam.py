import numbers
from typing import TYPE_CHECKING, Optional, Sequence, Union

from cognite.client import utils
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    APIKey,
    APIKeyList,
    ClientCredentials,
    CreatedSession,
    CreatedSessionList,
    Group,
    GroupList,
    SecurityCategory,
    SecurityCategoryList,
    ServiceAccount,
    ServiceAccountList,
    Session,
    SessionList,
)
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class IAMAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str = None, cognite_client: "CogniteClient" = None) -> None:
        super().__init__(config, api_version=api_version, cognite_client=cognite_client)
        self.service_accounts = ServiceAccountsAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.api_keys = APIKeysAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.groups = GroupsAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.security_categories = SecurityCategoriesAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.sessions = SessionsAPI(config, api_version=api_version, cognite_client=cognite_client)
        self.token = TokenAPI(config, cognite_client=cognite_client)


class ServiceAccountsAPI(APIClient):
    _RESOURCE_PATH = "/serviceaccounts"

    def list(self) -> ServiceAccountList:
        """`List service accounts. <https://docs.cognite.com/api/v1/#operation/getServiceAccounts>`_

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
        self, service_account: Union[ServiceAccount, Sequence[ServiceAccount]]
    ) -> Union[ServiceAccount, ServiceAccountList]:
        """`Create one or more new service accounts. <https://docs.cognite.com/api/v1/#operation/createServiceAccounts>`_

        Args:
            service_account (Union[ServiceAccount, Sequence[ServiceAccount]]): The service account(s) to create.

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
        return self._create_multiple(list_cls=ServiceAccountList, resource_cls=ServiceAccount, items=service_account)

    def delete(self, id: Union[int, Sequence[int]]) -> None:
        """`Delete one or more service accounts. <https://docs.cognite.com/api/v1/#operation/deleteServiceAccounts>`_

        Args:
            id (Union[int, Sequence[int]]): ID or list of IDs to delete.

        Returns:
            None

        Example:

            Delete service account by id::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.iam.service_accounts.delete(1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


class APIKeysAPI(APIClient):
    _RESOURCE_PATH = "/apikeys"

    def list(self, include_deleted: bool = False, all: bool = False, service_account_id: bool = None) -> APIKeyList:
        """`List api keys. <https://docs.cognite.com/api/v1/#operation/getApiKeys>`_

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

    def create(self, service_account_id: Union[int, Sequence[int]]) -> Union[APIKey, APIKeyList]:
        """`Create a new api key for one or more service accounts. <https://docs.cognite.com/api/v1/#operation/createApiKeys>`_

        Args:
            service_account_id (Union[int, Sequence[int]]): ID or list of IDs of service accounts to create an api key for.

        Returns:
            Union[APIKey, APIKeyList]: API key or list of api keys.

        Example:

            Create new api key for a given service account::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.api_keys.create(1)
        """
        if isinstance(service_account_id, numbers.Integral):
            items: Union[list, dict] = {"serviceAccountId": service_account_id}
        elif isinstance(service_account_id, Sequence):
            items = [{"serviceAccountId": sa_id} for sa_id in service_account_id]
        else:
            raise TypeError("service_account_id must be of type int or Sequence[int]")
        return self._create_multiple(list_cls=APIKeyList, resource_cls=APIKey, items=items)

    def delete(self, id: Union[int, Sequence[int]]) -> None:
        """`Delete one or more api keys. <https://docs.cognite.com/api/v1/#operation/deleteApiKeys>`_

        Args:
            id (Union[int, Sequence[int]]): ID or list of IDs of api keys to delete.

        Returns:
            None

        Example:

            Delete api key for a given service account::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.iam.api_keys.delete(1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


class GroupsAPI(APIClient):
    _RESOURCE_PATH = "/groups"

    def list(self, all: bool = False) -> GroupList:
        """`List groups. <https://docs.cognite.com/api/v1/#operation/getGroups>`_

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

    def create(self, group: Union[Group, Sequence[Group]]) -> Union[Group, GroupList]:
        """`Create one or more groups. <https://docs.cognite.com/api/v1/#operation/createGroups>`_

        Args:
            group (Union[Group, Sequence[Group]]): Group or list of groups to create.
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
        return self._create_multiple(list_cls=GroupList, resource_cls=Group, items=group)

    def delete(self, id: Union[int, Sequence[int]]) -> None:
        """`Delete one or more groups. <https://docs.cognite.com/api/v1/#operation/deleteGroups>`_

        Args:
            id (Union[int, Sequence[int]]): ID or list of IDs of groups to delete.

        Returns:
            None

        Example:

            Delete group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.iam.groups.delete(1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)

    def list_service_accounts(self, id: int) -> ServiceAccountList:
        """`List service accounts in a group. <https://docs.cognite.com/api/v1/#operation/getMembersOfGroups>`_

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
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/serviceaccounts", id)
        return ServiceAccountList._load(self._get(resource_path).json()["items"])

    def add_service_account(self, id: int, service_account_id: Union[int, Sequence[int]]) -> None:
        """`Add one or more service accounts to a group. <https://docs.cognite.com/api/v1/#operation/addServiceAccountsToGroup>`_

        Args:
            id (int): Add service accounts to the group with this id.
            service_account_id (Union[int, Sequence[int]]): Add these service accounts to the specified group.

        Returns:
            None

        Example:

            Add service account to group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.groups.add_service_account(id=1, service_account_id=1)
        """
        resource_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/serviceaccounts", id)

        all_ids = IdentifierSequence.load(ids=service_account_id, external_ids=None).as_primitives()
        self._post(resource_path, json={"items": all_ids})

    def remove_service_account(self, id: int, service_account_id: Union[int, Sequence[int]]) -> None:
        """`Remove one or more service accounts from a group. <https://docs.cognite.com/api/v1/#operation/removeServiceAccountsFromGroup>`_

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
        url_path = utils._auxiliary.interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/serviceaccounts/remove", id)

        all_ids = IdentifierSequence.load(ids=service_account_id).as_primitives()
        self._post(url_path, json={"items": all_ids})


class SecurityCategoriesAPI(APIClient):
    _RESOURCE_PATH = "/securitycategories"

    def list(self, limit: int = 25) -> SecurityCategoryList:
        """`List security categories. <https://docs.cognite.com/api/v1/#operation/getSecurityCategories>`_

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
        return self._list(list_cls=SecurityCategoryList, resource_cls=SecurityCategory, method="GET", limit=limit)

    def create(
        self, security_category: Union[SecurityCategory, Sequence[SecurityCategory]]
    ) -> Union[SecurityCategory, SecurityCategoryList]:
        """`Create one or more security categories. <https://docs.cognite.com/api/v1/#operation/createSecurityCategories>`_

        Args:
            security_category (Union[SecurityCategory, Sequence[SecurityCategory]]): Security category or list of categories to create.

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
        return self._create_multiple(
            list_cls=SecurityCategoryList, resource_cls=SecurityCategory, items=security_category
        )

    def delete(self, id: Union[int, Sequence[int]]) -> None:
        """`Delete one or more security categories. <https://docs.cognite.com/api/v1/#operation/deleteSecurityCategories>`_

        Args:
            id (Union[int, Sequence[int]]): ID or list of IDs of security categories to delete.

        Returns:
            None

        Example:

            Delete security category::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.iam.security_categories.delete(1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


class TokenAPI(APIClient):
    def inspect(self) -> TokenInspection:
        """Inspect a token.

        Get details about which projects it belongs to and which capabilities are granted to it.

        Returns:
            TokenInspection: The object with token inspection details.
        """
        return TokenInspection._load(self._get("/api/v1/token/inspect").json())


class SessionsAPI(APIClient):
    _LIST_CLASS = SessionList
    _RESOURCE_PATH = "/sessions"

    def __init__(self, config: ClientConfig, api_version: str = None, cognite_client: "CogniteClient" = None) -> None:
        super().__init__(config, api_version=api_version, cognite_client=cognite_client)
        self._LIST_LIMIT = 100

    def create(self, client_credentials: Optional[ClientCredentials] = None) -> CreatedSession:
        """`Create a session. <https://docs.cognite.com/api/v1/#operation/createSessions>`_

        Args:
            client_credentials (Optional[ClientCredentials]): client credentials to create the session, set to None to create session with token exchange.

        Returns:
            CreatedSession: The object with token inspection details.
        """
        if client_credentials is None and isinstance(self._config.credentials, OAuthClientCredentials):
            client_credentials = ClientCredentials(
                self._config.credentials.client_id, self._config.credentials.client_secret
            )

        json = {"items": [client_credentials.dump(True) if client_credentials else {"tokenExchange": True}]}

        return CreatedSessionList._load(self._post(self._RESOURCE_PATH, json).json()["items"])[0]

    def revoke(self, id: Union[int, Sequence[int]]) -> SessionList:
        """`Revoke access to a session. Revocation of a session may in some cases take up to 1 hour to take effect. <https://docs.cognite.com/api/v1/#operation/revokeSessions>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of session ids

        Returns:
            List of revoked sessions. If the user does not have the sessionsAcl:LIST  capability,
            then only the session IDs will be present in the response.
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        items = {"items": identifiers.as_dicts()}

        return SessionList._load(self._post(self._RESOURCE_PATH + "/revoke", items).json()["items"])

    def list(self, status: Optional[str] = None) -> SessionList:
        """`List all sessions in the current project. <https://docs.cognite.com/api/v1/#operation/listSessions>`_

        Args:
            status (Optional[str]): If given, only sessions with the given status are returned.

        Returns:
            SessionList: a list of sessions in the current project.
        """
        filter = {"status": status} if status else None
        return self._list(list_cls=SessionList, resource_cls=Session, method="GET", filter=filter)
