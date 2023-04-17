from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Union

from cognite.client._api_client import APIClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    ClientCredentials,
    CreatedSession,
    Group,
    GroupList,
    SecurityCategory,
    SecurityCategoryList,
    Session,
    SessionList,
)
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class IAMAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.groups = GroupsAPI(config, api_version, cognite_client)
        self.security_categories = SecurityCategoriesAPI(config, api_version, cognite_client)
        self.sessions = SessionsAPI(config, api_version, cognite_client)
        # TokenAPI only uses base_url, so we pass `api_version=None`:
        self.token = TokenAPI(config, api_version=None, cognite_client=cognite_client)


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


class SecurityCategoriesAPI(APIClient):
    _RESOURCE_PATH = "/securitycategories"

    def list(self, limit: int = LIST_LIMIT_DEFAULT) -> SecurityCategoryList:
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

    def __init__(self, config: ClientConfig, api_version: Optional[str], cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._LIST_LIMIT = 100

    def create(self, client_credentials: Optional[ClientCredentials] = None) -> CreatedSession:
        """`Create a session. <https://docs.cognite.com/api/v1/#operation/createSessions>`_

        Args:
            client_credentials (Optional[ClientCredentials]): The client credentials to create the session. If set to None,
                a session will be created using the credentials used to instantiate -this- CogniteClient object. If that
                was done using a token, a session will be created using token exchange. Similarly, if the credentials were
                client credentials, a session will be created using client credentials. This method does not work when
                using client certificates (not supported server-side).

        Returns:
            CreatedSession: The object with token inspection details.
        """
        if client_credentials is None and isinstance((creds := self._config.credentials), OAuthClientCredentials):
            client_credentials = ClientCredentials(creds.client_id, creds.client_secret)

        items = {"tokenExchange": True} if client_credentials is None else client_credentials.dump(camel_case=True)
        return CreatedSession._load(self._post(self._RESOURCE_PATH, {"items": [items]}).json()["items"][0])

    def revoke(self, id: Union[int, Sequence[int]]) -> SessionList:
        """`Revoke access to a session. Revocation of a session may in some cases take up to 1 hour to take effect. <https://docs.cognite.com/api/v1/#operation/revokeSessions>`_

        Args:
            id (Union[int, Sequence[int]): Id or list of session ids

        Returns:
            SessionList: List of revoked sessions. If the user does not have the sessionsAcl:LIST capability, then only the session IDs will be present in the response.
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        items = {"items": identifiers.as_dicts()}

        return self._LIST_CLASS._load(self._post(self._RESOURCE_PATH + "/revoke", items).json()["items"])

    def list(self, status: Optional[str] = None) -> SessionList:
        """`List all sessions in the current project. <https://docs.cognite.com/api/v1/#operation/listSessions>`_

        Args:
            status (Optional[str]): If given, only sessions with the given status are returned.

        Returns:
            SessionList: a list of sessions in the current project.
        """
        filter = {"status": status} if status else None
        return self._list(list_cls=self._LIST_CLASS, resource_cls=Session, method="GET", filter=filter)
