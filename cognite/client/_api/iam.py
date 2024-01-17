from __future__ import annotations

import warnings
from itertools import groupby
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Dict, Sequence, Union, overload

from typing_extensions import TypeAlias

from cognite.client._api.user_profiles import UserProfilesAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
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
from cognite.client.data_classes.capabilities import (
    AllScope,
    Capability,
    LegacyCapability,
    ProjectCapability,
    ProjectCapabilityList,
    RawAcl,
    UnknownAcl,
)
from cognite.client.data_classes.iam import GroupWrite, SecurityCategoryWrite, TokenInspection
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


ComparableCapability: TypeAlias = Union[
    Capability,
    Sequence[Capability],
    Dict[str, Any],
    Sequence[Dict[str, Any]],
    Group,
    GroupList,
    ProjectCapability,
    ProjectCapabilityList,
]


def _convert_capability_to_tuples(capabilities: ComparableCapability, project: str | None = None) -> set[tuple]:
    if isinstance(capabilities, ProjectCapability):
        return ProjectCapabilityList([capabilities]).as_tuples(project)
    if isinstance(capabilities, ProjectCapabilityList):
        return capabilities.as_tuples(project)

    if isinstance(capabilities, (dict, Capability)):
        capabilities = [capabilities]  # type: ignore [assignment]
    elif isinstance(capabilities, Group):
        capabilities = capabilities.capabilities or []
    elif isinstance(capabilities, GroupList):
        capabilities = [cap for grp in capabilities for cap in grp.capabilities or []]
    if isinstance(capabilities, Sequence):
        tpls: set[tuple] = set()
        has_skipped = False
        for cap in capabilities:
            if isinstance(cap, dict):
                cap = Capability.load(cap)
            if isinstance(cap, UnknownAcl):
                warnings.warn(f"Unknown capability {cap.capability_name} will be ignored in comparison")
                has_skipped = True
                continue
            if isinstance(cap, LegacyCapability):
                # Legacy capabilities are no longer in use, so they are safe to skip.
                has_skipped = True
                continue
            tpls.update(cap.as_tuples())  # type: ignore [union-attr]
        if tpls or has_skipped:
            return tpls
        raise ValueError("No capabilities given")
    raise TypeError(
        "input capabilities not understood, expected a ComparableCapability: "
        f"{ComparableCapability} not {type(capabilities)}"
    )


class IAMAPI(APIClient):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.groups = GroupsAPI(config, api_version, cognite_client)
        self.security_categories = SecurityCategoriesAPI(config, api_version, cognite_client)
        self.sessions = SessionsAPI(config, api_version, cognite_client)
        self.user_profiles = UserProfilesAPI(config, api_version, cognite_client)
        # TokenAPI only uses base_url, so we pass `api_version=None`:
        self.token = TokenAPI(config, api_version=None, cognite_client=cognite_client)

    @staticmethod
    def compare_capabilities(
        existing_capabilities: ComparableCapability,
        desired_capabilities: ComparableCapability,
        project: str | None = None,
        ignore_allscope_meaning: bool = False,
    ) -> list[Capability]:
        """Helper method to compare capabilities across two groups (of capabilities) to find which are missing from the first.

        Note:
            Capabilities that are no longer in use by the API will be ignored. These have names prefixed with `Legacy` and
            all inherit from the base class `LegacyCapability`. If you want to check for these, you must do so manually.

        Args:
            existing_capabilities (ComparableCapability): List of existing capabilities.
            desired_capabilities (ComparableCapability): List of wanted capabilities to check against existing.
            project (str | None): If a ProjectCapability or ProjectCapabilityList is passed, we need to know which CDF project
                to pull capabilities from (existing might be from several). If project is not passed, and ProjectCapabilityList
                is used, it will be inferred from the CogniteClient used to call retrieve it via token/inspect.
            ignore_allscope_meaning (bool): Option on how to treat scopes that encompass other scopes, like allScope. When True,
                this function will return e.g. an Acl scoped to a dataset even if the user have the same Acl scoped to all. The
                same logic applies to RawAcl scoped to a specific database->table, even when the user have access to all tables
                in that database. Defaults to False.

        Returns:
            list[Capability]: A flattened list of the missing capabilities, meaning they each have exactly 1 action, 1 scope, 1 id etc.

        Examples:

            Ensure that a user's groups grant access to read- and write for assets in all scope,
            and events write, scoped to a specific dataset with id=123:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.capabilities import AssetsAcl, EventsAcl
                >>> client = CogniteClient()
                >>> my_groups = client.iam.groups.list(all=False)
                >>> to_check = [
                ...     AssetsAcl(
                ...         actions=[AssetsAcl.Action.Read, AssetsAcl.Action.Write],
                ...         scope=AssetsAcl.Scope.All()),
                ...     EventsAcl(
                ...         actions=[EventsAcl.Action.Write],
                ...         scope=EventsAcl.Scope.DataSet([123]),
                ... )]
                >>> missing = client.iam.compare_capabilities(
                ...     existing_capabilities=my_groups,
                ...     desired_capabilities=to_check)
                >>> if missing:
                ...     pass  # do something

            Capabilities can also be passed as dictionaries:

                >>> to_check = [
                ...     {'assetsAcl': {'actions': ['READ', 'WRITE'], 'scope': {'all': {}}}},
                ...     {'eventsAcl': {'actions': ['WRITE'], 'scope': {'datasetScope': {'ids': [123]}}}},
                ... ]
                >>> missing = client.iam.compare_capabilities(
                ...     existing_capabilities=my_groups,
                ...     desired_capabilities=to_check)

        Tip:
            If you just want to check against your existing capabilities, you may use the helper method
            ``client.iam.verify_capabilities`` instead.
        """
        has_capabilties = _convert_capability_to_tuples(existing_capabilities, project)
        to_check = _convert_capability_to_tuples(desired_capabilities, project)
        missing = to_check - has_capabilties

        has_capabilties_lookup = {k: set(grp) for k, grp in groupby(sorted(has_capabilties), key=itemgetter(slice(2)))}
        to_check_lookup = {k: set(grp) for k, grp in groupby(sorted(missing), key=itemgetter(slice(2)))}

        missing.clear()
        raw_group, raw_check_grp = set(), set()
        for key, check_grp in to_check_lookup.items():
            group = has_capabilties_lookup.get(key, set())
            if any(AllScope._scope_name == grp[2] for grp in group):
                continue  # If allScope exists for capability, we safely skip ahead
            elif RawAcl._capability_name == next(iter(check_grp))[0]:
                raw_group.update(group)
                raw_check_grp.update(check_grp)
            else:
                missing.update(check_grp)

        # Special handling of rawAcl which has a "hidden" database scope between "all" and "tables":
        raw_to_check = {k: sorted(grp) for k, grp in groupby(sorted(raw_check_grp), key=itemgetter(slice(4)))}
        raw_has_capabs = {k: sorted(grp) for k, grp in groupby(sorted(raw_group), key=itemgetter(slice(4)))}
        for key, check_db_grp in raw_to_check.items():
            if (db_group := raw_has_capabs.get(key)) and not db_group[0][-1]:
                # [0] because empty string sorts first; [-1] is table; if no table -> db scope -> skip ahead
                continue
            missing.update(check_db_grp)

        return [Capability.from_tuple(tpl) for tpl in missing]

    def verify_capabilities(
        self,
        desired_capabilities: ComparableCapability,
        ignore_allscope_meaning: bool = False,
    ) -> list[Capability]:
        """Helper method to compare your current capabilities with a set of desired capabilities and return any missing.

        Args:
            desired_capabilities (ComparableCapability): List of desired capabilities to check against existing.
            ignore_allscope_meaning (bool): Option on how to treat allScopes. When True, this function will return
                e.g. an Acl scoped to a dataset even if the user have the same Acl scoped to all. Defaults to False.

        Returns:
            list[Capability]: A flattened list of the missing capabilities, meaning they each have exactly 1 action, 1 scope, 1 id etc.

        Examples:

            Ensure that the user's credentials have access to read- and write assets in all scope,
            and write events scoped to a specific dataset with id=123:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.capabilities import AssetsAcl, EventsAcl
                >>> client = CogniteClient()
                >>> to_check = [
                ...     AssetsAcl(
                ...         actions=[AssetsAcl.Action.Read, AssetsAcl.Action.Write],
                ...         scope=AssetsAcl.Scope.All()),
                ...     EventsAcl(
                ...         actions=[EventsAcl.Action.Write],
                ...         scope=EventsAcl.Scope.DataSet([123]),
                ... )]
                >>> if missing := client.iam.verify_capabilities(to_check):
                ...     pass  # do something

            Capabilities can also be passed as dictionaries:

                >>> missing = client.iam.verify_capabilities([
                ...     {'assetsAcl': {'actions': ['READ', 'WRITE'], 'scope': {'all': {}}}},
                ...     {'eventsAcl': {'actions': ['WRITE'], 'scope': {'datasetScope': {'ids': [123]}}}},
                ... ])
        """
        existing_capabilities = self.token.inspect().capabilities
        return self.compare_capabilities(existing_capabilities, desired_capabilities)


class GroupsAPI(APIClient):
    _RESOURCE_PATH = "/groups"

    def list(self, all: bool = False) -> GroupList:
        """`List groups. <https://developer.cognite.com/api#tag/Groups/operation/getGroups>`_

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
        return GroupList.load(res.json()["items"], cognite_client=self._cognite_client)

    @overload
    def create(self, group: Group | GroupWrite) -> Group:
        ...

    @overload
    def create(self, group: Sequence[Group] | Sequence[GroupWrite]) -> GroupList:
        ...

    def create(self, group: Group | GroupWrite | Sequence[Group] | Sequence[GroupWrite]) -> Group | GroupList:
        """`Create one or more groups. <https://developer.cognite.com/api#tag/Groups/operation/createGroups>`_

        Args:
            group (Group | GroupWrite | Sequence[Group] | Sequence[GroupWrite]): Group or list of groups to create.
        Returns:
            Group | GroupList: The created group(s).

        Example:

            Create group::

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import GroupWrite
                >>> from cognite.client.data_classes.capabilities import GroupsAcl
                >>> c = CogniteClient()
                >>> my_capabilities = [GroupsAcl([GroupsAcl.Action.List], GroupsAcl.Scope.All())]
                >>> my_group = GroupWrite(name="My Group", capabilities=my_capabilities)
                >>> res = c.iam.groups.create(my_group)
        """
        return self._create_multiple(list_cls=GroupList, resource_cls=Group, items=group, input_resource_cls=GroupWrite)

    def delete(self, id: int | Sequence[int]) -> None:
        """`Delete one or more groups. <https://developer.cognite.com/api#tag/Groups/operation/deleteGroups>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs of groups to delete.

        Example:

            Delete group::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.iam.groups.delete(1)
        """
        self._delete_multiple(identifiers=IdentifierSequence.load(ids=id), wrap_ids=False)


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
                >>> c = CogniteClient()
                >>> res = c.iam.security_categories.list()
        """
        return self._list(list_cls=SecurityCategoryList, resource_cls=SecurityCategory, method="GET", limit=limit)

    @overload
    def create(self, security_category: SecurityCategory | SecurityCategoryWrite) -> SecurityCategory:
        ...

    @overload
    def create(
        self, security_category: Sequence[SecurityCategory] | Sequence[SecurityCategoryWrite]
    ) -> SecurityCategoryList:
        ...

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
                >>> c = CogniteClient()
                >>> my_category = SecurityCategoryWrite(name="My Category")
                >>> res = c.iam.security_categories.create(my_category)
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

        Example:

            Inspect token::

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.iam.token.inspect()
        """
        return TokenInspection.load(self._get("/api/v1/token/inspect").json(), self._cognite_client)


class SessionsAPI(APIClient):
    _RESOURCE_PATH = "/sessions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._LIST_LIMIT = 100

    def create(self, client_credentials: ClientCredentials | None = None) -> CreatedSession:
        """`Create a session. <https://developer.cognite.com/api#tag/Sessions/operation/createSessions>`_

        Args:
            client_credentials (ClientCredentials | None): The client credentials to create the session. If set to None, a session will be created using the credentials used to instantiate -this- CogniteClient object. If that was done using a token, a session will be created using token exchange. Similarly, if the credentials were client credentials, a session will be created using client credentials. This method does not work when using client certificates (not supported server-side).

        Returns:
            CreatedSession: The object with token inspection details.
        """
        if client_credentials is None and isinstance((creds := self._config.credentials), OAuthClientCredentials):
            client_credentials = ClientCredentials(creds.client_id, creds.client_secret)

        items = {"tokenExchange": True} if client_credentials is None else client_credentials.dump(camel_case=True)
        return CreatedSession.load(self._post(self._RESOURCE_PATH, {"items": [items]}).json()["items"][0])

    def revoke(self, id: int | Sequence[int]) -> SessionList:
        """`Revoke access to a session. Revocation of a session may in some cases take up to 1 hour to take effect. <https://developer.cognite.com/api#tag/Sessions/operation/revokeSessions>`_

        Args:
            id (int | Sequence[int]): Id or list of session ids

        Returns:
            SessionList: List of revoked sessions. If the user does not have the sessionsAcl:LIST capability, then only the session IDs will be present in the response.
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        items = {"items": identifiers.as_dicts()}

        return SessionList.load(self._post(self._RESOURCE_PATH + "/revoke", items).json()["items"])

    def list(self, status: str | None = None) -> SessionList:
        """`List all sessions in the current project. <https://developer.cognite.com/api#tag/Sessions/operation/listSessions>`_

        Args:
            status (str | None): If given, only sessions with the given status are returned.

        Returns:
            SessionList: a list of sessions in the current project.
        """
        filter = {"status": status} if status is not None else None
        return self._list(list_cls=SessionList, resource_cls=Session, method="GET", filter=filter)
