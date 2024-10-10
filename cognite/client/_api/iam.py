from __future__ import annotations

import warnings
from collections.abc import Iterable, Sequence
from itertools import groupby
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

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
    CapabilityTuple,
    DataModelInstancesAcl,
    LegacyCapability,
    ProjectCapability,
    ProjectCapabilityList,
    RawAcl,
    SpaceIDScope,
    UnknownAcl,
)
from cognite.client.data_classes.iam import (
    GroupWrite,
    SecurityCategoryWrite,
    SessionStatus,
    SessionType,
    TokenInspection,
)
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


ComparableCapability: TypeAlias = (
    Capability
    | Sequence[Capability]
    | dict[str, Any]
    | Sequence[dict[str, Any]]
    | Group
    | GroupList
    | ProjectCapability
    | ProjectCapabilityList
)


def _convert_capability_to_tuples(
    capabilities: ComparableCapability, project: str | None = None
) -> set[CapabilityTuple]:
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
        tpls: set[CapabilityTuple] = set()
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

            You may also load capabilities from a dict-representation directly into ACLs (access-control list)
            by using ``Capability.load``. This will also ensure that the capabilities are valid.

                >>> from cognite.client.data_classes.capabilities import Capability
                >>> acls = [Capability.load(cap) for cap in to_check]

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
        raw_group, raw_check_group = set(), set()
        for key, check_group in to_check_lookup.items():
            group = has_capabilties_lookup.get(key, set())
            if any(AllScope._scope_name == tpl.scope_name for tpl in group):
                continue  # If allScope exists for capability, we safely skip ahead

            cap_name, _ = key
            if cap_name == RawAcl._capability_name:
                # rawAcl needs specialized handling (below):
                raw_group.update(group)
                raw_check_group.update(check_group)
            elif key == (DataModelInstancesAcl._capability_name, DataModelInstancesAcl.Action.Write_Properties.value):
                # For dataModelInstancesAcl, 'WRITE_PROPERTIES' may covered by 'WRITE', so we must check AllScope:
                write_grp = has_capabilties_lookup.get((cap_name, DataModelInstancesAcl.Action.Write.value), set())
                if any(AllScope._scope_name == grp.scope_name for grp in write_grp):
                    continue
                # ...and if no AllScope, check individual SpaceIDScope:
                for check_tpl in check_group:
                    to_find = (SpaceIDScope._scope_name, check_tpl.scope_id)
                    if any(to_find == (tpl.scope_name, tpl.scope_id) for tpl in write_grp):
                        continue
                    missing.add(check_tpl)
            else:
                missing.update(check_group)

        # Special handling of rawAcl which has a "hidden" database scope between "all" and "tables":
        raw_to_check = {k: sorted(grp) for k, grp in groupby(sorted(raw_check_group), key=itemgetter(slice(4)))}
        raw_has_capabs = {k: sorted(grp) for k, grp in groupby(sorted(raw_group), key=itemgetter(slice(4)))}
        for key, check_db_grp in raw_to_check.items():
            if (db_group := raw_has_capabs.get(key)) and not db_group[0].table:
                # [0] because empty string sorts first; if no table -> db scope -> skip ahead
                continue
            missing.update(check_db_grp)

        return [Capability.from_tuple(tpl) for tpl in sorted(missing)]

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

                >>> to_check = [
                ...     {'assetsAcl': {'actions': ['READ', 'WRITE'], 'scope': {'all': {}}}},
                ...     {'eventsAcl': {'actions': ['WRITE'], 'scope': {'datasetScope': {'ids': [123]}}}},
                ... ]
                >>> missing = client.iam.verify_capabilities(to_check)

            You may also load capabilities from a dict-representation directly into ACLs (access-control list)
            by using ``Capability.load``. This will also ensure that the capabilities are valid.

                >>> from cognite.client.data_classes.capabilities import Capability
                >>> acls = [Capability.load(cap) for cap in to_check]
        """
        existing_capabilities = self.token.inspect().capabilities
        return self.compare_capabilities(existing_capabilities, desired_capabilities)


class _GroupListAdapter(GroupList):
    @classmethod
    def _load(  # type: ignore[override]
        cls,
        resource_list: Iterable[dict[str, Any]],
        cognite_client: CogniteClient | None = None,
        allow_unknown: bool = False,
    ) -> GroupList:
        return GroupList._load(resource_list, cognite_client=cognite_client, allow_unknown=True)


class _GroupAdapter(Group):
    @classmethod
    def _load(  # type: ignore[override]
        cls,
        resource: dict[str, Any],
        cognite_client: CogniteClient | None = None,
        allow_unknown: bool = False,
    ) -> Group:
        return Group._load(resource, cognite_client=cognite_client, allow_unknown=True)


# We need an adapter for GroupWrite in case the API returns a non 200-status code.
# As, then, in the unwrap_element method, the _create_multiple method will try to load the resource.
# This will fail if the GroupWrite contains an UnknownAcl.
class _GroupWriteAdapter(GroupWrite):
    @classmethod
    def _load(  # type: ignore[override]
        cls,
        resource: dict[str, Any],
        cognite_client: CogniteClient | None = None,
        allow_unknown: bool = False,
    ) -> GroupWrite:
        return GroupWrite._load(resource, cognite_client=cognite_client, allow_unknown=True)


class GroupsAPI(APIClient):
    _RESOURCE_PATH = "/groups"

    def list(self, all: bool = False) -> GroupList:
        """`List groups. <https://developer.cognite.com/api#tag/Groups/operation/getGroups>`_

        Args:
            all (bool): Whether to get all groups, only available with the groups:list acl.

        Returns:
            GroupList: List of groups.

        Example:

            List your own groups:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> my_groups = client.iam.groups.list()

            List all groups:

                >>> all_groups = client.iam.groups.list(all=True)
        """
        res = self._get(self._RESOURCE_PATH, params={"all": all})
        # Dev.note: We don't use public load method here (it is final) and we need to pass a magic keyword arg. to
        # not raise whenever new Acls/actions/scopes are added to the API. So we specifically allow the 'unknown':
        return GroupList._load(res.json()["items"], cognite_client=self._cognite_client, allow_unknown=True)

    @overload
    def create(self, group: Group | GroupWrite) -> Group: ...

    @overload
    def create(self, group: Sequence[Group] | Sequence[GroupWrite]) -> GroupList: ...

    def create(self, group: Group | GroupWrite | Sequence[Group] | Sequence[GroupWrite]) -> Group | GroupList:
        """`Create one or more groups. <https://developer.cognite.com/api#tag/Groups/operation/createGroups>`_

        Args:
            group (Group | GroupWrite | Sequence[Group] | Sequence[GroupWrite]): Group or list of groups to create.
        Returns:
            Group | GroupList: The created group(s).

        Example:

            Create a group without any members:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import GroupWrite
                >>> from cognite.client.data_classes.capabilities import AssetsAcl, EventsAcl
                >>> client = CogniteClient()
                >>> my_capabilities = [
                ...     AssetsAcl([AssetsAcl.Action.Read], AssetsAcl.Scope.All()),
                ...     EventsAcl([EventsAcl.Action.Write], EventsAcl.Scope.DataSet([123, 456]))]
                >>> my_group = GroupWrite(name="My Group", capabilities=my_capabilities)
                >>> res = client.iam.groups.create(my_group)

            Create a group whose members are managed externally (by your company's identity provider (IdP)).
            This is done by using the ``source_id`` field. If this is the same ID as a group in the IdP,
            a user in that group will implicitly be a part of this group as well.

                >>> grp = GroupWrite(
                ...     name="Externally managed group",
                ...     capabilities=my_capabilities,
                ...     source_id="b7c9a5a4...")
                >>> res = client.iam.groups.create(grp)

            Create a group whose members are managed internally by Cognite. This group may grant access through
            listing specific users or include them all. This is done by passing the ``members`` field, either a
            list of strings with the unique user identifiers or as the constant ``ALL_USER_ACCOUNTS``. To find the
            user identifiers, you may use the UserProfilesAPI: ``client.iam.user_profiles.list()``.

                >>> from cognite.client.data_classes import ALL_USER_ACCOUNTS
                >>> all_group = GroupWrite(
                ...     name="Everyone is welcome!",
                ...     capabilities=my_capabilities,
                ...     members=ALL_USER_ACCOUNTS,
                ... )
                >>> user_list_group = GroupWrite(
                ...     name="Specfic users only",
                ...     capabilities=my_capabilities,
                ...     members=["XRsSD1k3mTIKG", "M0SxY6bM9Jl"])
                >>> res = client.iam.groups.create([user_list_group, all_group])

            Capabilities are often defined in configuration files, like YAML or JSON. You may convert capabilities
            from a dict-representation directly into ACLs (access-control list) by using ``Capability.load``.
            This will also ensure that the capabilities are valid.

                >>> from cognite.client.data_classes.capabilities import Capability
                >>> unparsed_capabilities = [
                ...     {'assetsAcl': {'actions': ['READ', 'WRITE'], 'scope': {'all': {}}}},
                ...     {'eventsAcl': {'actions': ['WRITE'], 'scope': {'datasetScope': {'ids': [123]}}}},
                ... ]
                >>> acls = [Capability.load(cap) for cap in unparsed_capabilities]
                >>> group = GroupWrite(name="Another group", capabilities=acls)
        """

        return self._create_multiple(
            list_cls=_GroupListAdapter, resource_cls=_GroupAdapter, items=group, input_resource_cls=_GroupWriteAdapter
        )

    def delete(self, id: int | Sequence[int]) -> None:
        """`Delete one or more groups. <https://developer.cognite.com/api#tag/Groups/operation/deleteGroups>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs of groups to delete.

        Example:

            Delete group::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.iam.groups.delete(1)
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


class TokenAPI(APIClient):
    def inspect(self) -> TokenInspection:
        """Inspect a token.

        Get details about which projects it belongs to and which capabilities are granted to it.

        Returns:
            TokenInspection: The object with token inspection details.

        Example:

            Inspect token::

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.iam.token.inspect()
        """
        # To not raise whenever new Acls/actions/scopes are added to the API, we specifically allow the unknown:
        return TokenInspection.load(self._get("/api/v1/token/inspect").json(), self._cognite_client, allow_unknown=True)


class SessionsAPI(APIClient):
    _RESOURCE_PATH = "/sessions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._LIST_LIMIT = 100

    def create(
        self,
        client_credentials: ClientCredentials | None = None,
        session_type: SessionType | Literal["DEFAULT"] = "DEFAULT",
    ) -> CreatedSession:
        """`Create a session. <https://developer.cognite.com/api#tag/Sessions/operation/createSessions>`_

        Args:
            client_credentials (ClientCredentials | None): The client credentials to create the session. This is required
                if session_type is set to 'CLIENT_CREDENTIALS'.
            session_type (SessionType | Literal['DEFAULT']): The type of session to create. Can be
                either 'CLIENT_CREDENTIALS', 'TOKEN_EXCHANGE', 'ONESHOT_TOKEN_EXCHANGE' or 'DEFAULT'.
                Defaults to 'DEFAULT' which will use -this- CogniteClient object to create the session.
                If this client was created using a token, 'TOKEN_EXCHANGE' will be used, and if
                this client was created using client credentials, 'CLIENT_CREDENTIALS' will be used.

        Session Types:

            * **client_credentials**: Credentials for a session using client credentials from an identity provider.
            * **token_exchange**: Credentials for a session using token exchange to reuse the user's credentials.
            * **one_shot_token_exchange**: Credentials for a session using one-shot token exchange to reuse the user's credentials. One-shot sessions are short-lived sessions that are not refreshed and do not require support for token exchange from the identity provider.

        Returns:
            CreatedSession: The object with token inspection details.
        """
        if client_credentials is None and isinstance(creds := self._config.credentials, OAuthClientCredentials):
            client_credentials = ClientCredentials(creds.client_id, creds.client_secret)

        session_type_up = session_type.upper()
        if session_type_up == "DEFAULT":  # For backwards compatibility after session_type was introduced
            items = {"tokenExchange": True} if client_credentials is None else client_credentials.dump(camel_case=True)

        elif session_type_up == "CLIENT_CREDENTIALS":
            if client_credentials is None:
                raise ValueError(
                    "For session_type='CLIENT_CREDENTIALS', either `client_credentials` must be provided OR "
                    "this client must be using OAuthClientCredentials"
                )
            items = client_credentials.dump(camel_case=True)

        elif session_type_up == "TOKEN_EXCHANGE":
            items = {"tokenExchange": True}

        elif session_type_up == "ONESHOT_TOKEN_EXCHANGE":
            items = {"oneshotTokenExchange": True}
        else:
            raise ValueError(f"Session type not understood: {session_type}")
        return CreatedSession.load(self._post(self._RESOURCE_PATH, {"items": [items]}).json()["items"][0])

    @overload
    def revoke(self, id: int) -> Session: ...

    @overload
    def revoke(self, id: Sequence[int]) -> SessionList: ...

    def revoke(self, id: int | Sequence[int]) -> Session | SessionList:
        """`Revoke access to a session. Revocation of a session may in some cases take up to 1 hour to take effect. <https://developer.cognite.com/api#tag/Sessions/operation/revokeSessions>`_

        Args:
            id (int | Sequence[int]): Id or list of session ids

        Returns:
            Session | SessionList: List of revoked sessions. If the user does not have the sessionsAcl:LIST capability, then only the session IDs will be present in the response.
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        items = {"items": identifiers.as_dicts()}

        result = SessionList._load(self._post(self._RESOURCE_PATH + "/revoke", items).json()["items"])
        return result[0] if isinstance(id, int) else result

    @overload
    def retrieve(self, id: int) -> Session: ...

    @overload
    def retrieve(self, id: Sequence[int]) -> SessionList: ...

    def retrieve(self, id: int | Sequence[int]) -> Session | SessionList:
        """`Retrieves sessions with given IDs. <https://developer.cognite.com/api#tag/Sessions/operation/getSessionsByIds>`_

        The request will fail if any of the IDs does not belong to an existing session.

        Args:
            id (int | Sequence[int]): Id or list of session ids

        Returns:
            Session | SessionList: Session or list of sessions.
        """

        identifiers = IdentifierSequence.load(ids=id, external_ids=None)
        return self._retrieve_multiple(
            list_cls=SessionList,
            resource_cls=Session,
            identifiers=identifiers,
        )

    def list(self, status: SessionStatus | None = None, limit: int = DEFAULT_LIMIT_READ) -> SessionList:
        """`List all sessions in the current project. <https://developer.cognite.com/api#tag/Sessions/operation/listSessions>`_

        Args:
            status (SessionStatus | None): If given, only sessions with the given status are returned.
            limit (int): Max number of sessions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            SessionList: a list of sessions in the current project.
        """
        filter = {"status": status.upper()} if status is not None else None
        return self._list(list_cls=SessionList, resource_cls=Session, method="GET", filter=filter, limit=limit)
