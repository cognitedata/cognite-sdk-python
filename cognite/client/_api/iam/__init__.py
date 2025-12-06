from __future__ import annotations

import warnings
from collections.abc import Sequence
from itertools import groupby
from operator import itemgetter
from typing import TYPE_CHECKING, Any, TypeAlias

from cognite.client._api.iam.groups import GroupsAPI
from cognite.client._api.iam.security_categories import SecurityCategoriesAPI
from cognite.client._api.iam.sessions import SessionsAPI
from cognite.client._api.iam.token import TokenAPI
from cognite.client._api.org_apis.principals import PrincipalsAPI
from cognite.client._api.user_profiles import UserProfilesAPI
from cognite.client._api_client import APIClient
from cognite.client.config import ClientConfig
from cognite.client.data_classes import Group, GroupList
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
        self.principals = PrincipalsAPI(config, api_version, cognite_client)
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
