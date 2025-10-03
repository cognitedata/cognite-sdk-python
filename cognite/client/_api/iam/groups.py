from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client.data_classes import Group, GroupList
from cognite.client.data_classes.iam import GroupWrite
from cognite.client.utils._identifier import IdentifierSequence

if TYPE_CHECKING:
    from cognite.client import CogniteClient


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
