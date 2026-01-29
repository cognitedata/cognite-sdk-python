"""
===============================================================================
f58575ab692dc7501138d7afa895c59c
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes import Group, GroupList
from cognite.client.data_classes.iam import GroupWrite
from cognite.client.utils._async_helpers import run_sync


class SyncGroupsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def list(self, all: bool = False) -> GroupList:
        """
        `List groups. <https://developer.cognite.com/api#tag/Groups/operation/getGroups>`_

        Args:
            all (bool): Whether to get all groups, only available with the groups:list acl.

        Returns:
            GroupList: List of groups.

        Example:

            List your own groups:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> my_groups = client.iam.groups.list()

            List all groups:

                >>> all_groups = client.iam.groups.list(all=True)
        """
        return run_sync(self.__async_client.iam.groups.list(all=all))

    @overload
    def create(self, group: Group | GroupWrite) -> Group: ...

    @overload
    def create(self, group: Sequence[Group] | Sequence[GroupWrite]) -> GroupList: ...

    def create(self, group: Group | GroupWrite | Sequence[Group] | Sequence[GroupWrite]) -> Group | GroupList:
        """
        `Create one or more groups. <https://developer.cognite.com/api#tag/Groups/operation/createGroups>`_

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
                >>> # async_client = AsyncCogniteClient()  # another option
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
        return run_sync(self.__async_client.iam.groups.create(group=group))

    def delete(self, id: int | Sequence[int]) -> None:
        """
        `Delete one or more groups. <https://developer.cognite.com/api#tag/Groups/operation/deleteGroups>`_

        Args:
            id (int | Sequence[int]): ID or list of IDs of groups to delete.

        Example:

            Delete group::

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.iam.groups.delete(1)
        """
        return run_sync(self.__async_client.iam.groups.delete(id=id))
