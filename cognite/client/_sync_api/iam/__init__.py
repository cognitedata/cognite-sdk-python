"""
===============================================================================
4757d559b846e5066a596fd5502a1c79
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._api.iam import ComparableCapability
from cognite.client._sync_api.iam.groups import SyncGroupsAPI
from cognite.client._sync_api.iam.security_categories import SyncSecurityCategoriesAPI
from cognite.client._sync_api.iam.sessions import SyncSessionsAPI
from cognite.client._sync_api.iam.token import SyncTokenAPI
from cognite.client._sync_api.org_apis.principals import SyncPrincipalsAPI
from cognite.client._sync_api.user_profiles import SyncUserProfilesAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.capabilities import (
    Capability,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncIAMAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient):
        self.__async_client = async_client
        self.groups = SyncGroupsAPI(async_client)
        self.security_categories = SyncSecurityCategoriesAPI(async_client)
        self.sessions = SyncSessionsAPI(async_client)
        self.user_profiles = SyncUserProfilesAPI(async_client)
        self.principals = SyncPrincipalsAPI(async_client)
        self.token = SyncTokenAPI(async_client)

    def verify_capabilities(self, desired_capabilities: ComparableCapability) -> list[Capability]:
        """
        Helper method to compare your current capabilities with a set of desired capabilities and return any missing.

        Args:
            desired_capabilities (ComparableCapability): List of desired capabilities to check against existing.

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
        return run_sync(self.__async_client.iam.verify_capabilities(desired_capabilities=desired_capabilities))
