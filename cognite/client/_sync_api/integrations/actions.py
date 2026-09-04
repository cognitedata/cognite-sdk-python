"""
===============================================================================
3f635530474c2e7d60de287a95002ad1
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.integrations.actions import Action, ActionList, ActionWrite
from cognite.client.utils._async_helpers import run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient

_CREATE_LIMIT = 20
_CANCEL_LIMIT = 100


class SyncIntegrationActionsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def create(self, external_id: str, action: ActionWrite) -> Action: ...

    @overload
    def create(self, external_id: str, action: Sequence[ActionWrite]) -> ActionList: ...

    def create(self, external_id: str, action: ActionWrite | Sequence[ActionWrite]) -> Action | ActionList:
        """
        `Create one or more actions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/create_actions>`_

        Args:
            external_id (str): External id of the integration to trigger the action(s) against.
            action (ActionWrite | Sequence[ActionWrite]): Action or list of actions to create.

        Returns:
            Action | ActionList: Created action(s)

        Examples:

            Trigger an action against an integration:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.integrations import ActionWrite
                >>> client = CogniteClient()
                >>> action = ActionWrite(external_id="my-restart-action", action_name="restart")
                >>> res = client.integrations.actions.create("my-integration", action)
        """
        return run_sync(self.__async_client.integrations.actions.create(external_id=external_id, action=action))

    def list(
        self,
        external_id: str | None = None,
        created_after: int | None = None,
        include_completed: bool = True,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ActionList:
        """
        `List actions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/list_actions>`_

        Args:
            external_id (str | None): Only return actions for the integration with this external id. If not given, actions across all integrations you have access to are returned.
            created_after (int | None): Only return actions created at or after this time, in milliseconds since epoch.
            include_completed (bool): Whether to include actions in a terminal state (succeeded, failed, canceled). If False, only pending/running/cancel_pending actions are returned.
            limit (int | None): Maximum number of actions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            ActionList: List of actions

        Examples:

            List pending actions for an integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.actions.list(
                ...     external_id="my-integration", include_completed=False
                ... )
        """
        return run_sync(
            self.__async_client.integrations.actions.list(
                external_id=external_id, created_after=created_after, include_completed=include_completed, limit=limit
            )
        )

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> Action | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> ActionList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Action | ActionList | None:
        """
        `Retrieve one or more actions by external id <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/retrieve_actions>`_

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids of actions to retrieve.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Returns:
            Action | ActionList | None: Requested action(s), or None if a single requested external id is not found.

        Examples:

            Retrieve an action by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.actions.retrieve("my-restart-action")
        """
        return run_sync(
            self.__async_client.integrations.actions.retrieve(
                external_id=external_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def cancel(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> ActionList:
        """
        `Cancel one or more actions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/cancel_actions>`_

        Only actions in the `pending`, `running` or `cancel_pending` state can be cancelled.

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids of actions to cancel.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Returns:
            ActionList: The action(s), with their updated status.

        Examples:

            Cancel a pending action:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.actions.cancel("my-restart-action")
        """
        return run_sync(
            self.__async_client.integrations.actions.cancel(
                external_id=external_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )
