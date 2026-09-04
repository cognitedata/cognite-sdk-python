from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.integrations.actions import Action, ActionList, ActionWrite
from cognite.client.utils._auxiliary import drop_none_values, split_into_chunks
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig

_CREATE_LIMIT = 20
_CANCEL_LIMIT = 100


class IntegrationActionsAPI(APIClient):
    _RESOURCE_PATH = "/integrations/actions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="alpha", sdk_maturity="alpha", feature_name="Integrations")

    @overload
    async def create(self, external_id: str, action: ActionWrite) -> Action: ...

    @overload
    async def create(self, external_id: str, action: Sequence[ActionWrite]) -> ActionList: ...

    async def create(self, external_id: str, action: ActionWrite | Sequence[ActionWrite]) -> Action | ActionList:
        """`Create one or more actions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/create_actions>`_

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
        self._warning.warn()
        single_item = isinstance(action, ActionWrite)
        items: list[ActionWrite] = [action] if isinstance(action, ActionWrite) else list(action)

        created: list[dict[str, Any]] = []
        for chunk in split_into_chunks(items, _CREATE_LIMIT):
            response = await self._post(
                self._RESOURCE_PATH,
                params={"externalId": external_id},
                json={"items": [item.dump(camel_case=True) for item in chunk]},
                headers=self._alpha_version_header(),
                semaphore=self._get_semaphore("write"),
            )
            created.extend(response.json()["items"])

        if single_item:
            return Action._load(created[0])
        return ActionList._load(created)

    async def list(
        self,
        external_id: str | None = None,
        created_after: int | None = None,
        include_completed: bool = True,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> ActionList:
        """`List actions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/list_actions>`_

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
        self._warning.warn()
        return await self._list(
            method="GET",
            list_cls=ActionList,
            resource_cls=Action,
            limit=limit,
            filter=drop_none_values(
                {
                    "externalId": external_id,
                    "createdAfter": created_after,
                    "includeCompleted": include_completed,
                }
            ),
            headers=self._alpha_version_header(),
        )

    @overload
    async def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> Action | None: ...

    @overload
    async def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> ActionList: ...

    async def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Action | ActionList | None:
        """`Retrieve one or more actions by external id <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/retrieve_actions>`_

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
        self._warning.warn()
        identifiers = IdentifierSequence.load(external_ids=external_id)
        return await self._retrieve_multiple(
            list_cls=ActionList,
            resource_cls=Action,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            headers=self._alpha_version_header(),
        )

    async def cancel(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> ActionList:
        """`Cancel one or more actions <https://api-docs.cognite.com/20230101-alpha/tag/Integration-Actions/operation/cancel_actions>`_

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
        self._warning.warn()
        identifiers = IdentifierSequence.load(external_ids=external_id)

        cancelled: list[dict[str, Any]] = []
        for chunk in identifiers.chunked(_CANCEL_LIMIT):
            body: dict[str, Any] = {"items": chunk.as_dicts()}
            if ignore_unknown_ids:
                body["ignoreUnknownIds"] = True
            response = await self._post(
                f"{self._RESOURCE_PATH}/cancel",
                json=body,
                headers=self._alpha_version_header(),
                semaphore=self._get_semaphore("write"),
            )
            cancelled.extend(response.json()["items"])

        return ActionList._load(cancelled)
