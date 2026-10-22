from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client._api.integrations.errors import IntegrationErrorsAPI
from cognite.client._api.integrations.history import IntegrationHistoryAPI
from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.integrations.integrations import (
    Integration,
    IntegrationList,
    IntegrationUpdate,
    IntegrationWrite,
)
from cognite.client.data_classes.integrations.tasks import SyncResult
from cognite.client.utils._auxiliary import drop_none_values
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient
    from cognite.client.config import ClientConfig


class IntegrationsAPI(APIClient):
    _RESOURCE_PATH = "/integrations"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: AsyncCogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.history = IntegrationHistoryAPI(config, api_version, cognite_client)
        self.errors = IntegrationErrorsAPI(config, api_version, cognite_client)
        self._warning = FeaturePreviewWarning(api_maturity="beta", sdk_maturity="alpha", feature_name="Integrations")
        self._CREATE_LIMIT = 20
        self._UPDATE_LIMIT = 20
        self._RETRIEVE_LIMIT = 100
        self._DELETE_LIMIT = 100
        self._LIST_LIMIT = 100

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> AsyncIterator[Integration]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> AsyncIterator[IntegrationList]: ...

    async def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> AsyncIterator[Integration] | AsyncIterator[IntegrationList]:
        """Iterate over integrations

        Fetches integrations as they are iterated over, so you keep a limited number of integrations in memory.

        Args:
            chunk_size (int | None): Number of integrations to return in each chunk. Defaults to yielding one integration at a time.
            limit (int | None): Maximum number of integrations to return. Defaults to return all items.

        Yields:
            Integration | IntegrationList: yields Integration one by one if chunk_size is not specified, else IntegrationList objects.
        """  # noqa: DOC404
        self._warning.warn()
        async for item in self._list_generator(
            method="GET",
            list_cls=IntegrationList,
            resource_cls=Integration,
            chunk_size=chunk_size,
            limit=limit,
            headers=self._beta_version_header(),
        ):
            yield item

    async def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> IntegrationList:
        """`List integrations <https://api-docs.cognite.com/20230101-beta/tag/Integrations/operation/list_integrations>`_

        Args:
            limit (int | None): Maximum number of integrations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            IntegrationList: List of integrations

        Examples:

            List integrations:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.list(limit=10)

            Iterate over integrations, one-by-one:

                >>> for integration in client.integrations():
                ...     integration  # do something with the integration
        """
        self._warning.warn()
        return await self._list(
            method="GET",
            list_cls=IntegrationList,
            resource_cls=Integration,
            limit=limit,
            headers=self._beta_version_header(),
        )

    @overload
    async def create(self, integration: IntegrationWrite) -> Integration: ...

    @overload
    async def create(self, integration: Sequence[IntegrationWrite]) -> IntegrationList: ...

    async def create(self, integration: IntegrationWrite | Sequence[IntegrationWrite]) -> Integration | IntegrationList:
        """`Create one or more integrations <https://api-docs.cognite.com/20230101-beta/tag/Integrations/operation/create_integrations>`_

        Args:
            integration (IntegrationWrite | Sequence[IntegrationWrite]): Integration or list of integrations to create.

        Returns:
            Integration | IntegrationList: Created integration(s)

        Examples:

            Create a new integration:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.integrations import Extractor, IntegrationWrite
                >>> client = CogniteClient()
                >>> integration = IntegrationWrite(
                ...     external_id="my-integration",
                ...     extractor=Extractor(external_id="cognite-simple-influxdb-extractor"),
                ... )
                >>> res = client.integrations.create(integration)
        """
        self._warning.warn()
        return await self._create_multiple(
            list_cls=IntegrationList,
            resource_cls=Integration,
            items=integration,
            input_resource_cls=IntegrationWrite,
            headers=self._beta_version_header(),
        )

    @overload
    async def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> Integration | None: ...

    @overload
    async def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> IntegrationList: ...

    async def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Integration | IntegrationList | None:
        """`Retrieve one or more integrations by external id <https://api-docs.cognite.com/20230101-beta/tag/Integrations/operation/retrieve_integrations>`_

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids to retrieve.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Returns:
            Integration | IntegrationList | None: Requested integration(s), or None if a single requested external id is not found.

        Examples:

            Retrieve an integration by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.retrieve("my-integration")
        """
        self._warning.warn()
        identifiers = IdentifierSequence.load(external_ids=external_id)
        return await self._retrieve_multiple(
            list_cls=IntegrationList,
            resource_cls=Integration,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            headers=self._beta_version_header(),
        )

    @overload
    async def update(self, item: Integration | IntegrationWrite | IntegrationUpdate) -> Integration: ...

    @overload
    async def update(self, item: Sequence[Integration | IntegrationWrite | IntegrationUpdate]) -> IntegrationList: ...

    async def update(
        self,
        item: Integration
        | IntegrationWrite
        | IntegrationUpdate
        | Sequence[Integration | IntegrationWrite | IntegrationUpdate],
    ) -> Integration | IntegrationList:
        """`Update one or more integrations <https://api-docs.cognite.com/20230101-beta/tag/Integrations/operation/update_integrations>`_

        Args:
            item (Integration | IntegrationWrite | IntegrationUpdate | Sequence[Integration | IntegrationWrite | IntegrationUpdate]): Integration(s) to update.

        Returns:
            Integration | IntegrationList: Updated integration(s)

        Examples:

            Update an integration that you have fetched. This will perform a full update of the integration:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.integrations import IntegrationUpdate
                >>> client = CogniteClient()
                >>> update = IntegrationUpdate(external_id="my-integration")
                >>> update.description.set("My new description")
                >>> res = client.integrations.update(update)
        """
        self._warning.warn()
        return await self._update_multiple(
            list_cls=IntegrationList,
            resource_cls=Integration,
            update_cls=IntegrationUpdate,
            items=item,
            headers=self._beta_version_header(),
        )

    async def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more integrations <https://api-docs.cognite.com/20230101-beta/tag/Integrations/operation/delete_integrations>`_

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete integrations by external id:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.integrations.delete(external_id=["my-integration"])
        """
        self._warning.warn()
        await self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
            extra_body_fields={"ignoreUnknownIds": ignore_unknown_ids},
            headers=self._beta_version_header(),
        )

    async def sync(
        self,
        external_id: str,
        task_name: str | None = None,
        include_errors: bool = False,
        include_task_updates: bool = False,
        start_time: int | None = None,
        cursor: str | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> SyncResult:
        """`Sync integration history <https://api-docs.cognite.com/20230101-beta/tag/Integration-Tasks/operation/sync_integration_history>`_

        Incrementally fetch task history and/or errors for an integration since a previous sync call. This is more
        efficient than repeatedly listing task history/errors when polling for updates, e.g. for dashboards or
        alerting. At least one of `include_errors` and `include_task_updates` must be True.

        Args:
            external_id (str): Only return history for the integration with this external id.
            task_name (str | None): Only return history for the task with this name.
            include_errors (bool): Include errors reported since the last sync.
            include_task_updates (bool): Include task history entries reported since the last sync.
            start_time (int | None): Only return items reported at or after this time, in milliseconds since epoch. Only used on the first call, pass the returned cursor on subsequent calls instead.
            cursor (str | None): Cursor returned from a previous call to this method, to continue syncing from where you left off.
            limit (int | None): Maximum number of items to return in this page. Defaults to 25.

        Raises:
            ValueError: If both `include_errors` and `include_task_updates` are False.

        Returns:
            SyncResult: A single page of results. Inspect `more_data` to see whether you should immediately call this method again with the returned `next_cursor`, or back off before doing so.

        Examples:

            Sync task history and errors for an integration:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.integrations.sync(
                ...     external_id="my-integration", include_errors=True, include_task_updates=True
                ... )
                >>> while res.more_data:
                ...     res = client.integrations.sync(
                ...         external_id="my-integration",
                ...         include_errors=True,
                ...         include_task_updates=True,
                ...         cursor=res.next_cursor,
                ...     )
        """
        if not include_errors and not include_task_updates:
            raise ValueError("At least one of 'include_errors' or 'include_task_updates' must be True.")

        self._warning.warn()
        response = await self._get(
            url_path=f"{self._RESOURCE_PATH}/sync",
            params=drop_none_values(
                {
                    "externalId": external_id,
                    "taskName": task_name,
                    "includeErrors": include_errors,
                    "includeTaskUpdates": include_task_updates,
                    "startTime": start_time,
                    "cursor": cursor,
                    "limit": limit,
                }
            ),
            headers=self._beta_version_header(),
            semaphore=self._get_semaphore("read"),
        )
        return SyncResult._load(response.json())
