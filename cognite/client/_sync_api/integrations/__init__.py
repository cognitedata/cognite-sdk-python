"""
===============================================================================
05ec77fce9ac730a8d9873350250de4f
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.integrations.actions import SyncIntegrationActionsAPI
from cognite.client._sync_api.integrations.config import SyncIntegrationConfigAPI
from cognite.client._sync_api.integrations.errors import SyncIntegrationErrorsAPI
from cognite.client._sync_api.integrations.tasks import SyncIntegrationTasksAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.integrations.integrations import (
    Integration,
    IntegrationList,
    IntegrationUpdate,
    IntegrationWrite,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncIntegrationsAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.tasks = SyncIntegrationTasksAPI(async_client)
        self.errors = SyncIntegrationErrorsAPI(async_client)
        self.config = SyncIntegrationConfigAPI(async_client)
        self.actions = SyncIntegrationActionsAPI(async_client)

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Integration]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[IntegrationList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Integration] | Iterator[IntegrationList]:
        """
        Iterate over integrations

        Fetches integrations as they are iterated over, so you keep a limited number of integrations in memory.

        Args:
            chunk_size (int | None): Number of integrations to return in each chunk. Defaults to yielding one integration a time.
            limit (int | None): Maximum number of integrations to return. Defaults to return all items.

        Yields:
            Integration | IntegrationList: yields Integration one by one if chunk_size is not specified, else IntegrationList objects.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.integrations(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> IntegrationList:
        """
        `List integrations <https://api-docs.cognite.com/20230101-alpha/tag/Integrations/operation/list_integrations>`_

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
        return run_sync(self.__async_client.integrations.list(limit=limit))

    @overload
    def create(self, integration: IntegrationWrite) -> Integration: ...

    @overload
    def create(self, integration: Sequence[IntegrationWrite]) -> IntegrationList: ...

    def create(self, integration: IntegrationWrite | Sequence[IntegrationWrite]) -> Integration | IntegrationList:
        """
        `Create one or more integrations <https://api-docs.cognite.com/20230101-alpha/tag/Integrations/operation/create_integrations>`_

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
        return run_sync(self.__async_client.integrations.create(integration=integration))

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> Integration | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> IntegrationList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Integration | IntegrationList | None:
        """
        `Retrieve one or more integrations by external id <https://api-docs.cognite.com/20230101-alpha/tag/Integrations/operation/retrieve_integrations>`_

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
        return run_sync(
            self.__async_client.integrations.retrieve(external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )

    @overload
    def update(self, item: Integration | IntegrationWrite | IntegrationUpdate) -> Integration: ...

    @overload
    def update(self, item: Sequence[Integration | IntegrationWrite | IntegrationUpdate]) -> IntegrationList: ...

    def update(
        self,
        item: Integration
        | IntegrationWrite
        | IntegrationUpdate
        | Sequence[Integration | IntegrationWrite | IntegrationUpdate],
    ) -> Integration | IntegrationList:
        """
        `Update one or more integrations <https://api-docs.cognite.com/20230101-alpha/tag/Integrations/operation/update_integrations>`_

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
        return run_sync(self.__async_client.integrations.update(item=item))

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete one or more integrations <https://api-docs.cognite.com/20230101-alpha/tag/Integrations/operation/delete_integrations>`_

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
        return run_sync(
            self.__async_client.integrations.delete(external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )
