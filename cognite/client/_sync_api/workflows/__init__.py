"""
===============================================================================
01007117ee0b66c3adc02434b19e6cfe
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api.workflows.executions import SyncWorkflowExecutionAPI
from cognite.client._sync_api.workflows.tasks import SyncWorkflowTaskAPI
from cognite.client._sync_api.workflows.triggers import SyncWorkflowTriggerAPI
from cognite.client._sync_api.workflows.versions import SyncWorkflowVersionAPI
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.workflows import Workflow, WorkflowList, WorkflowUpsert
from cognite.client.utils._async_helpers import SyncIterator, run_sync
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncWorkflowAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client
        self.versions = SyncWorkflowVersionAPI(async_client)
        self.executions = SyncWorkflowExecutionAPI(async_client)
        self.tasks = SyncWorkflowTaskAPI(async_client)
        self.triggers = SyncWorkflowTriggerAPI(async_client)

    @overload
    def __call__(self, chunk_size: None = None, limit: int | None = None) -> Iterator[Workflow]: ...

    @overload
    def __call__(self, chunk_size: int, limit: int | None = None) -> Iterator[WorkflowList]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Workflow] | Iterator[WorkflowList]:
        """
        Iterate over workflows

        Args:
            chunk_size: The number of workflows to return in each chunk. Defaults to yielding one workflow at a time.
            limit: Maximum number of workflows to return. Defaults to returning all items.

        Yields:
            Yields Workflow one by one if chunk_size is None, otherwise yields WorkflowList objects.
        """  # noqa: DOC404
        yield from SyncIterator(self.__async_client.workflows(chunk_size=chunk_size, limit=limit))  # type: ignore [misc]

    @overload
    def upsert(self, workflow: WorkflowUpsert, mode: Literal["replace"] = "replace") -> Workflow: ...

    @overload
    def upsert(self, workflow: Sequence[WorkflowUpsert], mode: Literal["replace"] = "replace") -> WorkflowList: ...

    def upsert(
        self, workflow: WorkflowUpsert | Sequence[WorkflowUpsert], mode: Literal["replace"] = "replace"
    ) -> Workflow | WorkflowList:
        """
        `Create one or more workflow(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflow>`_

        Note this is an upsert endpoint, so workflows that already exist will be updated, and new ones will be created.

        Args:
            workflow: The workflow(s) to upsert.
            mode: This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            The created workflow(s).

        Examples:

            Create one workflow with external id "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowUpsert
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> wf = WorkflowUpsert(external_id="my_workflow", description="my workflow description")
                >>> res = client.workflows.upsert(wf)

            Create multiple workflows:

                >>> wf2 = WorkflowUpsert(external_id="other", data_set_id=123)
                >>> res = client.workflows.upsert([wf, wf2])
        """
        return run_sync(self.__async_client.workflows.upsert(workflow=workflow, mode=mode))

    @overload
    def retrieve(self, external_id: str, ignore_unknown_ids: bool = False) -> Workflow | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], ignore_unknown_ids: bool = False) -> WorkflowList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Workflow | WorkflowList | None:
        """
        `Retrieve one or more workflows. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/fetchWorkflowDetails>`_

        Args:
            external_id: Identifier (or sequence of identifiers) for a Workflow. Must be unique.
            ignore_unknown_ids: When requesting multiple workflows, whether to ignore external IDs that are not found rather than throwing an exception.

        Returns:
            the requested workflows.

        Examples:

            Retrieve workflow with external ID "my_workflow":

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> workflow = client.workflows.retrieve("my_workflow")

            Retrieve multiple workflows:

                >>> workflow_list = client.workflows.retrieve(["foo", "bar"])
        """
        return run_sync(
            self.__async_client.workflows.retrieve(external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """
        `Delete one or more workflows with versions. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/DeleteWorkflows>`_

        Args:
            external_id: External id or list of external ids to delete.
            ignore_unknown_ids: Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow with external_id "my_workflow":

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.delete("my_workflow")
        """
        return run_sync(
            self.__async_client.workflows.delete(external_id=external_id, ignore_unknown_ids=ignore_unknown_ids)
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowList:
        """
        `List workflows in the project. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/FetchAllWorkflows>`_

        Args:
            limit: Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            Workflows in the CDF project.

        Examples:

            List all workflows:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.list(limit=None)
        """
        return run_sync(self.__async_client.workflows.list(limit=limit))
