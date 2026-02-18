"""
===============================================================================
c89364f15d0ee4178e12fb002ea38399
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import Iterator, MutableSequence, Sequence
from typing import TYPE_CHECKING, Literal, overload

from cognite.client import AsyncCogniteClient
from cognite.client._api.workflows import WorkflowIdentifier, WorkflowVersionIdentifier
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.workflows import (
    WorkflowIds,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.utils._async_helpers import SyncIterator, run_sync

if TYPE_CHECKING:
    from cognite.client import AsyncCogniteClient


class SyncWorkflowVersionAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    @overload
    def __call__(
        self,
        chunk_size: None = None,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = None,
    ) -> Iterator[WorkflowVersion]: ...

    @overload
    def __call__(
        self,
        chunk_size: int,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = None,
    ) -> Iterator[WorkflowVersionList]: ...

    def __call__(
        self,
        chunk_size: int | None = None,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = None,
    ) -> Iterator[WorkflowVersion] | Iterator[WorkflowVersionList]:
        """
        Iterate over workflow versions

        Args:
            chunk_size (int | None): The number of workflow versions to return in each chunk. Defaults to yielding one workflow version at a time.
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int | None): Maximum number of workflow versions to return. Defaults to returning all.

        Yields:
            WorkflowVersion | WorkflowVersionList: Yields WorkflowVersion one by one if chunk_size is None, otherwise yields WorkflowVersionList objects.
        """  # noqa: DOC404
        yield from SyncIterator(
            self.__async_client.workflows.versions(
                chunk_size=chunk_size, workflow_version_ids=workflow_version_ids, limit=limit
            )
        )  # type: ignore [misc]

    @overload
    def upsert(self, version: WorkflowVersionUpsert) -> WorkflowVersion: ...

    @overload
    def upsert(self, version: Sequence[WorkflowVersionUpsert]) -> WorkflowVersionList: ...

    def upsert(
        self, version: WorkflowVersionUpsert | Sequence[WorkflowVersionUpsert], mode: Literal["replace"] = "replace"
    ) -> WorkflowVersion | WorkflowVersionList:
        """
        `Create one or more workflow version(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflowVersion>`_

        Note this is an upsert endpoint, so workflow versions that already exist will be updated, and new ones will be created.

        Args:
            version (WorkflowVersionUpsert | Sequence[WorkflowVersionUpsert]): The workflow version(s) to upsert.
            mode (Literal['replace']): This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            WorkflowVersion | WorkflowVersionList: The created workflow version(s).

        Examples:

            Create one workflow version with a single Function task:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import (
                ...     WorkflowVersionUpsert, WorkflowDefinitionUpsert,
                ...     WorkflowTask, FunctionTaskParameters,
                ... )
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> function_task = WorkflowTask(
                ...     external_id="my_workflow-task1",
                ...     parameters=FunctionTaskParameters(
                ...         external_id="my_fn_xid",
                ...         data={"a": 1, "b": 2},
                ...     ),
                ... )
                >>> new_version = WorkflowVersionUpsert(
                ...    workflow_external_id="my_workflow",
                ...    version="1",
                ...    workflow_definition=WorkflowDefinitionUpsert(
                ...        tasks=[function_task],
                ...        description="This workflow has one step",
                ...    ),
                ... )
                >>> res = client.workflows.versions.upsert(new_version)
        """
        return run_sync(
            self.__async_client.workflows.versions.upsert(version=version, mode=mode)  # type: ignore [call-overload]
        )

    def delete(
        self,
        workflow_version_id: WorkflowVersionIdentifier
        | MutableSequence[WorkflowVersionId]
        | MutableSequence[tuple[str, str]],
        ignore_unknown_ids: bool = False,
    ) -> None:
        """
        `Delete a workflow version(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/DeleteSpecificVersionsOfWorkflow>`_

        Args:
            workflow_version_id (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionId] | MutableSequence[tuple[str, str]]): Workflow version id or list of workflow version ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow version "1" of workflow "my workflow" specified by using a tuple:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> client.workflows.versions.delete(("my workflow", "1"))

            Delete workflow version "1" of workflow "my workflow" and workflow version "2" of workflow "my workflow 2" using the WorkflowVersionId class:

                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client.workflows.versions.delete([WorkflowVersionId("my workflow", "1"), WorkflowVersionId("my workflow 2", "2")])
        """
        return run_sync(
            self.__async_client.workflows.versions.delete(
                workflow_version_id=workflow_version_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    @overload
    def retrieve(self, workflow_external_id: WorkflowVersionIdentifier) -> WorkflowVersion | None: ...

    @overload
    def retrieve(
        self, workflow_external_id: Sequence[WorkflowVersionIdentifier] | WorkflowIds
    ) -> WorkflowVersionList: ...

    def retrieve(
        self,
        workflow_external_id: WorkflowVersionIdentifier | Sequence[WorkflowVersionIdentifier] | WorkflowIds,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersion | WorkflowVersionList | None:
        """
        `Retrieve a workflow version. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/GetSpecificVersion>`_

        Args:
            workflow_external_id (WorkflowVersionIdentifier | Sequence[WorkflowVersionIdentifier] | WorkflowIds): External id of the workflow.
            ignore_unknown_ids (bool): When requesting multiple, whether to ignore external IDs that are not found rather than throwing an exception.

        Returns:
            WorkflowVersion | WorkflowVersionList | None: If a single identifier is specified: the requested workflow version, or None if it does not exist. If several ids are specified: the requested workflow versions.

        Examples:

            Retrieve workflow version 'v1' of workflow "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.versions.retrieve(WorkflowVersionId("my_workflow", "v1"))

            Retrieve multiple workflow versions and ignore unknown:

                >>> res = client.workflows.versions.retrieve(
                ...     [WorkflowVersionId("my_workflow", "v1"), WorkflowVersionId("other", "v3.2")],
                ...     ignore_unknown_ids=True,
                ... )
                >>> # A sequence of tuples is also accepted:
                >>> res = client.workflows.versions.retrieve([("my_workflow", "v1"), ("other", "v3.2")])
        """
        return run_sync(
            self.__async_client.workflows.versions.retrieve(  # type: ignore [call-overload]
                workflow_external_id=workflow_external_id, ignore_unknown_ids=ignore_unknown_ids
            )
        )

    def list(
        self,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> WorkflowVersionList:
        """
        `List workflow versions in the project <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/ListWorkflowVersions>`_

        Args:
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowVersionList: The requested workflow versions.

        Examples:

            Get all workflow version for workflows 'my_workflow' and 'my_workflow_2':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.versions.list(["my_workflow", "my_workflow_2"])

            Get all workflow versions for workflows 'my_workflow' and 'my_workflow_2' using the WorkflowVersionId class:

                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> res = client.workflows.versions.list(
                ...     [WorkflowVersionId("my_workflow"), WorkflowVersionId("my_workflow_2")])

            Get all workflow versions for workflows 'my_workflow' version '1' and 'my_workflow_2' version '2' using tuples:

                >>> res = client.workflows.versions.list(
                ...     [("my_workflow", "1"), ("my_workflow_2", "2")])
        """
        return run_sync(
            self.__async_client.workflows.versions.list(workflow_version_ids=workflow_version_ids, limit=limit)
        )
