"""
===============================================================================
60c7e539a4068a90816b0ff964f3d4ab
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from collections.abc import MutableSequence
from typing import TYPE_CHECKING

from cognite.client import AsyncCogniteClient
from cognite.client._api.workflows import WorkflowVersionIdentifier
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.workflows import (
    WorkflowExecution,
    WorkflowExecutionDetailed,
    WorkflowExecutionList,
    WorkflowStatus,
)
from cognite.client.utils._async_helpers import run_sync

if TYPE_CHECKING:
    from cognite.client._api.workflows import WorkflowVersionIdentifier
from cognite.client.data_classes import ClientCredentials


class SyncWorkflowExecutionAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def retrieve_detailed(self, id: str) -> WorkflowExecutionDetailed | None:
        """
        `Retrieve a workflow execution with detailed information. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/ExecutionOfSpecificRunOfWorkflow>`_

        Args:
            id (str): The server-generated id of the workflow execution.

        Returns:
            WorkflowExecutionDetailed | None: The requested workflow execution if it exists, None otherwise.

        Examples:

            Retrieve workflow execution with id '000560bc-9080-4286-b242-a27bb4819253':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.executions.retrieve_detailed("000560bc-9080-4286-b242-a27bb4819253")

            List workflow executions and retrieve detailed information for the first one:

                >>> res = client.workflows.executions.list()
                >>> res = client.workflows.executions.retrieve_detailed(res[0].id)
        """
        return run_sync(self.__async_client.workflows.executions.retrieve_detailed(id=id))

    def run(
        self,
        workflow_external_id: str,
        version: str,
        input: dict | None = None,
        metadata: dict | None = None,
        client_credentials: ClientCredentials | None = None,
        nonce: str | None = None,
    ) -> WorkflowExecution:
        """
        `Run a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/TriggerRunOfSpecificVersionOfWorkflow>`_

        Args:
            workflow_external_id (str): External id of the workflow.
            version (str): Version of the workflow.
            input (dict | None): The input to the workflow execution. This will be available for tasks that have specified it as an input with the string "${workflow.input}" See tip below for more information.
            metadata (dict | None): Application specific metadata. Keys have a maximum length of 32 characters, values a maximum of 255, and there can be a maximum of 10 key-value pairs.
            client_credentials (ClientCredentials | None): Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.
            nonce (str | None): The nonce to use to bind the session. If not provided, a new session will be created using the given 'client_credentials'. If this is not given, the current credentials will be used.

        Tip:
            The workflow input can be available in the workflow tasks. For example, if you have a Task with
            function parameters then you can specify it as follows

                >>> from cognite.client.data_classes import WorkflowTask, FunctionTaskParameters
                >>> task = WorkflowTask(
                ...     external_id="my_workflow-task1",
                ...     parameters=FunctionTaskParameters(
                ...         external_id="cdf_deployed_function:my_function",
                ...         data={"workflow_data": "${workflow.input}"}))

        Tip:
            You can create a session via the Sessions API, using the client.iam.session.create() method.

        Returns:
            WorkflowExecution: The created workflow execution.

        Examples:

            Trigger a workflow execution for the workflow "foo", version 1:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.executions.run("foo", "1")

            Trigger a workflow execution with input data:

                >>> res = client.workflows.executions.run("foo", "1", input={"a": 1, "b": 2})

            Trigger a workflow execution using a specific set of client credentials (i.e. not your current credentials):

                >>> import os
                >>> from cognite.client.data_classes import ClientCredentials
                >>> credentials = ClientCredentials("my-client-id", os.environ["MY_CLIENT_SECRET"])
                >>> res = client.workflows.executions.run("foo", "1", client_credentials=credentials)
        """
        return run_sync(
            self.__async_client.workflows.executions.run(
                workflow_external_id=workflow_external_id,
                version=version,
                input=input,
                metadata=metadata,
                client_credentials=client_credentials,
                nonce=nonce,
            )
        )

    def list(
        self,
        workflow_version_ids: WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: WorkflowStatus | MutableSequence[WorkflowStatus] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> WorkflowExecutionList:
        """
        `List workflow executions in the project. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/ListWorkflowExecutions>`_

        Args:
            workflow_version_ids (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            created_time_start (int | None): Filter out executions that was created before this time. Time is in milliseconds since epoch.
            created_time_end (int | None): Filter out executions that was created after this time. Time is in milliseconds since epoch.
            statuses (WorkflowStatus | MutableSequence[WorkflowStatus] | None): Workflow status or list of workflow statuses to filter on.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowExecutionList: The requested workflow executions.

        Examples:

            Get all workflow executions for workflows 'my_workflow' version '1':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.executions.list(("my_workflow", "1"))

            Get all workflow executions from the last 24 hours:

                >>> from cognite.client.utils import timestamp_to_ms
                >>> res = client.workflows.executions.list(
                ...     created_time_start=timestamp_to_ms("1d-ago"))
        """
        return run_sync(
            self.__async_client.workflows.executions.list(
                workflow_version_ids=workflow_version_ids,
                created_time_start=created_time_start,
                created_time_end=created_time_end,
                statuses=statuses,
                limit=limit,
            )
        )

    def cancel(self, id: str, reason: str | None) -> WorkflowExecution:
        """
        `Cancel a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/WorkflowExecutionCancellation>`_

        Note:
            Cancelling a workflow will immediately cancel the `in_progress` tasks, but not their spawned work in
            other services (like transformations and functions).

        Args:
            id (str): The server-generated id of the workflow execution.
            reason (str | None): The reason for the cancellation, this will be put within the execution's `reasonForIncompletion` field. It is defaulted to 'cancelled' if not provided.


        Returns:
            WorkflowExecution: The canceled workflow execution.

        Examples:

            Trigger a workflow execution for the workflow "foo", version 1 and cancel it:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.executions.run("foo", "1")
                >>> client.workflows.executions.cancel(id="foo", reason="test cancellation")
        """
        return run_sync(self.__async_client.workflows.executions.cancel(id=id, reason=reason))

    def retry(self, id: str, client_credentials: ClientCredentials | None = None) -> WorkflowExecution:
        """
        `Retry a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/WorkflowExecutionRetryn>`_

        Args:
            id (str): The server-generated id of the workflow execution.
            client_credentials (ClientCredentials | None): Specific credentials that should be used to retry the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            WorkflowExecution: The retried workflow execution.

        Examples:
            Retry a workflow execution that has been cancelled or failed:

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.executions.run("foo", "1")
                >>> client.workflows.executions.cancel(id=res.id, reason="test cancellation")
                >>> client.workflows.executions.retry(res.id)
        """
        return run_sync(self.__async_client.workflows.executions.retry(id=id, client_credentials=client_credentials))
