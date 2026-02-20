"""
===============================================================================
b10fdffcf288bd295bdd2a52edd9fadc
This file is auto-generated from the Async API modules, - do not edit manually!
===============================================================================
"""

from __future__ import annotations

from typing import Literal

from cognite.client import AsyncCogniteClient
from cognite.client._sync_api_client import SyncAPIClient
from cognite.client.data_classes.workflows import WorkflowTaskExecution
from cognite.client.utils._async_helpers import run_sync


class SyncWorkflowTaskAPI(SyncAPIClient):
    """Auto-generated, do not modify manually."""

    def __init__(self, async_client: AsyncCogniteClient) -> None:
        self.__async_client = async_client

    def update(
        self,
        task_id: str,
        status: Literal["completed", "failed", "failed_with_terminal_error"],
        output: dict | None = None,
    ) -> WorkflowTaskExecution:
        """
        `Update status of async task. <https://api-docs.cognite.com/20230101/tag/Tasks/operation/UpdateTaskStatus>`_

        For tasks that has been marked with 'is_async = True', the status must be updated by calling this endpoint with either 'completed', 'failed' or 'failed_with_terminal_error'.

        Args:
            task_id: The server-generated id of the task.
            status: The new status of the task. Must be either 'completed', 'failed' or 'failed_with_terminal_error'.
            output: The output of the task. This will be available for tasks that has specified it as an output with the string "${<taskExternalId>.output}"

        Returns:
            The updated task execution.

        Examples:

            Update task with id '000560bc-9080-4286-b242-a27bb4819253' to status 'completed':

                >>> from cognite.client import CogniteClient, AsyncCogniteClient
                >>> client = CogniteClient()
                >>> # async_client = AsyncCogniteClient()  # another option
                >>> res = client.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "completed")

            Update task with id '000560bc-9080-4286-b242-a27bb4819253' to status 'failed' with output '{"a": 1, "b": 2}':

                >>> res = client.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "failed", output={"a": 1, "b": 2})

            Trigger workflow, retrieve detailed task execution and update status of the second task (assumed to be async) to 'completed':

                >>> res = client.workflows.executions.run("my workflow", "1")
                >>> res = client.workflows.executions.retrieve_detailed(res.id)
                >>> res = client.workflows.tasks.update(res.tasks[1].id, "completed")
        """
        return run_sync(self.__async_client.workflows.tasks.update(task_id=task_id, status=status, output=output))
