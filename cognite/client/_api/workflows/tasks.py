from __future__ import annotations

from typing import Any, Literal

from cognite.client._api_client import APIClient
from cognite.client.data_classes.workflows import WorkflowTaskExecution


class WorkflowTaskAPI(APIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    def update(
        self,
        task_id: str,
        status: Literal["completed", "failed", "failed_with_terminal_error"],
        output: dict | None = None,
    ) -> WorkflowTaskExecution:
        """`Update status of async task. <https://api-docs.cognite.com/20230101/tag/Tasks/operation/UpdateTaskStatus>`_

        For tasks that has been marked with 'is_async = True', the status must be updated by calling this endpoint with either 'completed', 'failed' or 'failed_with_terminal_error'.

        Args:
            task_id (str): The server-generated id of the task.
            status (Literal['completed', 'failed', 'failed_with_terminal_error']): The new status of the task. Must be either 'completed', 'failed' or 'failed_with_terminal_error'.
            output (dict | None): The output of the task. This will be available for tasks that has specified it as an output with the string "${<taskExternalId>.output}"

        Returns:
            WorkflowTaskExecution: The updated task execution.

        Examples:

            Update task with id '000560bc-9080-4286-b242-a27bb4819253' to status 'completed':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "completed")

            Update task with id '000560bc-9080-4286-b242-a27bb4819253' to status 'failed' with output '{"a": 1, "b": 2}':

                >>> res = client.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "failed", output={"a": 1, "b": 2})

            Trigger workflow, retrieve detailed task execution and update status of the second task (assumed to be async) to 'completed':

                >>> res = client.workflows.executions.run("my workflow", "1")
                >>> res = client.workflows.executions.retrieve_detailed(res.id)
                >>> res = client.workflows.tasks.update(res.tasks[1].id, "completed")

        """
        body: dict[str, Any] = {"status": status.upper()}
        if output is not None:
            body["output"] = output
        response = self._post(url_path=f"{self._RESOURCE_PATH}/{task_id}/update", json=body)
        return WorkflowTaskExecution.load(response.json())
