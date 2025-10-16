from __future__ import annotations

import warnings
from collections.abc import MutableSequence
from typing import TYPE_CHECKING, Any

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    WorkflowExecution,
    WorkflowExecutionDetailed,
    WorkflowExecutionList,
    WorkflowIds,
    WorkflowStatus,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import at_least_one_is_not_none, interpolate_and_url_encode
from cognite.client.utils._session import create_session_and_return_nonce

if TYPE_CHECKING:
    from cognite.client._api.workflows.versions import WorkflowVersionIdentifier
    from cognite.client.data_classes import ClientCredentials


class WorkflowExecutionAPI(APIClient):
    _RESOURCE_PATH = "/workflows/executions"

    def retrieve_detailed(self, id: str) -> WorkflowExecutionDetailed | None:
        """`Retrieve a workflow execution with detailed information. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/ExecutionOfSpecificRunOfWorkflow>`_

        Args:
            id (str): The server-generated id of the workflow execution.

        Returns:
            WorkflowExecutionDetailed | None: The requested workflow execution if it exists, None otherwise.

        Examples:

            Retrieve workflow execution with id '000560bc-9080-4286-b242-a27bb4819253':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.retrieve_detailed("000560bc-9080-4286-b242-a27bb4819253")

            List workflow executions and retrieve detailed information for the first one:

                >>> res = client.workflows.executions.list()
                >>> res = client.workflows.executions.retrieve_detailed(res[0].id)

        """
        try:
            response = self._get(url_path=f"{self._RESOURCE_PATH}/{id}")
        except CogniteAPIError as e:
            if e.code == 400:
                return None
            raise
        return WorkflowExecutionDetailed._load(response.json())

    def trigger(
        self,
        workflow_external_id: str,
        version: str,
        input: dict | None = None,
        metadata: dict | None = None,
        client_credentials: ClientCredentials | None = None,
    ) -> WorkflowExecution:
        """Trigger a workflow execution.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.run' instead. It will be completely removed in the next major version.
        """
        warnings.warn(
            "This methods has been deprecated, use '.run' instead. It will completely removed in the next major release.",
            UserWarning,
        )
        return self.run(workflow_external_id, version, input, metadata, client_credentials)

    def run(
        self,
        workflow_external_id: str,
        version: str,
        input: dict | None = None,
        metadata: dict | None = None,
        client_credentials: ClientCredentials | None = None,
        nonce: str | None = None,
    ) -> WorkflowExecution:
        """`Run a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/TriggerRunOfSpecificVersionOfWorkflow>`_

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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.run("foo", "1")

            Trigger a workflow execution with input data:

                >>> res = client.workflows.executions.run("foo", "1", input={"a": 1, "b": 2})

            Trigger a workflow execution using a specific set of client credentials (i.e. not your current credentials):

                >>> import os
                >>> from cognite.client.data_classes import ClientCredentials
                >>> credentials = ClientCredentials("my-client-id", os.environ["MY_CLIENT_SECRET"])
                >>> res = client.workflows.executions.run("foo", "1", client_credentials=credentials)
        """
        nonce = nonce or create_session_and_return_nonce(
            self._cognite_client, api_name="Workflow API", client_credentials=client_credentials
        )
        body = {"authentication": {"nonce": nonce}}
        if input is not None:
            body["input"] = input
        if metadata is not None:
            body["metadata"] = metadata

        response = self._post(
            url_path=interpolate_and_url_encode("/workflows/{}/versions/{}/run", workflow_external_id, version),
            json=body,
        )
        return WorkflowExecution._load(response.json())

    def list(
        self,
        workflow_version_ids: WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: WorkflowStatus | MutableSequence[WorkflowStatus] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> WorkflowExecutionList:
        """`List workflow executions in the project. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/ListWorkflowExecutions>`_

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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.list(("my_workflow", "1"))

            Get all workflow executions from the last 24 hours:

                >>> from cognite.client.utils import timestamp_to_ms
                >>> res = client.workflows.executions.list(
                ...     created_time_start=timestamp_to_ms("1d-ago"))

        """
        # Passing at least one filter criterion is required:
        if not at_least_one_is_not_none(workflow_version_ids, created_time_start, created_time_end, statuses):
            raise ValueError(
                "At least one of 'workflow_version_ids', 'created_time_start', "
                "'created_time_end', 'statuses' must be provided."
            )
        filter_: dict[str, Any] = {}
        if workflow_version_ids is not None:
            filter_["workflowFilters"] = WorkflowIds.load(workflow_version_ids).dump(
                camel_case=True, as_external_id=True
            )
        if created_time_start is not None:
            filter_["createdTimeStart"] = created_time_start
        if created_time_end is not None:
            filter_["createdTimeEnd"] = created_time_end
        if statuses is not None:
            if isinstance(statuses, MutableSequence):
                filter_["status"] = [status.upper() for status in statuses]
            else:  # Assume it is a stringy type
                filter_["status"] = [statuses.upper()]

        return self._list(
            method="POST",
            resource_cls=WorkflowExecution,
            list_cls=WorkflowExecutionList,
            filter=filter_,
            limit=limit,
        )

    def cancel(self, id: str, reason: str | None) -> WorkflowExecution:
        """`Cancel a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/WorkflowExecutionCancellation>`_

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

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.run("foo", "1")
                >>> client.workflows.executions.cancel(id="foo", reason="test cancellation")
        """
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{id}/cancel",
            json={"reason": reason} if reason else {},
        )
        return WorkflowExecution._load(response.json())

    def retry(self, id: str, client_credentials: ClientCredentials | None = None) -> WorkflowExecution:
        """`Retry a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/WorkflowExecutionRetryn>`_

        Args:
            id (str): The server-generated id of the workflow execution.
            client_credentials (ClientCredentials | None): Specific credentials that should be used to retry the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            WorkflowExecution: The retried workflow execution.

        Examples:
            Retry a workflow execution that has been cancelled or failed:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.run("foo", "1")
                >>> client.workflows.executions.cancel(id=res.id, reason="test cancellation")
                >>> client.workflows.executions.retry(res.id)
        """
        nonce = create_session_and_return_nonce(
            self._cognite_client, api_name="Workflow API", client_credentials=client_credentials
        )
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{id}/retry",
            json={"authentication": {"nonce": nonce}},
        )
        return WorkflowExecution._load(response.json())
