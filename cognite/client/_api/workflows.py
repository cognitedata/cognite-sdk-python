from __future__ import annotations

import warnings
from collections.abc import Iterator, MutableSequence, Sequence
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowExecution,
    WorkflowExecutionDetailed,
    WorkflowExecutionList,
    WorkflowIds,
    WorkflowList,
    WorkflowStatus,
    WorkflowTaskExecution,
    WorkflowTrigger,
    WorkflowTriggerCreate,
    WorkflowTriggerList,
    WorkflowTriggerRun,
    WorkflowTriggerRunList,
    WorkflowTriggerUpsert,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import at_least_one_is_not_none, interpolate_and_url_encode, split_into_chunks
from cognite.client.utils._concurrency import execute_tasks
from cognite.client.utils._identifier import (
    IdentifierSequence,
    WorkflowVersionIdentifierSequence,
)
from cognite.client.utils._session import create_session_and_return_nonce
from cognite.client.utils._validation import assert_type
from cognite.client.utils.useful_types import SequenceNotStr

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient
    from cognite.client.data_classes import ClientCredentials

WorkflowIdentifier: TypeAlias = WorkflowVersionId | tuple[str, str] | str
WorkflowVersionIdentifier: TypeAlias = WorkflowVersionId | tuple[str, str]


def wrap_workflow_ids(
    workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None,
) -> list[dict[str, Any]]:
    if workflow_version_ids is None:
        return []
    return WorkflowIds.load(workflow_version_ids).dump(camel_case=True, as_external_id=True)


class WorkflowTriggerAPI(APIClient):
    _RESOURCE_PATH = "/workflows/triggers"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 1

    def upsert(
        self,
        workflow_trigger: WorkflowTriggerUpsert,
        client_credentials: ClientCredentials | dict | None = None,
    ) -> WorkflowTrigger:
        """`Create or update a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/CreateOrUpdateTriggers>`_

        Args:
            workflow_trigger (WorkflowTriggerUpsert): The workflow trigger specification.
            client_credentials (ClientCredentials | dict | None): Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            WorkflowTrigger: The created or updated workflow trigger specification.

        Examples:

            Create or update a scheduled trigger for a workflow:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.workflows import WorkflowTriggerUpsert, WorkflowScheduledTriggerRule
                >>> client = CogniteClient()
                >>> client.workflows.triggers.upsert(
                ...     WorkflowTriggerUpsert(
                ...         external_id="my_trigger",
                ...         trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 0 * * *"),
                ...         workflow_external_id="my_workflow",
                ...         workflow_version="1",
                ...         input={"a": 1, "b": 2},
                ...         metadata={"key": "value"},
                ...     )
                ... )

            Create or update a data modeling trigger for a workflow:

                >>> from cognite.client.data_classes.workflows import WorkflowDataModelingTriggerRule, WorkflowTriggerDataModelingQuery
                >>> from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Select, SourceSelector
                >>> from cognite.client.data_classes.data_modeling import ViewId
                >>> from cognite.client.data_classes.filters import Equals
                >>> view_id = ViewId("my_space_id", "view_external_id", "v1")
                >>> client.workflows.triggers.upsert(
                ...     WorkflowTriggerUpsert(
                ...         external_id="my_trigger",
                ...         trigger_rule=WorkflowDataModelingTriggerRule(
                ...             data_modeling_query=WorkflowTriggerDataModelingQuery(
                ...                 with_={"timeseries": NodeResultSetExpression(filter=Equals(view_id.as_property_ref("name"), value="my_name"))},
                ...                 select={"timeseries": Select([SourceSelector(view_id, ["name"])])},
                ...             ),
                ...             batch_size=500,
                ...             batch_timeout=300,
                ...         ),
                ...         workflow_external_id="my_workflow",
                ...         workflow_version="1",
                ...     )
                ... )

        """
        nonce = create_session_and_return_nonce(
            self._cognite_client, api_name="Workflow API", client_credentials=client_credentials
        )
        dumped = workflow_trigger.dump(camel_case=True)
        dumped["authentication"] = {"nonce": nonce}
        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [dumped]},
        )
        return WorkflowTrigger._load(response.json().get("items")[0])

    # TODO: remove method and associated data classes in next major release
    def create(
        self,
        workflow_trigger: WorkflowTriggerCreate,
        client_credentials: ClientCredentials | dict | None = None,
    ) -> WorkflowTrigger:
        """Create or update a trigger for a workflow.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.upsert' instead. It will be removed in the next major version.
        """
        warnings.warn(
            "This method is deprecated, use '.upsert' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.upsert(workflow_trigger, client_credentials)

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """`Delete one or more triggers for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/deleteTriggers>`_

        Args:
            external_id (str | SequenceNotStr[str]): The external id(s) of the trigger(s) to delete.

        Examples:

            Delete a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.triggers.delete("my_trigger")

            Delete a list of triggers:

                >>> client.workflows.triggers.delete(["my_trigger", "another_trigger"])
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )

    def get_triggers(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """List the workflow triggers.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.list' instead. It will be removed in the next major version.
        """
        warnings.warn(
            "The 'get_triggers' method is deprecated, use 'list' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.list(limit)

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerList:
        """`List the workflow triggers. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggers>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerList: The list of triggers.

        Examples:

            List all triggers:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.triggers.list(limit=None)
        """
        return self._list(
            method="GET",
            url_path=self._RESOURCE_PATH,
            resource_cls=WorkflowTrigger,
            list_cls=WorkflowTriggerList,
            limit=limit,
        )

    def get_trigger_run_history(
        self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ
    ) -> WorkflowTriggerRunList:
        """List the history of runs for a trigger.

        .. admonition:: Deprecation Warning

            This method is deprecated, use '.list_runs' instead. It will be removed in the next major version.
        """
        warnings.warn(
            "The 'get_trigger_run_history' method is deprecated, use 'list_runs' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.list_runs(external_id, limit)

    def list_runs(self, external_id: str, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowTriggerRunList:
        """`List the history of runs for a trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggerHistory>`_

        Args:
            external_id (str): The external id of the trigger to list runs for.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerRunList: The requested trigger runs.

        Examples:

            Get all runs for a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.triggers.list_runs("my_trigger", limit=None)
        """
        return self._list(
            method="GET",
            url_path=interpolate_and_url_encode(self._RESOURCE_PATH + "/{}/history", external_id),
            resource_cls=WorkflowTriggerRun,
            list_cls=WorkflowTriggerRunList,
            limit=limit,
        )


class WorkflowTaskAPI(APIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    def update(
        self, task_id: str, status: Literal["completed", "failed"], output: dict | None = None
    ) -> WorkflowTaskExecution:
        """`Update status of async task. <https://api-docs.cognite.com/20230101/tag/Tasks/operation/UpdateTaskStatus>`_

        For tasks that has been marked with 'is_async = True', the status must be updated by calling this endpoint with either 'completed' or 'failed'.

        Args:
            task_id (str): The server-generated id of the task.
            status (Literal['completed', 'failed']): The new status of the task. Must be either 'completed' or 'failed'.
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


class WorkflowVersionAPI(APIClient):
    _RESOURCE_PATH = "/workflows/versions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1
        self._RETRIEVE_LIMIT = 1
        self._DELETE_LIMIT = 100

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
        """Iterate over workflow versions

        Args:
            chunk_size (int | None): The number of workflow versions to return in each chunk. Defaults to yielding one workflow version at a time.
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int | None): Maximum number of workflow versions to return. Defaults to returning all.

        Returns:
            Iterator[WorkflowVersion] | Iterator[WorkflowVersionList]: Yields WorkflowVersion one by one if chunk_size is None, otherwise yields WorkflowVersionList objects.
        """
        return self._list_generator(
            method="GET",
            resource_cls=WorkflowVersion,
            list_cls=WorkflowVersionList,
            filter={"workflowFilters": wrap_workflow_ids(workflow_version_ids)},
            limit=limit,
            chunk_size=chunk_size,
        )

    def __iter__(self) -> Iterator[WorkflowVersion]:
        """Iterate all over workflow versions"""
        return self()

    @overload
    def upsert(self, version: WorkflowVersionUpsert) -> WorkflowVersion: ...

    @overload
    def upsert(self, version: Sequence[WorkflowVersionUpsert]) -> WorkflowVersionList: ...

    def upsert(
        self, version: WorkflowVersionUpsert | Sequence[WorkflowVersionUpsert], mode: Literal["replace"] = "replace"
    ) -> WorkflowVersion | WorkflowVersionList:
        """`Create one or more workflow version(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflowVersion>`_

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
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflow versions.")

        assert_type(version, "workflow version", [WorkflowVersionUpsert, Sequence])

        return self._create_multiple(
            list_cls=WorkflowVersionList,
            resource_cls=WorkflowVersion,
            items=version,
            input_resource_cls=WorkflowVersionUpsert,
        )

    def delete(
        self,
        workflow_version_id: WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier],
        ignore_unknown_ids: bool = False,
    ) -> None:
        """`Delete a workflow version(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/DeleteSpecificVersionsOfWorkflow>`_

        Args:
            workflow_version_id (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier]): Workflow version id or list of workflow version ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow version "1" of workflow "my workflow" specified by using a tuple:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.versions.delete(("my workflow", "1"))

            Delete workflow version "1" of workflow "my workflow" and workflow version "2" of workflow "my workflow 2" using the WorkflowVersionId class:

                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client.workflows.versions.delete([WorkflowVersionId("my workflow", "1"), WorkflowVersionId("my workflow 2", "2")])

        """
        identifiers = WorkflowIds.load(workflow_version_id).dump(camel_case=True)
        self._delete_multiple(
            identifiers=WorkflowVersionIdentifierSequence.load(identifiers),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    @overload
    def retrieve(
        self,
        workflow_external_id: WorkflowVersionIdentifier | str,
        version: str | None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersion | None: ...

    @overload
    def retrieve(
        self,
        workflow_external_id: Sequence[WorkflowVersionIdentifier] | WorkflowIds,
        version: None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersionList: ...

    def retrieve(
        self,
        workflow_external_id: WorkflowVersionIdentifier | Sequence[WorkflowVersionIdentifier] | WorkflowIds | str,
        version: str | None = None,
        *,
        ignore_unknown_ids: bool = False,
    ) -> WorkflowVersion | WorkflowVersionList | None:
        """`Retrieve a workflow version. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/GetSpecificVersion>`_

        Args:
            workflow_external_id (WorkflowVersionIdentifier | Sequence[WorkflowVersionIdentifier] | WorkflowIds | str): External id of the workflow.
            version (str | None): Version of the workflow.
            ignore_unknown_ids (bool): When requesting multiple, whether to ignore external IDs that are not found rather than throwing an exception.

        Returns:
            WorkflowVersion | WorkflowVersionList | None: If a single identifier is specified: the requested workflow version, or None if it does not exist. If several ids are specified: the requested workflow versions.

        Examples:

            Retrieve workflow version 'v1' of workflow "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.retrieve(WorkflowVersionId("my_workflow", "v1"))

            Retrieve multiple workflow versions and ignore unknown:

                >>> res = client.workflows.versions.retrieve(
                ...     [WorkflowVersionId("my_workflow", "v1"), WorkflowVersionId("other", "v3.2")],
                ...     ignore_unknown_ids=True,
                ... )
                >>> # A sequence of tuples is also accepted:
                >>> res = client.workflows.versions.retrieve([("my_workflow", "v1"), ("other", "v3.2")])

            DEPRECATED: You can also pass workflow_external_id and version as separate arguments:

                >>> res = client.workflows.versions.retrieve("my_workflow", "v1")

        """
        match workflow_external_id, version:
            case str(), str():
                warnings.warn(
                    "This usage is deprecated, please pass one or more `WorkflowVersionId` instead.'",
                    DeprecationWarning,
                )
                workflow_external_id = WorkflowVersionId(workflow_external_id, version)
            case str(), None:
                raise TypeError(
                    "You must specify which 'version' of the workflow to retrieve. Deprecation Warning: This usage is deprecated, please pass "
                    "one or more `WorkflowVersionId` instead."
                )
            case WorkflowVersionId() | Sequence(), str():
                warnings.warn("Argument 'version' is ignored when passing one or more 'WorkflowVersionId'", UserWarning)

        # We can not use _retrieve_multiple as the backend doesn't support 'ignore_unknown_ids':
        def get_single(wf_xid: WorkflowVersionId, ignore_missing: bool = ignore_unknown_ids) -> WorkflowVersion | None:
            try:
                response = self._get(
                    url_path=interpolate_and_url_encode("/workflows/{}/versions/{}", *wf_xid.as_tuple())
                )
                return WorkflowVersion._load(response.json())
            except CogniteAPIError as e:
                if ignore_missing and e.code == 404:
                    return None
                raise

        # WorkflowVersionId doesn't require 'version' to be given, so we raise in case it is missing:
        given_wf_ids = WorkflowIds.load(workflow_external_id)
        if any(wf_id.version is None for wf_id in given_wf_ids):
            raise ValueError("Version must be specified for all workflow version IDs.")

        is_single = isinstance(workflow_external_id, WorkflowVersionId) or (
            isinstance(workflow_external_id, tuple) and len(given_wf_ids) == 1
        )
        if is_single:
            return get_single(given_wf_ids[0], ignore_missing=True)

        # Not really a point in splitting into chunks when chunk_size is 1, but...
        tasks = list(map(tuple, split_into_chunks(given_wf_ids, self._RETRIEVE_LIMIT)))
        tasks_summary = execute_tasks(get_single, tasks=tasks, max_workers=self._config.max_workers, fail_fast=True)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        return WorkflowVersionList(list(filter(None, tasks_summary.results)), cognite_client=self._cognite_client)

    def list(
        self,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
    ) -> WorkflowVersionList:
        """`List workflow versions in the project <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/ListWorkflowVersions>`_

        Args:
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowVersionList: The requested workflow versions.

        Examples:

            Get all workflow version for workflows 'my_workflow' and 'my_workflow_2':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.list(["my_workflow", "my_workflow_2"])

            Get all workflow versions for workflows 'my_workflow' and 'my_workflow_2' using the WorkflowVersionId class:

                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> res = client.workflows.versions.list(
                ...     [WorkflowVersionId("my_workflow"), WorkflowVersionId("my_workflow_2")])

            Get all workflow versions for workflows 'my_workflow' version '1' and 'my_workflow_2' version '2' using tuples:

                >>> res = client.workflows.versions.list(
                ...     [("my_workflow", "1"), ("my_workflow_2", "2")])

        """
        return self._list(
            method="POST",
            resource_cls=WorkflowVersion,
            list_cls=WorkflowVersionList,
            filter={"workflowFilters": wrap_workflow_ids(workflow_version_ids)},
            limit=limit,
        )


class WorkflowAPI(APIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self.versions = WorkflowVersionAPI(config, api_version, cognite_client)
        self.executions = WorkflowExecutionAPI(config, api_version, cognite_client)
        self.tasks = WorkflowTaskAPI(config, api_version, cognite_client)
        self.triggers = WorkflowTriggerAPI(config, api_version, cognite_client)
        self._RETRIEVE_LIMIT = 1
        self._CREATE_LIMIT = 1
        self._DELETE_LIMIT = 100

    @overload
    def __call__(self, chunk_size: None = None, limit: None = None) -> Iterator[Workflow]: ...

    @overload
    def __call__(self, chunk_size: int, limit: None) -> Iterator[Workflow]: ...

    def __call__(
        self, chunk_size: int | None = None, limit: int | None = None
    ) -> Iterator[Workflow] | Iterator[WorkflowList]:
        """Iterate over workflows

        Args:
            chunk_size (int | None): The number of workflows to return in each chunk. Defaults to yielding one workflow at a time.
            limit (int | None): Maximum number of workflows to return. Defaults to returning all items.

        Returns:
            Iterator[Workflow] | Iterator[WorkflowList]: Yields Workflow one by one if chunk_size is None, otherwise yields WorkflowList objects.

        """
        return self._list_generator(
            method="GET", resource_cls=Workflow, list_cls=WorkflowList, limit=limit, chunk_size=chunk_size
        )

    def __iter__(self) -> Iterator[Workflow]:
        """Iterate all over workflows"""
        return self()

    @overload
    def upsert(self, workflow: WorkflowUpsert) -> Workflow: ...

    @overload
    def upsert(self, workflow: Sequence[WorkflowUpsert]) -> WorkflowList: ...

    def upsert(
        self, workflow: WorkflowUpsert | Sequence[WorkflowUpsert], mode: Literal["replace"] = "replace"
    ) -> Workflow | WorkflowList:
        """`Create one or more workflow(s). <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflow>`_

        Note this is an upsert endpoint, so workflows that already exist will be updated, and new ones will be created.

        Args:
            workflow (WorkflowUpsert | Sequence[WorkflowUpsert]): The workflow(s) to upsert.
            mode (Literal['replace']): This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            Workflow | WorkflowList: The created workflow(s).

        Examples:

            Create one workflow with external id "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowUpsert
                >>> client = CogniteClient()
                >>> wf = WorkflowUpsert(external_id="my_workflow", description="my workflow description")
                >>> res = client.workflows.upsert(wf)

            Create multiple workflows:

                >>> wf2 = WorkflowUpsert(external_id="other", data_set_id=123)
                >>> res = client.workflows.upsert([wf, wf2])
        """
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflows.")

        assert_type(workflow, "workflow", [WorkflowUpsert, Sequence])

        return self._create_multiple(
            list_cls=WorkflowList,
            resource_cls=Workflow,
            items=workflow,
            input_resource_cls=WorkflowUpsert,
        )

    @overload
    def retrieve(self, external_id: str) -> Workflow | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> WorkflowList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False
    ) -> Workflow | WorkflowList | None:
        """`Retrieve one or more workflows. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/fetchWorkflowDetails>`_

        Args:
            external_id (str | SequenceNotStr[str]): Identifier (or sequence of identifiers) for a Workflow. Must be unique.
            ignore_unknown_ids (bool): When requesting multiple workflows, whether to ignore external IDs that are not found rather than throwing an exception.

        Returns:
            Workflow | WorkflowList | None: If a single external ID is specified: the requested workflow, or None if it does not exist. If several external IDs are specified: the requested workflows.

        Examples:

            Retrieve workflow with external ID "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> workflow = client.workflows.retrieve("my_workflow")

            Retrieve multiple workflows:

                >>> workflow_list = client.workflows.retrieve(["foo", "bar"])
        """

        # We can not use _retrieve_multiple as the backend doesn't support 'ignore_unknown_ids':
        def get_single(xid: str, ignore_missing: bool = ignore_unknown_ids) -> Workflow | None:
            try:
                response = self._get(url_path=interpolate_and_url_encode("/workflows/{}", xid))
                return Workflow._load(response.json())
            except CogniteAPIError as e:
                if ignore_missing and e.code == 404:
                    return None
                raise

        if isinstance(external_id, str):
            return get_single(external_id, ignore_missing=True)

        # Not really a point in splitting into chunks when chunk_size is 1, but...
        tasks = list(map(tuple, split_into_chunks(external_id, self._RETRIEVE_LIMIT)))
        tasks_summary = execute_tasks(get_single, tasks=tasks, max_workers=self._config.max_workers, fail_fast=True)
        tasks_summary.raise_compound_exception_if_failed_tasks()
        return WorkflowList(list(filter(None, tasks_summary.results)), cognite_client=self._cognite_client)

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more workflows with versions. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/DeleteWorkflows>`_

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow with external_id "my_workflow":

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.delete("my_workflow")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowList:
        """`List workflows in the project. <https://api-docs.cognite.com/20230101/tag/Workflows/operation/FetchAllWorkflows>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowList: Workflows in the CDF project.

        Examples:

            List all workflows:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.list(limit=None)
        """
        return self._list(
            method="GET",
            resource_cls=Workflow,
            list_cls=WorkflowList,
            limit=limit,
        )
