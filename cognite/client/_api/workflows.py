from __future__ import annotations

import warnings
from collections.abc import Iterator, MutableSequence
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload
from urllib.parse import quote

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
from cognite.client.utils._identifier import (
    IdentifierSequence,
    WorkflowVersionIdentifierSequence,
)
from cognite.client.utils._session import create_session_and_return_nonce
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

    def upsert(
        self,
        workflow_trigger: WorkflowTriggerUpsert,
        client_credentials: ClientCredentials | dict | None = None,
    ) -> WorkflowTrigger:
        """`Create or update a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/CreateOrUpdateTriggers>`_

        Args:
            workflow_trigger (WorkflowTriggerUpsert): The workflow trigger specitification.
            client_credentials (ClientCredentials | dict | None): Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            WorkflowTrigger: The created  or updated workflow trigger specification.

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

    # TODO: remove method and associated data classes in next release
    def create(
        self,
        workflow_trigger: WorkflowTriggerCreate,
        client_credentials: ClientCredentials | dict | None = None,
    ) -> WorkflowTrigger:
        """`[DEPRECATED] Create or update a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/CreateOrUpdateTriggers>`_

        This method is deprecated, use '.upsert' instead. It will be completely removed October 2024.

        Args:
            workflow_trigger (WorkflowTriggerCreate): The workflow trigger specitification.
            client_credentials (ClientCredentials | dict | None): Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.

        Returns:
            WorkflowTrigger: The created or updated workflow trigger specification.

        Examples:

            Create or update a scheduled trigger for a workflow:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes.workflows import WorkflowTriggerCreate, WorkflowScheduledTriggerRule
                >>> client = CogniteClient()
                >>> client.workflows.triggers.create(
                ...     WorkflowTriggerCreate(
                ...         external_id="my_trigger",
                ...         trigger_rule=WorkflowScheduledTriggerRule(cron_expression="0 0 * * *"),
                ...         workflow_external_id="my_workflow",
                ...         workflow_version="1",
                ...         input={"a": 1, "b": 2},
                ...     )
                ... )
        """
        warnings.warn(
            "This method is deprecated, use '.upsert' instead. It will be removed in the next major release.",
            UserWarning,
        )
        return self.upsert(workflow_trigger, client_credentials)

    def delete(
        self,
        external_id: str,
    ) -> None:
        """`Delete a trigger for a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/deleteTriggers>`_

        Args:
            external_id (str): The external id of the trigger to delete.

        Examples:

            Delete a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.triggers.delete("my_trigger")
        """
        self._post(
            url_path=self._RESOURCE_PATH + "/delete",
            json={"items": [{"externalId": external_id}]},
        )

    def get_triggers(
        self,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> WorkflowTriggerList:
        """`Retrieve the trigger list. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/getTriggers>`_

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerList: The trigger list.

        Examples:

            Get all triggers:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.triggers.get_triggers()
        """
        return self._list(
            method="GET",
            url_path=self._RESOURCE_PATH,
            resource_cls=WorkflowTrigger,
            list_cls=WorkflowTriggerList,
            limit=limit,
        )

    def get_trigger_run_history(
        self,
        external_id: str,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> WorkflowTriggerRunList:
        """`List the history of runs for a trigger. <https://api-docs.cognite.com/20230101/tag/Workflow-triggers/operation/listTriggerRuns>`_

        Args:
            external_id (str): The external id of the trigger to list runs for.
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.

        Returns:
            WorkflowTriggerRunList: The requested trigger runs.

        Examples:

            Get all runs for a trigger with external id 'my_trigger':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.triggers.get_trigger_run_history("my_trigger")
        """
        return self._list(
            method="GET",
            url_path=self._RESOURCE_PATH + f"/{external_id}/history",
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

            Update task with UUID '000560bc-9080-4286-b242-a27bb4819253' to status 'completed':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "completed")

            Update task with UUID '000560bc-9080-4286-b242-a27bb4819253' to status 'failed' with output '{"a": 1, "b": 2}':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "failed", output={"a": 1, "b": 2})

            Trigger workflow, retrieve detailed task execution and update status of the second task (assumed to be async) to 'completed':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.run("my workflow", "1")
                >>> res = client.workflows.executions.retrieve_detailed(res.id)
                >>> res = client.workflows.tasks.update(res.tasks[1].id, "completed")

        """
        body: dict[str, Any] = {"status": status.upper()}
        if output is not None:
            body["output"] = output
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{task_id}/update",
            json=body,
        )
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

            Retrieve workflow execution with UUID '000560bc-9080-4286-b242-a27bb4819253':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.retrieve_detailed("000560bc-9080-4286-b242-a27bb4819253")

            List workflow executions and retrieve detailed information for the first one:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
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
        """`[DEPRECATED]Trigger a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/TriggerRunOfSpecificVersionOfWorkflow>`_

        This method is deprecated, use '.run' instead. It will be completely removed October 2024.

        Args:
            workflow_external_id (str): External id of the workflow.
            version (str): Version of the workflow.
            input (dict | None): The input to the workflow execution. This will be available for tasks that have specified it as an input with the string "${workflow.input}" See tip below for more information.
            metadata (dict | None): Application specific metadata. Keys have a maximum length of 32 characters, values a maximum of 255, and there can be a maximum of 10 key-value pairs.
            client_credentials (ClientCredentials | None): Specific credentials that should be used to trigger the workflow execution. When passed will take precedence over the current credentials.
        Returns:
            WorkflowExecution: No description.
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
            nonce (str | None): The nonce to use to bind the session. If not provided, a new session will be created using the current credentials.

        Tip:
            The workflow input can be available in the workflow tasks. For example, if you have a Task with
            function parameters then you can specify it as follows

                >>> from cognite.client.data_classes import WorkflowTask, FunctionTaskParameters
                >>> task = WorkflowTask(
                ...     external_id="my_workflow-task1",
                ...     parameters=FunctionTaskParameters(
                ...         external_id="cdf_deployed_function:my_function",
                ...         data={"workflow_data": "${workflow.input}",}))

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

        response = self._post(url_path=f"/workflows/{workflow_external_id}/versions/{version}/run", json=body)
        return WorkflowExecution._load(response.json())

    def list(
        self,
        workflow_version_ids: WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        statuses: WorkflowStatus | MutableSequence[WorkflowStatus] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> WorkflowExecutionList:
        """`List workflow executions in the project. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/ListWorkflowExecutions>`_

        Args:
            workflow_version_ids (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            created_time_start (int | None): Filter out executions that was created before this time. Time is in milliseconds since epoch.
            created_time_end (int | None): Filter out executions that was created after this time. Time is in milliseconds since epoch.
            statuses (WorkflowStatus | MutableSequence[WorkflowStatus] | None): Workflow status or list of workflow statuses to filter on.
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None
                        to return all items.
        Returns:
            WorkflowExecutionList: The requested workflow executions.

        Examples:

            Get all workflow executions for workflows 'my_workflow' version '1':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.list(("my_workflow", "1"))

            Get all workflow executions for workflows after last 24 hours:

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timedelta
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.list(created_time_start=int((datetime.now() - timedelta(days=1)).timestamp() * 1000))

        """
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
        """`cancel a workflow execution. <https://api-docs.cognite.com/20230101/tag/Workflow-executions/operation/WorkflowExecutionCancellation>`_

        Args:
            id (str): The server-generated id of the workflow execution.
            reason (str | None): The reason for the cancellation, this will be put within the execution's `reasonForIncompletion` field. It is defaulted to 'cancelled' if not provided.

        Note:
            Cancelling a workflow will immediately cancel the `in_progress` tasks, but not their spawned work in
            other services (like transformations and functions).

        Returns:
            WorkflowExecution: The canceled workflow execution.

        Examples:

            Trigger a workflow execution for the workflow "foo", version 1 and cancel it:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.executions.run("foo", "1")
                >>> client.workflows.executions.cancel(id="foo", reason="test cancelation")
        """
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{id}/cancel",
            json={
                "reason": reason,
            }
            if reason
            else {},
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

    def upsert(self, version: WorkflowVersionUpsert, mode: Literal["replace"] = "replace") -> WorkflowVersion:
        """`Create a workflow version. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflowVersion>`_

        Note this is an upsert endpoint, so if a workflow with the same version external id already exists, it will be updated.

        Furthermore, if the workflow does not exist, it will be created.

        Args:
            version (WorkflowVersionUpsert): The workflow version to create or update.
            mode (Literal['replace']): This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            WorkflowVersion: The created workflow version.

        Examples:

            Create workflow version with one Function task:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionUpsert, WorkflowDefinitionUpsert, WorkflowTask, FunctionTaskParameters
                >>> client = CogniteClient()
                >>> new_version = WorkflowVersionUpsert(
                ...    workflow_external_id="my_workflow",
                ...    version="1",
                ...    workflow_definition=WorkflowDefinitionUpsert(
                ...        tasks=[
                ...            WorkflowTask(
                ...                external_id="my_workflow-task1",
                ...                parameters=FunctionTaskParameters(
                ...                    external_id="cdf_deployed_function:my_function",
                ...                    data={"a": 1, "b": 2},
                ...                ),
                ...            )
                ...        ],
                ...        description="This workflow has one step",
                ...    ),
                ... )
                >>> res = client.workflows.versions.upsert(new_version)
        """
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflow versions.")

        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [version.dump(camel_case=True)]},
        )

        return WorkflowVersion._load(response.json()["items"][0])

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

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client = CogniteClient()
                >>> client.workflows.versions.delete([WorkflowVersionId("my workflow", "1"), WorkflowVersionId("my workflow 2", "2")])

        """
        identifiers = WorkflowIds.load(workflow_version_id).dump(camel_case=True)
        self._delete_multiple(
            identifiers=WorkflowVersionIdentifierSequence.load(identifiers),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def retrieve(self, workflow_external_id: str, version: str) -> WorkflowVersion | None:
        """`Retrieve a workflow version. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/GetSpecificVersion>`_

        Args:
            workflow_external_id (str): External id of the workflow.
            version (str): Version of the workflow.

        Returns:
            WorkflowVersion | None: The requested workflow version if it exists, None otherwise.

        Examples:

            Retrieve workflow version 1 of workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.retrieve("my workflow", "1")
        """
        try:
            response = self._get(
                url_path=f"/workflows/{quote(workflow_external_id, '')}/versions/{quote(version, '')}",
            )
        except CogniteAPIError as e:
            if e.code == 404:
                return None
            raise e

        return WorkflowVersion._load(response.json())

    def list(
        self,
        workflow_version_ids: WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> WorkflowVersionList:
        """`List workflow versions in the project <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/ListWorkflowVersions>`_

        Args:
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowVersionList: The requested workflow versions.

        Examples:

            Get all workflow version for workflows 'my_workflow' and 'my_workflow_2':

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.list(["my_workflow", "my_workflow_2"])

            Get all workflow versions for workflows 'my_workflow' and 'my_workflow_2' using the WorkflowVersionId class:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.list([WorkflowVersionId("my_workflow"), WorkflowVersionId("my_workflow_2")])

            Get all workflow versions for workflows 'my_workflow' version '1' and 'my_workflow_2' version '2' using tuples:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.versions.list([("my_workflow", "1"), ("my_workflow_2", "2")])

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

    def upsert(self, workflow: WorkflowUpsert, mode: Literal["replace"] = "replace") -> Workflow:
        """`Create a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflow>`_

        Note this is an upsert endpoint, so if a workflow with the same external id already exists, it will be updated.

        Args:
            workflow (WorkflowUpsert): The workflow to create or update.
            mode (Literal['replace']): This is not an option for the API, but is included here to document that the upserts are always done in replace mode.

        Returns:
            Workflow: The created workflow.

        Examples:

            Create workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowUpsert
                >>> client = CogniteClient()
                >>> res = client.workflows.upsert(WorkflowUpsert(external_id="my workflow", description="my workflow description"))
        """
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflows.")

        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [workflow.dump(camel_case=True)]},
        )
        return Workflow._load(response.json()["items"][0])

    def retrieve(self, external_id: str) -> Workflow | None:
        """`Retrieve a workflow. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/CreateOrUpdateWorkflow>`_

        Args:
            external_id (str): Identifier for a Workflow. Must be unique for the project.

        Returns:
            Workflow | None: The requested workflow if it exists, None otherwise.

        Examples:

            Retrieve workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.retrieve("my workflow")
        """
        try:
            response = self._get(url_path=f"{self._RESOURCE_PATH}/{quote(external_id, '')}")
        except CogniteAPIError as e:
            if e.code == 404:
                return None
            raise e
        return Workflow._load(response.json())

    def delete(self, external_id: str | SequenceNotStr[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more workflows with versions. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflows/operation/DeleteWorkflows>`_

        Args:
            external_id (str | SequenceNotStr[str]): External id or list of external ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> client.workflows.delete("my workflow")
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def list(self, limit: int | None = DEFAULT_LIMIT_READ) -> WorkflowList:
        """`List all workflows in the project. <https://api-docs.cognite.com/20230101/tag/Workflow-versions/operation/FetchAllWorkflows>`_

        Args:
            limit (int | None): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None
        Returns:
            WorkflowList: All workflows in the CDF project.

        Examples:

            List all workflows:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.workflows.list()
        """
        return self._list(
            method="GET",
            resource_cls=Workflow,
            list_cls=WorkflowList,
            limit=limit,
        )
