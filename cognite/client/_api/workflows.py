from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any, Literal, MutableSequence, Sequence, Tuple, Union

from typing_extensions import TypeAlias

from cognite.client._api_client import APIClient
from cognite.client._constants import DEFAULT_LIMIT_READ
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowExecution,
    WorkflowExecutionDetailed,
    WorkflowExecutionList,
    WorkflowIds,
    WorkflowList,
    WorkflowTaskExecution,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionId,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._experimental import FeaturePreviewWarning
from cognite.client.utils._identifier import (
    IdentifierSequence,
    WorkflowVersionIdentifierSequence,
)
from cognite.client.utils._session import create_session_and_return_nonce

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class BetaWorkflowAPIClient(APIClient, ABC):
    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"
        self._warning = FeaturePreviewWarning(
            api_maturity="beta", sdk_maturity="alpha", feature_name="Workflow Orchestration"
        )


WorkflowIdentifier: TypeAlias = Union[WorkflowVersionId, Tuple[str, str], str]
WorkflowVersionIdentifier: TypeAlias = Union[WorkflowVersionId, Tuple[str, str]]


class WorkflowTaskAPI(BetaWorkflowAPIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    def update(
        self, task_id: str, status: Literal["completed", "failed"], output: dict | None = None
    ) -> WorkflowTaskExecution:
        """`Update status of async task. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Tasks/operation/UpdateTaskStatus>`_

        For tasks that has been marked with 'is_async = True', the status must be updated by calling this endpoint with either 'completed' or 'failed'.

        Args:
            task_id (str): The server-generated id of the task.
            status (Literal["completed", "failed"]): The new status of the task. Must be either 'completed' or 'failed'.
            output (dict | None): The output of the task. This will be available for tasks that has specified it as an output with the string "${<taskExternalId>.output}"

        Returns:
            WorkflowTaskExecution: The updated task execution.

        Examples:

            Update task with UUID '000560bc-9080-4286-b242-a27bb4819253' to status 'completed':

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "completed")

            Update task with UUID '000560bc-9080-4286-b242-a27bb4819253' to status 'failed' with output '{"a": 1, "b": 2}':

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.tasks.update("000560bc-9080-4286-b242-a27bb4819253", "failed", output={"a": 1, "b": 2})

            Trigger workflow, retrieve detailed task execution and update status of the second task (assumed to be async) to 'completed':

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.trigger("my workflow", "1")
                >>> res = c.workflows.executions.retrieve_detailed(res.id)
                >>> res = c.workflows.tasks.update(res.tasks[1].id, "completed")

        """
        self._warning.warn()

        body: dict[str, Any] = {"status": status.upper()}
        if output is not None:
            body["output"] = output
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/{task_id}/update",
            json=body,
        )
        return WorkflowTaskExecution._load(response.json())


class WorkflowExecutionAPI(BetaWorkflowAPIClient):
    _RESOURCE_PATH = "/workflows/executions"

    def retrieve_detailed(self, id: str) -> WorkflowExecutionDetailed | None:
        """`Retrieve a workflow execution with detailed information. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflow-Execution/operation/ExecutionOfSpecificRunOfWorkflow>`_

        Args:
            id (str): The server-generated id of the workflow execution.

        Returns:
            WorkflowExecutionDetailed | None: The requested workflow execution if it exists, None otherwise.

        Examples:

            Retrieve workflow execution with UUID '000560bc-9080-4286-b242-a27bb4819253':

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.retrieve_detailed("000560bc-9080-4286-b242-a27bb4819253")

            List workflow executions and retrieve detailed information for the first one:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.list()
                >>> res = c.workflows.executions.retrieve_detailed(res[0].id)

        """
        self._warning.warn()
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
    ) -> WorkflowExecution:
        """`Trigger a workflow execution. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflow-Execution/operation/TriggerRunOfSpecificVersionOfWorkflow>`_

        Args:
            workflow_external_id (str): External id of the workflow.
            version (str): Version of the workflow.
            input (dict | None): The input to the workflow execution. This will be available for tasks that have specified it as an input with the string "${workflow.input}" See tip below for more information.
            metadata (dict | None): Application specific metadata. Keys have a maximum length of 32 characters, values a maximum of 255, and there can be a maximum of 10 key-value pairs.

        Tip:
            The workflow input can be available in the workflow tasks. For example, if you have a Task with
            function parameters then you can specify it as follows

                >>> from cognite.client.data_classes import WorkflowTask, FunctionTaskParameters
                >>> task = WorkflowTask(
                ...     external_id="my_workflow-task1",
                ...     parameters=FunctionTaskParameters(
                ...         external_id="cdf_deployed_function:my_function",
                ...         data={"workflow_data": "${workflow.input}",}))

        Returns:
            WorkflowExecution: The created workflow execution.

        Examples:

            Trigger workflow execution for workflow my workflow version 1:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.trigger("my workflow", "1")

            Trigger workflow execution for workflow my workflow version 1 with input data '{"a": 1, "b": 2}:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.trigger("my workflow", "1", input={"a": 1, "b": 2})

        """
        self._warning.warn()
        nonce = create_session_and_return_nonce(self._cognite_client, api_name="Workflow API")
        body = {"authentication": {"nonce": nonce}}
        if input is not None:
            body["input"] = input
        if metadata is not None:
            body["metadata"] = metadata

        response = self._post(
            url_path=f"/workflows/{workflow_external_id}/versions/{version}/run",
            json=body,
        )
        return WorkflowExecution._load(response.json())

    def list(
        self,
        workflow_version_ids: WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> WorkflowExecutionList:
        """`List workflow executions in the project. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflow-Execution/operation/ListWorkflowExecutions>`_

        Args:
            workflow_version_ids (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            created_time_start (int | None): Filter out executions that was created before this time. Time is in milliseconds since epoch.
            created_time_end (int | None): Filter out executions that was created after this time. Time is in milliseconds since epoch.
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None
                        to return all items.

        Returns:
            WorkflowExecutionList: The requested workflow executions.

        Examples:

            Get all workflow executions for workflows 'my_workflow' version '1':

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.list(("my_workflow", "1"))

            Get all workflow executions for workflows after last 24 hours:

                >>> from cognite.client import CogniteClient
                >>> from datetime import datetime, timedelta
                >>> c = CogniteClient()
                >>> res = c.workflows.executions.list(created_time_start=int((datetime.now() - timedelta(days=1)).timestamp() * 1000))

        """
        self._warning.warn()
        filter_: dict[str, Any] = {}
        if workflow_version_ids is not None:
            filter_["workflowFilters"] = WorkflowIds._load(workflow_version_ids).dump(
                camel_case=True, as_external_id=True
            )
        if created_time_start is not None:
            filter_["createdTimeStart"] = created_time_start
        if created_time_end is not None:
            filter_["createdTimeEnd"] = created_time_end

        return self._list(
            method="POST",
            resource_cls=WorkflowExecution,
            list_cls=WorkflowExecutionList,
            filter=filter_,
            limit=limit,
        )


class WorkflowVersionAPI(BetaWorkflowAPIClient):
    _RESOURCE_PATH = "/workflows/versions"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._DELETE_LIMIT = 100

    def upsert(self, version: WorkflowVersionUpsert, mode: Literal["replace"] = "replace") -> WorkflowVersion:
        """`Create a workflow version. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflows/operation/CreateOrUpdateWorkflow>`_

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
                >>> c = CogniteClient()
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
                >>> res = c.workflows.upsert(new_version)
        """
        self._warning.warn()
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
        """`Delete a workflow version(s). <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflow-Version/operation/DeleteSpecificVersionsOfWorkflow>`_

        Args:
            workflow_version_id (WorkflowVersionIdentifier | MutableSequence[WorkflowVersionIdentifier]): Workflow version id or list of workflow version ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow version "1" of workflow "my workflow" specified by using a tuple:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.workflows.versions.delete(("my workflow", "1"))

            Delete workflow version "1" of workflow "my workflow" and workflow version "2" of workflow "my workflow 2" using the WorkflowVersionId class:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> c = CogniteClient()
                >>> c.workflows.versions.delete([WorkflowVersionId("my workflow", "1"), WorkflowVersionId("my workflow 2", "2")])

        """
        self._warning.warn()
        identifiers = WorkflowIds._load(workflow_version_id).dump(camel_case=True)
        self._delete_multiple(
            identifiers=WorkflowVersionIdentifierSequence.load(identifiers),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def retrieve(self, workflow_external_id: str, version: str) -> WorkflowVersion | None:
        """`Retrieve a workflow version. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflow-Version/operation/GetSpecificVersion>`_

        Args:
            workflow_external_id (str): External id of the workflow.
            version (str): Version of the workflow.

        Returns:
            WorkflowVersion | None: The requested workflow version if it exists, None otherwise.

        Examples:

            Retrieve workflow version 1 of workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.versions.retrieve("my workflow", "1")
        """
        self._warning.warn()
        try:
            response = self._get(
                url_path=f"/workflows/{workflow_external_id}/versions/{version}",
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
        """`List workflow versions in the project <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflow-Version/operation/ListWorkflowVersions>`_

        Args:
            workflow_version_ids (WorkflowIdentifier | MutableSequence[WorkflowIdentifier] | None): Workflow version id or list of workflow version ids to filter on.
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None

        Returns:
            WorkflowVersionList: The requested workflow versions.

        Examples:

            Get all workflow version for workflows 'my_workflow' and 'my_workflow_2':

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.versions.list(["my_workflow", "my_workflow_2"])

            Get all workflow versions for workflows 'my_workflow' and 'my_workflow_2' using the WorkflowVersionId class:

                >>> from cognite.client import CogniteClient
                >>> from cognite.client.data_classes import WorkflowVersionId
                >>> c = CogniteClient()
                >>> res = c.workflows.versions.list([WorkflowVersionId("my_workflow"), WorkflowVersionId("my_workflow_2")])

            Get all workflow versions for workflows 'my_workflow' version '1' and 'my_workflow_2' version '2' using tuples:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.versions.list([("my_workflow", "1"), ("my_workflow_2", "2")])

        """
        self._warning.warn()
        if workflow_version_ids is None:
            workflow_ids_dumped = []
        else:
            workflow_ids_dumped = WorkflowIds._load(workflow_version_ids).dump(camel_case=True, as_external_id=True)

        return self._list(
            method="POST",
            resource_cls=WorkflowVersion,
            list_cls=WorkflowVersionList,
            filter={"workflowFilters": workflow_ids_dumped},
            limit=limit,
        )


class WorkflowAPI(BetaWorkflowAPIClient):
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
        self._DELETE_LIMIT = 100

    def upsert(self, workflow: WorkflowUpsert, mode: Literal["replace"] = "replace") -> Workflow:
        """`Create a workflow. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflows/operation/CreateOrUpdateWorkflow>`_

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
                >>> c = CogniteClient()
                >>> res = c.workflows.upsert(WorkflowUpsert(external_id="my workflow", description="my workflow description"))
        """
        self._warning.warn()
        if mode != "replace":
            raise ValueError("Only replace mode is supported for upserting workflows.")

        response = self._post(
            url_path=self._RESOURCE_PATH,
            json={"items": [workflow.dump(camel_case=True)]},
        )
        return Workflow._load(response.json()["items"][0])

    def retrieve(self, external_id: str) -> Workflow | None:
        """`Retrieve a workflow. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflows/operation/CreateOrUpdateWorkflow>`_

        Args:
            external_id (str): Identifier for a Workflow. Must be unique for the project.

        Returns:
            Workflow | None: The requested workflow if it exists, None otherwise.

        Examples:

            Retrieve workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.retrieve("my workflow")
        """
        self._warning.warn()
        try:
            response = self._get(url_path=self._RESOURCE_PATH + f"/{external_id}")
        except CogniteAPIError as e:
            if e.code == 404:
                return None
            raise e
        return Workflow._load(response.json())

    def delete(self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False) -> None:
        """`Delete one or more workflows with versions. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflows/operation/DeleteWorkflows>`_

        Args:
            external_id (str | Sequence[str]): External id or list of external ids to delete.
            ignore_unknown_ids (bool): Ignore external ids that are not found rather than throw an exception.

        Examples:

            Delete workflow my workflow:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> c.workflows.delete("my workflow")
        """
        self._warning.warn()
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            params={"ignoreUnknownIds": ignore_unknown_ids},
            wrap_ids=True,
        )

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> WorkflowList:
        """`List all workflows in the project. <https://pr-2282.specs.preview.cogniteapp.com/20230101.json.html#tag/Workflows/operation/FetchAllWorkflows>`_

        Args:
            limit (int): Maximum number of results to return. Defaults to 25. Set to -1, float("inf") or None
        Returns:
            WorkflowList: All workflows in the CDF project.

        Examples:

            List all workflows:

                >>> from cognite.client import CogniteClient
                >>> c = CogniteClient()
                >>> res = c.workflows.list()
        """
        self._warning.warn()

        return self._list(
            method="GET",
            resource_cls=Workflow,
            list_cls=WorkflowList,
            limit=limit,
        )
