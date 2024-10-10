from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, cast

from typing_extensions import Self

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteResource,
    CogniteResourceList,
    ExternalIDTransformerMixin,
    UnknownCogniteObject,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.utils._text import convert_all_keys_to_camel_case, to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient

TaskStatus: TypeAlias = Literal[
    "in_progress",
    "cancelled",
    "failed",
    "failed_with_terminal_error",
    "completed",
    "completed_with_errors",
    "timed_out",
    "skipped",
]

WorkflowStatus: TypeAlias = Literal["completed", "failed", "running", "terminated", "timed_out"]


class WorkflowCore(WriteableCogniteResource["WorkflowUpsert"], ABC):
    def __init__(self, external_id: str, description: str | None) -> None:
        self.external_id = external_id
        self.description = description


class WorkflowUpsert(WorkflowCore):
    """
    This class represents a workflow. This is the write version, used when creating or updating a workflow.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        description (str | None): Description of the workflow. Note that when updating a workflow, the description will
                            always be overwritten also if it is set to None. Meaning if the wokflow already has a description,
                            and you want to keep it, you need to provide the description when updating the workflow.
    """

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(external_id=resource["externalId"], description=resource.get("description"))

    def as_write(self) -> WorkflowUpsert:
        """Returns this workflow instance."""
        return self


class Workflow(WorkflowCore):
    """
    This class represents a workflow. This is the reading version, used when reading or listing a workflows.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        created_time (int): The time when the workflow was created. Unix timestamp in milliseconds.
        description (str | None): Description of the workflow. Defaults to None.

    """

    def __init__(
        self,
        external_id: str,
        created_time: int,
        description: str | None = None,
    ) -> None:
        super().__init__(external_id, description)
        self.created_time = created_time

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            external_id=resource["externalId"],
            description=resource.get("description"),
            created_time=resource["createdTime"],
        )

    def as_write(self) -> WorkflowUpsert:
        """Returns this workflow in the writing format."""
        return WorkflowUpsert(
            external_id=self.external_id,
            description=self.description,
        )


class WorkflowUpsertList(CogniteResourceList[WorkflowUpsert], ExternalIDTransformerMixin):
    _RESOURCE = WorkflowUpsert


class WorkflowList(WriteableCogniteResourceList[WorkflowUpsert, Workflow], ExternalIDTransformerMixin):
    """This class represents a list of workflows."""

    _RESOURCE = Workflow

    def as_write(self) -> WorkflowUpsertList:
        """Returns these workflows in the writing format."""
        return WorkflowUpsertList([workflow.as_write() for workflow in self.data])


ValidTaskType = Literal["function", "transformation", "cdf", "dynamic", "subworkflow"]


class WorkflowTaskParameters(CogniteObject, ABC):
    task_type: ClassVar[ValidTaskType]

    @classmethod
    def load_parameters(cls, data: dict) -> WorkflowTaskParameters:
        type_ = data.get("type", data.get("taskType"))
        parameters = data.get("parameters", data.get("input"))
        if parameters is None:
            raise ValueError(
                "You must provide parameter data either with key "
                "'parameter' or 'input', with parameter taking precedence."
            )

        if type_ == "function":
            return FunctionTaskParameters._load(parameters)
        elif type_ == "transformation":
            return TransformationTaskParameters._load(parameters)
        elif type_ == "cdf":
            return CDFTaskParameters._load(parameters)
        elif type_ == "dynamic":
            return DynamicTaskParameters._load(parameters)
        elif type_ == "subworkflow" and "tasks" in parameters["subworkflow"]:
            return SubworkflowTaskParameters._load(parameters)
        elif type_ == "subworkflow" and "workflowExternalId" in parameters["subworkflow"]:
            return SubworkflowReferenceParameters._load(parameters)
        else:
            raise ValueError(f"Unknown task type: {type_}. Expected {ValidTaskType}")


class FunctionTaskParameters(WorkflowTaskParameters):
    """The function parameters are used to specify the Cognite Function to be called.

    Args:
        external_id (str): The external ID of the function to be called.
        data (dict | str | None): The data to be passed to the function. Defaults to None. The data can be used to specify the input to the function from previous tasks or the workflow input. See the tip below for more information.
        is_async_complete (bool | None): Whether the function is asynchronous. Defaults to None, which the API will interpret as False.

    If a function is asynchronous, you need to call the client.workflows.tasks.update() endpoint to update the status of the task.
    While synchronous tasks update the status automatically.

    Tip:
        You can dynamically specify data from other tasks or the workflow. You do this by following the format
        `${prefix.jsonPath}` in the expression. The valid are:

        - `${workflow.input}`: The workflow input.
        - `${<taskExternalId>.output}`: The output of the task with the given external id.
        - `${<taskExternalId>.input}`: The input of the task with the given external id.
        - `${<taskExternalId>.input.someKey}`: A specific key within the input of the task with the given external id.

        For example, if you have a workflow containing two tasks, and the external_id of the first task is `task1` then,
        you can specify the data for the second task as follows:

            >>> from cognite.client.data_classes  import WorkflowTask, FunctionTaskParameters
            >>> task = WorkflowTask(
            ...     external_id="task2",
            ...     parameters=FunctionTaskParameters(
            ...         external_id="cdf_deployed_function",
            ...         data={
            ...             "workflow_data": "${workflow.input}",
            ...             "task1_input": "${task1.input}",
            ...             "task1_output": "${task1.output}"
            ...         },
            ...     ),
            ... )
    """

    task_type = "function"

    def __init__(
        self,
        external_id: str,
        data: dict | str | None = None,
        is_async_complete: bool | None = None,
    ) -> None:
        self.external_id = external_id
        self.data = data
        self.is_async_complete = is_async_complete

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> FunctionTaskParameters:
        function: dict[str, Any] = resource["function"]

        return cls(
            external_id=function["externalId"],
            data=function.get("data"),
            # API uses isAsyncComplete and asyncComplete inconsistently:
            is_async_complete=resource.get("isAsyncComplete", resource.get("asyncComplete")),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        function: dict[str, Any] = {
            ("externalId" if camel_case else "external_id"): self.external_id,
        }
        if self.data:
            function["data"] = self.data

        output: dict[str, Any] = {
            "function": function,
        }
        if self.is_async_complete is not None:
            output[("isAsyncComplete" if camel_case else "is_async_complete")] = self.is_async_complete
        return output


class TransformationTaskParameters(WorkflowTaskParameters):
    """
    The transformation parameters are used to specify the transformation to be called.

    Args:
        external_id (str): The external ID of the transformation to be called.
        concurrency_policy (Literal['fail', 'restartAfterCurrent', 'waitForCurrent']): Determines the behavior of the task if the Transformation is already running. ``fail``: The task fails if another instance of the Transformation is currently running. ``waitForCurrent``: The task will pause and wait for the already running Transformation to complete. Once completed, the task is completed. This mode is useful for preventing redundant Transformation runs. ``restartAfterCurrent``: The task waits for the ongoing Transformation to finish. After completion, the task restarts the Transformation. This mode ensures that the most recent data can be used by following tasks.
    """

    task_type = "transformation"

    def __init__(
        self, external_id: str, concurrency_policy: Literal["fail", "restartAfterCurrent", "waitForCurrent"] = "fail"
    ) -> None:
        self.external_id = external_id
        self.concurrency_policy = concurrency_policy

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> TransformationTaskParameters:
        return cls(
            resource["transformation"]["externalId"],
            resource["transformation"]["concurrencyPolicy"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        transformation = {
            "externalId" if camel_case else "external_id": self.external_id,
            "concurrencyPolicy" if camel_case else "concurrency_policy": self.concurrency_policy,
        }

        return {"transformation": transformation}


class CDFTaskParameters(WorkflowTaskParameters):
    """
    The CDF request parameters are used to specify a request to the Cognite Data Fusion API.

    Args:
        resource_path (str): The resource path of the request. Note the path of the request which is prefixed by '{cluster}.cognitedata.com/api/v1/project/{project}' based on the cluster and project of the request.
        method (Literal['GET', 'POST', 'PUT', 'DELETE'] | str): The HTTP method of the request.
        query_parameters (dict | str | None): The query parameters of the request. Defaults to None.
        body (dict | str | None): The body of the request. Defaults to None. Limited to 1024KiB in size
        request_timeout_in_millis (int | str): The timeout of the request in milliseconds. Defaults to 10000.

    Examples:

        Call the asset/list endpoint with a limit of 10:

            >>> from cognite.client.data_classes import WorkflowTask, CDFTaskParameters
            >>> task = WorkflowTask(
            ...     external_id="task1",
            ...     parameters=CDFTaskParameters(
            ...         resource_path="/assets/list",
            ...         method="GET",
            ...         query_parameters={"limit": 10},
            ...     ),
            ... )

    """

    task_type = "cdf"

    def __init__(
        self,
        resource_path: str,
        method: Literal["GET", "POST", "PUT", "DELETE"] | str,
        query_parameters: dict | str | None = None,
        body: dict | str | None = None,
        request_timeout_in_millis: int | str = 10000,
    ) -> None:
        self.resource_path = resource_path
        self.method = method
        self.query_parameters = query_parameters or {}
        self.body = body or {}
        self.request_timeout_in_millis = request_timeout_in_millis

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        cdf_request: dict[str, Any] = resource["cdfRequest"]

        return cls(
            cdf_request["resourcePath"],
            cdf_request["method"],
            cdf_request.get("queryParameters"),
            cdf_request.get("body"),
            cdf_request.get("requestTimeoutInMillis", 10000),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        return {
            ("cdfRequest" if camel_case else "cdf_request"): output,
        }


class SubworkflowTaskParameters(WorkflowTaskParameters):
    """
    The subworkflow task parameters are used to specify a subworkflow task.

    When a workflow is made of stages with dependencies between them, we can use subworkflow tasks for convenience. It takes the tasks parameter which is an array of
    function, transformation, cdf, ..., task definitions. This array needs to be statically set on the workflow definition (if it needs to be defined at runtime, use a
    dynamic task).

    Args:
        tasks (list[WorkflowTask]): The tasks belonging to the subworkflow.
    """

    task_type = "subworkflow"

    def __init__(self, tasks: list[WorkflowTask]) -> None:
        self.tasks = tasks

    @classmethod
    def _load(cls: type[Self], resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        subworkflow: dict[str, Any] = resource[cls.task_type]

        return cls(
            [WorkflowTask._load(task) for task in subworkflow["tasks"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {self.task_type: {"tasks": [task.dump(camel_case) for task in self.tasks]}}


class SubworkflowReferenceParameters(WorkflowTaskParameters):
    """
    The subworkflow task parameters are used to specify a subworkflow task.
    When a workflow is made of stages with dependencies between them, we can use subworkflow tasks for convenience.
    The subworkflow reference is used to specifying a reference to another workflow which will be embedded into the execution at start time.

    Args:
        workflow_external_id (str): The external ID of the referenced workflow.
        version (str): The version of the referenced workflow.
    """

    task_type = "subworkflow"

    def __init__(self, workflow_external_id: str, version: str) -> None:
        self.workflow_external_id = workflow_external_id
        self.version = version

    @classmethod
    def _load(cls: type[Self], resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        subworkflow: dict[str, Any] = resource[cls.task_type]

        return cls(workflow_external_id=subworkflow["workflowExternalId"], version=subworkflow["version"])

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            self.task_type: {
                "workflowExternalId": self.workflow_external_id,
                "version": self.version,
            }
        }


class DynamicTaskParameters(WorkflowTaskParameters):
    """
    The dynamic task parameters are used to specify a dynamic task.

    When the tasks and their order of execution are determined at runtime, we use dynamic tasks. It takes the tasks parameter which is a Reference to
    an array of function, transformation, and cdf task definitions. This array should be generated and returned by a previous step in the workflow, for instance,
    a Cognite Function task.

    Tip:
        You can reference data from other tasks or the workflow. You do this by following the format
        `${prefix.jsonPath}` in the expression. Some valid option are:

        - `${workflow.input}`: The workflow input.
        - `${<taskExternalId>.output}`: The output of the task with the given external id.
        - `${<taskExternalId>.input}`: The input of the task with the given external id.
        - `${<taskExternalId>.input.someKey}`: A specific key within the input of the task with the given external id.

    Args:
        tasks (list[WorkflowTask] | str): The tasks to be dynamically executed. The dynamic task is a string that is evaluated
                    during the workflow's execution. When calling Version Upsert, the tasks parameter must be a Reference string.
                    When calling Execution details, the tasks parameter will be a list of WorkflowTask objects.
    """

    task_type = "dynamic"

    def __init__(self, tasks: list[WorkflowTask] | str) -> None:
        self.tasks = tasks

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        dynamic: dict[str, Any] = resource[cls.task_type]

        # can either be a reference string (i.e., in case of WorkflowDefinitions)
        if isinstance(dynamic["tasks"], str):
            return cls(dynamic["tasks"])

        # or can be resolved to a list of Tasks (i.e., during or after execution)
        return cls(
            [WorkflowTask._load(task) for task in dynamic["tasks"]],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            self.task_type: {
                "tasks": self.tasks if isinstance(self.tasks, str) else [task.dump(camel_case) for task in self.tasks]
            }
        }


class WorkflowTask(CogniteResource):
    """
    This class represents a workflow task.

    Note:
        Tasks do not distinguish between write and read versions.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        parameters (WorkflowTaskParameters): The parameters of the task.
        name (str | None): The name of the task. Defaults to None.
        description (str | None): The description of the task. Defaults to None.
        retries (int): The number of retries for the task. Defaults to 3.
        timeout (int): The timeout of the task in seconds. Defaults to 3600.
        on_failure (Literal['abortWorkflow', 'skipTask']): The policy to handle failures and timeouts. Defaults to *abortWorkflow*. ``skipTask``: For both failures and timeouts, the task will retry until the retries are exhausted. After that, the Task is marked as COMPLETED_WITH_ERRORS and the subsequent tasks are executed. ``abortWorkflow``: In case of failures, retries will be performed until exhausted. After which the task is marked as FAILED and the Workflow is marked the same. In the event of a timeout, no retries are undertaken; the task is marked as TIMED_OUT and the Workflow is marked as FAILED.
        depends_on (list[str] | None): The external ids of the tasks that this task depends on. Defaults to None.
    """

    def __init__(
        self,
        external_id: str,
        parameters: WorkflowTaskParameters,
        name: str | None = None,
        description: str | None = None,
        retries: int = 3,
        timeout: int = 3600,
        on_failure: Literal["abortWorkflow", "skipTask"] = "abortWorkflow",
        depends_on: list[str] | None = None,
    ) -> None:
        self.external_id = external_id
        self.parameters = parameters
        self.name = name
        self.description = description
        self.retries = retries
        self.timeout = timeout
        self.on_failure = on_failure
        self.depends_on = depends_on

    @property
    def type(self) -> ValidTaskType:
        return self.parameters.task_type

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowTask:
        return cls(
            external_id=resource["externalId"],
            parameters=WorkflowTaskParameters.load_parameters(resource),
            name=resource.get("name"),
            description=resource.get("description"),
            # Allow default to come from the API.
            retries=resource.get("retries"),  # type: ignore[arg-type]
            timeout=resource.get("timeout"),  # type: ignore[arg-type]
            on_failure=resource.get("onFailure"),  # type: ignore[arg-type]
            depends_on=[dep["externalId"] for dep in resource.get("dependsOn", [])] or None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "externalId" if camel_case else "external_id": self.external_id,
            "type": self.type,
            "parameters": self.parameters.dump(camel_case),
            "retries": self.retries,
            "timeout": self.timeout,
            "onFailure" if camel_case else "on_failure": self.on_failure,
        }
        if self.name:
            output["name"] = self.name
        if self.description:
            output["description"] = self.description
        if self.depends_on:
            output[("dependsOn" if camel_case else "depends_on")] = [
                {("externalId" if camel_case else "external_id"): dependency} for dependency in self.depends_on
            ]
        return output


class WorkflowTaskOutput(ABC):
    task_type: ClassVar[str]

    @classmethod
    @abstractmethod
    def load(cls, data: dict) -> Self:
        raise NotImplementedError

    @classmethod
    def load_output(cls, data: dict) -> WorkflowTaskOutput:
        task_type = data["taskType"]
        if task_type == "function":
            return FunctionTaskOutput.load(data)
        elif task_type == "transformation":
            return TransformationTaskOutput.load(data)
        elif task_type == "cdf":
            return CDFTaskOutput.load(data)
        elif task_type == "dynamic":
            return DynamicTaskOutput.load(data)
        elif task_type == "subworkflow":
            return SubworkflowTaskOutput.load(data)
        else:
            raise ValueError(f"Unknown task type: {task_type}")

    @abstractmethod
    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        raise NotImplementedError


class FunctionTaskOutput(WorkflowTaskOutput):
    """
    The class represent the output of Cognite Function task.

    Args:
        call_id (int | None): The call_id of the CDF Function call.
        function_id (int | None): The function_id of the CDF Function.
        response (dict | None): The response of the CDF Function call.

    """

    task_type: ClassVar[str] = "function"

    def __init__(self, call_id: int | None, function_id: int | None, response: dict | None) -> None:
        self.call_id = call_id
        self.function_id = function_id
        self.response = response

    @classmethod
    def load(cls, data: dict[str, Any]) -> FunctionTaskOutput:
        output = data["output"]
        return cls(output.get("callId"), output.get("functionId"), output.get("response"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "callId" if camel_case else "call_id": self.call_id,
            "functionId" if camel_case else "function_id": self.function_id,
            "response": self.response,
        }


class TransformationTaskOutput(WorkflowTaskOutput):
    """
    The transformation output is used to specify the output of a transformation task.

    Args:
        job_id (int | None): The job id of the transformation job.
    """

    task_type: ClassVar[str] = "transformation"

    def __init__(self, job_id: int | None) -> None:
        self.job_id = job_id

    @classmethod
    def load(cls, data: dict[str, Any]) -> TransformationTaskOutput:
        output = data["output"]
        return cls(output.get("jobId"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {("jobId" if camel_case else "job_id"): self.job_id}


class CDFTaskOutput(WorkflowTaskOutput):
    """
    The CDF Request output is used to specify the output of a CDF Request.

    Args:
        response (str | dict | None): The response of the CDF Request. Will be a JSON object if content-type is application/json, otherwise will be a string.
        status_code (int | None): The status code of the CDF Request.
    """

    task_type: ClassVar[str] = "cdf"

    def __init__(self, response: str | dict | None, status_code: int | None) -> None:
        self.response = response
        self.status_code = status_code

    @classmethod
    def load(cls, data: dict[str, Any]) -> CDFTaskOutput:
        output = data["output"]
        return cls(output.get("response"), output.get("statusCode"))

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "response": self.response,
            ("statusCode" if camel_case else "status_code"): self.status_code,
        }


class DynamicTaskOutput(WorkflowTaskOutput):
    """
    The dynamic task output is used to specify the output of a dynamic task.
    """

    task_type: ClassVar[str] = "dynamic"

    def __init__(self) -> None: ...

    @classmethod
    def load(cls, data: dict[str, Any]) -> DynamicTaskOutput:
        return cls()

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {}


class SubworkflowTaskOutput(WorkflowTaskOutput):
    """
    The subworkflow task output is used to specify the output of a subworkflow task.
    """

    task_type: ClassVar[str] = "subworkflow"

    def __init__(self) -> None: ...

    @classmethod
    def load(cls, data: dict[str, Any]) -> SubworkflowTaskOutput:
        return cls()

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {}


class WorkflowTaskExecution(CogniteObject):
    """
    This class represents a task execution.

    Args:
        id (str): The server generated id of the task execution.
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        status (TaskStatus): The status of the task execution.
        input (WorkflowTaskParameters): The input parameters of the task execution.
        output (WorkflowTaskOutput): The output of the task execution.
        version (str | None): The version of the task execution. Defaults to None.
        start_time (int | None): The start time of the task execution. Unix timestamp in milliseconds. Defaults to None.
        end_time (int | None): The end time of the task execution. Unix timestamp in milliseconds. Defaults to None.
        reason_for_incompletion (str | None): Provides the reason if the workflow did not complete successfully. Defaults to None.
    """

    def __init__(
        self,
        id: str,
        external_id: str,
        status: TaskStatus,
        input: WorkflowTaskParameters,
        output: WorkflowTaskOutput,
        version: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
    ) -> None:
        self.id = id
        self.external_id = external_id
        self.status = status
        self.input = input
        self.output = output
        self.version = version
        self.start_time = start_time
        self.end_time = end_time
        self.reason_for_incompletion = reason_for_incompletion

    @property
    def task_type(self) -> ValidTaskType:
        return self.input.task_type

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowTaskExecution:
        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            status=cast(TaskStatus, to_snake_case(resource["status"])),
            input=WorkflowTaskParameters.load_parameters(resource),
            output=WorkflowTaskOutput.load_output(resource),
            version=resource.get("version"),
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            reason_for_incompletion=resource.get("reasonForIncompletion"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        output["input"] = self.input.dump(camel_case)
        output["status"] = self.status.upper()
        output[("taskType" if camel_case else "task_type")] = self.task_type
        # API uses isAsyncComplete and asyncComplete inconsistently:
        if self.task_type == "function":
            if (is_async_complete := output["input"].get("isAsyncComplete")) is not None:
                output["input"]["asyncComplete"] = is_async_complete
                del output["input"]["isAsyncComplete"]

        output["output"] = self.output.dump(camel_case)
        return output


class WorkflowDefinitionCore(WriteableCogniteResource["WorkflowDefinitionUpsert"], ABC):
    """This class represents a workflow definition.

    A workflow definition defines the tasks and order/dependencies of these tasks.

    Args:
        tasks (list[WorkflowTask]): The tasks of the workflow definition.
        description (str | None): The description of the workflow definition. Note that when updating a workflow definition
                            description, it will always be overwritten also if it is set to None. Meaning if the
                            wokflow definition already has a description, and you want to keep it, you need to provide
                            the description when updating it.
    """

    def __init__(
        self,
        tasks: list[WorkflowTask],
        description: str | None,
    ) -> None:
        self.tasks = tasks
        self.description = description

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        return cls(
            tasks=[WorkflowTask._load(task) for task in resource["tasks"]],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"tasks": [task.dump(camel_case) for task in self.tasks]}
        if self.description:
            output["description"] = self.description
        return output


class WorkflowDefinitionUpsert(WorkflowDefinitionCore):
    """
    This class represents a workflow definition. This represents the write/update version of a workflow definiton.

    A workflow definition defines the tasks and order/dependencies of these tasks.

    Args:
        tasks (list[WorkflowTask]): The tasks of the workflow definition.
        description (str | None): The description of the workflow definition. Note that when updating a workflow definition
                            description, it will always be overwritten also if it is set to None. Meaning if the
                            wokflow definition already has a description, and you want to keep it, you need to provide
                            the description when updating it.
    """

    def __init__(
        self,
        tasks: list[WorkflowTask],
        description: str | None,
    ) -> None:
        super().__init__(tasks, description)

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowDefinitionUpsert:
        return cls(
            tasks=[WorkflowTask._load(task) for task in resource["tasks"]],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {"tasks": [task.dump(camel_case) for task in self.tasks]}
        if self.description:
            output["description"] = self.description
        return output

    def as_write(self) -> WorkflowDefinitionUpsert:
        """Returns this WorkflowDefinitionUpsert in its write format."""
        return self


class WorkflowDefinition(WorkflowDefinitionCore):
    """
    This class represents a workflow definition. This represents the read version of a workflow definiton.

    A workflow definition defines the tasks and order/dependencies of these tasks.

    Args:
        hash_ (str): The hash of the tasks and description. This is used to uniquely identify the workflow definition as you can overwrite a workflow version.
        tasks (list[WorkflowTask]): The tasks of the workflow definition.
        description (str | None): The description of the workflow definition. Defaults to None.
    """

    def __init__(
        self,
        hash_: str,
        tasks: list[WorkflowTask],
        description: str | None = None,
    ) -> None:
        super().__init__(tasks, description)
        self.hash_ = hash_

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowDefinition:
        return cls(
            hash_=resource["hash"],
            tasks=[WorkflowTask._load(task) for task in resource["tasks"]],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["hash"] = self.hash_
        return output

    def as_write(self) -> WorkflowDefinitionUpsert:
        """Returns this WorkflowDefinition in its write format."""
        return WorkflowDefinitionUpsert(
            tasks=self.tasks,
            description=self.description,
        )


class WorkflowVersionCore(WriteableCogniteResource["WorkflowVersionUpsert"], ABC):
    """
    This class represents a workflow version.

    Args:
        workflow_external_id (str): The external ID of the workflow.
        version (str): The version of the workflow.
    """

    def __init__(
        self,
        workflow_external_id: str,
        version: str,
    ) -> None:
        self.workflow_external_id = workflow_external_id
        self.version = version

    def as_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(
            workflow_external_id=self.workflow_external_id,
            version=self.version,
        )


class WorkflowVersionUpsert(WorkflowVersionCore):
    """
    This class represents a workflow version. This is the write-variant, used when creating or updating a workflow variant.

    Args:
        workflow_external_id (str): The external ID of the workflow.
        version (str): The version of the workflow.
        workflow_definition (WorkflowDefinitionUpsert): The workflow definition of the workflow version.

    """

    def __init__(self, workflow_external_id: str, version: str, workflow_definition: WorkflowDefinitionUpsert) -> None:
        super().__init__(
            workflow_external_id=workflow_external_id,
            version=version,
        )
        self.workflow_definition = workflow_definition

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        workflow_definition: dict[str, Any] = resource["workflowDefinition"]
        return cls(
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
            workflow_definition=WorkflowDefinitionUpsert._load(workflow_definition),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            ("workflowExternalId" if camel_case else "workflow_external_id"): self.workflow_external_id,
            "version": self.version,
            ("workflowDefinition" if camel_case else "workflow_definition"): self.workflow_definition.dump(camel_case),
        }

    def as_write(self) -> WorkflowVersionUpsert:
        """Returns this WorkflowVersionUpsert instance."""
        return self


class WorkflowVersion(WorkflowVersionCore):
    """
    This class represents a workflow version. This is the read variant, used when retrieving/listing a workflow variant.

    Args:
        workflow_external_id (str): The external ID of the workflow.
        version (str): The version of the workflow.
        workflow_definition (WorkflowDefinition): The workflow definition of the workflow version.
    """

    def __init__(
        self,
        workflow_external_id: str,
        version: str,
        workflow_definition: WorkflowDefinition,
    ) -> None:
        super().__init__(
            workflow_external_id=workflow_external_id,
            version=version,
        )
        self.workflow_definition = workflow_definition

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowVersion:
        workflow_definition: dict[str, Any] = resource["workflowDefinition"]
        return cls(
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
            workflow_definition=WorkflowDefinition._load(workflow_definition),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            ("workflowExternalId" if camel_case else "workflow_external_id"): self.workflow_external_id,
            "version": self.version,
            ("workflowDefinition" if camel_case else "workflow_definition"): self.workflow_definition.dump(camel_case),
        }

    def as_write(self) -> WorkflowVersionUpsert:
        """Returns a WorkflowVersionUpsert object with the same data."""
        return WorkflowVersionUpsert(
            workflow_external_id=self.workflow_external_id,
            version=self.version,
            workflow_definition=self.workflow_definition.as_write(),
        )


class WorkflowVersionUpsertList(CogniteResourceList[WorkflowVersionUpsert]):
    """This class represents a list of workflow versions."""

    _RESOURCE = WorkflowVersionUpsert

    def as_ids(self) -> WorkflowIds:
        """Returns a WorkflowIds object with the workflow version ids."""
        return WorkflowIds([workflow_version.as_id() for workflow_version in self.data])


class WorkflowVersionList(WriteableCogniteResourceList[WorkflowVersionUpsert, WorkflowVersion]):
    """
    This class represents a list of workflow versions.
    """

    _RESOURCE = WorkflowVersion

    def as_ids(self) -> WorkflowIds:
        """Returns a WorkflowIds object with the workflow version ids."""
        return WorkflowIds([workflow_version.as_id() for workflow_version in self.data])

    def as_write(self) -> WorkflowVersionUpsertList:
        """Returns a WorkflowVersionUpsertList object with the same data."""
        return WorkflowVersionUpsertList([workflow_version.as_write() for workflow_version in self.data])


class WorkflowExecution(CogniteResource):
    """
    This class represents a workflow execution.

    Args:
        id (str): The server generated id of the workflow execution.
        workflow_external_id (str): The external ID of the workflow.
        status (WorkflowStatus): The status of the workflow execution.
        created_time (int): The time when the workflow execution was created. Unix timestamp in milliseconds.
        version (str | None): The version of the workflow. Defaults to None.
        start_time (int | None): The start time of the workflow execution. Unix timestamp in milliseconds. Defaults to None.
        end_time (int | None): The end time of the workflow execution. Unix timestamp in milliseconds. Defaults to None.
        reason_for_incompletion (str | None): Provides the reason if the workflow did not complete successfully. Defaults to None.
        metadata (dict | None): Application specific metadata.
    """

    def __init__(
        self,
        id: str,
        workflow_external_id: str,
        status: WorkflowStatus,
        created_time: int,
        version: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
        metadata: dict | None = None,
    ) -> None:
        self.id = id
        self.workflow_external_id = workflow_external_id
        self.version = version
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.end_time = end_time
        self.reason_for_incompletion = reason_for_incompletion
        self.metadata = metadata

    def as_workflow_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(
            workflow_external_id=self.workflow_external_id,
            version=self.version,
        )

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowExecution:
        return cls(
            id=resource["id"],
            workflow_external_id=resource["workflowExternalId"],
            version=resource.get("version"),
            status=cast(
                WorkflowStatus,
                to_snake_case(resource["status"]),
            ),
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            reason_for_incompletion=resource.get("reasonForIncompletion"),
            metadata=resource.get("metadata"),
        )


class WorkflowExecutionList(CogniteResourceList[WorkflowExecution]):
    """
    This class represents a list of workflow executions.
    """

    _RESOURCE = WorkflowExecution


class WorkflowExecutionDetailed(WorkflowExecution):
    """
    This class represents a detailed workflow execution.

    A detailed workflow execution contains the input and output of each task in the workflow execution. In addition,
    it contains the workflow definition of the workflow.

    Args:
        id (str): The server generated id of the workflow execution.
        workflow_external_id (str): The external ID of the workflow.
        workflow_definition (WorkflowDefinition): The workflow definition of the workflow.
        status (WorkflowStatus): The status of the workflow execution.
        executed_tasks (list[WorkflowTaskExecution]): The executed tasks of the workflow execution.
        created_time (int): The time when the workflow execution was created. Unix timestamp in milliseconds.
        version (str | None): The version of the workflow. Defaults to None.
        start_time (int | None): The start time of the workflow execution. Unix timestamp in milliseconds. Defaults to None.
        end_time (int | None): The end time of the workflow execution. Unix timestamp in milliseconds. Defaults to None.
        reason_for_incompletion (str | None): Provides the reason if the workflow did not complete successfully. Defaults to None.
        input (dict | None): Input arguments the workflow was triggered with.
        metadata (dict | None): Metadata set when the workflow was triggered.
    """

    def __init__(
        self,
        id: str,
        workflow_external_id: str,
        workflow_definition: WorkflowDefinition,
        status: WorkflowStatus,
        executed_tasks: list[WorkflowTaskExecution],
        created_time: int,
        version: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
        input: dict | None = None,
        metadata: dict | None = None,
    ) -> None:
        super().__init__(
            id, workflow_external_id, status, created_time, version, start_time, end_time, reason_for_incompletion
        )
        self.workflow_definition = workflow_definition
        self.executed_tasks = executed_tasks
        self.input = input
        self.metadata = metadata

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowExecutionDetailed:
        return cls(
            id=resource["id"],
            workflow_external_id=resource["workflowExternalId"],
            version=resource.get("version"),
            status=cast(
                WorkflowStatus,
                to_snake_case(resource["status"]),
            ),
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            reason_for_incompletion=resource.get("reasonForIncompletion"),
            workflow_definition=WorkflowDefinition._load(resource["workflowDefinition"]),
            executed_tasks=[WorkflowTaskExecution.load(task) for task in resource["executedTasks"]],
            input=resource.get("input"),
            metadata=resource.get("metadata"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output = super().dump(camel_case)
        output[("workflowDefinition" if camel_case else "workflow_definition")] = self.workflow_definition.dump(
            camel_case
        )
        output[("executedTasks" if camel_case else "executed_tasks")] = [
            task.dump(camel_case) for task in self.executed_tasks
        ]
        if self.input:
            output["input"] = self.input
        if self.metadata:
            output["metadata"] = self.metadata
        return output

    def as_execution(self) -> WorkflowExecution:
        return WorkflowExecution(
            id=self.id,
            workflow_external_id=self.workflow_external_id,
            version=self.version,
            status=self.status,
            created_time=self.created_time,
            start_time=self.start_time,
            end_time=self.end_time,
            reason_for_incompletion=self.reason_for_incompletion,
            metadata=self.metadata,
        )


@dataclass(frozen=True)
class WorkflowVersionId:
    """
    This class represents a Workflow Version Identifier.

    Args:
        workflow_external_id (str): The external ID of the workflow.
        version (str, optional): The version of the workflow. Defaults to None.
    """

    workflow_external_id: str
    version: str | None = None

    def as_primitive(self) -> tuple[str, str | None]:
        return self.workflow_external_id, self.version

    @classmethod
    def load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowVersionId:
        if "workflowExternalId" in resource:
            workflow_external_id = resource["workflowExternalId"]
        elif "externalId" in resource:
            workflow_external_id = resource["externalId"]
        else:
            raise ValueError("Invalid input to WorkflowVersionId.load")

        return cls(
            workflow_external_id=workflow_external_id,
            version=resource.get("version"),
        )

    def dump(self, camel_case: bool = True, as_external_id_key: bool = False) -> dict[str, Any]:
        if as_external_id_key:
            output: dict[str, Any] = {("externalId" if camel_case else "external_id"): self.workflow_external_id}
        else:
            output = {("workflowExternalId" if camel_case else "workflow_external_id"): self.workflow_external_id}
        if self.version:
            output["version"] = self.version
        return output


class WorkflowIds(UserList):
    """
    This class represents a list of Workflow Version Identifiers.
    """

    def __init__(self, workflow_ids: Collection[WorkflowVersionId]) -> None:
        for workflow_id in workflow_ids:
            if not isinstance(workflow_id, WorkflowVersionId):
                raise TypeError(
                    f"All resources for class '{type(self).__name__}' must be of type "
                    f"'{type(WorkflowVersionId).__name__}', not '{type(workflow_id)}'."
                )
        super().__init__(workflow_ids)

    @classmethod
    def load(cls, resource: Any, cognite_client: CogniteClient | None = None) -> WorkflowIds:
        workflow_ids: Sequence[WorkflowVersionId]
        if isinstance(resource, tuple) and len(resource) == 2 and all(isinstance(x, str) for x in resource):
            workflow_ids = [WorkflowVersionId(*resource)]
        elif isinstance(resource, WorkflowVersionId):
            workflow_ids = [resource]
        elif isinstance(resource, str):
            workflow_ids = [WorkflowVersionId(workflow_external_id=resource)]
        elif isinstance(resource, dict):
            workflow_ids = [WorkflowVersionId.load(resource)]
        elif isinstance(resource, Sequence) and resource and isinstance(resource[0], tuple):
            workflow_ids = [WorkflowVersionId(*x) for x in resource]
        elif isinstance(resource, Sequence) and resource and isinstance(resource[0], WorkflowVersionId):
            workflow_ids = resource
        elif isinstance(resource, Sequence) and resource and isinstance(resource[0], str):
            workflow_ids = [WorkflowVersionId(workflow_external_id=x) for x in resource]
        else:
            raise ValueError("Invalid input to WorkflowIds")
        return cls(workflow_ids)

    def dump(self, camel_case: bool = True, as_external_id: bool = False) -> list[dict[str, Any]]:
        return [workflow_id.dump(camel_case, as_external_id_key=as_external_id) for workflow_id in self.data]


class WorkflowTriggerRule(CogniteObject, ABC):
    """This is the base class for all workflow trigger rules."""

    _trigger_type: ClassVar[str]

    def __init__(self) -> None:
        self.trigger_type = self._trigger_type

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> Self:
        trigger_type = resource["triggerType"]
        if trigger_type in _TRIGGER_RULE_BY_TYPE:
            return cast(Self, _TRIGGER_RULE_BY_TYPE[trigger_type]._load_trigger(resource))
        # If more triggers are added in the future, this ensures that the SDK does not break.
        return cast(Self, UnknownCogniteObject(resource))

    @classmethod
    @abstractmethod
    def _load_trigger(cls, data: dict) -> Self:
        raise NotImplementedError()


class WorkflowScheduledTriggerRule(WorkflowTriggerRule):
    """
    This class represents a scheduled trigger rule.

    Args:
        cron_expression(str | None): The cron specification for the scheduled trigger.
    """

    _trigger_type = "schedule"

    def __init__(self, cron_expression: str | None = None) -> None:
        super().__init__()
        self.cron_expression = cron_expression

    @classmethod
    def _load_trigger(cls, data: dict) -> WorkflowScheduledTriggerRule:
        return cls(cron_expression=data.get("cronExpression"))


_TRIGGER_RULE_BY_TYPE: dict[str, type[WorkflowTriggerRule]] = {
    subclass._trigger_type: subclass  # type: ignore
    for subclass in WorkflowTriggerRule.__subclasses__()
}


class WorkflowTriggerCore(WriteableCogniteResource["WorkflowTriggerUpsert"], ABC):
    """
    This class represents a base class for a workflow trigger.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        trigger_rule (WorkflowTriggerRule): The trigger rule of the workflow version trigger.
        workflow_external_id (str): The external ID of the workflow.
        workflow_version (str): The version of the workflow.
        input (dict | None): The input data of the workflow version trigger. Defaults to None.
    """

    def __init__(
        self,
        external_id: str,
        trigger_rule: WorkflowTriggerRule,
        workflow_external_id: str,
        workflow_version: str,
        input: dict | None = None,
    ) -> None:
        self.external_id = external_id
        self.trigger_rule = trigger_rule
        self.workflow_external_id = workflow_external_id
        self.workflow_version = workflow_version
        self.input = input


class WorkflowTriggerUpsert(WorkflowTriggerCore):
    """
    This class represents a workflow trigger for upsertion.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        trigger_rule (WorkflowTriggerRule): The trigger rule of the workflow version trigger.
        workflow_external_id (str): The external ID of the workflow.
        workflow_version (str): The version of the workflow.
        input (dict | None): The input data of the workflow version trigger. Defaults to None.
    """

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        item: dict[str, Any] = {
            "external_id": self.external_id,
            "trigger_rule": self.trigger_rule.dump(camel_case=camel_case),
            "workflow_external_id": self.workflow_external_id,
            "workflow_version": self.workflow_version,
        }
        if self.input:
            item["input"] = self.input
        if camel_case:
            return convert_all_keys_to_camel_case(item)
        return item

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowTriggerUpsert:
        return cls(
            external_id=resource["externalId"],
            workflow_external_id=resource["workflowExternalId"],
            workflow_version=resource["workflowVersion"],
            trigger_rule=WorkflowTriggerRule._load(resource["triggerRule"]),
            input=resource.get("input"),
        )

    def as_write(self) -> WorkflowTriggerUpsert:
        """Returns this workflow trigger create instance."""
        return self


WorkflowTriggerCreate = WorkflowTriggerUpsert


class WorkflowTrigger(WorkflowTriggerCore):
    """
    This class represents a workflow trigger.

    Args:
        external_id (str): The external ID provided by the client. Must be unique for the resource type.
        trigger_rule (WorkflowTriggerRule): The trigger rule of the workflow version trigger.
        workflow_external_id (str): The external ID of the workflow.
        workflow_version (str): The version of the workflow.
        input (dict | None): The input data passed to the workflow when an execution is started. Defaults to None.
        created_time (int | None): The time when the workflow version trigger was created. Unix timestamp in milliseconds. Defaults to None.
        last_updated_time (int | None): The time when the workflow version trigger was last updated. Unix timestamp in milliseconds. Defaults to None.
    """

    def __init__(
        self,
        external_id: str,
        trigger_rule: WorkflowTriggerRule,
        workflow_external_id: str,
        workflow_version: str,
        input: dict | None = None,
        created_time: int | None = None,
        last_updated_time: int | None = None,
    ) -> None:
        super().__init__(
            external_id=external_id,
            trigger_rule=trigger_rule,
            workflow_external_id=workflow_external_id,
            workflow_version=workflow_version,
            input=input,
        )
        self.created_time = created_time
        self.last_updated_time = last_updated_time

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        item: dict[str, Any] = {
            "external_id": self.external_id,
            "trigger_rule": self.trigger_rule.dump(camel_case=camel_case),
            "workflow_external_id": self.workflow_external_id,
            "workflow_version": self.workflow_version,
        }
        if self.input:
            item["input"] = self.input
        if self.created_time:
            item["created_time"] = self.created_time
        if self.last_updated_time:
            item["last_updated_time"] = self.last_updated_time
        if camel_case:
            return convert_all_keys_to_camel_case(item)
        return item

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowTrigger:
        return cls(
            external_id=resource["externalId"],
            workflow_external_id=resource["workflowExternalId"],
            workflow_version=resource["workflowVersion"],
            trigger_rule=WorkflowTriggerRule._load(resource["triggerRule"]),
            input=resource.get("input"),
            created_time=resource.get("createdTime"),
            last_updated_time=resource.get("lastUpdatedTime"),
        )

    def as_write(self) -> WorkflowTriggerUpsert:
        """Returns this workflow trigger instance."""
        return WorkflowTriggerUpsert(
            external_id=self.external_id,
            trigger_rule=self.trigger_rule,
            workflow_external_id=self.workflow_external_id,
            workflow_version=self.workflow_version,
            input=self.input,
        )


class WorkflowTriggerUpsertList(CogniteResourceList[WorkflowTriggerUpsert]):
    _RESOURCE = WorkflowTriggerUpsert


class WorkflowTriggerList(WriteableCogniteResourceList[WorkflowTriggerUpsert, WorkflowTrigger]):
    """
    This class represents a list of workflow triggers.
    """

    _RESOURCE = WorkflowTrigger

    def as_write(self) -> WorkflowTriggerUpsertList:
        """Returns a WorkflowTriggerUpsertList object with the same data."""
        return WorkflowTriggerUpsertList([workflow_trigger.as_write() for workflow_trigger in self.data])


class WorkflowTriggerRun(CogniteResource):
    """
    This class represents a workflow trigger run.
    """

    def __init__(
        self,
        external_id: str,
        fire_time: int,
        workflow_external_id: str,
        workflow_version: str,
        status: Literal["success", "failed"],
        workflow_execution_id: str | None = None,
        reason_for_failure: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.fire_time = fire_time
        self.workflow_external_id = workflow_external_id
        self.workflow_version = workflow_version
        self.workflow_execution_id = workflow_execution_id
        self.status = status
        self.reason_for_failure = reason_for_failure

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        item = {
            "external_id": self.external_id,
            "fire_time": self.fire_time,
            "workflow_external_id": self.workflow_external_id,
            "workflow_version": self.workflow_version,
            "status": self.status,
        }
        if self.workflow_execution_id:
            item["workflow_execution_id"] = self.workflow_execution_id
        if self.reason_for_failure:
            item["reason_for_failure"] = self.reason_for_failure
        if camel_case:
            return convert_all_keys_to_camel_case(item)
        return item

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> WorkflowTriggerRun:
        return cls(
            external_id=resource["externalId"],
            fire_time=resource["fireTime"],
            workflow_external_id=resource["workflowExternalId"],
            workflow_version=resource["workflowVersion"],
            status=resource["status"],
            workflow_execution_id=resource.get("workflowExecutionId"),
            reason_for_failure=resource.get("reasonForFailure"),
        )


class WorkflowTriggerRunList(CogniteResourceList[WorkflowTriggerRun]):
    """
    This class represents a list of workflow trigger runs.
    """

    _RESOURCE = WorkflowTriggerRun
