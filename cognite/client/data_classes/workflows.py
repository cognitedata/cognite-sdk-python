from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections import UserList
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Sequence, TypeVar, cast

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    T_CogniteResource,
)
from cognite.client.utils._text import convert_all_keys_to_snake_case, to_snake_case

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class WorkflowCreate(CogniteResource):
    def __init__(self, external_id: str, description: str | None = None):
        self.external_id = external_id
        self.description = description

    @classmethod
    def _load(
        cls: type[T_CogniteResource],
        resource: dict | str,
        cognite_client: CogniteClient | None = None,
    ) -> T_CogniteResource:
        resource = json.loads(resource) if isinstance(resource, str) else resource

        resource = convert_all_keys_to_snake_case(resource)
        return cls(**resource)


class Workflow(WorkflowCreate):
    def __init__(
        self,
        external_id: str,
        created_time: str,
        description: str | None = None,
    ):
        super().__init__(external_id, description)
        self.created_time = created_time


class WorkflowList(CogniteResourceList[Workflow]):
    _RESOURCE = Workflow


class Parameters(CogniteResource, ABC):
    ...


class FunctionParameters(Parameters):
    def __init__(
        self,
        external_id: str,
        data: dict | None = None,
        is_async_complete: bool = False,
    ):
        self.external_id = external_id
        self.data = data
        self.is_async_complete = is_async_complete

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> FunctionParameters:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        function: dict[str, Any] = resource["function"]

        return cls(
            external_id=function["externalId"],
            data=function.get("data"),
            is_async_complete=resource.get("isAsyncComplete", False),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        function: dict[str, Any] = {
            ("externalId" if camel_case else "external_id"): self.external_id,
        }
        if self.data:
            function["data"] = self.data

        output: dict[str, Any] = {
            "function": function,
            "isAsyncComplete": self.is_async_complete,
        }
        return output


class TransformationParameters(Parameters):
    def __init__(self, external_id: str):
        self.external_id = external_id

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> TransformationParameters:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            resource["transformation"]["externalId"],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "transformation": {
                ("externalId" if camel_case else "external_id"): self.external_id,
            }
        }


class CDFRequestParameters(Parameters):
    def __init__(
        self,
        resource_path: str,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        query_parameters: dict | None = None,
        body: dict | None = None,
        request_timeout_in_millis: int = 10000,
    ):
        self.resource_path = resource_path
        self.method = method
        self.query_parameters = query_parameters or {}
        self.body = body or {}
        self.request_timeout_in_millis = request_timeout_in_millis

    @classmethod
    def _load(
        cls: type[T_CogniteResource], resource: dict | str, cognite_client: CogniteClient | None = None
    ) -> T_CogniteResource:
        resource = json.loads(resource) if isinstance(resource, str) else resource

        cdf_request: dict[str, Any] = resource["cdfRequest"]

        return cls(
            resource_path=cdf_request["resourcePath"],
            method=cdf_request["method"],
            query_parameters=cdf_request.get("queryParameters"),
            body=cdf_request.get("body"),
            request_timeout_in_millis=cdf_request.get("requestTimeoutInMillis"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        return {
            ("cdfRequest" if camel_case else "cdfRequest"): output,
        }


class DynamicTaskParameters(Parameters):
    def __init__(self, dynamic: str):
        self.dynamic = dynamic


class Task(CogniteResource):
    def __init__(
        self,
        external_id: str,
        parameters: Parameters,
        name: str | None = None,
        description: str | None = None,
        retries: int = 3,
        timeout: int = 3600,
        depends_on: list[str] | None = None,
    ):
        self.external_id = external_id
        self.type = type
        self.parameters = parameters
        self.name = name
        self.description = description
        self.retries = retries
        self.timeout = timeout
        self.depends_on = depends_on

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> Task:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        type_ = resource["type"]
        parameters: Parameters
        if type_ == "function":
            parameters = FunctionParameters._load(resource["parameters"])
        elif type_ == "transformation":
            parameters = TransformationParameters._load(resource["parameters"])
        elif type_ == "cdf":
            parameters = CDFRequestParameters._load(resource["parameters"])
        elif type_ == "dynamic":
            parameters = DynamicTaskParameters._load(resource["parameters"])
        else:
            raise ValueError(f"Unknown task type: {type_}")
        return cls(
            external_id=resource["externalId"],
            parameters=parameters,
            name=resource.get("name"),
            description=resource.get("description"),
            retries=resource.get("retries", 3),
            timeout=resource.get("timeout", 3600),
            depends_on=[dependency["externalId"] for dependency in depends_on]
            if (depends_on := resource.get("depends_on"))
            else None,
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        if isinstance(self.parameters, FunctionParameters):
            type_ = "function"
        elif isinstance(self.parameters, TransformationParameters):
            type_ = "transformation"
        elif isinstance(self.parameters, CDFRequestParameters):
            type_ = "cdf"
        elif isinstance(self.parameters, DynamicTaskParameters):
            type_ = "dynamic"
        else:
            raise ValueError(f"Unknown task type: {type(self.parameters)}")

        output: dict[str, Any] = {
            ("externalId" if camel_case else "external_id"): self.external_id,
            "type": type_,
            "parameters": self.parameters.dump(camel_case),
            "retries": self.retries,
            "timeout": self.timeout,
        }
        if self.name:
            output["name"] = self.name
        if self.description:
            output["description"] = self.description
        if self.depends_on:
            output["dependsOn"] = [{"externalId": dependency} for dependency in self.depends_on]
        return output


class Output(ABC):
    @classmethod
    @abstractmethod
    def load(cls: type[T_Output], data: dict) -> T_Output:
        raise NotImplementedError()

    @abstractmethod
    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        raise NotImplementedError()


T_Output = TypeVar("T_Output", bound=Output)


class FunctionOutput(Output):
    def __init__(self, call_id: int | None, function_id: int | None, response: dict | None):
        self.call_id = call_id
        self.function_id = function_id
        self.response = response

    @classmethod
    def load(cls, data: dict[str, Any]) -> FunctionOutput:
        return cls(data.get("callId"), data.get("functionId"), data.get("response"))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            ("callId" if camel_case else "call_id"): self.call_id,
            ("functionId" if camel_case else "function_id"): self.function_id,
            "response": self.response,
        }


class TransformationOutput(Output):
    def __init__(self, job_id: int):
        self.job_id = job_id

    @classmethod
    def load(cls, data: dict[str, Any]) -> TransformationOutput:
        return cls(data["jobId"])

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            ("jobId" if camel_case else "job_id"): self.job_id,
        }


class CDFTaskOutput(Output):
    def __init__(self, response: str | dict | None, status_code: int | None):
        self.response = response
        self.status_code = status_code

    @classmethod
    def load(cls, data: dict[str, Any]) -> CDFTaskOutput:
        return cls(data.get("response"), data.get("statusCode"))

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "response": self.response,
            ("statusCode" if camel_case else "status_code"): self.status_code,
        }


class DynamicTaskOutput(Output):
    def __init__(self, tasks: list[Task]):
        self.tasks = tasks

    @classmethod
    def load(cls, data: dict[str, Any]) -> DynamicTaskOutput:
        return cls([Task._load(task) for task in data["tasks"]])

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            "tasks": [task.dump(camel_case) for task in self.tasks],
        }


class TaskExecution(CogniteResource):
    def __init__(
        self,
        id: str,
        external_id: str,
        status: Literal[
            "in_progress",
            "cancelled",
            "failed",
            "failed_with_terminal_error",
            "completed",
            "completed_with_errors",
            "timed_out",
            "skipped",
        ],
        input: dict,
        output: Output,
        version: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
    ):
        self.id = id
        self.external_id = external_id
        self.status = status
        self.input = input
        self.output = output
        self.version = version
        self.start_time = start_time
        self.end_time = end_time
        self.reason_for_incompletion = reason_for_incompletion

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> TaskExecution:
        resource = json.loads(resource) if isinstance(resource, str) else resource

        task_type = resource["taskType"]
        output: Output
        if task_type == "function":
            output = FunctionOutput.load(resource["output"])
        elif task_type == "transformation":
            output = TransformationOutput.load(resource["output"])
        elif task_type == "cdf":
            output = CDFTaskOutput.load(resource["output"])
        elif task_type == "dynamic":
            output = DynamicTaskOutput.load(resource["output"])
        else:
            raise ValueError(f"Unknown task type: {task_type}")

        return cls(
            id=resource["id"],
            external_id=resource["externalId"],
            status=cast(
                Literal[
                    "in_progress",
                    "cancelled",
                    "failed",
                    "failed_with_terminal_error",
                    "completed",
                    "completed_with_errors",
                    "timed_out",
                    "skipped",
                ],
                to_snake_case(resource["status"]),
            ),
            input=resource["input"],
            output=output,
            version=resource.get("version"),
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            reason_for_incompletion=resource.get("reasonForIncompletion"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = super().dump(camel_case)
        task_type_key = "taskType" if camel_case else "task_type"
        if isinstance(self.output, FunctionOutput):
            output[task_type_key] = "function"
        elif isinstance(self.output, TransformationOutput):
            output[task_type_key] = "transformation"
        elif isinstance(self.output, CDFTaskOutput):
            output[task_type_key] = "cdf"
        elif isinstance(self.output, DynamicTaskOutput):
            output[task_type_key] = "dynamic"
        else:
            raise ValueError(f"Unknown task type: {type(self.output)}")
        output["output"] = self.output.dump(camel_case)
        return output


class WorkflowDefinitionCreate(CogniteResource):
    def __init__(
        self,
        tasks: list[Task],
        description: str | None = None,
    ):
        self.hash = hash
        self.tasks = tasks
        self.description = description

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowDefinitionCreate:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            tasks=[Task._load(task) for task in resource["tasks"]],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output: dict[str, Any] = {
            "tasks": [task.dump(camel_case) for task in self.tasks],
        }
        if self.description:
            output["description"] = self.description
        return output


class WorkflowDefinition(WorkflowDefinitionCreate):
    def __init__(
        self,
        hash_: str,
        tasks: list[Task],
        description: str | None = None,
    ):
        super().__init__(tasks, description)
        self.hash_ = hash_

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowDefinition:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            hash_=resource["hash"],
            tasks=[Task._load(task) for task in resource["tasks"]],
            description=resource.get("description"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        output["hash"] = self.hash_
        return output


class WorkflowVersionCreate(CogniteResource):
    def __init__(
        self,
        workflow_external_id: str,
        version: str,
        workflow_definition: WorkflowDefinitionCreate,
    ):
        self.workflow_external_id = workflow_external_id
        self.version = version
        self.workflow_definition = workflow_definition

    @classmethod
    def _load(
        cls: type[T_CogniteResource], resource: dict | str, cognite_client: CogniteClient | None = None
    ) -> T_CogniteResource:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        workflow_definition: dict[str, Any] = resource["workflowDefinition"]
        return cls(
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
            workflow_definition=WorkflowDefinitionCreate._load(workflow_definition),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return {
            ("workflowExternalId" if camel_case else "workflow_external_id"): self.workflow_external_id,
            "version": self.version,
            ("workflowDefinition" if camel_case else "workflow_definition"): self.workflow_definition.dump(camel_case),
        }

    def as_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(
            workflow_external_id=self.workflow_external_id,
            version=self.version,
        )


class WorkflowVersion(WorkflowVersionCreate):
    def __init__(
        self,
        workflow_external_id: str,
        version: str,
        workflow_definition: WorkflowDefinition,
    ):
        super().__init__(workflow_external_id, version, workflow_definition)

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowVersion:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
            workflow_definition=WorkflowDefinition._load(resource["workflowDefinition"]),
        )


class WorkflowVersionList(CogniteResourceList[WorkflowVersion]):
    _RESOURCE = WorkflowVersion

    def as_ids(self) -> WorkflowIds:
        return WorkflowIds([workflow_version.as_id() for workflow_version in self.data])


class WorkflowExecution(CogniteResource):
    def __init__(
        self,
        id: str,
        workflow_external_id: str,
        status: Literal["running", "completed", "failed", "timed_out", "terminated", "paused"],
        created_time: int,
        version: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
    ):
        self.id = id
        self.workflow_external_id = workflow_external_id
        self.version = version
        self.status = status
        self.created_time = created_time
        self.start_time = start_time
        self.end_time = end_time
        self.reason_for_incompletion = reason_for_incompletion

    def as_workflow_id(self) -> WorkflowVersionId:
        return WorkflowVersionId(
            workflow_external_id=self.workflow_external_id,
            version=self.version,
        )

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowExecution:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            id=resource["id"],
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
            status=cast(
                Literal["running", "completed", "failed", "timed_out", "terminated", "paused"],
                to_snake_case(resource["status"]),
            ),
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            reason_for_incompletion=resource.get("reasonForIncompletion"),
        )


class WorkflowExecutionList(CogniteResourceList[WorkflowExecution]):
    _RESOURCE = WorkflowExecution


class WorkflowExecutionDetailed(WorkflowExecution):
    def __init__(
        self,
        id: str,
        workflow_external_id: str,
        workflow_definition: WorkflowDefinition,
        status: Literal["running", "completed", "failed", "timed_out", "terminated", "paused"],
        executed_tasks: list[TaskExecution],
        created_time: int,
        version: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
    ):
        super().__init__(
            id, workflow_external_id, status, created_time, version, start_time, end_time, reason_for_incompletion
        )
        self.workflow_definition = workflow_definition
        self.executed_tasks = executed_tasks

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowExecutionDetailed:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        return cls(
            id=resource["id"],
            workflow_external_id=resource["workflowExternalId"],
            version=resource.get("version"),
            status=cast(
                Literal["running", "completed", "failed", "timed_out", "terminated", "paused"],
                to_snake_case(resource["status"]),
            ),
            created_time=resource["createdTime"],
            start_time=resource.get("startTime"),
            end_time=resource.get("endTime"),
            reason_for_incompletion=resource.get("reasonForIncompletion"),
            workflow_definition=WorkflowDefinition._load(resource["workflowDefinition"]),
            executed_tasks=[TaskExecution._load(task) for task in resource["executedTasks"]],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        output[("workflowDefinition" if camel_case else "workflow_definition")] = self.workflow_definition.dump(
            camel_case
        )
        output[("executedTasks" if camel_case else "executed_tasks")] = [
            task.dump(camel_case) for task in self.executed_tasks
        ]
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
        )


@dataclass(frozen=True)
class WorkflowVersionId:
    workflow_external_id: str
    version: str | None = None

    def as_primitive(self) -> tuple[str, str | None]:
        return self.workflow_external_id, self.version

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowVersionId:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        if "workflowExternalId" in resource:
            workflow_external_id = resource["workflowExternalId"]
        elif "externalId" in resource:
            workflow_external_id = resource["externalId"]
        else:
            raise ValueError("Invalid input to WorkflowVersionId")

        return cls(
            workflow_external_id=workflow_external_id,
            version=resource.get("version"),
        )

    def dump(self, camel_case: bool = False, as_external_id_key: bool = False) -> dict[str, Any]:
        if as_external_id_key:
            output: dict[str, Any] = {
                ("externalId" if camel_case else "external_id"): self.workflow_external_id,
            }
        else:
            output = {
                ("workflowExternalId" if camel_case else "workflow_external_id"): self.workflow_external_id,
            }
        if self.version:
            output["version"] = self.version
        return output


class WorkflowIds(UserList):
    _RESOURCE = WorkflowVersionId

    @classmethod
    def _load(cls, resource: Any, cognite_client: CogniteClient | None = None) -> WorkflowIds:
        workflow_ids: Sequence[WorkflowVersionId]
        if isinstance(resource, tuple) and len(resource) == 2 and all(isinstance(x, str) for x in resource):
            workflow_ids = [WorkflowVersionId(*resource)]
        elif isinstance(resource, WorkflowVersionId):
            workflow_ids = [resource]
        elif isinstance(resource, str):
            workflow_ids = [WorkflowVersionId(workflow_external_id=resource)]
        elif isinstance(resource, dict):
            workflow_ids = [WorkflowVersionId._load(resource)]
        elif isinstance(resource, Sequence) and resource and isinstance(resource[0], tuple):
            workflow_ids = [WorkflowVersionId(*x) for x in resource]
        elif isinstance(resource, Sequence) and resource and isinstance(resource[0], WorkflowVersionId):
            workflow_ids = resource
        elif isinstance(resource, Sequence) and resource and isinstance(resource[0], str):
            workflow_ids = [WorkflowVersionId(workflow_external_id=x) for x in resource]
        else:
            raise ValueError("Invalid input to WorkflowIds")
        return cls(workflow_ids)

    def dump(self, camel_case: bool = False, as_external_id: bool = False) -> list[dict[str, Any]]:
        return [workflow_id.dump(camel_case, as_external_id_key=as_external_id) for workflow_id in self.data]
