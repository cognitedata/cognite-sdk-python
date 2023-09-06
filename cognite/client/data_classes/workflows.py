from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    T_CogniteResource,
)
from cognite.client.utils._text import convert_all_keys_to_snake_case

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


class Parameters(CogniteResource):
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
        request_timeout_millis: int = 10000,
    ):
        self.resource_path = resource_path
        self.method = method
        self.query_parameters = query_parameters or {}
        self.body = body or {}
        self.request_timeout_millis = request_timeout_millis

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
            request_timeout_millis=cdf_request.get("requestTimeoutMillis"),
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


class Output:
    ...


class FunctionOutput(Output):
    def __init__(self, call_id: int, function_id: int, response: dict):
        self.call_id = call_id
        self.function_id = function_id
        self.response = response


class TransformationOutput(Output):
    def __init__(self, call_id: int):
        self.call_id = call_id


class HTTPRequestOutput(Output):
    def __init__(self, response: str | dict, headers: dict, status_code: int):
        self.response = response
        self.headers = headers
        self.status_code = status_code


class DynamicTaskOutput(Output):
    def __init__(self, tasks: list[Task]):
        self.tasks = tasks


class TaskExecution:
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
        task_type: Literal["function", "transformation", "http", "dynamic"],
        started_time: int,
        ended_time: int,
        input: dict,
        output: Output,
    ):
        self.id = id
        self.external_id = external_id
        self.status = status
        self.task_type = task_type
        self.started_time = started_time
        self.ended_time = ended_time
        self.input = input
        self.output = output


class WorkflowVersionCreate(CogniteResource):
    def __init__(
        self,
        workflow_external_id: str,
        version: str,
        tasks: list[Task],
        description: str | None = None,
    ):
        self.workflow_external_id = workflow_external_id
        self.version = version
        self.tasks = tasks
        self.description = description

    @classmethod
    def _load(
        cls: type[T_CogniteResource], resource: dict | str, cognite_client: CogniteClient | None = None
    ) -> T_CogniteResource:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        workflow_definition: dict[str, Any] = resource["workflowDefinition"]
        return cls(
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
            tasks=[Task._load(task) for task in workflow_definition["tasks"]],
            description=workflow_definition.get("description"),
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        workflow_definition: dict[str, Any] = {
            "tasks": [task.dump(camel_case) for task in self.tasks],
        }
        if self.description:
            workflow_definition["description"] = self.description
        return {
            ("workflowExternalId" if camel_case else "workflow_external_id"): self.workflow_external_id,
            "version": self.version,
            ("workflowDefinition" if camel_case else "workflow_definition"): workflow_definition,
        }


class WorkflowVersion(WorkflowVersionCreate):
    def __init__(
        self,
        workflow_external_id: str,
        version: str,
        hash: str,
        tasks: list[Task],
        description: str | None = None,
    ):
        super().__init__(workflow_external_id, version, tasks, description)
        self.hash = hash

    @classmethod
    def _load(cls, resource: dict | str, cognite_client: CogniteClient | None = None) -> WorkflowVersion:
        resource = json.loads(resource) if isinstance(resource, str) else resource
        workflow_definition: dict[str, Any] = resource["workflowDefinition"]
        return cls(
            hash=workflow_definition["hash"],
            tasks=[Task._load(task) for task in workflow_definition["tasks"]],
            description=workflow_definition.get("description"),
            workflow_external_id=resource["workflowExternalId"],
            version=resource["version"],
        )

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        output[("workflowDefinition" if camel_case else "workflow_definition")]["hash"] = self.hash
        return output


class WorkflowVersionList(CogniteResourceList[WorkflowVersion]):
    _RESOURCE = WorkflowVersion


class WorkflowExecution(CogniteResource):
    def __init__(
        self,
        id: str,
        workflow_external_id: str,
        workflow_definition: WorkflowVersion,
        version: str,
        status: Literal["running", "completed", "failed", "timed_out", "terminated", "paused"],
        input: dict | None = None,
        created_time: int | None = None,
        started_time: int | None = None,
        end_time: int | None = None,
        reason_for_incompletion: str | None = None,
    ):
        self.id = id
        self.workflow_external_id = workflow_external_id
        self.workflow_definition = workflow_definition
        self.version = version
        self.status = status
        self.input = input
        self.created_time = created_time
        self.started_time = started_time
        self.end_time = end_time
        self.reason_for_incompletion = reason_for_incompletion


class WorkflowExecutionList(CogniteResourceList[WorkflowExecution]):
    _RESOURCE = WorkflowExecution


@dataclass
class WorkflowId:
    external_id: str
    version: str | None = None
