from __future__ import annotations

from typing import Literal

from cognite.client.data_classes._base import (
    CogniteResource,
)


class WorkflowCreate(CogniteResource):
    def __init__(self, external_id: str, description: str | None = None):
        self.external_id = external_id
        self.description = description


class Workflow(WorkflowCreate):
    def __init__(
        self,
        external_id: str,
        created_time: int,
        description: str | None = None,
    ):
        super().__init__(external_id, description)
        self.created_time = created_time


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


class TransformationParameters(Parameters):
    def __init__(self, external_id: str):
        self.external_id = external_id


class HTTPRequestParameters(Parameters):
    def __init__(
        self,
        url: str,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        body: dict | None = None,
        headers: dict | None = None,
        request_timeout_millis: int | None = None,
        cdf_authenticate: bool | None = None,
        is_async_complete: bool = False,
    ):
        self.url = url
        self.method = method
        self.body = body
        self.headers = headers
        self.request_timeout_millis = request_timeout_millis
        self.cdf_authenticate = cdf_authenticate
        self.is_async_complete = is_async_complete


class DynamicTaskParameters(Parameters):
    def __init__(self, dynamic: str):
        self.dynamic = dynamic


class Task(CogniteResource):
    def __init__(
        self,
        external_id: str,
        type: Literal["function", "transformation", "http", "dynamic"],
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


class WorkflowDefinition(CogniteResource):
    def __init__(
        self,
        hash: str,
        tasks: list[Task],
        description: str | None = None,
    ):
        self.hash = hash
        self.tasks = tasks
        self.description = description


class WorkflowExecution:
    def __init__(
        self,
        id: str,
        workflow_external_id: str,
        workflow_definition: WorkflowDefinition,
        version: str,
        status: Literal["running", "completed", "failed", "timed_out", "terminated", "paused"],
    ):
        self.id = id
        self.workflow_external_id = workflow_external_id
        self.workflow_definition = workflow_definition
        self.version = version
        self.status = status
