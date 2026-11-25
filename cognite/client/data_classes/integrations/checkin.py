from __future__ import annotations

from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.integrations.integrations import ActiveConfigRevision, Extractor, Task

TaskUpdateType: TypeAlias = Literal["started", "ended"]


class TaskUpdate(CogniteResource):
    """A single start/stop event for a task, reported by the extractor on check-in.

    Args:
        type (TaskUpdateType): Whether the task started or ended.
        name (str): Name of the task being updated.
        timestamp (int): Time of the event, in milliseconds since epoch.
        message (str | None): Optional message tied to the task run.
    """

    def __init__(self, type: TaskUpdateType, name: str, timestamp: int, message: str | None = None) -> None:
        self.type = type
        self.name = name
        self.timestamp = timestamp
        self.message = message

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            type=resource["type"],
            name=resource["name"],
            timestamp=resource["timestamp"],
            message=resource.get("message"),
        )


class ErrorWithTask(CogniteResource):
    """An error reported by the extractor as part of a check-in.

    Args:
        level (Literal['warning', 'error', 'fatal']): Severity of the error.
        description (str): Short description of the error.
        start_time (int): Time the error started, in milliseconds since epoch.
        details (str | None): Full details of the error, e.g. a stack trace.
        task (str | None): Name of the task the error occurred in. Not set if the error applies to the extractor generally.
        end_time (int | None): Time the error was resolved, in milliseconds since epoch. Not set while unresolved.
        active_config_revision (ActiveConfigRevision | None): The config revision (or "local") active when the error occurred.
    """

    def __init__(
        self,
        level: Literal["warning", "error", "fatal"],
        description: str,
        start_time: int,
        details: str | None = None,
        task: str | None = None,
        end_time: int | None = None,
        active_config_revision: ActiveConfigRevision | None = None,
    ) -> None:
        self.level = level
        self.description = description
        self.start_time = start_time
        self.details = details
        self.task = task
        self.end_time = end_time
        self.active_config_revision = active_config_revision

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            level=resource["level"],
            description=resource["description"],
            start_time=resource["startTime"],
            details=resource.get("details"),
            task=resource.get("task"),
            end_time=resource.get("endTime"),
            active_config_revision=resource.get("activeConfigRevision"),
        )


class StartupRequest(CogniteResource):
    """Reported by an extractor on startup: general information about the extractor, and an
    indication that it has (re)started. This closes any currently running tasks with an error.

    Note:
        This is normally only sent by extractor implementations as part of the integrations
        startup protocol, not by typical SDK consumers.

    Args:
        external_id (str): External id of the integration.
        extractor (Extractor): The extractor reporting the startup event.
        tasks (list[Task] | None): The tasks configured for this extractor.
        active_config_revision (ActiveConfigRevision | None): The config revision (or "local") currently active.
        timestamp (int | None): Time of the startup event, in milliseconds since epoch.
    """

    def __init__(
        self,
        external_id: str,
        extractor: Extractor,
        tasks: list[Task] | None = None,
        active_config_revision: ActiveConfigRevision | None = None,
        timestamp: int | None = None,
    ) -> None:
        self.external_id = external_id
        self.extractor = extractor
        self.tasks = tasks
        self.active_config_revision = active_config_revision
        self.timestamp = timestamp

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        result["extractor"] = self.extractor.dump(camel_case)
        if self.tasks is not None:
            result["tasks"] = [task.dump(camel_case) for task in self.tasks]
        return result

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            extractor=Extractor._load(resource["extractor"]),
            tasks=[Task._load(task) for task in resource["tasks"]] if resource.get("tasks") is not None else None,
            active_config_revision=resource.get("activeConfigRevision"),
            timestamp=resource.get("timestamp"),
        )


class CheckinRequest(CogniteResource):
    """Reported periodically by an extractor to signal it is still alive, and to report task
    start/stop events and errors that have occurred since the last check-in.

    Note:
        This is normally only sent by extractor implementations as part of the integrations
        check-in protocol, not by typical SDK consumers.

    Args:
        external_id (str): External id of the integration.
        task_events (list[TaskUpdate] | None): Task start/stop events since the last check-in.
        errors (list[ErrorWithTask] | None): Errors reported since the last check-in.
    """

    def __init__(
        self,
        external_id: str,
        task_events: list[TaskUpdate] | None = None,
        errors: list[ErrorWithTask] | None = None,
    ) -> None:
        self.external_id = external_id
        self.task_events = task_events
        self.errors = errors

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if self.task_events is not None:
            key = "taskEvents" if camel_case else "task_events"
            result[key] = [event.dump(camel_case) for event in self.task_events]
        if self.errors is not None:
            result["errors"] = [error.dump(camel_case) for error in self.errors]
        return result

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            task_events=[TaskUpdate._load(event) for event in resource["taskEvents"]]
            if resource.get("taskEvents") is not None
            else None,
            errors=[ErrorWithTask._load(error) for error in resource["errors"]]
            if resource.get("errors") is not None
            else None,
        )


class CheckinResponse(CogniteResource):
    """Response returned from both startup and check-in, containing the latest config revision.

    Args:
        external_id (str): External id of the integration.
        last_config_revision (int | None): The latest stored configuration revision for this integration.
    """

    def __init__(self, external_id: str, last_config_revision: int | None = None) -> None:
        self.external_id = external_id
        self.last_config_revision = last_config_revision

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            last_config_revision=resource.get("lastConfigRevision"),
        )
