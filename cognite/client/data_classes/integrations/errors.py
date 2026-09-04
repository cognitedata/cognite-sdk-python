from __future__ import annotations

from typing import Any, Literal, TypeAlias

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, ExternalIDTransformerMixin
from cognite.client.data_classes.integrations.integrations import ActiveConfigRevision

ErrorLevel: TypeAlias = Literal["warning", "error", "fatal"]
IntegrationErrorType: TypeAlias = Literal["general", "config", "task_never_closed", "seen_deadline_missed"]


class IntegrationError(CogniteResource):
    """A problem an extractor encountered while running a task, reported to CDF.

    Args:
        external_id (str): External id of the integration the error belongs to.
        level (ErrorLevel): Severity of the error.
        description (str): Short description of the error.
        start_time (int): Time the error started, in milliseconds since epoch.
        details (str | None): Full details of the error, e.g. a stack trace.
        end_time (int | None): Time the error was resolved, in milliseconds since epoch. Not set while unresolved.
        task (str | None): Name of the task the error occurred in. Not set if the error applies to the extractor generally.
        type (IntegrationErrorType | None): Category of the error.
        active_config_revision (ActiveConfigRevision | None): The config revision (or "local") active when the error occurred.
    """

    def __init__(
        self,
        external_id: str,
        level: ErrorLevel,
        description: str,
        start_time: int,
        details: str | None = None,
        end_time: int | None = None,
        task: str | None = None,
        type: IntegrationErrorType | None = None,
        active_config_revision: ActiveConfigRevision | None = None,
    ) -> None:
        self.external_id = external_id
        self.level = level
        self.description = description
        self.details = details
        self.start_time = start_time
        self.end_time = end_time
        self.task = task
        self.type = type
        self.active_config_revision = active_config_revision

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            level=resource["level"],
            description=resource["description"],
            details=resource.get("details"),
            start_time=resource["startTime"],
            end_time=resource.get("endTime"),
            task=resource.get("task"),
            type=resource.get("type"),
            active_config_revision=resource.get("activeConfigRevision"),
        )


class IntegrationErrorList(CogniteResourceList[IntegrationError], ExternalIDTransformerMixin):
    _RESOURCE = IntegrationError
