from __future__ import annotations

from typing import Any

from typing_extensions import Self

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, ExternalIDTransformerMixin
from cognite.client.data_classes.integrations.errors import IntegrationErrorList
from cognite.client.data_classes.integrations.integrations import ActiveConfigRevision


class TaskHistory(CogniteResource):
    """A single start/stop event of a task, reported by the extractor.

    Args:
        external_id (str): External id of the integration the task belongs to.
        task_name (str): Name of the task.
        start_time (int): Time the task started, in milliseconds since epoch.
        end_time (int | None): Time the task ended, in milliseconds since epoch. Not set while the task is still running.
        message (str | None): Optional message reported when the task started or ended.
        error_count (int): Number of errors reported for this task run.
        warning_count (int): Number of warnings reported for this task run.
        fatal_count (int): Number of fatal errors reported for this task run.
        active_config_revision (ActiveConfigRevision | None): The config revision (or "local") active at the time of this task run.
        sources (list[str] | None): Lineage: URIs of the systems/resources this task read from.
        targets (list[str] | None): Lineage: URIs of the CDF (or other) resources this task wrote to.
    """

    def __init__(
        self,
        external_id: str,
        task_name: str,
        start_time: int,
        end_time: int | None = None,
        message: str | None = None,
        error_count: int = 0,
        warning_count: int = 0,
        fatal_count: int = 0,
        active_config_revision: ActiveConfigRevision | None = None,
        sources: list[str] | None = None,
        targets: list[str] | None = None,
    ) -> None:
        self.external_id = external_id
        self.task_name = task_name
        self.start_time = start_time
        self.end_time = end_time
        self.message = message
        self.error_count = error_count
        self.warning_count = warning_count
        self.fatal_count = fatal_count
        self.active_config_revision = active_config_revision
        self.sources = sources
        self.targets = targets

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            external_id=resource["externalId"],
            task_name=resource["taskName"],
            start_time=resource["startTime"],
            end_time=resource.get("endTime"),
            message=resource.get("message"),
            error_count=resource.get("errorCount", 0),
            warning_count=resource.get("warningCount", 0),
            fatal_count=resource.get("fatalCount", 0),
            active_config_revision=resource.get("activeConfigRevision"),
            sources=resource.get("sources"),
            targets=resource.get("targets"),
        )


class TaskHistoryList(CogniteResourceList[TaskHistory], ExternalIDTransformerMixin):
    _RESOURCE = TaskHistory


class SyncResult(CogniteResource):
    """The result of a single call to the incremental integration sync endpoint.

    Args:
        next_cursor (str): Cursor to pass into the next call to continue from where this page left off.
        more_data (bool): Whether there is more data available immediately (True), or whether the caller should back off before polling again (False).
        history (TaskHistoryList | None): Task history entries since the previous cursor, if requested.
        errors (IntegrationErrorList | None): Errors reported since the previous cursor, if requested.
    """

    def __init__(
        self,
        next_cursor: str,
        more_data: bool = False,
        history: TaskHistoryList | None = None,
        errors: IntegrationErrorList | None = None,
    ) -> None:
        self.next_cursor = next_cursor
        self.more_data = more_data
        self.history = history
        self.errors = errors

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        result = super().dump(camel_case)
        if self.history is not None:
            result["history"] = self.history.dump(camel_case)
        if self.errors is not None:
            result["errors"] = self.errors.dump(camel_case)
        return result

    @classmethod
    def _load(cls, resource: dict[str, Any]) -> Self:
        return cls(
            next_cursor=resource["nextCursor"],
            more_data=resource.get("moreData", False),
            history=TaskHistoryList._load(resource["history"]) if resource.get("history") is not None else None,
            errors=IntegrationErrorList._load(resource["errors"]) if resource.get("errors") is not None else None,
        )
