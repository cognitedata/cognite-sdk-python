from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Sequence

from cognite.client._api_client import APIClient
from cognite.client.data_classes.workflows import (
    Workflow,
    WorkflowCreate,
    WorkflowExecution,
    WorkflowExecutionList,
    WorkflowId,
    WorkflowList,
)

if TYPE_CHECKING:
    from cognite.client import ClientConfig, CogniteClient


class WorkflowTasks(APIClient):
    _RESOURCE_PATH = "/workflows/tasks"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def update(self, task_id: str, status: Literal["completed", "failed"], output: dict) -> None:
        ...


class WorkflowExecutions(APIClient):
    _RESOURCE_PATH = "/workflows/executions"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def retrieve(self, external_id: str) -> WorkflowExecution:
        ...

    def trigger(
        self,
        workflow_id: WorkflowId | tuple[str, str],
        input: dict,
        authentication: dict,
    ) -> dict:
        ...

    def list(
        self,
        ids: WorkflowId | Sequence[WorkflowId] | None = None,
        created_time_start: int | None = None,
        created_time_end: int | None = None,
    ) -> WorkflowExecutionList:
        ...


class WorkflowDefinitions(APIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"

    def apply(self, workflow_definition: WorkflowDefinitions) -> Workflow:
        ...

    def delete(
        self,
        workflow_external_id: str,
        versions: str | Sequence[str],
        ignore_unknown_ids: bool = False,
    ) -> None:
        ...

    def list(self, workflow_external_id: str, version: str | None = None) -> WorkflowList:
        ...


class WorkflowAPI(APIClient):
    _RESOURCE_PATH = "/workflows"

    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        super().__init__(config, api_version, cognite_client)
        self._api_subversion = "beta"
        self.definitions = WorkflowDefinitions(config, api_version, cognite_client)
        self.executions = WorkflowExecutions(config, api_version, cognite_client)
        self.tasks = WorkflowTasks(config, api_version, cognite_client)

    def create(self, workflow: WorkflowCreate) -> Workflow:
        ...

    def delete(self, external_id: str | Sequence[str], ignore_unknown_ids: bool = False) -> None:
        ...

    def list(self) -> WorkflowList:
        ...
